from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from aiohttp import ClientError
from slack_sdk.errors import SlackApiError, SlackRequestError
from slack_sdk.web.async_client import AsyncWebClient

from .events import SlackApprovalCardPosted, SlackApprovalCardPostFailed, SlackEvent, SlackThread


log = logging.getLogger(__name__)


class SlackOutbox:
    def __init__(self, web: AsyncWebClient):
        self._web = web
        self._queue: asyncio.Queue[_Write] = asyncio.Queue()

    def started(self, thread: SlackThread) -> None:
        self._queue.put_nowait(_Reaction(thread, "eyes"))

    def closed(self, thread: SlackThread) -> None:
        self._queue.put_nowait(_Reaction(thread, "white_check_mark"))

    def reply(self, thread: SlackThread, text: str) -> None:
        self._queue.put_nowait(_Reply(thread, text))

    def post_card(self, thread: SlackThread, token: str, text: str,
                  buttons: tuple[tuple[str, str], ...]) -> None:
        self._queue.put_nowait(_Card(thread, token, text, buttons))

    def disable_card(self, thread: SlackThread, ts: str) -> None:
        self._queue.put_nowait(_DisableCard(thread, ts))

    def open_questions(self, trigger_id: str, token: str, questions: tuple[ModalQuestion, ...]) -> None:
        self._queue.put_nowait(_Modal(trigger_id, token, questions))

    async def run(self, publish: Callable[[SlackEvent], None]) -> None:
        while True:
            write = await self._queue.get()
            try:
                async with asyncio.timeout(8):
                    outcome = await self._write(write)
            except (SlackApiError, SlackRequestError, ClientError, OSError) as error:
                log.warning("Slack %s failed (%s).", type(write).__name__, type(error).__name__)
                if isinstance(write, _Card):
                    publish(SlackApprovalCardPostFailed(write.token))
            else:
                if outcome is not None:
                    publish(outcome)

    async def _write(self, write: _Write) -> SlackEvent | None:
        match write:
            case _Reaction(thread, emoji):
                await self._web.reactions_add(channel=thread.channel_id, timestamp=thread.root_ts, name=emoji)
            case _Reply(thread, text):
                await self._web.chat_postMessage(
                    channel=thread.channel_id, thread_ts=thread.root_ts, text=text,
                    unfurl_links=False, unfurl_media=False,
                )
            case _Card(thread, token, text, buttons):
                response = await self._web.chat_postMessage(
                    channel=thread.channel_id, thread_ts=thread.root_ts, text=text,
                    unfurl_links=False, unfurl_media=False,
                    blocks=[
                        {"type": "section", "text": _plain(_preview(text))},
                        {"type": "actions", "elements": [
                            {"type": "button", "action_id": action, "text": _plain(label), "value": token}
                            for action, label in buttons
                        ]},
                    ],
                )
                ts = response.get("ts")
                if not isinstance(ts, str) or not ts:
                    return SlackApprovalCardPostFailed(token)
                return SlackApprovalCardPosted(token, thread, ts)
            case _DisableCard(thread, ts):
                # Remove the buttons without labeling the request approved or declined.
                await self._web.chat_update(channel=thread.channel_id, ts=ts, blocks=[
                    {"type": "section", "text": _plain("This request is no longer accepting responses.")},
                ])
            case _Modal(trigger_id, token, questions):
                await self._web.views_open(trigger_id=trigger_id, view=_modal(token, questions))
        return None


@dataclass(frozen=True)
class ModalQuestion:
    label: str
    hint: str
    choices: tuple[str, ...]


@dataclass(frozen=True)
class _Reaction:
    thread: SlackThread
    emoji: str


@dataclass(frozen=True)
class _Reply:
    thread: SlackThread
    text: str


@dataclass(frozen=True)
class _Card:
    thread: SlackThread
    token: str
    text: str
    buttons: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class _DisableCard:
    thread: SlackThread
    ts: str


@dataclass(frozen=True)
class _Modal:
    trigger_id: str
    token: str
    questions: tuple[ModalQuestion, ...]


_Write = _Reaction | _Reply | _Card | _DisableCard | _Modal


def _modal(token: str, questions: tuple[ModalQuestion, ...]) -> dict[str, Any]:
    blocks: list[dict[str, Any]] = []
    for index, question in enumerate(questions):
        if question.hint:
            blocks.append({"type": "section", "text": _plain(question.hint)})
        element: dict[str, Any] = {"type": "plain_text_input", "action_id": "answer"}
        if question.choices:
            element = {"type": "static_select", "action_id": "answer", "options": [
                {"text": _plain(choice), "value": choice} for choice in question.choices
            ]}
        blocks.append({
            "type": "input", "block_id": f"q{index}", "label": _plain(question.label[:200] or "Answer"),
            "element": element,
        })
    return {
        "type": "modal", "callback_id": "bridge:input", "private_metadata": token,
        "title": _plain("Codex input"), "submit": _plain("Submit"), "blocks": blocks,
    }


def _plain(text: str) -> dict[str, str]:
    return {"type": "plain_text", "text": text}


def _preview(text: str) -> str:
    suffix = "\n\n[Approval details trimmed]"
    return text if len(text) <= 3000 else text[:3000 - len(suffix)] + suffix
