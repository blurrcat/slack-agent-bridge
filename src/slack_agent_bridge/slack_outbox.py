"""FIFO worker for the Bridge's outbound Slack operations."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from aiohttp import ClientError
from slack_sdk.errors import SlackApiError, SlackRequestError
from slack_sdk.web.async_client import AsyncWebClient

from .events import (
    SlackApprovalCardPosted, SlackApprovalCardPostFailed, SlackEvent, SlackMessagePosted, SlackOriginLookedUp, SlackQuestionsOpenFailed, SlackThread,
)


log = logging.getLogger(__name__)


class SlackOutbox:
    """Enqueue casts synchronously and run all Web API calls in one worker."""
    def __init__(self, web: AsyncWebClient):
        self._web = web
        self._queue: asyncio.Queue[_Write] = asyncio.Queue()

    def started(self, thread: SlackThread) -> None:
        """Queue the acceptance reaction after the Agent Session opens."""
        self._queue.put_nowait(_Reaction(thread, "eyes"))

    def closed(self, thread: SlackThread) -> None:
        """Queue the close reaction; a subsequent farewell will follow it in FIFO order."""
        self._queue.put_nowait(_Reaction(thread, "white_check_mark"))

    def reply(self, thread: SlackThread, text: str) -> None:
        """Queue a threaded reply without waiting for Slack."""
        self._queue.put_nowait(_Reply(thread, text))

    def post_card(self, thread: SlackThread, token: str, text: str,
                  buttons: tuple[tuple[str, str], ...]) -> None:
        """Queue a card whose timestamp or failure will come back as an event."""
        self._queue.put_nowait(_Card(thread, token, text, buttons))

    def disable_card(self, thread: SlackThread, ts: str) -> None:
        """Queue removal of the buttons without claiming the request was approved."""
        self._queue.put_nowait(_DisableCard(thread, ts))

    def lookup_origin(self, thread: SlackThread) -> None:
        """Queue a history read and report the exact root message, if available."""
        self._queue.put_nowait(_LookupOrigin(thread))

    def open_questions(self, trigger_id: str, token: str, questions: tuple[ModalQuestion, ...]) -> None:
        """Queue use of a short-lived trigger; report failure so the Owner can retry."""
        self._queue.put_nowait(_Modal(trigger_id, token, questions))

    async def run(self, publish: Callable[[SlackEvent], None]) -> None:
        """Execute one command at a time and publish only outcomes that change Bridge policy."""
        while True:
            write = await self._queue.get()
            try:
                async with asyncio.timeout(8):
                    outcome = await self._write(write)
            except (SlackApiError, SlackRequestError, ClientError, OSError) as error:
                log.warning("Slack %s failed (%s).", type(write).__name__, type(error).__name__)
                if isinstance(write, _Card):
                    publish(SlackApprovalCardPostFailed(write.token))
                elif isinstance(write, _Modal):
                    publish(SlackQuestionsOpenFailed(write.token))
                elif isinstance(write, _LookupOrigin):
                    publish(SlackOriginLookedUp(write.thread, None))
            else:
                if outcome is not None:
                    publish(outcome)

    async def _write(self, write: _Write) -> SlackEvent | None:
        """Translate one queued command into Slack SDK calls."""
        match write:
            case _Reaction(thread, emoji):
                await self._web.reactions_add(channel=thread.channel_id, timestamp=thread.root_ts, name=emoji)
            case _Reply(thread, text):
                await self._web.chat_postMessage(
                    channel=thread.channel_id, thread_ts=thread.root_ts, text=_escape(text),
                    unfurl_links=False, unfurl_media=False,
                )
            case _Card(thread, token, text, buttons):
                response = await self._web.chat_postMessage(
                    channel=thread.channel_id, thread_ts=thread.root_ts,
                    text="Codex needs the bridge owner's input.", unfurl_links=False, unfurl_media=False,
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
            case _LookupOrigin(thread):
                response = await self._web.conversations_history(
                    channel=thread.channel_id, latest=thread.root_ts, inclusive=True, limit=1,
                )
                return SlackOriginLookedUp(thread, _exact_origin(thread, response.get("messages")))
        return None


@dataclass(frozen=True)
class ModalQuestion:
    """Slack presentation data; validation and question IDs stay with the Bridge."""
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


@dataclass(frozen=True)
class _LookupOrigin:
    thread: SlackThread


_Write = _Reaction | _Reply | _Card | _DisableCard | _Modal | _LookupOrigin


def _modal(token: str, questions: tuple[ModalQuestion, ...]) -> dict[str, Any]:
    """Build Slack Block Kit input rows addressed by their question position."""
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
    """Fit approval details into Slack's section limit with an explicit truncation marker."""
    suffix = "\n\n[Approval details trimmed]"
    return text if len(text) <= 3000 else text[:3000 - len(suffix)] + suffix


def _escape(text: str) -> str:
    # Keep Markdown formatting without activating Slack mentions or special links.
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _exact_origin(thread: SlackThread, messages: object) -> SlackMessagePosted | None:
    """Reject nearby messages: only the requested root can authorize recovery."""
    if not isinstance(messages, list) or len(messages) != 1 or not isinstance(messages[0], dict):
        return None
    record = messages[0]
    text = record.get("text")
    if record.get("ts") != thread.root_ts or record.get("thread_ts", thread.root_ts) != thread.root_ts:
        return None
    if not isinstance(text, str):
        return None
    user, bot, subtype = (record.get(key) for key in ("user", "bot_id", "subtype"))
    return SlackMessagePosted(thread, thread.root_ts, thread.root_ts, text,
                              user if isinstance(user, str) else None,
                              bot if isinstance(bot, str) else None,
                              subtype if isinstance(subtype, str) else None)
