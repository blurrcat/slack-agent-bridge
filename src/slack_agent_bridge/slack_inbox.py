"""Socket Mode intake and Slack payload decoding, without Bridge policy."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator
from typing import Any

from slack_sdk.socket_mode.aiohttp import SocketModeClient
from slack_sdk.socket_mode.async_client import AsyncBaseSocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse

from .events import (
    SlackEvent, SlackInteractionReceived, SlackMessagePosted, SlackReactionAdded,
    SlackReactionRemoved, SlackReplyPosted, SlackThread,
)


log = logging.getLogger(__name__)


class SlackInbox:
    """Single-consumer Slack event source; it does not authorize or route work."""
    def __init__(self, socket: SocketModeClient):
        self._socket = socket

    async def events(self) -> AsyncIterator[SlackEvent]:
        """Yield supported observations until the SDK processor exits or the pump is cancelled."""
        queue: asyncio.Queue[SlackEvent | None] = asyncio.Queue()

        def processor_stopped(task: asyncio.Future[None]) -> None:
            """Wake the iterator so the Bridge can see that its producer ended."""
            if not task.cancelled() and (error := task.exception()) is not None:
                log.error("Slack SDK message processor failed (%s).", type(error).__name__)
            queue.put_nowait(None)

        async def receive(client: AsyncBaseSocketModeClient, request: SocketModeRequest) -> None:
            """Queue observations and acknowledge even envelopes that have no supported event."""
            event = _decode(request)
            if event is not None:
                queue.put_nowait(event)
            if request.envelope_id:
                await client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))

        listeners = self._socket.socket_mode_request_listeners
        listeners.append(receive)
        processor = self._socket.message_processor
        if processor is not None:
            processor.add_done_callback(processor_stopped)
        try:
            async with asyncio.timeout(15):
                await self._socket.connect()
            log.info("Connected to Slack Socket Mode.")
            while (event := await queue.get()) is not None:
                yield event
        finally:
            if processor is not None:
                processor.remove_done_callback(processor_stopped)
            listeners.remove(receive)
            await self._socket.close()


def _decode(request: SocketModeRequest) -> SlackEvent | None:
    """Decode only the wire shape; leave workspace, channel, and sender checks to Bridge."""
    payload = request.payload
    if request.type == "interactive":
        return _interaction(payload)
    if request.type != "events_api":
        return None

    event = _object(payload.get("event"))
    workspace = _string(payload.get("team_id"))
    event_id = _string(payload.get("event_id"))
    if not workspace or not event_id:
        return None
    match event.get("type"):
        case "message":
            channel = _string(event.get("channel"))
            ts = _timestamp(event.get("ts"))
            root = _timestamp(event.get("thread_ts", ts))
            body = _string(event.get("text"))
            if channel and ts and root and body is not None:
                kind = SlackMessagePosted if root == ts else SlackReplyPosted
                return kind(SlackThread(workspace, channel, root), event_id, ts, body,
                            _string(event.get("user")), _string(event.get("bot_id")),
                            _string(event.get("subtype")))
        case "reaction_added" | "reaction_removed":
            item = _object(event.get("item"))
            channel = _string(item.get("channel"))
            ts = _timestamp(item.get("ts"))
            user = _string(event.get("user"))
            emoji = _string(event.get("reaction"))
            if item.get("type") == "message" and channel and ts and user and emoji:
                kind = SlackReactionAdded if event["type"] == "reaction_added" else SlackReactionRemoved
                return kind(workspace, event_id, channel, ts, user, emoji)
    return None


def _interaction(payload: dict[str, Any]) -> SlackInteractionReceived | None:
    """Preserve button or modal identifiers without interpreting what they authorize."""
    workspace = (
        _string(_object(payload.get("team")).get("id")) or _string(payload.get("app_installed_team_id"))
    )
    user = _string(_object(payload.get("user")).get("id"))
    if not workspace or not user:
        return None
    match payload.get("type"):
        case "block_actions":
            actions = payload.get("actions")
            if not isinstance(actions, list) or not actions:
                return None
            action = _object(actions[0])
            action_id = _string(action.get("action_id"))
            value = _string(action.get("value"))
            if not action_id or value is None:
                return None
            container = _object(payload.get("container"))
            return SlackInteractionReceived(workspace, user, action_id, value,
                                            _string(container.get("channel_id")),
                                            _timestamp(container.get("message_ts")),
                                            _string(payload.get("trigger_id")))
        case "view_submission":
            view = _object(payload.get("view"))
            callback = _string(view.get("callback_id"))
            metadata = _string(view.get("private_metadata"))
            if not callback or metadata is None:
                return None
            state = _object(_object(view.get("state")).get("values"))
            return SlackInteractionReceived(workspace, user, callback, metadata,
                                            input_values=_input_values(state))
    return None


def _input_values(state: dict[str, Any]) -> tuple[tuple[str, str, str], ...]:
    """Flatten Slack's nested view state into immutable block, action, and value triples."""
    values = []
    for block_id, block in state.items():
        for action_id, raw in _object(block).items():
            action = _object(raw)
            value = _string(action.get("value"))
            if value is None:
                value = _string(_object(action.get("selected_option")).get("value"))
            if value is not None:
                values.append((block_id, action_id, value))
    return tuple(values)


def _object(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _timestamp(value: object) -> str | None:
    """Validate Slack's timestamp shape without losing its exact string identity."""
    return value if isinstance(value, str) and re.fullmatch(r"[0-9]+\.[0-9]+", value) else None
