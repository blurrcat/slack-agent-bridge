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
from .json_decoder import as_object, as_string


log = logging.getLogger(__name__)


class SlackInbox:
    """Single-consumer Slack event source; it does not authorize or route work."""
    def __init__(self, socket: SocketModeClient):
        self._socket = socket
        self._queue: asyncio.Queue[SlackEvent | None] = asyncio.Queue()
        self._processor = socket.message_processor
        socket.socket_mode_request_listeners.append(self._receive)
        if self._processor is not None:
            self._processor.add_done_callback(self._processor_stopped)

    async def events(self) -> AsyncIterator[SlackEvent]:
        """Connect and yield observations until the SDK processor stops or the pump is cancelled."""
        try:
            async with asyncio.timeout(15):
                await self._socket.connect()
            log.info("Connected to Slack Socket Mode.")
            while (event := await self._queue.get()) is not None:
                yield event
        finally:
            if self._processor is not None:
                self._processor.remove_done_callback(self._processor_stopped)
            self._socket.socket_mode_request_listeners.remove(self._receive)
            await self._socket.close()

    async def _receive(self, client: AsyncBaseSocketModeClient, request: SocketModeRequest) -> None:
        """Queue observations and acknowledge even envelopes that have no supported event."""
        event = _decode(request)
        if event is not None:
            self._queue.put_nowait(event)
        if request.envelope_id:
            await client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))

    def _processor_stopped(self, task: asyncio.Future[None]) -> None:
        """Wake the iterator so the Bridge can see that its producer ended."""
        if not task.cancelled() and (error := task.exception()) is not None:
            log.error("Slack SDK message processor failed (%s).", type(error).__name__)
        self._queue.put_nowait(None)


def _decode(request: SocketModeRequest) -> SlackEvent | None:
    """Decode only the wire shape; leave workspace, channel, and sender checks to Bridge."""
    payload = request.payload
    if request.type == "interactive":
        return _interaction(payload)
    if request.type != "events_api":
        return None

    event = as_object(payload.get("event"))
    workspace = as_string(payload.get("team_id"))
    event_id = as_string(payload.get("event_id"))
    if not workspace or not event_id:
        return None
    match event.get("type"):
        case "message":
            channel = as_string(event.get("channel"))
            ts = _timestamp(event.get("ts"))
            root = _timestamp(event.get("thread_ts", ts))
            body = as_string(event.get("text"))
            if channel and ts and root and body is not None:
                kind = SlackMessagePosted if root == ts else SlackReplyPosted
                return kind(SlackThread(workspace, channel, root), event_id, ts, body,
                            as_string(event.get("user")), as_string(event.get("bot_id")),
                            as_string(event.get("subtype")))
        case "reaction_added" | "reaction_removed":
            item = as_object(event.get("item"))
            channel = as_string(item.get("channel"))
            ts = _timestamp(item.get("ts"))
            user = as_string(event.get("user"))
            emoji = as_string(event.get("reaction"))
            if item.get("type") == "message" and channel and ts and user and emoji:
                kind = SlackReactionAdded if event["type"] == "reaction_added" else SlackReactionRemoved
                return kind(workspace, event_id, channel, ts, user, emoji)
    return None


def _interaction(payload: dict[str, Any]) -> SlackInteractionReceived | None:
    """Preserve button or modal identifiers without interpreting what they authorize."""
    workspace = (
        as_string(as_object(payload.get("team")).get("id")) or as_string(payload.get("app_installed_team_id"))
    )
    user = as_string(as_object(payload.get("user")).get("id"))
    if not workspace or not user:
        return None
    match payload.get("type"):
        case "block_actions":
            actions = payload.get("actions")
            if not isinstance(actions, list) or not actions:
                return None
            action = as_object(actions[0])
            action_id = as_string(action.get("action_id"))
            value = as_string(action.get("value"))
            if not action_id or value is None:
                return None
            container = as_object(payload.get("container"))
            return SlackInteractionReceived(workspace, user, action_id, value,
                                            as_string(container.get("channel_id")),
                                            _timestamp(container.get("message_ts")),
                                            as_string(payload.get("trigger_id")))
        case "view_submission":
            view = as_object(payload.get("view"))
            callback = as_string(view.get("callback_id"))
            metadata = as_string(view.get("private_metadata"))
            if not callback or metadata is None:
                return None
            state = as_object(as_object(view.get("state")).get("values"))
            return SlackInteractionReceived(workspace, user, callback, metadata,
                                            input_values=_input_values(state))
    return None


def _input_values(state: dict[str, Any]) -> tuple[tuple[str, str, str], ...]:
    """Flatten Slack's nested view state into immutable block, action, and value triples."""
    values = []
    for block_id, block in state.items():
        for action_id, raw in as_object(block).items():
            action = as_object(raw)
            value = as_string(action.get("value"))
            if value is None:
                value = as_string(as_object(action.get("selected_option")).get("value"))
            if value is not None:
                values.append((block_id, action_id, value))
    return tuple(values)


def _timestamp(value: object) -> str | None:
    """Validate Slack's timestamp shape without losing its exact string identity."""
    return value if isinstance(value, str) and re.fullmatch(r"[0-9]+\.[0-9]+", value) else None
