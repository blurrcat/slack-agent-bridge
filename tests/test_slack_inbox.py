"""Exercise Socket Mode decoding and acknowledgment at the SDK seam."""

import asyncio
import unittest
from contextlib import aclosing
from unittest.mock import AsyncMock

from slack_sdk.socket_mode.request import SocketModeRequest

from slack_agent_bridge.events import SlackReactionAdded, SlackReplyPosted
from slack_agent_bridge.slack_inbox import SlackInbox


class SlackInboxTests(unittest.IsolatedAsyncioTestCase):
    async def test_forwards_events_from_other_channels_and_acks_unsupported_envelopes(self):
        socket = AsyncMock()
        socket.socket_mode_request_listeners = []
        socket.message_processor = asyncio.get_running_loop().create_future()

        async def on_connect():
            receive, = socket.socket_mode_request_listeners
            for index, event in enumerate((
                {"type": "app_mention"},
                {"type": "reaction_added", "user": "UUSER", "reaction": "eyes",
                 "item": {"type": "message", "channel": "COTHER", "ts": "1.000000"}},
                {"type": "message", "user": "UUSER", "text": "reply",
                 "channel": "COTHER", "ts": "2.000000", "thread_ts": "1.000000"},
            )):
                await receive(socket, SocketModeRequest(
                    type="events_api", envelope_id=f"envelope-{index}",
                    payload={"team_id": "TWORKSPACE", "event_id": f"Ev{index}", "event": event},
                ))

        socket.connect.side_effect = on_connect
        async with aclosing(SlackInbox(socket).events()) as events:
            self.assertEqual(await anext(events), SlackReactionAdded(
                "TWORKSPACE", "Ev1", "COTHER", "1.000000", "UUSER", "eyes",
            ))
            reply = await anext(events)
            self.assertIsInstance(reply, SlackReplyPosted)
            self.assertEqual(reply.thread.channel_id, "COTHER")
            self.assertEqual(reply.thread.root_ts, "1.000000")
        self.assertEqual(socket.send_socket_mode_response.await_count, 3)
        socket.close.assert_awaited_once()

    async def test_stream_ends_when_slack_sdk_worker_exits(self):
        socket = AsyncMock()
        socket.socket_mode_request_listeners = []
        socket.message_processor = asyncio.get_running_loop().create_future()
        socket.connect.side_effect = lambda: socket.message_processor.set_result(None)

        async with aclosing(SlackInbox(socket).events()) as events:
            async with asyncio.timeout(0.2):
                with self.assertRaises(StopAsyncIteration):
                    await anext(events)
        socket.close.assert_awaited_once()
