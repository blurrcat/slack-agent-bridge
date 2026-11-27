import asyncio
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from slack_agent_bridge.codex import Codex
from slack_agent_bridge.config import Config
from slack_agent_bridge.inbox import Inbox, SessionStore
from slack_agent_bridge.slack import SlackOutput, message_from_event


class InboxTests(unittest.IsolatedAsyncioTestCase):
    async def test_owner_job_replies_once_in_the_session_thread(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Config("CINBOX", "UOWNER", "xapp-test", "xoxb-test")
            web = AsyncMock()
            command = (sys.executable, str(Path(__file__).with_name("fake_codex.py").resolve()))
            with SessionStore(Path(tmp) / "state.sqlite3") as store:
                async with Codex(command) as codex:
                    inbox = Inbox(config, "TWORKSPACE", "UBRIDGE", codex, store, SlackOutput(web))
                    event = message_from_event({
                        "team_id": "TWORKSPACE", "event_id": "Ev1",
                        "event": {
                            "type": "message", "channel": "CINBOX", "ts": "1.100000",
                            "text": "hello", "user": "UOWNER",
                        },
                    })
                    await asyncio.gather(inbox.handle(event), inbox.handle(event))
            web.reactions_add.assert_awaited_once_with(channel="CINBOX", timestamp="1.100000", name="eyes")
            web.chat_postMessage.assert_awaited_once_with(
                channel="CINBOX", thread_ts="1.100000", text="codex-1: hello",
                unfurl_links=False, unfurl_media=False,
            )

    async def test_only_an_approved_workflow_bot_can_start_a_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Config(
                "CINBOX", "UOWNER", "xapp-test", "xoxb-test",
                allowed_bot_ids=frozenset({"BFLOW", "BBRIDGE"}),
            )
            web = AsyncMock()
            command = (sys.executable, str(Path(__file__).with_name("fake_codex.py").resolve()))
            payload = {
                "team_id": "TWORKSPACE", "event_id": "Ev1",
                "event": {
                    "type": "message", "subtype": "bot_message", "channel": "CINBOX",
                    "ts": "1.100000", "text": "investigate https://example.test/archives/target",
                },
            }
            with SessionStore(Path(tmp) / "state.sqlite3") as store:
                async with Codex(command) as codex:
                    inbox = Inbox(config, "TWORKSPACE", "UBRIDGE", codex, store, SlackOutput(web), "BBRIDGE")
                    for bot_id in ("BOTHER", "BBRIDGE", "BFLOW"):
                        event = message_from_event({
                            **payload, "event": {**payload["event"], "bot_id": bot_id},
                        })
                        await inbox.handle(event)
            web.chat_postMessage.assert_awaited_once_with(
                channel="CINBOX", thread_ts="1.100000",
                text="codex-1: investigate https://example.test/archives/target",
                unfurl_links=False, unfurl_media=False,
            )


    async def test_session_storage_uses_write_ahead_logging(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.sqlite3"
            with SessionStore(path):
                connection = sqlite3.connect(path)
                try:
                    self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone(), ("wal",))
                finally:
                    connection.close()

    async def test_job_still_replies_if_the_reaction_times_out(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Config("CINBOX", "UOWNER", "xapp-test", "xoxb-test")
            web = AsyncMock()
            web.reactions_add.side_effect = TimeoutError
            command = (sys.executable, str(Path(__file__).with_name("fake_codex.py").resolve()))
            with SessionStore(Path(tmp) / "state.sqlite3") as store:
                async with Codex(command) as codex:
                    inbox = Inbox(config, "TWORKSPACE", "UBRIDGE", codex, store, SlackOutput(web))
                    await inbox.handle(message_from_event({
                        "team_id": "TWORKSPACE", "event_id": "Ev1",
                        "event": {
                            "type": "message", "channel": "CINBOX", "ts": "1.100000",
                            "text": "hello", "user": "UOWNER",
                        },
                    }))
            web.chat_postMessage.assert_awaited_once_with(
                channel="CINBOX", thread_ts="1.100000", text="codex-1: hello",
                unfurl_links=False, unfurl_media=False,
            )
