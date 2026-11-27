import asyncio
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, call, patch

from slack_agent_bridge.codex import Codex
from slack_agent_bridge.config import Config
from slack_agent_bridge.inbox import Inbox, SessionStore
from slack_agent_bridge.slack import SlackOutput, interaction_from_payload, message_from_event


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
            self.assertEqual(web.reactions_add.await_args_list.count(
                call(channel="CINBOX", timestamp="1.100000", name="eyes")
            ), 1)
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

    async def test_authorized_followup_uses_the_existing_codex_thread(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Config("CINBOX", "UOWNER", "xapp-test", "xoxb-test", allowed_user_ids=frozenset({"UGUEST"}))
            web = AsyncMock()
            command = (sys.executable, str(Path(__file__).with_name("fake_codex.py").resolve()))
            with SessionStore(Path(tmp) / "state.sqlite3") as store:
                async with Codex(command) as codex:
                    inbox = Inbox(config, "TWORKSPACE", "UBRIDGE", codex, store, SlackOutput(web))
                    await inbox.handle(slack_message("1.000000", "hello", user="UGUEST"))
                    await inbox.handle(slack_message("2.000000", "not allowed", root="1.000000", user="UOTHER"))
                    await inbox.handle(slack_message("3.000000", "again", root="1.000000", user="UGUEST"))
            replies = [
                call.kwargs["text"] for call in web.chat_postMessage.await_args_list
                if call.kwargs.get("thread_ts") == "1.000000"
            ]
            self.assertIn("codex-1: again", replies)
            self.assertNotIn("codex-1: not allowed", replies)
            self.assertEqual(sum(x.kwargs["name"] == "eyes" for x in web.reactions_add.await_args_list), 1)


    async def test_busy_followup_steers_interrupt_leaves_open_and_close_stays_closed(self):
        # A follow-up can be posted before its turn actually begins.
        with tempfile.TemporaryDirectory() as tmp, patch(
            "slack_agent_bridge.inbox.time_ns",
            side_effect=[1_000_000_000, 3_000_000_000, 4_500_000_000, 6_000_000_000, 7_000_000_000],
        ):
            config = Config("CINBOX", "UOWNER", "xapp-test", "xoxb-test", allowed_user_ids=frozenset({"UGUEST"}))
            web = AsyncMock()
            reached_tool = asyncio.Event()

            async def watch_output(**kwargs):
                if kwargs["text"] == "Command completed.":
                    reached_tool.set()

            web.chat_postMessage.side_effect = watch_output
            command = (sys.executable, str(Path(__file__).with_name("fake_codex.py").resolve()))
            with SessionStore(Path(tmp) / "state.sqlite3") as store:
                async with Codex(command) as codex, asyncio.TaskGroup() as jobs:
                    inbox = Inbox(config, "TWORKSPACE", "UBRIDGE", codex, store, SlackOutput(web))
                    initial = jobs.create_task(inbox.handle(slack_message("1.000000", "hold", user="UGUEST")))
                    async with asyncio.timeout(2):
                        await reached_tool.wait()
                        await inbox.handle(slack_message("2.000000", "!done", root="1.000000", user="UGUEST"))
                        await inbox.handle(slack_message("3.000000", "change direction", root="1.000000", user="UGUEST"))
                        await initial
                        reached_tool.clear()
                        second = jobs.create_task(inbox.handle(slack_message("4.000000", "hold", root="1.000000")))
                        await reached_tool.wait()
                        # A delayed interrupt posted before this turn cannot stop it.
                        await inbox.handle(slack_message("4.250000", "!esc", root="1.000000"))
                        self.assertFalse(second.done())
                        await inbox.handle(slack_message("5.000000", "!esc", root="1.000000"))
                        await second
                        await inbox.handle(slack_message("6.000000", "still open", root="1.000000"))
                        reached_tool.clear()
                        last = jobs.create_task(inbox.handle(slack_message("7.000000", "hold", root="1.000000")))
                        await reached_tool.wait()
                        await inbox.handle(slack_message("8.000000", "!end", root="1.000000"))
                        await last
                        await inbox.handle(slack_message("9.000000", "must not run", root="1.000000"))
            output = [args.kwargs["text"] for args in web.chat_postMessage.await_args_list]
            self.assertEqual(output.count("steered: change direction"), 1)
            self.assertIn("Turn interrupted.", output)
            self.assertIn("codex-1: still open", output)
            self.assertNotIn("codex-1: must not run", output)
            self.assertEqual(output.count("Bye."), 1)
            self.assertFalse(any("DO NOT EXPOSE" in line for line in output))
            self.assertIn(call(channel="CINBOX", timestamp="1.000000", name="white_check_mark"),
                          web.reactions_add.await_args_list)


    async def test_approval_requires_the_owner_and_honors_the_chosen_scope(self):
        with tempfile.TemporaryDirectory() as tmp, patch("slack_agent_bridge.inbox.time_ns", return_value=0):
            config = Config("CINBOX", "UOWNER", "xapp-test", "xoxb-test", allowed_user_ids=frozenset({"UGUEST"}))
            web = AsyncMock()
            card_ready = asyncio.Event()
            buttons = []

            async def post(**kwargs):
                for block in kwargs.get("blocks", []):
                    if block["type"] == "actions":
                        buttons.extend(block["elements"])
                if buttons and kwargs.get("blocks"):
                    card_ready.set()
                return {"ts": "2.000000"}

            web.chat_postMessage.side_effect = post
            command = (sys.executable, str(Path(__file__).with_name("fake_codex.py").resolve()))
            with SessionStore(Path(tmp) / "state.sqlite3") as store:
                async with Codex(command) as codex, asyncio.TaskGroup() as jobs:
                    inbox = Inbox(config, "TWORKSPACE", "UBRIDGE", codex, store, SlackOutput(web))
                    job = jobs.create_task(inbox.handle(slack_message("1.000000", "approve", user="UGUEST")))
                    async with asyncio.timeout(2):
                        await card_ready.wait()
                        approve = next(button for button in buttons if button["text"]["text"] == "Approve once")

                        def click(user, button=approve, channel="CINBOX"):
                            return interaction_from_payload({
                                "type": "block_actions", "team": {"id": "TWORKSPACE"},
                                "user": {"id": user}, "container": {"channel_id": channel, "message_ts": "2.000000"},
                                "actions": [button],
                            })

                        await inbox.interact(click("UGUEST"))
                        await inbox.interact(click("UOWNER", channel="OTHER"))
                        self.assertFalse(job.done())
                        # A confirmed rejection of !esc must not strand an owner approval.
                        await inbox.handle(slack_message("3.000000", "!esc", root="1.000000"))
                        await inbox.interact(click("UOWNER"))
                        await job
                        buttons.clear()
                        card_ready.clear()
                        followup = jobs.create_task(inbox.handle(slack_message("4.000000", "approve", root="1.000000")))
                        await card_ready.wait()
                        approve_session = next(
                            button for button in buttons if button["text"]["text"] == "Approve for session"
                        )
                        await inbox.interact(click("UOWNER"))  # The old button cannot answer this request.
                        await inbox.interact(click("UGUEST", approve_session))
                        self.assertFalse(followup.done())
                        await inbox.interact(click("UOWNER", approve_session))
                        await followup
            replies = [c.kwargs["text"] for c in web.chat_postMessage.await_args_list]
            self.assertEqual(replies.count("decision: accept"), 1)
            self.assertEqual(replies.count("decision: acceptForSession"), 1)


    async def test_owner_answers_a_question_through_a_modal(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Config("CINBOX", "UOWNER", "xapp-test", "xoxb-test")
            web = AsyncMock()
            card_ready = asyncio.Event()
            buttons = []

            async def post(**kwargs):
                for block in kwargs.get("blocks", []):
                    if block["type"] == "actions":
                        buttons.extend(block["elements"])
                        card_ready.set()
                return {"ts": "2.000000"}

            web.chat_postMessage.side_effect = post
            command = (sys.executable, str(Path(__file__).with_name("fake_codex.py").resolve()))
            with SessionStore(Path(tmp) / "state.sqlite3") as store:
                async with Codex(command) as codex, asyncio.TaskGroup() as jobs:
                    inbox = Inbox(config, "TWORKSPACE", "UBRIDGE", codex, store, SlackOutput(web))
                    job = jobs.create_task(inbox.handle(slack_message("1.000000", "question")))
                    async with asyncio.timeout(2):
                        await card_ready.wait()
                        await inbox.interact(interaction_from_payload({
                            "type": "block_actions", "team": {"id": "TWORKSPACE"},
                            "user": {"id": "UOWNER"}, "container": {
                                "channel_id": "CINBOX", "message_ts": "2.000000",
                            },
                            "actions": buttons, "trigger_id": "trigger",
                        }))
                        assert web.views_open.await_args is not None
                        modal = web.views_open.await_args.kwargs["view"]
                        self.assertEqual(modal["blocks"][1]["element"]["options"][0]["value"], "Python")

                        def submit(user, answer):
                            return interaction_from_payload({
                                "type": "view_submission", "team": {"id": "TWORKSPACE"},
                                "user": {"id": user}, "view": {
                                    "callback_id": modal["callback_id"], "private_metadata": modal["private_metadata"],
                                    "state": {"values": {"q0": {"answer": {"selected_option": {"value": answer}}}}},
                                },
                            })

                        await inbox.interact(submit("UGUEST", "Python"))
                        await inbox.interact(submit("UOWNER", "Invalid option"))
                        self.assertFalse(job.done())
                        await inbox.interact(submit("UOWNER", "Python"))
                        await job
            self.assertIn("decision: Python", [c.kwargs["text"] for c in web.chat_postMessage.await_args_list])


    async def test_followup_opens_a_new_turn_if_the_old_turn_already_finished(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Config("CINBOX", "UOWNER", "xapp-test", "xoxb-test")
            web = AsyncMock()
            reached_tool = asyncio.Event()

            async def post(**kwargs):
                if kwargs["text"] == "Command completed.":
                    reached_tool.set()

            web.chat_postMessage.side_effect = post
            command = (sys.executable, str(Path(__file__).with_name("fake_codex.py").resolve()))
            with SessionStore(Path(tmp) / "state.sqlite3") as store:
                async with Codex(command) as codex, asyncio.TaskGroup() as jobs:
                    inbox = Inbox(config, "TWORKSPACE", "UBRIDGE", codex, store, SlackOutput(web))
                    jobs.create_task(inbox.handle(slack_message("1.000000", "finish-before-followup")))
                    async with asyncio.timeout(3):
                        await reached_tool.wait()
                        await inbox.handle(slack_message("2.000000", "next", root="1.000000"))
            replies = [c.kwargs["text"] for c in web.chat_postMessage.await_args_list]
            self.assertIn("finished before follow-up", replies)
            self.assertEqual(replies.count("codex-1: next"), 1)


def slack_message(ts, text, *, root=None, user="UOWNER"):
    event = {"type": "message", "channel": "CINBOX", "ts": ts, "text": text, "user": user}
    if root is not None:
        event["thread_ts"] = root
    return message_from_event({"team_id": "TWORKSPACE", "event_id": f"Ev{ts}", "event": event})
