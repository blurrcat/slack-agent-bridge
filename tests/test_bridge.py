import asyncio
import sys
import tempfile
import time
import unittest
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from unittest.mock import AsyncMock

from slack_sdk.socket_mode.request import SocketModeRequest

from slack_agent_bridge.bridge import Bridge
from slack_agent_bridge.config import Config
from slack_agent_bridge.harness import Harness
from slack_agent_bridge.slack_inbox import SlackInbox
from slack_agent_bridge.slack_outbox import SlackOutbox


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_prompt_ack_does_not_block_approval_in_another_session(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "wait-for-other-approval")
            await slack.card("1.000000")
            await slack.message("2.000000", "approve")
            card = await slack.card("2.000000")
            await slack.click(card, "Approve once")
            await slack.posted("2.000000", "decision: accept")
            await slack.posted("1.000000", "codex-1: first prompt acknowledged")


    async def test_owner_can_close_while_active_prompt_ack_is_outstanding(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "ack-only-after-interrupt")
            await slack.card("1.000000")
            await slack.message("2.000000", "!done", root="1.000000")
            await slack.posted("1.000000", "Bye.")

    async def test_workflow_bot_is_authorized_by_bot_id_and_cannot_reply(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "unknown bot", bot="BUNKNOWN")
            await slack.message("2.000000", "itself", bot="BSELF")
            await slack.message("3.000000", "authorized", bot="BFLOW")
            await slack.message("3.000000", "duplicate", bot="BFLOW")
            await slack.posted("3.000000", "codex-1: authorized")
            await slack.message("4.000000", "wrong follow-up", root="3.000000", user="UGUEST")
            await slack.message("5.000000", "owner follow-up", root="3.000000")
            await slack.posted("3.000000", "codex-1: owner follow-up")
            eyes = [value["timestamp"] for name, value in slack.observed if name == "reactions_add"]
            self.assertEqual(eyes, ["3.000000"])
            replies = [value["text"] for name, value in slack.observed if name == "chat_postMessage"]
            self.assertEqual(replies, ["codex-1: authorized", "codex-1: owner follow-up"])

    async def test_guest_steers_but_only_owner_closes(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "hold", user="UGUEST")
            await slack.card("1.000000")
            await slack.message("2.000000", "!done", root="1.000000", user="UGUEST")
            await slack.message("3.000000", "change course", root="1.000000", user="UGUEST")
            await slack.posted("1.000000", "steered: change course")
            first_card = await slack.card("1.000000")
            await slack.message("4.000000", "hold", root="1.000000")
            await slack.card("1.000000", after=first_card["ts"])
            await slack.message("5.000000", "!end", root="1.000000")
            await slack.message("6.000000", "must not run", root="1.000000")
            await slack.posted("1.000000", "Bye.")
            await slack.message("7.000000", "other session")
            await slack.posted("7.000000", "codex-2: other session")
            writes = [(method, value.get("name") or value.get("text")) for method, value in slack.observed]
            self.assertLess(writes.index(("reactions_add", "white_check_mark")),
                            writes.index(("chat_postMessage", "Bye.")))
            self.assertFalse(any("must not run" in (text or "") for _, text in writes))
            self.assertFalse(any("DO NOT EXPOSE" in (text or "") for _, text in writes))

    async def test_stale_interrupt_does_not_stop_a_later_turn(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "hold")
            await slack.card("1.000000")
            await slack.message("2.000000", "!esc", root="1.000000")
            await slack.message("3.000000", "still working", root="1.000000")
            await slack.posted("1.000000", "steered: still working")

    async def test_interrupt_stops_active_turn_but_leaves_session_open(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "hold")
            await slack.card("1.000000")
            await slack.message(f"{time.time() + 1:.6f}", "!esc", root="1.000000")
            await slack.posted("1.000000", "Turn interrupted.")
            await slack.message("3.000000", "again", root="1.000000")
            await slack.posted("1.000000", "codex-1: again")

    async def test_only_owner_can_approve_and_failed_close_allows_retry(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "approve", user="UGUEST")
            card = await slack.card("1.000000")
            await slack.click(card, "Approve once", user="UGUEST")
            await slack.click(card, "Approve once", channel="OTHER")
            await slack.message("2.000000", "!done", root="1.000000")
            await slack.posted("1.000000", "Codex did not confirm the session stopped. You can retry !done.")
            await slack.click(card, "Approve for session")
            await slack.posted("1.000000", "decision: acceptForSession")
            await slack.find("chat_update", lambda value: value["ts"] == card["ts"])
            await slack.click(card, "Approve once")
            await slack.message("3.000000", "!done", root="1.000000")
            await slack.posted("1.000000", "Bye.")

    async def test_approval_expiry_disables_button_and_late_click_is_harmless(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "expire-on-steer")
            card = await slack.card("1.000000")
            await slack.message("2.000000", "expire", root="1.000000")
            await slack.find("chat_update", lambda value: value["ts"] == card["ts"])
            await slack.click(card, "Approve for session")
            await slack.message("3.000000", "fresh prompt", root="1.000000")
            await slack.posted("1.000000", "codex-1: fresh prompt")
            self.assertFalse(any(v.get("text", "").startswith("decision:") for _, v in slack.observed))

    async def test_owner_can_answer_a_non_secret_question_through_modal(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "question")
            await slack.click(await slack.card("1.000000"), "Answer")
            modal = (await slack.find("views_open", lambda _: True))["view"]
            self.assertEqual(modal["blocks"][1]["element"]["options"][0]["value"], "Python")
            await slack.submit(modal, "Python", user="UGUEST")
            await slack.submit(modal, "wrong option")
            await slack.submit(modal, "Python")
            await slack.posted("1.000000", "decision: Python")

    async def test_secrets_are_not_requested_in_slack(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "secret-question")
            await slack.posted("1.000000", "decision: rejected")
            self.assertFalse(any("blocks" in value for _, value in slack.observed))

    async def test_bridge_dispatches_during_slow_slack_write_and_worker_stays_fifo(self):
        release = asyncio.Event()
        entered = asyncio.Event()

        async def stall_first_eyes(method, args):
            if method == "reactions_add" and args["timestamp"] == "1.000000":
                entered.set()
                await release.wait()
                raise TimeoutError

        async with running_bridge(before_write=stall_first_eyes) as slack:
            await slack.message("1.000000", "hello")
            await entered.wait()
            await slack.message("2.000000", "second session")
            # A second start gets to the app server before any of its Slack writes.
            while not slack.harness_trace.exists() or slack.harness_trace.read_text().count("started\n") != 2:
                await asyncio.sleep(0.005)
            self.assertEqual(slack.observed, [])
            release.set()
            await slack.posted("2.000000", "codex-2: second session")
            await slack.posted("1.000000", "codex-1: hello")
            self.assertEqual(len(slack.observed), 3)
            reaction = next(i for i, (method, v) in enumerate(slack.observed)
                            if method == "reactions_add" and v["timestamp"] == "2.000000")
            reply = next(i for i, (_, v) in enumerate(slack.observed) if v.get("text") == "codex-2: second session")
            self.assertLess(reaction, reply)

    async def test_early_turn_completion_does_not_mix_turn_responses(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "finish-before-followup")
            await slack.card("1.000000")
            await slack.message("2.000000", "next", root="1.000000")
            await slack.posted("1.000000", "finished before follow-up")
            await slack.posted("1.000000", "codex-1: next")

    async def test_failed_approval_card_post_does_not_leave_codex_waiting(self):
        async def fail_cards(method, args):
            if method == "chat_postMessage" and "blocks" in args:
                raise TimeoutError

        async with running_bridge(before_write=fail_cards) as slack:
            await slack.message("1.000000", "approve")
            await slack.posted("1.000000", "decision: rejected")

    async def test_long_approval_preview_is_trimmed_without_losing_buttons(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "approve-long")
            card = await slack.card("1.000000")
            preview = card["blocks"][0]["text"]["text"]
            self.assertIn("Command: echo test", preview)
            self.assertIn("[Approval details trimmed]", preview)
            self.assertLessEqual(len(preview), 3000)
            await slack.click(card, "Decline")
            await slack.posted("1.000000", "decision: decline")


@asynccontextmanager
async def running_bridge(*, before_write=None):
    config = Config("CINBOX", "UOWNER", "xapp-test", "xoxb-test",
                    frozenset({"UGUEST"}), frozenset({"BFLOW", "BSELF"}))
    slack = SlackHarness(before_write)
    with tempfile.TemporaryDirectory() as tmp:
        trace = Path(tmp) / "harness.log"
        command = (sys.executable, str(Path(__file__).with_name("fake_codex.py")), str(trace))
        slack.harness_trace = trace
        async with Harness(command) as harness:
            bridge = Bridge(config, SlackInbox(slack.socket), harness, SlackOutbox(slack.web),
                            Path(tmp) / "state.sqlite3", "TWORKSPACE", "UBRIDGE", "BSELF")
            task = asyncio.create_task(bridge.run())
            try:
                async with asyncio.timeout(5):
                    await slack.connected.wait()
                    yield slack
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task


class SlackHarness:
    def __init__(self, before_write):
        self.connected = asyncio.Event()
        self.socket = AsyncMock()
        self.socket.socket_mode_request_listeners = []
        self.socket.message_processor = None
        self.socket.connect.side_effect = self.connected.set
        self.web = AsyncMock()
        self.writes = asyncio.Queue()
        self.observed = []
        self.before_write = before_write
        for method in ("chat_postMessage", "chat_update", "reactions_add", "views_open"):
            getattr(self.web, method).side_effect = self._recorder(method)

    def _recorder(self, method):
        async def record(**kwargs):
            if self.before_write is not None:
                await self.before_write(method, kwargs)
            # An actual Slack timestamp, unique for each recorded write.
            result = {**kwargs, "ts": kwargs.get("ts", f"100.{len(self.observed):06}")}
            self.observed.append((method, result))
            self.writes.put_nowait((method, result))
            return result

        return record

    async def message(self, ts, text, *, root=None, user="UOWNER", bot=None):
        event = {"type": "message", "channel": "CINBOX", "ts": ts, "text": text, "user": user}
        if root is not None:
            event["thread_ts"] = root
        if bot is not None:
            event.update(bot_id=bot, subtype="bot_message")
        await self.deliver("events_api", {"team_id": "TWORKSPACE", "event_id": f"Ev{ts}", "event": event})

    async def card(self, root, *, after=None):
        return await self.find("chat_postMessage", lambda v: (
            v.get("thread_ts") == root and "blocks" in v and (after is None or v["ts"] > after)
        ))

    async def posted(self, root, text):
        return await self.find("chat_postMessage", lambda v: v.get("thread_ts") == root and v["text"] == text)

    async def click(self, card, label, *, user="UOWNER", channel="CINBOX"):
        button = next(
            item for block in card["blocks"] if block["type"] == "actions"
            for item in block["elements"] if item["text"]["text"] == label
        )
        await self.deliver("interactive", {
            "type": "block_actions", "team": {"id": "TWORKSPACE"}, "user": {"id": user},
            "container": {"channel_id": channel, "message_ts": card["ts"]},
            "actions": [button], "trigger_id": "trigger",
        })

    async def submit(self, modal, answer, *, user="UOWNER"):
        await self.deliver("interactive", {
            "type": "view_submission", "team": {"id": "TWORKSPACE"}, "user": {"id": user},
            "view": {
                "callback_id": modal["callback_id"], "private_metadata": modal["private_metadata"],
                "state": {"values": {"q0": {"answer": {"selected_option": {"value": answer}}}}},
            },
        })

    async def find(self, method, predicate):
        while True:
            for name, value in self.observed:
                if name == method and predicate(value):
                    return value
            await self.writes.get()

    async def deliver(self, kind, payload):
        request = SocketModeRequest(type=kind, envelope_id="envelope", payload=payload)
        for listener in self.socket.socket_mode_request_listeners:
            await listener(self.socket, request)
