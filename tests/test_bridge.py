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

    async def test_terminal_turn_closes_even_if_prompt_ack_never_comes(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "ack-never-after-terminal")
            await slack.card("1.000000")
            await slack.message("2.000000", "!done", root="1.000000")
            await slack.posted("1.000000", "Bye.")

    async def test_rejected_close_preserves_previously_accepted_queued_prompts(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "hold-ack-reject-interrupt")
            await slack.card("1.000000")
            await slack.message("2.000000", "queued prompt", root="1.000000")
            await slack.message("3.000000", "!done", root="1.000000")
            await slack.posted("1.000000", "Codex did not confirm the session stopped. You can retry !done.")
            await slack.posted("1.000000", "steered: queued prompt")

    async def test_workflow_bot_is_authorized_by_bot_id_and_cannot_reply(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "unknown bot", bot="BUNKNOWN")
            await slack.message("2.000000", "itself", bot="BSELF")
            await slack.message("2.100000", "!join https://example.test/archives/thread")
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
            await slack.wait_for_harness_starts(2)
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

    async def test_existing_session_resumes_without_replaying_prompts(self):
        with tempfile.TemporaryDirectory() as tmp:
            database = Path(tmp) / "state.sqlite3"
            async with running_bridge(database=database) as slack:
                await slack.message("1.000000", "original", user="UGUEST")
                await slack.posted("1.000000", "codex-1: original")
                await slack.message("2.000000", "already accepted", root="1.000000", user="UGUEST")
                await slack.posted("1.000000", "codex-1: already accepted")
            async with running_bridge(database=database, mode="resume") as slack:
                await slack.message("1.000000", "original", user="UGUEST")
                await slack.message("2.000000", "already accepted", root="1.000000", user="UGUEST")
                await slack.message("3.000000", "not authorized", root="1.000000", user="UOTHER")
                await slack.message("4.000000", "again", root="1.000000", user="UGUEST")
                await slack.posted("1.000000", "codex-1: again")
                self.assertEqual([v["text"] for name, v in slack.observed if name == "chat_postMessage"],
                                 ["codex-1: again"])

    async def test_missing_mapping_requires_a_verified_slack_origin(self):
        origin = {"ts": "1.000000", "user": "UGUEST", "text": "old prompt"}
        async with running_bridge(mode="missing", origin=origin) as slack:
            await slack.message("2.000000", "not my session", root="1.000000", user="UOTHER")
            await slack.message("3.000000", "new prompt", root="1.000000", user="UGUEST")
            await slack.posted("1.000000", "codex-2: new prompt")
            self.assertEqual([v["text"] for name, v in slack.observed if name == "chat_postMessage"],
                             ["codex-2: new prompt"])

    async def test_root_arriving_during_origin_lookup_does_not_reorder_replies(self):
        lookup_started = asyncio.Event()
        lookup_released = asyncio.Event()

        async def delayed_origin(*, channel, latest, **_):
            lookup_started.set()
            await lookup_released.wait()
            return {"messages": [{"ts": latest, "user": "UOWNER", "text": "origin"}]}

        async with running_bridge() as slack:
            slack.web.conversations_history.side_effect = delayed_origin
            await slack.message("2.000000", "first reply", root="1.000000")
            await lookup_started.wait()
            await slack.message("1.000000", "origin")
            await slack.message("3.000000", "second reply", root="1.000000")
            await slack.message("4.000000", "barrier")
            await slack.wait_for_harness_starts(2)
            lookup_released.set()
            await slack.posted("1.000000", "codex-1: second reply")
            await slack.posted("1.000000", "codex-1: first reply")
            replies = [value["text"] for name, value in slack.observed
                       if name == "chat_postMessage" and value.get("thread_ts") == "1.000000"]
            self.assertEqual(replies, ["codex-1: origin", "codex-1: first reply", "codex-1: second reply"])

    async def test_unknown_resume_failure_does_not_start_a_replacement(self):
        with tempfile.TemporaryDirectory() as tmp:
            database = Path(tmp) / "state.sqlite3"
            async with running_bridge(database=database) as slack:
                await slack.message("1.000000", "origin")
                await slack.posted("1.000000", "codex-1: origin")
            async with running_bridge(database=database, mode="resume-wrong-id") as slack:
                await slack.message("2.000000", "must not run", root="1.000000")
                await slack.posted("1.000000", "Codex did not confirm the prompt. The bridge did not retry it.")

    async def test_missing_mapped_session_does_not_run_prompt_when_priming_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            database = Path(tmp) / "state.sqlite3"
            async with running_bridge(database=database) as slack:
                await slack.message("1.000000", "original")
                await slack.posted("1.000000", "codex-1: original")
            async with running_bridge(database=database, mode="missing-history-failure") as slack:
                await slack.message("2.000000", "do not run", root="1.000000")
                await slack.posted("1.000000", "Codex could not recover the Slack thread history. Your prompt was not run.")
                self.assertEqual([v["text"] for name, v in slack.observed if name == "chat_postMessage"],
                                 ["Codex could not recover the Slack thread history. Your prompt was not run."])

    async def test_stalled_recovery_reports_failure_without_running_prompt(self):
        origin = {"ts": "1.000000", "user": "UOWNER", "text": "old prompt"}
        async with running_bridge(mode="missing-history-stalled", origin=origin, recovery_timeout=0.1) as slack:
            await slack.message("2.000000", "must not run", root="1.000000")
            await slack.posted("1.000000", "Codex could not recover the Slack thread history. Your prompt was not run.")
            self.assertFalse(any("must not run" in v.get("text", "") for _, v in slack.observed))

    async def test_agent_text_cannot_trigger_slack_mentions(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "say <@UOTHER> and <!channel>")
            await slack.posted("1.000000", "codex-1: say &lt;@UOTHER&gt; and &lt;!channel&gt;")

    async def test_modal_failure_allows_owner_to_retry(self):
        attempts = 0

        async def fail_first_modal(method, _args):
            nonlocal attempts
            if method == "views_open":
                attempts += 1
                if attempts == 1:
                    raise TimeoutError

        async with running_bridge(before_write=fail_first_modal) as slack:
            await slack.message("1.000000", "question")
            card = await slack.card("1.000000")
            await slack.click(card, "Answer")
            await slack.posted("1.000000", "Could not open the input form. Click Answer again to retry.")
            await slack.click(card, "Answer")
            modal = (await slack.find("views_open", lambda _: True))["view"]
            await slack.submit(modal, "Python")
            await slack.posted("1.000000", "decision: Python")

    async def test_long_approval_preview_is_trimmed_without_losing_buttons(self):
        async with running_bridge() as slack:
            await slack.message("1.000000", "approve-long")
            card = await slack.card("1.000000")
            preview = card["blocks"][0]["text"]["text"]
            self.assertIn("Command: echo test", preview)
            self.assertIn("[Approval details trimmed]", preview)
            self.assertLessEqual(len(preview), 3000)
            self.assertNotIn("Long justification", card["text"])
            await slack.click(card, "Decline")
            await slack.posted("1.000000", "decision: decline")


@asynccontextmanager
async def running_bridge(*, before_write=None, database=None, mode="", origin=None, recovery_timeout=60):
    config = Config("CINBOX", "UOWNER", "xapp-test", "xoxb-test",
                    frozenset({"UGUEST", "UOTHER"}), frozenset({"BFLOW", "BSELF"}))
    slack = SlackHarness(before_write)
    slack.web.conversations_history.return_value = {"messages": [origin] if origin else []}
    with tempfile.TemporaryDirectory() as tmp:
        trace = Path(tmp) / "harness.log"
        command = (sys.executable, str(Path(__file__).with_name("fake_codex.py")), str(trace), mode)
        slack.harness_trace = trace
        async with Harness(command) as harness:
            bridge = Bridge(config, SlackInbox(slack.socket), harness, SlackOutbox(slack.web),
                            database or Path(tmp) / "state.sqlite3", "TWORKSPACE", "UBRIDGE", "BSELF",
                            recovery_timeout=recovery_timeout)
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

    async def wait_for_harness_starts(self, count):
        while not self.harness_trace.exists() or self.harness_trace.read_text().count("started\n") < count:
            await asyncio.sleep(0.005)

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
