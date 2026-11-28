"""Exercise Codex command and notification races across a real subprocess pipe."""

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

from slack_agent_bridge.events import (
    HarnessRequestReceived, HarnessRequestResolved, HarnessTurnCompleted,
)
from slack_agent_bridge.harness import Harness


class HarnessTests(unittest.IsolatedAsyncioTestCase):
    async def test_codex_stderr_is_logged_at_error_level(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake = Path(__file__).with_name("fake_codex.py")
            args = (str(fake), str(Path(tmp) / "trace"), "stderr")
            with self.assertLogs("slack_agent_bridge.harness", level="ERROR") as logs:
                async with asyncio.timeout(1), Harness(sys.executable, args) as harness:
                    while not logs.records:
                        await asyncio.sleep(0.01)
                    self.assertTrue(await harness.start_session())
            self.assertEqual(logs.output, ["ERROR:slack_agent_bridge.harness:Codex stderr: test Codex diagnostic"])

    async def test_send_interrupt_and_unsubscribe_do_not_wait_for_replies(self):
        args = (str(Path(__file__).with_name("fake_codex.py")),)
        async with asyncio.timeout(5), Harness(sys.executable, args) as harness:
            session = await harness.start_session()
            await harness.send_message(session, "no-prompt-receipt")
            events = harness.events()
            request = await _event_of_type(events, HarnessRequestReceived)
            self.assertEqual(request.key.session_id, session)
            await harness.stop_session(session)

            # The fake keeps the turn active after unsubscribe. Interrupt is a separate call.
            await harness.interrupt(session, request.key.turn_id)
            completed = await _event_of_type(events, HarnessTurnCompleted)
            self.assertEqual(completed.status, "interrupted")

    async def test_request_resolution_is_forwarded_while_a_write_is_blocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            gate = Path(tmp) / "wire"
            args = (str(Path(__file__).with_name("fake_codex.py")), str(gate), "expiry-race")
            async with asyncio.timeout(5), Harness(sys.executable, args) as harness:
                approval_session = await harness.start_session()
                other_session = await harness.start_session()
                events = harness.events()

                await harness.send_message(approval_session, "pause-wire-for-expiry")
                request = await _event_of_type(events, HarnessRequestReceived)

                # Codex is not reading stdin. A submission bigger than the pipe has
                # to wait for its drain while the reader still forwards resolution.
                writing = asyncio.create_task(
                    harness.send_message(other_session, "large:" + "x" * (512 * 1024))
                )
                await asyncio.sleep(0)
                gate.with_suffix(".expire").touch()
                resolved = await _event_of_type(events, HarnessRequestResolved)
                self.assertEqual(resolved.session_id, request.key.session_id)
                self.assertEqual(resolved.request_id, request.key.request_id)
                self.assertFalse(writing.done())
                gate.with_suffix(".resume").touch()
                await writing
                finished = await _event_of_type(events, HarnessTurnCompleted)
                self.assertEqual(finished.session_id, other_session)
                await harness.send_message(other_session, "still alive")
                finished = await _event_of_type(events, HarnessTurnCompleted)
                self.assertEqual(finished.session_id, other_session)


async def _event_of_type(events, expected):
    async for event in events:
        if isinstance(event, expected):
            return event
    raise AssertionError(f"Missing {expected.__name__}")
