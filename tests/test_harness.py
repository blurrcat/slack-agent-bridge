import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

from slack_agent_bridge.events import HarnessRequestReceived, HarnessRequestResolved, HarnessTurnCompleted
from slack_agent_bridge.harness import Harness


class HarnessTests(unittest.IsolatedAsyncioTestCase):
    async def test_expired_approval_is_not_written_after_a_blocked_submission(self):
        with tempfile.TemporaryDirectory() as tmp:
            gate = Path(tmp) / "wire"
            command = (sys.executable, str(Path(__file__).with_name("fake_codex.py")), str(gate), "expiry-race")
            async with asyncio.timeout(5), Harness(command) as harness:
                approval_session = await harness.start_session()
                other_session = await harness.start_session()
                events = harness.events()

                harness.send_message(approval_session, "request", "pause-wire-for-expiry")
                request = await _event_of_type(events, HarnessRequestReceived)

                # Codex is not reading stdin. A submission bigger than the pipe has
                # to wait for its drain before the queued approval can be written.
                harness.send_message(other_session, "large", "large:" + "x" * (512 * 1024))
                await asyncio.sleep(0)
                harness.answer_request(request.key, "accept")
                gate.with_suffix(".expire").touch()
                resolved = await _event_of_type(events, HarnessRequestResolved)
                self.assertEqual(resolved.key, request.key)
                gate.with_suffix(".resume").touch()

                # The fake server rejects stale responses by exiting. A subsequent
                # prompt can only finish if the expired approval never went on the wire.
                finished = await _event_of_type(events, HarnessTurnCompleted)
                self.assertEqual(finished.session_id, other_session)
                harness.send_message(other_session, "confirm", "still alive")
                finished = await _event_of_type(events, HarnessTurnCompleted)
                self.assertEqual(finished.session_id, other_session)


async def _event_of_type(events, expected):
    async for event in events:
        if isinstance(event, expected):
            return event
    raise AssertionError(f"Missing {expected.__name__}")
