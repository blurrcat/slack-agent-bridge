import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from slack_agent_bridge.cli import main


class InitTests(unittest.TestCase):
    def test_creates_private_config_without_overwriting_it(self):
        with tempfile.TemporaryDirectory() as home, patch.dict(os.environ, {"HOME": home}):
            with (
                patch("builtins.input", side_effect=["CINBOX", "UOWNER"]),
                patch("getpass.getpass", side_effect=["xapp-example", "xoxb-example"]),
                redirect_stdout(io.StringIO()) as output,
            ):
                self.assertEqual(main(["init"]), 0)
            path = Path(home) / ".config/slack-agent-bridge/config.json"
            original = path.read_bytes()
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads(original)["inbox_channel_id"], "CINBOX")
            self.assertNotIn("xapp-example", output.getvalue())
            self.assertNotIn("xoxb-example", output.getvalue())
            with redirect_stderr(io.StringIO()):
                self.assertNotEqual(main(["init"]), 0)
            self.assertEqual(path.read_bytes(), original)
