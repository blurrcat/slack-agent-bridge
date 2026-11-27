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


class RunTests(unittest.TestCase):
    def test_state_path_is_independent_of_config_path(self):
        with tempfile.TemporaryDirectory() as home:
            root = Path(home)
            config = root / "custom.json"
            config.write_text(json.dumps({
                "inbox_channel_id": "CINBOX",
                "owner_user_id": "UOWNER",
                "slack_app_token": "xapp-example",
                "slack_bot_token": "xoxb-example",
            }))
            for xdg, base in (
                ("", root / ".local/state"),
                ("relative", root / ".local/state"),
                (str(root / "custom-state"), root / "custom-state"),
            ):
                with (
                    self.subTest(xdg=xdg),
                    patch.dict(os.environ, {"HOME": home, "XDG_STATE_HOME": xdg}),
                    patch("slack_agent_bridge.cli.serve") as serve,
                ):
                    self.assertEqual(main(["run", "--config", str(config)]), 0)
                    assert serve.await_args is not None
                    expected = base / "slack-agent-bridge/state.sqlite3"
                    self.assertEqual(serve.await_args.args[1], expected)
                    self.assertTrue(expected.parent.is_dir())
