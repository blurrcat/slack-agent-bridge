"""CLI entry point and composition of the Bridge's transport adapters."""

import argparse
import asyncio
import logging
import os
import sqlite3
import sys
from pathlib import Path
from time import time_ns

from aiohttp import ClientError, ClientSession
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode.aiohttp import SocketModeClient
from slack_sdk.web.async_client import AsyncWebClient

from .bridge import Bridge, BridgeError
from .config import Config, ConfigError, default_path, init_config, load_config
from .harness import Harness, HarnessError
from .slack_inbox import SlackInbox
from .slack_outbox import SlackOutbox


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="slack-agent-bridge")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Create the local configuration interactively")
    run = commands.add_parser("run", help="Run the bridge in the foreground")
    run.add_argument("--config", type=Path, default=default_path())
    args = parser.parse_args(argv)
    try:
        if args.command == "init":
            path = default_path()
            init_config(path)
            print(f"Wrote {path}. Invite the bot to your private inbox, then run slack-agent-bridge run.")
        else:
            path = args.config.expanduser()
            config = load_config(path)
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
            database = state_path()
            database.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            asyncio.run(serve(config, database))
        return 0
    except (ConfigError, HarnessError, BridgeError, OSError, sqlite3.Error) as error:
        print(f"slack-agent-bridge: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


async def serve(config: Config, database: Path) -> None:
    """Authenticate Slack, start Codex, and run the dispatcher with process-scoped resources."""
    async with ClientSession() as http:
        web = AsyncWebClient(token=config.slack_bot_token, session=http)
        try:
            auth = await web.auth_test()
        except (SlackApiError, ClientError, OSError):
            raise BridgeError("Slack bot authentication failed. Check the bot token and connection.") from None
        workspace_id = auth.get("team_id")
        user_id = auth.get("user_id")
        if not isinstance(workspace_id, str) or not isinstance(user_id, str):
            raise BridgeError("Slack did not return a workspace and bot user ID.")
        bot_id = auth.get("bot_id")
        try:
            binary, *args = config.codex_command
            async with Harness(binary, tuple(args), cwd=config.codex_cwd) as harness:
                socket = SocketModeClient(app_token=config.slack_app_token, web_client=web)
                inbox = SlackInbox(socket)
                outbox = SlackOutbox(web)
                await Bridge(config, inbox, harness, outbox, database, workspace_id, user_id,
                             bot_id if isinstance(bot_id, str) else None, fresh_since_ns=time_ns()).run()
        except* (HarnessError, BridgeError, OSError, ClientError, sqlite3.Error) as failures:
            raise BridgeError(_failure_message(failures)) from None


def _failure_message(error: BaseException) -> str:
    """Summarize supervised failures without leaking raw SDK payloads."""
    if isinstance(error, BaseExceptionGroup):
        return "; ".join(dict.fromkeys(_failure_message(nested) for nested in error.exceptions))
    if isinstance(error, (HarnessError, BridgeError)):
        return str(error)
    return type(error).__name__


def state_path() -> Path:
    """Keep runtime state independent of the location of the config file."""
    base = Path(os.environ.get("XDG_STATE_HOME", ""))
    if not base.is_absolute():
        base = Path.home() / ".local" / "state"
    return base / "slack-agent-bridge" / "state.sqlite3"


if __name__ == "__main__":
    raise SystemExit(main())
