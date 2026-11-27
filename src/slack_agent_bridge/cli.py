import argparse
import asyncio
import logging
import os
import sqlite3
import sys
from pathlib import Path

from .codex import CodexError
from .config import ConfigError, default_path, init_config, load_config
from .slack import BridgeError, serve


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
    except (ConfigError, CodexError, BridgeError, OSError, sqlite3.Error) as error:
        print(f"slack-agent-bridge: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


def state_path() -> Path:
    base = Path(os.environ.get("XDG_STATE_HOME", ""))
    if not base.is_absolute():
        base = Path.home() / ".local" / "state"
    return base / "slack-agent-bridge" / "state.sqlite3"


if __name__ == "__main__":
    raise SystemExit(main())
