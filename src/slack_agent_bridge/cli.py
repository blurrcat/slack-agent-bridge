import argparse
import asyncio
import logging
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
            asyncio.run(serve(config, path.with_name("state.sqlite3")))
        return 0
    except (ConfigError, CodexError, BridgeError, OSError) as error:
        print(f"slack-agent-bridge: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
