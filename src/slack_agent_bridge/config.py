import getpass
import json
import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_CODEX_COMMAND = ("codex", "app-server")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Config:
    inbox_channel_id: str
    owner_user_id: str
    slack_app_token: str = field(repr=False)
    slack_bot_token: str = field(repr=False)
    allowed_user_ids: frozenset[str] = frozenset()
    allowed_bot_ids: frozenset[str] = frozenset()
    codex_command: tuple[str, ...] = DEFAULT_CODEX_COMMAND
    codex_cwd: Path | None = None


def default_path() -> Path:
    return Path.home() / ".config" / "slack-agent-bridge" / "config.json"


def parse_config(data: object) -> Config:
    if not isinstance(data, dict):
        raise ConfigError("Configuration must be a JSON object.")

    def required(key: str, prefixes: tuple[str, ...]) -> str:
        value = data.get(key)
        if not isinstance(value, str) or not value.startswith(prefixes) or not value.strip() == value:
            raise ConfigError(f"Invalid or missing {key}.")
        return value

    def strings(key: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
        value = data.get(key, list(default))
        if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
            raise ConfigError(f"{key} must be an array of nonempty strings.")
        return tuple(value)

    command = strings("codex_command", DEFAULT_CODEX_COMMAND)
    if not command:
        raise ConfigError("codex_command cannot be empty.")
    cwd = data.get("codex_cwd")
    if cwd is not None and (not isinstance(cwd, str) or not cwd.strip()):
        raise ConfigError("codex_cwd must be a directory path.")
    return Config(
        inbox_channel_id=required("inbox_channel_id", ("C", "G")),
        owner_user_id=required("owner_user_id", ("U", "W")),
        slack_app_token=required("slack_app_token", ("xapp-",)),
        slack_bot_token=required("slack_bot_token", ("xoxb-",)),
        allowed_user_ids=frozenset(strings("allowed_user_ids")),
        allowed_bot_ids=frozenset(strings("allowed_bot_ids")),
        codex_command=command,
        codex_cwd=Path(cwd).expanduser() if cwd is not None else None,
    )


def load_config(path: Path) -> Config:
    try:
        with path.open() as file:
            return parse_config(json.load(file))
    except FileNotFoundError:
        raise ConfigError(f"No configuration at {path}. Run slack-agent-bridge init first.") from None
    except (json.JSONDecodeError, UnicodeError):
        raise ConfigError(f"Invalid JSON configuration at {path}.") from None


def init_config(path: Path) -> None:
    if path.exists() or path.is_symlink():
        raise ConfigError(f"Configuration already exists at {path}.")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            data = {
                "inbox_channel_id": input("Private inbox Slack channel ID: ").strip(),
                "owner_user_id": input("Your Slack user ID: ").strip(),
                "slack_app_token": getpass.getpass("Slack app token (xapp-…): ").strip(),
                "slack_bot_token": getpass.getpass("Slack bot token (xoxb-…): ").strip(),
            }
    except (EOFError, getpass.GetPassWarning):
        raise ConfigError("Init needs an interactive terminal to read tokens without echo.") from None
    parse_config(data)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise ConfigError(f"Configuration already exists at {path}.") from None
    with os.fdopen(fd, "w") as file:
        json.dump(data, file, indent=2)
        file.write("\n")
