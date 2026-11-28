# Slack Agent Bridge

Run your locally configured Codex harness from a private Slack inbox.

## Set up Slack

Create or reuse a Slack app. [slack-manifest.yaml](slack-manifest.yaml) shows
the minimum bot scopes and event subscription for the inbox. If you reuse an
app, add these settings without removing your existing scopes. Install or
reauthorize the app in your workspace, and **invite its bot to the private
inbox channel**. Workspace installation alone does not grant access to a
private channel.

Get a bot token (`xoxb-…`) from **OAuth & Permissions**. In **Basic
Information → App-Level Tokens**, generate an app token (`xapp-…`) with
`connections:write`. Enable **Socket Mode**, **Event Subscriptions**, and **Interactivity**.

## Install and run

Have a working, signed-in Codex CLI (0.171.0 or later) on this machine. Install the bridge with
[uv](https://docs.astral.sh/uv/):

```sh
uv tool install 'git+https://github.com/blurrcat/slack-agent-bridge.git@v0.1.0'
slack-agent-bridge init
slack-agent-bridge run
```

For a local checkout before a tag is published, use `uv tool install .`
instead. `init` asks for your channel ID, Slack user ID, and tokens. It
writes `~/.config/slack-agent-bridge/config.json` with owner-only permissions.
Treat that JSON file as a secret. `run` reads it without prompting; use
`run --config /path/to/config.json` to use another file. Press Ctrl-C to
stop the foreground process.

Optional JSON fields:

- `allowed_user_ids`: array of additional Slack user IDs.
- `allowed_bot_ids`: array of Slack **bot** IDs, not app or bot-user IDs.
- `codex_command`: executable followed by optional launcher arguments; defaults to
  `["codex"]`. The bridge adds `app-server --stdio`. Existing entries that
  include `app-server` and its stdio transport flags are still accepted.
- `codex_cwd`: working directory for Codex; by default it inherits yours.

Post a new top-level message from an authorized sender. The bridge adds 👀
when the Codex session opens and posts the first reply under that message.
Reply in the thread to start a new turn, or steer the current turn if Codex
is still working. Only the owner or the human who submitted the job can
prompt or use `!esc`.

Only the owner can use approval or input controls and close a session with
`!done` or `!end`. Approval cards offer **Approve once**, **Approve for session**,
or **Decline**. Session approvals are handled by Codex, not stored by the bridge.
Closing requests an interrupt for the known active turn, adds ✅ to the initial request, and posts “Bye.”
Slack is not used for secret inputs. Joined threads are not yet supported.

### Continue an inbox session after restart

Send a new reply in the same Slack thread to resume its mapped Codex session.
The bridge does not scan for missed jobs or retry a prompt whose outcome is
unknown. If the bridge reports that Codex did not confirm a reply, check for
side effects before sending a new prompt.

If Codex confirms the session is missing, the bridge opens a replacement in
the same thread and asks it to recover context before running the new prompt.
If the SQLite mapping is gone, the bridge first checks that the root message
was posted by an authorized inbox initiator. The replacement Codex agent
uses its configured Slack MCP tools to read the private thread. The bridge
submits the new prompt only after the agent confirms it read the history.
If it cannot confirm recovery, the bridge reports that in Slack and does
not run the new prompt.
The SQLite database uses WAL and lives at
`$XDG_STATE_HOME/slack-agent-bridge/state.sqlite3` (default:
`~/.local/state/slack-agent-bridge/state.sqlite3`). The `--config` flag does
not change this path. Run only one instance per inbox. Stop the bridge
before copying its database to another host.

## Deploy separately from your dev checkout

Keep the bridge code you run under `~/deployments/slack-agent-bridge` so
changes in your development checkout don't affect the running bridge.
Install from the GitHub `main` branch into a dedicated virtual environment:

```sh
mkdir -p -m 700 ~/deployments/slack-agent-bridge/work
git clone --single-branch --branch main https://github.com/blurrcat/slack-agent-bridge.git \
  ~/deployments/slack-agent-bridge/checkout
uv --directory ~/deployments/slack-agent-bridge/checkout sync --locked --no-dev
```

If you haven't configured the bridge already, run the deployed CLI's
`init` command once. The credentials stay in
`~/.config/slack-agent-bridge/config.json`, outside both Git checkouts.

```sh
~/deployments/slack-agent-bridge/checkout/.venv/bin/slack-agent-bridge init
```

Start the deployed CLI in its own tmux session. Running from `work/` keeps
Codex's default working directory separate from the deployed code. Set
`codex_cwd` in the config to work in a specific project instead.

```sh
tmux new-session -d -s slack-agent-bridge \
  -c ~/deployments/slack-agent-bridge/work \
  ~/deployments/slack-agent-bridge/checkout/.venv/bin/slack-agent-bridge run
```

Use `tmux attach -t slack-agent-bridge` to view logs, or
`tmux switch-client -t slack-agent-bridge` if you're already in tmux.
Check for `Connected to Slack Socket Mode.` Detaching leaves the bridge running; pressing
Ctrl-C stops it. This setup does not start automatically after a reboot.

To upgrade, **stop the running bridge first**, then pull and sync the
deployment checkout and run the same tmux start command. The config and
SQLite state stay in place. Existing sessions resume on the next authorized
reply, not at startup.

```sh
git -C ~/deployments/slack-agent-bridge/checkout pull --ff-only origin main
uv --directory ~/deployments/slack-agent-bridge/checkout sync --locked --no-dev
```

## Tests

```sh
uv run python -m unittest discover -s tests -v
```
