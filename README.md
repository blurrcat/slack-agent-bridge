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
`connections:write`. Enable **Socket Mode** and **Event Subscriptions**.

## Install and run

Have a working, signed-in Codex CLI on this machine. Install the bridge with
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
- `codex_command`: executable and argument array, by default
  `["codex", "app-server"]`. Keep the app server on its stdio transport.
- `codex_cwd`: working directory for Codex; by default it inherits yours.

Post a new top-level message from an authorized sender. The bridge adds 👀
when the Codex session opens and posts the first reply under that message.
This slice does not handle follow-ups, commands, or session recovery yet.
The bridge stores its SQLite state in WAL mode beside the config file. Run only one
instance per inbox. Stop the bridge before copying its database to another host;
don't copy only the main database file while writes are active.

## Tests

```sh
uv run python -m unittest discover -s tests -v
```
