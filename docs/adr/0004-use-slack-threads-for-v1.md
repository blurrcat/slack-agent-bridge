# Use Slack threads for v1

Use one dedicated Slack channel as the Agent Inbox. A top-level message from an authorized initiator opens an Agent Session, and its Slack thread is the durable conversation. Code Channels are deferred. Collaboration uses existing Slack threads rather than a separate channel lifecycle; see ADR-0005.

The bridge can move to another host with its local config and SQLite state. The database preserves Slack-to-Codex mappings, not Codex conversation state. A missing mapping or unavailable Codex session is not a reason to pin a Slack thread to a particular host.

An authorized reply to an old thread with no usable Codex session starts a fresh one and asks it to recover context via Slack MCP; it does not replay the original job. The bridge acknowledges accepted jobs in Slack but does not scan for or run missed jobs after downtime in v1.

A job from an authorized workflow bot starts immediately under the configured harness. That bot cannot steer the session or approve requests. The bridge posts progress and results in the session thread, not back to a conversation referenced by the job.
