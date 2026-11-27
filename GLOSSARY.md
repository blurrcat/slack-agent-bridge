# Slack Agent Bridge

The bridge connects Slack threads to a personal coding agent running on a laptop.

## Language

**Agent Inbox**:
The dedicated Slack channel where authorized senders submit jobs to the configured harness.

**Origin Message**:
The message in the **Agent Inbox** that requests an **Agent Session**, either as a top-level prompt or as a request to join an existing Slack thread.

**Authorized Initiator**:
The **Owner**, or a Slack user or bot explicitly configured to submit jobs in the **Agent Inbox**.

**Session Thread**:
The Slack thread where an **Agent Session** receives prompts and posts replies. For an inbox job, it belongs to the **Origin Message**, and the **Owner** and the initiating human can prompt it. For a **Joined Thread**, participants prompt it by mentioning the bot. The thread stays the same if the **Agent Session** has to be replaced.

**Joined Thread**:
An existing Slack thread selected for collaboration with a new **Agent Session**. Mentions of the agent are prompts; ordinary conversation is not.

**Agent Session**:
The coding agent conversation behind a **Session Thread**. Codex is the first supported agent.

**Owner**:
The configured Slack user who can start or steer any **Agent Session**. Only the **Owner** can answer interactive requests or end a session.

**Steer**:
A message delivered to the **Agent Session** while a turn is active, rather than held until the turn finishes.
