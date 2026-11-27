# Join existing Slack threads for collaboration

Use `!join <thread_link> [prompt]` from the private Agent Inbox to start a fresh Agent Session in an existing Slack thread. The owner and configured user and bot IDs can request a join. A joined session does not inherit private inbox history. It primes itself from the target thread's existing context, even without a prompt; it does not infer a task. Participants direct the agent with explicit mentions rather than ordinary discussion.

Anyone who can prompt the session can use `!esc` to interrupt its active turn. Only the owner can answer interactive requests or end the session with `!done` or `!end`. Mark the initial request with an eyes reaction when the session starts and a green checkmark when it ends; for a join these reactions belong on the target thread's root message, not the inbox command.

Joined threads are a semi-trusted, same-team environment: anyone in the thread may interact with the agent. Joined sessions use the operator's configured harness; a separate collaboration profile is not required. Issuing a join grants participants the ability to direct that harness, not to approve its interactive requests.

A reply permalink and its root permalink identify the same joined thread. A repeated join while the session is active points to that session; after close, an authorized join starts a fresh session. If the bridge loses its SQLite mapping, a mention outside the Agent Inbox does not restore the join: an authorized sender must explicitly join the thread again.
