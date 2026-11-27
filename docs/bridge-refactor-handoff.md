# Slack Agent Bridge refactor handoff

## Goal and scope

Keep the bridge simpler and less coupled. Borrow Erlang's mailbox model: one owner of policy state, asynchronous messages, casts by default, and calls only when an immediate result matters. Use plain asyncio, not an actor framework.

Write the new modules from scratch. Do not rename, move, or wrap the existing implementations. Use this handoff and the agreed behavior as the spec, not the old code structure. The draft against GitHub main also preserves behavior that landed after the handoff was written.

## Module layout

Create these files under `src/slack_agent_bridge/`:

- **`slack_inbox.py` — `SlackInbox`:** Own Socket Mode, acknowledge envelopes, and expose `events() -> AsyncIterator[SlackEvent]`. Validate and translate supported Slack payloads to facts: message posted, reply posted, reaction added or removed, and interaction received. Preserve the IDs and timestamps the Bridge needs. Don't filter by channel, sender, or authorization.
- **`harness.py` — `Harness`:** Own the Codex subprocess, protocol response matching, and multiplexing of Agent Sessions over one connection. Expose session start and stop, prompt submission, interrupt, request responses, and `events() -> AsyncIterator[HarnessEvent]`. Forward typed Codex facts with session, turn, and request IDs. Don't expose wire dictionaries or mutable turn objects, decide who may approve, or decide what appears in Slack.
- **`slack_outbox.py` — `SlackOutbox`:** Own outbound Slack writes. Methods such as `outbox.started(thread)` are plain `def`s that enqueue into a private FIFO and return immediately. One worker, `outbox.run()`, dequeues writes and awaits the unmodified SDK one at a time. No SDK hooks, per-write futures, or task per request. Emit a typed result on the Bridge queue only when the outcome affects policy, such as the timestamp or failure of an approval-card post.
- **`bridge.py` — `Bridge`:** Own the main queue, dispatcher, authorization, Session Thread ↔ Agent Session mappings, pending controls, and close state. Producers don't mutate Bridge state.
- **`events.py`:** Define the `SlackEvent` and `HarnessEvent` base classes and their concrete frozen dataclass subclasses. Use immutable fields. Outbox results are `SlackEvent` subclasses. Name wire observations as facts: a reply containing `!done` is still a Slack reply, not an authorized close request.

## Dispatch and command semantics

The Bridge owns an unbounded, in-memory queue for `SlackEvent` and `HarnessEvent`, plus its private recovery deadline event. One pump per inbound stream reads its async iterator and puts events into the queue. The dispatcher runs:

```python
async for event in self.events():
    await self._handle(event)
```

`_handle` uses `match` on concrete event types and calls one private helper per event. Preserve FIFO within each source; don't assume ordering across sources. A handler must never wait for another event on the main queue.

- **Start is a call.** The dispatcher can directly await `harness.start_session()` with a short deadline. The Harness resolves the matching response internally, independently of the Bridge queue. Return a session ID or raise; don't emit an acknowledgment event just to return the result. Add 👀 only after success.
- **Prompts are casts.** Record a command ID before submission. The Harness preserves submission order within each Agent Session and handles protocol acknowledgments internally. Don't emit a success acknowledgment for every prompt. Emit a correlated failure for rejected or unconfirmed submissions, and don't automatically retry. A turn may start or finish before its submission acknowledgment.
- **Slack writes are casts.** The Bridge never waits for Slack HTTP. For an approval card, the outbox posts asynchronously and returns its Slack timestamp or failure as an event. The Bridge uses that result to validate clicks or reject a request it couldn't present.

Supervise the dispatcher, both pumps, and the outbox worker together. If either input stream ends unexpectedly or a worker dies, exit nonzero so systemd can restart the bridge. An individual outbound failure is not a dead worker. Don't replay the in-memory queue after restart.

## Behavior to preserve

- Keep the current authorization rules, including Owner-only approval and close. Preserve prompt, steer, interrupt, and approval flows. **Remove the hourglass.** Keep GitHub main's suppression of tool-status replies and its existing inbox session recovery; don't extend recovery or add Joined Threads. SQLite keeps its role for mappings and deduplication, not event replay.
- For `!done` or `!end` during an active turn, mark the session closing, cast the interrupt, and return. Don't accept new prompts or approval clicks while closing. A terminal event finishes the close and causes ✅ and “Bye” to be sent in order. If interruption fails and the turn is still active, leave the session open for retry and report the failure.
- Codex owns approval expiry. Retire and disable the Slack control when Codex reports the request resolved or the turn ends. A click that races with expiry is a no-op: don't retry it or claim that the approval was applied. Resolution alone does not distinguish an answered request from one Codex cleared.

## Integration checks

Exercise start, prompt, steer, approval, interrupt, and close through the new interfaces. In particular, delay a prompt acknowledgment in one Session Thread and verify that the Bridge can still process an Owner approval in another. Also cover early turn completion, approval expiry racing a click, and ordered Slack writes while the dispatcher continues processing.
