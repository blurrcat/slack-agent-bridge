from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from time import time_ns
from typing import Protocol

from .codex import Codex, CodexError, CodexSessionMissing, Turn
from .config import Config
from .controls import Control, Interaction, control_for_request, result_for_interaction


log = logging.getLogger(__name__)


class Inbox:
    async def handle(self, message: SlackMessage | None) -> None:
        if message is None:
            return
        thread = message.thread
        if thread.workspace_id != self.workspace_id or thread.channel_id != self.config.inbox_channel_id:
            return
        if self._from_bridge(message):
            return

        live = self.sessions.get(thread)
        command = message.text.strip()
        control = command in {"!esc", "!done", "!end"}
        if message.is_root:
            if live is not None or (session := self._origin_session(message)) is None:
                return
            if not self.store.claim(session, message.event_id):
                log.info("Already handled inbox message %s/%s", thread.channel_id, thread.root_ts)
                return
            live = self.sessions[thread] = LiveSession(session)
        else:
            if live is None:
                live = await self._session_for_reply(message, allow_replacement=not control)
            if live is None or not self._can_prompt(message, live.session):
                return

        try:
            async with live.lock:
                if live.closed or message.event_id in live.seen:
                    return
                live.seen.add(message.event_id)
                if command in {"!done", "!end"}:
                    if message.user_id != self.config.owner_user_id:
                        return
                    stopping = live.running_turn
                    if stopping is not None:
                        await self._interrupt(stopping)
                    self.store.closed(thread)
                    live.closed = True
                    log.info("Closed inbox session %s/%s", thread.channel_id, thread.root_ts)
                    turn = None
                    closing = True
                elif command == "!esc":
                    running = live.running_turn
                    if running is not None and Decimal(message.message_ts) >= live.turn_started_at:
                        await self._interrupt(running)
                    return
                else:
                    closing = False
                    if not message.is_root and not self.store.claim_prompt(message):
                        return
                    harness = await self._load_harness(live, message.is_root)
                    if harness is None:
                        harness = await self._recover_harness(live, message)
                        if harness is None:
                            return
                    submitted_at = Decimal(time_ns()) / 1_000_000_000
                    turn = await self.codex.submit(harness.thread_id, message.text)
                    if turn is live.turn:
                        log.info("Steered inbox turn %s for %s/%s", turn.id, thread.channel_id, thread.root_ts)
                        return
                    live.turn = turn
                    live.turn_started_at = submitted_at
            if closing:
                if stopping is not None:
                    await stopping.stopped.wait()
                await self.slack.closed(thread)
                await self.slack.reply(thread, "Bye.")
                self.sessions.pop(thread, None)
            elif turn is not None:
                await self._forward_turn(live, turn, message.message_ts)
        except CodexError as error:
            log.warning("Codex could not process inbox message %s: %s", thread.root_ts, error)
            if not live.closed:
                if control:
                    await self.slack.reply(thread, str(error))
                else:
                    await self.slack.reply(thread, "Codex did not confirm a reply. The bridge did not retry the job.")

    async def interact(self, interaction: Interaction | None) -> None:
        if interaction is None or interaction.workspace_id != self.workspace_id:
            return
        if interaction.user_id != self.config.owner_user_id:
            return
        pending = self.controls.get(interaction.token)
        if pending is None or pending.turn.stopped.is_set() or pending.turn.interrupting:
            return
        if pending.control.request_id in pending.turn.resolved_requests:
            return
        if interaction.action != "submit" and (
            pending.message_ts is None
            or interaction.channel_id != pending.thread.channel_id
            or interaction.message_ts != pending.message_ts
        ):
            return
        if interaction.action == "answer" and pending.control.kind == "input" and interaction.trigger_id:
            await self.slack.open_questions(pending.control, interaction.trigger_id)
            return
        if (result := result_for_interaction(pending.control, interaction)) is None:
            return
        self.controls.pop(interaction.token, None)
        await self.codex.answer_request(pending.control.request_id, result)
        log.info("Forwarded owner control %s for turn %s", interaction.action, pending.turn.id)

    def __init__(
        self, config: Config, workspace_id: str, bridge_user_id: str, codex: Codex,
        store: SessionStore, slack: SlackReplies, bridge_bot_id: str | None = None,
    ):
        self.config = config
        self.workspace_id = workspace_id
        self.bridge_user_id = bridge_user_id
        self.bridge_bot_id = bridge_bot_id
        self.codex = codex
        self.store = store
        self.slack = slack
        self.sessions: dict[SlackThread, LiveSession] = {}
        self.controls: dict[str, PendingControl] = {}

    async def _session_for_reply(self, message: SlackMessage, *, allow_replacement: bool) -> LiveSession | None:
        thread = message.thread
        if saved := self.store.find(thread):
            session, closed = saved
            if closed or not self._can_prompt(message, session):
                return None
            live = self.sessions[thread] = LiveSession(session)
            return live
        if not allow_replacement or self._initiator(message) is None or message.bot_id is not None:
            return None
        origin = await self.slack.origin_message(thread)
        if origin is None or (verified := self._origin_session(origin)) is None:
            return None
        if not self._can_prompt(message, verified):
            return None
        # Another reply (or the root event) may have arrived during the Slack lookup.
        if current := self.sessions.get(thread):
            return current
        if not self.store.claim(verified, message.event_id):
            return None
        log.info("Verified unmapped inbox origin %s/%s", thread.channel_id, thread.root_ts)
        live = self.sessions[thread] = LiveSession(verified)
        return live

    async def _load_harness(self, live: LiveSession, is_origin: bool) -> HarnessSession | None:
        harness = live.session.harness_session
        if live.loaded:
            return harness
        thread = live.session.slack_thread
        if is_origin:
            harness = self._save_harness(live, await self.codex.start_thread())
            await self.slack.started(thread)
            log.info("Opened inbox session %s/%s", thread.channel_id, thread.root_ts)
        elif harness is not None:
            try:
                await self.codex.resume_thread(harness.thread_id)
            except CodexSessionMissing:
                log.info("Mapped Codex session is missing for %s/%s", thread.channel_id, thread.root_ts)
                return None
            live.loaded = True
            log.info("Resumed inbox session %s/%s", thread.channel_id, thread.root_ts)
        return harness

    async def _recover_harness(self, live: LiveSession, message: SlackMessage) -> HarnessSession | None:
        thread = message.thread
        try:
            async with asyncio.timeout(30):
                thread_id = await self.codex.start_thread()
                await self.codex.recover_slack_context(thread_id, thread.channel_id, thread.root_ts, message.message_ts)
        except (CodexError, TimeoutError) as error:
            log.warning("Slack context recovery failed for %s/%s: %s", thread.channel_id, thread.root_ts, error)
            await self.slack.reply(thread, "Codex could not recover the Slack thread history. Your prompt was not run.")
            return None
        log.info("Recovered inbox session %s/%s", thread.channel_id, thread.root_ts)
        return self._save_harness(live, thread_id)

    def _save_harness(self, live: LiveSession, thread_id: str) -> HarnessSession:
        harness = HarnessSession(thread_id)
        session = replace(live.session, harness_session=harness)
        self.store.opened(session)
        live.session = session
        live.loaded = True
        return harness

    async def _forward_turn(self, live: LiveSession, turn: Turn, prompt_ts: str) -> None:
        thread = live.session.slack_thread
        answer = ""
        await self.slack.working(thread, prompt_ts, True)
        try:
            while (event := await turn.events.get()) is not None:
                method, params = event
                if method == "item/completed" and params["turnId"] == turn.id and not live.closed:
                    item = params["item"]
                    if item["type"] == "agentMessage":
                        answer = item["text"]
                elif method == "request" and params["turnId"] == turn.id:
                    await self._present_request(thread, turn, params)
                elif method == "serverRequest/resolved":
                    self._forget_controls(turn, params["requestId"])
                elif method == "turn/completed" and params["turn"]["id"] == turn.id:
                    if live.closed:
                        return
                    status = params["turn"]["status"]
                    if status == "interrupted":
                        await self.slack.reply(thread, "Turn interrupted.")
                    elif status == "completed" and answer.strip():
                        await self.slack.reply(thread, answer)
                    else:
                        raise CodexError("Codex did not complete a text reply.")
                    return
            raise CodexError("Codex disconnected before finishing the turn.")
        finally:
            self._forget_controls(turn)
            await self.slack.working(thread, prompt_ts, False)

    async def _present_request(self, thread: SlackThread, turn: Turn, request: dict) -> None:
        if turn.interrupting or turn.stopped.is_set() or request["request_id"] in turn.resolved_requests:
            return
        control = control_for_request(request)
        if control is not None:
            pending = PendingControl(thread, turn, control)
            self.controls[control.token] = pending
            pending.message_ts = await self.slack.show_control(thread, control)
            if pending.message_ts is not None:
                return
            self.controls.pop(control.token, None)
        if not turn.stopped.is_set() and request["request_id"] not in turn.resolved_requests:
            log.info("Rejecting unavailable Slack control for turn %s", turn.id)
            await self.codex.reject_request(request["request_id"])

    def _forget_controls(self, turn: Turn, request_id: int | str | None = None) -> None:
        for token, pending in list(self.controls.items()):
            if pending.turn is turn and (request_id is None or pending.control.request_id == request_id):
                self.controls.pop(token)

    async def _interrupt(self, turn: Turn) -> None:
        turn.interrupting = True
        try:
            await self.codex.interrupt(turn)
        except CodexError as error:
            if turn.stopped.is_set() or (error.no_active_turn and await confirm_stopped(turn)):
                return
            if error.rejected:
                turn.interrupting = False
                raise
            raise CodexError("Codex did not confirm the interrupt. You can retry !esc.") from None
        log.info("Interrupted inbox turn %s", turn.id)

    def _from_bridge(self, message: SlackMessage) -> bool:
        return message.user_id == self.bridge_user_id or (
            message.bot_id is not None and message.bot_id == self.bridge_bot_id
        )

    def _origin_session(self, message: SlackMessage) -> Session | None:
        command = message.text.strip()
        if command in {"!esc", "!done", "!end", "!join"} or command.startswith("!join "):
            return None
        if self._from_bridge(message) or (initiator := self._initiator(message)) is None:
            return None
        return Session(message.thread, initiator, initiator_is_bot=message.bot_id is not None)

    def _initiator(self, message: SlackMessage) -> str | None:
        if message.bot_id is not None:
            return message.bot_id if message.bot_id in self.config.allowed_bot_ids else None
        if message.user_id == self.config.owner_user_id or message.user_id in self.config.allowed_user_ids:
            return message.user_id
        return None

    def _can_prompt(self, message: SlackMessage, session: Session) -> bool:
        return message.bot_id is None and (
            message.user_id == self.config.owner_user_id
            or (not session.initiator_is_bot and message.user_id == session.initiator_id)
        )


@dataclass(frozen=True)
class SlackThread:
    workspace_id: str
    channel_id: str
    root_ts: str

    @property
    def key(self) -> tuple[str, str, str]:
        return self.workspace_id, self.channel_id, self.root_ts


@dataclass(frozen=True)
class SlackMessage:
    thread: SlackThread
    event_id: str
    message_ts: str
    text: str
    user_id: str | None
    bot_id: str | None

    @property
    def is_root(self) -> bool:
        return self.message_ts == self.thread.root_ts


@dataclass(frozen=True)
class HarnessSession:
    thread_id: str


@dataclass(frozen=True)
class Session:
    slack_thread: SlackThread
    initiator_id: str
    initiator_is_bot: bool
    harness_session: HarnessSession | None = None


@dataclass
class LiveSession:
    session: Session
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    seen: set[str] = field(default_factory=set)
    turn: Turn | None = None
    turn_started_at: Decimal = Decimal(0)
    closed: bool = False
    loaded: bool = False

    @property
    def running_turn(self) -> Turn | None:
        if self.turn is not None and not self.turn.stopped.is_set():
            return self.turn
        return None


@dataclass
class PendingControl:
    thread: SlackThread
    turn: Turn
    control: Control
    message_ts: str | None = None


class SessionStore:
    def __init__(self, path: Path):
        self._db = sqlite3.connect(path)
        try:
            mode = self._db.execute("PRAGMA journal_mode = WAL").fetchone()
            if mode != ("wal",):
                raise sqlite3.OperationalError("Could not enable WAL for the bridge database.")
            self._db.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    workspace_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    root_ts TEXT NOT NULL,
                    origin_event_id TEXT NOT NULL,
                    initiator_id TEXT NOT NULL,
                    initiator_is_bot INTEGER NOT NULL,
                    harness_thread_id TEXT,
                    kind TEXT NOT NULL DEFAULT 'inbox',
                    state TEXT NOT NULL DEFAULT 'creating',
                    PRIMARY KEY (workspace_id, channel_id, root_ts),
                    UNIQUE (workspace_id, origin_event_id)
                )
            """)
            self._db.execute("""
                CREATE TABLE IF NOT EXISTS accepted_prompts (
                    workspace_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    PRIMARY KEY (workspace_id, event_id)
                )
            """)
        except sqlite3.Error:
            self._db.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._db.close()

    def claim(self, session: Session, event_id: str) -> bool:
        with self._db:
            cursor = self._db.execute("""
                INSERT INTO sessions (
                    workspace_id, channel_id, root_ts, origin_event_id, initiator_id, initiator_is_bot
                ) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
            """, (*session.slack_thread.key, event_id, session.initiator_id, session.initiator_is_bot))
            return cursor.rowcount == 1

    def find(self, thread: SlackThread) -> tuple[Session, bool] | None:
        row = self._db.execute("""
            SELECT initiator_id, initiator_is_bot, harness_thread_id, state
            FROM sessions WHERE workspace_id = ? AND channel_id = ? AND root_ts = ?
        """, thread.key).fetchone()
        if row is None:
            return None
        initiator, is_bot, thread_id, state = row
        harness = HarnessSession(thread_id) if thread_id is not None else None
        return Session(thread, initiator, bool(is_bot), harness), state == "closed"

    def claim_prompt(self, message: SlackMessage) -> bool:
        with self._db:
            cursor = self._db.execute("""
                INSERT INTO accepted_prompts VALUES (?, ?) ON CONFLICT DO NOTHING
            """, (message.thread.workspace_id, message.event_id))
            return cursor.rowcount == 1

    def opened(self, session: Session) -> None:
        assert session.harness_session is not None
        with self._db:
            self._db.execute("""
                UPDATE sessions SET harness_thread_id = ?, state = 'open'
                WHERE workspace_id = ? AND channel_id = ? AND root_ts = ?
            """, (session.harness_session.thread_id, *session.slack_thread.key))


    def closed(self, thread: SlackThread) -> None:
        with self._db:
            self._db.execute("""
                UPDATE sessions SET state = 'closed'
                WHERE workspace_id = ? AND channel_id = ? AND root_ts = ?
            """, thread.key)


async def confirm_stopped(turn: Turn) -> bool:
    # An interrupt can report no active turn before the terminal notification arrives.
    try:
        await asyncio.wait_for(turn.stopped.wait(), timeout=2)
    except TimeoutError:
        return False
    return True


class SlackReplies(Protocol):
    async def origin_message(self, thread: SlackThread) -> SlackMessage | None: ...
    async def started(self, thread: SlackThread) -> None: ...
    async def closed(self, thread: SlackThread) -> None: ...
    async def working(self, thread: SlackThread, message_ts: str, active: bool) -> None: ...
    async def reply(self, thread: SlackThread, text: str) -> None: ...
    async def show_control(self, thread: SlackThread, control: Control) -> str | None: ...
    async def open_questions(self, control: Control, trigger_id: str) -> None: ...
