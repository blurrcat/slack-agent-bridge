import logging
import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol

from .codex import Codex, CodexError
from .config import Config


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SlackThread:
    workspace_id: str
    channel_id: str
    root_ts: str

    @property
    def key(self) -> tuple[str, str, str]:
        return self.workspace_id, self.channel_id, self.root_ts


@dataclass(frozen=True)
class OriginMessage:
    thread: SlackThread
    event_id: str
    text: str
    user_id: str | None
    bot_id: str | None


@dataclass(frozen=True)
class HarnessSession:
    thread_id: str


@dataclass(frozen=True)
class Session:
    slack_thread: SlackThread
    initiator_id: str
    initiator_is_bot: bool
    harness_session: HarnessSession | None = None


class SessionStore:
    def __init__(self, path: Path):
        self._db = sqlite3.connect(path)
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

    def opened(self, session: Session) -> None:
        assert session.harness_session is not None
        with self._db:
            self._db.execute("""
                UPDATE sessions SET harness_thread_id = ?, state = 'open'
                WHERE workspace_id = ? AND channel_id = ? AND root_ts = ?
            """, (session.harness_session.thread_id, *session.slack_thread.key))


class SlackReplies(Protocol):
    async def started(self, thread: SlackThread) -> None: ...
    async def reply(self, thread: SlackThread, text: str) -> None: ...


class Inbox:
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

    async def handle(self, origin: OriginMessage | None) -> None:
        if origin is None:
            return
        thread = origin.thread
        if thread.workspace_id != self.workspace_id or thread.channel_id != self.config.inbox_channel_id:
            return
        if origin.user_id == self.bridge_user_id or (origin.bot_id and origin.bot_id == self.bridge_bot_id):
            return
        if origin.bot_id:
            if origin.bot_id not in self.config.allowed_bot_ids:
                return
            initiator = origin.bot_id
        else:
            if not origin.user_id or (
                origin.user_id != self.config.owner_user_id and origin.user_id not in self.config.allowed_user_ids
            ):
                return
            initiator = origin.user_id

        session = Session(thread, initiator, initiator_is_bot=bool(origin.bot_id))
        if not self.store.claim(session, origin.event_id):
            log.info("Already handled inbox message %s/%s", thread.channel_id, thread.root_ts)
            return

        log.info("Accepted inbox message %s/%s", thread.channel_id, thread.root_ts)
        try:
            codex_thread = await self.codex.start_thread()
            session = replace(session, harness_session=HarnessSession(codex_thread))
            self.store.opened(session)
            await self.slack.started(thread)
            answer = await self.codex.first_reply(codex_thread, origin.text)
        except CodexError as error:
            log.warning("Codex did not complete inbox message %s: %s", thread.root_ts, error)
            await self.slack.reply(thread, "Codex did not confirm a reply. The bridge did not retry the job.")
            return

        await self.slack.reply(thread, answer)
        log.info("Posted the first reply for %s/%s", thread.channel_id, thread.root_ts)
