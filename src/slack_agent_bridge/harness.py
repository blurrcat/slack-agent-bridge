"""Codex app-server transport and multiplexed Agent Session lifecycle."""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
import time
from collections.abc import AsyncIterator, Coroutine
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .events import (
    ApprovalDecision, HarnessApproval, HarnessCommandFailed, HarnessEvent, HarnessItemCompleted,
    HarnessQuestion, HarnessQuestions, HarnessRequestKey, HarnessRequestReceived,
    HarnessRequestResolved, HarnessSessionStopped, HarnessTurnCompleted, HarnessTurnStarted, InputAnswers,
)


log = logging.getLogger(__name__)
Wire = dict[str, Any]


class HarnessError(RuntimeError):
    """An unconfirmed Harness operation or a transport failure."""
    def __init__(self, message: str, *, no_active_turn: bool = False):
        super().__init__(message)
        self.no_active_turn = no_active_turn


class HarnessSessionMissing(HarnessError):
    """Codex explicitly confirmed the requested persisted session is absent."""
    pass


class Harness:
    """A single app-server connection with a FIFO of prompt submissions per session."""

    def __init__(self, command: tuple[str, ...], cwd: Path | None = None, *, secrets: tuple[str, ...] = ()):
        self._command = command
        self._cwd = cwd
        self._secrets = secrets
        self._ids = itertools.count(1)
        self._events: asyncio.Queue[HarnessEvent] = asyncio.Queue()
        self._rpcs: dict[int, asyncio.Future[Wire]] = {}
        self._sessions: dict[str, _Session] = {}
        self._requests: dict[tuple[str, int | str], _ServerRequest] = {}
        self._write_lock = asyncio.Lock()
        self._tasks = asyncio.TaskGroup()
        self._running: set[asyncio.Task[None]] = set()

    async def start_session(self) -> str:
        """Wait for a new session ID; no application-event acknowledgment is needed."""
        response = await self._rpc("thread/start", {}, timeout=5)
        session_id = _object(response.get("thread")).get("id")
        if not isinstance(session_id, str) or not session_id:
            raise HarnessError("Codex returned no session ID.")
        self._attach_session(session_id)
        return session_id

    async def resume_session(self, session_id: str) -> None:
        """Subscribe to a known session without reloading its entire turn history."""
        response = await self._rpc("thread/resume", {"threadId": session_id, "excludeTurns": True}, timeout=5)
        if _object(response.get("thread")).get("id") != session_id:
            raise HarnessError("Codex did not confirm the mapped session.")
        self._attach_session(session_id)

    def _attach_session(self, session_id: str) -> None:
        """Give the session its own FIFO so another session can submit independently."""
        session = _Session()
        session.can_submit.set()
        self._sessions[session_id] = session
        session.worker = self._spawn(self._run_session(session_id, session))

    def send_message(self, session_id: str, command_id: str, text: str) -> None:
        """Cast a prompt; receipt matching stays internal unless submission fails."""
        self._sessions[session_id].commands.put_nowait(_Prompt(command_id, text))

    def stop_session(self, session_id: str, command_id: str) -> None:
        """Pause submissions while the stop runs, retaining queued prompts until it succeeds."""
        session = self._sessions[session_id]
        session.can_submit.clear()
        self._spawn(self._stop_session(session_id, session, command_id))

    def interrupt(self, session_id: str, turn_id: str, command_id: str) -> None:
        """Cast an interrupt for the specific turn if it is still known."""
        if (turn := self._sessions[session_id].turns.get(turn_id)) is not None:
            self._spawn(self._interrupt_cast(session_id, turn_id, turn, command_id))

    def answer_request(self, key: HarnessRequestKey, response: ApprovalDecision | InputAnswers) -> None:
        """Cast a typed response; the writer will check that the request is still live."""
        self._spawn(self._respond(key, response))

    def reject_request(self, key: HarnessRequestKey) -> None:
        """Return a protocol error when the Bridge cannot present the request."""
        self._spawn(self._respond(key, None))

    async def events(self) -> AsyncIterator[HarnessEvent]:
        """Expose observations from all attached sessions in arrival order."""
        while True:
            yield await self._events.get()

    async def __aenter__(self) -> Harness:
        """Launch and initialize the child without forwarding the bridge's Slack tokens."""
        env = {
            key: value for key, value in os.environ.items()
            if key not in {"SLACK_APP_TOKEN", "SLACK_BOT_TOKEN"}
            and not any(secret and secret in value for secret in self._secrets)
        }
        self._process = await asyncio.create_subprocess_exec(
            *self._command, cwd=self._cwd, env=env, limit=2**22,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        await self._tasks.__aenter__()
        self._spawn(self._read())
        self._spawn(self._drain_stderr())
        try:
            await self._rpc("initialize", {"clientInfo": {"name": "slack_agent_bridge", "version": "0.1.0"}})
            await self._send({"method": "initialized"})
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, *_: object) -> None:
        """Stop transport tasks, then close stdin before falling back to killing Codex."""
        for task in self._running:
            task.cancel()
        try:
            await self._tasks.__aexit__(None, None, None)
        finally:
            stdin = self._process.stdin
            assert stdin is not None
            stdin.close()
            try:
                await asyncio.wait_for(self._process.wait(), 3)
            except TimeoutError:
                try:
                    self._process.kill()
                except ProcessLookupError:
                    pass
                await self._process.wait()

    async def _run_session(self, session_id: str, session: _Session) -> None:
        """Allow one unacknowledged submission per session without waiting for turn completion."""
        while True:
            prompt = await session.commands.get()
            await session.can_submit.wait()
            await self._submit(session_id, session, prompt)

    async def _stop_session(self, session_id: str, session: _Session, command_id: str) -> None:
        """Interrupt observed turns and confirm no pending submission can start another."""
        try:
            async with asyncio.timeout(10):
                while True:
                    session.changed.clear()
                    for turn_id, turn in list(session.turns.items()):
                        await self._stop_turn(session_id, turn_id, turn)
                        await turn.completed.wait()
                    if not session.turns:
                        if session.finished_during_submit is None:
                            break
                        if session.turn_started_during_submit in session.finished_during_submit:
                            # The wire showed this submission's turn start and finish. A late
                            # receipt is no reason to keep a confirmed stop open.
                            break
                    # A submission may still announce a turn even before its reply arrives.
                    # Without a matching terminal turn or receipt, don't claim the session stopped.
                    await session.changed.wait()
        except (HarnessError, TimeoutError):
            self._events.put_nowait(HarnessCommandFailed(session_id, command_id, "stop"))
            session.can_submit.set()
        else:
            assert session.worker is not None
            session.worker.cancel()
            self._sessions.pop(session_id)
            self._events.put_nowait(HarnessSessionStopped(session_id))
            try:
                await self._rpc("thread/unsubscribe", {"threadId": session_id}, timeout=5)
            except HarnessError as error:
                # The turns are already stopped. Unloading the thread is best effort.
                log.warning("Unable to unsubscribe stopped Codex session: %s", error)

    async def _submit(self, session_id: str, session: _Session, command: _Prompt) -> None:
        """Let Codex choose whether this prompt starts a new turn or steers the current one."""
        session.finished_during_submit = set()
        session.turn_started_during_submit = None
        try:
            result = await self._rpc("turn/start", {
                "threadId": session_id,
                "input": [{"type": "text", "text": command.text, "text_elements": []}],
            })
            turn = _object(result.get("turn"))
            turn_id = turn.get("id")
            # Codex may have delivered the complete lifecycle before acknowledging the submission.
            if not isinstance(turn_id, str):
                raise HarnessError("Codex returned no turn ID.")
            if turn.get("status") == "inProgress" and turn_id not in session.finished_during_submit:
                self._observe_turn(session_id, session, turn_id)
        except HarnessError:
            self._events.put_nowait(HarnessCommandFailed(session_id, command.id, "prompt"))
        finally:
            session.finished_during_submit = None
            session.changed.set()

    async def _interrupt_cast(self, session_id: str, turn_id: str, turn: _Turn, command_id: str) -> None:
        """Report a failed interrupt against the original command and turn IDs."""
        try:
            await self._stop_turn(session_id, turn_id, turn)
        except HarnessError:
            self._events.put_nowait(HarnessCommandFailed(session_id, command_id, "interrupt", turn_id))

    async def _stop_turn(self, session_id: str, turn_id: str, turn: _Turn) -> None:
        """Share one interrupt between a normal interrupt and a concurrent session close."""
        async with turn.interrupt_lock:
            if turn.completed.is_set() or turn.interrupt_acknowledged:
                return
            try:
                await self._rpc("turn/interrupt", {"threadId": session_id, "turnId": turn_id}, timeout=5)
            except HarnessError as error:
                if turn.completed.is_set():
                    return
                # A completed turn can win this race before stdout delivers its terminal event.
                if error.no_active_turn:
                    try:
                        async with asyncio.timeout(2):
                            await turn.completed.wait()
                        return
                    except TimeoutError:
                        pass
                raise
            else:
                turn.interrupt_acknowledged = True

    async def _respond(self, key: HarnessRequestKey, response: ApprovalDecision | InputAnswers | None) -> None:
        """Recheck expiry under the write lock before anything reaches stdin."""
        async with self._write_lock:
            pending = self._requests.get((key.session_id, key.request_id))
            if pending is None or pending.key != key or pending.answered:
                return
            pending.answered = True
            message: Wire = {"id": key.request_id}
            if response is None:
                message["error"] = _unsupported_request()
            elif isinstance(response, tuple):
                message["result"] = {"answers": {name: {"answers": [value]} for name, value in response}}
            elif pending.permissions is not None:
                message["result"] = {
                    "permissions": {} if response == "decline" else pending.permissions,
                    "scope": "session" if response == "acceptForSession" else "turn",
                }
            else:
                message["result"] = {"decision": response}
            await self._write_wire(message)

    async def _read(self) -> None:
        """Separate server requests, notifications, and replies from a single stdout reader."""
        stdout = self._process.stdout
        assert stdout is not None
        while line := await stdout.readline():
            try:
                message = _object(json.loads(line))
            except (ValueError, UnicodeError):
                raise HarnessError("Invalid JSON from Codex app-server.") from None
            method = message.get("method")
            wire_id = message.get("id")
            if isinstance(method, str):
                params = _object(message.get("params"))
                if isinstance(wire_id, (int, str)):
                    await self._receive_request(method, wire_id, params)
                else:
                    self._receive_event(method, params)
            elif isinstance(wire_id, int):
                future = self._rpcs.get(wire_id)
                if future is not None and not future.done():
                    future.set_result(message)
        raise HarnessError("Codex app-server disconnected.")

    def _receive_event(self, method: str, params: Wire) -> None:
        """Update protocol state before publishing the corresponding immutable fact."""
        session_id = params.get("threadId")
        if not isinstance(session_id, str) or (session := self._sessions.get(session_id)) is None:
            return
        if method == "serverRequest/resolved":
            self._resolve_request(session_id, params.get("requestId"))
            return

        turn = _object(params.get("turn"))
        turn_id = turn.get("id") if method in {"turn/started", "turn/completed"} else params.get("turnId")
        if not isinstance(turn_id, str):
            return
        match method:
            case "turn/started":
                if turn_id not in session.turns and session.finished_during_submit is not None:
                    session.turn_started_during_submit = turn_id
                self._observe_turn(session_id, session, turn_id)
            case "item/completed":
                item = _object(params.get("item"))
                kind = item.get("type")
                if isinstance(kind, str):
                    self._observe_turn(session_id, session, turn_id)
                    self._events.put_nowait(HarnessItemCompleted(
                        session_id, turn_id, kind, _string(item.get("status")),
                        _string(item.get("text")) if kind == "agentMessage" else None,
                    ))
            case "turn/completed":
                if session.finished_during_submit is not None:
                    session.finished_during_submit.add(turn_id)
                if (active := session.turns.pop(turn_id, None)) is not None:
                    active.completed.set()
                for index, request in list(self._requests.items()):
                    if request.key.session_id == session_id and request.key.turn_id == turn_id:
                        self._requests.pop(index)
                self._events.put_nowait(HarnessTurnCompleted(
                    session_id, turn_id, _string(turn.get("status")) or "unknown",
                ))

    async def _receive_request(self, method: str, request_id: int | str, params: Wire) -> None:
        """Associate a server request with its live session and retain wire-only data."""
        session_id = params.get("threadId")
        turn_id = params.get("turnId")
        if not isinstance(session_id, str) or not isinstance(turn_id, str):
            await self._reject_wire(request_id)
            return
        session = self._sessions.get(session_id)
        if session is None:
            await self._reject_wire(request_id)
            return
        key = HarnessRequestKey(session_id, turn_id, request_id)
        request = _decode_request(method, params)
        permissions = (_object(params.get("permissions")) if method == "item/permissions/requestApproval" else None)
        self._requests[(session_id, request_id)] = _ServerRequest(key, permissions)
        self._observe_turn(session_id, session, turn_id)
        self._events.put_nowait(HarnessRequestReceived(key, request))

    def _observe_turn(self, session_id: str, session: _Session, turn_id: str) -> None:
        """Publish one start observation even if several protocol messages introduce the turn."""
        if turn_id not in session.turns:
            session.turns[turn_id] = _Turn()
            session.changed.set()
            self._events.put_nowait(HarnessTurnStarted(session_id, turn_id, time.time_ns()))

    def _resolve_request(self, session_id: str, request_id: object) -> None:
        """Forget the request before publishing its resolution, making late casts inert."""
        if isinstance(request_id, (int, str)):
            pending = self._requests.pop((session_id, request_id), None)
            if pending:
                self._events.put_nowait(HarnessRequestResolved(pending.key))

    async def _reject_wire(self, request_id: int | str) -> None:
        await self._send({"id": request_id, "error": _unsupported_request()})

    async def _rpc(self, method: str, params: Wire, *, timeout: float = 30) -> Wire:
        """Match one JSON-RPC receipt independently of the Bridge dispatcher."""
        request_id = next(self._ids)
        future: asyncio.Future[Wire] = asyncio.get_running_loop().create_future()
        self._rpcs[request_id] = future
        try:
            async with asyncio.timeout(timeout):
                await self._send({"id": request_id, "method": method, "params": params})
                reply = await future
        except TimeoutError:
            raise HarnessError(f"Codex did not confirm {method}.") from None
        finally:
            self._rpcs.pop(request_id, None)
        if "error" in reply:
            error = _object(reply["error"])
            reason = (_string(error.get("message")) or "").strip().lower().rstrip(".")
            if method == "thread/resume" and error.get("code") in {-32600, -32603}:
                missing = f"no rollout found for thread id {params['threadId'].lower()}"
                if reason in {missing, f"invalid thread-store request: {missing}"}:
                    raise HarnessSessionMissing("Codex could not find the mapped session.")
            raise HarnessError(f"Codex rejected {method}.", no_active_turn=reason in {
                "no active turn", "no active turn to interrupt",
            })
        return _object(reply.get("result"))

    async def _send(self, message: Wire) -> None:
        async with self._write_lock:
            await self._write_wire(message)

    async def _write_wire(self, message: Wire) -> None:
        """Write exactly one newline-delimited message with the writer lock already held."""
        stdin = self._process.stdin
        assert stdin is not None
        stdin.write(json.dumps(message).encode() + b"\n")
        await stdin.drain()

    async def _drain_stderr(self) -> None:
        stderr = self._process.stderr
        assert stderr is not None
        while await stderr.read(65536):
            # Don't log arbitrary host or tool data from the subprocess.
            pass

    def _spawn(self, coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        """Track task lifetimes so shutdown can cancel even an idle session worker."""
        task = self._tasks.create_task(coroutine)
        self._running.add(task)
        task.add_done_callback(self._running.discard)
        return task


@dataclass(frozen=True)
class _Prompt:
    """One submission cast and the Bridge ID used to correlate a failure."""
    id: str
    text: str


@dataclass
class _Turn:
    """Protocol completion and interrupt state, kept inside the Harness."""
    completed: asyncio.Event = field(default_factory=asyncio.Event)
    interrupt_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    interrupt_acknowledged: bool = False


@dataclass
class _Session:
    """Per-session submission and turn state, independent of Slack routing."""
    commands: asyncio.Queue[_Prompt] = field(default_factory=asyncio.Queue)
    worker: asyncio.Task[None] | None = None
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    can_submit: asyncio.Event = field(default_factory=asyncio.Event)
    turns: dict[str, _Turn] = field(default_factory=dict)
    finished_during_submit: set[str] | None = None
    turn_started_during_submit: str | None = None


@dataclass
class _ServerRequest:
    """Keep request identity and permission payloads private to the transport."""
    key: HarnessRequestKey
    permissions: Wire | None
    answered: bool = False


def _decode_request(method: str, params: Wire) -> HarnessApproval | HarnessQuestions | None:
    """Project supported protocol requests into typed, immutable data."""
    if method == "item/tool/requestUserInput":
        raw = params.get("questions")
        if not isinstance(raw, list):
            return None
        questions = []
        for entry in raw:
            question = _object(entry)
            question_id = _string(question.get("id"))
            label = _string(question.get("header"))
            text = _string(question.get("question"))
            if not question_id or label is None or text is None:
                return None
            options = tuple(
                name for option in (question.get("options") or [])
                if (name := _string(_object(option).get("label"))) is not None
            )
            questions.append(HarnessQuestion(question_id, label, text, options,
                                             bool(question.get("isOther")), bool(question.get("isSecret"))))
        return HarnessQuestions(tuple(questions))

    kind = _APPROVALS.get(method)
    if kind is None:
        return None
    details = tuple(
        (label, value if isinstance(value, str) else json.dumps(value))
        for name, label in _APPROVAL_FIELDS if (value := params.get(name)) is not None
    )
    raw_decisions = params.get("availableDecisions")
    decisions = tuple(value for value in raw_decisions if isinstance(value, str)) if isinstance(raw_decisions, list) else None
    return HarnessApproval(kind, details, decisions)


def _unsupported_request() -> Wire:
    return {"code": -32601, "message": "Request not supported here."}


def _object(value: object) -> Wire:
    return value if isinstance(value, dict) else {}


def _string(value: object) -> str | None:
    return value if isinstance(value, str) else None


_APPROVALS = {
    "item/commandExecution/requestApproval": "command",
    "item/fileChange/requestApproval": "file changes",
    "item/permissions/requestApproval": "permissions",
}
_APPROVAL_FIELDS = (
    ("command", "Command"), ("cwd", "Working directory"), ("grantRoot", "Requested root"),
    ("networkApprovalContext", "Network request"), ("permissions", "Permissions"), ("reason", "Reason"),
)
