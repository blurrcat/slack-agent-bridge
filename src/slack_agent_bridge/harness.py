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
    def __init__(self, message: str, *, no_active_turn: bool = False):
        super().__init__(message)
        self.no_active_turn = no_active_turn


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
        response = await self._rpc("thread/start", {}, timeout=5)
        session_id = _object(response.get("thread")).get("id")
        if not isinstance(session_id, str) or not session_id:
            raise HarnessError("Codex returned no session ID.")
        session = _Session()
        self._sessions[session_id] = session
        session.worker = self._spawn(self._run_session(session_id, session))
        return session_id

    def send_message(self, session_id: str, command_id: str, text: str) -> None:
        self._sessions[session_id].commands.put_nowait(_Prompt(command_id, text))

    def stop_session(self, session_id: str, command_id: str) -> None:
        session = self._sessions[session_id]
        while not session.commands.empty():
            session.commands.get_nowait()
        self._spawn(self._stop_session(session_id, session, command_id))

    def interrupt(self, session_id: str, turn_id: str, command_id: str) -> None:
        if (turn := self._sessions[session_id].turns.get(turn_id)) is not None:
            self._spawn(self._interrupt_cast(session_id, turn_id, turn, command_id))

    def answer_request(self, key: HarnessRequestKey, response: ApprovalDecision | InputAnswers) -> None:
        self._spawn(self._respond(key, response))

    def reject_request(self, key: HarnessRequestKey) -> None:
        self._spawn(self._respond(key, None))

    async def events(self) -> AsyncIterator[HarnessEvent]:
        while True:
            yield await self._events.get()

    async def __aenter__(self) -> Harness:
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
        while True:
            await self._submit(session_id, session, await session.commands.get())

    async def _stop_session(self, session_id: str, session: _Session, command_id: str) -> None:
        try:
            async with asyncio.timeout(10):
                while True:
                    session.changed.clear()
                    for turn_id, turn in list(session.turns.items()):
                        await self._stop_turn(session_id, turn_id, turn)
                        await turn.completed.wait()
                    if session.finished_during_submit is None and not session.turns:
                        break
                    # A submission may still announce a turn even before its reply arrives.
                    await session.changed.wait()
                await self._rpc("thread/unsubscribe", {"threadId": session_id}, timeout=5)
        except (HarnessError, TimeoutError):
            self._events.put_nowait(HarnessCommandFailed(session_id, command_id, "stop"))
        else:
            assert session.worker is not None
            session.worker.cancel()
            self._sessions.pop(session_id)
            self._events.put_nowait(HarnessSessionStopped(session_id))

    async def _submit(self, session_id: str, session: _Session, command: _Prompt) -> None:
        session.finished_during_submit = set()
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
        try:
            await self._stop_turn(session_id, turn_id, turn)
        except HarnessError:
            self._events.put_nowait(HarnessCommandFailed(session_id, command_id, "interrupt", turn_id))

    async def _stop_turn(self, session_id: str, turn_id: str, turn: _Turn) -> None:
        async with turn.interrupt_lock:
            if turn.completed.is_set() or turn.interrupt_acknowledged:
                return
            try:
                await self._rpc("turn/interrupt", {"threadId": session_id, "turnId": turn_id}, timeout=5)
            except HarnessError as error:
                if turn.completed.is_set():
                    return
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
        index = (key.session_id, key.request_id)
        pending = self._requests.get(index)
        if pending is None or pending.key != key or pending.answered:
            return
        pending.answered = True
        if response is None:
            await self._reject_wire(key.request_id)
        elif isinstance(response, tuple):
            await self._send({"id": key.request_id, "result": {
                "answers": {name: {"answers": [value]} for name, value in response},
            }})
        elif pending.permissions is not None:
            await self._send({"id": key.request_id, "result": {
                "permissions": {} if response == "decline" else pending.permissions,
                "scope": "session" if response == "acceptForSession" else "turn",
            }})
        else:
            await self._send({"id": key.request_id, "result": {"decision": response}})

    async def _read(self) -> None:
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
        if turn_id not in session.turns:
            session.turns[turn_id] = _Turn()
            session.changed.set()
            self._events.put_nowait(HarnessTurnStarted(session_id, turn_id, time.time_ns()))

    def _resolve_request(self, session_id: str, request_id: object) -> None:
        if isinstance(request_id, (int, str)):
            pending = self._requests.pop((session_id, request_id), None)
            if pending:
                self._events.put_nowait(HarnessRequestResolved(pending.key))

    async def _reject_wire(self, request_id: int | str) -> None:
        await self._send({"id": request_id, "error": {"code": -32601, "message": "Request not supported here."}})

    async def _rpc(self, method: str, params: Wire, *, timeout: float = 30) -> Wire:
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
            reason = _string(_object(reply["error"]).get("message")) or ""
            raise HarnessError(f"Codex rejected {method}.", no_active_turn=reason.strip().lower().rstrip(".") in {
                "no active turn", "no active turn to interrupt",
            })
        return _object(reply.get("result"))

    async def _send(self, message: Wire) -> None:
        stdin = self._process.stdin
        assert stdin is not None
        async with self._write_lock:
            stdin.write(json.dumps(message).encode() + b"\n")
            await stdin.drain()

    async def _drain_stderr(self) -> None:
        stderr = self._process.stderr
        assert stderr is not None
        while await stderr.read(65536):
            # Don't log arbitrary host or tool data from the subprocess.
            pass

    def _spawn(self, coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = self._tasks.create_task(coroutine)
        self._running.add(task)
        task.add_done_callback(self._running.discard)
        return task


@dataclass(frozen=True)
class _Prompt:
    id: str
    text: str


@dataclass
class _Turn:
    completed: asyncio.Event = field(default_factory=asyncio.Event)
    interrupt_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    interrupt_acknowledged: bool = False


@dataclass
class _Session:
    commands: asyncio.Queue[_Prompt] = field(default_factory=asyncio.Queue)
    worker: asyncio.Task[None] | None = None
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    turns: dict[str, _Turn] = field(default_factory=dict)
    finished_during_submit: set[str] | None = None


@dataclass
class _ServerRequest:
    key: HarnessRequestKey
    permissions: Wire | None
    answered: bool = False


def _decode_request(method: str, params: Wire) -> HarnessApproval | HarnessQuestions | None:
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
