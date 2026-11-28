"""Codex app-server transport and multiplexed Agent Session lifecycle."""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from .events import (
    ApprovalDecision, HarnessApproval, HarnessEvent, HarnessItemCompleted,
    HarnessQuestion, HarnessQuestions, HarnessRequestKey, HarnessRequestReceived,
    HarnessRequestResolved, HarnessTurnCompleted, HarnessTurnStarted, InputAnswers,
)
from .json_decoder import as_object, as_string


log = logging.getLogger(__name__)
Wire = dict[str, Any]


class HarnessError(RuntimeError):
    """An unconfirmed Harness operation or a transport failure."""


class HarnessSessionMissing(HarnessError):
    """Codex explicitly confirmed the requested persisted session is absent."""
    pass


class Harness:
    """A single app-server connection with one reader for all sessions."""

    def __init__(self, binary: str, args: tuple[str, ...] = (), *, cwd: Path | None = None):
        self._binary = binary
        self._args = args
        self._cwd = cwd
        self._ids = itertools.count(1)
        self._events: asyncio.Queue[HarnessEvent] = asyncio.Queue()
        # Only calls register a reply waiter. Casts have no pending entry.
        self._pending: dict[int, asyncio.Future[Wire]] = {}
        self._write_lock = asyncio.Lock()
        self._tasks = asyncio.TaskGroup()

    async def start_session(self) -> str:
        """Wait for a new session ID; no application-event acknowledgment is needed."""
        response = await self._call("thread/start", {}, timeout=5)
        session_id = as_object(response.get("thread")).get("id")
        if not isinstance(session_id, str) or not session_id:
            raise HarnessError("Codex returned no session ID.")
        return session_id

    async def resume_session(self, session_id: str) -> None:
        """Subscribe to a known session without reloading its entire turn history."""
        response = await self._call("thread/resume", {"threadId": session_id, "excludeTurns": True}, timeout=5)
        if as_object(response.get("thread")).get("id") != session_id:
            raise HarnessError("Codex did not confirm the mapped session.")

    async def send_message(self, session_id: str, text: str) -> None:
        """Cast a prompt: wait for the write, not the reply."""
        await self._cast({"id": next(self._ids), "method": "turn/start", "params": {
            "threadId": session_id,
            "input": [{"type": "text", "text": text, "text_elements": []}],
        }})

    async def stop_session(self, session_id: str) -> None:
        """Cast an unsubscribe without waiting for confirmation."""
        await self._cast({"id": next(self._ids), "method": "thread/unsubscribe", "params": {
            "threadId": session_id,
        }})

    async def interrupt(self, session_id: str, turn_id: str) -> None:
        """Cast an interrupt for the supplied turn without waiting for confirmation."""
        await self._cast({"id": next(self._ids), "method": "turn/interrupt", "params": {
            "threadId": session_id, "turnId": turn_id,
        }})

    async def answer_request(
        self, request_id: int | str, response: ApprovalDecision | InputAnswers, *, permissions_json: str | None = None,
    ) -> None:
        """Encode the Bridge's answer and write it to Codex."""
        if isinstance(response, tuple):
            result = {"answers": {name: {"answers": [value]} for name, value in response}}
        elif permissions_json is not None:
            result = {
                "permissions": {} if response == "decline" else json.loads(permissions_json),
                "scope": "session" if response == "acceptForSession" else "turn",
            }
        else:
            result = {"decision": response}
        await self._cast({"id": request_id, "result": result})

    async def reject_request(self, request_id: int | str) -> None:
        """Return a protocol error when the request cannot be presented."""
        await self._cast({"id": request_id, "error": {"code": -32601, "message": "Request not supported here."}})

    async def events(self) -> AsyncIterator[HarnessEvent]:
        """Expose observations from all attached sessions in arrival order."""
        while True:
            yield await self._events.get()

    async def __aenter__(self) -> Harness:
        """Launch and initialize the Codex app-server."""
        self._process = await asyncio.create_subprocess_exec(
            self._binary, *self._args, "app-server", "--stdio", cwd=self._cwd, limit=2**22,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        await self._tasks.__aenter__()
        self._reader = self._tasks.create_task(self._read())
        self._stderr_reader = self._tasks.create_task(self._drain_stderr())
        try:
            await self._call("initialize", {"clientInfo": {"name": "slack_agent_bridge", "version": "0.1.0"}})
            await self._cast({"method": "initialized"})
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, *_: object) -> None:
        """Stop transport tasks, then close stdin before falling back to killing Codex."""
        self._reader.cancel()
        self._stderr_reader.cancel()
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

    async def _read(self) -> None:
        """Separate server requests, notifications, and replies from a single stdout reader."""
        stdout = self._process.stdout
        assert stdout is not None
        while line := await stdout.readline():
            try:
                message = as_object(json.loads(line))
            except (ValueError, UnicodeError):
                raise HarnessError("Invalid JSON from Codex app-server.") from None
            method = message.get("method")
            wire_id = message.get("id")
            if isinstance(method, str):
                params = as_object(message.get("params"))
                if isinstance(wire_id, (int, str)):
                    await self._receive_request(method, wire_id, params)
                else:
                    self._receive_event(method, params)
            elif isinstance(wire_id, int):
                future = self._pending.pop(wire_id, None)
                if future is not None and not future.done():
                    future.set_result(message)
        raise HarnessError("Codex app-server disconnected.")

    def _receive_event(self, method: str, params: Wire) -> None:
        """Translate a wire notification without tracking session or request state."""
        session_id = params.get("threadId")
        if not isinstance(session_id, str):
            return
        if method == "serverRequest/resolved":
            request_id = params.get("requestId")
            if isinstance(request_id, (int, str)):
                self._events.put_nowait(HarnessRequestResolved(session_id, request_id))
            return

        turn = as_object(params.get("turn"))
        turn_id = turn.get("id") if method in {"turn/started", "turn/completed"} else params.get("turnId")
        if not isinstance(turn_id, str):
            return
        match method:
            case "turn/started":
                self._events.put_nowait(HarnessTurnStarted(session_id, turn_id, time.time_ns()))
            case "item/completed":
                item = as_object(params.get("item"))
                kind = item.get("type")
                if isinstance(kind, str):
                    self._events.put_nowait(HarnessItemCompleted(
                        session_id, turn_id, kind, as_string(item.get("status")),
                        as_string(item.get("text")) if kind == "agentMessage" else None,
                    ))
            case "turn/completed":
                self._events.put_nowait(HarnessTurnCompleted(
                    session_id, turn_id, as_string(turn.get("status")) or "unknown",
                ))

    async def _receive_request(self, method: str, request_id: int | str, params: Wire) -> None:
        """Forward the request with the IDs the Bridge needs to manage it."""
        session_id = params.get("threadId")
        turn_id = params.get("turnId")
        if not isinstance(session_id, str) or not isinstance(turn_id, str):
            await self.reject_request(request_id)
            return
        key = HarnessRequestKey(session_id, turn_id, request_id)
        self._events.put_nowait(HarnessRequestReceived(key, _decode_request(method, params)))

    async def _call(self, method: str, params: Wire, *, timeout: float = 30) -> Wire:
        """Write a request, then wait for its matching Codex reply or raise."""
        request_id = next(self._ids)
        future: asyncio.Future[Wire] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            async with asyncio.timeout(timeout):
                await self._cast({"id": request_id, "method": method, "params": params})
                reply = await future
        except TimeoutError:
            raise HarnessError(f"Codex did not confirm {method}.") from None
        finally:
            self._pending.pop(request_id, None)
        if "error" in reply:
            error = as_object(reply["error"])
            reason = (as_string(error.get("message")) or "").strip().lower().rstrip(".")
            if method == "thread/resume" and error.get("code") in {-32600, -32603}:
                missing = f"no rollout found for thread id {params['threadId'].lower()}"
                if reason in {missing, f"invalid thread-store request: {missing}"}:
                    raise HarnessSessionMissing("Codex could not find the mapped session.")
            raise HarnessError(f"Codex rejected {method}.")
        return as_object(reply.get("result"))

    async def _cast(self, message: Wire) -> None:
        """Wait only for the wire write; don't register or wait for a reply."""
        async with self._write_lock:
            stdin = self._process.stdin
            assert stdin is not None
            stdin.write(json.dumps(message).encode() + b"\n")
            await stdin.drain()

    async def _drain_stderr(self) -> None:
        """Keep stderr flowing and send Codex diagnostics to the service log."""
        stderr = self._process.stderr
        assert stderr is not None
        while chunk := await stderr.read(65536):
            if message := chunk.decode(errors="replace").rstrip():
                log.error("Codex stderr: %s", message)


def _decode_request(method: str, params: Wire) -> HarnessApproval | HarnessQuestions | None:
    """Project supported protocol requests into typed, immutable data."""
    if method == "item/tool/requestUserInput":
        raw = params.get("questions")
        if not isinstance(raw, list):
            return None
        questions = []
        for entry in raw:
            question = as_object(entry)
            question_id = as_string(question.get("id"))
            label = as_string(question.get("header"))
            text = as_string(question.get("question"))
            if not question_id or label is None or text is None:
                return None
            options = tuple(
                name for option in (question.get("options") or [])
                if (name := as_string(as_object(option).get("label"))) is not None
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
    permissions_json = json.dumps(as_object(params.get("permissions"))) if kind == "permissions" else None
    return HarnessApproval(kind, details, decisions, permissions_json)


_APPROVALS = {
    "item/commandExecution/requestApproval": "command",
    "item/fileChange/requestApproval": "file changes",
    "item/permissions/requestApproval": "permissions",
}
_APPROVAL_FIELDS = (
    ("command", "Command"), ("cwd", "Working directory"), ("grantRoot", "Requested root"),
    ("networkApprovalContext", "Network request"), ("permissions", "Permissions"), ("reason", "Reason"),
)
