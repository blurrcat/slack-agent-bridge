from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path


log = logging.getLogger(__name__)


class CodexError(RuntimeError):
    def __init__(self, message: str, *, no_active_turn: bool = False):
        super().__init__(message)
        self.no_active_turn = no_active_turn


class Codex:
    """One app-server connection; requests and notifications can interleave."""

    async def start_thread(self) -> str:
        result = await self._request("thread/start", {})
        return result["thread"]["id"]

    async def start_turn(self, thread_id: str, prompt: str) -> Turn:
        turn = Turn(thread_id)
        self._turns[thread_id] = turn
        try:
            result = await self._request("turn/start", {"threadId": thread_id, "input": text_input(prompt)})
            turn.id = result["turn"]["id"]
            return turn
        except BaseException:
            self._turns.pop(thread_id, None)
            raise

    async def steer(self, turn: Turn, prompt: str) -> None:
        await self._request("turn/steer", {
            "threadId": turn.thread_id, "expectedTurnId": turn.id, "input": text_input(prompt),
        })

    async def interrupt(self, turn: Turn) -> None:
        await self._request("turn/interrupt", {"threadId": turn.thread_id, "turnId": turn.id})

    async def answer_request(self, request_id: int | str, result: dict) -> None:
        await self._send({"id": request_id, "result": result})

    async def reject_request(self, request_id: int | str) -> None:
        await self._send({"id": request_id, "error": {"code": -32601, "message": "Request not supported here."}})

    def release_turn(self, turn: Turn) -> None:
        if self._turns.get(turn.thread_id) is turn:
            self._turns.pop(turn.thread_id)

    def __init__(self, command: tuple[str, ...], cwd: Path | None = None, *, secrets: tuple[str, ...] = ()):
        self.command = command
        self.cwd = cwd
        self.secrets = secrets
        self.closed = asyncio.Event()
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._turns: dict[str, Turn] = {}

    async def __aenter__(self):
        env = {
            key: value for key, value in os.environ.items()
            if key not in {"SLACK_APP_TOKEN", "SLACK_BOT_TOKEN"}
            and not any(secret and secret in value for secret in self.secrets)
        }
        self._process = await asyncio.create_subprocess_exec(
            *self.command, cwd=self.cwd, env=env, limit=2**22,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._reader = asyncio.create_task(self._read())
        self._stderr = asyncio.create_task(self._drain_stderr())
        try:
            await self._request("initialize", {
                "clientInfo": {"name": "slack_agent_bridge", "version": "0.1.0"},
            })
            await self._send({"method": "initialized"})
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, *_):
        assert self._process.stdin is not None
        self._process.stdin.close()
        try:
            await asyncio.wait_for(self._process.wait(), 3)
        except TimeoutError:
            try:
                self._process.kill()
            except ProcessLookupError:
                pass
            await self._process.wait()
        finally:
            self._reader.cancel()
            self._stderr.cancel()
            await asyncio.gather(self._reader, self._stderr, return_exceptions=True)

    async def _request(self, method: str, params: dict) -> dict:
        if self.closed.is_set():
            raise CodexError("Codex app-server is not running.")
        request_id = next(self._ids)
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            async with asyncio.timeout(30):
                await self._send({"id": request_id, "method": method, "params": params})
                reply = await future
        except TimeoutError:
            raise CodexError(f"Codex did not confirm {method}.") from None
        finally:
            self._pending.pop(request_id, None)
        if "error" in reply:
            reason = str(reply["error"].get("message", "")).strip().lower().rstrip(".")
            raise CodexError(f"Codex rejected {method}.", no_active_turn=reason in {
                "no active turn", "no active turn to steer", "no active turn to interrupt",
            })
        return reply["result"]

    async def _read(self) -> None:
        assert self._process.stdout is not None
        try:
            while line := await self._process.stdout.readline():
                message = json.loads(line)
                request_id = message.get("id")
                method = message.get("method")
                if request_id is not None:
                    if method is not None:
                        params = message.get("params", {})
                        turn = self._turns.get(params.get("threadId"))
                        if turn is not None and not turn.stopped.is_set():
                            turn.events.put_nowait(("request", params | {"request_id": request_id, "method": method}))
                        else:
                            await self.reject_request(request_id)
                    elif (future := self._pending.get(request_id)) is not None and not future.done():
                        future.set_result(message)
                elif method in {"item/completed", "turn/completed", "serverRequest/resolved"}:
                    params = message["params"]
                    turn = self._turns.get(params["threadId"])
                    if turn is not None:
                        if method == "turn/completed":
                            if turn.id is not None and params["turn"]["id"] != turn.id:
                                continue
                            turn.stopped.set()
                        elif method == "serverRequest/resolved":
                            turn.resolved_requests.add(params["requestId"])
                        turn.events.put_nowait((method, params))
        except (OSError, ValueError, TypeError, KeyError, AttributeError, CodexError) as error:
            log.warning("Could not read Codex stdio (%s).", type(error).__name__)
        finally:
            self.closed.set()
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(CodexError("Codex app-server disconnected."))
            for turn in self._turns.values():
                turn.stopped.set()
                turn.events.put_nowait(None)

    async def _send(self, message: dict) -> None:
        assert self._process.stdin is not None
        try:
            self._process.stdin.write(json.dumps(message).encode() + b"\n")
            await self._process.stdin.drain()
        except OSError:
            raise CodexError("Codex stdin closed.") from None

    async def _drain_stderr(self) -> None:
        assert self._process.stderr is not None
        while await self._process.stderr.read(65536):
            # Keep the pipe flowing without logging host or tool data.
            pass



def text_input(prompt: str) -> list[dict]:
    return [{"type": "text", "text": prompt, "text_elements": []}]

@dataclass(eq=False)
class Turn:
    thread_id: str
    id: str | None = None
    interrupting: bool = False
    resolved_requests: set[int | str] = field(default_factory=set)
    stopped: asyncio.Event = field(default_factory=asyncio.Event)
    events: asyncio.Queue[tuple[str, dict] | None] = field(default_factory=asyncio.Queue)
