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
    def __init__(self, message: str, *, rejected: bool = False, no_active_turn: bool = False):
        super().__init__(message)
        self.rejected = rejected
        self.no_active_turn = no_active_turn


class CodexSessionMissing(CodexError):
    pass


class Codex:
    """One app-server connection; requests and notifications can interleave."""

    async def start_thread(self) -> str:
        result = await self._request("thread/start", {})
        return result["thread"]["id"]

    async def resume_thread(self, thread_id: str) -> None:
        result = await self._request("thread/resume", {"threadId": thread_id, "excludeTurns": True})
        if result.get("thread", {}).get("id") != thread_id:
            raise CodexError("Codex did not confirm the mapped thread.")

    async def prime_thread(self, thread_id: str, prompt: str) -> str:
        """Run a context-only turn without presenting interactive requests in Slack."""
        turn = await self.submit(thread_id, prompt)
        answer = ""
        try:
            async with asyncio.timeout(60):
                while (event := await turn.events.get()) is not None:
                    method, params = event
                    if method == "request" and params["request_id"] not in turn.resolved_requests:
                        await self.reject_request(params["request_id"])
                    elif method == "item/completed" and params["turnId"] == turn.id:
                        item = params["item"]
                        if item["type"] == "agentMessage":
                            answer = item["text"]
                    elif method == "turn/completed" and params["turn"]["id"] == turn.id:
                        if params["turn"]["status"] == "completed":
                            return answer.strip()
                        break
        except TimeoutError:
            try:
                async with asyncio.timeout(3):
                    if not turn.stopped.is_set():
                        await self.interrupt(turn)
            except (CodexError, TimeoutError):
                log.warning("Codex did not confirm interruption of the priming turn.")
            raise CodexError("Codex did not finish priming the replacement session in time.") from None
        raise CodexError("Codex did not complete the priming turn.")

    async def submit(self, thread_id: str, prompt: str) -> Turn:
        """Start or steer; return the same Turn object if Codex steers it."""
        if thread_id in self._submitting:
            raise CodexError("Another input submission is awaiting acknowledgment.")
        # Keep turns observed before the response, even if they already completed.
        observed: dict[str, Turn] = {}
        self._submitting[thread_id] = observed
        try:
            result = await self._request("turn/start", {"threadId": thread_id, "input": text_input(prompt)})
            turn_id = result["turn"]["id"]
            key = (thread_id, turn_id)
            turn = self._turns.get(key) or observed.get(turn_id)
            if turn is None:
                turn = self._turns[key] = Turn(thread_id, turn_id)
            return turn
        finally:
            self._submitting.pop(thread_id)

    async def interrupt(self, turn: Turn) -> None:
        await self._request("turn/interrupt", {"threadId": turn.thread_id, "turnId": turn.id})

    async def answer_request(self, request_id: int | str, result: dict) -> None:
        await self._send({"id": request_id, "result": result})

    async def reject_request(self, request_id: int | str) -> None:
        await self._send({"id": request_id, "error": {"code": -32601, "message": "Request not supported here."}})

    def __init__(self, command: tuple[str, ...], cwd: Path | None = None, *, secrets: tuple[str, ...] = ()):
        self.command = command
        self.cwd = cwd
        self.secrets = secrets
        self.closed = asyncio.Event()
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._turns: dict[tuple[str, str], Turn] = {}
        self._submitting: dict[str, dict[str, Turn]] = {}

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
            error = reply["error"]
            reason = str(error.get("message", "")).strip().lower().rstrip(".")
            if (
                method == "thread/resume" and error.get("code") in {-32600, -32603}
                and reason in {
                    f"no rollout found for thread id {params['threadId'].lower()}",
                    f"invalid thread-store request: no rollout found for thread id {params['threadId'].lower()}",
                }
            ):
                raise CodexSessionMissing("Codex could not find the mapped session.", rejected=True)
            raise CodexError(f"Codex rejected {method}.", rejected=True, no_active_turn=reason in {
                "no active turn", "no active turn to interrupt",
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
                        turn = self._observe_turn(params.get("threadId"), params.get("turnId"))
                        if turn is not None and not turn.stopped.is_set():
                            turn.events.put_nowait(("request", params | {"request_id": request_id, "method": method}))
                        else:
                            await self.reject_request(request_id)
                    elif (future := self._pending.get(request_id)) is not None and not future.done():
                        future.set_result(message)
                elif method in {"turn/started", "item/completed", "turn/completed", "serverRequest/resolved"}:
                    self._notify(method, message["params"])
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

    def _notify(self, method: str, params: dict) -> None:
        thread_id = params["threadId"]
        if method == "serverRequest/resolved":
            for active in self._turns.values():
                if active.thread_id == thread_id:
                    active.resolved_requests.add(params["requestId"])
                    active.events.put_nowait((method, params))
            return

        turn_id = params["turn"]["id"] if method in {"turn/started", "turn/completed"} else params["turnId"]
        turn = self._observe_turn(thread_id, turn_id)
        if turn is None or turn.stopped.is_set() or method == "turn/started":
            return
        if method == "turn/completed":
            turn.stopped.set()
            self._turns.pop((thread_id, turn_id))
            if (observed := self._submitting.get(thread_id)) is not None:
                observed[turn_id] = turn
        turn.events.put_nowait((method, params))

    def _observe_turn(self, thread_id: str | None, turn_id: str | None) -> Turn | None:
        if thread_id is None or turn_id is None:
            return None
        key = (thread_id, turn_id)
        if turn := self._turns.get(key):
            return turn
        observed = self._submitting.get(thread_id)
        if observed is None:
            return None
        if turn_id not in observed:
            turn = observed[turn_id] = self._turns[key] = Turn(thread_id, turn_id)
            return turn
        return observed[turn_id]

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
    id: str
    interrupting: bool = False
    resolved_requests: set[int | str] = field(default_factory=set)
    stopped: asyncio.Event = field(default_factory=asyncio.Event)
    events: asyncio.Queue[tuple[str, dict] | None] = field(default_factory=asyncio.Queue)
