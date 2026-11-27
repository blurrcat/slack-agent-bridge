import asyncio
import itertools
import json
import os
from pathlib import Path


class CodexError(RuntimeError):
    pass


class Codex:
    """One app-server connection; requests and notifications can interleave."""

    def __init__(self, command: tuple[str, ...], cwd: Path | None = None, *, secrets: tuple[str, ...] = ()):
        self.command = command
        self.cwd = cwd
        self.secrets = secrets
        self.closed = asyncio.Event()
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._turns: dict[str, asyncio.Queue] = {}

    async def __aenter__(self):
        env = {
            key: value for key, value in os.environ.items()
            if key not in {"SLACK_APP_TOKEN", "SLACK_BOT_TOKEN"}
            and not any(secret and secret in value for secret in self.secrets)
        }
        self._process = await asyncio.create_subprocess_exec(
            *self.command, cwd=self.cwd, env=env, limit=2**22,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        self._reader = asyncio.create_task(self._read())
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
            await asyncio.gather(self._reader, return_exceptions=True)

    async def _send(self, message: dict) -> None:
        assert self._process.stdin is not None
        try:
            self._process.stdin.write(json.dumps(message).encode() + b"\n")
            await self._process.stdin.drain()
        except OSError:
            raise CodexError("Codex stdin closed.") from None

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
            raise CodexError(f"Codex rejected {method}.")
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
                        await self._send({
                            "id": request_id,
                            "error": {"code": -32601, "message": "Interactive requests are not supported yet."},
                        })
                    elif (future := self._pending.get(request_id)) is not None and not future.done():
                        future.set_result(message)
                elif method in {"item/completed", "turn/completed"}:
                    params = message["params"]
                    queue = self._turns.get(params["threadId"])
                    if queue is not None and (
                        method == "turn/completed" or params["item"]["type"] == "agentMessage"
                    ):
                        queue.put_nowait((method, params))
        except (OSError, ValueError, TypeError, KeyError, AttributeError, CodexError):
            # Do not write protocol payloads or tool output to default logs.
            pass
        finally:
            self.closed.set()
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(CodexError("Codex app-server disconnected."))
            for queue in self._turns.values():
                queue.put_nowait(None)

    async def start_thread(self) -> str:
        result = await self._request("thread/start", {})
        return result["thread"]["id"]

    async def first_reply(self, thread_id: str, prompt: str) -> str:
        messages: asyncio.Queue = asyncio.Queue()
        self._turns[thread_id] = messages
        try:
            result = await self._request("turn/start", {
                "threadId": thread_id,
                "input": [{"type": "text", "text": prompt, "text_elements": []}],
            })
            turn_id = result["turn"]["id"]
            answer = ""
            async with asyncio.timeout(300):
                while True:
                    event = await messages.get()
                    if event is None:
                        raise CodexError("Codex disconnected before finishing the turn.")
                    method, params = event
                    if method == "item/completed" and params["turnId"] == turn_id:
                        answer = params["item"]["text"]
                    if method == "turn/completed" and params["turn"]["id"] == turn_id:
                        if params["turn"]["status"] != "completed" or not answer.strip():
                            raise CodexError("Codex did not complete a text reply.")
                        return answer
        except TimeoutError:
            raise CodexError("Codex did not confirm completion of the turn.") from None
        finally:
            self._turns.pop(thread_id, None)
