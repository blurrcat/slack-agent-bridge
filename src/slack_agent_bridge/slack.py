import asyncio
import logging
from pathlib import Path

from aiohttp import ClientError
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode.aiohttp import SocketModeClient
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.web.async_client import AsyncWebClient

from .codex import Codex
from .config import Config
from .inbox import Inbox, OriginMessage, SessionStore, SlackThread


log = logging.getLogger(__name__)


class BridgeError(RuntimeError):
    pass


async def serve(config: Config, database: Path) -> None:
    web = AsyncWebClient(token=config.slack_bot_token)
    try:
        auth = await web.auth_test()
    except SlackApiError:
        raise BridgeError("Slack bot authentication failed. Check the bot token.") from None

    with SessionStore(database) as store:
        async with Codex(config.codex_command, config.codex_cwd, secrets=(
            config.slack_app_token, config.slack_bot_token,
        )) as codex:
            inbox = Inbox(config, auth["team_id"], auth["user_id"], codex, store, SlackOutput(web), auth.get("bot_id"))
            async with asyncio.TaskGroup() as jobs:
                async def handle(origin: OriginMessage) -> None:
                    try:
                        await inbox.handle(origin)
                    except Exception as error:
                        log.error("Could not process inbox message %s (%s).", origin.thread.root_ts, type(error).__name__)

                async def on_request(client, request):
                    if request.envelope_id:
                        await client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))
                    if request.type == "events_api" and (origin := message_from_event(request.payload)):
                        jobs.create_task(handle(origin))

                socket = SocketModeClient(app_token=config.slack_app_token, web_client=web)
                try:
                    socket.socket_mode_request_listeners.append(on_request)
                    try:
                        await socket.connect()
                    except SlackApiError:
                        raise BridgeError("Could not connect to Slack Socket Mode. Check the app token and settings.") from None
                    log.info("Listening in %s. The bot must be invited to the private inbox.", config.inbox_channel_id)
                    await codex.closed.wait()
                finally:
                    await socket.close()
            raise BridgeError("Codex app-server disconnected.")


def message_from_event(payload: dict) -> OriginMessage | None:
    event = payload.get("event", {})
    if not isinstance(event, dict) or event.get("type") != "message" or event.get("thread_ts"):
        return None
    bot_id = event.get("bot_id")
    subtype = event.get("subtype")
    if subtype is not None and (subtype != "bot_message" or not bot_id):
        return None
    workspace = payload.get("team_id")
    event_id = payload.get("event_id")
    channel = event.get("channel")
    ts = event.get("ts")
    prompt = event.get("text")
    if not isinstance(workspace, str) or not isinstance(event_id, str):
        return None
    if not isinstance(channel, str) or not isinstance(ts, str) or not isinstance(prompt, str):
        return None
    if not (workspace and event_id and channel and ts and prompt.strip()):
        return None
    user_id = event.get("user")
    if user_id is not None and not isinstance(user_id, str):
        return None
    if bot_id is not None and (not isinstance(bot_id, str) or not bot_id):
        return None
    return OriginMessage(SlackThread(workspace, channel, ts), event_id, prompt, user_id, bot_id)


class SlackOutput:
    def __init__(self, web: AsyncWebClient):
        self.web = web

    async def started(self, thread: SlackThread) -> None:
        try:
            await self.web.reactions_add(channel=thread.channel_id, timestamp=thread.root_ts, name="eyes")
        except (SlackApiError, ClientError, TimeoutError) as error:
            if isinstance(error, SlackApiError) and error.response.get("error") == "already_reacted":
                return
            log.warning("Could not add the eyes reaction to %s/%s", thread.channel_id, thread.root_ts)

    async def reply(self, thread: SlackThread, text: str) -> None:
        # Escape Slack's special angle-bracket syntax, including group mentions.
        text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        await self.web.chat_postMessage(
            channel=thread.channel_id, thread_ts=thread.root_ts, text=text,
            unfurl_links=False, unfurl_media=False,
        )
