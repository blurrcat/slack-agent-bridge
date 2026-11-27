import asyncio
import logging
import re
from pathlib import Path

from aiohttp import ClientError
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode.aiohttp import SocketModeClient
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.web.async_client import AsyncWebClient

from .codex import Codex
from .config import Config
from .controls import Control, Interaction
from .inbox import Inbox, SessionStore, SlackMessage, SlackThread


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
                async def handle(origin: SlackMessage) -> None:
                    try:
                        await inbox.handle(origin)
                    except Exception as error:
                        log.error("Could not process inbox message %s (%s).", origin.thread.root_ts, type(error).__name__)

                async def interact(interaction: Interaction) -> None:
                    try:
                        await inbox.interact(interaction)
                    except Exception as error:
                        log.error("Could not process a Slack control (%s).", type(error).__name__)

                async def on_request(client, request):
                    if request.envelope_id:
                        await client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))
                    if request.type == "events_api" and (origin := message_from_event(request.payload)):
                        jobs.create_task(handle(origin))
                    elif request.type == "interactive" and (action := interaction_from_payload(request.payload)):
                        jobs.create_task(interact(action))

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


def message_from_event(payload: dict) -> SlackMessage | None:
    event = payload.get("event", {})
    if not isinstance(event, dict) or event.get("type") != "message":
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
    root_ts = event.get("thread_ts") or ts
    if not isinstance(root_ts, str) or not all(re.fullmatch(r"[0-9]+\.[0-9]+", item) for item in (root_ts, ts)):
        return None
    return SlackMessage(SlackThread(workspace, channel, root_ts), event_id, ts, prompt, user_id, bot_id)


def interaction_from_payload(payload: dict) -> Interaction | None:
    workspace = (payload.get("team") or {}).get("id") or payload.get("app_installed_team_id")
    user = (payload.get("user") or {}).get("id")
    if not isinstance(workspace, str) or not isinstance(user, str):
        return None
    if payload.get("type") == "view_submission":
        view = payload.get("view") or {}
        token = view.get("private_metadata")
        if view.get("callback_id") == "sb_questions" and isinstance(token, str):
            values = (view.get("state") or {}).get("values") or {}
            return Interaction(workspace, user, token, "submit", values=values)
    if payload.get("type") == "block_actions":
        actions = payload.get("actions") or []
        if len(actions) != 1:
            return None
        action = actions[0]
        name = {
            "sb_approve": "approve",
            "sb_approve_session": "approve_session",
            "sb_decline": "decline",
            "sb_answer": "answer",
        }.get(action.get("action_id"))
        token = action.get("value")
        if name and isinstance(token, str):
            container = payload.get("container") or {}
            return Interaction(
                workspace, user, token, name,
                container.get("channel_id"), container.get("message_ts"), payload.get("trigger_id"),
            )
    return None


class SlackOutput:
    def __init__(self, web: AsyncWebClient):
        self.web = web

    async def started(self, thread: SlackThread) -> None:
        await self._reaction(thread, thread.root_ts, "eyes", True)

    async def closed(self, thread: SlackThread) -> None:
        await self._reaction(thread, thread.root_ts, "white_check_mark", True)

    async def working(self, thread: SlackThread, message_ts: str, active: bool) -> None:
        await self._reaction(thread, message_ts, "hourglass_flowing_sand", active)

    async def reply(self, thread: SlackThread, text: str) -> None:
        # Escape Slack's special angle-bracket syntax, including group mentions.
        text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        await self.web.chat_postMessage(
            channel=thread.channel_id, thread_ts=thread.root_ts, text=text,
            unfurl_links=False, unfurl_media=False,
        )


    async def show_control(self, thread: SlackThread, control: Control) -> str | None:
        def button(label, action, style=None):
            item = {
                "type": "button", "text": plain(label),
                "action_id": f"sb_{action}", "value": control.token,
            }
            if style:
                item["style"] = style
            return item

        buttons = (
            [button("Answer", "answer")]
            if control.kind == "input"
            else [
                button("Approve once", "approve", "primary"),
                button("Approve for session", "approve_session"),
                button("Decline", "decline"),
            ]
        )
        try:
            response = await self.web.chat_postMessage(
                channel=thread.channel_id, thread_ts=thread.root_ts,
                text="Codex needs the bridge owner's input.",
                blocks=[
                    {"type": "section", "text": plain(control.details)},
                    {"type": "actions", "elements": buttons},
                ],
                unfurl_links=False, unfurl_media=False,
            )
        except (SlackApiError, ClientError, TimeoutError) as error:
            log.warning("Could not display a Codex request (%s).", type(error).__name__)
            return None
        ts = response["ts"]
        return ts if isinstance(ts, str) else None

    async def open_questions(self, control: Control, trigger_id: str) -> None:
        blocks = []
        for index, question in enumerate(control.questions):
            text = question.text
            element = {"type": "plain_text_input", "action_id": "answer", "max_length": 1000}
            if question.requires_choice:
                element = {
                    "type": "static_select", "action_id": "answer",
                    "options": [{"text": plain(label), "value": label} for label in question.options],
                }
            elif question.options:
                text += "\nOptions: " + ", ".join(question.options)
            blocks.extend([
                {"type": "section", "text": plain(text)},
                {"type": "input", "block_id": f"q{index}", "label": plain(question.label[:200] or "Answer"),
                 "element": element},
            ])
        await self.web.views_open(trigger_id=trigger_id, view={
            "type": "modal", "callback_id": "sb_questions", "private_metadata": control.token,
            "title": plain("Codex questions"), "submit": plain("Send to Codex"), "blocks": blocks,
        })

    async def _reaction(self, thread: SlackThread, ts: str, name: str, add: bool) -> None:
        change = self.web.reactions_add if add else self.web.reactions_remove
        try:
            await change(channel=thread.channel_id, timestamp=ts, name=name)
        except (SlackApiError, ClientError, TimeoutError) as error:
            if isinstance(error, SlackApiError) and error.response.get("error") in {"already_reacted", "no_reaction"}:
                return
            log.warning("Could not change %s reaction on %s/%s", name, thread.channel_id, ts)


def plain(text: str) -> dict:
    return {"type": "plain_text", "text": text}
