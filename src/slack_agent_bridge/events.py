from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class SlackEvent:
    pass


@dataclass(frozen=True)
class SlackThread:
    workspace_id: str
    channel_id: str
    root_ts: str


@dataclass(frozen=True)
class SlackMessage(SlackEvent):
    thread: SlackThread
    event_id: str
    message_ts: str
    text: str
    user_id: str | None = None
    bot_id: str | None = None
    subtype: str | None = None


@dataclass(frozen=True)
class SlackMessagePosted(SlackMessage):
    pass


@dataclass(frozen=True)
class SlackReplyPosted(SlackMessage):
    pass


@dataclass(frozen=True)
class SlackReaction(SlackEvent):
    workspace_id: str
    event_id: str
    channel_id: str
    message_ts: str
    user_id: str
    emoji: str


@dataclass(frozen=True)
class SlackReactionAdded(SlackReaction):
    pass


@dataclass(frozen=True)
class SlackReactionRemoved(SlackReaction):
    pass


@dataclass(frozen=True)
class SlackInteractionReceived(SlackEvent):
    workspace_id: str
    user_id: str
    action_id: str
    value: str
    channel_id: str | None = None
    message_ts: str | None = None
    trigger_id: str | None = None
    input_values: tuple[tuple[str, str, str], ...] = ()


@dataclass(frozen=True)
class SlackApprovalCardPosted(SlackEvent):
    token: str
    thread: SlackThread
    message_ts: str


@dataclass(frozen=True)
class SlackApprovalCardPostFailed(SlackEvent):
    token: str


@dataclass(frozen=True)
class SlackOriginLookedUp(SlackEvent):
    thread: SlackThread
    origin: SlackMessagePosted | None


@dataclass(frozen=True)
class SlackQuestionsOpenFailed(SlackEvent):
    token: str


@dataclass(frozen=True)
class HarnessEvent:
    pass


@dataclass(frozen=True)
class HarnessTurnStarted(HarnessEvent):
    session_id: str
    turn_id: str
    observed_at_ns: int


@dataclass(frozen=True)
class HarnessItemCompleted(HarnessEvent):
    session_id: str
    turn_id: str
    kind: str
    status: str | None = None
    text: str | None = None


@dataclass(frozen=True)
class HarnessTurnCompleted(HarnessEvent):
    session_id: str
    turn_id: str
    status: str


@dataclass(frozen=True)
class HarnessSessionStopped(HarnessEvent):
    session_id: str


@dataclass(frozen=True)
class HarnessCommandFailed(HarnessEvent):
    session_id: str
    command_id: str
    operation: Literal["prompt", "interrupt", "stop"]
    turn_id: str | None = None


@dataclass(frozen=True)
class HarnessRequestKey:
    session_id: str
    turn_id: str
    request_id: int | str


ApprovalDecision = Literal["accept", "acceptForSession", "decline"]
InputAnswers = tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class HarnessApproval:
    kind: str
    details: tuple[tuple[str, str], ...]
    available_decisions: tuple[str, ...] | None = None


@dataclass(frozen=True)
class HarnessQuestion:
    id: str
    label: str
    text: str
    options: tuple[str, ...]
    allow_other: bool
    secret: bool

    @property
    def requires_choice(self) -> bool:
        return bool(self.options) and not self.allow_other


@dataclass(frozen=True)
class HarnessQuestions:
    questions: tuple[HarnessQuestion, ...]


@dataclass(frozen=True)
class HarnessRequestReceived(HarnessEvent):
    key: HarnessRequestKey
    request: HarnessApproval | HarnessQuestions | None


@dataclass(frozen=True)
class HarnessRequestResolved(HarnessEvent):
    key: HarnessRequestKey
