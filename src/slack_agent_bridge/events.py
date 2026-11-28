"""Immutable messages exchanged by the Bridge and its transport adapters."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class SlackEvent:
    """Base for observations from SlackInbox and results from SlackOutbox."""
    pass


@dataclass(frozen=True)
class SlackThread:
    """Stable Slack routing identity, independent of whichever Agent Session is attached."""
    workspace_id: str
    channel_id: str
    root_ts: str


@dataclass(frozen=True)
class SlackMessage(SlackEvent):
    """Common Slack message metadata; event IDs allow deduplication."""
    thread: SlackThread
    event_id: str
    message_ts: str
    text: str
    user_id: str | None = None
    bot_id: str | None = None
    subtype: str | None = None


@dataclass(frozen=True)
class SlackMessagePosted(SlackMessage):
    """A root message, not yet an authorized Origin Message."""
    pass


@dataclass(frozen=True)
class SlackReplyPosted(SlackMessage):
    """A threaded reply, not yet an authorized prompt or command."""
    pass


@dataclass(frozen=True)
class SlackReaction(SlackEvent):
    """A reaction targeting a specific Slack message."""
    workspace_id: str
    event_id: str
    channel_id: str
    message_ts: str
    user_id: str
    emoji: str


@dataclass(frozen=True)
class SlackReactionAdded(SlackReaction):
    """A Slack user added an emoji to a message."""
    pass


@dataclass(frozen=True)
class SlackReactionRemoved(SlackReaction):
    """A Slack user removed an emoji from a message."""
    pass


@dataclass(frozen=True)
class SlackInteractionReceived(SlackEvent):
    """An untrusted card click or modal submission."""
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
    """Confirmation Slack created the card at a specific timestamp."""
    token: str
    thread: SlackThread
    message_ts: str


@dataclass(frozen=True)
class SlackApprovalCardPostFailed(SlackEvent):
    """The card did not get a usable Slack timestamp."""
    token: str


@dataclass(frozen=True)
class SlackOriginLookedUp(SlackEvent):
    """Result of verifying a potential Session Thread's root in Slack history."""
    thread: SlackThread
    origin: SlackMessagePosted | None


@dataclass(frozen=True)
class SlackQuestionsOpenFailed(SlackEvent):
    """Slack did not open the requested input modal."""
    token: str


@dataclass(frozen=True)
class HarnessEvent:
    """Base for multiplexed observations from the Codex app-server."""
    pass


@dataclass(frozen=True)
class HarnessTurnStarted(HarnessEvent):
    """The first observation that this turn exists, not a prompt acknowledgment."""
    session_id: str
    turn_id: str
    observed_at_ns: int


@dataclass(frozen=True)
class HarnessItemCompleted(HarnessEvent):
    """A completed item; only agent messages carry text."""
    session_id: str
    turn_id: str
    kind: str
    status: str | None = None
    text: str | None = None


@dataclass(frozen=True)
class HarnessTurnCompleted(HarnessEvent):
    """Terminal status for exactly one turn."""
    session_id: str
    turn_id: str
    status: str


@dataclass(frozen=True)
class HarnessSessionStopped(HarnessEvent):
    """The Harness confirmed that the session has no remaining work."""
    session_id: str


@dataclass(frozen=True)
class HarnessCommandFailed(HarnessEvent):
    """A cast could not be confirmed; the ID identifies the original command."""
    session_id: str
    command_id: str
    operation: Literal["prompt", "interrupt", "stop"]
    turn_id: str | None = None


@dataclass(frozen=True)
class HarnessRequestKey:
    """Scope a protocol request to the session and turn that issued it."""
    session_id: str
    turn_id: str
    request_id: int | str


ApprovalDecision = Literal["accept", "acceptForSession", "decline"]
InputAnswers = tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class HarnessApproval:
    """Typed approval details and options, not an authorization decision."""
    kind: str
    details: tuple[tuple[str, str], ...]
    available_decisions: tuple[str, ...] | None = None


@dataclass(frozen=True)
class HarnessQuestion:
    """Description of one Codex question, including whether Slack may display it."""
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
    """An immutable group of questions received in one request."""
    questions: tuple[HarnessQuestion, ...]


@dataclass(frozen=True)
class HarnessRequestReceived(HarnessEvent):
    """A pending Codex request; no supported presentation is represented by None."""
    key: HarnessRequestKey
    request: HarnessApproval | HarnessQuestions | None


@dataclass(frozen=True)
class HarnessRequestResolved(HarnessEvent):
    """The request ended, whether answered or cleared."""
    key: HarnessRequestKey
