from __future__ import annotations

import json
import secrets
from dataclasses import dataclass, field


def control_for_request(request: dict) -> Control | None:
    method = request["method"]
    request_id = request["request_id"]
    if method == "item/tool/requestUserInput":
        questions = tuple(
            Question(
                q["id"], q["header"], q["question"],
                tuple(option["label"] for option in q.get("options") or []),
                bool(q.get("isOther")),
            )
            for q in request["questions"] if not q.get("isSecret")
        )
        if not questions or len(questions) != len(request["questions"]) or len(questions) > 10:
            return None
        if any(
            len(q.text) + sum(len(o) + 2 for o in q.options) > 2800
            or len(q.options) > 100
            for q in questions
        ):
            return None
        if any(q.requires_choice and any(not 0 < len(o) <= 75 for o in q.options) for q in questions):
            return None
        return Control(request_id, "input", "Codex needs your input.", questions)

    kinds = {
        "item/commandExecution/requestApproval": "command",
        "item/fileChange/requestApproval": "file changes",
        "item/permissions/requestApproval": "permissions",
    }
    if not (kind := kinds.get(method)):
        return None
    fields = (
        ("Command", "command"),
        ("Working directory", "cwd"),
        ("Requested root", "grantRoot"),
        ("Network request", "networkApprovalContext"),
        ("Permissions", "permissions"),
        ("Reason", "reason"),
    )
    lines = [f"Codex requests approval for {kind}."]
    for label, name in fields:
        if (value := request.get(name)) is not None:
            lines.append(f"{label}: {value if isinstance(value, str) else json.dumps(value)}")
    details = "\n".join(lines)
    if len(details) > 2900:
        return None  # Don't offer approval for a request we can't display in full.
    if kind == "command" and not (request.get("command") or request.get("networkApprovalContext")):
        return None
    return Control(request_id, kind, details, permissions=request.get("permissions") or {})


def result_for_interaction(control: Control, interaction: Interaction) -> dict | None:
    if control.kind == "input":
        if interaction.action != "submit":
            return None
        answers = {}
        for index, question in enumerate(control.questions):
            selected = interaction.values.get(f"q{index}", {}).get("answer", {})
            answer = selected.get("value") or (selected.get("selected_option") or {}).get("value")
            if not isinstance(answer, str) or not answer.strip():
                return None
            if question.requires_choice and answer not in question.options:
                return None
            answers[question.id] = {"answers": [answer]}
        return {"answers": answers}

    if interaction.action not in {"approve", "decline"}:
        return None
    approve = interaction.action == "approve"
    if control.kind == "permissions":
        return {"permissions": control.permissions if approve else {}, "scope": "turn"}
    return {"decision": "accept" if approve else "decline"}

@dataclass(frozen=True)
class Control:
    """One Slack control for a specific live Codex request."""

    request_id: int | str
    kind: str
    details: str
    questions: tuple[Question, ...] = ()
    permissions: dict = field(default_factory=dict, repr=False)
    token: str = field(default_factory=lambda: secrets.token_urlsafe(18))


@dataclass(frozen=True)
class Question:
    id: str
    label: str
    text: str
    options: tuple[str, ...]
    allow_other: bool

    @property
    def requires_choice(self) -> bool:
        return bool(self.options) and not self.allow_other


@dataclass(frozen=True)
class Interaction:
    workspace_id: str
    user_id: str
    token: str
    action: str
    channel_id: str | None = None
    message_ts: str | None = None
    trigger_id: str | None = None
    values: dict = field(default_factory=dict, repr=False)
