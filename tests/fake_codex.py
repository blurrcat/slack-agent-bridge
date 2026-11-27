"""Controllable Codex app server for the Slack-event/stdio integration tests."""

import itertools
import json
import sys

threads = itertools.count(1)
turns = itertools.count(1)
active = {}
pending = {}
modes = {}


def send(message):
    print(json.dumps(message), flush=True)


def completed(thread, turn, text=None, status="completed"):
    event = {"threadId": thread, "turnId": turn["id"]}
    if text is not None:
        send({"method": "item/completed", "params": {
            **event, "item": {"type": "agentMessage", "text": text},
        }})
    send({"method": "turn/completed", "params": {**event, "turn": {**turn, "status": status}}})


for line in sys.stdin:
    message = json.loads(line)
    if "method" not in message:
        thread, prompt = pending.pop(message["id"])
        turn = active.pop(thread)
        if prompt == "question":
            received = message["result"]["answers"]["language"]["answers"][0]
        else:
            received = message.get("result", {}).get("decision", "rejected")
        completed(thread, turn, f"decision: {received}")
        continue
    method = message["method"]
    if method == "initialized":
        continue
    params = message["params"]
    if method == "initialize":
        result = {}
    elif method == "thread/start":
        result = {"thread": {"id": f"codex-{next(threads)}"}}
    elif method == "turn/start":
        thread = params["threadId"]
        prompt = params["input"][0]["text"]
        finishing = None
        if (existing := active.pop(thread, None)) is not None:
            if modes[thread] == "finish-before-followup":
                finishing = existing
            else:
                completed(thread, existing, "steered: " + prompt)
                send({"id": message["id"], "result": {"turn": existing}})
                continue
        turn = {"id": f"turn-{next(turns)}", "status": "inProgress"}
        if prompt in {"hold", "approve", "approve-long", "question", "finish-before-followup"}:
            active[thread] = turn
            modes[thread] = prompt
            send({"method": "item/completed", "params": {
                "threadId": thread, "turnId": turn["id"],
                "item": {"type": "commandExecution", "status": "completed", "aggregatedOutput": "DO NOT EXPOSE"},
            }})
            if prompt in {"approve", "approve-long"}:
                request_id = f"approval-for-{turn['id']}"
                pending[request_id] = (thread, prompt)
                send({
                    "id": request_id, "method": "item/commandExecution/requestApproval",
                    "params": {
                        "threadId": thread, "turnId": turn["id"], "command": "echo test",
                        "reason": "Long justification. " * 250 if prompt == "approve-long" else None,
                    },
                })
            elif prompt == "question":
                request_id = f"question-for-{turn['id']}"
                pending[request_id] = (thread, prompt)
                send({
                    "id": request_id, "method": "item/tool/requestUserInput", "params": {
                        "threadId": thread, "turnId": turn["id"], "questions": [{
                            "id": "language", "header": "Language", "question": "Pick a language.",
                            "options": [{"label": "Python", "description": "Python"}],
                        }],
                    },
                })
        else:
            # Notifications can arrive before the turn/start response.
            completed(thread, turn, f"{thread}: {prompt}")
        if finishing is not None:
            # Both turns' terminal notifications can be in flight before the receipt.
            completed(thread, finishing, "finished before follow-up")
        result = {"turn": turn}
    elif method == "turn/interrupt":
        thread = params["threadId"]
        if modes[thread] == "approve":
            send({"id": message["id"], "error": {"message": "interrupt rejected", "code": -32600}})
            continue
        turn = active.pop(thread)
        assert params["turnId"] == turn["id"]
        completed(thread, turn, status="interrupted")
        result = {}
    else:
        raise AssertionError(method)
    send({"id": message["id"], "result": result})
