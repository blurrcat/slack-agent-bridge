"""Controllable Codex app server for the Slack-event/stdio integration tests."""

import itertools
import json
import sys
from pathlib import Path

threads = itertools.count(1)
turns = itertools.count(1)
active = {}
pending = {}
modes = {}
held_ack = None
interrupt_acks = {}


def send(message):
    print(json.dumps(message), flush=True)


def completed(thread, turn, text=None, status="completed"):
    event = {"threadId": thread, "turnId": turn["id"]}
    pending.pop(f"approval-for-{turn['id']}", None)
    if text is not None:
        send({"method": "item/completed", "params": {
            **event, "item": {"type": "agentMessage", "text": text},
        }})
    send({"method": "turn/completed", "params": {**event, "turn": {**turn, "status": status}}})


def approval(thread, turn, prompt):
    request_id = f"approval-for-{turn['id']}"
    pending[request_id] = (thread, prompt)
    send({
        "id": request_id, "method": "item/commandExecution/requestApproval",
        "params": {
            "threadId": thread, "turnId": turn["id"], "command": "echo test",
            "reason": "Long justification. " * 250 if prompt == "approve-long" else None,
        },
    })


for line in sys.stdin:
    message = json.loads(line)
    if "method" not in message:
        entry = pending.pop(message["id"], None)
        if entry is None:
            raise AssertionError("response sent for a request that was already resolved")
        thread, prompt = entry
        turn = active.pop(thread)
        if prompt == "question":
            received = message["result"]["answers"]["language"]["answers"][0]
        elif prompt == "secret-question":
            assert "error" in message
            received = "rejected"
        else:
            received = message.get("result", {}).get("decision", "rejected")
        send({"method": "serverRequest/resolved", "params": {
            "threadId": thread, "requestId": message["id"],
        }})
        completed(thread, turn, f"decision: {received}")
        if held_ack is not None:
            first, first_turn, rpc_id = held_ack
            completed(first, first_turn, f"{first}: first prompt acknowledged")
            send({"id": rpc_id, "result": {"turn": first_turn}})
            held_ack = None
        continue
    method = message["method"]
    if method == "initialized":
        continue
    params = message["params"]
    if method == "initialize":
        result = {}
    elif method == "thread/start":
        if len(sys.argv) > 1:
            with Path(sys.argv[1]).open("a") as file:
                file.write("started\n")
        result = {"thread": {"id": f"codex-{next(threads)}"}}
    elif method == "turn/start":
        thread = params["threadId"]
        prompt = params["input"][0]["text"]
        finishing = None
        if (existing := active.pop(thread, None)) is not None:
            if modes[thread] == "finish-before-followup":
                finishing = existing
            else:
                if modes[thread] == "expire-on-steer":
                    request_id = f"approval-for-{existing['id']}"
                    del pending[request_id]
                    send({"method": "serverRequest/resolved", "params": {
                        "threadId": thread, "requestId": request_id,
                    }})
                completed(thread, existing, "steered: " + prompt)
                send({"id": message["id"], "result": {"turn": existing}})
                continue
        turn = {"id": f"turn-{next(turns)}", "status": "inProgress"}
        send({"method": "turn/started", "params": {"threadId": thread, "turn": turn}})
        if prompt == "wait-for-other-approval":
            held_ack = (thread, turn, message["id"])
            send({"method": "item/completed", "params": {
                "threadId": thread, "turnId": turn["id"],
                "item": {"type": "commandExecution", "status": "completed"},
            }})
            approval(thread, turn, prompt)
            continue
        if prompt == "ack-only-after-interrupt":
            interrupt_acks[thread] = message["id"]
            active[thread] = turn
            modes[thread] = prompt
            send({"method": "item/completed", "params": {
                "threadId": thread, "turnId": turn["id"],
                "item": {"type": "commandExecution", "status": "completed"},
            }})
            approval(thread, turn, prompt)
            continue
        if prompt in {"hold", "approve", "approve-long", "question", "secret-question", "finish-before-followup", "expire-on-steer"}:
            active[thread] = turn
            modes[thread] = prompt
            send({"method": "item/completed", "params": {
                "threadId": thread, "turnId": turn["id"],
                "item": {"type": "commandExecution", "status": "completed", "aggregatedOutput": "DO NOT EXPOSE"},
            }})
            if prompt in {"approve", "approve-long", "expire-on-steer", "hold", "finish-before-followup"}:
                approval(thread, turn, prompt)
            elif prompt in {"question", "secret-question"}:
                request_id = f"question-for-{turn['id']}"
                pending[request_id] = (thread, prompt)
                send({
                    "id": request_id, "method": "item/tool/requestUserInput", "params": {
                        "threadId": thread, "turnId": turn["id"], "questions": [{
                            "id": "language", "header": "Language", "question": "Pick a language.",
                            "isSecret": prompt == "secret-question",
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
    elif method == "thread/unsubscribe":
        assert params["threadId"] not in active
        result = {"status": "unsubscribed"}
    elif method == "turn/interrupt":
        thread = params["threadId"]
        if modes[thread] == "approve":
            send({"id": message["id"], "error": {"message": "interrupt rejected", "code": -32600}})
            continue
        turn = active.pop(thread)
        assert params["turnId"] == turn["id"]
        completed(thread, turn, status="interrupted")
        if thread in interrupt_acks:
            send({"id": interrupt_acks.pop(thread), "result": {"turn": turn}})
        result = {}
    else:
        raise AssertionError(method)
    send({"id": message["id"], "result": result})
