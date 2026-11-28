"""Controllable Codex app server for the Slack-event/stdio integration tests."""

import itertools
import json
import sys
import time
from pathlib import Path

assert sys.argv[-2:] == ["app-server", "--stdio"]
args = sys.argv[1:-2]
mode = args[1] if len(args) > 1 else ""
threads = itertools.count(1 if not mode.startswith("missing") else 2)
turns = itertools.count(1)
active = {}
pending = {}
modes = {}
held_ack = None
requested_permissions = {"network": {"enabled": True}}


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
        "id": request_id,
        "method": "item/permissions/requestApproval" if prompt == "permissions" else "item/commandExecution/requestApproval",
        "params": {
            "threadId": thread, "turnId": turn["id"],
            **({"permissions": requested_permissions} if prompt == "permissions" else {"command": "echo test"}),
            "reason": "Long justification. " * 250 if prompt == "approve-long" else None,
        },
    })


def wait_for_gate(suffix):
    gate = Path(args[0]).with_suffix(suffix)
    deadline = time.monotonic() + 4
    while not gate.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"missing test gate {suffix}")
        time.sleep(0.005)


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
        elif prompt == "permissions":
            result = message["result"]
            assert result["permissions"] == requested_permissions
            received = result["scope"]
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
        if mode == "stderr":
            print("test Codex diagnostic", file=sys.stderr, flush=True)
        result = {}
    elif method == "thread/start":
        if args:
            with Path(args[0]).open("a") as file:
                file.write("started\n")
        result = {"thread": {"id": f"codex-{next(threads)}"}}
    elif method == "thread/resume":
        thread_id = params["threadId"]
        if mode == "resume-wrong-id":
            send({"id": message["id"], "error": {
                "code": -32600, "message": "no rollout found for thread id other-thread",
            }})
            continue
        if mode.startswith("missing"):
            send({"id": message["id"], "error": {
                "code": -32600, "message": f"invalid thread-store request: no rollout found for thread id {thread_id}",
            }})
            continue
        result = {"thread": {"id": thread_id}}
    elif method == "turn/start":
        thread = params["threadId"]
        prompt = params["input"][0]["text"]
        if prompt == "reject-prompt":
            send({"id": message["id"], "error": {"code": -32600, "message": "prompt rejected"}})
            continue
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
        if prompt == "pause-wire-for-expiry":
            approval(thread, turn, prompt)
            send({"id": message["id"], "result": {"turn": turn}})
            wait_for_gate(".expire")
            request_id = f"approval-for-{turn['id']}"
            del pending[request_id]
            send({"method": "serverRequest/resolved", "params": {
                "threadId": thread, "requestId": request_id,
            }})
            wait_for_gate(".resume")
            continue
        if prompt.startswith("large:"):
            completed(thread, turn, "large prompt accepted")
            send({"id": message["id"], "result": {"turn": turn}})
            continue
        if prompt == "wait-for-other-approval":
            held_ack = (thread, turn, message["id"])
            send({"method": "item/completed", "params": {
                "threadId": thread, "turnId": turn["id"],
                "item": {"type": "commandExecution", "status": "completed"},
            }})
            approval(thread, turn, prompt)
            continue
        if prompt == "no-prompt-receipt":
            active[thread] = turn
            modes[thread] = prompt
            send({"method": "item/completed", "params": {
                "threadId": thread, "turnId": turn["id"],
                "item": {"type": "commandExecution", "status": "completed"},
            }})
            approval(thread, turn, prompt)
            continue
        if "SLACK_CONTEXT_READY" in prompt:
            if mode == "missing-history-stalled":
                active[thread] = turn
                modes[thread] = mode
                send({"id": message["id"], "result": {"turn": turn}})
                continue
            text = ("history not available" if mode == "missing-history-failure" else "SLACK_CONTEXT_READY")
            completed(thread, turn, text)
            send({"id": message["id"], "result": {"turn": turn}})
            continue
        if prompt in {"hold", "approve", "approve-long", "permissions", "question", "secret-question", "finish-before-followup", "expire-on-steer"}:
            active[thread] = turn
            modes[thread] = prompt
            send({"method": "item/completed", "params": {
                "threadId": thread, "turnId": turn["id"],
                "item": {"type": "commandExecution", "status": "completed", "aggregatedOutput": "DO NOT EXPOSE"},
            }})
            if prompt in {"approve", "approve-long", "permissions", "expire-on-steer", "hold", "finish-before-followup"}:
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
        if args:
            with Path(args[0]).open("a") as file:
                file.write("unsubscribe\n")
        continue  # Unsubscribe does not need to acknowledge a cast.
    elif method == "turn/interrupt":
        if args:
            with Path(args[0]).open("a") as file:
                file.write("interrupt\n")
        thread = params["threadId"]
        turn = active.pop(thread)
        assert params["turnId"] == turn["id"]
        completed(thread, turn, status="interrupted")
        continue  # The terminal event, not an RPC reply, tells the Bridge what happened.
    else:
        raise AssertionError(method)
    send({"id": message["id"], "result": result})
