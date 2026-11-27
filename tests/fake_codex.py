"""Controllable Codex app server for the Slack-event/stdio integration tests."""

import itertools
import json
import sys

restart = sys.argv[1] if len(sys.argv) > 1 else None
threads = itertools.count(2 if restart else 1)
loaded = set()
recovered = set()
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
        thread = f"codex-{next(threads)}"
        loaded.add(thread)
        result = {"thread": {"id": thread}}
    elif method == "thread/resume":
        thread = params["threadId"]
        if restart == "crash":
            sys.exit(1)
        if restart != "resume" or thread != "codex-1":
            send({"id": message["id"], "error": {
                "code": -32600, "message": f"no rollout found for thread id {thread}",
            }})
            continue
        loaded.add(thread)
        result = {"thread": {"id": thread}}
    elif method == "turn/start":
        thread = params["threadId"]
        assert thread in loaded
        prompt = params["input"][0]["text"]
        if prompt.startswith("You are continuing an existing Slack thread"):
            assert "TWORKSPACE" in prompt and "CINBOX" in prompt
            assert "1.000000" in prompt and "3.000000" in prompt
            assert "Slack MCP" in prompt and "Do not replay" in prompt
            assert "recover my session" not in prompt and "do not run" not in prompt
            turn = {"id": f"turn-{next(turns)}", "status": "inProgress"}
            if restart == "missing":
                recovered.add(thread)
                answer = "SLACK_CONTEXT_READY"
            elif restart == "history-failure":
                answer = "SLACK_CONTEXT_UNAVAILABLE"
            else:
                answer = "I could only read some of the history."
            completed(thread, turn, answer)
            send({"id": message["id"], "result": {"turn": turn}})
            continue
        if restart == "missing":
            assert thread in recovered
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
