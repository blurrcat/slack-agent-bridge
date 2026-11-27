"""Controllable Codex app server for the Slack-event/stdio integration tests."""

import itertools
import json
import sys


threads = itertools.count(1)


def send(message):
    print(json.dumps(message), flush=True)


for line in sys.stdin:
    message = json.loads(line)
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
        turn = {"id": f"turn-{thread}", "status": "completed"}
        event = {"threadId": thread, "turnId": turn["id"]}
        # Notifications can arrive before the turn/start response.
        send({"method": "item/completed", "params": {
            **event,
            "item": {"type": "agentMessage", "text": f"{thread}: {params['input'][0]['text']}"},
        }})
        send({"method": "turn/completed", "params": {**event, "turn": turn}})
        result = {"turn": turn}
    else:
        raise AssertionError(method)
    send({"id": message["id"], "result": result})
