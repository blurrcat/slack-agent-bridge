---
status: superseded by ADR-0004
---

# Use Code Channel membership for follow-ups

Only the owner can start an agent session, answer its interactive requests, or end it. Anyone who can post in its Code Channel can prompt or steer the agent. Code Channels start private; the owner manages membership and visibility in Slack.

Same-workspace collaboration is supported. Slack Connect (cross-workspace shared channels) is out of scope. Adding participants or making a Code Channel public can expose its transcript and any information the agent retrieves with the owner's configured tools.
