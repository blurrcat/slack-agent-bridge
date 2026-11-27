# Keep Codex setup outside the bridge

The bridge is a Slack interface to the operator's coding harness, not an execution environment. The operator chooses the host and supplies Codex launch arguments in local configuration; that host supplies the toolchain, access, and workspace for the job. The bridge routes Slack threads over stdio without needing to know whether the host is set up for iOS development, analytics, or something else.
