# Source provenance

The baseline comes from the historical `mcp_events_prototype` source inspected on 2026-10-09, originally developed on 2026-09-30. The original directory and its parents were not Git repositories. There is no historical Git commit series to import; no historical commits are invented.

The explicitly requested `pur1fying/DotBot` remote was empty at clone time. `a828567` preserves the historical owner-only text source and 68 passing offline tests. A subsequent feature commit contains the media, local TTS and public documentation changes. Both commits are new migration commits; original source and the existing live service are preserved.

Excluded from both commits: runtime/binding credentials, private message databases, callback/tunnel connection snapshots, private status/history documents, generated or private media, logs, cache and machine-specific launch configuration. Repository launch templates resolve their own directory. A Git push does not enable media permissions or deploy/restart the existing service.

Validation and sensitive-file checks run before the feature commit and push. Full local validation logs are kept outside tracked source. The extension regression suite includes the original text/event/proxy tests, synthetic encrypted media transfer, actual temporary Unix IPC/stdio, and local eSpeak NG synthesis. Online Plugin discovery/refresh, dot media understanding and mobile receipt remain separate tests.
