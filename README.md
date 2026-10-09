# DotBot

Connect the same existing dot to a QR-bound owner's WeChat conversation through iLink, an owner-only local broker and MCP Events. This repository does not create a replacement assistant.

This first migration commit preserves the historical text implementation. The source directory had no Git history; this is a source baseline, not an imported historical commit.

Run offline tests with `python3 -B -m unittest -v`. Do not start the broker/tunnel against private runtime until the owner approves deployment. Credentials, private chats, media, logs and machine-specific tunnel configuration are excluded.
