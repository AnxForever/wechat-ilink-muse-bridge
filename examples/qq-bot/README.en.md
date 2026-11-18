# QQ variant: the same hook contract, on QQ's official bot platform

[中文](README.md) | [English](README.en.md)

`examples/muse-hook/` connects Muse to WeChat via iLink. This directory connects
the *same* contract to the **QQ official bot platform (bot API v2)**. The Muse
worker contract does not change by a single line: it still sees
`qq-pending.json`, the same `wake`/`silent`, the same claim-and-commit rules.

## What it cannot do — read this first

Three platform facts change what this adapter can promise:

1. **No proactive push.** The platform discontinued proactive messages on
   2025-04-21; calls now fail. WeChat's flagship feature (scheduled reminders,
   unprompted check-ins) has no QQ equivalent. This adapter therefore has no
   `proactive` verb — not an omission, a platform limit.
2. **Passive reply windows are short.** C2C: **60 minutes** after a user message,
   at most **4 replies**. Group: **5 minutes**, at most 5 replies. Miss the
   window or exceed the count and the send fails outright. The worker must be
   fast; in groups especially, it cannot think for minutes.
3. **Production needs a fixed public IP in the whitelist.** The console requires
   your server's egress IP; a dynamic home IP is blocked. **The sandbox does not
   require it**, so develop there — but plan for a fixed IP before going live.

Also: URLs in message content must be pre-registered under *Developer settings →
Message URL configuration* (ICP-filed domain) or the send fails; Markdown needs
separate approval; typing indicators work in C2C only.

## Structure

```
QQ user ──message──> QQ gateway
                        │  WebSocket push (they push; we do not poll)
                        ▼
                  qqbot.py serve       ← resident, holds the gateway connection
                        │  event → inbox/ (tmp→fsync→rename), THEN seq
                        ▼
                  data/inbox/*.json    ← at-least-once: reconnect re-delivers
                        │  qq-watch.sh runs every ~5s
                        ▼
                  qqbot.py poll        ← reads inbox, advances nothing
                        │
                  qq-pending.json      ← atomic write, then commit
                        │  worker claims with an atomic mv → qq-processing.json
                        ▼
                  Muse worker agent    ← rechecks the queue, merges bursts,
                                           sends one composed reply
```

The only structural difference from the WeChat variant: WeChat is long-polled by
us, QQ pushes to us. So "fetch" splits into `serve` (receive) and `poll`
(drain) — and `poll` keeps the exact same contract: **read-only, advancing
nothing, safe to call repeatedly.**

## Why `serve` and `poll` are separate

The platform's Node SDK has a verified defect: it persists `lastSeq` *before*
dispatching and never awaits the async handler. If the process dies mid-handler,
the event was neither handled nor re-delivered — **the SDK's
`sessionPersistence` cannot give you at-least-once.**

This implementation inverts the order: the event goes to `inbox/` first
(tmp file → fsync → rename, atomically visible), and only then is the sequence
persisted. Crash anywhere in between and the gateway replays on reconnect;
`inbox_write()` deduplicates by seq. Worst case is duplicate *processing*
(covered by the worker's idempotency key), never a silent drop.

For the same reason `msg_seq` is not the SDK's random 16-bit value: it is
derived deterministically from the idempotency key's bubble suffix
(`<msg_id>-1`, `<msg_id>-2`), so a retry lands on the **same**
`(msg_id, msg_seq)` pair the server deduplicates. The ledger records that pair
before the call, so a send that succeeded but whose response was lost does not
become a second bubble.

## Setup

```bash
export QQ_APP_ID=<your AppID>
export QQ_APP_SECRET=<your AppSecret>
export QQ_DATA_DIR=$HOME/qq-bot-data      # credentials, queue, inbox
export QQ_SANDBOX=1                        # sandbox API while developing

cp examples/qq-bot/qqbot.py "$QQ_DATA_DIR/"
python3 "$QQ_DATA_DIR/qqbot.py" health     # credentials + receiver status
```

Credentials can also live in `$QQ_DATA_DIR/config.json`
(`{"app_id": ..., "app_secret": ...}`); chmod it 600.
**Everything in `data/` is sensitive — never commit it, never share it.**

## Bind the owner (required, once)

As on WeChat, the owner is **enforced in code**: senders not in `owner.json`
never enter the queue. A fresh bot does not know who that is, so open one
binding window:

```bash
python3 qqbot.py serve &          # start the receiver first
python3 qqbot.py bind 600         # 600s window, first C2C sender wins
```

Message the bot once (C2C) inside the window. On success it prints
`BOUND:<openid>` and sends a one-line confirmation.

Groups are ignored by default. To allow one, list it explicitly:

```json
{"owner_id": "...", "groups": ["<group_openid>", "..."]}
```

## Wiring it into Muse

`qq-watch.sh` is a drop-in structural replacement for `ilink-watch.sh`: the same
two-phase commit, the same pending claim, the same alert state machine, the same
warm wait. Register it as a ~5s hook:

```
script_path = examples/qq-bot/qq-watch.sh
env: QQ_DATA_DIR=..., QQ_HOOK_STATE_DIR=~/.muse/hooks/state
```

You **must** also keep `qqbot.py serve` running: it is the only data source. If
it dies, `poll` reports `POLL_ERROR` and rides the same alert path — it does not
go quietly silent.

Topics mirror the WeChat variant:

| topic | meaning |
|---|---|
| `new_qq_messages` | a batch landed; payload carries `count` / `pending_file` |
| `pending_qq_messages` | the previous batch is still unclaimed |
| `token_expired` | credentials missing or rejected (immediate, then hourly) |
| `not_bound` | no owner yet (immediate, then hourly) |
| `poll_failing` | after 10 consecutive minutes of failure |
| `recovered` | a previously alerted fault cleared (`kind`: token / bind / poll_error) |

## Worker-side differences (only these)

The contract is unchanged; the platforms differ, so the worker needs to know:

- **Replies must carry the msg_id.** QQ only allows passive replies:
  `qqbot.py send <reply_to> <msg_id> - --key <msg_id>-1`.
  `reply_to` is on the batch entry (sender openid for C2C, group_openid for groups).
- **Bubbles per msg_id are capped.** 4 for C2C, 5 for groups (platform
  "message overview"). One bubble past the cap returns `SEND_BUBBLE_CAP`
  without calling the API. The "2–3 short bubbles" guidance is a hard limit
  here, not a style suggestion.
- **Add `--group` for groups.** C2C is the default; channels/DMs use
  `--channel` / `--dm`.
- **No typing indicator in groups.** The platform supports C2C only;
  `qq-watch.sh` skips it automatically.
- **Shorten the warm wait.** Group windows are 5 minutes; set
  `QQ_WARM_IDLE=60`. C2C can keep the 180s default.

## Self-check

```bash
python3 qqbot.py status     # token / gateway session / inbox backlog / ledger
python3 qqbot.py health     # exit 0 ok / 1 warn / 2 bad
```

`serve_heartbeat_age` in `status` is the receiver's liveness proof — the same
role as `ilink-heartbeat` on WeChat: quiet with a fresh heartbeat is a healthy
idle bridge; quiet with a stale heartbeat is a dead receiver.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

Three QQ files:

- `tests/test_qqbot.py` — two-phase poll/commit, owner allowlist, dedupe,
  send idempotency and `msg_seq` derivation
- `tests/test_qq_hook_integration.py` — end-to-end `qq-watch.sh` contract
  (needs `bash` + `jq` + `flock` + `timeout`)
- `tests/test_qqbot_ws.py` — the minimal WebSocket client and the gateway loop,
  against a loopback gateway, pinning the "file first, then seq" order

Git Bash has no `flock`, so the hook integration tests skip on plain Windows;
run them in WSL or on Linux.

## Known limits

- No proactive push (point 1 above). Do not design around it.
- Media is **inbound only** (downloaded to `media/`, surfaced as `media_path`);
  outbound media is not implemented — `send` is text-only.
- C2C openids are per-bot; changing bots means re-binding.
- The platform audits message content (`MESSAGE_AUDIT`); this adapter does not
  subscribe to those events yet.
