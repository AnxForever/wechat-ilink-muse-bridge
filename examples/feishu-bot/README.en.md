# Feishu variant: the same hook contract, on a Feishu custom app

[中文](README.md) | [English](README.en.md)

`examples/muse-hook/` connects Muse to WeChat via iLink; `examples/qq-bot/`
connects it to QQ's official bot platform. This directory connects the *same*
contract to a **Feishu (Lark) custom app**. The Muse worker contract does not
change: it still sees a pending queue, the same `wake`/`silent`, the same
claim-and-commit rules — only the topic names and the id prefixes differ.

## What it cannot do — read this first

These platform facts change what this adapter can promise:

1. **No typing indicator.** Feishu has no bot-facing "typing" API (QQ has one
   for C2C). This adapter ships no `typing` verb, and the worker must not
   pretend it lit one.
2. **Groups deliver only messages that @ the bot.** That is a permission
   boundary, not a bug: `im.message.receive_v1` in groups needs an
   `im:message.group_at_msg`-class permission, and that scope only delivers
   @-mentions. Receiving every group message is a sensitive permission
   (`im:message.group_msg`) you should avoid. The adapter strips the
   `@_user_1` prefix from the text.
3. **A push must be acked within 3 seconds.** Otherwise the platform counts it
   as failed and retries after 15 s, 5 min, 1 h and 6 h (4 retries), then
   drops it. `feishubot.py serve` owns this: it persists the event first and
   acks second, which takes milliseconds.
4. **At-least-once, not exactly-once.** The platform redelivers, and the docs
   say explicitly to dedupe on **`message_id`**, not `event_id`. This adapter
   dedupes on `message_id` both when persisting and when polling.
5. **Rate limits.** 5 QPS per user or group, 1000/minute per app. Replies retry
   once on a rate-limit code; there is no exponential backoff queue.
6. **Proactive messages are bounded by the app's availability scope.** With an
   empty ctx, `send` uses the create endpoint, which only reaches users/groups
   inside that scope. Outside it, the call fails — it does not vanish silently.

What Feishu makes *easier* than QQ: **the long connection needs no public IP,
no domain, no tunnel.** Authentication happens once when the connection is
established; event pushes on the wire are plaintext (no Encrypt Key /
Verification Token — which also means no webhook-style signature check in long
connection mode). That is why this adapter uses the long connection rather than
a webhook: a home machine is enough.

## Structure

```
Feishu user ──message──> Feishu open platform
                            │  long-connection push (we dial; they push)
                            ▼
                      feishubot.py serve    ← resident, holds the connection
                            │  event → inbox/ (tmp→fsync→rename), THEN ack
                            ▼
                      data/inbox/*.json     ← at-least-once: unacked is redelivered
                            │  feishu-watch.sh runs every ~5s
                            ▼
                      feishubot.py poll     ← reads inbox, advances nothing
                            │
                      feishu-pending.json   ← atomic write, then commit
                            │  worker claims with an atomic mv → feishu-processing.json
                            ▼
                      Muse worker agent     ← rechecks the queue, merges bursts,
                                              sends one composed reply
```

The only structural difference from the WeChat variant is who dials: WeChat is
long-polled by us, Feishu gets a connection from us that the platform pushes
down. So "fetch" splits into `serve` (receive) and `poll` (drain) — and `poll`
keeps the exact same contract: **read-only, advancing nothing, safe to call
repeatedly.**

## Why hand-roll the long connection instead of the official SDK

`feishubot.py` is standard library only: `urllib` for REST, `socket` + `ssl`
for a minimal RFC 6455 client, and about twenty lines of hand-written protobuf
for the `pbbp2.Frame` wire format. That buys three things:

- like `ilink.py` / `qqbot.py`, deployment needs no `pip install`;
- ack timing is ours to choose (the SDK runs the handler synchronously before
  acking, so a slow handler eats into the 3-second budget; our ack waits for
  one fsync);
- the dedupe key is the one we chose (`message_id`), not whatever the SDK's
  dispatch layer does.

The cost is tracking protocol changes ourselves. `tests/test_feishubot_ws.py`
contains an **independent** codec and a loopback server, so "the client agrees
with itself" cannot make the tests pass.

## Setup

1. Create a **custom app** in the [Feishu open platform](https://open.feishu.cn)
   and note its `App ID` / `App Secret`.
2. Enable the bot capability and subscribe to `im.message.receive_v1`.
   Permissions at minimum: `im:message.p2p_msg:readonly` (DMs),
   `im:message.group_at_msg:readonly` (group @-mentions),
   `im:message:send_as_bot` (send as the bot).
3. **Choose "long connection" as the event delivery mode.** Do not configure a
   webhook — that is what would require a public address.
4. Publish a version and set the availability scope to include yourself.

```bash
export FEISHU_APP_ID=<your App ID>
export FEISHU_APP_SECRET=<your App Secret>
export FEISHU_DATA_DIR=$HOME/feishu-bot-data    # credentials, queue, inbox

cp examples/feishu-bot/feishubot.py "$FEISHU_DATA_DIR/"
python3 "$FEISHU_DATA_DIR/feishubot.py" health  # credentials + receiver status
```

Credentials can also live in `$FEISHU_DATA_DIR/config.json`
(`{"app_id": ..., "app_secret": ...}`); chmod it 600.
**Everything in `data/` is sensitive — never commit it, never share it.**

International Lark tenants set `FEISHU_DOMAIN=https://open.larksuite.com`;
everything else is identical.

## Bind the owner (required, once)

As on WeChat and QQ, the owner is **enforced in code**: senders not in
`owner.json` never enter the queue. A fresh deployment does not know who that
is, so open one binding window:

```bash
python3 feishubot.py serve &          # start the receiver first
python3 feishubot.py bind 600         # 600s window, first DM sender wins
```

Message the bot once (DM) inside the window. On success it prints
`BOUND:<open_id>` and sends a one-line confirmation — as a *reply* to the
binding message, so it does not consume a proactive-message slot.

Groups are ignored by default. To allow one, list it explicitly:

```json
{"owner_id": "ou_xxx", "groups": ["oc_xxx", "..."]}
```

Remember that group messages only arrive when the bot is @-mentioned; the
allowlist decides "is this group allowed", not "can we receive it".

(`allow_all_groups: true` turns the group allowlist off entirely — any group
that can pull the bot in would reach the queue. Leave it off unless you
actually mean it.)

## Wiring it into Muse

`feishu-watch.sh` is a drop-in structural replacement for `qq-watch.sh`: the
same two-phase commit, the same pending claim, the same alert state machine,
the same warm wait (minus the typing branch). Register it as a ~5s hook:

```
script_path = examples/feishu-bot/feishu-watch.sh
env: FEISHU_DATA_DIR=..., FEISHU_HOOK_STATE_DIR=~/.muse/hooks/state
```

You **must** also keep `feishubot.py serve` running: it is the only data
source. If it dies, `poll` reports `POLL_ERROR` and rides the same alert path —
it does not go quietly silent.

Topics mirror the other variants:

| topic | meaning |
|---|---|
| `new_feishu_messages` | a batch landed; payload carries `count` / `pending_file` |
| `pending_feishu_messages` | the previous batch is still unclaimed |
| `token_expired` | credentials missing or rejected (immediate, then hourly) |
| `not_bound` | no owner yet (immediate, then hourly) |
| `poll_failing` | after 10 consecutive minutes of failure |
| `recovered` | a previously alerted fault cleared (`kind`: token / bind / poll_error) |

## Worker-side differences (only these)

- **Reply with ctx (the message_id), not a guessed chat_id.** Batch entries
  carry `reply_to` and `message_id`:

  ```bash
  python3 feishubot.py send <reply_to> <message_id> - --key <message_id>-1
  ```

  That goes through `/im/v1/messages/:message_id/reply`, which is more reliable
  than a proactive send.
- **Proactive messages pass an empty ctx** (scheduled reminders, for example):

  ```bash
  python3 feishubot.py send <oc_ chat_id or ou_ open_id> "" - --key <your key>
  ```

  The id type is inferred from the prefix (`oc_` → `chat_id`, `ou_` → `open_id`);
  `--chat` / `--p2p` override it. Targets outside the app's scope fail.
- **Idempotency keys work as everywhere else.** The same key returns
  `SEND_ALREADY_SENT`; the key is also used as the request `uuid` (Feishu
  dedupes on it for one hour), so "sent but the response was lost" does not
  become a second message. Multi-bubble replies use `<message_id>-1`, `-2`.
- **Media is inbound only.** Images/files/voice/video are downloaded to
  `data/media/` and surfaced as `media_path` (`media_error` on failure);
  cards and stickers become placeholder text. Outbound is text-only.
- **No flag for groups.** `reply_to` is already the chat_id, so the adapter
  takes the group path itself.
- **Keep the default warm wait.** There is no QQ-style 5-minute group window,
  so 180 seconds is fine.

## Self-check

```bash
python3 feishubot.py status     # token / long connection / inbox backlog / ledger
python3 feishubot.py health     # exit 0 ok / 1 warn / 2 bad
```

`serve_heartbeat_age` in `status` is the receiver's liveness proof: quiet with
a fresh heartbeat is a healthy idle bridge; quiet with a stale heartbeat is a
dead receiver.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

Three Feishu files:

- `tests/test_feishubot.py` — two-phase poll/commit, owner allowlist,
  `message_id` dedupe, text/rich-text/media parsing, send idempotency and
  `uuid` derivation, rate-limit retry
- `tests/test_feishu_hook_integration.py` — end-to-end `feishu-watch.sh`
  contract, plus "never sends a typing signal" (needs `bash` + `jq` + `flock`
  + `timeout`)
- `tests/test_feishubot_ws.py` — the long-connection loop: byte-level protobuf
  assertions, split-payload reassembly, ack-after-write ordering, ping/pong,
  and "auth failure / fatal code does not reconnect"

Git Bash has no `flock`, so the hook integration tests skip on plain Windows;
run them in WSL or on Linux.

## Known limits

- No typing indicator (point 1). Do not design interactions around one.
- Groups only deliver @-mentions by default; receiving everything needs a
  sensitive permission, which is not recommended.
- Proactive messages cannot reach anyone outside the app's availability scope,
  and cannot guarantee the recipient sees them.
- Outbound media is not implemented; sending images/files needs the
  `im/v1/image` and `im/v1/file` upload endpoints first.
- A user's open_id is per-app: changing apps means re-binding.
