#!/usr/bin/env python3
"""QQ official-bot client for the Muse hook contract.

This is the QQ counterpart of `ilink.py`. It speaks the *same* three-verb
protocol so a Muse worker can drive either channel without changing:

  qqbot.py poll                 # fetch a batch (JSON), do NOT advance state
  qqbot.py commit               # commit the batch (commit block via stdin)
  qqbot.py send <to> <ctx> <text> [--key <id>]   # passive reply
  qqbot.py typing <to> <start|stop> [--msg-id <id>]   # C2C typing indicator

`ctx` is the inbound message id: QQ only delivers *passive* replies, which
must carry the id of the user message they answer (msg_id) plus a sequence
number that distinguishes bubbles (msg_seq). Proactive push was discontinued
on 2025-04-21, so there is deliberately no `proactive` verb here.

Inbound is push-based, not poll-based, so a resident process owns the
gateway and a poll only drains what that process already made durable:

  qqbot.py serve                # WebSocket gateway -> inbox/ (durable)

Durability note: the official SDK persists the gateway sequence *before*
dispatching, so a crash mid-dispatch loses the event for good. Here the
event is written to inbox/ (tmp -> fsync -> rename) and only then is the
sequence persisted, so a crash re-delivers instead of dropping.

Usage:
  qqbot.py serve [--once]       # hold the gateway, append events to inbox/
  qqbot.py poll                 # drain inbox/, print {"msgs",...,"commit"}
  qqbot.py commit               # record ids + advance cursor (idempotent)
  qqbot.py send <to> <ctx> <text> [--group|--channel] [--key <id>] [--seq N]
  qqbot.py typing <to> <start|stop> [--msg-id <id>]
  qqbot.py bind [seconds]       # first C2C sender becomes owner (+ welcome)
  qqbot.py status               # token + gateway + inbox snapshot (JSON)
  qqbot.py health               # exit 0 ok / 1 warn / 2 bad

Env: QQ_DATA_DIR (default ./data next to this file), QQ_APP_ID,
QQ_APP_SECRET, QQ_SANDBOX=1 for the sandbox API host.
"""
import base64
import json
import os
import socket
import ssl
import struct
import sys
import time
import urllib.error
import urllib.request

try:
    import fcntl
except ImportError:  # Windows: ledger locking degrades to single-process
    fcntl = None

DATA_DIR = os.environ.get(
    "QQ_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"),
)
INBOX_DIR = os.path.join(DATA_DIR, "inbox")
MEDIA_DIR = os.path.join(DATA_DIR, "media")
DONE_DIR = os.path.join(INBOX_DIR, "done")
TOKEN_FILE = os.path.join(DATA_DIR, "token.json")
GATEWAY_FILE = os.path.join(DATA_DIR, "gateway.json")
CURSOR_FILE = os.path.join(DATA_DIR, "cursor.json")
SEEN_FILE = os.path.join(DATA_DIR, "seen_ids.json")
SEND_LEDGER_FILE = os.path.join(DATA_DIR, "send_ledger.json")
SEND_LEDGER_LOCK_FILE = os.path.join(DATA_DIR, "send_ledger.lock")
SEND_AUDIT_FILE = os.path.join(DATA_DIR, "send_audit.jsonl")
OWNER_FILE = os.path.join(DATA_DIR, "owner.json")
UNKNOWN_FILE = os.path.join(DATA_DIR, "unknown_senders.json")
SERVE_HEARTBEAT = os.path.join(DATA_DIR, "serve-heartbeat")
POLL_STATE = os.path.join(DATA_DIR, "poll-state.json")

TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
API_HOST = ("https://sandbox.api.sgroup.qq.com" if os.environ.get("QQ_SANDBOX") == "1"
            else "https://api.sgroup.qq.com")
TOKEN_SKEW = 60          # refresh this long before expiry
HEARTBEAT_STALE = 120    # poll calls it a failure if serve is older than this
SEEN_CAP = 1000
LEDGER_TTL = 86400
AUDIT_CAP = 1000
MEDIA_MAX_BYTES = 20 * 1024 * 1024
POLL_BATCH_CAP = 50      # events per poll; the rest wait for the next round
INTENT_GROUP_AND_C2C = 1 << 25

# How many passive replies one inbound message allows, per the platform
# docs (message/overview, updated 2026-10-28): C2C — 60 minutes, at most
# **4**; group — 5 minutes, at most 5. A bubble beyond the cap is rejected
# server-side (40034128), so we refuse it here with a readable message
# instead of letting the worker burn a retry on it.
PASSIVE_REPLIES = {"c2c": 4, "group": 5}

# Close codes that mean "do not reconnect": the API doc's table marks them
# 否/否 (no RESUME, no IDENTIFY). Retrying turns a configuration mistake
# into an infinite 3-second loop.
FATAL_CLOSE_CODES = {4001, 4002, 4010, 4011, 4012, 4013, 4014, 4914, 4915}

# msg_seq reserved for typing notifications. Reply bubbles use 1..5, and the
# server dedupes on (msg_id, msg_seq) regardless of msg_type — a typing
# notification sharing seq 1 would make the *first real bubble* fail with
# 40054005 (消息被去重). Hence a base far outside the bubble range.
TYPING_SEQ_BASE = 100


# ---------------------------------------------------------------- plumbing

def _now():
    return int(time.time())


def _ensure_dirs():
    for d in (DATA_DIR, INBOX_DIR, MEDIA_DIR, DONE_DIR):
        os.makedirs(d, exist_ok=True)


def load_json(path, default):
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, type(default)) else default
    except Exception:
        return default


def _atomic_write(path, write_fn, mode=0o600):
    """Write via tmp + fsync + rename so a crash never leaves half a file."""
    tmp = f"{path}.tmp.{os.getpid()}"
    try:
        with open(tmp, "w") as f:
            write_fn(f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        try:
            os.chmod(path, mode)
        except OSError:
            pass
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def save_json(path, data):
    _atomic_write(path, lambda f: json.dump(data, f, ensure_ascii=False))


def log_err(msg):
    print(msg, file=sys.stderr, flush=True)


# ------------------------------------------------------------------ token

def _credentials():
    app_id = os.environ.get("QQ_APP_ID")
    app_secret = os.environ.get("QQ_APP_SECRET")
    if app_id and app_secret:
        return app_id, app_secret
    cfg = load_json(os.path.join(DATA_DIR, "config.json"), {})
    app_id = cfg.get("app_id") or app_id
    app_secret = cfg.get("app_secret") or app_secret
    if not app_id or not app_secret:
        return None, None
    return str(app_id), str(app_secret)


def get_token(force=False):
    """Return a live access token. Cached on disk so separate CLI calls share it.

    The platform only issues a new token inside the last 60s of the old one,
    so refreshing strictly on skew is correct, not just an optimisation.
    """
    cached = load_json(TOKEN_FILE, {})
    if not force:
        tok = cached.get("access_token")
        if tok and _now() < int(cached.get("expires_at") or 0) - TOKEN_SKEW:
            return tok
    app_id, app_secret = _credentials()
    if not app_id:
        raise RuntimeError("NO_CREDENTIALS: set QQ_APP_ID/QQ_APP_SECRET "
                           "or app_id/app_secret in data/config.json")
    body = json.dumps({"appId": app_id, "clientSecret": app_secret}).encode()
    req = urllib.request.Request(TOKEN_URL, data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read().decode())
    tok = data.get("access_token")
    if not tok:
        # Errors come back as HTTP 200 with a body code (100016 bad secret,
        # 10004 no such bot). Never echo the secret itself.
        raise RuntimeError(f"TOKEN_ERROR:{data.get('code')}:{data.get('message')}")
    # expires_in is a *string* on this endpoint ("7200"), not a number.
    try:
        ttl = int(data.get("expires_in") or 7200)
    except (TypeError, ValueError):
        ttl = 7200
    save_json(TOKEN_FILE, {"access_token": tok, "expires_at": _now() + ttl})
    return tok


# -------------------------------------------------------------------- api

class ApiError(RuntimeError):
    def __init__(self, code, message, status=0):
        super().__init__(f"API_ERROR:{code}:{message}")
        self.code = code
        self.status = status


def api(method, path, body=None, timeout=20, token=None):
    tok = token or get_token()
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        API_HOST + path, data=data, method=method,
        headers={"Content-Type": "application/json",
                 "Authorization": f"QQBot {tok}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            status = r.status
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        status = e.code
    try:
        payload = json.loads(raw) if raw.strip() else {}
    except ValueError:
        raise ApiError(-1, f"unparseable response: {raw[:200]}", status)
    # QQ returns business errors with HTTP 200 and a body code; treat any
    # non-zero code as failure regardless of status.
    if isinstance(payload, dict) and payload.get("code"):
        raise ApiError(payload.get("code"), payload.get("message") or "", status)
    if status >= 400:
        raise ApiError(status, str(payload)[:200], status)
    return payload


# --------------------------------------------------------------- websocket

class WSClosed(Exception):
    """Connection ended. `code` is the RFC 6455 close status when the server
    sent one, so the caller can tell a transient drop from a fatal refusal."""

    def __init__(self, msg, code=None):
        super().__init__(msg)
        self.code = code


class WebSocket:
    """Minimal RFC 6455 client: no third-party dependency, matches this repo's
    'one optional dep' posture. Only what the QQ gateway needs: text frames,
    ping/pong, close, and defragmentation."""

    def __init__(self, url, timeout=60):
        self.url = url
        self.sock = None
        self.timeout = timeout
        self._buf = b""

    def connect(self):
        scheme, rest = self.url.split("://", 1)
        hostport, _, path = rest.partition("/")
        path = "/" + path
        host, _, port = hostport.partition(":")
        port = int(port or (443 if scheme == "wss" else 80))
        sock = socket.create_connection((host, port), timeout=self.timeout)
        if scheme == "wss":
            ctx = ssl.create_default_context()
            sock = ctx.wrap_socket(sock, server_hostname=host)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (f"GET {path} HTTP/1.1\r\n"
               f"Host: {hostport}\r\n"
               "Upgrade: websocket\r\n"
               "Connection: Upgrade\r\n"
               f"Sec-WebSocket-Key: {key}\r\n"
               "Sec-WebSocket-Version: 13\r\n\r\n")
        sock.sendall(req.encode())
        head = b""
        try:
            while b"\r\n\r\n" not in head:
                chunk = sock.recv(4096)
                if not chunk:
                    raise WSClosed("handshake closed")
                head += chunk
            header, _, tail = head.partition(b"\r\n\r\n")
            if b" 101 " not in header.split(b"\r\n")[0]:
                raise WSClosed("handshake rejected: "
                               + header.split(b"\r\n")[0].decode(errors="replace"))
        except Exception:
            # Never leak a socket on a failed handshake: the caller retries
            # in a loop, so a leak here would accumulate one fd per attempt.
            try:
                sock.close()
            except OSError:
                pass
            raise
        self._buf = tail
        self.sock = sock
        return self

    # -- framing

    def _read_exact(self, n):
        while len(self._buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise WSClosed("connection closed")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def recv_frame(self):
        b1, b2 = self._read_exact(2)
        fin = b1 & 0x80
        opcode = b1 & 0x0F
        masked = b2 & 0x80
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._read_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._read_exact(8))[0]
        mask = self._read_exact(4) if masked else b""
        payload = self._read_exact(length) if length else b""
        if masked:
            payload = bytes(payload[i] ^ mask[i % 4] for i in range(len(payload)))
        return fin, opcode, payload

    def recv_message(self, deadline=None):
        """Return (opcode, bytes) for one complete data message."""
        parts = []
        while True:
            if deadline is not None:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise socket.timeout("read deadline")
                self.sock.settimeout(max(0.2, min(remaining, 60)))
            fin, opcode, payload = self.recv_frame()
            if opcode == 0x9:      # ping -> pong
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:      # pong
                continue
            if opcode == 0x8:      # close
                # The close frame may carry a 2-byte status code. Keeping it
                # lets `serve` stop on 4914/4915 ("bot offline/banned") rather
                # than reconnecting forever into the same refusal.
                code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else None
                raise WSClosed(f"server closed (code {code})", code)
            parts.append(payload)
            if fin:
                code = opcode if opcode != 0x0 else 0x1
                return code, b"".join(parts)

    def _send_frame(self, opcode, payload):
        mask = os.urandom(4)
        n = len(payload)
        if n < 126:
            header = struct.pack("!BB", 0x80 | opcode, 0x80 | n)
        elif n < (1 << 16):
            header = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, n)
        else:
            header = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, n)
        masked = bytes(payload[i] ^ mask[i % 4] for i in range(n))
        self.sock.sendall(header + mask + masked)

    def send_json(self, obj):
        self._send_frame(0x1, json.dumps(obj).encode())

    def close(self):
        try:
            if self.sock:
                self._send_frame(0x8, b"")
        except Exception:
            pass
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.sock = None


# ------------------------------------------------------------------ inbox

def _event_filename(seq, event_id):
    safe = "".join(c for c in str(event_id) if c.isalnum() or c in "-_")[:48]
    return f"{int(seq):010d}-{safe or 'event'}.json"


def inbox_write(seq, t, d):
    """Persist one dispatch event. Idempotent on (seq): a replayed frame after
    a reconnect overwrites nothing and returns False."""
    _ensure_dirs()
    path = os.path.join(INBOX_DIR, _event_filename(seq, d.get("id") or ""))
    if os.path.exists(path):
        return False
    payload = {"seq": seq, "t": t, "d": d, "received_at": _now()}
    _atomic_write(path, lambda f: json.dump(payload, f, ensure_ascii=False))
    return True


def inbox_pending():
    try:
        names = sorted(n for n in os.listdir(INBOX_DIR) if n.endswith(".json"))
    except FileNotFoundError:
        return []
    return [os.path.join(INBOX_DIR, n) for n in names]


# --------------------------------------------------------------- gateway

def gateway_url(token=None):
    """Ask the platform where to connect. The URL can change between
    sessions, so it is never hard-coded."""
    for path in ("/gateway/bot", "/gateway"):
        try:
            data = api("GET", path, timeout=20, token=token)
        except ApiError as e:
            if path == "/gateway":
                raise
            log_err(f"gateway lookup via {path} failed ({e}); trying next")
            continue
        url = data.get("url")
        if url:
            return url, data
    raise RuntimeError("GATEWAY_UNKNOWN: no url returned")


def _beat():
    """Liveness marker for `poll` / `health`: a quiet receiver is healthy,
    a silent one is only distinguishable by this file going stale."""
    try:
        with open(SERVE_HEARTBEAT, "w") as f:
            f.write(str(_now()))
    except OSError:
        pass


def cmd_serve(once=False, max_retries=None):
    """Hold the gateway connection and make every dispatch durable.

    Order matters and is the whole point: write the event, fsync, rename,
    *then* persist the sequence. A crash in between re-delivers the event.
    """
    _ensure_dirs()
    gw = load_json(GATEWAY_FILE, {})
    session_id = gw.get("session_id")
    last_seq = int(gw.get("seq") or 0)
    failures = 0
    while True:
        try:
            tok = get_token()
            url, _info = gateway_url(tok)
            ws = WebSocket(url, timeout=60).connect()
            hello = ws.recv_message(deadline=time.time() + 30)
            payload = json.loads(hello[1].decode())
            if payload.get("op") != 10:
                raise WSClosed(f"expected op 10, got {payload.get('op')}")
            interval = (payload.get("d") or {}).get("heartbeat_interval", 30000) / 1000.0
            if session_id and last_seq:
                ws.send_json({"op": 6, "d": {"token": f"QQBot {tok}",
                                             "session_id": session_id,
                                             "seq": last_seq}})
            else:
                ws.send_json({"op": 2, "d": {
                    "token": f"QQBot {tok}",
                    "intents": INTENT_GROUP_AND_C2C,
                    "shard": [0, 1],
                    "properties": {"$os": "linux", "$browser": "qqbot.py",
                                   "$device": "qqbot.py"}}})
            next_beat = time.time() + interval
            while True:
                try:
                    _, raw = ws.recv_message(deadline=next_beat)
                except socket.timeout:
                    ws.send_json({"op": 1, "d": last_seq or None})
                    next_beat = time.time() + interval
                    # A quiet gateway must still look alive: `poll` reads
                    # this file, and an idle-but-healthy receiver that
                    # stopped beating would be reported as dead.
                    _beat()
                    continue
                msg = json.loads(raw.decode())
                op = msg.get("op")
                if op == 7:      # server says reconnect
                    raise WSClosed("server requested reconnect")
                if op == 9:      # invalid session: drop it, identify afresh
                    session_id, last_seq = None, 0
                    save_json(GATEWAY_FILE, {})
                    raise WSClosed("invalid session")
                if op == 11:     # heartbeat ack
                    continue
                if op == 10:
                    continue
                if op != 0:
                    continue
                seq = int(msg.get("s") or 0)
                t = msg.get("t") or ""
                d = msg.get("d") or {}
                if t == "READY":
                    session_id = (d or {}).get("session_id") or session_id
                    # Persist at once: a restart before any event arrives
                    # can then resume instead of identifying afresh (which
                    # is harmless, but resume keeps the same session).
                    save_json(GATEWAY_FILE, {"session_id": session_id,
                                             "seq": last_seq})
                elif t == "RESUMED":
                    pass
                elif seq:
                    inbox_write(seq, t, d)
                    # Durable only now: the event file is on disk, so a
                    # crash before this line merely re-delivers.
                    last_seq = seq
                    save_json(GATEWAY_FILE, {"session_id": session_id,
                                             "seq": last_seq})
                    if once:
                        # `serve --once` means "one real event, then stop";
                        # READY/RESUMED must not end the run.
                        ws.close()
                        return 0
                _beat()
        except WSClosed as e:
            if e.code in FATAL_CLOSE_CODES:
                # The doc marks these "no RESUME, no IDENTIFY": the bot is
                # offline/banned or the request itself is wrong. Reconnecting
                # every 3s would only hide that from whoever reads the log.
                log_err(f"gateway loop: {e}; not retrying (fatal close code)")
                return 1
            failures += 1
            if max_retries is not None and failures >= max_retries:
                log_err(f"gateway loop: {e}; giving up after {failures} attempts")
                return 1
            log_err(f"gateway loop: {e}; reconnecting in 3s")
            time.sleep(3)
        except (OSError, RuntimeError, ValueError, socket.timeout) as e:
            failures += 1
            if max_retries is not None and failures >= max_retries:
                log_err(f"gateway loop: {e}; giving up after {failures} attempts")
                return 1
            log_err(f"gateway loop: {e}; reconnecting in 3s")
            time.sleep(3)
        except KeyboardInterrupt:
            return 0


# ------------------------------------------------------------------- poll

def _owner_sets():
    cfg = load_json(OWNER_FILE, {})
    single = cfg.get("owner_id")
    owners = {str(x) for x in (cfg.get("owners") or []) if x}
    if single:
        owners.add(str(single))
    groups = {str(x) for x in (cfg.get("groups") or []) if x}
    return owners, groups, bool(cfg.get("allow_all_groups"))


def _note_unknown(sender, chat):
    data = load_json(UNKNOWN_FILE, {})
    data[str(sender)] = {"chat": chat, "at": _now()}
    # Cap the file so a flood of strangers cannot grow it without bound.
    if len(data) > 200:
        for k in sorted(data, key=lambda k: data[k].get("at", 0))[:len(data) - 200]:
            data.pop(k, None)
    save_json(UNKNOWN_FILE, data)


def _download(url, name, msg_id, idx):
    if not url:
        return None, "no url"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "qqbot.py"})
        with urllib.request.urlopen(req, timeout=30) as r:
            declared = r.headers.get("Content-Length")
            if declared and int(declared) > MEDIA_MAX_BYTES:
                return None, "too large"
            blob = r.read(MEDIA_MAX_BYTES + 1)
        if len(blob) > MEDIA_MAX_BYTES:
            return None, "too large"
        ext = os.path.splitext(name or "")[1] or ".bin"
        safe = "".join(c for c in str(msg_id) if c.isalnum())[:32] or "msg"
        path = os.path.join(MEDIA_DIR, f"{safe}_{idx}{ext}")
        with open(path, "wb") as f:
            f.write(blob)
        return path, None
    except Exception as e:
        return None, str(e)[:100]


def _normalize(ev):
    """Map one stored QQ event to the worker-facing entry shape used by
    AGENTS.md (from_user_id / message_id / kind / text / quoted_*)."""
    d = ev.get("d") or {}
    t = ev.get("t") or ""
    msg_id = str(d.get("id") or "")
    if not msg_id:
        return None, None
    if t == "C2C_MESSAGE_CREATE":
        sender = str((d.get("author") or {}).get("user_openid") or "")
        chat = "c2c"
        target = sender
    elif t == "GROUP_AT_MESSAGE_CREATE":
        sender = str((d.get("author") or {}).get("member_openid") or "")
        chat = "group"
        target = str(d.get("group_openid") or "")
    elif t == "DIRECT_MESSAGE_CREATE":
        sender = str((d.get("author") or {}).get("id") or "")
        chat = "dm"
        target = str(d.get("guild_id") or "")
    elif t == "AT_MESSAGE_CREATE":
        sender = str((d.get("author") or {}).get("id") or "")
        chat = "channel"
        target = str(d.get("channel_id") or "")
    else:
        return None, None
    entry = {"from_user_id": sender, "message_id": msg_id, "chat_type": chat,
             "reply_to": target, "timestamp": d.get("timestamp")}
    content = d.get("content") or ""
    if chat in ("group", "channel"):
        # The platform prefixes an @ mention; strip it so the worker sees
        # the actual text instead of "<@!123> hello".
        content = content.replace(" ", " ").lstrip()
        if content.startswith("<@!") or content.startswith("<@"):
            content = content.split(">", 1)[1] if ">" in content else content
        content = content.strip()
    entry["text"] = content
    entry["kind"] = "text"
    atts = d.get("attachments") or []
    if atts:
        a = atts[0] if isinstance(atts, list) else {}
        ctype = str((a or {}).get("content_type") or "")
        kind = ("image" if ctype.startswith("image/")
                else "video" if ctype.startswith("video/")
                else "voice" if ctype == "voice"
                else "file")
        entry["kind"] = kind
        if kind == "file":
            entry["file_name"] = a.get("filename") or ""
        path, err = _download(a.get("url"), a.get("filename") or "", msg_id, 0)
        if path:
            entry["media_path"] = path
        elif err:
            entry["media_error"] = err
        if len(atts) > 1:
            entry["attachment_count"] = len(atts)
    return entry, chat


def cmd_poll():
    """Read the durable inbox. Never advances state: that is `commit`'s job.

    Ordering and replay are the inbox's job (sorted filenames, at-least-once).
    Strangers are dropped here for the same reason ilink drops them: they
    must never wake a worker, but their ids still ride along in the commit
    block so the queue cannot jam on them.
    """
    _ensure_dirs()
    heartbeat_age = None
    try:
        with open(SERVE_HEARTBEAT) as f:
            heartbeat_age = _now() - int(f.read().strip() or 0)
    except Exception:
        heartbeat_age = None
    files = inbox_pending()
    receiver_dead = heartbeat_age is None or heartbeat_age > HEARTBEAT_STALE
    if receiver_dead and not files:
        # A dead receiver with an empty inbox is a poll failure, not silence:
        # otherwise the worker is simply never woken and nothing alerts.
        age = "never" if heartbeat_age is None else f"{heartbeat_age}s"
        print(f"POLL_ERROR:gateway receiver stale (heartbeat {age})")
        return 1
    if receiver_dead:
        # Events already on disk are durable and must be delivered even if
        # the receiver has since died — the batch is drained first, and the
        # next empty poll surfaces the fault above.
        log_err(f"gateway receiver stale (heartbeat {heartbeat_age}s); "
                f"draining {len(files)} already-persisted event(s)")
    # Presence check only — never a network round-trip here: draining the
    # inbox is local and must not stall on a slow token endpoint. A missing
    # credential still has to be visible, because no reply could ever be
    # sent; the watch script turns this into its token_expired alert.
    if not _credentials()[0]:
        print("NO_CREDENTIALS: set QQ_APP_ID/QQ_APP_SECRET "
              "or app_id/app_secret in data/config.json")
        return 1
    seen = set(load_json(SEEN_FILE, {}).get("ids") or [])
    owners, groups, allow_all = _owner_sets()
    if not owners:
        print("NO_OWNER")   # nothing is allowed until `bind` has run
        return 1
    msgs = []
    dropped = []
    ids = []
    consumed = []
    max_seq = 0
    for path in files:
        if len(consumed) >= POLL_BATCH_CAP * 4 or len(msgs) >= POLL_BATCH_CAP:
            break
        ev = load_json(path, {})
        if not ev:
            consumed.append(os.path.basename(path))
            continue
        max_seq = max(max_seq, int(ev.get("seq") or 0))
        entry, chat = _normalize(ev)
        if entry is None:
            # Non-message events (READY, group add/remove, ...) are consumed
            # but never queued; keep them out of the commit id list.
            consumed.append(os.path.basename(path))
            continue
        msg_id = entry["message_id"]
        ids.append(msg_id)
        consumed.append(os.path.basename(path))
        if msg_id in seen:
            continue
        allowed = entry["from_user_id"] in owners
        if chat == "group" and not allowed:
            allowed = allow_all or entry["reply_to"] in groups
        if not allowed:
            _note_unknown(entry["from_user_id"], chat)
            dropped.append(entry["from_user_id"])
            continue
        msgs.append(entry)
    print(json.dumps({"msgs": msgs,
                      "filtered": {"dropped": len(set(dropped)),
                                   "senders": sorted(set(dropped))},
                      "commit": {"cursor": str(max_seq), "ids": ids,
                                 "files": consumed}},
                     ensure_ascii=False))
    return 0


def cmd_commit():
    """Record delivered ids, advance the cursor, release the inbox files.
    Idempotent: re-running with the same block is a no-op."""
    try:
        block = json.loads(sys.stdin.read() or "{}")
    except ValueError:
        print("COMMIT_BAD_INPUT")
        return 1
    if not isinstance(block, dict):
        print("COMMIT_BAD_INPUT")
        return 1
    ids = [str(i) for i in (block.get("ids") or [])]
    state = load_json(SEEN_FILE, {})
    seen = [str(i) for i in (state.get("ids") or [])]
    merged = list(dict.fromkeys(seen + ids))
    save_json(SEEN_FILE, {"ids": merged[-SEEN_CAP:]})
    if block.get("cursor"):
        save_json(CURSOR_FILE, {"cursor": str(block["cursor"]), "at": _now()})
    moved = 0
    for name in block.get("files") or []:
        src = os.path.join(INBOX_DIR, os.path.basename(str(name)))
        if os.path.exists(src):
            try:
                os.replace(src, os.path.join(DONE_DIR, os.path.basename(src)))
                moved += 1
            except OSError:
                pass
    print(f"COMMITTED {len(ids)} ids, {moved} files")
    return 0


# ------------------------------------------------------------------- send

def _ledger_load():
    return load_json(SEND_LEDGER_FILE, {})


class _LedgerLock:
    """Inter-process lock around read-modify-write of the send ledger. Two
    bubbles of one reply can race; only one may allocate a given msg_seq."""

    def __init__(self, path):
        self.path = path
        self.f = None

    def __enter__(self):
        _ensure_dirs()
        self.f = open(self.path, "a+")
        if fcntl is not None:
            fcntl.flock(self.f, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        try:
            if self.f and fcntl is not None:
                fcntl.flock(self.f, fcntl.LOCK_UN)
        finally:
            if self.f:
                self.f.close()
            self.f = None


def _audit(entry):
    try:
        _ensure_dirs()
        line = json.dumps(entry, ensure_ascii=False) + "\n"
        with open(SEND_AUDIT_FILE, "a") as f:
            f.write(line)
        with open(SEND_AUDIT_FILE) as f:
            lines = f.readlines()
        if len(lines) > AUDIT_CAP:
            with open(SEND_AUDIT_FILE + ".tmp", "w") as f:
                f.writelines(lines[-AUDIT_CAP:])
            os.replace(SEND_AUDIT_FILE + ".tmp", SEND_AUDIT_FILE)
    except OSError:
        pass


def _seq_from_key(key):
    """Bubble number is the trailing `-N` of the worker's idempotency key
    (`<msg_id>-1`, `<msg_id>-2`, ...), matching the upstream convention.
    That makes msg_seq deterministic instead of random, so a retry lands on
    the same (msg_id, msg_seq) pair the server dedupes."""
    if not key:
        return None
    _, sep, tail = key.rpartition("-")
    if sep and tail.isdigit() and 1 <= int(tail) <= 5:
        return int(tail)
    return None


def _next_seq(ledger, msg_id):
    """Highest msg_seq already used for this inbound message, +1.

    Keyed on the entry's stored msg_id rather than on the caller's key
    prefix: a caller may use an arbitrary idempotency key, and two bubbles
    that both fall through to auto-allocation must not collide on the same
    (msg_id, msg_seq) pair — the server rejects the second one outright.
    """
    used = [v.get("seq", 0) for v in ledger.values()
            if isinstance(v, dict) and v.get("msg_id") == msg_id]
    return (max(used) if used else 0) + 1


def cmd_send(to, ctx, text, chat="c2c", seq=None, key=None):
    if not to:
        print("SEND_BAD_TARGET")
        return 1
    if not ctx:
        print("SEND_NO_MSG_ID: QQ only allows passive replies; the inbound "
              "message id is required")
        return 1
    if text in (None, ""):
        print("SEND_EMPTY")
        return 1
    if len(text) > 4000:
        text = text[:4000]
    bubble_key = key or f"{ctx}-{seq or 1}"
    with _LedgerLock(SEND_LEDGER_LOCK_FILE):
        ledger = _ledger_load()
        _now_ts = _now()
        ledger = {k: v for k, v in ledger.items()
                  if not (isinstance(v, dict) and _now_ts - int(v.get("at") or 0) > LEDGER_TTL)}
        entry = ledger.get(bubble_key)
        if isinstance(entry, dict) and entry.get("status") == "sent":
            print("SEND_ALREADY_SENT")
            return 0
        if seq is None:
            seq = _seq_from_key(bubble_key)
        if seq is None:
            seq = (entry or {}).get("seq") or _next_seq(ledger, ctx)
        # A *new* bubble past the platform's cap can only fail server-side.
        # An existing intent is different: it was already within the cap when
        # first attempted, so its retry must go through unchanged.
        cap = PASSIVE_REPLIES.get(chat, 5)
        if entry is None and int(seq) > cap:
            print(f"SEND_BUBBLE_CAP:{chat} allows at most {cap} replies per "
                  f"msg_id; bubble {seq} would be rejected")
            return 1
        # Persist the intent (msg_id + msg_seq) BEFORE calling out: the pair
        # is the server-side dedupe key, so a crash after this line can only
        # cause a retry the server will reject, never a duplicate bubble.
        ledger[bubble_key] = {"to": to, "msg_id": ctx, "seq": int(seq),
                              "chat": chat, "status": "intent", "at": _now_ts}
        save_json(SEND_LEDGER_FILE, ledger)

    if chat == "group":
        path = f"/v2/groups/{to}/messages"
    elif chat == "channel":
        path = f"/channels/{to}/messages"
    elif chat == "dm":
        path = f"/dms/{to}/messages"
    else:
        path = f"/v2/users/{to}/messages"
    body = {"content": text, "msg_type": 0, "msg_id": ctx, "msg_seq": int(seq)}
    ok, err = False, None
    for attempt in (1, 2):
        try:
            api("POST", path, body, timeout=15)
            ok = True
            break
        except ApiError as e:
            err = e
            if e.code == 22009:        # rate limited: one backoff, then give up
                if attempt == 1:
                    time.sleep(1)
                    continue
            break
        except Exception as e:          # timeout: the send may have landed
            err = e
            if attempt == 1:
                time.sleep(1)
                continue
            break
    with _LedgerLock(SEND_LEDGER_LOCK_FILE):
        ledger = _ledger_load()
        entry = ledger.get(bubble_key) or {"msg_id": ctx, "seq": int(seq),
                                           "to": to, "chat": chat}
        entry["status"] = "sent" if ok else "intent"
        entry["at"] = _now()
        if err is not None:
            entry["error"] = str(err)[:200]
        ledger[bubble_key] = entry
        save_json(SEND_LEDGER_FILE, ledger)
    _audit({"ts": _now(), "to": to, "msg_id": ctx, "seq": seq, "chat": chat,
            "key": bubble_key, "result": "ok" if ok else "error",
            "error": None if ok else str(err)[:200]})
    if ok:
        print("SEND_OK")
        return 0
    print(f"SEND_FAILED:{err}")
    return 1


def cmd_typing(to, action, msg_id=None):
    """Typing indicator. C2C only on this platform (the group send API has
    no msg_type 6), and the platform wants the message it is a reply to, so
    `msg_id` is passed when the caller has one.

    Failures are advisory — this is decoration, never a reason to stall."""
    if not to or action not in ("start", "stop"):
        print("TYPING_BAD_ARGS")
        return 1
    # Wire format per the official send-C2C-message doc: msg_type 6 with
    # `input_notify` (input_type 1 = "对方正在输入…", input_second ≤ 60).
    # The current page documents only input_type 1; 2 ("取消展示") comes from
    # the SDK's own field comment, so a stop is best-effort by nature.
    body = {"msg_type": 6, "input_notify": {
        "input_type": 1 if action == "start" else 2,
        "input_second": 60}}
    if msg_id:
        body["msg_id"] = msg_id
        # Never seq 1: that slot belongs to the first real bubble, and a
        # typing notification with the same (msg_id, msg_seq) would make
        # the bubble itself fail as a duplicate.
        body["msg_seq"] = TYPING_SEQ_BASE
    try:
        api("POST", f"/v2/users/{to}/messages", body, timeout=10)
        print("TYPING_OK")
        return 0
    except Exception as e:
        print(f"TYPING_FAILED:{e}")
        return 0   # never let a cosmetic failure break the worker


# ------------------------------------------------------------------- bind

def cmd_bind(wait_secs=300):
    """One binding window: the first C2C sender becomes owner, code-enforced.

    The upstream bot has the same bootstrap problem (a new channel has no
    owner id yet); this mirrors `ilink.py bind` so a fresh deployment can be
    claimed by whoever messages the bot first, and by nobody afterwards.
    """
    _ensure_dirs()
    if load_json(OWNER_FILE, {}).get("owner_id"):
        print("ALREADY_BOUND")
        return 1
    lock_path = os.path.join(DATA_DIR, "bind.lock")
    with _LedgerLock(lock_path):
        deadline = time.time() + max(1, wait_secs)
        while time.time() < deadline:
            for path in inbox_pending():
                ev = load_json(path, {})
                if (ev.get("t") or "") != "C2C_MESSAGE_CREATE":
                    continue
                d = ev.get("d") or {}
                sender = str((d.get("author") or {}).get("user_openid") or "")
                if not sender:
                    continue
                if load_json(OWNER_FILE, {}).get("owner_id"):
                    print("ALREADY_BOUND")
                    return 1
                save_json(OWNER_FILE, {"owner_id": sender,
                                       "bound_at": _now()})
                print(f"BOUND:{sender}")
                # Welcome goes out through the same passive path, so it is
                # subject to the same 60-minute window — send immediately.
                cmd_send(sender, str(d.get("id") or ""),
                         "已绑定。之后直接说话就行。", "c2c", seq=1,
                         key=f"{d.get('id')}-1")
                return 0
            time.sleep(2)
    print("BIND_TIMEOUT")
    return 1


# ----------------------------------------------------------------- status

def cmd_status():
    gw = load_json(GATEWAY_FILE, {})
    cur = load_json(CURSOR_FILE, {})
    owners, groups, allow_all = _owner_sets()
    try:
        with open(SERVE_HEARTBEAT) as f:
            hb_age = _now() - int(f.read().strip() or 0)
    except Exception:
        hb_age = None
    ledger = _ledger_load()
    tok = load_json(TOKEN_FILE, {})
    print(json.dumps({
        "api": API_HOST,
        "token": "ok" if tok.get("access_token") else "missing",
        "token_expires_in": max(0, int(tok.get("expires_at") or 0) - _now()) if tok else 0,
        "gateway_session": "present" if gw.get("session_id") else "none",
        "gateway_seq": gw.get("seq"),
        "serve_heartbeat_age": hb_age,
        "inbox_pending": len(inbox_pending()),
        "inbox_done": len(os.listdir(DONE_DIR)) if os.path.isdir(DONE_DIR) else 0,
        "cursor": cur.get("cursor"),
        "owners": len(owners),
        "groups": len(groups),
        "allow_all_groups": allow_all,
        "ledger_entries": len(ledger),
    }, ensure_ascii=False, indent=2))
    return 0


def cmd_health():
    """Exit 0 ok / 1 warn / 2 bad — same convention as ilink.py health."""
    problems, warns = [], []
    if not _credentials()[0]:
        problems.append("no credentials (QQ_APP_ID/QQ_APP_SECRET)")
    else:
        try:
            get_token()
        except Exception as e:
            problems.append(str(e))
    try:
        with open(SERVE_HEARTBEAT) as f:
            age = _now() - int(f.read().strip() or 0)
        if age > HEARTBEAT_STALE:
            problems.append(f"gateway receiver stale ({age}s)")
    except Exception:
        problems.append("gateway receiver never ran")
    if not _owner_sets()[0]:
        warns.append("no owner bound (run `qqbot.py bind`)")
    pending = len(inbox_pending())
    if pending > 200:
        warns.append(f"{pending} inbox events uncommitted")
    if not os.path.isdir(DONE_DIR) or len(os.listdir(DONE_DIR)) == 0:
        warns.append("no committed events yet")
    for w in warns:
        print(f"WARN:{w}")
    for p in problems:
        print(f"BAD:{p}")
    if not problems and not warns:
        print("OK")
    return 2 if problems else 1 if warns else 0


# ------------------------------------------------------------------- main

def _flag(args, name, takes_value=False):
    if name in args:
        i = args.index(name)
        if takes_value and i + 1 < len(args):
            return args[i + 1]
        return True
    return None


def main(argv):
    _ensure_dirs()
    if len(argv) < 2:
        print(__doc__)
        return 2
    cmd, args = argv[1], argv[2:]
    if cmd == "serve":
        return cmd_serve(once=bool(_flag(args, "--once")))
    if cmd == "poll":
        return cmd_poll()
    if cmd == "commit":
        return cmd_commit()
    if cmd == "send":
        if len(args) < 3:
            print("usage: qqbot.py send <to> <msg_id> <text|-> [--group|--channel|--dm] "
                  "[--key <id>] [--seq N]")
            return 2
        to, ctx, text = args[0], args[1], args[2]
        if text == "-":
            text = sys.stdin.read()
        chat = "group" if _flag(args, "--group") else \
               "channel" if _flag(args, "--channel") else \
               "dm" if _flag(args, "--dm") else "c2c"
        seq = _flag(args, "--seq", True)
        return cmd_send(to, ctx, text, chat,
                        seq=int(seq) if seq not in (None, True) else None,
                        key=_flag(args, "--key", True))
    if cmd == "typing":
        if len(args) < 2:
            print("usage: qqbot.py typing <to> <start|stop> [--msg-id <id>]")
            return 2
        return cmd_typing(args[0], args[1], _flag(args, "--msg-id", True))
    if cmd == "bind":
        secs = args[0] if args and args[0].isdigit() else 300
        return cmd_bind(int(secs))
    if cmd == "status":
        return cmd_status()
    if cmd == "health":
        return cmd_health()
    print(f"unknown command: {cmd}")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
