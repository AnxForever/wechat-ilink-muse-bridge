#!/usr/bin/env python3
"""Feishu (Lark) official-bot client for the Muse hook contract.

This is the Feishu counterpart of `ilink.py` and `qqbot.py`. It speaks the
*same* three-verb protocol so a Muse worker can drive either channel:

  feishubot.py poll                 # fetch a batch (JSON), do NOT advance state
  feishubot.py commit               # commit the batch (commit block via stdin)
  feishubot.py send <to> <ctx> <text> [--key <id>]   # reply to <ctx>

`ctx` is the inbound message id (om_...). A reply is the passive path and is
preferred; when `ctx` is empty the send goes out as a proactive message to
`<to>` (an `oc_` chat id or an `ou_` open id).

Inbound is push-based. Feishu's "long connection" mode needs **no public IP,
no domain, no tunnel**: the client POSTs for a WebSocket endpoint, holds that
connection, and the platform pushes events over it. Authentication happens
once at connection setup; events on the wire are plaintext and are not
signature-checked (unlike webhook mode).

Durability: the platform treats an event as delivered when it gets an ack,
and retries at 15s / 5min / 1hr / 6hr (4 retries) otherwise. Duplicates can
still occur; Feishu's own docs say to dedupe on `message_id`, not `event_id`.
So the ack is sent only *after* the event is on disk (tmp -> fsync -> rename),
and `poll` dedupes on message_id. Same rule as the QQ channel: file first,
transport state second.

Usage:
  feishubot.py serve [--once]   # hold the long connection, append events to inbox/
  feishubot.py poll             # drain inbox/, print {"msgs",...,"commit"}
  feishubot.py commit           # record ids + release inbox files (idempotent)
  feishubot.py send <to> <ctx> <text> [--chat|--p2p] [--key <id>]
  feishubot.py bind [seconds]   # first DM sender becomes owner (+ welcome)
  feishubot.py status           # token + connection + inbox snapshot (JSON)
  feishubot.py health           # exit 0 ok / 1 warn / 2 bad

Env: FEISHU_DATA_DIR (default ./data next to this file), FEISHU_APP_ID,
FEISHU_APP_SECRET, FEISHU_DOMAIN (default https://open.feishu.cn; set to
https://open.larksuite.com for the international Lark tenant).
"""
import base64
import hashlib
import json
import os
import re
import socket
import ssl
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

try:
    import fcntl
except ImportError:  # Windows: ledger locking degrades to single-process
    fcntl = None

DATA_DIR = os.environ.get(
    "FEISHU_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"),
)
INBOX_DIR = os.path.join(DATA_DIR, "inbox")
MEDIA_DIR = os.path.join(DATA_DIR, "media")
DONE_DIR = os.path.join(INBOX_DIR, "done")
TOKEN_FILE = os.path.join(DATA_DIR, "token.json")
WS_FILE = os.path.join(DATA_DIR, "ws.json")
CURSOR_FILE = os.path.join(DATA_DIR, "cursor.json")
SEEN_FILE = os.path.join(DATA_DIR, "seen_ids.json")
INBOX_INDEX = os.path.join(DATA_DIR, "inbox_index.json")
SEND_LEDGER_FILE = os.path.join(DATA_DIR, "send_ledger.json")
SEND_LEDGER_LOCK_FILE = os.path.join(DATA_DIR, "send_ledger.lock")
SEND_AUDIT_FILE = os.path.join(DATA_DIR, "send_audit.jsonl")
OWNER_FILE = os.path.join(DATA_DIR, "owner.json")
UNKNOWN_FILE = os.path.join(DATA_DIR, "unknown_senders.json")
SERVE_HEARTBEAT = os.path.join(DATA_DIR, "serve-heartbeat")

DOMAIN = os.environ.get("FEISHU_DOMAIN", "https://open.feishu.cn").rstrip("/")
API = DOMAIN + "/open-apis"
EDGE = DOMAIN + "/callback/ws/endpoint"
USER_AGENT = "feishubot.py/1.0 (+muse-hook)"

TOKEN_SKEW = 120         # refresh this long before expiry
HEARTBEAT_STALE = 120    # poll calls it a failure if serve is older than this
SEEN_CAP = 1000
INBOX_INDEX_CAP = 2000
LEDGER_TTL = 86400
AUDIT_CAP = 1000
MEDIA_MAX_BYTES = 20 * 1024 * 1024
POLL_BATCH_CAP = 50      # events per poll; the rest wait for the next round
MAX_TEXT = 30000
DEDUPE_SECONDS = 300     # merged-frame reassembly window (sum > 1)

# Endpoint-discovery errors that will never heal by retrying: bad credentials,
# forbidden app, no credential, or too many connections for this app. The
# official SDK treats these as ClientException (no reconnect) for the same
# reason — retrying only hides a configuration mistake.
FATAL_ENDPOINT_CODES = {403, 514, 1000040343, 1000040344, 1000040350}

# Rate-limit business codes. The reply/create APIs allow 5 QPS per user or
# group, so a burst of bubbles can trip this; one short retry is enough.
RATE_LIMIT_CODES = {99991400, 99991401}


# ---------------------------------------------------------------- plumbing

def _now():
    return int(time.time())


def _now_ms():
    return int(time.time() * 1000)


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
    app_id = os.environ.get("FEISHU_APP_ID")
    app_secret = os.environ.get("FEISHU_APP_SECRET")
    if app_id and app_secret:
        return app_id, app_secret
    cfg = load_json(os.path.join(DATA_DIR, "config.json"), {})
    app_id = cfg.get("app_id") or app_id
    app_secret = cfg.get("app_secret") or app_secret
    if not app_id or not app_secret:
        return None, None
    return str(app_id), str(app_secret)


def get_token(force=False):
    """Return a live tenant_access_token, cached on disk for CLI calls.

    Feishu's token endpoint returns the *same* token while more than 30
    minutes remain, so refreshing on skew is cheap and never invalidates a
    token another process is using.
    """
    cached = load_json(TOKEN_FILE, {})
    if not force:
        tok = cached.get("access_token")
        if tok and _now() < int(cached.get("expires_at") or 0) - TOKEN_SKEW:
            return tok
    app_id, app_secret = _credentials()
    if not app_id:
        raise RuntimeError("NO_CREDENTIALS: set FEISHU_APP_ID/FEISHU_APP_SECRET "
                           "or app_id/app_secret in data/config.json")
    body = json.dumps({"app_id": app_id, "app_secret": app_secret}).encode()
    req = urllib.request.Request(
        API + "/auth/v3/tenant_access_token/internal", data=body, method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        raise RuntimeError(f"TOKEN_ERROR:{e.code}:{_brief(raw)}")
    except Exception as e:
        raise RuntimeError(f"TOKEN_ERROR:network:{e}")
    if data.get("code"):
        # Never echo app_secret; the platform's msg is enough to act on.
        raise RuntimeError(f"TOKEN_ERROR:{data.get('code')}:{data.get('msg')}")
    tok = data.get("tenant_access_token")
    if not tok:
        raise RuntimeError("TOKEN_ERROR:empty-token")
    try:
        ttl = int(data.get("expire") or 7200)
    except (TypeError, ValueError):
        ttl = 7200
    save_json(TOKEN_FILE, {"access_token": tok, "expires_at": _now() + ttl})
    return tok


def _brief(text, limit=200):
    return (text or "").replace("\n", " ")[:limit]


# -------------------------------------------------------------------- api

class ApiError(RuntimeError):
    def __init__(self, code, message, status=0):
        super().__init__(f"API_ERROR:{code}:{message}")
        self.code = code
        self.status = status


def api(method, path, body=None, timeout=20, token=None):
    tok = token or get_token()
    url = path if path.startswith("http") else API + path
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json; charset=utf-8",
                 "Authorization": f"Bearer {tok}"})
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
        raise ApiError(-1, f"unparseable response: {_brief(raw)}", status)
    if isinstance(payload, dict) and payload.get("code"):
        raise ApiError(payload.get("code"), payload.get("msg") or "", status)
    if status >= 400:
        raise ApiError(status, _brief(str(payload)), status)
    return payload


def api_raw(path, max_bytes=MEDIA_MAX_BYTES, timeout=30, token=None):
    """GET a binary resource. Returns (bytes, content_type)."""
    tok = token or get_token()
    url = path if path.startswith("http") else API + path
    req = urllib.request.Request(url, method="GET", headers={
        "Authorization": f"Bearer {tok}", "User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            ctype = r.headers.get("Content-Type") or ""
            declared = r.headers.get("Content-Length")
            if declared and int(declared) > max_bytes:
                raise ApiError(-1, "resource too large", r.status)
            blob = r.read(max_bytes + 1)
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            code = json.loads(raw).get("code") or e.code
            msg = json.loads(raw).get("msg") or ""
        except ValueError:
            code, msg = e.code, _brief(raw)
        raise ApiError(code, msg, e.code)
    if len(blob) > max_bytes:
        raise ApiError(-1, "resource too large", 200)
    return blob, ctype


# --------------------------------------------------------------- protobuf
# The long-connection wire format is the SDK's `pbbp2.proto`:
#
#   message Header { required string key = 1; required string value = 2; }
#   message Frame {
#     required uint64 SeqID = 1;  required uint64 LogID = 2;
#     required int32 service = 3; required int32 method = 4;
#     repeated Header headers = 5;
#     optional string payload_encoding = 6;
#     optional string payload_type = 7;
#     optional bytes payload = 8;
#     optional string LogIDNew = 9;
#   }
#
# That is all this channel needs, so it is encoded by hand rather than
# pulling in a protobuf runtime.

def _pb_varint(n):
    n = int(n) & ((1 << 64) - 1)
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _pb_uint(num, value):
    return _pb_varint(num << 3) + _pb_varint(value)


def _pb_bytes(num, value):
    if isinstance(value, str):
        value = value.encode()
    return _pb_varint((num << 3) | 2) + _pb_varint(len(value)) + value


def pb_encode_frame(frame):
    parts = [
        _pb_uint(1, frame.get("SeqID") or 0),
        _pb_uint(2, frame.get("LogID") or 0),
        _pb_uint(3, frame.get("service") or 0),
        _pb_uint(4, frame.get("method") or 0),
    ]
    for key, value in frame.get("headers") or []:
        parts.append(_pb_bytes(5, _pb_bytes(1, key) + _pb_bytes(2, value)))
    for num, name in ((6, "payload_encoding"), (7, "payload_type")):
        if frame.get(name):
            parts.append(_pb_bytes(num, frame[name]))
    if frame.get("payload"):
        parts.append(_pb_bytes(8, frame["payload"]))
    if frame.get("LogIDNew"):
        parts.append(_pb_bytes(9, frame["LogIDNew"]))
    return b"".join(parts)


def _pb_read_varint(buf, i):
    value = shift = 0
    while True:
        if i >= len(buf):
            raise ValueError("truncated varint")
        b = buf[i]
        i += 1
        value |= (b & 0x7F) << shift
        if not b & 0x80:
            return value, i
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def _pb_decode_header(buf):
    key = value = ""
    i = 0
    while i < len(buf):
        tag, i = _pb_read_varint(buf, i)
        num, wire = tag >> 3, tag & 7
        if wire == 2:
            n, i = _pb_read_varint(buf, i)
            if i + n > len(buf):
                raise ValueError("truncated header")
            v = buf[i:i + n].decode("utf-8", "replace")
            i += n
            if num == 1:
                key = v
            elif num == 2:
                value = v
        elif wire == 0:
            _, i = _pb_read_varint(buf, i)
        else:
            raise ValueError(f"unsupported header wire type {wire}")
    return key, value


def pb_decode_frame(buf):
    frame = {"SeqID": 0, "LogID": 0, "service": 0, "method": 0,
             "headers": [], "payload_encoding": "", "payload_type": "",
             "payload": b"", "LogIDNew": ""}
    i = 0
    while i < len(buf):
        tag, i = _pb_read_varint(buf, i)
        num, wire = tag >> 3, tag & 7
        if wire == 0:
            value, i = _pb_read_varint(buf, i)
            for field, name in ((1, "SeqID"), (2, "LogID"),
                                (3, "service"), (4, "method")):
                if num == field:
                    frame[name] = value
        elif wire == 2:
            n, i = _pb_read_varint(buf, i)
            if i + n > len(buf):
                raise ValueError("truncated field")
            value = buf[i:i + n]
            i += n
            if num == 5:
                frame["headers"].append(_pb_decode_header(value))
            elif num == 6:
                frame["payload_encoding"] = value.decode("utf-8", "replace")
            elif num == 7:
                frame["payload_type"] = value.decode("utf-8", "replace")
            elif num == 8:
                frame["payload"] = value
            elif num == 9:
                frame["LogIDNew"] = value.decode("utf-8", "replace")
        elif wire == 1:
            i += 8
        elif wire == 5:
            i += 4
        else:
            raise ValueError(f"unsupported wire type {wire}")
        if i > len(buf):
            raise ValueError("truncated frame")
    return frame


def _header(frame, name, default=""):
    for k, v in frame.get("headers") or []:
        if k == name:
            return v
    return default


# --------------------------------------------------------------- websocket

class WSClosed(Exception):
    """Connection ended. `code` is the RFC 6455 close status when present."""

    def __init__(self, msg, code=None):
        super().__init__(msg)
        self.code = code


class WSAuthError(WSClosed):
    """The handshake itself was refused (HTTP 4xx): retrying cannot help."""


class WebSocket:
    """Minimal RFC 6455 client: no third-party dependency, matches this repo's
    'one optional dep' posture. Only what the Feishu long connection needs:
    binary frames, ping/pong, close, and defragmentation."""

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
               f"User-Agent: {USER_AGENT}\r\n"
               "Sec-WebSocket-Version: 13\r\n\r\n")
        sock.sendall(req.encode())
        head = b""
        try:
            while b"\r\n\r\n" not in head:
                chunk = sock.recv(4096)
                if not chunk:
                    raise WSClosed("handshake closed")
                head += chunk
            status_line = head.split(b"\r\n")[0]
            if b" 101 " not in status_line:
                msg = "handshake rejected: " + status_line.decode(errors="replace")
                # 4xx is a credential/permission refusal: never retry it.
                text = status_line.decode(errors="replace").split()
                code = int(text[1]) if len(text) > 1 and text[1].isdigit() else 0
                if 400 <= code < 500:
                    raise WSAuthError(msg, code)
                raise WSClosed(msg)
        except Exception:
            try:
                sock.close()
            except OSError:
                pass
            raise
        self._buf = head.partition(b"\r\n\r\n")[2]
        self.sock = sock
        return self

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
                code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else None
                raise WSClosed(f"server closed (code {code})", code)
            parts.append(payload)
            if fin:
                code = opcode if opcode != 0x0 else 0x2
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

    def send_bytes(self, payload):
        self._send_frame(0x2, payload)

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

def _event_key(payload):
    """Dedupe key for one pushed event.

    The docs are explicit: duplicates are possible and must be deduped on
    `message_id`, *not* on `event_id`. Non-message events fall back to the
    event id so they are still consumed once.
    """
    event = payload.get("event") or {}
    message = event.get("message") or {}
    mid = str(message.get("message_id") or "")
    if mid:
        return mid
    header = payload.get("header") or {}
    return str(header.get("event_id") or "")


def _safe_name(value, limit=64):
    return "".join(c for c in str(value) if c.isalnum() or c in "-_.")[:limit]


def inbox_write(payload):
    """Persist one pushed event before it is acked.

    Idempotent on the dedupe key: an index entry means the event is already
    on disk, so a retried push returns False without writing a second file.
    """
    _ensure_dirs()
    key = _event_key(payload)
    if not key:
        return False
    index = load_json(INBOX_INDEX, {})
    if key in index:
        return False
    name = f"{time.time_ns():020d}-{_safe_name(key) or 'event'}.json"
    path = os.path.join(INBOX_DIR, name)
    stored = {"key": key, "payload": payload, "received_at": _now()}
    _atomic_write(path, lambda f: json.dump(stored, f, ensure_ascii=False))
    index[key] = _now()
    if len(index) > INBOX_INDEX_CAP:
        for k in sorted(index, key=lambda k: index[k])[:len(index) - INBOX_INDEX_CAP]:
            index.pop(k, None)
    save_json(INBOX_INDEX, index)
    return True


def inbox_pending():
    try:
        names = sorted(n for n in os.listdir(INBOX_DIR) if n.endswith(".json"))
    except FileNotFoundError:
        return []
    return [os.path.join(INBOX_DIR, n) for n in names]


def _beat():
    """Liveness marker for `poll` / `health`: a quiet receiver is healthy, a
    silent one is only distinguishable by this file going stale."""
    try:
        with open(SERVE_HEARTBEAT, "w") as f:
            f.write(str(_now()))
    except OSError:
        pass


# --------------------------------------------------------------- long conn

def ws_endpoint():
    """Ask the platform where to connect (POST /callback/ws/endpoint)."""
    app_id, app_secret = _credentials()
    if not app_id:
        raise RuntimeError("NO_CREDENTIALS: set FEISHU_APP_ID/FEISHU_APP_SECRET "
                           "or app_id/app_secret in data/config.json")
    body = json.dumps({"AppID": app_id, "AppSecret": app_secret}).encode()
    req = urllib.request.Request(EDGE, data=body, method="POST", headers={
        "Content-Type": "application/json", "locale": "zh",
        "User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read().decode()
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        raise RuntimeError(f"WS_ENDPOINT_ERROR:http-{e.code}:{_brief(raw)}")
    try:
        payload = json.loads(raw)
    except ValueError:
        raise RuntimeError(f"WS_ENDPOINT_ERROR:unparseable:{_brief(raw)}")
    code = payload.get("code")
    if code:
        raise RuntimeError(f"WS_ENDPOINT_ERROR:{code}:{payload.get('msg')}"
                           + (":FATAL" if code in FATAL_ENDPOINT_CODES else ""))
    data = payload.get("data") or {}
    url = data.get("URL")
    if not url:
        raise RuntimeError("WS_ENDPOINT_ERROR:no-url")
    return url, data.get("ClientConfig") or {}


def _ping_frame(service_id):
    return {"SeqID": 0, "LogID": 0, "service": service_id, "method": 0,
            "headers": [("type", "ping")], "payload": b""}


def _ack_frame(frame, code=200):
    """Echo the event frame back with the HTTP-style result the platform
    expects. The SDK reuses the received frame and adds `biz_rt`; keeping
    the same shape means the server matches the ack to the push."""
    out = dict(frame)
    out["headers"] = list(frame.get("headers") or []) + [("biz_rt", "0")]
    out["payload"] = json.dumps({"code": code}, separators=(",", ":")).encode()
    return out


def _service_id_from(url):
    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    svc = (query.get("service_id") or [0])[0]
    try:
        return int(svc)
    except (TypeError, ValueError):
        return 0


def _combine_payload(cache, frame):
    """Reassemble a payload split across frames (headers sum/seq). Returns
    None until all parts have arrived."""
    parts = int(_header(frame, "sum") or 1)
    if parts <= 1:
        return frame.get("payload") or b""
    msg_id = _header(frame, "message_id")
    seq = int(_header(frame, "seq") or 0)
    now = time.time()
    for key in [k for k, v in cache.items() if now - v[0] > DEDUPE_SECONDS]:
        cache.pop(key, None)
    entry = cache.get(msg_id)
    if entry is None:
        entry = [now, [None] * parts]
        cache[msg_id] = entry
    entry[0] = now
    if 0 <= seq < len(entry[1]):
        entry[1][seq] = frame.get("payload") or b""
    if any(p is None for p in entry[1]):
        return None
    cache.pop(msg_id, None)
    return b"".join(entry[1])


def cmd_serve(once=False, max_retries=None):
    """Hold the long connection and make every event durable before acking."""
    _ensure_dirs()
    failures = 0
    combine_cache = {}
    config = load_json(WS_FILE, {}).get("config") or {}
    while True:
        ws = None
        try:
            url, fresh = ws_endpoint()
            if fresh:
                config = fresh
                save_json(WS_FILE, {"url": url, "config": config, "at": _now()})
            service_id = _service_id_from(url)
            ws = WebSocket(url, timeout=60).connect()
            save_json(WS_FILE, {"url": url, "config": config,
                                "service_id": service_id, "at": _now()})
            _beat()
            ping_interval = float(config.get("PingInterval") or 120)
            next_ping = time.time() + ping_interval
            while True:
                try:
                    _, raw = ws.recv_message(deadline=next_ping)
                except socket.timeout:
                    ws.send_bytes(pb_encode_frame(_ping_frame(service_id)))
                    next_ping = time.time() + ping_interval
                    # A quiet connection must still look alive: `poll` reads
                    # this file, and an idle-but-healthy receiver that stopped
                    # beating would be reported as dead.
                    _beat()
                    continue
                try:
                    frame = pb_decode_frame(raw)
                except ValueError as e:
                    log_err(f"frame decode failed: {e}")
                    continue
                if frame["method"] == 0:        # control
                    if _header(frame, "type") == "pong" and frame.get("payload"):
                        try:
                            config = json.loads(frame["payload"].decode())
                            ping_interval = float(config.get("PingInterval") or ping_interval)
                            save_json(WS_FILE, {"url": url, "config": config,
                                                "service_id": service_id, "at": _now()})
                        except (ValueError, AttributeError):
                            pass
                    continue
                if frame["method"] != 1:        # only DATA frames carry events
                    continue
                if _header(frame, "type") != "event":
                    continue
                payload_bytes = _combine_payload(combine_cache, frame)
                if payload_bytes is None:
                    continue                    # waiting for the remaining parts
                try:
                    payload = json.loads(payload_bytes.decode())
                except (ValueError, UnicodeDecodeError) as e:
                    log_err(f"event payload unparseable ({e}); acked as bad")
                    ws.send_bytes(pb_encode_frame(_ack_frame(frame, 500)))
                    continue
                # File first, ack second. A failed write means no ack, so the
                # platform retries instead of considering the event lost.
                try:
                    inbox_write(payload)
                except OSError as e:
                    log_err(f"inbox write failed ({e}); not acking")
                    continue
                ws.send_bytes(pb_encode_frame(_ack_frame(frame, 200)))
                _beat()
                if once:
                    return 0
        except WSAuthError as e:
            log_err(f"long connection refused: {e}; not retrying "
                    "(credentials or app settings)")
            return 1
        except RuntimeError as e:
            msg = str(e)
            if "NO_CREDENTIALS" in msg:
                log_err(msg)
                return 1
            if msg.endswith(":FATAL"):
                log_err(msg[:-len(":FATAL")] + "; not retrying")
                return 1
            failures += 1
            if max_retries is not None and failures >= max_retries:
                log_err(f"long connection: {e}; giving up after {failures} attempts")
                return 1
            log_err(f"long connection: {e}; reconnecting")
            time.sleep(_backoff(config, failures))
        except (WSClosed, OSError, socket.timeout) as e:
            failures += 1
            if max_retries is not None and failures >= max_retries:
                log_err(f"long connection: {e}; giving up after {failures} attempts")
                return 1
            log_err(f"long connection: {e}; reconnecting")
            time.sleep(_backoff(config, failures))
        except KeyboardInterrupt:
            return 0
        finally:
            # Every exit path closes the socket: a leaked one would keep a
            # half-open connection alive and burn the app's connection quota.
            if ws is not None:
                ws.close()
                ws = None


def _backoff(config, failures):
    """Reconnect delay: the platform sends an interval in ClientConfig, but a
    hook deployment wants to recover in seconds, not minutes. Clamp it, and
    grow it while the same failure repeats."""
    try:
        base = float(config.get("ReconnectInterval") or 3)
    except (TypeError, ValueError):
        base = 3.0
    return max(1.0, min(base * (2 ** min(max(failures - 1, 0), 4)), 30.0))


# ------------------------------------------------------------------- poll

def _strip_mentions(text, mentions):
    for mention in mentions or []:
        key = (mention or {}).get("key")
        if key:
            text = text.replace(key, "")
    return re.sub(r"[ \t]{2,}", " ", text).strip()


def _post_text(obj):
    body = obj.get("zh_cn") or obj.get("en_us") or obj.get("ja_jp") or obj
    if not isinstance(body, dict):
        return ""
    lines = []
    title = body.get("title") or ""
    if title:
        lines.append(str(title))
    for paragraph in body.get("content") or []:
        buf = []
        for node in paragraph or []:
            if not isinstance(node, dict):
                continue
            tag = node.get("tag")
            if tag in ("text", "a", "at"):
                buf.append(str(node.get("text") or ""))
            elif tag == "img":
                buf.append("[图片]")
            elif tag == "media":
                buf.append("[视频]")
            elif tag == "emotion":
                buf.append(str(node.get("emoji_type") or ""))
        line = "".join(buf).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def _content(msg):
    """Return (kind, text, media_key, file_name) for one message body."""
    mtype = str(msg.get("message_type") or "")
    raw = msg.get("content") or "{}"
    try:
        obj = json.loads(raw)
    except ValueError:
        obj = {}
    if not isinstance(obj, dict):
        obj = {}
    if mtype == "text":
        return "text", str(obj.get("text") or ""), None, None
    if mtype == "post":
        return "post", _post_text(obj), None, None
    if mtype == "image":
        return "image", "[图片]", str(obj.get("image_key") or ""), None
    if mtype == "file":
        return "file", "[文件]", str(obj.get("file_key") or ""), \
            str(obj.get("file_name") or "")
    if mtype == "audio":
        return "voice", "[语音]", str(obj.get("file_key") or ""), None
    if mtype == "media":
        return "video", "[视频]", str(obj.get("file_key") or ""), None
    if mtype == "sticker":
        return "sticker", "[表情]", None, None
    if mtype == "interactive":
        return "card", "[卡片]", None, None
    return "other", f"[{mtype or 'unknown'}]", None, None


def _download(message_id, key, kind, file_name):
    if not key:
        return None, "no key"
    resource_type = "image" if kind == "image" else "file"
    path = (f"/im/v1/messages/{urllib.parse.quote(message_id, safe='')}"
            f"/resources/{urllib.parse.quote(key, safe='')}?type={resource_type}")
    try:
        blob, ctype = api_raw(path)
    except Exception as e:
        return None, _brief(str(e), 100)
    ext = os.path.splitext(file_name or "")[1]
    if not ext:
        ext = {"image/png": ".png", "image/jpeg": ".jpg", "image/gif": ".gif",
               "image/webp": ".webp", "application/pdf": ".pdf"}.get(
            (ctype or "").split(";")[0], ".bin")
    safe = _safe_name(message_id, 32) or "msg"
    dest = os.path.join(MEDIA_DIR, f"{safe}_{_safe_name(key, 24)}{ext}")
    try:
        os.makedirs(MEDIA_DIR, exist_ok=True)
        with open(dest, "wb") as f:
            f.write(blob)
    except OSError as e:
        return None, str(e)[:100]
    return dest, None


def _normalize(stored):
    """Map one stored event to the worker-facing entry shape used by
    AGENTS.md (from_user_id / message_id / kind / text / ...)."""
    payload = stored.get("payload") or {}
    event = payload.get("event") or {}
    message = event.get("message") or {}
    sender = event.get("sender") or {}
    msg_id = str(message.get("message_id") or "")
    if not msg_id:
        return None, None
    sender_id = sender.get("sender_id") or {}
    from_user = str(sender_id.get("open_id") or "")
    # Feishu calls a DM "p2p"; every channel here speaks "c2c" to the worker
    # (examples/CHANNELS.md §1), so the platform's vocabulary stops at this
    # boundary. Group is the only other value the platform sends.
    chat_raw = str(message.get("chat_type") or "p2p")
    chat = "c2c" if chat_raw == "p2p" else "group"
    chat_id = str(message.get("chat_id") or "")
    entry = {
        "from_user_id": from_user,
        "message_id": msg_id,
        "chat_type": chat,
        "chat_id": chat_id,
        # For a group the reply target is the chat; for a DM the sender.
        "reply_to": chat_id if chat == "group" else from_user,
        "timestamp": message.get("create_time"),
        "from_bot": str(sender.get("sender_type") or "") == "bot",
    }
    kind, text, media_key, file_name = _content(message)
    text = _strip_mentions(text, message.get("mentions"))
    entry["kind"] = kind
    entry["text"] = text
    if file_name:
        entry["file_name"] = file_name
    if media_key and kind in ("image", "file", "voice", "video"):
        path, err = _download(msg_id, media_key, kind, file_name)
        if path:
            entry["media_path"] = path
        elif err:
            entry["media_error"] = err
    parent = str(message.get("parent_id") or "")
    if parent:
        # The API needs another round-trip to fetch the quoted body; the id
        # is surfaced so the worker can at least say what it is replying to.
        entry["quoted_message_id"] = parent
    return entry, chat


def cmd_poll():
    """Read the durable inbox. Never advances state: that is `commit`'s job."""
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
        age = "never" if heartbeat_age is None else f"{heartbeat_age}s"
        print(f"POLL_ERROR:long-connection receiver stale (heartbeat {age})")
        return 1
    if receiver_dead:
        log_err(f"receiver stale (heartbeat {heartbeat_age}s); draining "
                f"{len(files)} already-persisted event(s)")
    # Presence check only — never a network round-trip here. A missing
    # credential is still visible, because no reply could ever be sent.
    if not _credentials()[0]:
        print("NO_CREDENTIALS: set FEISHU_APP_ID/FEISHU_APP_SECRET "
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
    batch_seen = set()
    for path in files:
        if len(consumed) >= POLL_BATCH_CAP * 4 or len(msgs) >= POLL_BATCH_CAP:
            break
        stored = load_json(path, {})
        if not stored:
            consumed.append(os.path.basename(path))
            continue
        entry, chat = _normalize(stored)
        if entry is None:
            consumed.append(os.path.basename(path))
            continue
        msg_id = entry["message_id"]
        consumed.append(os.path.basename(path))
        # Duplicates are expected (the platform retries), and the same batch
        # can contain the same message twice: filter both cases here.
        if msg_id in seen or msg_id in batch_seen:
            continue
        batch_seen.add(msg_id)
        ids.append(msg_id)
        if entry.get("from_bot"):
            continue
        allowed = entry["from_user_id"] in owners
        if chat == "group" and not allowed:
            allowed = allow_all or entry["chat_id"] in groups
        if not allowed:
            _note_unknown(entry["from_user_id"], chat)
            dropped.append(entry["from_user_id"])
            continue
        entry.pop("from_bot", None)
        msgs.append(entry)
    print(json.dumps({"msgs": msgs,
                      "filtered": {"dropped": len(set(dropped)),
                                   "senders": sorted(set(dropped))},
                      "commit": {"cursor": "", "ids": ids,
                                 "files": consumed}},
                     ensure_ascii=False))
    return 0


def cmd_commit():
    """Record delivered ids, release the inbox files, and persist a cursor
    if the block carries one (Feishu has no cursor of its own; the ack frame
    already advanced the platform's view). Idempotent: re-running the same
    block is a no-op."""
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
    index = load_json(INBOX_INDEX, {})
    if index:
        for mid in ids:
            index.pop(mid, None)
        save_json(INBOX_INDEX, index)
    print(f"COMMITTED {len(ids)} ids, {moved} files")
    return 0


# ------------------------------------------------------------------- send

def _ledger_load():
    return load_json(SEND_LEDGER_FILE, {})


class _LedgerLock:
    """Inter-process lock around read-modify-write of the send ledger."""

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
        with open(SEND_AUDIT_FILE, "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        with open(SEND_AUDIT_FILE) as f:
            lines = f.readlines()
        if len(lines) > AUDIT_CAP:
            with open(SEND_AUDIT_FILE + ".tmp", "w") as f:
                f.writelines(lines[-AUDIT_CAP:])
            os.replace(SEND_AUDIT_FILE + ".tmp", SEND_AUDIT_FILE)
    except OSError:
        pass


def _uuid_from_key(key):
    """Deterministic request-dedupe id for one bubble.

    Feishu dedupes `(uuid)` for one hour, content included, so the same key
    must mean the same bubble. The platform caps uuid at 50 characters; a
    bubble key like `om_xxx-1` fits, anything longer is hashed.
    """
    if key and len(key) <= 50 and re.fullmatch(r"[A-Za-z0-9_.:-]+", key):
        return key
    return "fb" + hashlib.sha1(key.encode()).hexdigest()[:40]


def _receive_id_type(receive_id):
    if receive_id.startswith("oc_"):
        return "chat_id"
    if receive_id.startswith("on_"):
        return "union_id"
    if "@" in receive_id:
        return "email"
    return "open_id"


def cmd_send(to, ctx, text, chat=None, key=None):
    if not to:
        print("SEND_BAD_TARGET")
        return 1
    if text in (None, ""):
        print("SEND_EMPTY")
        return 1
    if len(text) > MAX_TEXT:
        text = text[:MAX_TEXT]
    bubble_key = key or f"{ctx or to}-1"
    with _LedgerLock(SEND_LEDGER_LOCK_FILE):
        ledger = _ledger_load()
        now_ts = _now()
        ledger = {k: v for k, v in ledger.items()
                  if not (isinstance(v, dict)
                          and now_ts - int(v.get("at") or 0) > LEDGER_TTL)}
        entry = ledger.get(bubble_key)
        if isinstance(entry, dict) and entry.get("status") == "sent":
            print("SEND_ALREADY_SENT")
            return 0
        uuid = (entry or {}).get("uuid") or _uuid_from_key(bubble_key)
        # Persist the intent (uuid) BEFORE calling out: the uuid is the
        # server-side dedupe key, so a crash after this line can only cause a
        # retry the server rejects within the hour, never a duplicate bubble.
        ledger[bubble_key] = {"to": to, "msg_id": ctx or "", "uuid": uuid,
                              "chat": chat or "", "status": "intent",
                              "at": now_ts}
        save_json(SEND_LEDGER_FILE, ledger)

    body = {"content": json.dumps({"text": text}, ensure_ascii=False),
            "msg_type": "text", "uuid": uuid}
    if ctx:
        path = (f"/im/v1/messages/{urllib.parse.quote(ctx, safe='')}/reply")
        target_kind = "reply"
    else:
        rid_type = chat or _receive_id_type(to)
        if rid_type in ("p2p", "group"):
            rid_type = "open_id" if rid_type == "p2p" else "chat_id"
        path = f"/im/v1/messages?receive_id_type={rid_type}"
        body["receive_id"] = to
        target_kind = "create"

    ok, err = False, None
    for attempt in (1, 2):
        try:
            result = api("POST", path, body, timeout=15)
            ok = True
            break
        except ApiError as e:
            err = e
            if e.code in RATE_LIMIT_CODES and attempt == 1:
                time.sleep(1.0)
                continue
            break
        except Exception as e:      # network, timeout, TLS
            err = e
            if attempt == 1:
                time.sleep(0.5)
                continue
            break

    if not ok:
        print(f"SEND_FAILED:{err}")
        return 1
    with _LedgerLock(SEND_LEDGER_LOCK_FILE):
        ledger = _ledger_load()
        item = ledger.get(bubble_key) or {}
        item.update({"status": "sent", "at": _now(),
                     "message_id": ((result or {}).get("data") or {}).get("message_id")})
        ledger[bubble_key] = item
        save_json(SEND_LEDGER_FILE, ledger)
    _audit({"at": now_ts, "key": bubble_key, "kind": target_kind,
            "to": to, "ctx": ctx or "", "ok": True, "chars": len(text)})
    print("SEND_OK")
    return 0


# ------------------------------------------------------------------- bind

def cmd_bind(wait_secs=300):
    """One binding window: the first DM sender becomes owner, in code.

    A fresh app has no owner id yet; this mirrors `ilink.py bind` so a new
    deployment can be claimed by whoever messages the bot first, and by
    nobody afterwards.
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
                stored = load_json(path, {})
                entry, chat = _normalize(stored)
                if entry is None or chat != "c2c" or not entry.get("from_user_id"):
                    continue
                if load_json(OWNER_FILE, {}).get("owner_id"):
                    print("ALREADY_BOUND")
                    return 1
                sender = entry["from_user_id"]
                save_json(OWNER_FILE, {"owner_id": sender, "bound_at": _now()})
                print(f"BOUND:{sender}")
                # Welcome goes out as a reply, so no proactive-message limits
                # apply; the key makes a crash-and-rerun idempotent.
                cmd_send(sender, entry["message_id"],
                         "已绑定。之后直接说话就行。", key=f"{entry['message_id']}-1")
                return 0
            time.sleep(2)
    print("BIND_TIMEOUT")
    return 1


# ----------------------------------------------------------------- status

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
    if len(data) > 200:
        for k in sorted(data, key=lambda k: data[k].get("at", 0))[:len(data) - 200]:
            data.pop(k, None)
    save_json(UNKNOWN_FILE, data)


def cmd_status():
    ws = load_json(WS_FILE, {})
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
        "api": API,
        "token": "ok" if tok.get("access_token") else "missing",
        "token_expires_in": max(0, int(tok.get("expires_at") or 0) - _now()),
        "connection": "present" if ws.get("url") else "none",
        "connection_age": (_now() - int(ws.get("at") or 0)) if ws.get("at") else None,
        "service_id": ws.get("service_id"),
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
        problems.append("no credentials (FEISHU_APP_ID/FEISHU_APP_SECRET)")
    else:
        try:
            get_token()
        except Exception as e:
            problems.append(str(e))
    try:
        with open(SERVE_HEARTBEAT) as f:
            age = _now() - int(f.read().strip() or 0)
        if age > HEARTBEAT_STALE:
            problems.append(f"long-connection receiver stale ({age}s)")
    except Exception:
        problems.append("long-connection receiver never ran")
    if not _owner_sets()[0]:
        warns.append("no owner bound (run `feishubot.py bind`)")
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
            print("usage: feishubot.py send <to> <ctx|''|-> <text|->"
                  " [--chat|--p2p] [--key <id>]")
            return 2
        chat = None
        if _flag(args, "--chat"):
            chat = "chat_id"
        elif _flag(args, "--p2p"):
            chat = "open_id"
        ctx = args[1]
        if ctx == "-":
            ctx = ""
        # Same convention as ilink.py / qqbot.py: a bare "-" reads the body
        # from stdin, so a worker can pipe a long reply without argv limits.
        text = sys.stdin.read() if args[2] == "-" else args[2]
        return cmd_send(args[0], ctx, text, chat,
                        _flag(args, "--key", True))
    if cmd == "bind":
        wait = 300
        if args and args[0].isdigit():
            wait = int(args[0])
        return cmd_bind(wait)
    if cmd == "status":
        return cmd_status()
    if cmd == "health":
        return cmd_health()
    print(f"unknown command: {cmd}")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
