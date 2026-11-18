#!/usr/bin/env python3
"""Tests for the Feishu long-connection loop in feishubot.py.

`serve` is the one part of the adapter the hook tests stub out, and it is
where the platform's rules actually bite:

  * an event is acked only after it is on disk — an ack the platform got
    before the write could lose the event on a crash
  * a redelivered event (same message_id) does not create a second file
  * a payload split across frames (headers sum/seq) is reassembled and
    acked once
  * a malformed payload is acked with code 500 and does not crash the loop
  * pings go out on the ClientConfig interval and a pong's payload updates
    the stored config
  * a refused handshake (HTTP 4xx) and a fatal endpoint code (403) stop
    `serve` immediately instead of feeding a reconnect loop

The fake server here implements the protocol independently: it builds and
parses frames with its own small codec, so a bug in feishubot's hand-written
protobuf cannot make these tests pass by agreeing with itself.

    python3 -m unittest discover -s tests -v
"""
import base64
import hashlib
import importlib.util
import json
import os
import shutil
import socket
import struct
import tempfile
import threading
import time
import traceback
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "examples", "feishu-bot", "feishubot.py")


# ------------------------------------------------- independent frame codec

def _varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _enc_frame(seq, log, service, method, headers, payload=b""):
    parts = [_varint(1 << 3) + _varint(seq),
             _varint(2 << 3) + _varint(log),
             _varint(3 << 3) + _varint(service),
             _varint(4 << 3) + _varint(method)]
    for k, v in headers:
        kb, vb = k.encode(), v.encode()
        inner = (_varint((1 << 3) | 2) + _varint(len(kb)) + kb
                 + _varint((2 << 3) | 2) + _varint(len(vb)) + vb)
        parts.append(_varint((5 << 3) | 2) + _varint(len(inner)) + inner)
    if payload:
        parts.append(_varint((8 << 3) | 2) + _varint(len(payload)) + payload)
    return b"".join(parts)


def _read_varint(buf, i):
    value = shift = 0
    while True:
        b = buf[i]
        i += 1
        value |= (b & 0x7F) << shift
        if not b & 0x80:
            return value, i
        shift += 7


def _dec_frame(buf):
    out = {"seq": 0, "log": 0, "service": 0, "method": 0,
           "headers": [], "payload": b""}
    i = 0
    while i < len(buf):
        tag, i = _read_varint(buf, i)
        num, wire = tag >> 3, tag & 7
        if wire == 0:
            v, i = _read_varint(buf, i)
            if num in (1, 2, 3, 4):
                out[{1: "seq", 2: "log", 3: "service", 4: "method"}[num]] = v
        elif wire == 2:
            n, i = _read_varint(buf, i)
            v = buf[i:i + n]
            i += n
            if num == 5:
                hk = hv = ""
                j = 0
                while j < len(v):
                    t2, j = _read_varint(v, j)
                    n2, j = _read_varint(v, j)
                    s = v[j:j + n2].decode()
                    j += n2
                    if t2 >> 3 == 1:
                        hk = s
                    elif t2 >> 3 == 2:
                        hv = s
                out["headers"].append((hk, hv))
            elif num == 8:
                out["payload"] = v
        else:
            raise AssertionError(f"unexpected wire type {wire}")
    return out


def _hdr(frame, name, default=""):
    for k, v in frame["headers"]:
        if k == name:
            return v
    return default


# ------------------------------------------------------- fake server bits

def _recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    return buf


def _recv_client_frame(conn):
    b1, b2 = _recv_exact(conn, 2)
    assert b1 & 0x0F == 0x2, f"expected binary frame, got opcode {b1 & 0x0F}"
    masked = b2 & 0x80
    n = b2 & 0x7F
    if n == 126:
        n = struct.unpack("!H", _recv_exact(conn, 2))[0]
    elif n == 127:
        n = struct.unpack("!Q", _recv_exact(conn, 8))[0]
    mask = _recv_exact(conn, 4) if masked else b""
    payload = _recv_exact(conn, n) if n else b""
    if masked:
        payload = bytes(payload[i] ^ mask[i % 4] for i in range(len(payload)))
    return payload


def _server_frame(payload, opcode=0x2):
    n = len(payload)
    if n < 126:
        return struct.pack("!BB", 0x80 | opcode, n) + payload
    if n < (1 << 16):
        return struct.pack("!BBH", 0x80 | opcode, 126, n) + payload
    return struct.pack("!BBQ", 0x80 | opcode, 127, n) + payload


class FakeFeishu:
    """Serves POST /callback/ws/endpoint, then a scripted WebSocket.

    `endpoint(port)` returns the JSON body for discovery. `handler(conn,
    key, server)` runs after a successful upgrade; the default just closes.
    """

    def __init__(self, endpoint=None, handler=None):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.sock.settimeout(0.2)
        self.port = self.sock.getsockname()[1]
        self.endpoint = endpoint or (lambda port: {
            "code": 0, "msg": "ok",
            "data": {"URL": f"ws://127.0.0.1:{port}/ws?device_id=1&service_id=7",
                     "ClientConfig": {"ReconnectCount": -1,
                                      "ReconnectInterval": 1,
                                      "ReconnectNonce": 30,
                                      "PingInterval": 120}}})
        self.handler = handler
        self.reject = None
        self.ws_connections = 0
        self.endpoint_calls = 0
        self.errors = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while not self.stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except (socket.timeout, OSError):
                continue
            conn.settimeout(10)
            try:
                head = b""
                while b"\r\n\r\n" not in head:
                    chunk = conn.recv(4096)
                    if not chunk:
                        raise ConnectionError("closed")
                    head += chunk
                lines = head.split(b"\r\n")
                request = lines[0].decode(errors="replace")
                headers = {}
                for line in lines[1:]:
                    if b":" in line:
                        k, v = line.split(b":", 1)
                        headers[k.strip().lower()] = v.strip()
                # Drain the request body. Closing a socket that still has
                # unread data makes Windows send RST, and on loopback that RST
                # can hit the client's *next* connection (which often reuses
                # the same ephemeral port) — that is the whole reason this
                # read exists.
                length = int(headers.get(b"content-length", b"0") or 0)
                leftover = head.partition(b"\r\n\r\n")[2]
                if length > len(leftover):
                    _recv_exact(conn, length - len(leftover))
                if headers.get(b"upgrade", b"").lower() == b"websocket":
                    self.ws_connections += 1
                    if self.reject is not None:
                        # Refuse the upgrade itself. The client must treat a
                        # 4xx here as fatal, not as a droppable connection.
                        conn.sendall(self.reject)
                        time.sleep(0.2)
                        continue
                    key = headers.get(b"sec-websocket-key", b"")
                    accept = base64.b64encode(hashlib.sha1(
                        key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                    ).digest()).decode()
                    conn.sendall(("HTTP/1.1 101 Switching Protocols\r\n"
                                  "Upgrade: websocket\r\n"
                                  "Connection: Upgrade\r\n"
                                  f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
                                  ).encode())
                    if self.handler is not None:
                        try:
                            self.handler(conn, key, self)
                        except Exception:
                            # A scripted handler failing must surface as a
                            # test failure, not as a mysterious client RST.
                            self.errors.append(traceback.format_exc())
                    else:
                        time.sleep(0.1)
                else:
                    self.endpoint_calls += 1
                    body = json.dumps(self.endpoint(self.port)).encode()
                    conn.sendall(b"HTTP/1.1 200 OK\r\n"
                                 b"Content-Type: application/json\r\n"
                                 + f"Content-Length: {len(body)}\r\n\r\n".encode()
                                 + body)
            except (ConnectionError, OSError, socket.timeout):
                pass
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

    def refuse(self, status_line=b"HTTP/1.1 401 Unauthorized\r\n\r\n"):
        """Refuse the upgrade handshake instead of completing it."""
        self.reject = status_line

    def close(self):
        self.stop.set()
        try:
            self.sock.close()
        except OSError:
            pass
        self.thread.join(timeout=5)


def load_feishubot(data_dir, domain):
    os.environ["FEISHU_DATA_DIR"] = data_dir
    os.environ["FEISHU_APP_ID"] = "cli_test"
    os.environ["FEISHU_APP_SECRET"] = "test-secret"
    os.environ["FEISHU_DOMAIN"] = domain
    spec = importlib.util.spec_from_file_location(
        f"feishubot_ws_{abs(hash((data_dir, domain)))}", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._ensure_dirs()
    return mod


def event_body(msg_id, chat="p2p"):
    return {
        "schema": "2.0",
        "header": {"event_id": f"ev_{msg_id}",
                   "event_type": "im.message.receive_v1"},
        "event": {
            "sender": {"sender_id": {"open_id": "ou_owner1"},
                       "sender_type": "user"},
            "message": {"message_id": msg_id,
                        "chat_id": "oc_group1" if chat == "group" else "oc_p2p",
                        "chat_type": chat, "message_type": "text",
                        "content": json.dumps({"text": "hi"}),
                        "create_time": "1700000000000"},
        },
    }


class WsTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="feishubot-ws-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.servers = []
        self.addCleanup(lambda: [s.close() for s in self.servers])

    def boot(self, handler=None, endpoint=None):
        srv = FakeFeishu(handler=handler, endpoint=endpoint)
        self.servers.append(srv)
        self.fb = load_feishubot(self.dir, f"http://127.0.0.1:{srv.port}")
        return srv

    def inbox_files(self):
        try:
            return sorted(n for n in os.listdir(self.fb.INBOX_DIR)
                          if n.endswith(".json"))
        except FileNotFoundError:
            return []

    def wait_done(self, srv, done):
        """Wait for the scripted exchange to finish, surfacing server-side
        failures instead of letting them look like a client disconnect."""
        ok = done.wait(5)
        self.assertFalse(srv.errors, "\n".join(srv.errors))
        self.assertTrue(ok, "scripted handler never finished")


class DurabilityTest(WsTestBase):
    def test_event_is_on_disk_before_the_ack(self):
        seen = {}
        done = threading.Event()

        def handler(conn, key, srv):
            body = json.dumps(event_body("om_1")).encode()
            conn.sendall(_server_frame(_enc_frame(
                1, 1, 7, 1,
                [("type", "event"), ("message_id", "om_1"),
                 ("sum", "1"), ("seq", "0")], body)))
            ack = _dec_frame(_recv_client_frame(conn))
            # Read the ack, then look at the disk: the file must already be
            # there, because the client writes before it acks.
            seen["ack"] = ack
            seen["files_at_ack"] = self.inbox_files()
            done.set()
            time.sleep(0.2)

        srv = self.boot(handler)
        self.assertEqual(self.fb.cmd_serve(once=True), 0)
        # The client returns as soon as it acks; the server thread may still
        # be finishing the read, so wait for its observation.
        self.wait_done(srv, done)
        self.assertEqual(len(seen["files_at_ack"]), 1)
        self.assertEqual(json.loads(seen["ack"]["payload"]), {"code": 200})
        self.assertEqual(_hdr(seen["ack"], "biz_rt"), "0")
        self.assertEqual(seen["ack"]["seq"], 1)
        stored = self.fb.load_json(
            os.path.join(self.fb.INBOX_DIR, seen["files_at_ack"][0]), {})
        self.assertEqual(stored["key"], "om_1")
        self.assertEqual(stored["payload"]["event"]["message"]["message_id"],
                         "om_1")
        # The connection details are remembered for `status` / diagnostics.
        ws = self.fb.load_json(self.fb.WS_FILE, {})
        self.assertEqual(ws.get("service_id"), 7)

    def test_redelivery_does_not_duplicate_the_inbox(self):
        acks = []
        done = threading.Event()

        def handler(conn, key, srv):
            body = json.dumps(event_body("om_1")).encode()
            frame = _enc_frame(1, 1, 7, 1,
                               [("type", "event"), ("message_id", "om_1"),
                                ("sum", "1"), ("seq", "0")], body)
            conn.sendall(_server_frame(frame))
            acks.append(_dec_frame(_recv_client_frame(conn)))
            conn.sendall(_server_frame(frame))      # same message again
            acks.append(_dec_frame(_recv_client_frame(conn)))
            done.set()
            time.sleep(0.2)

        srv = self.boot(handler)
        rc = self.fb.cmd_serve(once=False, max_retries=1)
        self.assertEqual(rc, 1)     # the server hangs up after the exchange
        self.wait_done(srv, done)
        self.assertEqual([json.loads(a["payload"]) for a in acks],
                         [{"code": 200}, {"code": 200}])
        self.assertEqual(len(self.inbox_files()), 1)

    def test_split_payload_is_reassembled_and_acked_once(self):
        acks = []
        done = threading.Event()
        body = json.dumps(event_body("om_split")).encode()
        third = len(body) // 3 + 1
        parts = [body[0:third], body[third:2 * third], body[2 * third:]]

        def handler(conn, key, srv):
            for seq, part in enumerate(parts):
                conn.sendall(_server_frame(_enc_frame(
                    seq + 1, 1, 7, 1,
                    [("type", "event"), ("message_id", "om_split"),
                     ("sum", "3"), ("seq", str(seq))], part)))
                time.sleep(0.05)
            acks.append(_dec_frame(_recv_client_frame(conn)))
            done.set()
            time.sleep(0.2)

        srv = self.boot(handler)
        self.assertEqual(self.fb.cmd_serve(once=True), 0)
        self.wait_done(srv, done)
        self.assertEqual(len(acks), 1, "split payload must be acked once")
        self.assertEqual(len(self.inbox_files()), 1)
        stored = self.fb.load_json(
            os.path.join(self.fb.INBOX_DIR, self.inbox_files()[0]), {})
        self.assertEqual(stored["payload"]["event"]["message"]["message_id"],
                         "om_split")

    def test_malformed_payload_is_acked_500_and_not_stored(self):
        acks = []
        done = threading.Event()

        def handler(conn, key, srv):
            conn.sendall(_server_frame(_enc_frame(
                1, 1, 7, 1,
                [("type", "event"), ("message_id", "om_bad"),
                 ("sum", "1"), ("seq", "0")], b"{not json")))
            acks.append(_dec_frame(_recv_client_frame(conn)))
            done.set()
            time.sleep(0.2)

        srv = self.boot(handler)
        rc = self.fb.cmd_serve(once=False, max_retries=1)
        self.assertEqual(rc, 1)
        self.wait_done(srv, done)
        self.assertEqual(json.loads(acks[0]["payload"]), {"code": 500})
        self.assertEqual(self.inbox_files(), [])


class PingTest(WsTestBase):
    def test_ping_goes_out_and_pong_updates_the_config(self):
        pings = []

        def endpoint(port):
            return {"code": 0, "msg": "ok", "data": {
                "URL": f"ws://127.0.0.1:{port}/ws?device_id=1&service_id=7",
                # Half a second: long enough that the event below arrives
                # before a second ping could be due.
                "ClientConfig": {"ReconnectInterval": 1, "PingInterval": 0.5}}}

        def handler(conn, key, srv):
            pings.append(_dec_frame(_recv_client_frame(conn)))
            pong = json.dumps({"PingInterval": 5}).encode()
            conn.sendall(_server_frame(_enc_frame(
                0, 0, 7, 0, [("type", "pong")], pong)))
            conn.sendall(_server_frame(_enc_frame(
                2, 2, 7, 1,
                [("type", "event"), ("message_id", "om_1"),
                 ("sum", "1"), ("seq", "0")],
                json.dumps(event_body("om_1")).encode())))
            _recv_client_frame(conn)
            time.sleep(0.2)

        srv = self.boot(handler, endpoint)
        self.assertEqual(self.fb.cmd_serve(once=True), 0)
        self.assertFalse(srv.errors, "\n".join(srv.errors))
        self.assertGreaterEqual(len(pings), 1)
        self.assertEqual(pings[0]["method"], 0)
        self.assertEqual(_hdr(pings[0], "type"), "ping")
        self.assertEqual(pings[0]["service"], 7)
        self.assertEqual(
            self.fb.load_json(self.fb.WS_FILE, {})["config"]["PingInterval"], 5)


class FatalErrorTest(WsTestBase):
    def test_refused_handshake_ends_serve_without_retrying(self):
        srv = self.boot()
        srv.refuse()
        started = time.time()
        # max_retries=2 makes a wrong implementation terminate too: what
        # proves "no retry" is the attempt count, not the wall clock.
        self.assertEqual(self.fb.cmd_serve(once=False, max_retries=2), 1)
        self.assertEqual(srv.ws_connections, 1)
        self.assertEqual(srv.endpoint_calls, 1)
        self.assertLess(time.time() - started, 10)

    def test_fatal_endpoint_code_ends_serve_without_connecting(self):
        srv = self.boot(endpoint=lambda port: {
            "code": 403, "msg": "forbidden", "data": {}})
        started = time.time()
        self.assertEqual(self.fb.cmd_serve(once=False, max_retries=2), 1)
        self.assertEqual(srv.endpoint_calls, 1)
        self.assertEqual(srv.ws_connections, 0)
        self.assertLess(time.time() - started, 10)

    def test_missing_credentials_ends_serve(self):
        srv = self.boot()
        os.environ.pop("FEISHU_APP_ID", None)
        os.environ.pop("FEISHU_APP_SECRET", None)
        self.addCleanup(os.environ.__setitem__, "FEISHU_APP_ID", "cli_test")
        self.addCleanup(os.environ.__setitem__, "FEISHU_APP_SECRET",
                        "test-secret")
        try:
            self.assertEqual(self.fb.cmd_serve(once=False, max_retries=None), 1)
            self.assertEqual(srv.endpoint_calls, 0)
        finally:
            os.environ["FEISHU_APP_ID"] = "cli_test"
            os.environ["FEISHU_APP_SECRET"] = "test-secret"


if __name__ == "__main__":
    unittest.main()
