#!/usr/bin/env python3
"""Tests for the minimal WebSocket client and the gateway loop in qqbot.py.

`serve` is the one part of the QQ adapter that cannot be exercised by the
hook tests (they stub the client out), and it is also the part with the
subtle durability rule: the event must be on disk *before* the gateway
sequence is persisted. So this suite runs a real loopback WebSocket server
that speaks the QQ gateway handshake and asserts on the resulting files.

It also pins the fatal-close-code rule: codes the API doc marks "no RESUME,
no IDENTIFY" (4915 = bot banned, …) must end `serve` immediately instead of
feeding a 3-second reconnect loop.

    python3 -m unittest discover -s tests -v
"""
import base64
import hashlib
import io
import json
import os
import shutil
import socket
import struct
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "examples", "qq-bot", "qqbot.py")


def load_qqbot(data_dir):
    import importlib.util
    os.environ["QQ_DATA_DIR"] = data_dir
    spec = importlib.util.spec_from_file_location(
        f"qqbot_ws_{abs(hash(data_dir))}", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._ensure_dirs()
    return mod


def _frame(payload, opcode=0x1):
    """Server-side frame: unmasked, single frame."""
    data = payload if isinstance(payload, bytes) else payload.encode()
    n = len(data)
    if n < 126:
        head = struct.pack("!BB", 0x80 | opcode, n)
    elif n < (1 << 16):
        head = struct.pack("!BBH", 0x80 | opcode, 126, n)
    else:
        head = struct.pack("!BBQ", 0x80 | opcode, 127, n)
    return head + data


class fake_gateway:
    """A loopback WebSocket server. By default it completes the HTTP
    handshake and replays `frames`; pass `handler(conn, ws_key)` to script
    a different exchange (bad handshake, fragmentation, ...)."""

    def __init__(self, frames, handler=None):
        self.frames = frames
        self.handler = handler
        self.received = []
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.sock.settimeout(10)
        self.port = self.sock.getsockname()[1]
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    @staticmethod
    def _accept_key(conn):
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = conn.recv(4096)
            if not chunk:
                return None
            head += chunk
        headers = {}
        for line in head.split(b"\r\n")[1:]:
            if b":" in line:
                k, v = line.split(b":", 1)
                headers[k.strip().lower()] = v.strip()
        return headers.get(b"sec-websocket-key")

    @staticmethod
    def _accept(conn, ws_key):
        accept = base64.b64encode(hashlib.sha1(
            ws_key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest()).decode()
        conn.sendall(("HTTP/1.1 101 Switching Protocols\r\n"
                      "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                      f"Sec-WebSocket-Accept: {accept}\r\n\r\n").encode())

    def _run(self):
        try:
            conn, _ = self.sock.accept()
        except (OSError, socket.timeout):
            return
        conn.settimeout(10)
        try:
            ws_key = self._accept_key(conn)
            if ws_key is None:
                return
            if self.handler is not None:
                self.handler(conn, ws_key)
                return
            self._accept(conn, ws_key)
            for frame in self.frames:
                conn.sendall(_frame(frame))
                time.sleep(0.05)
            # Keep the connection open briefly so the client can finish
            # reading and persist its state before we drop it.
            time.sleep(0.4)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


class WebSocketClientTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="qqbot-ws-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.qq = load_qqbot(self.dir)

    def test_handshake_and_text_frame(self):
        gw = fake_gateway([json.dumps({"op": 10, "d": {"heartbeat_interval": 45000}})])
        self.addCleanup(gw.close)
        ws = self.qq.WebSocket(f"ws://127.0.0.1:{gw.port}/").connect()
        self.addCleanup(ws.close)
        opcode, payload = ws.recv_message(deadline=time.time() + 5)
        self.assertEqual(opcode, 0x1)
        self.assertEqual(json.loads(payload.decode())["op"], 10)

    def test_fragmented_message_is_reassembled(self):
        # Two frames, first with FIN clear: the client must join them.
        part1_payload = b'{"op"'
        part2_payload = b':0}'
        part1 = struct.pack("!BB", 0x01, len(part1_payload)) + part1_payload
        part2 = struct.pack("!BB", 0x80, len(part2_payload)) + part2_payload

        def handler(conn, ws_key):
            fake_gateway._accept(conn, ws_key)
            conn.sendall(part1 + part2)
            time.sleep(0.4)

        gw = fake_gateway([], handler=handler)
        self.addCleanup(gw.close)
        ws = self.qq.WebSocket(f"ws://127.0.0.1:{gw.port}/").connect()
        self.addCleanup(ws.close)
        _, payload = ws.recv_message(deadline=time.time() + 5)
        self.assertEqual(json.loads(payload.decode()), {"op": 0})

    def test_rejects_handshake(self):
        def handler(conn, ws_key):
            conn.sendall(b"HTTP/1.1 401 Unauthorized\r\n\r\n")
            time.sleep(0.2)

        gw = fake_gateway([], handler=handler)
        self.addCleanup(gw.close)
        with self.assertRaises(self.qq.WSClosed):
            self.qq.WebSocket(f"ws://127.0.0.1:{gw.port}/").connect()


class GatewayDurabilityTest(unittest.TestCase):
    """The order guarantee: event on disk, then the sequence."""

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="qqbot-gw-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.qq = load_qqbot(self.dir)

    def test_dispatch_lands_in_inbox_before_seq(self):
        hello = json.dumps({"op": 10, "d": {"heartbeat_interval": 45000}})
        ready = json.dumps({"op": 0, "s": 1, "t": "READY",
                            "d": {"session_id": "sess1"}})
        msg = json.dumps({"op": 0, "s": 2, "t": "C2C_MESSAGE_CREATE",
                          "d": {"id": "m1", "content": "hi",
                                "author": {"user_openid": "owner1"}}})
        gw = fake_gateway([hello, ready, msg])
        self.addCleanup(gw.close)
        url = f"ws://127.0.0.1:{gw.port}/"
        self.qq.gateway_url = lambda token=None: (url, {})
        self.qq.get_token = lambda force=False: "tok"

        # `serve --once` returns after the first dispatch event.
        rc = self.qq.cmd_serve(once=True, max_retries=1)
        self.assertEqual(rc, 0)
        pending = self.qq.inbox_pending()
        self.assertEqual(len(pending), 1)
        with open(pending[0]) as f:
            ev = json.load(f)
        self.assertEqual(ev["t"], "C2C_MESSAGE_CREATE")
        self.assertEqual(ev["seq"], 2)
        # The sequence is persisted AFTER the file exists, with the session.
        saved = self.qq.load_json(self.qq.GATEWAY_FILE, {})
        self.assertEqual(saved.get("seq"), 2)
        self.assertEqual(saved.get("session_id"), "sess1")

    def test_duplicate_seq_does_not_duplicate_the_inbox(self):
        self.assertTrue(self.qq.inbox_write(5, "C2C_MESSAGE_CREATE",
                                            {"id": "m1"}))
        self.assertFalse(self.qq.inbox_write(5, "C2C_MESSAGE_CREATE",
                                             {"id": "m1"}))
        self.assertEqual(len(self.qq.inbox_pending()), 1)


class FatalCloseTest(unittest.TestCase):
    """Fatal close codes must stop the loop, not feed a 3-second retry cycle."""

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="qqbot-fatal-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.qq = load_qqbot(self.dir)

    @staticmethod
    def _close_with(code):
        def handler(conn, ws_key):
            fake_gateway._accept(conn, ws_key)
            conn.sendall(_frame(struct.pack("!H", code), opcode=0x8))
            time.sleep(0.2)
        return handler

    def test_close_frame_carries_its_code(self):
        gw = fake_gateway([], handler=self._close_with(4915))
        self.addCleanup(gw.close)
        ws = self.qq.WebSocket(f"ws://127.0.0.1:{gw.port}/").connect()
        self.addCleanup(ws.close)
        with self.assertRaises(self.qq.WSClosed) as ctx:
            ws.recv_message(deadline=time.time() + 5)
        self.assertEqual(ctx.exception.code, 4915)

    def test_fatal_code_ends_serve_without_retrying(self):
        # 4915 = bot banned: the doc marks it "no RESUME, no IDENTIFY".
        gw = fake_gateway([], handler=self._close_with(4915))
        self.addCleanup(gw.close)
        self.qq.gateway_url = lambda token=None: (
            f"ws://127.0.0.1:{gw.port}/", {})
        self.qq.get_token = lambda force=False: "tok"
        started = time.time()
        rc = self.qq.cmd_serve(once=False, max_retries=None)
        self.assertEqual(rc, 1)
        # A retry would have slept 3s first; returning immediately is the
        # difference between "refused" and "stuck in a loop".
        self.assertLess(time.time() - started, 3)
        self.assertEqual(self.qq.inbox_pending(), [])


if __name__ == "__main__":
    unittest.main()
