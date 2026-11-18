#!/usr/bin/env python3
"""Unit tests for examples/feishu-bot/feishubot.py.

feishubot.py is the Feishu counterpart of ilink.py / qqbot.py. It must
honour the same two-phase poll/commit contract the Muse worker already
relies on, plus the Feishu-specific rules:

  * the protobuf codec is hand-written, so it is pinned byte-for-byte
    against the schema in pbbp2.proto (and must skip unknown fields)
  * dedupe is on `message_id`, never `event_id` (the platform redelivers)
  * poll never advances state — re-polling returns the same batch
  * commit is what advances it (seen ids + cursor + inbox release)
  * strangers are dropped from the batch but still committed past
  * a bot's own messages never enter the batch (no feedback loop)
  * a dead long-connection receiver is a POLL failure, never silence
  * an unbound bot refuses to queue anything (NO_OWNER)
  * a corrupt inbox file is consumed, not fatal
  * send is keyed: the same key is SEND_ALREADY_SENT, and the request
    `uuid` is derived from the key so a retry dedupes server-side
  * a failed send leaves the intent in the ledger so a retry reuses it
  * reply (ctx present) and create (ctx empty) take different endpoints
  * a rate-limit business code is retried exactly once

    python3 -m unittest discover -s tests -v
"""
import importlib.util
import io
import json
import os
import shutil
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "examples", "feishu-bot", "feishubot.py")


def load_feishubot(data_dir):
    """Import a fresh module instance bound to `data_dir` (DATA_DIR is
    resolved at import time)."""
    os.environ["FEISHU_DATA_DIR"] = data_dir
    os.environ.pop("FEISHU_DOMAIN", None)
    spec = importlib.util.spec_from_file_location(
        f"feishubot_{abs(hash(data_dir))}", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._ensure_dirs()
    return mod


def event(msg_id, sender="ou_owner1", chat="p2p", text="hi", mtype="text",
          chat_id=None, sender_type="user", event_id=None, content=None,
          mentions=None):
    """One `im.message.receive_v1` body, shaped like the real push."""
    return {
        "schema": "2.0",
        "header": {
            "event_id": event_id or f"ev_{msg_id}",
            "event_type": "im.message.receive_v1",
            "create_time": "1700000000000",
            "app_id": "cli_test",
            "tenant_key": "tenant",
        },
        "event": {
            "sender": {
                "sender_id": {"union_id": "un1", "user_id": "u1",
                              "open_id": sender},
                "sender_type": sender_type,
            },
            "message": {
                "message_id": msg_id,
                "chat_id": chat_id or ("oc_group1" if chat == "group"
                                       else "oc_p2p1"),
                "chat_type": chat,
                "message_type": mtype,
                "content": content if content is not None
                else json.dumps({"text": text}, ensure_ascii=False),
                "create_time": "1700000000000",
                "mentions": mentions or [],
            },
        },
    }


class FeishubotTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="feishubot-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        for k, v in (("FEISHU_APP_ID", "cli_test"),
                     ("FEISHU_APP_SECRET", "test-secret")):
            old = os.environ.get(k)
            self.addCleanup(
                lambda k=k, old=old: os.environ.pop(k, None) if old is None
                else os.environ.__setitem__(k, old))
            os.environ[k] = v
        self.fb = load_feishubot(self.dir)
        self.fb.save_json(self.fb.TOKEN_FILE,
                          {"access_token": "cached",
                           "expires_at": int(time.time()) + 3600})
        # A live-looking receiver + a bound owner: the steady state.
        with open(self.fb.SERVE_HEARTBEAT, "w") as f:
            f.write(str(int(time.time())))
        self.fb.save_json(self.fb.OWNER_FILE, {"owner_id": "ou_owner1"})

    def add_event(self, payload):
        self.assertTrue(self.fb.inbox_write(payload), "event not queued")

    def poll(self):
        buf = io.StringIO()
        old, sys.stdout = sys.stdout, buf
        try:
            rc = self.fb.cmd_poll()
        finally:
            sys.stdout = old
        return rc, buf.getvalue().strip()

    def commit(self, out):
        block = json.loads(out)["commit"]
        old_stdin, sys.stdin = sys.stdin, io.StringIO(json.dumps(block))
        buf = io.StringIO()
        old_out, sys.stdout = sys.stdout, buf
        try:
            rc = self.fb.cmd_commit()
        finally:
            sys.stdin, sys.stdout = old_stdin, old_out
        return rc, buf.getvalue().strip()


class ProtobufTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="feishubot-pb-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.fb = load_feishubot(self.dir)

    def test_encode_matches_the_schema_byte_for_byte(self):
        fb = self.fb
        frame = {"SeqID": 1, "LogID": 2, "service": 3, "method": 1,
                 "headers": [("type", "event")], "payload": b'{"code":200}'}
        inner = b"\x0a\x04type\x12\x05event"
        expected = (b"\x08\x01\x10\x02\x18\x03\x20\x01"
                    + b"\x2a" + bytes([len(inner)]) + inner
                    + b"\x42" + bytes([12]) + b'{"code":200}')
        self.assertEqual(fb.pb_encode_frame(frame), expected)

    def test_round_trip(self):
        fb = self.fb
        frame = {"SeqID": 2 ** 40, "LogID": 7, "service": 5, "method": 1,
                 "headers": [("type", "event"), ("message_id", "om_1"),
                             ("sum", "1"), ("seq", "0"),
                             ("trace_id", "中文 trace")],
                 "payload_encoding": "json", "payload_type": "json",
                 "payload": "内容".encode(), "LogIDNew": "log-new"}
        out = fb.pb_decode_frame(fb.pb_encode_frame(frame))
        self.assertEqual(out["SeqID"], 2 ** 40)
        self.assertEqual(out["LogIDNew"], "log-new")
        self.assertEqual(out["payload"], "内容".encode())
        self.assertEqual(out["payload_encoding"], "json")
        self.assertEqual(dict(out["headers"])["trace_id"], "中文 trace")
        self.assertEqual(fb._header(out, "message_id"), "om_1")
        self.assertEqual(fb._header(out, "missing", "d"), "d")

    def test_decode_skips_unknown_fixed_width_fields(self):
        # A future server field is fixed32 (wire type 5) or fixed64 (1).
        fb = self.fb
        base = fb.pb_encode_frame({"SeqID": 1, "LogID": 2, "service": 3,
                                   "method": 1, "payload": b"x"})
        unknown32 = fb._pb_varint((20 << 3) | 5) + b"\x01\x02\x03\x04"
        unknown64 = fb._pb_varint((21 << 3) | 1) + b"\x01" * 8
        out = fb.pb_decode_frame(base + unknown32 + unknown64)
        self.assertEqual(out["SeqID"], 1)
        self.assertEqual(out["payload"], b"x")

    def test_decode_rejects_truncated_input(self):
        with self.assertRaises(ValueError):
            self.fb.pb_decode_frame(b"\x2a\x0a\x0a")

    def test_ack_echoes_the_frame_and_adds_biz_rt(self):
        fb = self.fb
        frame = {"SeqID": 9, "LogID": 8, "service": 1, "method": 1,
                 "headers": [("type", "event"), ("message_id", "om_9")],
                 "payload": b"{}"}
        ack = fb._ack_frame(frame)
        self.assertEqual(ack["SeqID"], 9)
        self.assertIn(("message_id", "om_9"), ack["headers"])
        self.assertIn(("biz_rt", "0"), ack["headers"])
        self.assertEqual(json.loads(ack["payload"]), {"code": 200})
        self.assertEqual(json.loads(fb._ack_frame(frame, 500)["payload"]),
                         {"code": 500})
        # The original must not be mutated: the same frame can be acked twice.
        self.assertEqual(frame["payload"], b"{}")
        self.assertEqual(len(frame["headers"]), 2)

    def test_service_id_comes_from_the_socket_url(self):
        fb = self.fb
        self.assertEqual(fb._service_id_from(
            "wss://host/ws?device_id=1&service_id=42"), 42)
        self.assertEqual(fb._service_id_from("wss://host/ws"), 0)


class PollCommitTest(FeishubotTestBase):
    def test_poll_is_two_phase(self):
        self.add_event(event("om_1"))
        rc, out = self.poll()
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual([m["message_id"] for m in payload["msgs"]], ["om_1"])
        self.assertEqual(payload["msgs"][0]["from_user_id"], "ou_owner1")
        # Platform vocabulary ("p2p") is translated to the repo's ("c2c")
        # at the adapter boundary, so a worker can treat every channel alike.
        self.assertEqual(payload["msgs"][0]["chat_type"], "c2c")
        self.assertEqual(payload["msgs"][0]["reply_to"], "ou_owner1")
        self.assertEqual(payload["msgs"][0]["text"], "hi")
        # Nothing advanced yet: a second poll must see the same batch.
        rc2, out2 = self.poll()
        self.assertEqual(rc2, 0)
        self.assertEqual(json.loads(out2)["msgs"], payload["msgs"])
        # Commit releases the inbox and records the id.
        rc3, _ = self.commit(out)
        self.assertEqual(rc3, 0)
        rc4, out4 = self.poll()
        self.assertEqual(rc4, 0)
        self.assertEqual(json.loads(out4)["msgs"], [])
        self.assertEqual(self.fb.inbox_pending(), [])

    def test_commit_is_idempotent(self):
        self.add_event(event("om_1"))
        _, out = self.poll()
        self.commit(out)
        rc, _ = self.commit(out)          # replay the same block
        self.assertEqual(rc, 0)
        self.assertEqual(len(self.fb.inbox_pending()), 0)

    def test_duplicate_push_is_deduped_on_message_id(self):
        # The platform redelivers an unacked event; the event_id differs but
        # the message does not. Feishu's docs say dedupe on message_id.
        self.add_event(event("om_1", event_id="ev_a"))
        self.assertFalse(self.fb.inbox_write(event("om_1", event_id="ev_b")))
        self.assertEqual(len(self.fb.inbox_pending()), 1)
        _, out = self.poll()
        self.assertEqual([m["message_id"] for m in json.loads(out)["msgs"]],
                         ["om_1"])

    def test_late_duplicate_after_commit_is_not_redelivered(self):
        # Once committed, the index entry is gone; a re-push lands in the
        # inbox again and the seen-id check is what stops a second reply.
        self.add_event(event("om_1"))
        _, out = self.poll()
        self.commit(out)
        self.add_event(event("om_1", event_id="ev_late"))
        _, out2 = self.poll()
        self.assertEqual(json.loads(out2)["msgs"], [])
        self.commit(out2)
        self.assertEqual(self.fb.inbox_pending(), [])

    def test_stranger_is_dropped_and_committed_past(self):
        self.add_event(event("om_1", sender="ou_stranger"))
        self.add_event(event("om_2"))
        rc, out = self.poll()
        payload = json.loads(out)
        self.assertEqual([m["message_id"] for m in payload["msgs"]], ["om_2"])
        self.assertEqual(payload["filtered"]["dropped"], 1)
        self.assertIn("ou_stranger", payload["filtered"]["senders"])
        unknown = self.fb.load_json(self.fb.UNKNOWN_FILE, {})
        self.assertIn("ou_stranger", unknown)
        # The stranger's id still rides in the commit block so the queue
        # can never jam on a message that will never be answered.
        self.assertIn("om_1", payload["commit"]["ids"])
        self.commit(json.dumps(payload))
        self.assertEqual(len(self.fb.inbox_pending()), 0)

    def test_bot_sender_is_dropped_without_a_reply_loop(self):
        self.add_event(event("om_1", sender="ou_other_bot",
                             sender_type="bot"))
        _, out = self.poll()
        payload = json.loads(out)
        self.assertEqual(payload["msgs"], [])
        self.assertIn("om_1", payload["commit"]["ids"])
        # Not a stranger: a bot is not a human to approve later.
        self.assertEqual(self.fb.load_json(self.fb.UNKNOWN_FILE, {}), {})

    def test_group_allowlist_and_mention_stripping(self):
        self.add_event(event(
            "om_g1", sender="ou_member", chat="group", text="@_user_1 hello",
            mentions=[{"key": "@_user_1", "id": {"open_id": "ou_bot"},
                       "name": "bot", "mentioned_type": "bot"}]))
        # Unknown group: dropped even though it is well formed.
        _, out = self.poll()
        self.assertEqual(json.loads(out)["msgs"], [])
        # Explicitly allowed group: queued, and the @ prefix is stripped.
        self.fb.save_json(self.fb.OWNER_FILE,
                          {"owner_id": "ou_owner1", "groups": ["oc_group1"]})
        fb = load_feishubot(self.dir)
        with open(fb.SERVE_HEARTBEAT, "w") as f:
            f.write(str(int(time.time())))
        buf = io.StringIO()
        old, sys.stdout = sys.stdout, buf
        try:
            fb.cmd_poll()
        finally:
            sys.stdout = old
        payload = json.loads(buf.getvalue().strip())
        self.assertEqual(len(payload["msgs"]), 1)
        self.assertEqual(payload["msgs"][0]["text"], "hello")
        self.assertEqual(payload["msgs"][0]["reply_to"], "oc_group1")
        self.assertEqual(payload["msgs"][0]["chat_type"], "group")

    def test_stale_receiver_is_poll_error(self):
        with open(self.fb.SERVE_HEARTBEAT, "w") as f:
            f.write(str(int(time.time()) - 600))
        self.add_event(event("om_1"))
        rc, out = self.poll()
        # A persisted batch is drained even with a dead receiver — losing it
        # would be worse than reporting the fault a round later.
        self.assertEqual(rc, 0)
        self.assertEqual([m["message_id"] for m in json.loads(out)["msgs"]],
                         ["om_1"])
        self.commit(out)
        rc2, out2 = self.poll()
        self.assertEqual(rc2, 1)
        self.assertTrue(out2.startswith("POLL_ERROR"), out2)

    def test_missing_receiver_is_poll_error(self):
        os.remove(self.fb.SERVE_HEARTBEAT)
        rc, out = self.poll()
        self.assertEqual(rc, 1)
        self.assertTrue(out.startswith("POLL_ERROR"), out)

    def test_unbound_is_no_owner(self):
        self.fb.save_json(self.fb.OWNER_FILE, {})
        self.add_event(event("om_1"))
        rc, out = self.poll()
        self.assertEqual(rc, 1)
        self.assertTrue(out.startswith("NO_OWNER"), out)

    def test_corrupt_inbox_file_is_consumed_not_fatal(self):
        self.add_event(event("om_1"))
        bad = os.path.join(self.fb.INBOX_DIR, "0000000002-bad.json")
        with open(bad, "w") as f:
            f.write("{not json")
        rc, out = self.poll()
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual([m["message_id"] for m in payload["msgs"]], ["om_1"])
        self.commit(json.dumps(payload))
        self.assertEqual(self.fb.inbox_pending(), [])

    def test_non_message_events_are_consumed_quietly(self):
        self.fb.inbox_write({"schema": "2.0",
                             "header": {"event_id": "ev_x",
                                        "event_type": "im.chat.updated"},
                             "event": {"chat_id": "oc_p2p1"}})
        self.add_event(event("om_1"))
        rc, out = self.poll()
        payload = json.loads(out)
        self.assertEqual([m["message_id"] for m in payload["msgs"]], ["om_1"])
        self.commit(json.dumps(payload))
        self.assertEqual(self.fb.inbox_pending(), [])

    def test_batch_is_capped(self):
        old = self.fb.POLL_BATCH_CAP
        self.fb.POLL_BATCH_CAP = 2
        try:
            for i in range(5):
                self.add_event(event(f"om_{i}"))
            _, out = self.poll()
            self.assertEqual(len(json.loads(out)["msgs"]), 2)
        finally:
            self.fb.POLL_BATCH_CAP = old

    def test_post_message_is_flattened_to_text(self):
        content = json.dumps({"zh_cn": {"title": "标题", "content": [
            [{"tag": "text", "text": "第一行"},
             {"tag": "a", "text": "链接", "href": "https://x"}],
            [{"tag": "img", "image_key": "img_1"}],
        ]}}, ensure_ascii=False)
        self.add_event(event("om_p", mtype="post", content=content))
        _, out = self.poll()
        self.assertEqual(json.loads(out)["msgs"][0]["text"],
                         "标题\n第一行链接\n[图片]")

    def test_media_is_downloaded_to_disk(self):
        self.fb.api_raw = lambda path, **k: (b"PNGDATA", "image/png")
        self.add_event(event("om_img", mtype="image",
                             content=json.dumps({"image_key": "img_v2_1"})))
        _, out = self.poll()
        entry = json.loads(out)["msgs"][0]
        self.assertEqual(entry["kind"], "image")
        self.assertTrue(entry["media_path"].endswith(".png"), entry)
        with open(entry["media_path"], "rb") as f:
            self.assertEqual(f.read(), b"PNGDATA")

    def test_media_download_failure_is_reported_not_fatal(self):
        def boom(path, **k):
            raise RuntimeError("404 resource not found")
        self.fb.api_raw = boom
        self.add_event(event("om_img", mtype="image",
                             content=json.dumps({"image_key": "img_v2_1"})))
        rc, out = self.poll()
        self.assertEqual(rc, 0)
        entry = json.loads(out)["msgs"][0]
        self.assertIn("media_error", entry)
        self.assertNotIn("media_path", entry)


class SendTest(FeishubotTestBase):
    def setUp(self):
        super().setUp()
        self.calls = []

        def fake_api(method, path, body=None, timeout=20, token=None):
            self.calls.append((method, path, body))
            return {"code": 0, "msg": "success",
                    "data": {"message_id": f"om_sent{len(self.calls)}"}}

        self.fb.api = fake_api

    def send(self, *args, **kwargs):
        buf = io.StringIO()
        old, sys.stdout = sys.stdout, buf
        try:
            rc = self.fb.cmd_send(*args, **kwargs)
        finally:
            sys.stdout = old
        return rc, buf.getvalue().strip()

    def test_reply_goes_to_the_message_endpoint(self):
        rc, out = self.send("ou_owner1", "om_1", "hello", key="om_1-1")
        self.assertEqual((rc, out), (0, "SEND_OK"))
        method, path, body = self.calls[0]
        self.assertEqual((method, path), ("POST", "/im/v1/messages/om_1/reply"))
        self.assertEqual(body["msg_type"], "text")
        self.assertEqual(json.loads(body["content"]), {"text": "hello"})
        self.assertEqual(body["uuid"], "om_1-1")
        self.assertNotIn("receive_id", body)

    def test_create_infers_receive_id_type_from_the_prefix(self):
        self.send("oc_chat1", "", "hello", key="k-1")
        self.send("ou_user1", "", "hello", key="k-2")
        self.assertEqual([c[1] for c in self.calls], [
            "/im/v1/messages?receive_id_type=chat_id",
            "/im/v1/messages?receive_id_type=open_id",
        ])
        self.assertEqual([c[2]["receive_id"] for c in self.calls],
                         ["oc_chat1", "ou_user1"])

    def test_dash_reads_the_body_from_stdin(self):
        # ilink.py / qqbot.py both accept "-" for the body; a worker piping a
        # long reply must not get a literal hyphen delivered instead.
        old_in, old_out = sys.stdin, sys.stdout
        sys.stdin, sys.stdout = io.StringIO("from stdin"), io.StringIO()
        try:
            rc = self.fb.main(["feishubot.py", "send", "ou_owner1", "om_1",
                               "-", "--key", "om_1-9"])
        finally:
            sys.stdin, sys.stdout = old_in, old_out
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(self.calls[0][2]["content"]),
                         {"text": "from stdin"})

    def test_explicit_chat_flag_overrides_inference(self):
        self.send("oc_chat1", "", "hello", chat="chat_id", key="k-1")
        self.assertEqual(self.calls[0][1],
                         "/im/v1/messages?receive_id_type=chat_id")
        self.send("ou_user1", "", "hello", chat="p2p", key="k-2")
        self.assertEqual(self.calls[1][1],
                         "/im/v1/messages?receive_id_type=open_id")

    def test_same_key_is_not_sent_twice(self):
        self.assertEqual(self.send("ou_owner1", "om_1", "hello",
                                   key="om_1-1")[1], "SEND_OK")
        rc, out = self.send("ou_owner1", "om_1", "hello", key="om_1-1")
        self.assertEqual((rc, out), (0, "SEND_ALREADY_SENT"))
        self.assertEqual(len(self.calls), 1)

    def test_bubbles_get_distinct_uuids(self):
        self.send("ou_owner1", "om_1", "one", key="om_1-1")
        self.send("ou_owner1", "om_1", "two", key="om_1-2")
        self.assertEqual([c[2]["uuid"] for c in self.calls],
                         ["om_1-1", "om_1-2"])

    def test_long_key_is_hashed_to_fit_the_50_char_cap(self):
        key = "om_" + "x" * 80
        uuid = self.fb._uuid_from_key(key)
        self.assertLessEqual(len(uuid), 50)
        self.assertEqual(uuid, self.fb._uuid_from_key(key))   # deterministic
        self.assertNotEqual(uuid, self.fb._uuid_from_key(key + "y"))

    def test_empty_text_and_target_are_refused(self):
        self.assertEqual(self.send("ou_owner1", "om_1", "")[0], 1)
        self.assertEqual(self.send("", "om_1", "hi")[0], 1)
        self.assertEqual(self.calls, [])

    def test_failed_send_keeps_intent_for_retry(self):
        def boom(*a, **k):
            raise RuntimeError("network down")
        self.fb.api = boom
        rc, out = self.send("ou_owner1", "om_1", "hello", key="om_1-1")
        self.assertEqual(rc, 1)
        self.assertIn("SEND_FAILED", out)
        ledger = self.fb.load_json(self.fb.SEND_LEDGER_FILE, {})
        self.assertEqual(ledger["om_1-1"]["status"], "intent")
        self.assertEqual(ledger["om_1-1"]["uuid"], "om_1-1")
        # Retry with a working transport reuses the same uuid.
        self.fb.api = lambda *a, **k: self.calls.append(a) or {
            "data": {"message_id": "om_1"}}
        rc2, out2 = self.send("ou_owner1", "om_1", "hello", key="om_1-1")
        self.assertEqual((rc2, out2), (0, "SEND_OK"))
        self.assertEqual(self.calls[-1][2]["uuid"], "om_1-1")

    def test_rate_limit_code_is_retried_once(self):
        real_sleep = self.fb.time.sleep
        self.fb.time.sleep = lambda s: None
        self.addCleanup(setattr, self.fb.time, "sleep", real_sleep)
        attempts = []

        def flaky(*a, **k):
            attempts.append(1)
            if len(attempts) == 1:
                raise self.fb.ApiError(99991400, "too many requests", 429)
            return {"data": {"message_id": "om_1"}}

        self.fb.api = flaky
        rc, out = self.send("ou_owner1", "om_1", "hello", key="om_1-1")
        self.assertEqual((rc, out), (0, "SEND_OK"))
        self.assertEqual(len(attempts), 2)

    def test_other_api_errors_are_not_retried(self):
        attempts = []

        def bad_request(*a, **k):
            attempts.append(1)
            raise self.fb.ApiError(230001, "bad content", 400)

        self.fb.api = bad_request
        rc, out = self.send("ou_owner1", "om_1", "hello", key="om_1-1")
        self.assertEqual(rc, 1)
        self.assertIn("SEND_FAILED", out)
        self.assertEqual(len(attempts), 1)


class BindTest(FeishubotTestBase):
    def setUp(self):
        super().setUp()
        self.calls = []

        def fake_api(method, path, body=None, timeout=20, token=None):
            self.calls.append((method, path, body))
            return {"data": {"message_id": "om_welcome"}}

        self.fb.api = fake_api

    def bind(self, wait_secs=1):
        buf = io.StringIO()
        old, sys.stdout = sys.stdout, buf
        try:
            rc = self.fb.cmd_bind(wait_secs=wait_secs)
        finally:
            sys.stdout = old
        return rc, buf.getvalue().strip()

    def test_first_p2p_sender_becomes_owner(self):
        self.fb.save_json(self.fb.OWNER_FILE, {})
        self.add_event(event("om_1", sender="ou_first"))
        rc, out = self.bind()
        self.assertEqual(rc, 0)
        self.assertTrue(out.startswith("BOUND:ou_first"), out)
        self.assertEqual(
            self.fb.load_json(self.fb.OWNER_FILE, {})["owner_id"], "ou_first")
        # The welcome goes out as a reply to the binding message, so no
        # proactive-message rule applies, and it is keyed for idempotency.
        self.assertEqual(self.calls[0][1], "/im/v1/messages/om_1/reply")
        self.assertEqual(self.calls[0][2]["uuid"], "om_1-1")
        self.assertEqual(self.fb.load_json(self.fb.SEND_LEDGER_FILE, {})[
            "om_1-1"]["status"], "sent")

    def test_already_bound_refuses(self):
        rc, out = self.bind()
        self.assertEqual(rc, 1)
        self.assertEqual(out, "ALREADY_BOUND")
        self.assertEqual(self.fb.load_json(self.fb.OWNER_FILE, {})["owner_id"],
                         "ou_owner1")

    def test_group_message_never_binds(self):
        self.fb.save_json(self.fb.OWNER_FILE, {})
        self.add_event(event("om_g1", sender="ou_member", chat="group"))
        rc, out = self.bind()
        self.assertEqual(rc, 1)
        self.assertEqual(out, "BIND_TIMEOUT")
        self.assertEqual(self.fb.load_json(self.fb.OWNER_FILE, {}), {})


class CredentialTest(unittest.TestCase):
    def test_missing_credentials_is_explicit(self):
        d = tempfile.mkdtemp(prefix="feishubot-nocred-")
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        old_env = {k: os.environ.pop(k) for k in
                   ("FEISHU_APP_ID", "FEISHU_APP_SECRET") if k in os.environ}
        try:
            fb = load_feishubot(d)
            with self.assertRaises(RuntimeError) as ctx:
                fb.get_token()
            self.assertIn("NO_CREDENTIALS", str(ctx.exception))
        finally:
            os.environ.update(old_env)

    def test_token_is_cached_until_the_skew(self):
        d = tempfile.mkdtemp(prefix="feishubot-tok-")
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        old_env = {k: os.environ.pop(k) for k in
                   ("FEISHU_APP_ID", "FEISHU_APP_SECRET") if k in os.environ}
        try:
            fb = load_feishubot(d)
            fb.save_json(fb.TOKEN_FILE, {
                "access_token": "still-good",
                "expires_at": int(time.time()) + fb.TOKEN_SKEW + 10})
            self.assertEqual(fb.get_token(), "still-good")
        finally:
            os.environ.update(old_env)


if __name__ == "__main__":
    unittest.main()
