#!/usr/bin/env python3
"""Unit tests for examples/qq-bot/qqbot.py.

qqbot.py is the QQ counterpart of ilink.py. It must honour the same
two-phase poll/commit contract the Muse worker already relies on:

  * poll never advances state — re-polling returns the same batch
  * commit is what advances it (seen ids + cursor + inbox release)
  * strangers are dropped from the batch but still committed past
  * a dead gateway receiver is a POLL failure, never silence
  * an unbound bot refuses to queue anything (NO_OWNER)
  * a corrupt inbox file is consumed, not fatal
  * send is keyed: a second send with the same key is SEND_ALREADY_SENT,
    and msg_seq is derived from the key's bubble suffix so a retry lands
    on the same (msg_id, msg_seq) pair the server dedupes
  * a failed send leaves the intent in the ledger so a retry reuses it
  * a bubble past the platform's passive-reply cap (C2C 4, group 5) is
    refused locally, but an existing intent's retry is never blocked
  * the typing indicator uses `msg_type 6` + `input_notify` on a seq that
    cannot collide with a reply bubble (the server dedupes across types)

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
SRC = os.path.join(HERE, "..", "examples", "qq-bot", "qqbot.py")


def load_qqbot(data_dir):
    """Import a fresh module instance bound to `data_dir`.

    DATA_DIR is resolved at import time, so each test needs its own import
    rather than a shared module-level constant.
    """
    os.environ["QQ_DATA_DIR"] = data_dir
    os.environ.pop("QQ_SANDBOX", None)
    spec = importlib.util.spec_from_file_location(f"qqbot_{abs(hash(data_dir))}", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._ensure_dirs()
    return mod


class QqbotTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="qqbot-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        # Dummy credentials: the tests never make a network call (the token
        # cache is pre-seeded), but `poll` requires them to be present.
        for k, v in (("QQ_APP_ID", "1000"), ("QQ_APP_SECRET", "test-secret")):
            old = os.environ.get(k)
            self.addCleanup(lambda k=k, old=old:
                            os.environ.pop(k, None) if old is None
                            else os.environ.__setitem__(k, old))
            os.environ[k] = v
        self.qq = load_qqbot(self.dir)
        self.qq.save_json(self.qq.TOKEN_FILE,
                          {"access_token": "cached", "expires_at": int(time.time()) + 3600})
        # A live-looking receiver + a bound owner: the normal steady state.
        with open(self.qq.SERVE_HEARTBEAT, "w") as f:
            f.write(str(int(time.time())))
        self.qq.save_json(self.qq.OWNER_FILE, {"owner_id": "owner1"})

    def add_event(self, seq, t, d):
        self.assertTrue(self.qq.inbox_write(seq, t, d))

    def c2c(self, seq, msg_id, sender="owner1", text="hi"):
        self.add_event(seq, "C2C_MESSAGE_CREATE",
                       {"id": msg_id, "content": text,
                        "author": {"user_openid": sender},
                        "timestamp": "2026-01-01T00:00:00+08:00"})

    def poll(self):
        buf = io.StringIO()
        old, sys.stdout = sys.stdout, buf
        try:
            rc = self.qq.cmd_poll()
        finally:
            sys.stdout = old
        return rc, buf.getvalue().strip()

    def commit(self, out):
        block = json.loads(out)["commit"]
        old_stdin, sys.stdin = sys.stdin, io.StringIO(json.dumps(block))
        buf = io.StringIO()
        old_out, sys.stdout = sys.stdout, buf
        try:
            rc = self.qq.cmd_commit()
        finally:
            sys.stdin, sys.stdout = old_stdin, old_out
        return rc, buf.getvalue().strip()


class PollCommitTest(QqbotTestBase):
    def test_poll_is_two_phase(self):
        self.c2c(1, "m1")
        rc, out = self.poll()
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual([m["message_id"] for m in payload["msgs"]], ["m1"])
        self.assertEqual(payload["msgs"][0]["from_user_id"], "owner1")
        self.assertEqual(payload["msgs"][0]["chat_type"], "c2c")
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
        self.assertEqual(self.qq.inbox_pending(), [])

    def test_commit_is_idempotent(self):
        self.c2c(1, "m1")
        _, out = self.poll()
        self.commit(out)
        rc, _ = self.commit(out)          # replay the same block
        self.assertEqual(rc, 0)
        self.assertEqual(len(self.qq.inbox_pending()), 0)

    def test_stranger_is_dropped_and_committed_past(self):
        self.c2c(1, "m1", sender="stranger")
        self.c2c(2, "m2", sender="owner1")
        rc, out = self.poll()
        payload = json.loads(out)
        self.assertEqual([m["message_id"] for m in payload["msgs"]], ["m2"])
        self.assertEqual(payload["filtered"]["dropped"], 1)
        self.assertIn("stranger", payload["filtered"]["senders"])
        unknown = self.qq.load_json(self.qq.UNKNOWN_FILE, {})
        self.assertIn("stranger", unknown)
        # The stranger's id still rides in the commit block so the queue
        # can never jam on a message that will never be answered.
        self.assertIn("m1", payload["commit"]["ids"])
        self.commit(json.dumps(payload))
        self.assertEqual(len(self.qq.inbox_pending()), 0)

    def test_seen_ids_are_not_redelivered(self):
        self.qq.save_json(self.qq.SEEN_FILE, {"ids": ["m1"]})
        self.c2c(1, "m1")
        rc, out = self.poll()
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(out)["msgs"], [])

    def test_group_allowlist(self):
        self.add_event(1, "GROUP_AT_MESSAGE_CREATE",
                       {"id": "g1", "content": " hello",
                        "author": {"member_openid": "member1"},
                        "group_openid": "group1"})
        # Unknown group: dropped even though it is well formed.
        _, out = self.poll()
        self.assertEqual(json.loads(out)["msgs"], [])
        # Explicitly allowed group: queued, and the @ prefix is stripped.
        self.qq.save_json(self.qq.OWNER_FILE,
                          {"owner_id": "owner1", "groups": ["group1"]})
        qq = self.qq
        qq.save_json(qq.SEEN_FILE, {"ids": []})
        # re-poll from a fresh module so OWNER_FILE is re-read
        qq = load_qqbot(self.dir)
        with open(qq.SERVE_HEARTBEAT, "w") as f:
            f.write(str(int(time.time())))
        buf = io.StringIO()
        old, sys.stdout = sys.stdout, buf
        try:
            qq.cmd_poll()
        finally:
            sys.stdout = old
        payload = json.loads(buf.getvalue().strip())
        self.assertEqual(len(payload["msgs"]), 1)
        self.assertEqual(payload["msgs"][0]["text"], "hello")
        self.assertEqual(payload["msgs"][0]["reply_to"], "group1")
        self.assertEqual(payload["msgs"][0]["chat_type"], "group")

    def test_stale_receiver_is_poll_error(self):
        with open(self.qq.SERVE_HEARTBEAT, "w") as f:
            f.write(str(int(time.time()) - 600))
        self.c2c(1, "m1")
        rc, out = self.poll()
        # A persisted batch is drained even with a dead receiver — losing it
        # would be worse than reporting the fault a round later.
        self.assertEqual(rc, 0)
        self.assertEqual([m["message_id"] for m in json.loads(out)["msgs"]], ["m1"])
        # With the inbox drained by commit, the dead receiver surfaces.
        self.commit(out)
        rc2, out2 = self.poll()
        self.assertEqual(rc2, 1)
        self.assertTrue(out2.startswith("POLL_ERROR"), out2)

    def test_missing_receiver_is_poll_error(self):
        os.remove(self.qq.SERVE_HEARTBEAT)
        rc, out = self.poll()
        self.assertEqual(rc, 1)
        self.assertTrue(out.startswith("POLL_ERROR"), out)

    def test_unbound_is_no_owner(self):
        self.qq.save_json(self.qq.OWNER_FILE, {})
        self.c2c(1, "m1")
        rc, out = self.poll()
        self.assertEqual(rc, 1)
        self.assertTrue(out.startswith("NO_OWNER"), out)

    def test_corrupt_inbox_file_is_consumed_not_fatal(self):
        self.c2c(1, "m1")
        bad = os.path.join(self.qq.INBOX_DIR, "0000000002-bad.json")
        with open(bad, "w") as f:
            f.write("{not json")
        rc, out = self.poll()
        self.assertEqual(rc, 0)
        payload = json.loads(out)
        self.assertEqual([m["message_id"] for m in payload["msgs"]], ["m1"])
        self.commit(json.dumps(payload))
        self.assertEqual(self.qq.inbox_pending(), [])

    def test_non_message_events_are_consumed_quietly(self):
        self.add_event(1, "GROUP_ADD_ROBOT", {"group_openid": "g1"})
        self.c2c(2, "m1")
        rc, out = self.poll()
        payload = json.loads(out)
        self.assertEqual([m["message_id"] for m in payload["msgs"]], ["m1"])
        self.commit(json.dumps(payload))
        self.assertEqual(self.qq.inbox_pending(), [])

    def test_batch_is_capped(self):
        old = self.qq.POLL_BATCH_CAP
        self.qq.POLL_BATCH_CAP = 2
        try:
            for i in range(5):
                self.c2c(i + 1, f"m{i}")
            _, out = self.poll()
            self.assertEqual(len(json.loads(out)["msgs"]), 2)
        finally:
            self.qq.POLL_BATCH_CAP = old


class SendTest(QqbotTestBase):
    def setUp(self):
        super().setUp()
        self.calls = []

        def fake_api(method, path, body=None, timeout=20, token=None):
            self.calls.append((method, path, body))
            return {"id": f"sent{len(self.calls)}", "timestamp": int(time.time())}

        self.qq.api = fake_api

    def send(self, *args):
        buf = io.StringIO()
        old, sys.stdout = sys.stdout, buf
        try:
            rc = self.qq.cmd_send(*args)
        finally:
            sys.stdout = old
        return rc, buf.getvalue().strip()

    def test_c2c_send_carries_msg_id_and_seq(self):
        rc, out = self.send("owner1", "m1", "hello", "c2c", None, "m1-1")
        self.assertEqual((rc, out), (0, "SEND_OK"))
        method, path, body = self.calls[0]
        self.assertEqual((method, path), ("POST", "/v2/users/owner1/messages"))
        self.assertEqual(body["msg_id"], "m1")
        self.assertEqual(body["msg_seq"], 1)
        self.assertEqual(body["content"], "hello")

    def test_group_send_uses_group_endpoint(self):
        rc, _ = self.send("group1", "m1", "hello", "group", None, "m1-1")
        self.assertEqual(rc, 0)
        self.assertEqual(self.calls[0][1], "/v2/groups/group1/messages")

    def test_same_key_is_not_sent_twice(self):
        self.assertEqual(self.send("owner1", "m1", "hello", "c2c", None, "m1-1")[1],
                         "SEND_OK")
        rc, out = self.send("owner1", "m1", "hello", "c2c", None, "m1-1")
        self.assertEqual((rc, out), (0, "SEND_ALREADY_SENT"))
        self.assertEqual(len(self.calls), 1)

    def test_bubble_suffix_selects_msg_seq(self):
        self.send("owner1", "m1", "one", "c2c", None, "m1-1")
        self.send("owner1", "m1", "two", "c2c", None, "m1-2")
        self.assertEqual([c[2]["msg_seq"] for c in self.calls], [1, 2])

    def test_auto_allocated_seq_does_not_collide(self):
        # Two bubbles with arbitrary (non-numeric-suffixed) keys must still
        # get distinct msg_seq values: the server rejects a duplicate
        # (msg_id, msg_seq) pair, so a collision would silently lose a bubble.
        self.send("owner1", "m1", "one", "c2c", None, "alpha")
        self.send("owner1", "m1", "two", "c2c", None, "beta")
        self.assertEqual([c[2]["msg_seq"] for c in self.calls], [1, 2])
        self.assertEqual({c[2]["msg_id"] for c in self.calls}, {"m1"})

    def test_send_without_msg_id_refuses(self):
        rc, out = self.send("owner1", "", "hello", "c2c", None, "x-1")
        self.assertEqual(rc, 1)
        self.assertIn("SEND_NO_MSG_ID", out)
        self.assertEqual(self.calls, [])

    def test_failed_send_keeps_intent_for_retry(self):
        def boom(*a, **k):
            raise RuntimeError("network down")
        self.qq.api = boom
        rc, out = self.send("owner1", "m1", "hello", "c2c", None, "m1-1")
        self.assertEqual(rc, 1)
        self.assertIn("SEND_FAILED", out)
        ledger = self.qq.load_json(self.qq.SEND_LEDGER_FILE, {})
        self.assertEqual(ledger["m1-1"]["status"], "intent")
        self.assertEqual(ledger["m1-1"]["seq"], 1)
        # Retry with a working transport reuses the same seq.
        self.qq.api = lambda *a, **k: self.calls.append(a) or {"id": "x"}
        rc2, out2 = self.send("owner1", "m1", "hello", "c2c", None, "m1-1")
        self.assertEqual((rc2, out2), (0, "SEND_OK"))
        self.assertEqual(self.calls[-1][2]["msg_seq"], 1)

    def test_c2c_fifth_bubble_is_refused_locally(self):
        # The platform allows 4 passive replies per C2C msg_id (message
        # overview, 2026-10-28). The 5th would come back as 40034128, so it
        # must be refused here without ever reaching the network.
        for n in range(1, 5):
            rc, out = self.send("owner1", "m1", f"b{n}", "c2c", None, f"m1-{n}")
            self.assertEqual((rc, out), (0, "SEND_OK"), f"bubble {n}")
        rc, out = self.send("owner1", "m1", "b5", "c2c", None, "m1-5")
        self.assertEqual(rc, 1)
        self.assertIn("SEND_BUBBLE_CAP:c2c allows at most 4", out)
        self.assertEqual(len(self.calls), 4)

    def test_group_fifth_bubble_is_still_allowed(self):
        # Groups get 5, so the same 5th bubble must go through there — the
        # cap is per chat type, not a blanket 4.
        for n in range(1, 6):
            rc, out = self.send("group1", "m1", f"b{n}", "group", None, f"m1-{n}")
            self.assertEqual((rc, out), (0, "SEND_OK"), f"bubble {n}")
        self.assertEqual(len(self.calls), 5)

    def test_bubble_cap_does_not_block_existing_intent_retry(self):
        # An intent was within the cap when it was first attempted, so its
        # retry must go out unchanged even if the ledger now looks full.
        def boom(*a, **k):
            raise RuntimeError("network down")
        self.qq.api = boom
        for n in range(1, 5):
            self.send("owner1", "m1", f"b{n}", "c2c", None, f"m1-{n}")
        ledger = self.qq.load_json(self.qq.SEND_LEDGER_FILE, {})
        self.assertEqual(ledger["m1-4"]["status"], "intent")
        calls = []
        self.qq.api = lambda m, p, b=None, **k: calls.append(b) or {"id": "x"}
        rc, out = self.send("owner1", "m1", "b4", "c2c", None, "m1-4")
        self.assertEqual((rc, out), (0, "SEND_OK"))
        self.assertEqual(calls[0]["msg_seq"], 4)


class TypingTest(QqbotTestBase):
    def setUp(self):
        super().setUp()
        self.calls = []

        def fake_api(method, path, body=None, timeout=20, token=None):
            self.calls.append((method, path, body))
            return {"id": "x"}

        self.qq.api = fake_api

    def typing(self, *args):
        buf = io.StringIO()
        old, sys.stdout = sys.stdout, buf
        try:
            rc = self.qq.cmd_typing(*args)
        finally:
            sys.stdout = old
        return rc, buf.getvalue().strip()

    def test_payload_uses_input_notify(self):
        rc, out = self.typing("owner1", "start", "m1")
        self.assertEqual((rc, out), (0, "TYPING_OK"))
        method, path, body = self.calls[0]
        self.assertEqual((method, path), ("POST", "/v2/users/owner1/messages"))
        self.assertEqual(body["msg_type"], 6)
        self.assertEqual(body["input_notify"]["input_type"], 1)
        self.assertLessEqual(body["input_notify"]["input_second"], 60)
        self.assertEqual(body["msg_id"], "m1")

    def test_typing_seq_never_collides_with_a_bubble(self):
        # The server dedupes on (msg_id, msg_seq) regardless of msg_type, so
        # a typing notification at seq 1 would make the first real bubble
        # fail with 40054005.
        self.typing("owner1", "start", "m1")
        self.assertEqual(self.calls[0][2]["msg_seq"], self.qq.TYPING_SEQ_BASE)
        self.assertNotIn(self.calls[0][2]["msg_seq"], range(1, 6))

    def test_stop_sends_the_cancel_type(self):
        self.typing("owner1", "stop", "m1")
        self.assertEqual(self.calls[0][2]["input_notify"]["input_type"], 2)

    def test_failure_is_not_an_error_exit(self):
        # The indicator is decoration; a failed call must not stop the worker.
        def boom(*a, **k):
            raise RuntimeError("429 too many")
        self.qq.api = boom
        rc, out = self.typing("owner1", "start", "m1")
        self.assertEqual(rc, 0)
        self.assertIn("TYPING_FAILED", out)

    def test_bad_args_refused(self):
        rc, out = self.typing("", "start", "m1")
        self.assertEqual(rc, 1)
        self.assertIn("TYPING_BAD_ARGS", out)
        self.assertEqual(self.calls, [])


class CredentialTest(unittest.TestCase):
    def test_missing_credentials_is_explicit(self):
        d = tempfile.mkdtemp(prefix="qqbot-nocred-")
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        old_env = {k: os.environ.pop(k) for k in ("QQ_APP_ID", "QQ_APP_SECRET")
                   if k in os.environ}
        try:
            qq = load_qqbot(d)
            with self.assertRaises(RuntimeError) as ctx:
                qq.get_token()
            self.assertIn("NO_CREDENTIALS", str(ctx.exception))
        finally:
            os.environ.update(old_env)


if __name__ == "__main__":
    unittest.main()
