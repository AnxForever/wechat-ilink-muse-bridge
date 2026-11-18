#!/usr/bin/env python3
"""Integration tests for examples/feishu-bot/feishu-watch.sh.

Same job as the WeChat and QQ suites: run the driver in a sandbox against a
fake client and pin the two-phase contract end to end. The failure modes are
the same on every channel, so the same cases are pinned here:

  * a batch is queued BEFORE commit, exactly once
  * an empty batch still commits (the inbox must drain even with nothing
    to wake the worker for)
  * a failing commit leaves the queue intact and is retried at the START
    of the next round, before any new poll
  * stale-claim recycling never overwrites a non-empty pending queue
  * a hard crash (empty poll output) is a poll failure, not silence
  * a corrupt pending queue is quarantined, never treated as empty
  * a second instance cannot run while the state-dir lock is held
  * a missing credential wakes `token_expired` once, then throttles
  * an unbound bot wakes `not_bound` instead of silently doing nothing
  * warm-worker suppression works the same as on WeChat

One case is Feishu-specific: the bot API has no "typing" indicator, so the
driver must never signal one. A `TYPING` call in the trace is a bug.

The script is copied into the sandbox with ONE test-only substitution:
`-cmin +10` becomes `-mmin +10` so `touch -d` can age queue files (ctime
cannot be backdated). The logic under test is unchanged.

    python3 -m unittest discover -s tests -v
"""
import json
import os
import shutil
import subprocess
import tempfile
import time
import unittest

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
SCRIPT_SRC = os.path.join(ROOT, "examples", "feishu-bot", "feishu-watch.sh")
BASH = shutil.which("bash") or "bash"


def _posix(path):
    """Git Bash rejects `C:\\...` on its command line, so every path handed to
    bash is passed as `/c/...`. Non-Windows paths pass through."""
    p = os.path.abspath(path)
    drive, rest = os.path.splitdrive(p)
    if not drive:
        return p
    return "/" + drive[0].lower() + rest.replace("\\", "/")

RUNTIME_STUB = """# Test stub for the Muse hook runtime library (the script sources it).
log() { echo "LOG $*" >> "$TRACE_CALLS"; }
silent() { echo "SILENT $*" >> "$TRACE_CALLS"; }
wake() { echo "WAKE $1 $2" >> "$TRACE_CALLS"; }
"""

FAKE_FEISHUBOT = """#!/usr/bin/env python3
import json, os, sys
trace = os.environ["TRACE_CALLS"]
HERE = os.path.dirname(os.path.abspath(__file__))
CURSOR_FILE = os.path.join(HERE, "cursor")
FAIL_ONCE_FILE = os.path.join(HERE, "fail_commit_once")
cmd = sys.argv[1] if len(sys.argv) > 1 else ""
if cmd == "poll":
    if os.environ.get("FAKE_POLL_OUTPUT") == "unbound":
        print("NO_OWNER")
        sys.exit(1)
    if os.environ.get("FAKE_POLL_OUTPUT") == "nokey":
        print("NO_CREDENTIALS: set FEISHU_APP_ID/FEISHU_APP_SECRET")
        sys.exit(1)
    cur = ""
    if os.path.exists(CURSOR_FILE):
        with open(CURSOR_FILE) as f:
            cur = f.read().strip()
    out = os.environ.get("FAKE_OUT2", "") if cur == "Q2" \\
        else os.environ.get("FAKE_OUT", "")
    if out:
        print(out)
elif cmd == "commit":
    data = sys.stdin.read()
    if os.environ.get("FAKE_COMMIT_FAIL") or os.path.exists(FAIL_ONCE_FILE):
        if os.path.exists(FAIL_ONCE_FILE):
            os.remove(FAIL_ONCE_FILE)
        with open(trace, "a") as f:
            f.write("COMMIT_FAIL " + data.strip() + "\\n")
        sys.exit(1)
    pend = os.path.join(os.environ.get("FEISHU_HOOK_STATE_DIR")
                        or os.environ["FEISHU_HOOK_STATE"], "feishu-pending.json")
    order = "ORDER_OK" if os.path.exists(pend) else "ORDER_NA"
    with open(trace, "a") as f:
        f.write("COMMIT " + data.strip() + " " + order + "\\n")
    try:
        payload = json.loads(data)
    except Exception:
        payload = {}
    if payload.get("cursor"):
        with open(CURSOR_FILE, "w") as f:
            f.write(payload["cursor"])
elif cmd == "typing":
    # Feishu has no bot typing API; reaching this branch is a failure.
    with open(trace, "a") as f:
        f.write("TYPING " + " ".join(sys.argv[2:]) + "\\n")
"""

BATCH = json.dumps({
    "msgs": [{"from_user_id": "ou_owner1", "message_id": "om_1",
              "chat_type": "c2c", "reply_to": "ou_owner1",
              "kind": "text", "text": "hi"}],
    "commit": {"cursor": "Q2", "ids": ["om_1"], "files": ["0000000001-om_1.json"]},
})
EMPTY = json.dumps({"msgs": [], "commit": {"cursor": "Q9", "ids": [],
                                           "files": []}})


@unittest.skipUnless(shutil.which("jq") and shutil.which("bash")
                     and shutil.which("flock") and shutil.which("timeout"),
                     "needs bash + jq + flock + timeout (Git Bash on Windows "
                     "has no flock, and these scripts are single-instance "
                     "locked with it)")
class FeishuHookIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="feishu-hook-it-")
        self.bridge = os.path.join(self.dir, "bridge")
        self.state = os.path.join(self.dir, "state")
        os.makedirs(self.bridge)
        os.makedirs(self.state)
        self.calls = os.path.join(self.dir, "calls.log")
        open(self.calls, "w").close()
        with open(os.path.join(self.bridge, "feishubot.py"), "w") as f:
            f.write(FAKE_FEISHUBOT)
        self.runtime = os.path.join(self.dir, "runtime.sh")
        with open(self.runtime, "w") as f:
            f.write(RUNTIME_STUB)
        with open(SCRIPT_SRC) as f:
            script = f.read().replace("-cmin +10", "-mmin +10")
        self.script = os.path.join(self.dir, "watch.sh")
        with open(self.script, "w") as f:
            f.write(script)
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def run_hook(self, fake_out=None, dry=False, extra_env=None):
        env = dict(os.environ)
        env.update({
            "FEISHU_DATA_DIR": _posix(self.bridge),
            # Pin BOTH names: the suite shares one process env, so an
            # unpinned alias would leak another module's state dir in.
            "FEISHU_HOOK_STATE_DIR": _posix(self.state),
            "FEISHU_HOOK_STATE": _posix(self.state),
            "FEISHU_CLIENT": _posix(os.path.join(self.bridge, "feishubot.py")),
            "HATCH_HOOK_RUNTIME": _posix(self.runtime),
            "HATCH_HOOK_DRY_RUN": "1" if dry else "0",
            "TRACE_CALLS": _posix(self.calls),
        })
        if fake_out is not None:
            env["FAKE_OUT"] = fake_out
        if extra_env:
            env.update(extra_env)
        r = subprocess.run([BASH, _posix(self.script)], env=env,
                           capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, r.stderr
        with open(self.calls) as f:
            return f.read()

    def pending(self):
        p = os.path.join(self.state, "feishu-pending.json")
        if not os.path.exists(p):
            return None
        with open(p) as f:
            return json.load(f)

    def test_batch_is_queued_before_commit(self):
        calls = self.run_hook(BATCH)
        self.assertIn("new_feishu_messages", calls)
        self.assertEqual(self.pending(), {"msgs": json.loads(BATCH)["msgs"]})
        self.assertIn('COMMIT {"cursor":"Q2"', calls)
        self.assertIn("ORDER_OK", calls)

    def test_no_typing_signal_is_ever_sent(self):
        # The Feishu bot API has no typing indicator. Pretending otherwise
        # would mean shipping a call that always fails.
        calls = self.run_hook(BATCH)
        self.assertNotIn("TYPING", calls)

    def test_empty_batch_still_commits(self):
        calls = self.run_hook(EMPTY)
        self.assertNotIn("new_feishu_messages", calls)
        self.assertIsNone(self.pending())
        self.assertIn('COMMIT {"cursor":"Q9"', calls)

    def test_failed_commit_keeps_queue(self):
        calls = self.run_hook(BATCH, extra_env={"FAKE_COMMIT_FAIL": "1"})
        self.assertIn("new_feishu_messages", calls)
        self.assertEqual(self.pending(), {"msgs": json.loads(BATCH)["msgs"]})
        with open(os.path.join(self.bridge, "poll.log")) as f:
            self.assertIn("COMMIT_FAILED", f.read())

    def test_recycling_never_overwrites_pending(self):
        proc = os.path.join(self.state, "feishu-processing.json")
        with open(proc, "w") as f:
            json.dump({"msgs": [{"message_id": 1}]}, f)
        subprocess.run(["touch", "-d", "15 minutes ago", proc], check=True)
        pend_path = os.path.join(self.state, "feishu-pending.json")
        with open(pend_path, "w") as f:
            json.dump({"msgs": [{"message_id": 2}]}, f)
        self.run_hook(EMPTY)
        self.assertEqual(self.pending(), {"msgs": [{"message_id": 2}]})
        self.assertTrue(os.path.exists(proc))  # deferred, not moved

    def test_crash_output_is_a_poll_failure(self):
        self.run_hook("")
        with open(os.path.join(self.bridge, "poll.log")) as f:
            self.assertIn("POLL_CRASH", f.read())
        with open(os.path.join(self.bridge, "alert-state.json")) as f:
            self.assertIn("err_since", json.load(f))

    def test_missing_credentials_wakes_token_expired_once(self):
        calls = self.run_hook(extra_env={"FAKE_POLL_OUTPUT": "nokey"})
        self.assertIn("WAKE token_expired {}", calls)
        open(self.calls, "w").close()
        calls2 = self.run_hook(extra_env={"FAKE_POLL_OUTPUT": "nokey"})
        self.assertNotIn("WAKE token_expired", calls2)
        self.assertIn("throttled", calls2)

    def test_unbound_bot_wakes_not_bound(self):
        calls = self.run_hook(extra_env={"FAKE_POLL_OUTPUT": "unbound"})
        self.assertIn("WAKE not_bound", calls)

    def test_unbound_then_recovery_reports_bind(self):
        self.run_hook(extra_env={"FAKE_POLL_OUTPUT": "unbound"})
        open(self.calls, "w").close()
        calls = self.run_hook(EMPTY)
        self.assertIn('WAKE recovered {"kind":"bind"', calls)

    def write_warm(self, heartbeat_age=0, until_offset=1800):
        now = int(time.time())
        warm = {"since": now - 60, "activity": now - 10,
                "heartbeat": now - heartbeat_age,
                "until": now + until_offset}
        with open(os.path.join(self.state, "feishu-warm.json"), "w") as f:
            json.dump(warm, f)

    def test_warm_worker_suppresses_pending_wake(self):
        pend_path = os.path.join(self.state, "feishu-pending.json")
        with open(pend_path, "w") as f:
            json.dump({"msgs": [{"from_user_id": "ou_owner1",
                                 "message_id": "om_21"}]}, f)
        self.write_warm()
        calls = self.run_hook()
        self.assertNotIn("WAKE", calls)
        self.assertIn("warm worker on duty", calls)

    def test_stale_warm_marker_rearms_wake(self):
        pend_path = os.path.join(self.state, "feishu-pending.json")
        with open(pend_path, "w") as f:
            json.dump({"msgs": [{"from_user_id": "ou_owner1",
                                 "message_id": "om_22"}]}, f)
        self.write_warm(heartbeat_age=120)
        calls = self.run_hook()
        self.assertIn("WAKE pending_feishu_messages", calls)

    def test_corrupt_pending_is_quarantined(self):
        pend_path = os.path.join(self.state, "feishu-pending.json")
        with open(pend_path, "w") as f:
            f.write("{corrupt queue")
        calls = self.run_hook(EMPTY)
        self.assertFalse(os.path.exists(pend_path))
        quarantined = [n for n in os.listdir(self.state)
                       if n.startswith("feishu-pending.json.corrupt-")]
        self.assertEqual(len(quarantined), 1)
        self.assertIn("PENDING_CORRUPT", calls)
        self.assertIn('COMMIT {"cursor":"Q9"', calls)

    def test_structurally_invalid_pending_is_quarantined(self):
        pend_path = os.path.join(self.state, "feishu-pending.json")
        for content in ('{"msgs": "invalid"}', '{}', '{"msgs": 5}'):
            with open(pend_path, "w") as f:
                f.write(content)
            before = {n for n in os.listdir(self.state)
                      if n.startswith("feishu-pending.json.corrupt-")}
            calls = self.run_hook(EMPTY)
            self.assertFalse(os.path.exists(pend_path),
                             f"misshapen pending survived: {content}")
            after = {n for n in os.listdir(self.state)
                     if n.startswith("feishu-pending.json.corrupt-")}
            self.assertEqual(len(after - before), 1, content)
            self.assertIn("PENDING_CORRUPT", calls)

    def test_valid_empty_pending_is_quiet(self):
        pend_path = os.path.join(self.state, "feishu-pending.json")
        with open(pend_path, "w") as f:
            json.dump({"msgs": []}, f)
        calls = self.run_hook()
        self.assertIn("pending file empty", calls)
        self.assertNotIn("PENDING_CORRUPT", calls)
        self.assertTrue(os.path.exists(pend_path))

    def test_lock_excludes_second_instance(self):
        import fcntl
        lock_path = os.path.join(self.state, "feishu-watch.lock")
        with open(lock_path, "w") as lock_f:
            fcntl.flock(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            calls = self.run_hook(BATCH)
        self.assertEqual(calls.strip(), "")
        self.assertIsNone(self.pending())

    def test_failed_commit_is_deferred_via_ledger_not_duplicated(self):
        open(os.path.join(self.bridge, "fail_commit_once"), "w").close()
        ledger = os.path.join(self.state, "feishu-commit-pending.json")
        calls1 = self.run_hook(BATCH)
        self.assertIn("new_feishu_messages", calls1)
        self.assertIn("COMMIT_FAIL", calls1)
        self.assertEqual(self.pending(), {"msgs": json.loads(BATCH)["msgs"]})
        with open(ledger) as f:
            saved = json.load(f)
        self.assertEqual(saved["cursor"], "Q2")
        # Worker claims the batch, then round 2 retries the ledger FIRST.
        os.remove(os.path.join(self.state, "feishu-pending.json"))
        open(self.calls, "w").close()
        calls2 = self.run_hook(BATCH, extra_env={"FAKE_OUT2": EMPTY})
        self.assertIn('COMMIT {"cursor":"Q2"', calls2)
        self.assertLess(calls2.index('COMMIT {"cursor":"Q2"'),
                        calls2.index('COMMIT {"cursor":"Q9"'))
        self.assertNotIn("new_feishu_messages", calls2)
        self.assertIsNone(self.pending())
        self.assertFalse(os.path.exists(ledger))
        with open(os.path.join(self.bridge, "cursor")) as f:
            self.assertEqual(f.read(), "Q9")

    def test_pending_wake_reports_real_message_count(self):
        pend_path = os.path.join(self.state, "feishu-pending.json")
        with open(pend_path, "w") as f:
            json.dump({"msgs": [{"from_user_id": "ou_owner1",
                                 "message_id": f"om_{n}"}
                                for n in (11, 12, 13)]}, f)
        calls = self.run_hook()
        self.assertIn('WAKE pending_feishu_messages {"count":3', calls)

    def test_dry_run_leaves_no_queue(self):
        calls = self.run_hook(BATCH, dry=True)
        self.assertIn("dry-run", calls)
        self.assertIsNone(self.pending())
        self.assertNotIn("COMMIT", calls)


if __name__ == "__main__":
    unittest.main()
