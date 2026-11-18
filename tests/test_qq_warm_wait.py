#!/usr/bin/env python3
"""Tests for examples/qq-bot/qq-warm-wait.sh.

Same three exits as the WeChat variant, plus the QQ-specific reason the
idle window is configurable:

  * IDLE_TIMEOUT  quiet for QQ_WARM_IDLE seconds, marker removed
  * NEW_MESSAGES  a batch appears in pending, marker kept
  * TOTAL_CAP     the session's total cap is already past

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
HELPER = os.path.join(ROOT, "examples", "qq-bot", "qq-warm-wait.sh")
BASH = shutil.which("bash") or "bash"


def _posix(path):
    p = os.path.abspath(path)
    drive, rest = os.path.splitdrive(p)
    if not drive:
        return p
    return "/" + drive[0].lower() + rest.replace("\\", "/")


@unittest.skipUnless(shutil.which("jq") and shutil.which("bash"),
                     "needs bash + jq")
class QqWarmWaitTest(unittest.TestCase):
    def setUp(self):
        self.state = tempfile.mkdtemp(prefix="qq-warm-")
        self.addCleanup(shutil.rmtree, self.state, ignore_errors=True)
        self.env = dict(os.environ)
        self.env.update({"QQ_HOOK_STATE_DIR": _posix(self.state),
                         "QQ_WARM_IDLE": "2", "QQ_WARM_POLL": "1",
                         "QQ_WARM_TOTAL": "1800"})

    def run_helper(self, timeout=15):
        return subprocess.run([BASH, _posix(HELPER)], env=self.env,
                              capture_output=True, text=True, timeout=timeout)

    def marker(self):
        p = os.path.join(self.state, "qq-warm.json")
        if not os.path.exists(p):
            return None
        with open(p) as f:
            return json.load(f)

    def test_idle_timeout_removes_marker(self):
        r = self.run_helper()
        self.assertEqual(r.stdout.strip(), "IDLE_TIMEOUT")
        self.assertIsNone(self.marker())

    def test_new_messages_exit_keeps_marker(self):
        proc = subprocess.Popen([BASH, _posix(HELPER)], env=self.env,
                                stdout=subprocess.PIPE, text=True)
        time.sleep(0.5)
        with open(os.path.join(self.state, "qq-pending.json"), "w") as f:
            json.dump({"msgs": [{"message_id": "m1"}]}, f)
        out, _ = proc.communicate(timeout=10)
        self.assertEqual(out.strip(), "NEW_MESSAGES")
        self.assertIsNotNone(self.marker())

    def test_dead_session_marker_hits_total_cap(self):
        now = int(time.time())
        with open(os.path.join(self.state, "qq-warm.json"), "w") as f:
            json.dump({"since": now - 3600, "activity": now - 3600,
                       "heartbeat": now - 3600, "until": now - 1800}, f)
        r = self.run_helper()
        self.assertEqual(r.stdout.strip(), "TOTAL_CAP")
        self.assertIsNone(self.marker())

    def test_group_friendly_short_idle_is_honored(self):
        # Groups have a 5-minute passive window; the README tells operators
        # to shorten the idle wait. Prove the knob actually takes effect.
        self.env.update({"QQ_WARM_IDLE": "1", "QQ_WARM_POLL": "1"})
        started = time.time()
        r = self.run_helper()
        self.assertEqual(r.stdout.strip(), "IDLE_TIMEOUT")
        self.assertLess(time.time() - started, 10)


if __name__ == "__main__":
    unittest.main()
