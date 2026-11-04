"""离线回归：配置入口、Windows 锁、审批并发与通知重试；不发送飞书消息。"""
import atexit
import json
import logging
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

BASE = Path(__file__).resolve().parents[1]
_module_runtime = tempfile.TemporaryDirectory()
os.environ["DSH_FEISHU_BRIDGE_DIR"] = _module_runtime.name
def _cleanup_runtime():
    logging.shutdown()
    _module_runtime.cleanup()
atexit.register(_cleanup_runtime)
sys.path.insert(0, str(BASE))
import bridge
import bridge_env


class RegressionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.patches = [patch.object(bridge, "BRIDGE_DIR", self.root),
                        patch.object(bridge, "log", Mock()),
                        patch.object(bridge, "_approval_pipe_dir", return_value=self.root),
                        patch.object(bridge, "_read_state_chat_map", return_value={"session-a": "chat-a"})]
        for item in self.patches:
            item.start()
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(lambda: [item.stop() for item in reversed(self.patches)])
        self.worker = bridge.Bridge.__new__(bridge.Bridge)
        self.worker.allowed_chat_ids = {"chat-a", "chat-b"}
        self.worker.feishu = Mock()

    def pending(self, **kwargs):
        req = {"id": "abcd1234", "sessionId": "session-a", "toolName": "pwsh",
               "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "timeoutSeconds": 300}
        req.update(kwargs)
        path = self.root / "pending" / "abcd1234.json"
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps(req), encoding="utf-8")
        return req, path

    def test_entry_uses_same_env_file_before_import(self):
        env_file = self.root / ".env"
        env_file.write_text("FEISHU_APP_ID=test-app\nFEISHU_APP_SECRET=test-secret\n", encoding="utf-8-sig")
        observed = {}
        def main():
            observed.update({key: os.environ.get(key) for key in
                             ("FEISHU_ENV_FILE", "FEISHU_APP_ID", "FEISHU_APP_SECRET")})
            return 0
        with patch.dict(os.environ, {"FEISHU_ENV_FILE": str(env_file)}, clear=True), \
                patch.dict(sys.modules, {"bridge": SimpleNamespace(main=main)}):
            with self.assertRaises(SystemExit) as result:
                runpy.run_path(str(BASE / "bridge_env.py"), run_name="__main__")
        self.assertEqual(result.exception.code, 0)
        self.assertEqual(observed["FEISHU_ENV_FILE"], str(env_file))
        self.assertEqual(observed["FEISHU_APP_ID"], "test-app")
        self.assertEqual(observed["FEISHU_APP_SECRET"], "test-secret")

    def test_environment_credentials_override_file(self):
        with patch.object(bridge, "_load_hermes_env", return_value={"FEISHU_APP_ID": "old", "FEISHU_APP_SECRET": "old"}), \
                patch.dict(os.environ, {"FEISHU_APP_ID": "test-app", "FEISHU_APP_SECRET": "test-secret"}), \
                patch.object(bridge, "FeishuSender"), patch.object(bridge, "CodexChannel"), \
                patch.object(bridge.Bridge, "_build_seed_memory_prompt", return_value=""):
            worker = bridge.Bridge()
        try:
            self.assertEqual(worker.app_id, "test-app")
            self.assertEqual(worker.app_secret, "test-secret")
        finally:
            for pool in (worker.executor, worker.codex_executor, worker.approval_executor):
                pool.shutdown(wait=True)

    def test_lock_blocks_another_process_then_releases(self):
        marker = self.root / "acquired"
        attempted = self.root / "attempted"
        code = ("import sys;from pathlib import Path;sys.path.insert(0,sys.argv[1]);import bridge;"
                "bridge.BRIDGE_DIR=Path(sys.argv[2]);Path(sys.argv[3]).touch();"
                "exec('with bridge._exclusive_chat_lock(\"chat-a\"):\\n Path(sys.argv[4]).touch()')")
        env = dict(os.environ, DSH_FEISHU_BRIDGE_DIR=str(self.root))
        proc = None
        try:
            with bridge._exclusive_chat_lock("chat-a"):
                proc = subprocess.Popen([sys.executable, "-c", code, str(BASE), str(self.root), str(attempted), str(marker)], env=env)
                deadline = time.monotonic() + 5
                while not attempted.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(attempted.exists())
                time.sleep(0.2)
                self.assertFalse(marker.exists())
            self.assertEqual(proc.wait(timeout=5), 0)
            self.assertTrue(marker.exists())
            with self.assertRaises(ValueError):
                with bridge._exclusive_chat_lock("chat-a"):
                    raise ValueError("release")
            with bridge._exclusive_chat_lock("chat-a"):
                pass
        finally:
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait()

    def test_approval_bypasses_busy_pool_and_chat_lock(self):
        gate, handled = threading.Event(), threading.Event()
        worker = self.worker
        worker.allowed_senders = {"sender-a"}
        worker.executor = ThreadPoolExecutor(max_workers=1)
        worker.approval_executor = ThreadPoolExecutor(max_workers=1)
        worker.executor.submit(gate.wait)
        worker._claim_message = Mock(return_value=True)
        worker._chat_lock = Mock(side_effect=AssertionError("must bypass chat lock"))
        worker._run_approval_command = Mock(side_effect=lambda *args: handled.set())
        msg = SimpleNamespace(chat_id="chat-a", message_id="message-a", message_type="text",
                              content=json.dumps({"text": "批准 abcd1234"}))
        event = SimpleNamespace(message=msg, sender=SimpleNamespace(sender_id=SimpleNamespace(open_id="sender-a")))
        try:
            worker.on_feishu_message(SimpleNamespace(event=event))
            self.assertTrue(handled.wait(2), "approval queued behind running task")
            worker._chat_lock.assert_not_called()
        finally:
            gate.set()
            worker.executor.shutdown(wait=True)
            worker.approval_executor.shutdown(wait=True)

    def test_watcher_handles_claimed_request_after_restart(self):
        _, path = self.pending()
        os.utime(path, (time.time() - 120, time.time() - 120))
        claim = self.root / "claim"
        claim.mkdir()
        (claim / path.name).write_text("{}", encoding="utf-8")
        self.worker._notify_approval = Mock()
        with patch.object(bridge.time, "sleep", side_effect=StopIteration):
            with self.assertRaises(StopIteration):
                self.worker._approval_watcher()
        self.worker._notify_approval.assert_called_once()

    def test_failed_notification_retries(self):
        req, _ = self.pending()
        self.worker.feishu.send_card.side_effect = [RuntimeError("temporary"), None]
        notice = self.root / "notified" / "abcd1234.json"
        self.worker._notify_approval(req)
        self.assertFalse(notice.exists())
        self.worker._notify_approval(req)
        self.assertTrue(notice.exists())

    def test_notifications_only_go_to_owning_chat(self):
        req, _ = self.pending(sessionId="unknown-session")
        self.worker._notify_approval(req)
        self.worker.feishu.send_card.assert_not_called()

    def test_approval_rejects_wrong_chat_and_expired_request(self):
        self.pending()
        self.worker._run_approval_command("chat-b", "message-a", "批准 abcd1234")
        self.assertFalse((self.root / "decisions" / "abcd1234.json").exists())
        self.pending(createdAt="2000-01-01T00:00:00Z")
        self.worker._run_approval_command("chat-a", "message-a", "批准 abcd1234")
        self.assertFalse((self.root / "decisions" / "abcd1234.json").exists())

    def test_approval_writes_once_without_overwriting_answer(self):
        self.pending()
        self.worker._run_approval_command("chat-a", "message-a", "批准 abcd1234")
        self.worker._run_approval_command("chat-a", "message-b", "拒绝 abcd1234")
        decision = json.loads((self.root / "decisions" / "abcd1234.json").read_text(encoding="utf-8"))
        self.assertEqual(decision["outcome"], "allowed-once")

    def test_bootstrap_uses_sender(self):
        self.worker._send_handshake("chat-a", "sender-a")
        self.worker.feishu.send_card.assert_called_once()

    def test_websocket_uses_installed_sdk_signature(self):
        observed = []
        class CompatibleClient:
            def __init__(self, app_id, app_secret, log_level=None, event_handler=None,
                         domain=None, auto_reconnect=True):
                observed.append((app_id, domain, auto_reconnect))
            def start(self):
                pass
        builder = MagicMock()
        for name in ("register_p2_im_message_receive_v1", "register_p2_im_message_message_read_v1",
                     "register_p2_im_message_reaction_created_v1", "register_p2_im_message_reaction_deleted_v1",
                     "register_p2_im_message_recalled_v1", "register_p2_im_chat_member_bot_added_v1",
                     "register_p2_im_chat_member_bot_deleted_v1", "register_p2_im_chat_access_event_bot_p2p_chat_entered_v1",
                     "register_p2_card_action_trigger"):
            getattr(builder, name).return_value = builder
        modules = {
            "lark_oapi": SimpleNamespace(LogLevel=SimpleNamespace(INFO=1)),
            "lark_oapi.event.callback.model.p2_card_action_trigger": SimpleNamespace(P2CardActionTriggerResponse=Mock()),
            "lark_oapi.event.dispatcher_handler": SimpleNamespace(EventDispatcherHandler=SimpleNamespace(builder=lambda *args: builder)),
            "lark_oapi.ws": SimpleNamespace(Client=CompatibleClient),
        }
        self.worker.app_id, self.worker.app_secret = "test-app", "test-secret"
        with patch.dict(sys.modules, modules), patch.object(bridge.threading, "Thread"):
            self.worker.start_feishu()
        self.assertEqual(observed, [("test-app", "https://open.feishu.cn", True)])


if __name__ == "__main__":
    unittest.main(verbosity=2)
