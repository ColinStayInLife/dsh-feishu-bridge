#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DSH <-> Feishu bridge.

Takes over the Feishu channel previously owned by the Hermes gateway on this
machine and forwards inbound DMs to a dedicated, persistent DeepSeek Harness
session through the local DSH web API. Outbound replies are sent as static
Feishu cards (no buttons), matching the owner's saved preference.
"""

from __future__ import annotations

try:
    import fcntl  # POSIX
except ImportError:  # Windows
    fcntl = None

import hashlib
import errno
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
import unicodedata
import uuid
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from http.cookiejar import CookieJar
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
try:
    from codex_commands import CodexChannel, is_command as is_codex_command
    CODEX_AVAILABLE = True
except ImportError:  # Windows 未迁移 Codex 桥时的空实现
    CODEX_AVAILABLE = False

    class CodexChannel:  # noqa: D101
        def __init__(self, *a, **k) -> None:
            pass

        def start(self) -> None:
            pass

        def stop(self) -> None:
            pass

    def is_codex_command(text):  # noqa: D103
        return False

# ---------------------------------------------------------------------------
# Paths / constants
# ---------------------------------------------------------------------------

BRIDGE_DIR = Path(os.getenv("DSH_FEISHU_BRIDGE_DIR", str(Path.home() / ".dsh" / "feishu-bridge")))
STATE_PATH = BRIDGE_DIR / "state.json"
LOG_PATH = BRIDGE_DIR / "logs" / "bridge.log"
HEARTBEAT_PATH = BRIDGE_DIR / "bridge.heartbeat"
# 凭据目录默认 ~/.hermes，可用 FEISHU_ENV_FILE 覆盖
HERMES_DIR = Path(os.getenv("FEISHU_ENV_DIR", str(Path.home() / ".hermes")))
HERMES_ENV_PATH = Path(os.getenv(
    "FEISHU_ENV_FILE",
    str(BRIDGE_DIR / ".env" if (BRIDGE_DIR / ".env").is_file() else HERMES_DIR / ".env"),
))
HERMES_USER_MD = HERMES_DIR / "memories" / "USER.md"
HERMES_MEMORY_MD = HERMES_DIR / "memories" / "MEMORY.md"

DSH_API_BASE = os.getenv("DSH_API_BASE", "http://127.0.0.1:3080").rstrip("/")
DSH_WORKSPACE_ID = os.getenv("DSH_FEISHU_WORKSPACE_ID", "")
# 单次轮询窗口。⚠️ 2026-09-18 修 bug：此前 900s（15 分钟）是**硬超时**，
# 超时后桥接直接放弃并 return，**那条回复永久丢失** —— 表现为"思考久了有结果也不回复"。
# 现在 900s 只是**一次轮询窗口**：到期只提示"仍在生成"，然后继续等，
# 直到 DSH_TURN_HARD_CAP 才真正放弃。
DSH_TURN_TIMEOUT = int(os.getenv("DSH_FEISHU_TURN_TIMEOUT", "900"))
DSH_TURN_HARD_CAP = int(os.getenv("DSH_FEISHU_TURN_HARD_CAP", "10800"))   # 追加等待上限，默认 3 小时

# 白名单：留空 = 拒绝所有消息。必须通过 FEISHU_ALLOWED_CHAT_IDS /
# FEISHU_ALLOWED_SENDER_OPEN_IDS 环境变量配置（首次可用 FEISHU_BOOTSTRAP=1 自报 id）
DEFAULT_ALLOWED_CHAT_IDS = ""
DEFAULT_ALLOWED_SENDERS = ""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(
            LOG_PATH,
            maxBytes=2_000_000,
            backupCount=3,
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger("dsh-feishu")


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------

def _hostname() -> str:
    """跨平台主机名（Windows 无 os.uname）。"""
    try:
        import socket
        return socket.gethostname()
    except Exception:
        return os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME") or "?"


def _load_hermes_env() -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not HERMES_ENV_PATH.exists():
        log.warning("Hermes .env not found: %s", HERMES_ENV_PATH)
        return env
    for raw in HERMES_ENV_PATH.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip("'").strip('"')
    return env


def _load_state() -> Dict[str, Any]:
    try:
        if STATE_PATH.exists():
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        log.exception("Failed to read bridge state; starting with empty state")
    return {
        "version": 1,
        "chats": {},           # chat_id -> {"session_id": str, "created_at": float}
        "seen_message_ids": {},  # message_id -> first_seen_ts
        "memory_seeded": {},    # session_id -> bool
    }


_state_lock = threading.RLock()


def _save_state(state: Dict[str, Any]) -> None:
    with _state_lock:
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE_PATH)


def _atomic_state_update(mutator) -> Any:
    with _state_lock:
        state = _load_state()
        result = mutator(state)
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE_PATH)
        return result


def _heartbeat() -> None:
    try:
        HEARTBEAT_PATH.write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "updated_at": time.time(),
                    "state": "running",
                }
            ),
            encoding="utf-8",
        )
    except Exception:
        log.exception("Failed to write heartbeat")


def _heartbeat_loop() -> None:
    while True:
        _heartbeat()
        time.sleep(30)


def _chat_lock_path(chat_id: str) -> Path:
    digest = hashlib.sha1(chat_id.encode("utf-8")).hexdigest()[:16]
    return BRIDGE_DIR / f"chat-{digest}.lock"


@contextmanager
def _exclusive_chat_lock(chat_id: str):
    """Cross-process serialization for one Feishu chat's DSH turns.

    The long-running bridge and the daily-report cron runner are separate
    processes.  Both must take this lock so a scheduled report and an inbound
    message cannot interleave two turns on the same persistent DSH session.
    """
    lock_path = _chat_lock_path(chat_id)
    with open(lock_path, "a+b") as lock_file:
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        else:
            import msvcrt

            if os.fstat(lock_file.fileno()).st_size == 0:
                lock_file.write(b"\0")
                lock_file.flush()
            while True:
                lock_file.seek(0)
                try:
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError as exc:
                    if exc.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                        raise
                    time.sleep(0.1)
            try:
                yield
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)


def _strip_feishu_ats(text: str) -> str:
    return re.sub(r"<at[^>]*>.*?</at>", "", text, flags=re.DOTALL).strip()


# ---------------------------------------------------------------------------
# 飞书卡片里的表格渲染
#
# 飞书交互卡片的 markdown 元素**不支持表格语法** —— 直接塞 | a | b | 只会显示
# 一堆竖线。参考 openclaw 的做法（它给每个渠道一个 markdown 表格模式：
# off / bullets / code / block；飞书没声明，走默认的 code），这里默认把表格
# 转成「等宽对齐的代码块」。比 openclaw 多一步：中文按 2 个字符宽度计算，
# 否则中英混排会歪。
# ---------------------------------------------------------------------------

# 卡片已升级到 JSON 2.0，markdown 元素原生支持表格 → 默认不再做转换。
# 表格渲染模式：
#   auto  —— 表格少时保留飞书原生表格（好看），超过阈值自动转等宽代码块（防超限）
#   off   —— 始终保留原生表格（表格多时会被飞书拒绝：ErrCode 11310）
#   code  —— 始终转等宽代码块
#   bullets —— 摊成「标题：值」要点
#
# ⚠️ 2026-09-18 修 bug：此前默认值是 "off"，但上面一行注释写的是「code（默认）」。
# 原生表格数量超过飞书上限时 update_card 报 11310（card table number over limit），
# 表现为**长回复在飞书里卡住不更新**。改为 auto + 阈值兜底。
TABLE_MODE = os.getenv("DSH_FEISHU_TABLE_MODE", "auto").strip().lower()
TABLE_MAX_NATIVE = int(os.getenv("DSH_FEISHU_TABLE_MAX_NATIVE", "5"))

_TABLE_SEP_RE = re.compile(r"^\s*\|?\s*:?-{2,}:?\s*(?:\|\s*:?-{2,}:?\s*)*\|?\s*$")
_FENCE_RE = re.compile(r"^\s*(```|~~~)")


def _disp_width(text: str) -> int:
    """终端等宽字体的显示宽度：东亚宽字符算 2，其余算 1。"""
    width = 0
    for ch in text:
        width += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return width


def _pad_to(text: str, width: int) -> str:
    pad = width - _disp_width(text)
    return text + " " * pad if pad > 0 else text


def _split_table_row(line: str) -> list:
    """拆一行表格，支持 \\| 转义，容忍首尾竖线缺失。"""
    raw = line.strip()
    if raw.startswith("|"):
        raw = raw[1:]
    if raw.endswith("|") and not raw.endswith("\\|"):
        raw = raw[:-1]
    cells, current, i = [], "", 0
    while i < len(raw):
        ch = raw[i]
        if ch == "\\" and i + 1 < len(raw) and raw[i + 1] == "|":
            current += "|"
            i += 2
            continue
        if ch == "|":
            cells.append(current.strip())
            current = ""
            i += 1
            continue
        current += ch
        i += 1
    cells.append(current.strip())
    return cells


def _render_table_as_code(header: list, rows: list) -> str:
    """等宽对齐的表格，包在代码块里（飞书卡片的默认做法）。"""
    cols = max([len(header)] + [len(r) for r in rows]) if (header or rows) else 0
    if cols == 0:
        return ""
    norm = lambda cells: [(cells[i] if i < len(cells) else "") for i in range(cols)]
    header, rows = norm(header), [norm(r) for r in rows]
    widths = [
        max([_disp_width(header[i])] + [_disp_width(r[i]) for r in rows])
        for i in range(cols)
    ]
    out = ["| " + " | ".join(_pad_to(header[i], widths[i]) for i in range(cols)) + " |"]
    out.append("|" + "|".join("-" * (widths[i] + 2) for i in range(cols)) + "|")
    for r in rows:
        out.append("| " + " | ".join(_pad_to(r[i], widths[i]) for i in range(cols)) + " |")
    return "```\n" + "\n".join(out) + "\n```"


def _render_table_as_bullets(header: list, rows: list) -> str:
    """把表格摊成「标题：值」的要点式排版。"""
    def name(i):
        return header[i] if i < len(header) and header[i] else f"列{i + 1}"

    out = []
    if len(header) > 1 and rows:
        for row in rows:
            if not row:
                continue
            if row[0]:
                out.append(f"**{row[0]}**")
            for i in range(1, len(row)):
                if row[i]:
                    out.append(f"  • {name(i)}：{row[i]}")
            out.append("")
    else:
        for row in rows:
            for i, value in enumerate(row):
                if value:
                    out.append(f"• **{name(i)}**：{value}")
            out.append("")
    return "\n".join(out).rstrip()


def _count_md_tables(text: str) -> int:
    """数一段 markdown 里有几个顶层表格（跳过代码块内部）。"""
    lines = text.split("\n")
    n, in_fence = 0, False
    for i, line in enumerate(lines):
        if _FENCE_RE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if "|" in line and i + 1 < len(lines) and _TABLE_SEP_RE.match(lines[i + 1]):
            n += 1
    return n


def render_markdown_tables(text: str, mode: Optional[str] = None) -> str:
    """把 markdown 表格转成飞书卡片能正常显示的形式。

    mode: auto（默认，按数量自动选）/ code（等宽代码块）/ bullets（要点式）/ off（原样保留）
    代码块内部的表格不动。

    auto 的判据：表格数 ≤ TABLE_MAX_NATIVE 时保留原生表格（观感最好）；
    超过则全部转代码块 —— 飞书对单卡原生表格数量有上限（ErrCode 11310）。
    """
    mode = (mode or TABLE_MODE or "auto").lower()
    if mode == "auto":
        mode = "off" if _count_md_tables(text) <= TABLE_MAX_NATIVE else "code"
    if mode == "off" or not text or "|" not in text:
        return text
    lines = text.split("\n")
    out, i, in_fence = [], 0, False
    while i < len(lines):
        line = lines[i]
        if _FENCE_RE.match(line):
            in_fence = not in_fence
            out.append(line)
            i += 1
            continue
        is_table = (
            not in_fence
            and "|" in line
            and i + 1 < len(lines)
            and _TABLE_SEP_RE.match(lines[i + 1])
        )
        if is_table:
            header = _split_table_row(line)
            rows, j = [], i + 2
            while j < len(lines) and lines[j].strip() and "|" in lines[j]:
                rows.append(_split_table_row(lines[j]))
                j += 1
            block = (
                _render_table_as_bullets(header, rows)
                if mode == "bullets"
                else _render_table_as_code(header, rows)
            )
            log.debug("Rendered markdown table (%d cols, %d rows) as %s",
                      len(header), len(rows), mode)
            out.append(block)
            i = j
            continue
        out.append(line)
        i += 1
    return "\n".join(out)


def _extract_text(message_type: str, content: str) -> str:
    """Extract plain text from the Feishu message content payload."""
    if not content:
        return ""
    try:
        data = json.loads(content)
    except Exception:
        return content.strip()

    if message_type == "text":
        return str(data.get("text", "")).strip()

    if message_type == "post":
        parts = []
        title = data.get("title") if isinstance(data, dict) else None
        if isinstance(title, str) and title.strip():
            parts.append(title.strip())
        post_content = data.get("content") if isinstance(data, dict) else None
        if isinstance(post_content, list):
            for row in post_content:
                if not isinstance(row, list):
                    continue
                for item in row:
                    if not isinstance(item, dict):
                        continue
                    item_text = item.get("text")
                    if isinstance(item_text, str):
                        parts.append(item_text)
        elif isinstance(post_content, str):
            parts.append(post_content)
        return " ".join(parts).strip()

    return ""


def _read_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        log.warning("Could not read %s", path)
        return ""


# Reset commands are handled by the bridge itself (never forwarded to DSH).
_RESET_COMMANDS = {
    "/new",
    "/reset",
    "/clear",
    "/新会话",
    "／new",
    "／reset",
}


def _is_reset_command(text: str) -> bool:
    stripped = (text or "").strip()
    return stripped.lower() in _RESET_COMMANDS or stripped in _RESET_COMMANDS


def _is_table_limit_error(message: str) -> bool:
    """Detect Feishu's 'card table number over limit' rejection.

    ErrCode 11310 / "card table number over limit"。飞书卡片对单卡原生表格数量有
    上限，超过后整张卡被拒 —— 表现为长回复在飞书里**卡住不更新**（只看到"⏳ 生成中"）。
    兜底做法：改用等宽代码块重新渲染（代码块不占表格额度）。
    """
    msg_l = (message or "").lower()
    return ("11310" in msg_l
            or "table number over limit" in msg_l
            or "table" in msg_l and "limit" in msg_l)


def _is_moderation_error(code: str, message: str) -> bool:
    """Detect DeepSeek content-moderation rejections (HTTP 400 risk control).

    Once such a message enters a session's history, every later turn on that
    session keeps failing because the harness resends the whole transcript.
    The bridge reacts by resetting the session (see ``_run_dsh_turn``).
    """
    code_l = (code or "").lower()
    msg_l = (message or "").lower()
    if "content exists risk" in msg_l:
        return True
    if "risk" in msg_l or "moderation" in msg_l or "content_filter" in msg_l:
        return True
    if "内容" in (message or "") and "风险" in (message or ""):
        return True
    return code_l == "invalid_request" and "content" in msg_l


# ---------------------------------------------------------------------------
# 审批流转（宿主插件 dsh-approval-feishu <-> 飞书）
# ---------------------------------------------------------------------------
#
# 宿主插件截住 approval/request 后，把待办写成 <pipe>/pending/<id>.json，并用
# 独占创建占住 <pipe>/claim/<id>.json（抢到才算数）。本桥负责：发现新待办 →
# 发飞书卡片 → 收「批准/拒绝 <id>」→ 写 <pipe>/decisions/<id>.json 回执。
#
# 只有盖过归属戳的会话（<pipe>/channels/<sessionId>）插件才接管，所以 Web UI
# 的会话不会被抢卡片；超时/桥掉线由插件 fail-closed 成拒绝。

_NL = chr(10)
_APPROVE_RE = re.compile(
    "[" + re.escape(" /") + r"]*(?:批准|approve|allow|允许|同意)"
    + r"[" + re.escape(" :：") + r"]*([0-9a-fA-F]{4,16})[" + re.escape(" ") + r"]*$"
)
_REJECT_RE = re.compile(
    "[" + re.escape(" /") + r"]*(?:拒绝|reject|deny|驳回|不允许)"
    + r"[" + re.escape(" :：") + r"]*([0-9a-fA-F]{4,16})[" + re.escape(" ") + r"]*$"
)

APPROVAL_POLL_SECONDS = float(os.getenv("FEISHU_APPROVAL_POLL_SECONDS", "1.5"))


def _approval_pipe_dir() -> Path:
    raw = os.getenv("FEISHU_APPROVAL_PIPE_DIR", "").strip()
    return Path(raw) if raw else Path.home() / ".dsh" / "approvals"


def _stamp_session_owner(session_id: str) -> None:
    """给会话盖上「审批归飞书管」的戳；失败只告警，不影响正常对话。"""
    if not session_id:
        return
    try:
        folder = _approval_pipe_dir() / "channels"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / session_id).write_text("feishu", encoding="utf-8")
    except Exception:
        log.warning("无法写入审批归属戳 session=%s", session_id, exc_info=True)


def _approval_keyword_only(text: str) -> str:
    """用户只写了「批准」/「拒绝」而没带编号时返回关键词，否则空串。"""
    stripped = (text or "").strip()
    for keyword in ("批准", "拒绝", "approve", "reject", "allow", "deny", "同意", "驳回"):
        if stripped.lower() == keyword.lower():
            return keyword
    return ""


def _looks_like_approval_command(text: str) -> bool:
    stripped = (text or "").strip()
    return bool(_APPROVE_RE.match(stripped) or _REJECT_RE.match(stripped))


def _parse_approval_command(text: str):
    stripped = (text or "").strip()
    m = _APPROVE_RE.match(stripped)
    if m:
        return m.group(1).lower(), "allowed-once"
    m = _REJECT_RE.match(stripped)
    if m:
        return m.group(1).lower(), "rejected"
    return None, None


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _approval_expired(req: Dict[str, Any], path: Path) -> bool:
    from datetime import datetime

    try:
        created = req.get("createdAt")
        started = datetime.fromisoformat(str(created).replace("Z", "+00:00")).timestamp() if created else path.stat().st_mtime
        timeout = float(req.get("timeoutSeconds", 300))
        return timeout <= 0 or time.time() >= started + timeout
    except (ValueError, TypeError, OSError):
        return True


def _write_decision(pipe: Path, request_id: str, outcome: str, by: str) -> bool:
    """原子写裁决文件；失败返回 False，由调用方如实告诉用户。"""
    decisions = pipe / "decisions"
    try:
        decisions.mkdir(parents=True, exist_ok=True)
        payload = {
            "type": "approval-decision",
            "id": request_id,
            "outcome": outcome,
            "by": by,
            "decidedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        target = decisions / (request_id + ".json")
        tmp = decisions / ("." + request_id + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(str(tmp), str(target))
        return True
    except Exception:
        log.exception("写审批裁决失败 id=%s", request_id)
        return False


def _read_state_chat_map() -> Dict[str, str]:
    """session_id -> chat_id（state.json 是 chat -> session 的映射）。"""
    out: Dict[str, str] = {}
    try:
        state = _load_state()
        for chat_id, item in ((state or {}).get("chats") or {}).items():
            if not isinstance(item, dict):
                continue
            session_id = str(item.get("session_id") or "")
            if session_id:
                out[session_id] = str(chat_id)
    except Exception:
        log.debug("读取 state.json 失败（审批通知将广播）", exc_info=True)
    return out


def _describe_approval(req: Dict[str, Any]) -> Tuple[str, str]:
    tool = str(req.get("toolName") or "unknown")
    reason = str(req.get("reason") or "").strip()
    command = str(req.get("command") or "").strip()
    label = {
        "pwsh": "执行命令（需要提权）",
        "bash": "执行命令（需要提权）",
        "str-replace-editor": "修改文件",
        "fs": "文件写入",
    }.get(tool, tool)
    lines = ["请求工具: " + label, "编号: " + str(req.get("id") or "")]
    if reason:
        lines.append("原因: " + reason[:400])
    if req.get("cwd"):
        lines.append("目录: " + str(req["cwd"]))
    waited = req.get("timeoutSeconds")
    if waited:
        lines.append("超时秒数: " + str(waited) + "（超时自动拒绝）")
    return label, _NL.join(lines)



# ---------------------------------------------------------------------------
# DSH web API client
# ---------------------------------------------------------------------------


class DshApiError(RuntimeError):
    def __init__(self, code: str, message: str, details: Any = None):
        super().__init__(f"DSH API error [{code}]: {message}")
        self.code = code
        self.message = message
        self.details = details


class DshApi:
    def __init__(self, base_url: str = DSH_API_BASE) -> None:
        self.base_url = base_url
        self._auth_cookie: Optional[Tuple[str, str]] = None

    # ------------------------------------------------------------------
    # Browser-session auth.  The DSH web API authenticates requests with
    # the signed session cookie that the printed `dsh web:` startup URL
    # exchanges for.  The signing secret is durable across harness
    # restarts, so a cached cookie stays valid until its 30-day expiry;
    # on a 401 we re-exchange once against the latest startup URL from
    # the harness journal.
    # ------------------------------------------------------------------

    def _load_cookie(self) -> Optional[Tuple[str, str]]:
        if self._auth_cookie is None:
            saved = _load_state().get("dsh_auth_cookie")
            if isinstance(saved, dict) and saved.get("name") and saved.get("value"):
                self._auth_cookie = (str(saved["name"]), str(saved["value"]))
        return self._auth_cookie

    def _save_cookie(self, cookie: Tuple[str, str]) -> None:
        self._auth_cookie = cookie
        _atomic_state_update(
            lambda state: state.__setitem__(
                "dsh_auth_cookie", {"name": cookie[0], "value": cookie[1]}
            )
        )

    def _acquire_cookie(self) -> Optional[Tuple[str, str]]:
        """从本机 Web 日志交换 Cookie；Windows 无 journalctl。"""
        sources = []
        log_paths = [BRIDGE_DIR / "logs" / name for name in ("web_stdout.log", "web.log")]
        for path in sorted(
            (p for p in log_paths if p.is_file()),
            key=lambda p: p.stat().st_mtime, reverse=True,
        ):
            try:
                with path.open("rb") as stream:
                    stream.seek(max(0, path.stat().st_size - 262144))
                    raw = stream.read()
                encoding = "utf-16" if raw.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8"
                sources.append(raw.decode(encoding, errors="replace").replace("\x00", ""))
            except OSError:
                log.warning("Cannot read DSH startup log: %s", path)
        if os.name != "nt":
            try:
                proc = subprocess.run(
                    ["journalctl", "--user", "-u", "deepseek-harness.service",
                     "--no-pager", "-n", "200"],
                    capture_output=True, text=True, timeout=15,
                )
                sources.append(proc.stdout)
            except OSError:
                log.warning("Cannot read DSH startup journal")
        origin = urlsplit(self.base_url)
        seen = set()
        for source in sources:
            for token_url in reversed(re.findall(r"dsh web: (https?://\S+)", source)):
                candidate = urlsplit(token_url)
                if (candidate.scheme, candidate.netloc) != (origin.scheme, origin.netloc):
                    continue
                if token_url in seen:
                    continue
                seen.add(token_url)
                jar = CookieJar()
                opener = urlrequest.build_opener(urlrequest.HTTPCookieProcessor(jar))
                try:
                    with opener.open(token_url, timeout=10) as resp:
                        resp.read()
                except Exception as exc:
                    # 异常文本可能含启动 token，日志仅记异常类型。
                    log.warning("DSH token exchange failed (%s)", type(exc).__name__)
                    continue
                for cookie in jar:
                    if cookie.name.startswith("dsh-auth-"):
                        self._save_cookie((cookie.name, cookie.value))
                        log.info("DSH auth cookie refreshed via token exchange")
                        return (cookie.name, cookie.value)
        log.warning("No usable DSH startup URL; check the local Web log")
        return None

    def warmup_auth(self) -> None:
        """Fetch (or refresh) the DSH browser-session cookie at startup."""
        self._acquire_cookie()

    def _auth_headers(self) -> Dict[str, str]:
        cookie = self._load_cookie()
        if cookie is None:
            return {}
        return {"Cookie": f"{cookie[0]}={cookie[1]}"}

    # DSH >= 0.1.2 wire conventions:
    #   - endpoints are slash-separated (`session/prompt`, `session/page`)
    #   - the client-request payload is `{"args": {"<paramWire>": {...}}}`
    #     (`session/modelCatalog` and other no-arg endpoints use `{"args": {}}`).
    _ARG_KEYS = {
        "session/create": "request",
        "session/page": "request",
        "session/prompt": "request",
        "session/selectModel": "request",
        "session/list": "_request",
    }

    def call(
        self,
        method: str,
        payload: Optional[Dict[str, Any]] = None,
        timeout: int = 30,
        _retried_auth: bool = False,
    ) -> Any:
        arg_key = self._ARG_KEYS.get(method, "request")
        wire_payload = {"args": {arg_key: payload}} if payload is not None else {"args": {}}
        envelope = {
            "type": "client-request",
            "rpcId": str(uuid.uuid4()),
            "method": method,
            "payload": wire_payload,
        }
        data = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        headers.update(self._auth_headers())
        req = urlrequest.Request(
            f"{self.base_url}/api/{method}",
            data=data,
            headers=headers,
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
        except HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            if exc.code == 401 and not _retried_auth:
                if self._acquire_cookie() is not None:
                    return self.call(method, payload, timeout=timeout, _retried_auth=True)
            raise DshApiError("http", f"HTTP {exc.code}: {body[:500]}") from exc
        except URLError as exc:
            raise DshApiError("transport", str(exc)) from exc

        try:
            response = json.loads(raw)
        except Exception as exc:
            raise DshApiError("bad-response", raw[:500]) from exc

        if response.get("type") != "server-response":
            raise DshApiError("bad-response", f"unexpected envelope: {raw[:500]}")
        result = response.get("result") or {}
        if not result.get("ok"):
            error = result.get("error") or {}
            raise DshApiError(
                str(error.get("code") or "unknown"),
                str(error.get("message") or "unknown error"),
                error.get("details"),
            )
        return result.get("value")

    def _session_cursor(self, session_id: str) -> int:
        """Discover the session log's latest committed seq.

        `session/page` requires `throughSeq <= cursor`.  Probing with the
        largest safe integer makes DSH answer with `past cursor <N>` in the
        error message, which is the only v0.1.2 unary way to learn the cursor
        without subscribing to the `session/follow` stream.
        """
        try:
            self.call(
                "session/page",
                {
                    "address": {"kind": "session", "sessionId": session_id},
                    "throughSeq": 9007199254740991,
                    "maxMessages": 1,
                },
                timeout=20,
            )
            return -1
        except DshApiError as exc:
            match = re.search(r"past cursor (\d+)", exc.message)
            if match:
                return int(match.group(1))
            if exc.code in {"session/not-found", "session-not-found"}:
                raise
            # No cursor in the error means the probe itself failed for another
            # reason; surface it rather than guessing.
            raise

    @staticmethod
    def _normalize_page(value: Any) -> Dict[str, Any]:
        events = []
        for record in (value or {}).get("records") or []:
            if isinstance(record, dict) and record.get("type") == "event":
                event = record.get("event")
                if isinstance(event, dict):
                    events.append({"event": event})
        return {"events": events}

    def history(self, session_id: str, max_messages: int = 200) -> Dict[str, Any]:
        cursor = self._session_cursor(session_id)
        value = self.call(
            "session/page",
            {
                "address": {"kind": "session", "sessionId": session_id},
                "throughSeq": cursor,
                "maxMessages": max_messages,
            },
        )
        return self._normalize_page(value)

    def tail_seq(self, session_id: str) -> int:
        return self._session_cursor(session_id)

    def create_session(self, session_id: str) -> str:
        value = self.call(
            "session/create",
            {
                "workspaceId": DSH_WORKSPACE_ID,
                "sessionId": session_id,
            },
        )
        return str(value.get("sessionId") or session_id)

    def ensure_session(self, chat_id: str) -> Tuple[str, bool]:
        """Return (session_id, created). Reuse the persisted DSH session when possible."""
        state = _load_state()
        entry = state.get("chats", {}).get(chat_id) or {}
        existing = entry.get("session_id")
        if existing:
            try:
                self.history(existing, max_messages=1)
                return existing, False
            except DshApiError as exc:
                if exc.code not in {"session/not-found", "session-not-found", "internal"}:
                    raise
                log.warning("Persisted DSH session %s is gone; recreating", existing)

        slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", chat_id)[:60]
        session_id = f"feishu-{slug}-{int(time.time())}"

        try:
            self.history(session_id, max_messages=1)
            created = False
        except DshApiError:
            try:
                session_id = self.create_session(session_id)
                created = True
            except DshApiError:
                # Race with another bridge process or a session created between
                # probe and create; verify then continue.
                self.history(session_id, max_messages=1)
                created = False

        def save(current: Dict[str, Any]) -> None:
            current.setdefault("chats", {})[chat_id] = {
                "session_id": session_id,
                "created_at": entry.get("created_at") or time.time(),
            }
            current.setdefault("memory_seeded", {})

        _atomic_state_update(save)
        return session_id, created

    def prompt(
        self,
        session_id: str,
        text: str,
        images: Optional[list] = None,
    ) -> None:
        """Dispatch a turn. `images` is a list of dicts with keys
        media_type (image/jpeg|png|webp|gif), data (base64 string) and an
        optional name."""
        content: list = [{"type": "text", "text": text}]
        for img in images or []:
            content.append(
                {
                    "type": "image",
                    "mediaType": img["media_type"],
                    "data": img["data"],
                    "name": img.get("name", "image"),
                }
            )
        self.call(
            "session/prompt",
            {
                "requestId": str(uuid.uuid4()),
                "sessionId": session_id,
                "mode": "queue",
                "content": content,
            },
            timeout=20,
        )

    def select_model(
        self,
        session_id: str,
        provider: str,
        model: str,
        reasoning_effort: Optional[str] = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "sessionId": session_id,
            "provider": provider,
            "model": model,
        }
        if reasoning_effort:
            payload["reasoningEffort"] = reasoning_effort
        self.call("session/selectModel", payload, timeout=20)

    def current_model(self, session_id: str) -> str:
        """Return the deployment default model id from `session/modelCatalog`."""
        try:
            value = self.call("session/modelCatalog", None, timeout=20)
            current = value.get("default") or {}
            return str(current.get("model") or "unknown")
        except Exception:
            return "unknown"

    def wait_for_turn_end(
        self,
        session_id: str,
        start_seq: int,
        timeout: float,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """Poll history until the turn that follows start_seq ends.

        Returns (final_text, turn_end_data). The final_text is the last
        non-empty assistant text emitted in that turn, matching the semantics
        of dsh's own headless runner.
        """
        deadline = time.monotonic() + timeout
        last_text = ""
        last_progress = ""
        while True:
            if time.monotonic() > deadline:
                raise TimeoutError(f"DSH turn did not finish within {int(timeout)}s")

            # Small tail window is enough: turn/end and the final assistant
            # message are always at the newest end of the history, so we don't
            # need to pull 400 entries (which grows with the session and slows
            # every poll). 80 covers a full turn's tail cheaply.
            hist = self.history(session_id, max_messages=80)
            events = hist.get("events") or []
            for entry in events:
                event = entry.get("event") or {}
                seq = int(event.get("seq", 0))
                if seq <= start_seq:
                    continue
                etype = event.get("type")
                data = event.get("data") or {}

                if etype == "assistant/message":
                    message = data.get("message") or {}
                    blocks = message.get("content") or []
                    text = "".join(
                        block.get("text", "")
                        for block in blocks
                        if isinstance(block, dict) and block.get("type") == "text"
                    )
                    if text:
                        last_text = text

                if etype == "turn/end":
                    if on_progress and last_text and last_text != last_progress:
                        try:
                            on_progress(last_text)
                        except Exception:
                            log.exception("on_progress callback failed")
                    return last_text, data

            if on_progress and last_text and last_text != last_progress:
                last_progress = last_text
                try:
                    on_progress(last_text)
                except Exception:
                    log.exception("on_progress callback failed")

            time.sleep(2.0)


# ---------------------------------------------------------------------------
# Feishu sender
# ---------------------------------------------------------------------------

class FeishuSendError(RuntimeError):
    pass


# DSH image admission limits (see dsh-client-connection: maxImageDimension=2000,
# maxImagePixels=4e7). Downscale anything larger so the turn is admitted.
_DSH_MAX_DIMENSION = 2000
_DSH_MAX_PIXELS = 40_000_000


def _downscale_image(raw: bytes, content_type: str) -> Tuple[bytes, str]:
    """Downscale image bytes to DSH's admitted pixel/dimension limits.

    Returns (new_raw, new_content_type). Keeps the original format when the
    image is already within limits; otherwise re-encodes as JPEG.
    """
    try:
        from PIL import Image
        import io
    except Exception:
        return raw, content_type

    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except Exception:
        return raw, content_type

    width, height = img.size
    if width <= 0 or height <= 0:
        return raw, content_type

    if width <= _DSH_MAX_DIMENSION and height <= _DSH_MAX_DIMENSION and (
        width * height <= _DSH_MAX_PIXELS
    ):
        return raw, content_type

    scale = min(_DSH_MAX_DIMENSION / width, _DSH_MAX_DIMENSION / height)
    if width * height * scale * scale > _DSH_MAX_PIXELS:
        scale = min(scale, (_DSH_MAX_PIXELS / (width * height)) ** 0.5)
    new_size = (max(1, int(width * scale)), max(1, int(height * scale)))
    img = img.convert("RGB")
    img = img.resize(new_size, Image.LANCZOS)

    out = io.BytesIO()
    img.save(out, format="JPEG", quality=88)
    return out.getvalue(), "image/jpeg"


# 飞书卡片 header 允许的色板（与 OpenClaw @openclaw/feishu 的
# FEISHU_CARD_TEMPLATES 完全一致，共 13 色）。
# 颜色表达**卡片意图**，不是内容好看与否 —— 参考 OpenClaw 的用法：
#   orange = 需要用户确认/审批   indigo = 快捷操作菜单   blue = 普通消息
FEISHU_CARD_TEMPLATES = frozenset({
    "blue", "green", "red", "orange", "purple", "indigo", "wathet",
    "turquoise", "yellow", "grey", "carmine", "violet", "lime",
})

# 从标题推断意图色。显式传入的 template 优先级更高。
_TEMPLATE_HINTS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("❌", "失败", "错误", "超时", "异常", "崩溃", "中断"), "red"),
    (("⚠", "警告", "注意", "确认", "待办", "阻塞"), "orange"),
    (("✅", "完成", "成功", "已发送", "通过"), "green"),
    (("📋", "菜单", "选项", "操作", "命令"), "indigo"),
    (("⏳", "处理中", "进行中", "正在"), "wathet"),
)


# ---------------------------------------------------------------------------
# 卡片标题：用回复内容概览，而不是固定的 "✅ DeepSeek"
# ---------------------------------------------------------------------------
#
# 用户反馈「每次都只显示 deepseek」→ 标题取回复第一行有内容的文本：
#   跳过围栏代码块 / 装饰线 / 空行 → 去 markdown 装饰与零宽字符
#   → 压成一行 → 按句子边界截断到 CARD_TITLE_MAX → 加前缀。
# 可调：FEISHU_CARD_TITLE_PREFIX（默认 "🤖 "）、FEISHU_CARD_TITLE_MAX（默认 36）、
#       FEISHU_CARD_TITLE_STYLE=model（回退成固定 "✅ DeepSeek"）。

CARD_TITLE_PREFIX = os.getenv("FEISHU_CARD_TITLE_PREFIX", "\U0001f916 ")
CARD_TITLE_MAX = int(os.getenv("FEISHU_CARD_TITLE_MAX", "36") or "36")
CARD_TITLE_STYLE = (os.getenv("FEISHU_CARD_TITLE_STYLE", "content") or "content").strip().lower()

_MD_DECOR_RE = re.compile(r"\*\*|__|`+|~~")
_MD_HEAD_RE = re.compile(r"^\s*(#{1,6}|>+|[-*+]|\d+\.)\s+")
_ZW_RE = re.compile("[\u200b-\u200f\u202a-\u202e\u2060\ufeff]")
_CTRL_RE = re.compile("[\x00-\x08\x0b-\x1f\x7f]")
_LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_FENCE_ANY_RE = re.compile(r"^\s*(```|~~~)")
_DECOR_LINE_RE = re.compile(r"[-=_*~#\s|]{3,}")


def _reply_card_title(text: str) -> str:
    """从回复正文提炼一行卡片标题（失败一律回退到固定标题）。"""
    try:
        if CARD_TITLE_STYLE == "model":
            return "✅ DeepSeek"
        body = _LINK_RE.sub(r"\1", text or "")
        in_fence = False
        for raw in body.splitlines():
            line = raw.strip()
            if _FENCE_ANY_RE.match(line):
                in_fence = not in_fence
                continue
            if in_fence or not line:
                continue
            if _DECOR_LINE_RE.fullmatch(line):
                continue
            line = _CTRL_RE.sub("", _ZW_RE.sub("", line))
            line = _MD_DECOR_RE.sub("", _MD_HEAD_RE.sub("", line)).strip()
            if not line:
                continue
            line = re.sub(r"\s+", " ", line)
            if len(line) > CARD_TITLE_MAX:
                limit = CARD_TITLE_MAX
                cut = -1
                for ch in "。！？；!?;":
                    pos = line.rfind(ch, 0, limit)
                    if pos >= limit // 2:
                        cut = max(cut, pos)
                if cut < 0:
                    for ch in "，,、 ":
                        pos = line.rfind(ch, 0, limit)
                        if pos >= limit // 2:
                            cut = max(cut, pos)
                line = (line[:cut] if cut > 0 else line[:limit]).rstrip("，,、。.;；:：") + "…"
            return (CARD_TITLE_PREFIX + line)[:120]
    except Exception:
        log.debug("卡片标题生成失败，回退固定标题", exc_info=True)
    return "✅ DeepSeek"


def pick_card_template(title: str, explicit: Optional[str] = None) -> str:
    """选卡片 header 颜色：显式值 > 按标题语义推断 > blue。

    无效或未知的显式值会被忽略而不是报错 —— 发消息不该因为颜色名写错而失败。
    """
    if explicit:
        name = str(explicit).strip().lower()
        if name in FEISHU_CARD_TEMPLATES:
            return name
    t = title or ""
    for keys, name in _TEMPLATE_HINTS:
        if any(k in t for k in keys):
            return name
    return "blue"


class FeishuSender:
    def __init__(self, app_id: str, app_secret: str) -> None:
        import lark_oapi as lark
        from lark_oapi.api.im.v1 import (
            CreateFileRequest,
            CreateFileRequestBody,
            CreateMessageRequest,
            CreateMessageRequestBody,
            ReplyMessageRequest,
            ReplyMessageRequestBody,
        )

        self._lark = lark
        self._CreateFileRequest = CreateFileRequest
        self._CreateFileRequestBody = CreateFileRequestBody
        self._CreateMessageRequest = CreateMessageRequest
        self._CreateMessageRequestBody = CreateMessageRequestBody
        self._ReplyMessageRequest = ReplyMessageRequest
        self._ReplyMessageRequestBody = ReplyMessageRequestBody
        self._client = (
            lark.Client.builder()
            .app_id(app_id)
            .app_secret(app_secret)
            .domain("https://open.feishu.cn")
            .log_level(lark.LogLevel.WARNING)
            .build()
        )
        self._app_id = app_id
        self._app_secret = app_secret
        self._tenant_token: Optional[Tuple[str, float]] = None

    def _tenant_access_token(self) -> str:
        """Fetch (and cache) the app tenant access token for resource download."""
        now = time.monotonic()
        if self._tenant_token and self._tenant_token[1] > now:
            return self._tenant_token[0]
        body = json.dumps(
            {"app_id": self._app_id, "app_secret": self._app_secret}
        ).encode("utf-8")
        req = urlrequest.Request(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlrequest.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        token = str(data.get("tenant_access_token") or "")
        if not token:
            raise FeishuSendError("tenant_access_token fetch failed")
        # Tokens last ~2h; cache for 100 minutes.
        self._tenant_token = (token, now + 6000)
        return token

    def download_image(self, message_id: str, file_key: str) -> Tuple[bytes, str]:
        """Download a Feishu image message resource.

        Returns (bytes, content_type). Raises FeishuSendError on failure.
        """
        token = self._tenant_access_token()
        url = (
            "https://open.feishu.cn/open-apis/im/v1/messages/"
            f"{message_id}/resources/{file_key}?type=image"
        )
        req = urlrequest.Request(
            url,
            headers={"Authorization": f"Bearer {token}"},
            method="GET",
        )
        with urlrequest.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            content_type = str(resp.headers.get("Content-Type") or "image/jpeg")
        return raw, content_type

    @staticmethod
    def _media_type(content_type: str) -> str:
        ct = (content_type or "").lower().split(";")[0].strip()
        if ct in ("image/png", "image/webp", "image/gif"):
            return ct
        return "image/jpeg"
    @staticmethod
    def _card(title: str, markdown: str, note: Optional[str] = None,
              template: Optional[str] = None,
              table_mode: Optional[str] = None) -> Dict[str, Any]:
        # 飞书 Card JSON 2.0 的 markdown 元素原生支持表格和代码块；
        # 旧版（v1）卡片不支持表格，只能显示成一堆竖线 —— 所以这里用 2.0。
        #
        # 三个对齐 OpenClaw 的细节（2026-09-17 学过来的）：
        #  ① config 里必须有 width_mode="fill"，否则卡片不是全宽、观感差一截。
        #     本项目还要 update_multi=True —— update_card() 走 PATCH，依赖它。
        #  ② header.template 按语义选色（见 pick_card_template），不再一律 blue。
        #  ③ note 前加一条 hr 分隔线。OpenClaw 是 `hr` + 灰色 markdown；
        #     2.0 没有 v1 的 note 元素，用 text_size=notation 出小字。
        elements: list = [
            {"tag": "markdown", "content": render_markdown_tables(markdown, table_mode)}
        ]
        if note:
            elements.append({"tag": "hr"})
            elements.append({
                "tag": "markdown",
                "content": f"<font color='grey'>{note}</font>",
                "text_size": "notation",
            })
        return {
            "schema": "2.0",
            "config": {"update_multi": True, "width_mode": "fill"},
            "header": {
                "template": pick_card_template(title, template),
                "title": {"tag": "plain_text", "content": title[:200]},
            },
            "body": {"elements": elements},
        }

    @staticmethod
    def _chunks(text: str, size: int = 5500) -> list:
        if len(text) <= size:
            return [text]
        chunks = []
        while text:
            if len(text) <= size:
                chunks.append(text)
                break
            cut = text.rfind("\n", 0, size)
            if cut < size // 2:
                cut = size
            chunks.append(text[:cut])
            text = text[cut:].lstrip("\n")
        return chunks

    @staticmethod
    def _split_blocks(text: str) -> list:
        """把 markdown 切成块：**整张表格算一块**，其余按空行分段。

        必须整表成块，否则按字符切会把一张表劈成两半，两边都渲染不出表格。
        """
        lines = text.split("\n")
        blocks, cur, i, in_fence = [], [], 0, False
        while i < len(lines):
            line = lines[i]
            if _FENCE_RE.match(line):
                in_fence = not in_fence
                cur.append(line)
                i += 1
                continue
            if (not in_fence and "|" in line
                    and i + 1 < len(lines) and _TABLE_SEP_RE.match(lines[i + 1])):
                tbl = [line]
                i += 1
                while i < len(lines) and lines[i].strip() and "|" in lines[i]:
                    tbl.append(lines[i])
                    i += 1
                if cur:
                    blocks.append("\n".join(cur))
                    cur = []
                blocks.append("\n".join(tbl))
                continue
            if not in_fence and not line.strip():
                if cur:
                    blocks.append("\n".join(cur))
                    cur = []
                i += 1
                continue
            cur.append(line)
            i += 1
        if cur:
            blocks.append("\n".join(cur))
        return blocks

    # ---- 附件标记：[[FILE: 绝对路径 | 显示名]] ----
    # 让 agent 能把生成的产物直接发给用户。此前 send_file() 存在但无人调用。
    FILE_MARKER_RE = re.compile(
        r"\[\[\s*FILE\s*:\s*([^\]|]+?)\s*(?:\|\s*([^\]]+?)\s*)?\]\]",
        re.IGNORECASE,
    )
    MAX_FILES_PER_REPLY = 3
    MAX_FILE_BYTES = 30 * 1024 * 1024      # 飞书文件上限附近留余量
    FILE_ALLOW_ROOTS = tuple(os.getenv("DSH_FEISHU_FILE_ROOTS", f"{Path.home()}/,/tmp/").split(","))

    @classmethod
    def _extract_file_markers(cls, text: str) -> tuple[str, list[tuple[str, str]]]:
        """抽出 [[FILE: 路径|显示名]] 标记 -> (去掉标记的正文, [(路径, 显示名)])。

        保守策略：只认绝对路径、只认真实存在的普通文件、限制条数与大小、
        限制在白名单根目录下；任何不合规的标记都只从正文里删掉、不发文件。
        """
        found: list[tuple[str, str]] = []
        for m in cls.FILE_MARKER_RE.finditer(text):
            raw_path = (m.group(1) or "").strip().strip('`"\'')
            name = (m.group(2) or "").strip() or None
            path = Path(raw_path)
            if not path.is_absolute():
                log.warning("附件标记被拒（非绝对路径）：%s", raw_path)
                continue
            if not path.is_file():
                log.warning("附件标记被拒（文件不存在）：%s", raw_path)
                continue
            if not str(path).startswith(cls.FILE_ALLOW_ROOTS):
                log.warning("附件标记被拒（不在白名单根目录）：%s", raw_path)
                continue
            try:
                size = path.stat().st_size
            except OSError:
                continue
            if size <= 0 or size > cls.MAX_FILE_BYTES:
                log.warning("附件标记被拒（大小 %d 不合规）：%s", size, raw_path)
                continue
            found.append((str(path), name or path.name))
        clean = cls.FILE_MARKER_RE.sub("", text).strip()
        clean = re.sub(r"\n{3,}", "\n\n", clean)
        return clean, found[: cls.MAX_FILES_PER_REPLY]

    @staticmethod
    def _chunks_by_tables(text: str, max_tables: int = None,
                          max_size: int = 5500) -> list:
        """按「每段 ≤ max_tables 张原生表格 且 ≤ max_size 字符」切分。

        这样每张卡都在飞书的表格上限之内，**正文可以继续用原生表格**，
        不必退化成等宽代码块（观感差）。超出部分作为后续卡片发送。
        """
        if max_tables is None:
            max_tables = TABLE_MAX_NATIVE
        blocks = FeishuSender._split_blocks(text)
        chunks, cur, ntab, size = [], [], 0, 0
        for b in blocks:
            bt = _count_md_tables(b)
            bl = len(b) + 2
            if cur and (ntab + bt > max_tables or size + bl > max_size):
                chunks.append("\n\n".join(cur))
                cur, ntab, size = [], 0, 0
            cur.append(b)
            ntab += bt
            size += bl
        if cur:
            chunks.append("\n\n".join(cur))
        # 兜底：单个块自身就超过 max_size（例如一大段无空行的长文）时按字符再切，
        # 否则会生成一张超出飞书长度上限的卡片。
        out = []
        for c in (chunks or [text]):
            if len(c) <= max_size:
                out.append(c)
            else:
                out.extend(FeishuSender._chunks(c, max_size))
        return out or [text]

    def send_card(
        self,
        chat_id: str,
        title: str,
        markdown: str,
        reply_message_id: Optional[str] = None,
        note: Optional[str] = None,
        template: Optional[str] = None,
    ) -> None:
        # 按【表格数量】切分而不是按字符数：每张卡都在飞书表格上限内，
        # 正文就能继续用原生表格（观感好），代价只是拆成多条卡片。
        chunks = self._chunks_by_tables(markdown)
        for index, chunk in enumerate(chunks, 1):
            card_title = title if len(chunks) == 1 else f"{title} ({index}/{len(chunks)})"
            chunk_note = note if index == len(chunks) else None
            # 默认表格模式；若因原生表格超限被拒，改用代码块重渲染一次（11310）
            try:
                content = json.dumps(self._card(card_title, chunk, chunk_note, template),
                                     ensure_ascii=False)
                self._send(chat_id, content, reply_message_id if index == 1 else None)
            except FeishuSendError as exc:
                if not _is_table_limit_error(str(exc)):
                    raise
                log.warning("send_card 原生表格超限，改用代码块重试: %s", exc)
                content = json.dumps(
                    self._card(card_title, chunk, chunk_note, template, "code"),
                    ensure_ascii=False)
                self._send(chat_id, content, reply_message_id if index == 1 else None)

    def send_card_return_id(
        self,
        chat_id: str,
        title: str,
        markdown: str,
    ) -> Optional[str]:
        """Send one interactive card and return its message_id (for later
        PATCH updates). Returns None on failure."""
        content = json.dumps(self._card(title, markdown), ensure_ascii=False)
        body = (
            self._CreateMessageRequestBody.builder()
            .receive_id(chat_id)
            .msg_type("interactive")
            .content(content)
            .uuid(str(uuid.uuid4()))
            .build()
        )
        receive_id_type = "chat_id"
        if chat_id.startswith("ou_"):
            receive_id_type = "open_id"
        elif chat_id.startswith("on_"):
            receive_id_type = "union_id"
        req = (
            self._CreateMessageRequest.builder()
            .receive_id_type(receive_id_type)
            .request_body(body)
            .build()
        )
        try:
            response = self._client.im.v1.message.create(req)
            if not bool(getattr(response, "success", lambda: False)()):
                log.warning("send_card_return_id create failed: %s",
                            getattr(response, "msg", "?"))
                return None
            data = getattr(response, "data", None)
            mid = getattr(data, "message_id", None) if data else None
            return str(mid) if mid else None
        except Exception as exc:
            log.exception("send_card_return_id crashed: %s", exc)
            return None

    def update_card(
        self, message_id: str, title: str, markdown: str, note: Optional[str] = None,
        template: Optional[str] = None,
    ) -> bool:
        """PATCH an existing interactive card in place (streaming update)."""
        try:
            from lark_oapi.api.im.v1 import PatchMessageRequest, PatchMessageRequestBody
        except Exception:
            return False
        # 第一次用默认表格模式；若飞书以「表格数量超限」拒绝，改用代码块重试一次。
        # 错误码 11310 = card table number over limit。
        for attempt, tmode in enumerate((None, "code")):
            content = json.dumps(self._card(title, markdown, note, template, tmode),
                                 ensure_ascii=False)
            try:
                body = PatchMessageRequestBody.builder().content(content).build()
                req = (
                    PatchMessageRequest.builder()
                    .message_id(message_id)
                    .request_body(body)
                    .build()
                )
                response = self._client.im.v1.message.update(req)
            except Exception as exc:
                log.exception("update_card crashed for %s: %s", message_id, exc)
                return False
            if bool(getattr(response, "success", lambda: False)()):
                if attempt:
                    log.info("update_card 用代码块模式重试成功 %s", message_id)
                return True
            msg = str(getattr(response, "msg", "?"))
            if attempt == 0 and _is_table_limit_error(msg):
                log.warning("update_card 原生表格超限，改用代码块重试 %s: %s",
                            message_id, msg)
                continue
            log.warning("update_card failed for %s: %s", message_id, msg)
            return False
        return False

    def send_file(
        self,
        chat_id: str,
        file_path: str,
        file_name: Optional[str] = None,
        reply_message_id: Optional[str] = None,
    ) -> None:
        """Upload a local file and deliver it as a Feishu file message."""
        path = Path(file_path)
        if not path.is_file():
            raise FeishuSendError(f"file not found: {file_path}")

        display_name = file_name or path.name
        suffix = path.suffix.lower().lstrip(".")
        file_type_map = {
            "pdf": "pdf",
            "doc": "doc",
            "docx": "doc",
            "xls": "xls",
            "xlsx": "xls",
            "ppt": "ppt",
            "pptx": "ppt",
            "csv": "stream",
            "txt": "stream",
            "json": "stream",
            "md": "stream",
        }
        file_type = file_type_map.get(suffix, "stream")

        with open(path, "rb") as file_obj:
            upload_body = (
                self._CreateFileRequestBody.builder()
                .file_type(file_type)
                .file_name(display_name)
                .file(file_obj)
                .build()
            )
            upload_req = self._CreateFileRequest.builder().request_body(upload_body).build()
            upload_resp = self._client.im.v1.file.create(upload_req)

        if not bool(getattr(upload_resp, "success", lambda: False)()):
            raise FeishuSendError(
                f"file upload failed: [{getattr(upload_resp, 'code', '?')}] {getattr(upload_resp, 'msg', '')}"
            )
        file_key = getattr(getattr(upload_resp, "data", None), "file_key", None)
        if not file_key:
            raise FeishuSendError("file upload succeeded but returned no file_key")

        file_content = json.dumps({"file_key": file_key}, ensure_ascii=False)
        body = (
            self._CreateMessageRequestBody.builder()
            .receive_id(chat_id)
            .msg_type("file")
            .content(file_content)
            .uuid(str(uuid.uuid4()))
            .build()
        )
        receive_id_type = "chat_id" if chat_id.startswith("oc_") else "open_id"
        req = (
            self._CreateMessageRequest.builder()
            .receive_id_type(receive_id_type)
            .request_body(body)
            .build()
        )
        resp = self._client.im.v1.message.create(req)
        if not bool(getattr(resp, "success", lambda: False)()):
            raise FeishuSendError(
                f"file message failed: [{getattr(resp, 'code', '?')}] {getattr(resp, 'msg', '')}"
            )
        log.info("Sent file %s to %s", display_name, chat_id)

    def _send(self, chat_id: str, content: str, reply_message_id: Optional[str]) -> None:
        last_error: Optional[Exception] = None
        for attempt in range(3):
            response = None
            try:
                if reply_message_id:
                    body = (
                        self._ReplyMessageRequestBody.builder()
                        .msg_type("interactive")
                        .content(content)
                        .build()
                    )
                    req = (
                        self._ReplyMessageRequest.builder()
                        .message_id(reply_message_id)
                        .request_body(body)
                        .build()
                    )
                    response = self._client.im.v1.message.reply(req)
                    if bool(getattr(response, "success", lambda: False)()):
                        return
                    log.warning(
                        "Feishu reply to %s failed ([%s] %s); falling back to new card",
                        reply_message_id,
                        getattr(response, "code", "?"),
                        getattr(response, "msg", "reply failed"),
                    )

                if response is None or reply_message_id is None:
                    body = (
                        self._CreateMessageRequestBody.builder()
                        .receive_id(chat_id)
                        .msg_type("interactive")
                        .content(content)
                        .uuid(str(uuid.uuid4()))
                        .build()
                    )
                    receive_id_type = "chat_id"
                    if chat_id.startswith("ou_"):
                        receive_id_type = "open_id"
                    elif chat_id.startswith("oc_"):
                        receive_id_type = "chat_id"
                    elif chat_id.startswith("on_"):
                        receive_id_type = "union_id"
                    req = (
                        self._CreateMessageRequest.builder()
                        .receive_id_type(receive_id_type)
                        .request_body(body)
                        .build()
                    )
                    response = self._client.im.v1.message.create(req)
                    success = bool(
                        getattr(response, "success", lambda: False)()
                    )
                    if success:
                        return
                    raise FeishuSendError(
                        f"[{getattr(response, 'code', '?')}] {getattr(response, 'msg', 'create failed')}"
                    )
            except FeishuSendError as exc:
                last_error = exc
                log.warning("Feishu send attempt %d failed: %s", attempt + 1, exc)
                time.sleep(1 + attempt)
            except Exception as exc:
                last_error = exc
                log.exception("Feishu send attempt %d crashed", attempt + 1)
                time.sleep(1 + attempt)
        raise FeishuSendError(str(last_error) if last_error else "send failed")


# ---------------------------------------------------------------------------
# Bridge orchestration
# ---------------------------------------------------------------------------

class Bridge:
    def __init__(self) -> None:
        self.dsh = DshApi()
        env = {**_load_hermes_env(), **os.environ}
        self.app_id = env.get("FEISHU_APP_ID", "").strip()
        self.app_secret = env.get("FEISHU_APP_SECRET", "").strip()
        if not self.app_id or not self.app_secret:
            raise RuntimeError("FEISHU_APP_ID/FEISHU_APP_SECRET missing from environment or bridge .env")

        self.allowed_chat_ids = {
            item.strip()
            for item in os.getenv("FEISHU_ALLOWED_CHAT_IDS", DEFAULT_ALLOWED_CHAT_IDS).split(",")
            if item.strip()
        }
        self.allowed_senders = {
            item.strip()
            for item in os.getenv("FEISHU_ALLOWED_SENDER_OPEN_IDS", DEFAULT_ALLOWED_SENDERS).split(",")
            if item.strip()
        }
        self.executor = ThreadPoolExecutor(
            max_workers=4,
            thread_name_prefix="dsh-feishu-worker",
        )
        self.chat_locks: Dict[str, threading.Lock] = {}
        self.chat_locks_guard = threading.Lock()
        self.feishu = FeishuSender(self.app_id, self.app_secret)
        self.codex = CodexChannel(self.feishu, self.allowed_chat_ids)
        self.codex_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="codex-control")
        self.approval_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="approval-control")
        self.codex.start()
        self.seed_memory_prompt = self._build_seed_memory_prompt()
        log.info("Bridge configured: chats=%s senders=%s", sorted(self.allowed_chat_ids), sorted(self.allowed_senders))

    def _approval_watcher(self) -> None:
        """后台线程：把新出现的审批请求发到飞书。"""
        pipe = _approval_pipe_dir()
        pending_dir = pipe / "pending"
        log.info("审批监听已启动: %s", pending_dir)
        while True:
            try:
                if pending_dir.is_dir():
                    for path in sorted(pending_dir.glob("*.json")):
                        try:
                            req = _read_json(path)
                            if not isinstance(req, dict) or not req.get("id"):
                                continue
                            if _approval_expired(req, path):
                                continue
                            if (pipe / "decisions" / (str(req["id"]) + ".json")).exists():
                                continue
                            self._notify_approval(req)
                        except Exception:
                            log.exception("处理审批请求失败: %s", path)
            except Exception:
                log.exception("审批监听循环异常")
            time.sleep(APPROVAL_POLL_SECONDS)

    def _notify_approval(self, req: Dict[str, Any]) -> None:
        request_id = str(req["id"])
        pipe = _approval_pipe_dir()
        notice = pipe / "notified" / (request_id + ".json")
        if notice.exists():
            return
        _, detail = _describe_approval(req)
        session_id = str(req.get("sessionId") or "")
        chat_ids = []
        chat_id = _read_state_chat_map().get(session_id)
        if chat_id:
            chat_ids.append(chat_id)
        chat_ids = [cid for cid in chat_ids if cid in self.allowed_chat_ids]
        if not chat_ids:
            log.warning("审批请求没有允许的归属聊天 id=%s", request_id)
            return
        markdown = _NL.join([
            "**🔐 有一个操作在等你批准**",
            "",
            detail,
            "",
            "**怎么回**",
            "· 批准: 回复 " + "`批准 " + request_id + "`",
            "· 拒绝: 回复 " + "`拒绝 " + request_id + "`",
            "（超时不回 = 自动拒绝，不会默认放行）",
        ])
        for cid in chat_ids:
            try:
                self.feishu.send_card(cid, "🔐 待批准 " + request_id, markdown)
                notice.parent.mkdir(parents=True, exist_ok=True)
                notice.write_text(json.dumps({"at": time.time()}), encoding="utf-8")
                log.info("已发出审批卡片 id=%s chat=%s", request_id, cid)
            except Exception:
                log.exception("发审批卡片失败 id=%s chat=%s", request_id, cid)

    def _run_approval_command(self, chat_id: str, message_id: str, text: str) -> None:
        request_id, outcome = _parse_approval_command(text)
        if not request_id:
            return
        pipe = _approval_pipe_dir()
        word = "批准" if outcome == "allowed-once" else "拒绝"
        pending_file = pipe / "pending" / (request_id + ".json")
        req = _read_json(pending_file) or {}
        if not req or _approval_expired(req, pending_file) or (pipe / "decisions" / (request_id + ".json")).exists():
            self.feishu.send_card(
                chat_id,
                "⚠️ 找不到这个审批",
                "编号 `" + request_id + "` 没有待处理记录。" + _NL
                + "可能已被答复、已超时，或桥重启过。当前待处理 "
                + str(len(list((pipe / "pending").glob("*.json")))) + " 条。",
                reply_message_id=message_id,
            )
            return
        owner = _read_state_chat_map().get(str(req.get("sessionId") or ""))
        if owner != chat_id or chat_id not in self.allowed_chat_ids:
            self.feishu.send_card(
                chat_id, "⚠️ 审批归属不符", "这个审批不属于当前聊天，未写入裁决。",
                reply_message_id=message_id,
            )
            return
        ok = _write_decision(pipe, request_id, outcome, "feishu:" + chat_id)
        if not ok:
            self._write_decision_failed_card(chat_id, message_id, request_id)
            return
        tool = str(req.get("toolName") or "该操作")
        self.feishu.send_card(
            chat_id,
            "✅ 已" + word + " " + request_id,
            "**" + word + "** 已回执给宿主（工具: " + tool + "）。" + _NL
            + "如果这个操作本身还在跑，下一步输出会接着来。",
            reply_message_id=message_id,
        )
        log.info("审批已答复 id=%s outcome=%s", request_id, outcome)

    def _send_approval_hint(self, chat_id: str, message_id: str, keyword: str) -> None:
        """只写了「批准/拒绝」没带编号时给个格式提示。"""
        pipe = _approval_pipe_dir()
        rows = []
        for path in sorted((pipe / "pending").glob("*.json")):
            req = _read_json(path)
            if (isinstance(req, dict) and req.get("id") and not _approval_expired(req, path)
                    and _read_state_chat_map().get(str(req.get("sessionId") or "")) == chat_id
                    and not (pipe / "decisions" / (str(req["id"]) + ".json")).exists()):
                rows.append("`" + str(req["id"]) + "` " + str(req.get("toolName") or ""))
        markdown = _NL.join([
            "**要带编号我才能对上号**",
            "",
            "格式: " + "`" + keyword + " 编号`",
            "待处理: " + ("、".join(rows) if rows else "（当前没有待批准的请求）"),
        ])
        self.feishu.send_card(chat_id, "ℹ️ 缺审批编号", markdown, reply_message_id=message_id)

    def _write_decision_failed_card(self, chat_id: str, message_id: str, request_id: str) -> None:
        try:
            self.feishu.send_card(
                chat_id,
                "⚠️ 回执写入失败",
                "没能把裁决写入审批管道，编号 `" + request_id + "`。" + _NL
                + "该请求会按超时处理（自动拒绝）。详情见 logs/bridge.log。",
                reply_message_id=message_id,
            )
        except Exception:
            log.exception("发回执失败卡片失败")

    def _build_seed_memory_prompt(self) -> str:
        user_md = _read_file(HERMES_USER_MD) or "(empty)"
        memory_md = _read_file(HERMES_MEMORY_MD) or "(empty)"
        return (
            "[系统记忆初始化 · 仅确认一次]\n"
            "你是 DeepSeek Harness 飞书桥接助手，已接替本机 Hermes 的飞书渠道和记忆。\n"
            "以下内容长期有效，后续所有飞书任务都要遵守，不要向用户复述这段初始化。\n\n"
            "=== 用户档案（继承自 Hermes USER.md）===\n"
            f"{user_md}\n\n"
            "=== 长期记忆（继承自 Hermes MEMORY.md）===\n"
            f"{memory_md}\n\n"
            "=== 飞书回复规范 ===\n"
            "- 回复默认使用飞书静态卡片（无按钮），不要假设桥接层会帮你排版。\n"
            "- 不要在飞书使用 markdown 表格；结构化信息用 emoji + 粗体标题 + 换行缩进，"
            "项目名: 值 分行排列。\n"
            "- 结论优先：调研/研究/分析类任务必须给出综合结论、推荐或方案，"
            "不能只丢链接、标题或原始数据。\n"
            "- 语言中文，时区 Asia/Shanghai。\n\n"
            "请只回复：记忆已加载"
        )

    def _chat_lock(self, chat_id: str) -> threading.Lock:
        with self.chat_locks_guard:
            lock = self.chat_locks.get(chat_id)
            if lock is None:
                lock = threading.Lock()
                self.chat_locks[chat_id] = lock
            return lock

    def start_heartbeat(self) -> None:
        threading.Thread(target=_heartbeat_loop, name="dsh-feishu-heartbeat", daemon=True).start()

    def start_feishu(self) -> None:
        import lark_oapi as lark
        from lark_oapi.event.callback.model.p2_card_action_trigger import (
            P2CardActionTriggerResponse,
        )
        from lark_oapi.event.dispatcher_handler import EventDispatcherHandler
        from lark_oapi.ws import Client as FeishuWSClient

        def _ignore(_data: Any) -> None:
            return None

        event_handler = (
            EventDispatcherHandler.builder("", "")
            .register_p2_im_message_receive_v1(self.on_feishu_message)
            .register_p2_im_message_message_read_v1(_ignore)
            .register_p2_im_message_reaction_created_v1(_ignore)
            .register_p2_im_message_reaction_deleted_v1(_ignore)
            .register_p2_im_message_recalled_v1(_ignore)
            .register_p2_im_chat_member_bot_added_v1(_ignore)
            .register_p2_im_chat_member_bot_deleted_v1(_ignore)
            .register_p2_im_chat_access_event_bot_p2p_chat_entered_v1(_ignore)
            .register_p2_card_action_trigger(lambda _data: P2CardActionTriggerResponse())
            .build()
        )
        client = FeishuWSClient(
            app_id=self.app_id,
            app_secret=self.app_secret,
            log_level=lark.LogLevel.INFO,
            event_handler=event_handler,
            domain="https://open.feishu.cn",
            auto_reconnect=True,
        )
        threading.Thread(
            target=self._approval_watcher, name="approval-watcher", daemon=True
        ).start()
        log.info("Starting Feishu websocket client (app_id=%s...)", self.app_id[:6])
        client.start()

    def on_feishu_message(self, data: Any) -> None:
        """Lark SDK callback. Runs on an SDK thread; enqueue and return fast."""
        try:
            event = getattr(data, "event", None)
            message = getattr(event, "message", None)
            sender = getattr(event, "sender", None)
            if message is None or sender is None:
                log.debug("Dropping malformed Feishu event")
                return

            sender_id = getattr(getattr(sender, "sender_id", None), "open_id", None) or ""
            chat_id = str(getattr(message, "chat_id", None) or "")
            message_id = str(getattr(message, "message_id", None) or "")
            message_type = str(getattr(message, "message_type", None) or "")
            content = str(getattr(message, "content", None) or "")

            image_key = ""
            if message_type == "image":
                try:
                    image_data = json.loads(content)
                except Exception:
                    image_data = {}
                image_key = str(image_data.get("image_key") or "")

            text = _strip_feishu_ats(_extract_text(message_type, content))

            if sender_id not in self.allowed_senders or chat_id not in self.allowed_chat_ids:
                log.info("Ignoring non-allowlisted message sender=%s chat=%s (bootstrap=%s)",
                         sender_id, chat_id, BOOTSTRAP_MODE)
                if BOOTSTRAP_MODE:
                    self.executor.submit(self._send_handshake, chat_id, sender_id)
                return
            if not message_id:
                log.warning("Ignoring Feishu message without message_id")
                return
            if not text and not image_key:
                log.info("Ignoring empty/non-text Feishu message %s (type=%s)", message_id, message_type)
                self.executor.submit(
                    self._send_unsupported_notice,
                    chat_id,
                    message_id,
                    message_type,
                )
                return

            if not self._claim_message(message_id):
                log.debug("Duplicate Feishu message %s ignored", message_id)
                return

            # 审批回复不能排在正在等待它的 DSH 轮次后面。
            if not image_key and (_looks_like_approval_command(text) or _approval_keyword_only(text)):
                self.approval_executor.submit(
                    self._handle_message, chat_id, sender_id, message_id, text
                )
                return

            # Control traffic must bypass the long-running DSH chat lock and pool.
            if not image_key and is_codex_command(text):
                self.codex_executor.submit(self.codex.handle, chat_id, message_id, text)
                return

            if image_key:
                log.info("Accepted Feishu image msg=%s chat=%s sender=%s image_key=%s", message_id, chat_id, sender_id, image_key)
                self.executor.submit(
                    self._handle_message, chat_id, sender_id, message_id, text, image_key
                )
                return
            log.info("Accepted Feishu msg=%s chat=%s sender=%s text=%.120s", message_id, chat_id, sender_id, text)
            self.executor.submit(self._handle_message, chat_id, sender_id, message_id, text)
        except Exception:
            log.exception("on_feishu_message crashed")

    def _claim_message(self, message_id: str) -> bool:
        now = time.time()

        def mutator(state: Dict[str, Any]) -> bool:
            seen = state.setdefault("seen_message_ids", {})
            # Prune entries older than 48h.
            for old_id in [mid for mid, ts in seen.items() if now - float(ts) > 172800]:
                seen.pop(old_id, None)
            if message_id in seen:
                return False
            seen[message_id] = now
            return True

        return _atomic_state_update(mutator)

    def _mark_seeded(self, session_id: str) -> None:
        def mutator(state: Dict[str, Any]) -> None:
            state.setdefault("memory_seeded", {})[session_id] = True

        _atomic_state_update(mutator)

    def _is_seeded(self, session_id: str) -> bool:
        state = _load_state()
        return bool(state.get("memory_seeded", {}).get(session_id))

    def _seed_memory(self, chat_id: str, session_id: str) -> None:
        try:
            start_seq = self.dsh.tail_seq(session_id)
            self.dsh.prompt(session_id, self.seed_memory_prompt)
            _final, _end = self.dsh.wait_for_turn_end(
                session_id, start_seq, min(300, DSH_TURN_TIMEOUT)
            )
            self._mark_seeded(session_id)
            log.info("Memory seeded in DSH session %s", session_id)
        except Exception:
            log.exception("Memory seed failed for DSH session %s; continuing", session_id)

    def _reset_session(self, chat_id: str) -> str:
        """Drop the persisted DSH session for a chat; next ensure_session is fresh.

        Returns the freshly created session id.
        """

        def mutator(state: Dict[str, Any]) -> None:
            old = (state.get("chats") or {}).pop(chat_id, None)
            seeded = state.get("memory_seeded") or {}
            for key in [k for k in seeded if chat_id in k]:
                seeded.pop(key, None)
            if old:
                log.info(
                    "Reset DSH session for chat=%s (was %s)",
                    chat_id,
                    old.get("session_id"),
                )

        _atomic_state_update(mutator)
        session_id, _created = self.dsh.ensure_session(chat_id)
        return session_id

    def _run_reset_command(self, chat_id: str, message_id: str) -> None:
        """Handle an explicit `/new`-style reset request from Feishu."""
        try:
            session_id = self._reset_session(chat_id)
            self._seed_memory(chat_id, session_id)
            log.info("Manual session reset for chat=%s -> %s", chat_id, session_id)
            self.feishu.send_card(
                chat_id,
                "🆕 新会话已开启",
                "已重置对话上下文并重新加载记忆 ✅\n接下来正常发消息即可。",
                reply_message_id=message_id,
            )
        except Exception:
            log.exception("Failed to reset session for chat=%s", chat_id)
            try:
                self.feishu.send_card(
                    chat_id,
                    "⚠️ 重置失败",
                    "重置会话时出错，请稍后重试。",
                    reply_message_id=message_id,
                )
            except Exception:
                log.exception("Failed to send reset-error card")

    def _send_handshake(self, chat_id: str, sender_id: str) -> None:
        """首次握手：把本应用的 open_id / chat_id 回给用户，便于填 .env。

        飞书的 open_id 是**按应用隔离**的 —— 同一个用户在新应用里 id 完全不同，
        所以新装的桥不可能预先知道白名单。FEISHU_BOOTSTRAP=1 时回一张卡片
        自报身份（**只回 id，不转发给 DSH**），填好 .env 再关掉即可。

        Linux 侧白名单已配好、平时走不到这里；保留它是为了让两台机器的
        bridge.py 完全同源 —— 否则「从 Linux 重新移植到 Windows」会把这个
        功能丢掉（2026-09-18 实际踩到过）。
        """
        markdown = (
            "**🔑 桥已连上，但白名单还没配**\n\n"
            "飞书的 open_id 是**按应用隔离**的，所以新应用里你的 id 和旧应用不同，"
            "桥没法预先知道。把下面两行填进本桥目录的 `.env` 然后重启桥就通了：\n\n"
            f"**FEISHU_ALLOWED_SENDER_OPEN_IDS**={sender_id}\n\n"
            f"**FEISHU_ALLOWED_CHAT_IDS**={chat_id}\n\n"
            f"*（本机是 {_hostname()}）*"
        )
        self.feishu.send_card(chat_id, "🔑 首次握手", markdown, note="本条不会转发给 DSH")

    def _send_unsupported_notice(self, chat_id: str, message_id: str, message_type: str) -> None:
        try:
            self.feishu.send_card(
                chat_id,
                "⚠️ 暂不支持",
                f"当前桥接只处理文字消息（收到类型：`{message_type or 'unknown'}`）。\n请把需求用文字发给我。",
                reply_message_id=message_id or None,
            )
        except Exception:
            log.exception("Failed to send unsupported-message notice")

    def _handle_message(
        self,
        chat_id: str,
        sender_id: str,
        message_id: str,
        text: str,
        image_key: str = "",
    ) -> None:
        if not image_key and _looks_like_approval_command(text):
            return self._run_approval_command(chat_id, message_id, text)
        if not image_key and _approval_keyword_only(text):
            return self._send_approval_hint(chat_id, message_id, _approval_keyword_only(text))
        lock = self._chat_lock(chat_id)
        with lock:
            try:
                with _exclusive_chat_lock(chat_id):
                    if not image_key and _is_reset_command(text):
                        return self._run_reset_command(chat_id, message_id)
                    return self._run_dsh_turn(chat_id, message_id, text, image_key)
            except DshApiError as exc:
                log.exception("DSH API failure for chat=%s", chat_id)
                try:
                    self.feishu.send_card(
                        chat_id,
                        "⚠️ 桥接错误",
                        f"DeepSeek Harness 调用失败：`{exc.code}`\n{exc.message[:500]}",
                        reply_message_id=message_id,
                    )
                except Exception:
                    log.exception("Failed to send error card")
            except Exception:
                log.exception("Unexpected bridge failure for chat=%s", chat_id)
                try:
                    self.feishu.send_card(
                        chat_id,
                        "⚠️ 桥接错误",
                        "处理这条飞书消息时发生异常，请稍后重试。",
                        reply_message_id=message_id,
                    )
                except Exception:
                    log.exception("Failed to send generic error card")

    def _run_dsh_turn(
        self, chat_id: str, message_id: str, text: str, image_key: str = ""
    ) -> None:
            try:
                session_id, _created = self.dsh.ensure_session(chat_id)
                _stamp_session_owner(session_id)
                if not self._is_seeded(session_id):
                    self._seed_memory(chat_id, session_id)

                images: Optional[list] = None
                if image_key:
                    try:
                        raw, content_type = self.feishu.download_image(
                            message_id, image_key
                        )
                        if len(raw) > 10 * 1024 * 1024:
                            self.feishu.send_card(
                                chat_id,
                                "⚠️ 图片过大",
                                "图片超过 10MB，请压缩后重发。",
                                reply_message_id=message_id,
                            )
                            return
                        import base64

                        # DSH 0.1.1-rc.2+ auto-scales/reformats images per model
                        # requirements, so we pass the original bytes through and
                        # let the harness handle resizing. Keep only a coarse size
                        # cap for the upload path.
                        images = [
                            {
                                "media_type": self.feishu._media_type(content_type),
                                "data": base64.b64encode(raw).decode("ascii"),
                                "name": f"{image_key[:8]}.jpg",
                            }
                        ]
                        # Vision model is required for image turns.
                        self.dsh.select_model(
                            session_id,
                            "deepseek-official",
                            "deepseek-v4-flash-vision-exp",
                            "high",
                        )
                    except Exception:
                        log.exception("Failed to download Feishu image msg=%s", message_id)
                        try:
                            self.feishu.send_card(
                                chat_id,
                                "⚠️ 图片下载失败",
                                "无法获取这张图片，请重新发送或改发文字。",
                                reply_message_id=message_id,
                            )
                        except Exception:
                            log.exception("Failed to send image-error card")
                        return

                if not text:
                    text = "（用户发来一张图片，请分析图片内容并给出结论。）"

                # Stream an editable card so the user sees live output; fall
                # back to a plain processing card if streaming init fails.
                card_id = None
                try:
                    card_id = self.feishu.send_card_return_id(
                        chat_id, "⏳ 正在生成", "…"
                    )
                except Exception:
                    log.exception("stream card init failed")
                if card_id is None:
                    try:
                        self.feishu.send_card(
                            chat_id,
                            "🔄 处理中",
                            "已收到，正在处理…",
                            reply_message_id=message_id,
                        )
                    except Exception:
                        log.exception("Failed to send processing card")

                start_seq = self.dsh.tail_seq(session_id)
                final_text = ""
                turn_end: Dict[str, Any] = {}
                attempt = 0
                max_retries = 4
                moderation_resets = 0
                max_moderation_resets = 1
                session_reset_note = ""
                while True:
                    try:
                        self.dsh.prompt(session_id, text, images)
                        # ── 内层等待：超时只提示、不放弃 ──────────────────────
                        # 长任务（大范围回测/分析）经常超过单个轮询窗口。
                        # 关键：`wait_for_turn_end` 每次调用自带 deadline，重复调用
                        # 等于延长等待；**绝不能**因为超时就从外层 while 重来，
                        # 那会重新 prompt 一次、把这一轮提交两遍。
                        extra_wait = 0
                        while True:
                            try:
                                if card_id:
                                    final_text, turn_end = self.dsh.wait_for_turn_end(
                                        session_id, start_seq, DSH_TURN_TIMEOUT,
                                        on_progress=lambda t: self.feishu.update_card(
                                            card_id, "⏳ 生成中", t
                                        ),
                                    )
                                else:
                                    final_text, turn_end = self.dsh.wait_for_turn_end(
                                        session_id, start_seq, DSH_TURN_TIMEOUT
                                    )
                                break
                            except TimeoutError:
                                if extra_wait >= DSH_TURN_HARD_CAP:
                                    raise
                                extra_wait += DSH_TURN_TIMEOUT
                                total_min = (DSH_TURN_TIMEOUT + extra_wait) // 60
                                cap_min = (DSH_TURN_TIMEOUT + DSH_TURN_HARD_CAP) // 60
                                log.warning(
                                    "DSH turn 已运行 %d 分钟仍未结束 chat=%s，继续等待…",
                                    total_min, chat_id,
                                )
                                notice = (
                                    f"已运行约 **{total_min} 分钟**，仍在生成。\n"
                                    f"最长等待 {cap_min} 分钟；完成后结果会自动发给你，"
                                    f"不用重发消息。"
                                )
                                try:
                                    if card_id:
                                        self.feishu.update_card(
                                            card_id, "⏳ 仍在生成", notice)
                                    else:
                                        self.feishu.send_card(
                                            chat_id, "⏳ 仍在生成", notice)
                                except Exception:
                                    log.exception("超时提示发送失败 chat=%s", chat_id)

                        # A moderation rejection poisons the whole transcript:
                        # every later turn on this session fails until it is
                        # reset. Detect it once per message, drop the session
                        # and re-run the same request on a clean one.
                        _reason = turn_end.get("reason") or {}
                        if _reason.get("kind") == "error":
                            _code = str(_reason.get("error", {}).get("code", ""))
                            _msg = str(_reason.get("error", {}).get("message", ""))
                            if (
                                _is_moderation_error(_code, _msg)
                                and moderation_resets < max_moderation_resets
                            ):
                                moderation_resets += 1
                                log.warning(
                                    "内容风控拦截 chat=%s (%s); 重置会话后重试 %d/%d",
                                    chat_id, _msg, moderation_resets, max_moderation_resets,
                                )
                                if card_id:
                                    self.feishu.update_card(
                                        card_id, "🧹 重置会话",
                                        "检测到内容风控拦截，正在重置会话上下文并重试…",
                                    )
                                session_id = self._reset_session(chat_id)
                                self._seed_memory(chat_id, session_id)
                                start_seq = self.dsh.tail_seq(session_id)
                                final_text = ""
                                turn_end = {}
                                session_reset_note = "（原会话被内容风控拦截，已自动重置上下文）\n\n"
                                continue
                        break
                    except TimeoutError as exc:
                        cap_min = (DSH_TURN_TIMEOUT + DSH_TURN_HARD_CAP) // 60
                        log.error("DSH turn 超过总上限 for chat=%s: %s", chat_id, exc)
                        msg = (f"任务执行超过 {cap_min} 分钟，已中止等待。"
                               "**结果可能仍然生成完了** —— 发一句「继续」我就把结论发给你；"
                               "也可以让我换一个方式重试，或把任务拆小。")
                        if card_id:
                            self.feishu.update_card(card_id, "⏱ 任务超时", msg)
                        else:
                            self.feishu.send_card(
                                chat_id, "⏱ 任务超时", msg, reply_message_id=message_id
                            )
                        return
                    except DshApiError as exc:
                        # DSH service restart (or a transient transport blip):
                        # wait for it to come back, then re-submit the turn so the
                        # conversation keeps going instead of dropping mid-reply.
                        if exc.code == "transport" and attempt < max_retries:
                            attempt += 1
                            log.warning(
                                "DSH 服务连接失败(疑似重启) chat=%s，%d/%d 重试",
                                chat_id, attempt, max_retries,
                            )
                            if card_id:
                                self.feishu.update_card(
                                    card_id, "🔄 服务重启中",
                                    "DSH 服务正在重启，稍后自动恢复并继续…",
                                )
                            time.sleep(8)
                            start_seq = self.dsh.tail_seq(session_id)
                            continue
                        raise

                reason = turn_end.get("reason") or {}
                if reason.get("kind") == "error":
                    code = reason.get("error", {}).get("code", "unknown")
                    err = reason.get("error", {}).get("message", "unknown")
                    if _is_moderation_error(str(code), str(err)):
                        final_text = (
                            "这条消息被 DeepSeek 内容风控拦截了（Content Exists Risk），"
                            "已自动重置会话仍未通过。建议换种说法或删掉敏感内容后重发；"
                            "也可以发 /new 手动清空会话。"
                        )
                    else:
                        final_text = final_text or f"任务执行失败：`{code}` {err}"

                if session_reset_note:
                    final_text = session_reset_note + final_text

                if not final_text.strip():
                    final_text = "任务已完成，但没有生成文本回复。"

                # 抽出 [[FILE: ...]] 标记：正文照旧走卡片，文件在卡片之后单独发。
                final_text, marked_files = self.feishu._extract_file_markers(final_text)
                if marked_files:
                    log.info("检测到 %d 个附件标记 chat=%s", len(marked_files), chat_id)
                if not final_text.strip():
                    final_text = "（正文只有附件，见下条文件消息）"

                model = self.dsh.current_model(session_id)
                # 卡片标题取回复内容概览（不再一律显示 deepseek）
                final_title = _reply_card_title(final_text)
                model_note = f"模型：{model}"
                if card_id:
                    # 按【表格数量】切分：第一段更新原卡片（保留已读位置），
                    # 其余补发新卡片。这样正文继续用原生表格也不会因超限被整条拒绝。
                    parts = self.feishu._chunks_by_tables(final_text.strip())
                    ok = self.feishu.update_card(
                        card_id, final_title, parts[0], note=model_note
                    )
                    if not ok:
                        # 卡片更新整条失败时此前会静默丢弃内容，用户只看到
                        # 卡片停在「⏳ 生成中」。兜底：补发新卡片保证送达。
                        log.warning("最终卡片更新失败 chat=%s，改为补发新卡片", chat_id)
                        try:
                            self.feishu.send_card(
                                chat_id, final_title, parts[0], note=model_note)
                        except Exception:
                            log.exception("补发卡片也失败 chat=%s（结果未能送达）", chat_id)
                    elif len(parts) > 1:
                        for idx, part in enumerate(parts[1:], 2):
                            try:
                                self.feishu.send_card(
                                    chat_id, f"{final_title} ({idx}/{len(parts)})", part,
                                    note=model_note if idx == len(parts) else None,
                                )
                            except Exception:
                                log.exception("续卡 %d/%d 发送失败 chat=%s",
                                              idx, len(parts), chat_id)
                else:
                    self.feishu.send_card(
                        chat_id,
                        final_title,
                        final_text.strip(),
                        reply_message_id=message_id,
                        note=model_note,
                    )
                for fpath, fname in marked_files:
                    try:
                        self.feishu.send_file(chat_id, fpath, file_name=fname)
                        log.info("已发送附件 %s (%s)", fname, fpath)
                    except Exception:
                        log.exception("附件发送失败 %s", fpath)
                        try:
                            self.feishu.send_card(
                                chat_id, "⚠️ 附件发送失败",
                                f"`{fname}`\n\n路径：`{fpath}`",
                                note="文件仍在磁盘上，可手动取用",
                            )
                        except Exception:
                            log.exception("附件失败提示也发不出去 %s", fpath)
                log.info("Replied to Feishu msg=%s (%d chars)", message_id, len(final_text))
            except DshApiError as exc:
                log.exception("DSH API failure for chat=%s", chat_id)
                try:
                    self.feishu.send_card(
                        chat_id,
                        "⚠️ 桥接错误",
                        f"DeepSeek Harness 调用失败：`{exc.code}`\n{exc.message[:500]}",
                        reply_message_id=message_id,
                    )
                except Exception:
                    log.exception("Failed to send error card")
            except Exception:
                log.exception("Unexpected bridge failure for chat=%s", chat_id)
                try:
                    self.feishu.send_card(
                        chat_id,
                        "⚠️ 桥接错误",
                        "处理这条飞书消息时发生异常，请稍后重试。",
                        reply_message_id=message_id,
                    )
                except Exception:
                    log.exception("Failed to send generic error card")


def main() -> int:
    try:
        bridge = Bridge()
    except Exception:
        log.exception("Bridge startup failed")
        return 1

    bridge.dsh.warmup_auth()
    bridge.start_heartbeat()
    try:
        bridge.start_feishu()
    except KeyboardInterrupt:
        log.info("Bridge interrupted")
        return 0
    except Exception:
        log.exception("Feishu websocket client stopped unexpectedly")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

