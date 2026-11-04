# -*- coding: utf-8 -*-
"""飞书桥入口：先把 .env 灌进进程环境，再启动 bridge。

为什么需要这一层：
  bridge.py 在模块级就把 DSH_API_BASE / DSH_FEISHU_WORKSPACE_ID 等
  常量快照成模块变量（os.getenv(...)），如果在 import 之后才写
  os.environ，这些值会被静默忽略，表现为"连上了但不回话"。

所以必须在 import bridge 之前完成环境注入。
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
ENV_PATH = Path(os.environ.get("FEISHU_ENV_FILE") or (BASE / ".env"))


def load_env(path: Path) -> int:
    if not path.exists():
        print(f"[bridge_env] WARN .env not found: {path}", file=sys.stderr)
        return 0
    n = 0
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        # 不覆盖已有环境变量（便于临时用 shell 变量调试）
        if key not in os.environ:
            os.environ[key] = value.strip().strip("'").strip('"')
            n += 1
    return n


if __name__ == "__main__":
    os.environ.setdefault("FEISHU_ENV_FILE", str(ENV_PATH))
    loaded = load_env(ENV_PATH)
    # 兜底：桥目录默认为本文件所在目录
    os.environ.setdefault("DSH_FEISHU_BRIDGE_DIR", str(BASE))
    os.environ.setdefault("DSH_API_BASE", "http://127.0.0.1:3080")
    sys.path.insert(0, str(BASE))

    import bridge  # noqa: E402  （必须在上面的环境注入之后）

    print(f"[bridge_env] loaded {loaded} vars from {ENV_PATH}, "
          f"workspace={os.environ.get('DSH_FEISHU_WORKSPACE_ID', '')[:8]}...", flush=True)
    raise SystemExit(bridge.main())
