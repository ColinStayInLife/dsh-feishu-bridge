# -*- coding: utf-8 -*-
"""bridge 新功能离线自测：卡片标题 + 审批命令解析"""
import os
import sys
import tempfile
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
_runtime = tempfile.TemporaryDirectory()
os.environ["DSH_FEISHU_BRIDGE_DIR"] = _runtime.name
_pipe = Path(_runtime.name) / ".dsh" / "approvals"
(_pipe / "pending").mkdir(parents=True)
(_pipe / "channels").mkdir()
os.environ["FEISHU_APPROVAL_PIPE_DIR"] = str(_pipe)
sys.path.insert(0, str(BASE))

import bridge  # noqa: E402

ok = fail = 0


def check(name, got, want):
    global ok, fail
    good = got == want
    ok, fail = (ok + 1, fail) if good else (ok, fail + 1)
    print(f"{'PASS' if good else 'FAIL'}  {name}")
    if not good:
        print(f"      got={got!r}\n      want={want!r}")


print("=" * 64)
print("卡片标题 _reply_card_title")
print("=" * 64)
check("粗体+链接", bridge._reply_card_title("**Word 版已生成** 👇\n\n📄 [文件](docs/x.docx)"),
      "🤖 Word 版已生成 👇")
check("跳过代码块", bridge._reply_card_title("```\ncode here\n```\n结论：可以跑"),
      "🤖 结论：可以跑")
check("跳过装饰线", bridge._reply_card_title("---\n# 一级标题\n正文"),
      "🤖 一级标题")
check("空内容回退", bridge._reply_card_title(""), "✅ DeepSeek")
check("纯空白回退", bridge._reply_card_title("   \n\n  "), "✅ DeepSeek")
check("引用符", bridge._reply_card_title("> 行情结论：涨"), "🤖 行情结论：涨")
check("列表项", bridge._reply_card_title("- 第一个要点\n- 第二个"), "🤖 第一个要点")
long_line = "这是一段很长的回复开头，需要被截断到合适长度才对" * 2
t = bridge._reply_card_title(long_line)
check("长文本截断带省略号", t.endswith("…") and len(t) <= bridge.CARD_TITLE_MAX + 4, True)
check("零宽字符被清掉", bridge._reply_card_title("\u200b\u200b你好"), "🤖 你好")

print()
print("=" * 64)
print("审批命令解析")
print("=" * 64)
check("批准 带编号", bridge._parse_approval_command("批准 7f3a1c9d"), ("7f3a1c9d", "allowed-once"))
check("拒绝 带编号", bridge._parse_approval_command("拒绝 ab12"), ("ab12", "rejected"))
check("批准 中文冒号", bridge._parse_approval_command("批准：7f3a1c9d"), ("7f3a1c9d", "allowed-once"))
check("approve 英文", bridge._parse_approval_command("approve 7f3a1c9d"), ("7f3a1c9d", "allowed-once"))
check("无编号不解析", bridge._parse_approval_command("批准"), (None, None))
check("非 hex 不解析", bridge._parse_approval_command("批准 xyz12345"), (None, None))
check("过短编号不解析", bridge._parse_approval_command("批准 ab1"), (None, None))
check("普通消息不误判", bridge._looks_like_approval_command("帮我写个报告"), False)
check("审批命令被识别", bridge._looks_like_approval_command("批准 7f3a1c9d"), True)
check("仅关键词识别", bridge._approval_keyword_only("批准"), "批准")
check("仅关键词-拒绝", bridge._approval_keyword_only(" 拒绝 "), "拒绝")
check("非关键词返回空", bridge._approval_keyword_only("批准 7f3a1c9d"), "")

print()
print("=" * 64)
print("审批管道路径")
print("=" * 64)
pipe = bridge._approval_pipe_dir()
print("pipe dir      =", pipe)
print("pending 存在   =", (pipe / "pending").exists())
print("channels 存在  =", (pipe / "channels").exists())
print("poll 间隔     =", bridge.APPROVAL_POLL_SECONDS)
check("管道目录在 .dsh 下", ".dsh" in str(pipe), True)

print()
print("=" * 64)
print("卡片构造（标题真的进 header 吗）")
print("=" * 64)
card = bridge.FeishuSender._card("🤖 测试标题", "正文")
check("header.title == 传入标题",
      card["header"]["title"]["content"], "🤖 测试标题")

print()
print(f"结果: {ok} 通过 / {fail} 失败")
sys.exit(1 if fail else 0)
