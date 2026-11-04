# DSH Feishu Bridge

把飞书消息桥接进 [DeepSeek Harness](https://www.deepseek.com/en/harness/) 会话的桥接程序。

飞书 ↔ 桥 ↔ DSH Web API（`127.0.0.1:3080`）↔ 持久会话。

---

## 它做什么

- 飞书私聊/DM 的消息 → 转发给 DSH 的一个**持久会话**（每个 chat 一个 session）
- DSH 的回复 → 以**飞书静态卡片**发回
- 支持图片（自动切视觉模型）、文件发送（有白名单）、超时续等
- 首次可用握手模式自报 `open_id` / `chat_id`，方便配置白名单
- 回复卡片的标题自动提取内容概览
- 宿主审批转发到飞书；回复 `批准 编号` / `拒绝 编号`，不占用模型对话队列
- Windows 隐藏启动器、自动重启和单实例保护

---

## 前置条件

| 依赖 | 版本/说明 |
|---|---|
| Python | 3.10+ |
| `lark-oapi` | 飞书官方 SDK |
| `pillow` | 图片压缩 |
| DSH | 已跑起来，Web 在 `127.0.0.1:3080` |
| 飞书应用 | 自建应用，开启**机器人**能力 + **长连接（WebSocket）** 接收消息 |

---

## 安装

### 1. 拉代码

```bash
git clone https://github.com/ColinStayInLife/dsh-feishu-bridge.git ~/.dsh/feishu-bridge
cd ~/.dsh/feishu-bridge
```

### 2. 建虚拟环境

```bash
python3 -m venv ~/.venvs/feishu-bridge
~/.venvs/feishu-bridge/bin/pip install lark-oapi pillow
```

### 3. 配凭据

```bash
cp .env.example .env
# 编辑 .env，填 FEISHU_APP_ID / FEISHU_APP_SECRET
chmod 600 .env
```

> 编辑桥目录的 `.env`，填写凭据、白名单和 workspace id。入口 `bridge_env.py` 会在导入主程序前加载配置；已有进程环境变量优先。`FEISHU_ENV_FILE` 可指定其他配置文件。

### 4. 首次握手拿 id（重要）

白名单默认为空 = **拒绝所有消息**。第一次启动用握手模式：

```bash
FEISHU_BOOTSTRAP=1 python bridge_env.py
```

然后**在飞书里给机器人发一条消息**，它会回一张卡片，里面有你的 `chat_id` 和 `open_id`。
把这两个值填进 `.env`：

```
FEISHU_ALLOWED_CHAT_IDS=oc_xxxxxxxx
FEISHU_ALLOWED_SENDER_OPEN_IDS=ou_xxxxxxxx
```

填好后**去掉 `FEISHU_BOOTSTRAP=1`** 重启，桥才真正开始转发。

### 5. 拿到 DSH workspace id

```bash
python3 -c "import json;print(json.load(open('$HOME/.dsh/storages/workspace.json')))"
```

把里面的 workspace id 填进 `.env` 的 `DSH_FEISHU_WORKSPACE_ID`。

### 6. 装成 systemd 服务

```bash
mkdir -p ~/.config/systemd/user
cp systemd/dsh-feishu-bridge.service ~/.config/systemd/user/
# 按需修改 ExecStart 里的 python 路径
systemctl --user daemon-reload
systemctl --user enable --now dsh-feishu-bridge.service
journalctl --user -u dsh-feishu-bridge -f
```

> **注意**：如果你没有开启 linger，用户服务只在登录后启动。
> `loginctl enable-linger $USER` 可让它开机自启（不需要登录）。

---

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `FEISHU_APP_ID` | — | **必填**，飞书应用 App ID |
| `FEISHU_APP_SECRET` | — | **必填**，飞书应用 App Secret |
| `FEISHU_ALLOWED_CHAT_IDS` | 空 | 允许的 chat_id，逗号分隔。空=全部拒绝 |
| `FEISHU_ALLOWED_SENDER_OPEN_IDS` | 空 | 允许的发送者 open_id，逗号分隔 |
| `FEISHU_BOOTSTRAP` | `0` | `1`=握手模式，只回 id 不转发 |
| `FEISHU_ENV_FILE` | 桥目录的 `.env` | 凭据文件路径 |
| `DSH_API_BASE` | `http://127.0.0.1:3080` | DSH Web API |
| `DSH_FEISHU_WORKSPACE_ID` | 空 | **必填**，DSH workspace id |
| `DSH_FEISHU_TURN_TIMEOUT` | `900` | 单次轮询窗口（秒） |
| `DSH_FEISHU_TURN_HARD_CAP` | `10800` | 总等待上限（秒，默认 3 小时） |
| `DSH_FEISHU_TABLE_MODE` | `auto` | 表格模式：`auto`/`native`/`code`/`off` |
| `DSH_FEISHU_TABLE_MAX_NATIVE` | `5` | 单卡最多原生表格数 |
| `DSH_FEISHU_FILE_ROOTS` | `$HOME/,/tmp/` | 允许发送文件的根目录白名单 |

### Windows 启动

把仓库放到 `%USERPROFILE%\.dsh\feishu-bridge`，配置 `.env`，并用系统 Python 安装 `lark-oapi pillow`。
`dsh.cmd` 需要在 `PATH` 中。可将以下两条命令分别配置为登录时运行的计划任务：

```powershell
powershell.exe -NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -File "$env:USERPROFILE\.dsh\feishu-bridge\run_hidden.ps1" -Component Web
powershell.exe -NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -File "$env:USERPROFILE\.dsh\feishu-bridge\run_hidden.ps1" -Component Bridge
```

启动器会防止重复实例，在退出后重启进程，并把输出写到桥目录 `logs/`。Web 组件使用中立工作目录，避免 DSH 将桥专用 `.env` 中的启动键当成自己的项目配置。

### 飞书审批

`plugins/approval-feishu.mjs` 是宿主审批应答模块，`web.patch.yml` 是它的加载配置。Windows 的 Web 启动器会自动加载。
Linux 可在原有 Web 启动命令中加入该补丁，例如：

```bash
mkdir -p "$HOME/.dsh/feishu-bridge/logs"
cd "$HOME"
DSH_FEISHU_BRIDGE_DIR="$HOME/.dsh/feishu-bridge" dsh web \
  --patch "$HOME/.dsh/feishu-bridge/web.patch.yml" \
  --host 127.0.0.1 --port 3080 --no-open \
  >> "$HOME/.dsh/feishu-bridge/logs/web.log" 2>&1
```

审批目录默认是 `~/.dsh/approvals`。桥只转发自己会话的审批到对应的白名单聊天，回复会通过独立线程池直接写入回执。
通知发送失败会重试；超时、取消或没有回复均拒绝。审批策略为 `never` 的会话不会产生审批请求。
自定义目录时，桥的 `FEISHU_APPROVAL_PIPE_DIR` 和宿主的 `DSH_APPROVAL_PIPE_DIR` 必须一致。

### 离线验证

在仓库根目录运行以下命令，不需要真实飞书凭据，也不会发送飞书消息：

```bash
python tests/test_features.py
python tests/test_regressions.py
node --test tests/approval-feishu.test.mjs
```

共 38 项断言/测试，覆盖卡片标题、审批解析、入口配置、SDK 兼容、跨进程锁、任务等待时的审批调度、通知重试，以及宿主超时和取消清理。

---

## 关键设计（踩过的坑，别改回去）

### 1. 超时必须是「轮询窗口」而不是「硬超时」
早期版本 900s 到点直接 `return` → **那条回复永久丢失**，表现为"思考久了有结果也不回复"。
现在 900s 只是**一次轮询**：到期发"仍在生成"提示，然后继续等，直到 `TURN_HARD_CAP` 才放弃。

### 2. 飞书卡片表格有数量上限（ErrCode 11310）
单张卡片原生表格超过 5 张会被拒。`TABLE_MODE=auto` 的策略是**先拆卡**（每段 ≤5 张表），
拆完还被拒才降级成等宽代码块。**不要直接降级** —— 拆卡能保留飞书原生表格的观感。

### 3. 最终投递必须有兜底
`update_card` 失败要 `send_card` 补发，否则用户看到的是卡在"生成中"的旧卡片。

### 4. Windows 用 `msvcrt` 而不是 `fcntl`
两个平台的锁语义不同：POSIX 记录锁按**进程**，Windows `LockFile` 按**句柄**。跨平台必须分支。

---

## 跨平台

已在 Linux 与 Windows 上运行。差异点：
- 文件锁：`fcntl.flock` ↔ `msvcrt.locking`
- 进程存活判断不同
- systemd ↔ Windows 计划任务

---

## 许可

私有配置项目，未指定开源许可。
