# DSH Feishu Bridge

把飞书消息桥接进 [DeepSeek Harness](https://www.deepseek.com/en/harness/) 会话的桥接程序。

飞书 ↔ 桥 ↔ DSH Web API（`127.0.0.1:3080`）↔ 持久会话。

---

## 它做什么

- 飞书私聊/DM 的消息 → 转发给 DSH 的一个**持久会话**（每个 chat 一个 session）
- DSH 的回复 → 以**飞书静态卡片**发回
- 支持图片（自动切视觉模型）、文件发送（有白名单）、超时续等
- 首次可用握手模式自报 `open_id` / `chat_id`，方便配置白名单

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
git clone <this-repo> ~/.dsh/feishu-bridge
cd ~/.dsh/feishu-bridge
```

### 2. 建虚拟环境

```bash
python3 -m venv ~/.venvs/feishu-bridge
~/.venvs/feishu-bridge/bin/pip install lark-oapi pillow
```

### 3. 配凭据

```bash
mkdir -p ~/.hermes
cp .env.example ~/.hermes/.env
# 编辑 ~/.hermes/.env，填 FEISHU_APP_ID / FEISHU_APP_SECRET
chmod 600 ~/.hermes/.env
```

> 凭据来源可用 `FEISHU_ENV_FILE` 指向任意路径，默认 `~/.hermes/.env`。

### 4. 首次握手拿 id（重要）

白名单默认为空 = **拒绝所有消息**。第一次启动用握手模式：

```bash
FEISHU_BOOTSTRAP=1 python bridge.py
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
| `FEISHU_ENV_FILE` | `~/.hermes/.env` | 凭据文件路径 |
| `DSH_API_BASE` | `http://127.0.0.1:3080` | DSH Web API |
| `DSH_FEISHU_WORKSPACE_ID` | 空 | **必填**，DSH workspace id |
| `DSH_FEISHU_TURN_TIMEOUT` | `900` | 单次轮询窗口（秒） |
| `DSH_FEISHU_TURN_HARD_CAP` | `10800` | 总等待上限（秒，默认 3 小时） |
| `DSH_FEISHU_TABLE_MODE` | `auto` | 表格模式：`auto`/`native`/`code`/`off` |
| `DSH_FEISHU_TABLE_MAX_NATIVE` | `5` | 单卡最多原生表格数 |
| `DSH_FEISHU_FILE_ROOTS` | `$HOME/,/tmp/` | 允许发送文件的根目录白名单 |

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
