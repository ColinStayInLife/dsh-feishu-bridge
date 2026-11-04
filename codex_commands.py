"""Fast Codex commands and durable notifications for the existing Feishu bridge."""
import json
import logging
import re
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path.home() / '.dsh/codex-bridge'))
from client import call

log = logging.getLogger('dsh-feishu.codex')

HELP = '''📱 **独立 Codex 控制**
电脑开机联网即可运行，关闭 VS Code 不会停止任务。

• **/codex 直接写任务**（一步到位：自动建会话或接着当前会话，例：`/codex 把 utils.py 里的 TODO 清理掉`）
• /codex 新建 极板任务
• /codex 列表
• **/codex 连接codex1** 项目会话 ／ **/codex 连接codex2** 闲聊会话（粘性，切换后一直走它）
• /codex 发送 检查右端开孔代码，先不要运行 CAD
• /codex 状态
• /codex 停止
• /codex 通知 开 /codex 通知 关
• /codex 历史 关键词
• /codex 别名 （看已登记的别名）
• /codex 接管 外部会话ID（把 VS Code 等已有会话接过来，**同一个会话**，不复制）
• /codex 导入 源会话完整ID（复制历史成**新的**独立会话，两边之后分叉）
• /codex 回答 请求编号 你的回答
• /codex 批准 请求编号 /codex 拒绝 请求编号

新建/选择/连接/接管 都会自动订阅完成、报错和提问通知。
运行中“发送”会追加指令。
「直接写任务」= 有当前会话就追加，没有就自动新建；想换会话先用「连接」。
**连接 vs 导入**：连接 = 接管原会话，你在 VS Code 和飞书看到的是同一条线；
导入 = 复制一份新会话，之后两边各走各的。
别名表：`~/.dsh/codex-bridge/aliases.json`
默认项目：miniaturized-ht-pemfc。其他项目可由 DSH 的 codex_control 工具指定 cwd。
当前并行上限：1 个任务；执行服务内存上限：24 GiB。'''


# codex 子命令词表（中英文 + 别名），is_command 与 run 共用一份
SUBCOMMAND_ALIASES = {
    '帮助': 'help', 'help': 'help',
    '列表': 'list', 'list': 'list',
    '状态': 'status', 'status': 'status',
    '停止': 'stop', 'stop': 'stop',
    '发送': 'send', 'send': 'send',
    '新建': 'new', 'new': 'new',
    '选择': 'bind', '连接': 'bind', 'bind': 'bind',
    '通知': 'watch', 'watch': 'watch',
    '历史': 'history', 'history': 'history',
    '导入': 'fork', 'fork': 'fork',
    '接管': 'adopt', 'adopt': 'adopt',
    '别名': 'alias', 'alias': 'alias',
    '回答': 'respond', 'respond': 'respond',
    '批准': 'respond', '拒绝': 'respond',
    '健康': 'health', 'health': 'health',
}
SUBCOMMANDS = frozenset(SUBCOMMAND_ALIASES)

# 允许「子命令+参数」黏在一起写（如 连接codex1）的子命令。
# 只放**参数是会话ID/别名**的那几个：像「新建一个文件夹」这种正常任务名不能被误拆。
GLUE_OK = frozenset({'连接', '选择', 'bind', '导入', 'fork', '接管', 'adopt'})

ALIASES_PATH = Path.home() / '.dsh/codex-bridge/aliases.json'


def load_aliases():
    try:
        data = json.loads(ALIASES_PATH.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as exc:
        log.warning('别名文件读取失败：%s', exc)
        return {}


def save_aliases(data):
    try:
        ALIASES_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = ALIASES_PATH.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        tmp.replace(ALIASES_PATH)
    except Exception as exc:
        log.warning('别名文件写入失败：%s', exc)


BIND_ARG_SEP = re.compile(r'[\s，,、;；。!！?？]+')


def split_bind_arg(arg):
    """`codex1，汇报最新情况。` → ('codex1', '汇报最新情况。')

    用户经常把「连接哪个会话」和「要它干什么」写在一句话里。
    只取第一段当会话标识，剩下的当任务；没有剩余就返回空串。
    """
    parts = BIND_ARG_SEP.split(arg.strip(), maxsplit=1)
    spec = parts[0].strip()
    task = parts[1].strip() if len(parts) > 1 else ''
    return spec, task


def split_glued(op):
    """把 `连接codex1` 拆成 ('连接', 'codex1')。只对 GLUE_OK 里的子命令生效。"""
    if op in SUBCOMMAND_ALIASES:
        return op, ''
    for word in sorted(GLUE_OK, key=len, reverse=True):
        if op.startswith(word) and len(op) > len(word):
            return word, op[len(word):]
    return op, ''


def is_command(text):
    """判断是否为 codex 指令。

    注意：裸词 `codex` 只有在第二个词是已知子命令时才算指令。
    否则「codex 帮我看看这段代码」这类日常说法会被吞成帮助列表，
    而不是当成普通对话交给 DSH。
    """
    parts = text.strip().split(maxsplit=1)
    if not parts:
        return False
    head = parts[0].lower()
    # 显式带斜杠：一定是指令
    if head in ('/codex', '/cx'):
        return True
    if head != 'codex':
        return False
    # 裸 codex：后面没内容（等同帮助）或紧跟已知子命令才算
    if len(parts) == 1:
        return True
    sub = parts[1].strip().split(maxsplit=1)[0].lower()
    # `codex 连接codex1` 这种黏写也要认；split_glued 只拆 GLUE_OK 里的子命令，
    # 所以「codex 新建一个文件夹」仍会落回普通对话
    sub = split_glued(sub)[0]
    return sub in SUBCOMMANDS


def channel(chat_id):
    return 'feishu:' + chat_id


def resolve_prefix(prefix):
    matches = [x['id'] for x in call('list')['threads'] if x['id'].startswith(prefix)]
    if len(matches) != 1:
        raise ValueError('会话ID不存在或前缀不唯一，请用“列表”中的完整ID。')
    return matches[0]


def connect(channel, arg):
    """把 arg（别名 / 完整ID / 唯一前缀）接到当前渠道。

    顺序：① 别名查表 → ② 服务里已注册就直接绑定 →
    ③ 服务里没有就当成 Codex 里的外部会话**接管**（同一个会话，不是副本）。
    别名的值是 source_id 而不是 session_id 时，退回「导入」语义（fork 一次并记住新ID）。
    """
    aliases = load_aliases()
    entry = aliases.get(arg.lower()) or aliases.get(arg) or {}
    if isinstance(entry, str):
        entry = {'session_id': entry}

    # 只登记了源会话 → 首次使用时导入成独立会话，并把结果写回别名
    if not entry.get('session_id') and entry.get('source_id'):
        params = {'channel': channel, 'source_id': entry['source_id'],
                  'title': entry.get('title') or '手机接续任务'}
        if entry.get('cwd'):
            params['cwd'] = entry['cwd']
        result = call('fork', params)
        entry['session_id'] = result['id']
        aliases[arg.lower()] = entry
        save_aliases(aliases)
        return result

    sid = entry.get('session_id') or arg
    try:
        return call('bind', {'channel': channel, 'thread_id': resolve_prefix(sid)})
    except Exception as first:
        # 不在独立服务里 → 尝试接管；失败则把第一手错误原样抛出，便于定位
        params = {'channel': channel, 'thread_id': sid}
        for key in ('cwd', 'title'):
            if entry.get(key):
                params[key] = entry[key]
        try:
            result = call('adopt', params)
        except Exception as exc:
            raise ValueError('连接失败：%s（服务内绑定也失败：%s）' % (exc, first)) from exc
        if arg.lower() in aliases:
            aliases[arg.lower()]['session_id'] = result['id']
            save_aliases(aliases)
        return result


def render(result):
    if 'threads' in result:
        rows = result['threads']
        if not rows:
            return '暂无会话。发送 /codex 新建 任务名称。'
        return '\n\n'.join(
            f"**{str(x.get('title') or x.get('name') or x.get('preview') or '未命名')[:100]}**\n"
            f"ID：{x['id']}\n状态：{x.get('status', '历史记录')}\n项目：{x.get('cwd', '')}"
            for x in rows[:15])
    if 'events' in result:
        text = f"**{result.get('title', 'Codex')}**\nID：{result.get('id')}\n状态：{result.get('status')}\n"
        if result.get('executor_unavailable'):
            text += '执行服务暂不可用，以下是已保存的状态。\n'
        text += '\n'.join(str(e['text']) for e in result['events'][-4:])[-7000:]
        return text
    if 'message' in result:
        return result['message'] + (f"\nID：{result['thread_id']}" if result.get('thread_id') else '')
    if 'id' in result:
        return f"**{result['title']}**\nID：{result['id']}\n项目：{result['cwd']}\n已选定并开启通知。发送 /codex 发送 你的任务"
    return json.dumps(result, ensure_ascii=False, indent=2)


def _body_after_command(text):
    """取 /codex 之后的全部内容，保留原样（去掉首尾空白）。"""
    parts = text.strip().split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ''


def _has_session(channel):
    """该渠道是否已经绑定/选中了会话（未绑定时控制服务会抛错）。"""
    try:
        call('status', {'channel': channel})
        return True
    except Exception:
        return False


def _auto_task(body, params, message_id):
    """一句话直接派活：没有会话就自动新建（用任务开头 24 字当标题），然后发送。"""
    channel = params['channel']
    if not _has_session(channel):
        call('new', {'channel': channel, 'title': body[:24] or '手机派发的任务'})
    return render(call('send', {'channel': channel, 'text': body},
                       request_id='feishu:' + message_id))


def render_aliases():
    aliases = load_aliases()
    if not aliases:
        return ('暂无别名。别名文件：%s\n'
                '用 /codex 连接 <会话ID> 接上会话后，把 ID 填进该文件即可。' % ALIASES_PATH)
    lines = []
    for name, entry in sorted(aliases.items()):
        if isinstance(entry, str):
            entry = {'session_id': entry}
        sid = entry.get('session_id') or entry.get('source_id') or '?'
        tag = '' if entry.get('session_id') else '（未导入，首次连接时自动处理）'
        lines.append(f"**{name}**\nID：{sid}{tag}\n项目：{entry.get('cwd', '继承源会话')}")
    return '\n\n'.join(lines)


def run(text, chat_id, message_id):
    parts = text.strip().split(maxsplit=2)
    op = parts[1].lower() if len(parts) > 1 else '帮助'
    arg = parts[2].strip() if len(parts) > 2 else ''
    # 支持 `连接codex1` 这种黏在一起的写法
    op, glued = split_glued(op)
    if glued and not arg:
        arg = glued
    params = {'channel': channel(chat_id)}
    if op in ('帮助', 'help'):
        return HELP
    action = SUBCOMMAND_ALIASES.get(op, op)
    if action in ('new', 'send'):
        if not arg:
            raise ValueError('请在命令后填写任务名称或指令。')
        params['title' if action == 'new' else 'text'] = arg
    elif action == 'bind':
        if not arg:
            raise ValueError('请指定会话ID或别名，例：/codex 连接 codex1')
        spec, task = split_bind_arg(arg)
        result = connect(params['channel'], spec)
        out = render(result)
        if task:
            # 连接已经成功了，任务派发失败不能把结果吞掉
            try:
                out += '\n\n' + render(
                    call('send', {'channel': params['channel'], 'text': task},
                         request_id='feishu:' + message_id))
            except Exception as exc:
                out += '\n\n⚠️ 已连接，但任务派发失败：' + str(exc)[:600]
        return out
    elif action == 'adopt':
        if not arg:
            raise ValueError('请指定要接管的会话ID。')
        params['thread_id'] = arg
        return render(call('adopt', params))
    elif action == 'alias':
        return render_aliases()
    elif action == 'watch':
        if arg not in ('开', '关', 'on', 'off'):
            raise ValueError('通知参数为 开 或 关。')
        params['enabled'] = arg in ('开', 'on')
    elif action == 'history':
        params['search'] = arg or None
    elif action == 'fork':
        params['source_id'] = arg
        params['title'] = '手机接续任务'
    elif action == 'respond':
        reply = arg.split(maxsplit=1)
        if not reply:
            raise ValueError('请填写请求编号。')
        params['request_id'] = reply[0]
        if op in ('批准', '拒绝'):
            params['decision'] = 'accept' if op == '批准' else 'decline'
        elif len(reply) > 1:
            if reply[1].lstrip().startswith('{'):
                params['answers'] = json.loads(reply[1])
            else:
                params['text'] = reply[1]
        else:
            raise ValueError('请填写回答内容。')
    elif action not in ('list', 'status', 'stop', 'health'):
        # 一行直接派活：/codex <自然语言任务>
        # 已绑定会话就追加到当前会话；没有就自动新建一个再发。
        body = _body_after_command(text)
        if not body:
            return HELP
        return _auto_task(body, params, message_id)
    return render(call(action, params, request_id='feishu:' + message_id))


class CodexChannel:
    def __init__(self, sender, chat_ids):
        self.sender = sender
        self.chat_ids = set(chat_ids)
        self.stop_event = threading.Event()

    def handle(self, chat_id, message_id, text):
        log.info('Codex 命令 chat=%s msg=%s text=%.120s', chat_id, message_id, text)
        try:
            body = run(text, chat_id, message_id)
        except Exception as exc:
            log.warning('Codex 命令执行失败 msg=%s: %s: %s',
                        message_id, type(exc).__name__, exc)
            body = '⚠️ ' + str(exc)[:1400]
        try:
            self.sender.send_card(chat_id, '🛠 Codex', body, reply_message_id=message_id)
            log.info('Codex 命令已回卡 msg=%s (%d 字符)', message_id, len(body))
        except Exception as exc:
            # 回卡失败必须留痕：之前这里是静默的，出问题无从排查
            log.error('Codex 命令回卡失败 msg=%s: %s: %s',
                      message_id, type(exc).__name__, exc)
            raise

    def start(self):
        threading.Thread(target=self.poll, daemon=True, name='codex-notifications').start()

    def poll(self):
        while not self.stop_event.is_set():
            for chat_id in self.chat_ids:
                try:
                    events = call('notifications', {'channel': channel(chat_id)})['events']
                    for e in events:
                        title = {'completed': '✅ Codex 已完成', 'failed': '⚠️ Codex 失败',
                                 'interrupted': '⏹ Codex 已中断', 'attention': '💬 Codex 需要你回答',
                                 'error': '⚠️ Codex 错误'}.get(e['kind'], 'Codex')
                        self.sender.send_card(chat_id, title, f"会话：{e['thread_id']}\n\n{e['text'][:10000]}")
                        # Acknowledge only after Feishu accepts the card (at-least-once delivery).
                        call('ack', {'channel': channel(chat_id), 'thread_id': e['thread_id'], 'seq': e['seq']})
                except Exception as exc:
                    log.debug('Notification polling: %s', type(exc).__name__)
            self.stop_event.wait(4)
