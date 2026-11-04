/**
 * dsh-approval-feishu - 把 DSH 的审批请求搬到飞书的宿主应答者。
 *
 * 为什么需要它：`approval/request` 是宿主进程内部的 cordis 瀑布事件，官方 Web UI
 * 用自己的通道挂了一个应答者，所以审批卡只出现在本机浏览器里；飞书桥只订阅会话
 * 日志，看不到审批。本插件作为另一个应答者插进那条瀑布，把待办写成约定格式的
 * 文件，等飞书桥回执后返回裁决。
 *
 * 谁能接管（渠道归属戳）：只接管 <pipeDir>/channels/<sessionId> 这个戳文件存在
 * 的会话（由飞书桥在每次发消息前盖章）。其它会话一律 next() 让给 Web UI，否则
 * 两边各弹一张卡，用户点哪边都只是半张脸。
 *
 * 安全默认值（fail-closed）：超时、桥没回、文件写坏、任何异常，一律 rejected。
 * 不存在"无人应答反而放行"的路径。
 *
 * 协议（目录由 config.pipeDir 指定）：
 *   channels/<sessionId>  桥盖章：这个会话的审批归飞书管
 *   pending/<id>.json     插件写的请求（原子改名落盘）
 *   claim/<id>.json       取得唯一裁决权的凭据（存在即有人接了）
 *   decisions/<id>.json   桥写的裁决 {outcome, by, decidedAt}
 *   log.jsonl             审计流水
 *
 * wire（与飞书桥共享，字段名不要改）：
 *   请求 {"type":"approval-request","id","toolName","reason","callId","sessionId","createdAt","timeoutSeconds"}
 *   裁决 {"type":"approval-decision","id","outcome","by","decidedAt"}
 *
 * 兼容性：自带 apply(ctx) 与 ctx.on，不 import 任何 @deepseek-ai/*，只用 node:
 * 内建。这样 DSH 升级后本插件不需要跟着重新对齐（桥当初踩过这个坑）。
 */

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";

// 顶层留痕：模块一旦被 import 就写一行，用于区分"模块没加载"与"apply 里失败"。
try {
  fs.appendFileSync(
    path.join(process.env.USERPROFILE || process.env.HOME || ".", ".dsh", "approvals", "_probe", "plugin-eval.log"),
    new Date().toISOString() + " index.js evaluated\n",
  );
} catch {
  /* 留痕失败不能影响加载 */
}

// 不声明 inject：官方 dsh-acp 也是直接在 apply 里 ctx.on("approval/request")，不声明 inject。

const NAME = "approval-feishu";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const iso = (t = Date.now()) => new Date(t).toISOString();
const stamp = (t = Date.now()) => new Date(t).toISOString().replace(/[-:.]/g, "");

function readConfig(raw = {}) {
  const home = process.env.USERPROFILE || process.env.HOME || ".";
  const pipeDir = String(
    raw.pipeDir || process.env.DSH_APPROVAL_PIPE_DIR || path.join(home, ".dsh", "approvals"),
  ).trim();
  const num = (value, fallback) => {
    const n = Number(value);
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
  };
  return {
    pipeDir,
    timeoutSeconds: num(raw.timeoutSeconds, 300),
    pollMs: Math.max(100, num(raw.pollMs, 1000)),
    logShapes: raw.logShapes === true || raw.logShapes === "true",
    // 抢到裁决权后延迟这么久才写 pending 文件（= 延迟通知飞书）。
    // 用途：DSH 收到拒绝后还会做一次升权重试，那个审批请求晚几秒才出现；
    // 若用户抢在它出现之前就答复，会打到一个还不存在的编号上。
    announceDelayMs: Math.max(0, num(raw.announceDelayMs, 0)),
  };
}

/** 原子写：同目录临时文件 + rename，读者看不到半截 JSON。 */
async function writeJsonAtomic(target, value) {
  const dir = path.dirname(target);
  await fsp.mkdir(dir, { recursive: true });
  const tmp = path.join(dir, "." + path.basename(target) + "." + process.pid + "." + stamp() + ".tmp");
  await fsp.writeFile(tmp, JSON.stringify(value, null, 2), "utf8");
  await fsp.rename(tmp, target);
  return target;
}

/**
 * 独占创建（wx / O_EXCL）。**不能**用 rename 代替：Windows 的 rename 会覆盖已
 * 存在的目标文件，那样"抢裁决权"就失效了 —— 飞书与 Web UI 会各拿一次裁决权，
 * 用户看到两张卡，而且两边都能各自放行/拒绝，等于双开。
 */
async function writeJsonExclusive(target, value) {
  const dir = path.dirname(target);
  await fsp.mkdir(dir, { recursive: true });
  const handle = await fsp.open(target, "wx");
  try {
    await handle.writeFile(JSON.stringify(value, null, 2), "utf8");
  } finally {
    await handle.close();
  }
  return target;
}

/** 审计流水；写不进去不能影响审批本身。 */
async function appendAudit(file, record) {
  try {
    await fsp.mkdir(path.dirname(file), { recursive: true });
    await fsp.appendFile(file, JSON.stringify({ at: iso(), ...record }) + "\n", "utf8");
  } catch {
    /* ignore */
  }
}

/** 安全刮取会话信息；任何字段缺失都不许抛。 */
export function describeSession(agent) {
  const out = { sessionId: "", cwd: "" };
  try {
    const session = agent && agent.session;
    if (session && session.id !== undefined) out.sessionId = String(session.id);
    const cwd = (session && session.cwd) || (session && session.meta && session.meta.cwd);
    if (cwd) out.cwd = String(cwd);
  } catch {
    /* ignore */
  }
  return out;
}

/** 只接管盖过戳的会话。 */
export function ownsSession(pipeDir, sessionId) {
  if (!sessionId) return false;
  return fs.existsSync(path.join(pipeDir, "channels", sessionId));
}

/**
 * 等一份裁决。
 * @returns {Promise<{outcome:"allowed-once"|"rejected", by:string, waitedMs:number}>}
 */
export async function waitForDecision({
  pendingFile,
  claimFile,
  decisionFile,
  timeoutSeconds,
  pollMs,
  signal,
  now = Date.now,
  sleepImpl = sleep,
}) {
  const startedAt = now();
  const deadline = startedAt + timeoutSeconds * 1000;

  const claim = async () => {
    try {
      await writeJsonExclusive(claimFile, { type: "approval-claim", at: iso(now()) });
      return true;
    } catch {
      return false;
    }
  };
  const readDecision = async () => {
    try {
      const raw = JSON.parse(await fsp.readFile(decisionFile, "utf8"));
      return {
        outcome: raw && raw.outcome === "allowed-once" ? "allowed-once" : "rejected",
        by: String((raw && raw.by) || "unknown"),
      };
    } catch {
      return null;
    }
  };

  // 阶段 1：抢裁决权。
  //   · 进循环先读裁决（裁决可能在 sleep 期间到达，顺序反了会把"已批准"误判成超时）
  //   · 抢不到且裁决权已被别人持有 → 别的应答者接手了（Web UI 常驻应答者会先接），
  //     直接让出，不空转到超时
  //   · 抢不到但文件不存在 → 真的 I/O 故障，抛出去让 apply() catch 成 rejected
  const early = await readDecision();
  if (early) return { ...early, waitedMs: now() - startedAt };
  if (signal && signal.aborted) return { outcome: "rejected", by: "cancelled", waitedMs: now() - startedAt };
  if (!(await claim())) {
    if (fs.existsSync(claimFile)) return { outcome: "rejected", by: "another-answerer", waitedMs: now() - startedAt };
    throw new Error("cannot claim approval: " + claimFile);
  }

  // pending 必须保留到裁决结束，桥通过它发现并核对审批归属。

  // 阶段 2：轮询裁决（同样先读后睡，避免把 sleep 期间到达的裁决漏过去）
  while (now() < deadline) {
    const decision = await readDecision();
    if (decision) {
      await fsp.unlink(claimFile).catch(() => {});
      return { ...decision, waitedMs: now() - startedAt };
    }
    if (signal && signal.aborted) {
      await fsp.unlink(claimFile).catch(() => {});
      return { outcome: "rejected", by: "cancelled", waitedMs: now() - startedAt };
    }
    await sleepImpl(pollMs);
  }
  // 睡醒后可能刚好过了 deadline，再补一次读数，别把已到的裁决丢掉
  const lastChance = await readDecision();
  if (lastChance) {
    await fsp.unlink(claimFile).catch(() => {});
    return { ...lastChance, waitedMs: now() - startedAt };
  }

  // 阶段 3：超时 → 撤回裁决权，返回 rejected（fail-closed）
  await fsp.unlink(claimFile).catch(() => {});
  return { outcome: "rejected", by: "timeout", waitedMs: now() - startedAt };
}

export function apply(ctx, rawConfig = {}) {
  try {
    applyInner(ctx, rawConfig);
  } catch (exc) {
    try {
      fs.appendFileSync(
        path.join(process.env.USERPROFILE || process.env.HOME || ".", ".dsh", "approvals", "_probe", "plugin-errors.log"),
        new Date().toISOString() + " apply failed: " + ((exc && exc.stack) || exc) + "\n",
      );
    } catch {
      /* ignore */
    }
    throw exc;
  }
}

function applyInner(ctx, rawConfig = {}) {
  // 进函数第一件事就留痕：如果这行没出现，说明 apply 根本没被调用（而不是里面失败）。
  try {
    fs.appendFileSync(
      path.join(process.env.USERPROFILE || process.env.HOME || ".", ".dsh", "approvals", "_probe", "plugin-apply.log"),
      new Date().toISOString() + " apply called\n",
    );
  } catch {
    /* ignore */
  }
  const cfg = readConfig(rawConfig);
  const pendingDir = path.join(cfg.pipeDir, "pending");
  const claimDir = path.join(cfg.pipeDir, "claim");
  const decisionsDir = path.join(cfg.pipeDir, "decisions");
  const channelsDir = path.join(cfg.pipeDir, "channels");
  const auditFile = path.join(cfg.pipeDir, "log.jsonl");
  const _probeDir = path.join(process.env.USERPROFILE || process.env.HOME || ".", ".dsh", "approvals", "_probe");
  for (const dir of [pendingDir, claimDir, decisionsDir, channelsDir]) {
    let note = "mkdir+touch OK";
    try {
      fs.mkdirSync(dir, { recursive: true });
      // 真正试写一次：mkdir 成功不代表有写权限（宿主可能对外写有约束）
      const probeFile = path.join(dir, ".write-probe");
      fs.appendFileSync(probeFile, new Date().toISOString() + "\n");
      fs.unlinkSync(probeFile);
    } catch (exc) {
      note = "FAILED: " + exc.code + " " + exc.message;
    }
    try {
      fs.appendFileSync(path.join(_probeDir, "plugin-write-test.log"), new Date().toISOString() + " " + dir + " -> " + note + "\n");
    } catch {
      /* ignore */
    }
    if (note.startsWith("FAILED")) throw new Error("pipe dir not writable: " + dir + " (" + note + ")");
  }
  if (ctx && ctx.logger && ctx.logger.info) {
    ctx.logger.info("[%s] mounted: pipeDir=%s timeout=%ss poll=%sms", NAME, cfg.pipeDir, cfg.timeoutSeconds, cfg.pollMs);
  }

  if (typeof ctx.on !== "function") {
    throw new Error("ctx.on 不可用（cordis 版本不兼容？）");
  }
  ctx.on("approval/request", async (request, next) => {
    let id = "";
    try {
      const info = describeSession(request && request.agent);
      if (!ownsSession(cfg.pipeDir, info.sessionId)) return next();

      const callId = request && request.callId !== undefined ? String(request.callId) : "";
      const toolName = request && request.toolName ? String(request.toolName) : "unknown";
      const seed = info.sessionId + "|" + callId + "|" + toolName + "|" + Date.now() + "|" + crypto.randomUUID();
      id = crypto.createHash("sha256").update(seed).digest("hex").slice(0, 8);
      const pendingFile = path.join(pendingDir, id + ".json");
      const claimFile = path.join(claimDir, id + ".json");
      const decisionFile = path.join(decisionsDir, id + ".json");

      const record = {
        type: "approval-request",
        id,
        toolName,
        reason: request && request.reason ? String(request.reason) : "",
        callId,
        sessionId: info.sessionId,
        cwd: info.cwd,
        createdAt: iso(),
        timeoutSeconds: cfg.timeoutSeconds,
      };
      await writeJsonAtomic(pendingFile, record);
      await appendAudit(auditFile, { id, event: "asked", toolName, callId, sessionId: info.sessionId });

      if (cfg.logShapes) {
        const session = request && request.agent && request.agent.session;
        await appendAudit(auditFile, {
          id,
          event: "shape",
          sessionKeys: session ? Object.keys(session).slice(0, 40) : null,
          metaKeys: session && session.meta ? Object.keys(session.meta).slice(0, 40) : null,
        });
      }

      if (cfg.announceDelayMs > 0) await sleep(cfg.announceDelayMs);

      const decision = await waitForDecision({
        pendingFile,
        claimFile,
        decisionFile,
        timeoutSeconds: cfg.timeoutSeconds,
        pollMs: cfg.pollMs,
        signal: request && request.signal,
      });
      await fsp.unlink(pendingFile).catch(() => {});
      await fsp.unlink(claimFile).catch(() => {});
      await fsp.unlink(decisionFile).catch(() => {});
      await appendAudit(auditFile, {
        id, event: "result", outcome: decision.outcome, by: decision.by, waitedMs: decision.waitedMs,
      });
      if (ctx && ctx.logger && ctx.logger.info) {
        ctx.logger.info("[%s] approval %s -> %s (%s, %dms)", NAME, id, decision.outcome, decision.by, decision.waitedMs);
      }
      return decision.outcome;
    } catch (exc) {
      await appendAudit(auditFile, { id, event: "error", message: String((exc && exc.message) || exc) });
      if (ctx && ctx.logger && ctx.logger.warn) {
        ctx.logger.warn("[%s] answerer crashed, treating as rejected: %s", NAME, exc);
      }
      return "rejected";
    }
  }, { prepend: true });
}

export const name = NAME;
export { readConfig };
export default { name: NAME, apply };
