import {spawn} from 'node:child_process';
import {readFileSync} from 'node:fs';

/**
 * 适配器隔离执行器。
 *
 * 边界：每次调用都是一个独立的 Node.js 子进程（独立进程组），
 * 主进程绝不 import/require 合作方代码，只通过 stdin/stdout/stderr
 * 三条管道与适配器交换字节，并在时间、标准输出、诊断输出、内存四个
 * 维度上施加硬预算。任何预算或协议问题都会以明确 reason 结束调用，
 * 成功与否只由本模块返回，调用方据此决定是否发布结果。
 */

export const PROTOCOL_VERSION = 1;
export const SUPPORTED_PROTOCOLS = new Set([PROTOCOL_VERSION]);

export const DEFAULT_BUDGETS = {
  timeoutMs: 5000,
  stdoutBytes: 4 * 1024 * 1024,
  stderrBytes: 128 * 1024,
  memoryMb: 128,
};

/** 调用受限/失败的原因码，运维与文档都引用这一份枚举。 */
export const REASON = {
  INPUT_REJECTED: 'input-rejected',
  UNSUPPORTED_PROTOCOL: 'unsupported-protocol',
  ADAPTER_LAUNCH_FAILED: 'adapter-launch-failed',
  TIMEOUT: 'timeout',
  OUTPUT_OVER_BUDGET: 'output-over-budget',
  DIAGNOSTICS_OVER_BUDGET: 'diagnostics-over-budget',
  MEMORY_LIMIT: 'memory-limit',
  PROTOCOL_VIOLATION: 'protocol-violation',
  PARTIAL_RECORD: 'partial-record',
  NONZERO_EXIT: 'nonzero-exit',
  CANCELED: 'canceled',
  EXECUTOR_ERROR: 'executor-error',
};

const TERM_GRACE_MS = 500;
const RSS_POLL_MS = 100;
const TRUNCATION_MARKER = '\n…[诊断输出已按预算截断]';

/** 输入侧协议校验：合作方声明的协议版本必须在允许清单内，原文必须是对象。 */
export function validateEnvelope(envelope) {
  if (envelope === null || typeof envelope !== 'object' || Array.isArray(envelope)) {
    return {ok: false, reason: REASON.INPUT_REJECTED, message: '输入信封必须是 JSON 对象'};
  }
  if (!Number.isInteger(envelope.protocol) || !SUPPORTED_PROTOCOLS.has(envelope.protocol)) {
    return {
      ok: false,
      reason: REASON.UNSUPPORTED_PROTOCOL,
      message: `不支持的协议版本: ${String(envelope.protocol)}`,
    };
  }
  if (envelope.input === null || typeof envelope.input !== 'object') {
    return {ok: false, reason: REASON.INPUT_REJECTED, message: 'input 字段必须是结构化对象'};
  }
  return {ok: true};
}

/** 输出侧单条消息校验，record 累积进 state，end 记录期望条数。 */
function validateMessage(message, state) {
  if (message === null || typeof message !== 'object' || Array.isArray(message)) {
    return '协议消息必须是 JSON 对象';
  }
  if (message.type === 'record') {
    if (!Object.hasOwn(message, 'value')) return 'record 消息缺少 value 字段';
    if (state.ended) return 'end 之后仍出现 record 消息';
    state.records.push(message.value);
    return null;
  }
  if (message.type === 'end') {
    if (state.ended) return '出现重复的 end 消息';
    if (!Number.isInteger(message.count) || message.count < 0) {
      return 'end.count 必须是非负整数';
    }
    state.ended = true;
    state.expectedCount = message.count;
    return null;
  }
  return `不支持的消息类型: ${String(message.type)}`;
}

function failure(reason, details) {
  return {ok: false, reason, records: [], diagnostics: '', metrics: {}, ...details};
}

/**
 * 在独立子进程中执行一次适配器。
 *
 * @param {string} adapterPath 适配器脚本绝对路径
 * @param {object} raw 原始响应（已解析的结构化对象）
 * @param {number} protocol 合作方声明的协议版本
 * @param {object} budgets 本次调用预算
 * @param {string} cwd 本次调用专属工作目录（适配器相对路径临时文件落在这里）
 * @param {AbortSignal} [signal] 取消信号
 */
export async function runAdapter({adapterPath, raw, protocol = PROTOCOL_VERSION, budgets = {}, cwd, signal}) {
  const budget = {...DEFAULT_BUDGETS, ...budgets};
  const envelopeCheck = validateEnvelope({protocol, input: raw});
  if (!envelopeCheck.ok) {
    return failure(envelopeCheck.reason, {message: envelopeCheck.message});
  }

  const state = {
    records: [],
    ended: false,
    expectedCount: null,
    remainder: '',
    stdoutBytes: 0,
    stderrBytes: 0,
    diagnostics: '',
  };
  const startedAt = Date.now();
  let tripReason = null;
  let tripMessage = '';
  let killScheduled = false;
  let child;
  let killTree = () => {};

  // 取消监听必须在 spawn 之前注册：否则取消信号落在 spawn 与监听注册之间会丢失，
  // 子进程将一直跑到时间预算才结束（曾表现为取消用例偶发失败）。
  if (signal) {
    if (signal.aborted) {
      tripReason = REASON.CANCELED;
      tripMessage = '调用已被取消';
    } else {
      signal.addEventListener(
        'abort',
        () => {
          tripReason = REASON.CANCELED;
          tripMessage = '调用已被取消';
          killTree();
        },
        {once: true},
      );
    }
  }

  try {
    child = spawn(
      process.execPath,
      // V8 老生代硬上限：适配器自身的 JS 堆增长到阈值即被运行时中止。
      [`--max-old-space-size=${budget.memoryMb}`, adapterPath],
      {
        cwd,
        detached: true, // 子进程成为新进程组组长，便于连同孙进程一起清理
        stdio: ['pipe', 'pipe', 'pipe'],
        // 清空 NODE_OPTIONS，避免外部注入的参数削弱内存预算；同 UID 环境本身不是信任边界。
        env: {...process.env, NODE_OPTIONS: ''},
      },
    );
  } catch (error) {
    return failure(REASON.ADAPTER_LAUNCH_FAILED, {message: error.message});
  }

  const timers = [];
  const later = (callback, ms) => {
    const handle = setTimeout(callback, ms);
    handle.unref?.();
    timers.push(handle);
    return handle;
  };
  const clearTimers = () => timers.forEach(handle => clearTimeout(handle));

  killTree = () => {
    if (killScheduled) return;
    killScheduled = true;
    // 负 pid 表示向整个进程组发信号，适配器派生的孙进程同组会一并收到。
    try { process.kill(-child.pid, 'SIGTERM'); } catch {}
    later(() => {
      try { process.kill(-child.pid, 'SIGKILL'); } catch {}
    }, TERM_GRACE_MS);
  };

  /** 第一个触发的受限原因优先，随后的异常不再覆盖它。 */
  const trip = (reason, message = '') => {
    if (!tripReason) {
      tripReason = reason;
      tripMessage = message;
    }
    killTree();
  };

  later(() => trip(REASON.TIMEOUT, `超过 ${budget.timeoutMs}ms 时间预算`), budget.timeoutMs);

  // spawn 之前信号就已触发：补上 killTree 调用（监听器注册时 child 尚不存在）。
  if (signal?.aborted) killTree();

  // RSS 轮询是 V8 堆上限之外的兜底：原生内存、Buffer 等不受老生代限制。
  const rssLimitKb = budget.memoryMb * 2 * 1024;
  const rssTimer = setInterval(() => {
    try {
      const status = readFileSync(`/proc/${child.pid}/status`, 'utf8');
      const match = status.match(/VmRSS:\s+(\d+)\s+kB/);
      if (match && Number(match[1]) > rssLimitKb) {
        trip(REASON.MEMORY_LIMIT, `常驻内存超过 ${budget.memoryMb * 2}MB 兜底预算`);
      }
    } catch {}
  }, RSS_POLL_MS);
  rssTimer.unref();
  timers.push(rssTimer);

  child.stdout.on('data', chunk => {
    state.stdoutBytes += chunk.length;
    if (state.stdoutBytes > budget.stdoutBytes) {
      trip(REASON.OUTPUT_OVER_BUDGET, `标准输出超过 ${budget.stdoutBytes} 字节预算`);
      return;
    }
    state.remainder += chunk.toString('utf8');
    let newline;
    while ((newline = state.remainder.indexOf('\n')) >= 0) {
      const line = state.remainder.slice(0, newline);
      state.remainder = state.remainder.slice(newline + 1);
      if (line.length === 0) {
        trip(REASON.PROTOCOL_VIOLATION, '标准输出出现空行，不符合 JSON Lines 协议');
        return;
      }
      let message;
      try {
        message = JSON.parse(line);
      } catch {
        trip(REASON.PROTOCOL_VIOLATION, '标准输出存在无法解析为 JSON 的行（调试文本混入业务通道）');
        return;
      }
      const error = validateMessage(message, state);
      if (error) {
        trip(REASON.PROTOCOL_VIOLATION, error);
        return;
      }
    }
  });
  child.stdout.on('error', () => {});

  child.stderr.on('data', chunk => {
    state.stderrBytes += chunk.length;
    if (state.stderrBytes > budget.stderrBytes) {
      trip(REASON.DIAGNOSTICS_OVER_BUDGET, `诊断输出超过 ${budget.stderrBytes} 字节预算`);
      return;
    }
    state.diagnostics += chunk.toString('utf8');
  });
  child.stderr.on('error', () => {});

  child.stdin.on('error', () => {}); // 适配器提前退出时 EPIPE 不应打穿主进程
  child.stdin.end(JSON.stringify({protocol, input: raw}));

  let launchError = '';
  child.on('error', error => {
    launchError = error.message;
    trip(REASON.ADAPTER_LAUNCH_FAILED, error.message);
  });

  const {code, signal: exitSignal} = await new Promise(resolve => {
    child.on('close', (exitCode, sig) => resolve({code: exitCode, signal: sig}));
  });
  clearTimers();

  const durationMs = Date.now() - startedAt;
  const metrics = {
    pid: child.pid,
    durationMs,
    exitCode: code,
    signal: exitSignal,
    stdoutBytes: state.stdoutBytes,
    stderrBytes: state.stderrBytes,
    recordCount: state.records.length,
  };

  // 诊断输出与业务结果严格分离：仅保留预算内的有限片段用于排查。
  let diagnostics = state.diagnostics;
  if (state.stderrBytes > budget.stderrBytes) {
    diagnostics = diagnostics.slice(0, budget.stderrBytes) + TRUNCATION_MARKER;
  }

  const base = {records: state.records, diagnostics, metrics};

  if (tripReason) {
    return failure(tripReason, {...base, message: tripMessage});
  }
  // V8 堆耗尽会以 SIGABRT 中止并在 stderr 留下固定措辞。
  if (exitSignal === 'SIGABRT' && diagnostics.includes('heap out of memory')) {
    return failure(REASON.MEMORY_LIMIT, {...base, message: '适配器 JS 堆超过内存预算被运行时中止'});
  }
  if (state.remainder.trim().length > 0) {
    return failure(REASON.PARTIAL_RECORD, {
      ...base,
      message: '进程退出时标准输出残留半条没有换行结束的记录',
    });
  }
  if (!state.ended) {
    return failure(REASON.PROTOCOL_VIOLATION, {...base, message: '缺少结束消息 {type:"end"}'});
  }
  if (state.expectedCount !== state.records.length) {
    return failure(REASON.PROTOCOL_VIOLATION, {
      ...base,
      message: `end.count=${state.expectedCount} 与实际记录数 ${state.records.length} 不一致`,
    });
  }
  if (code !== 0) {
    return failure(REASON.NONZERO_EXIT, {...base, message: `适配器以退出码 ${code} 结束`});
  }
  if (launchError) {
    return failure(REASON.ADAPTER_LAUNCH_FAILED, {...base, message: launchError});
  }

  return {ok: true, reason: null, message: '', ...base};
}
