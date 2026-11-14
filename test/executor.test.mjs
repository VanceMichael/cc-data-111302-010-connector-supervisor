import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm, readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {runAdapter, REASON, PROTOCOL_VERSION} from '../src/executor.mjs';

const projectRoot = new URL('..', import.meta.url);
const adapter = name => new URL(`adapters/${name}.mjs`, projectRoot).pathname;
const testAdapter = name => new URL(`adapters/${name}.mjs`, new URL('.', import.meta.url)).pathname;
const raw = {items: [{id: 'a', name: '甲'}, {id: 'b', name: '乙'}]};

async function withWorkDir() {
  const dir = await mkdtemp(join(tmpdir(), 'executor-'));
  return {dir, cleanup: () => rm(dir, {recursive: true, force: true})};
}

async function assertProcessGroupGone(pid) {
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline) {
    let gone = false;
    try {
      process.kill(-pid, 0);
    } catch (error) {
      gone = error.code === 'ESRCH';
    }
    if (gone) return;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  assert.fail(`进程组 ${pid} 在取消后仍然存活`);
}

test('正常适配器：记录通过校验，诊断与业务分离，指标完整', async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const result = await runAdapter({
      adapterPath: adapter('normal'),
      raw,
      protocol: PROTOCOL_VERSION,
      cwd: dir,
      budgets: {timeoutMs: 5000},
    });
    assert.equal(result.ok, true, result.message);
    assert.equal(result.records.length, 2);
    assert.deepEqual(result.records[0], {id: 'a', name: '甲'});
    assert.equal(result.diagnostics, '');
    assert.equal(result.metrics.exitCode, 0);
    assert.ok(result.metrics.durationMs < 5000);
    assert.ok(result.metrics.stdoutBytes > 0);
    assert.equal(result.metrics.recordCount, 2);
  } finally {
    await cleanup();
  }
});

test('睡眠适配器超出时间预算：明确 timeout，子进程被终止', {timeout: 15000}, async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const started = Date.now();
    const result = await runAdapter({
      adapterPath: adapter('sleep'),
      raw: {items: []},
      cwd: dir,
      budgets: {timeoutMs: 300},
    });
    assert.equal(result.ok, false);
    assert.equal(result.reason, REASON.TIMEOUT);
    assert.ok(Date.now() - started < 4000);
    assert.notEqual(result.metrics.signal, null);
    await assertProcessGroupGone(result.metrics.pid);
  } finally {
    await cleanup();
  }
});

test('无终止计算同样受时间预算约束，不拖垮调用方', {timeout: 15000}, async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const result = await runAdapter({
      adapterPath: adapter('endless'),
      raw: {items: []},
      cwd: dir,
      budgets: {timeoutMs: 300},
    });
    assert.equal(result.ok, false);
    assert.equal(result.reason, REASON.TIMEOUT);
    await assertProcessGroupGone(result.metrics.pid);
  } finally {
    await cleanup();
  }
});

test('超量诊断输出：diagnostics-over-budget 且只保留预算内有限片段', {timeout: 15000}, async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const result = await runAdapter({
      adapterPath: adapter('logs'),
      raw: {items: []},
      cwd: dir,
      budgets: {timeoutMs: 4000, stderrBytes: 4096},
    });
    assert.equal(result.ok, false);
    assert.equal(result.reason, REASON.DIAGNOSTICS_OVER_BUDGET);
    assert.ok(Buffer.byteLength(result.diagnostics) <= 4096 + 64);
    assert.match(result.diagnostics, /诊断/);
    // stderr 内容绝不进入业务记录
    assert.deepEqual(result.records, []);
  } finally {
    await cleanup();
  }
});

test('半条记录退出：partial-record 失败，不返回残缺记录', async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const result = await runAdapter({
      adapterPath: adapter('partial'),
      raw: {items: []},
      cwd: dir,
      budgets: {timeoutMs: 2000},
    });
    assert.equal(result.ok, false);
    assert.equal(result.reason, REASON.PARTIAL_RECORD);
    assert.deepEqual(result.records, []);
  } finally {
    await cleanup();
  }
});

test('调试文本混入 stdout：protocol-violation，业务通道拒绝非协议字节', async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const result = await runAdapter({
      adapterPath: testAdapter('garbage'),
      raw: {items: []},
      cwd: dir,
      budgets: {timeoutMs: 2000},
    });
    assert.equal(result.ok, false);
    assert.equal(result.reason, REASON.PROTOCOL_VIOLATION);
  } finally {
    await cleanup();
  }
});

test('end.count 与记录数不一致：protocol-violation', async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const result = await runAdapter({
      adapterPath: testAdapter('badcount'),
      raw,
      cwd: dir,
      budgets: {timeoutMs: 2000},
    });
    assert.equal(result.ok, false);
    assert.equal(result.reason, REASON.PROTOCOL_VIOLATION);
    assert.match(result.message, /end\.count/);
  } finally {
    await cleanup();
  }
});

test('输出超量：output-over-budget，适配器被终止', {timeout: 15000}, async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const result = await runAdapter({
      adapterPath: adapter('normal'),
      raw: {items: new Array(5000).fill(0).map((_, index) => ({id: String(index), name: 'x'.repeat(200)}))},
      cwd: dir,
      budgets: {timeoutMs: 5000, stdoutBytes: 1024},
    });
    assert.equal(result.ok, false);
    assert.equal(result.reason, REASON.OUTPUT_OVER_BUDGET);
    await assertProcessGroupGone(result.metrics.pid);
  } finally {
    await cleanup();
  }
});

test('取消执行：AbortSignal 触发后整个进程组（含孙进程）被清理', {timeout: 20000}, async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const controller = new AbortController();
    const pending = runAdapter({
      adapterPath: adapter('child'),
      raw: {items: []},
      cwd: dir,
      budgets: {timeoutMs: 10000},
      signal: controller.signal,
    });
    await new Promise(resolve => setTimeout(resolve, 400));
    controller.abort();
    const result = await pending;
    assert.equal(result.ok, false);
    assert.equal(result.reason, REASON.CANCELED);
    // 孙进程 pid 由适配器写到 stderr，验证它随进程组一起消失
    const grandchildPid = Number(JSON.parse(result.diagnostics.trim()).childPid);
    assert.ok(grandchildPid > 0);
    await assertProcessGroupGone(result.metrics.pid);
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      let gone = false;
      try {
        process.kill(grandchildPid, 0);
      } catch (error) {
        gone = error.code === 'ESRCH';
      }
      if (gone) break;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    let grandchildGone = false;
    try {
      process.kill(grandchildPid, 0);
    } catch (error) {
      grandchildGone = error.code === 'ESRCH';
    }
    assert.ok(grandchildGone, `孙进程 ${grandchildPid} 未被清理`);
  } finally {
    await cleanup();
  }
});

test('信号在 spawn 之前已触发：仍判定 canceled 且进程组被回收', async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const controller = new AbortController();
    controller.abort();
    const result = await runAdapter({
      adapterPath: adapter('sleep'),
      raw: {items: []},
      cwd: dir,
      budgets: {timeoutMs: 10000},
      signal: controller.signal,
    });
    assert.equal(result.ok, false);
    assert.equal(result.reason, REASON.CANCELED);
    if (result.metrics.pid) await assertProcessGroupGone(result.metrics.pid);
  } finally {
    await cleanup();
  }
});

test('内存预算：持续分配的适配器被判定 memory-limit', {timeout: 30000}, async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const result = await runAdapter({
      adapterPath: testAdapter('memory'),
      raw: {items: []},
      cwd: dir,
      budgets: {timeoutMs: 20000, memoryMb: 32, stderrBytes: 256 * 1024},
    });
    assert.equal(result.ok, false);
    assert.equal(result.reason, REASON.MEMORY_LIMIT);
  } finally {
    await cleanup();
  }
});

test('输入侧协议校验：不支持的版本与畸形信封直接拒绝，不启动子进程', async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const badVersion = await runAdapter({
      adapterPath: adapter('normal'),
      raw,
      protocol: 99,
      cwd: dir,
    });
    assert.equal(badVersion.ok, false);
    assert.equal(badVersion.reason, REASON.UNSUPPORTED_PROTOCOL);
    assert.deepEqual(badVersion.metrics, {});

    const badInput = await runAdapter({
      adapterPath: adapter('normal'),
      raw: null,
      protocol: PROTOCOL_VERSION,
      cwd: dir,
    });
    assert.equal(badInput.ok, false);
    assert.equal(badInput.reason, REASON.INPUT_REJECTED);
  } finally {
    await cleanup();
  }
});

test('多个适配器并发执行：一个超时不影响健康来源的作业', {timeout: 20000}, async () => {
  const first = await withWorkDir();
  const second = await withWorkDir();
  try {
    const stuck = runAdapter({
      adapterPath: adapter('sleep'),
      raw: {items: []},
      cwd: first.dir,
      budgets: {timeoutMs: 300},
    });
    // 与卡住的调用并发发起健康调用
    const healthy = await runAdapter({
      adapterPath: adapter('normal'),
      raw,
      cwd: second.dir,
      budgets: {timeoutMs: 5000},
    });
    assert.equal(healthy.ok, true);
    assert.equal(healthy.records.length, 2);
    const stuckResult = await stuck;
    assert.equal(stuckResult.ok, false);
    assert.equal(stuckResult.reason, REASON.TIMEOUT);
  } finally {
    await Promise.all([first.cleanup(), second.cleanup()]);
  }
});

test('每次调用使用独立工作目录，适配器在 cwd 内的产物互不干扰', async () => {
  const {dir, cleanup} = await withWorkDir();
  try {
    const result = await runAdapter({
      adapterPath: testAdapter('tempfile'),
      raw: {items: []},
      cwd: dir,
      budgets: {timeoutMs: 500},
    });
    assert.equal(result.reason, REASON.TIMEOUT);
    const files = await readdir(dir);
    assert.ok(files.includes('job-artifact.tmp'));
  } finally {
    await cleanup();
  }
});
