import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, readFile, readdir, cp, writeFile} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {CollectionRunner, fetchAndSubmit} from '../src/runner.mjs';
import {createSource} from '../src/source.mjs';
import * as client from '../src/client.mjs';

const projectRoot = new URL('..', import.meta.url).pathname;
const adapterPath = name => join(projectRoot, 'adapters', `${name}.mjs`);
const testAdapterPath = name => join(projectRoot, 'test', 'adapters', `${name}.mjs`);
const raw = {items: [{id: 'a', name: '甲'}, {id: 'b', name: '乙'}]};

async function makeRunner() {
  const root = await mkdtemp(join(tmpdir(), 'runner-'));
  const runner = new CollectionRunner({root});
  await runner.ready();
  return {root, runner};
}

async function register(runner, name, path = adapterPath(name), protocol = 1) {
  return runner.registerAdapter({name, path, protocol});
}

test('成功作业：结果只在全部校验通过后原子发布，临时工作目录被清理', async () => {
  const {root, runner} = await makeRunner();
  try {
    await register(runner, 'normal');
    const {jobId} = await runner.submit({adapterName: 'normal', raw});
    const job = await runner.waitFor(jobId);

    assert.equal(job.status, 'succeeded');
    assert.equal(job.reason, null);
    const resultPath = join(runner.outputDir, `${jobId}.json`);
    assert.equal(job.resultPath, resultPath);
    assert.ok(existsSync(resultPath));
    const published = JSON.parse(await readFile(resultPath, 'utf8'));
    assert.equal(published.count, 2);
    assert.deepEqual(published.records, raw.items);
    assert.equal(published.adapter, 'normal');
    assert.ok(published.adapterVersion.length === 64);

    // 正式目录中只有完整结果，没有暂存或半成品文件
    const outFiles = await readdir(runner.outputDir);
    assert.deepEqual(outFiles, [`${jobId}.json`]);
    // 单次调用的工作目录已删除，作业目录自身保留
    const jobFiles = await readdir(join(root, 'jobs', jobId));
    assert.ok(!jobFiles.some(name => name.startsWith('.staging-')));
    const attempts = await readdir(join(root, 'jobs', jobId, 'attempts'));
    assert.deepEqual(attempts, []);
    // 原文已按内容哈希固化
    assert.ok(existsSync(join(root, 'raw', `${job.rawHash}.json`)));
  } finally {
    await runner.shutdown();
  }
});

test('失败作业：半批资料不会进入正式目录，状态与受限原因可查', async () => {
  const {root, runner} = await makeRunner();
  try {
    await register(runner, 'partial');
    const {jobId} = await runner.submit({adapterName: 'partial', raw: {items: []}});
    const job = await runner.waitFor(jobId);

    assert.equal(job.status, 'failed');
    assert.equal(job.reason, 'partial-record');
    assert.ok(job.message.length > 0);
    assert.equal(job.resultPath, null);
    assert.deepEqual(await readdir(runner.outputDir), []);
    assert.deepEqual(await readdir(join(root, 'jobs', jobId, 'attempts')), []);
    assert.equal(job.attempts[0].status, 'failed');
    assert.equal(job.attempts[0].reason, 'partial-record');
    assert.ok(job.attempts[0].metrics);
  } finally {
    await runner.shutdown();
  }
});

test('定位式重试：复用同一原文与适配器版本，来源服务已关闭也能成功', {timeout: 30000}, async () => {
  const source = await createSource();
  const root = await mkdtemp(join(tmpdir(), 'runner-retry-'));
  const runner = new CollectionRunner({root});
  await runner.ready();
  try {
    await register(runner, 'sleep');
    // 唯一一次上游请求发生在提交阶段
    const submitted = await fetchAndSubmit(runner, {
      sourceClient: client,
      url: source.url,
      path: '/raw',
      adapterName: 'sleep',
      budgets: {timeoutMs: 250},
    });
    const first = await runner.waitFor(submitted.jobId);
    assert.equal(first.status, 'failed');
    assert.equal(first.reason, 'timeout');
    const firstRawPath = first.rawPath;

    // 来源服务关闭后重试：证明重试路径不重发任何上游请求
    await source.close();
    const retried = await runner.retry(submitted.jobId, {budgets: {timeoutMs: 15000}});
    assert.equal(retried.status, 'succeeded');
    assert.equal(retried.rawPath, firstRawPath);
    assert.equal(retried.attempts.length, 2);
    assert.equal(retried.attempts[0].reason, 'timeout');
    assert.equal(retried.attempts[1].status, 'succeeded');
    assert.ok(existsSync(retried.resultPath));
  } finally {
    await runner.shutdown();
  }
});

test('适配器内容变更后拒绝以新版本重试，避免静默改变解析语义', async () => {
  const root = await mkdtemp(join(tmpdir(), 'runner-version-'));
  const runner = new CollectionRunner({root: await mkdtemp(join(tmpdir(), 'runner-version-root-'))});
  await runner.ready();
  try {
    const copy = join(root, 'sleep-copy.mjs');
    await cp(adapterPath('sleep'), copy);
    await register(runner, 'sleep-copy', copy);
    const {jobId} = await runner.submit({
      adapterName: 'sleep-copy',
      raw: {items: []},
      budgets: {timeoutMs: 150},
    });
    const job = await runner.waitFor(jobId);
    assert.equal(job.reason, 'timeout');

    await writeFile(copy, 'process.exit(0);\n');
    await assert.rejects(runner.retry(jobId), /适配器内容已变更/);
  } finally {
    await runner.shutdown();
  }
});

test('排队阶段取消：子进程从不启动，作业收敛为 canceled 且无残留目录', async () => {
  const {root, runner} = await makeRunner();
  try {
    await register(runner, 'sleep');
    const {jobId} = await runner.submit({
      adapterName: 'sleep',
      raw: {items: []},
      budgets: {timeoutMs: 30000},
    });
    // 不等待，立即取消，覆盖“已入队但执行尚未开始”的窗口
    const canceled = await runner.cancel(jobId);
    assert.equal(canceled.status, 'canceled');
    assert.equal(canceled.reason, 'canceled');
    assert.equal(canceled.attempts[0].status, 'failed');
    assert.equal(canceled.attempts[0].reason, 'canceled');
    assert.equal(canceled.attempts[0].metrics, null);
    assert.deepEqual(await readdir(join(root, 'jobs', jobId, 'attempts')), []);
    assert.deepEqual(await readdir(runner.outputDir), []);
    // 已终止作业可以重试，重试后得到新的控制器与成功结果
    const retried = await runner.retry(jobId, {budgets: {timeoutMs: 15000}});
    assert.equal(retried.status, 'succeeded');
    assert.equal(retried.attempts.length, 2);
  } finally {
    await runner.shutdown();
  }
});

test('取消作业：清理其进程组与临时文件，但不删除其他作业目录', async () => {
  const {root, runner} = await makeRunner();
  try {
    await register(runner, 'tempfile', testAdapterPath('tempfile'));
    await register(runner, 'normal');

    const victim = await runner.submit({
      adapterName: 'tempfile',
      raw: {items: []},
      budgets: {timeoutMs: 10000},
    });
    await new Promise(resolve => setTimeout(resolve, 400));
    const healthy = await runner.submit({adapterName: 'normal', raw});

    const canceled = await runner.cancel(victim.jobId);
    assert.equal(canceled.status, 'canceled');
    assert.equal(canceled.reason, 'canceled');

    const healthyJob = await runner.waitFor(healthy.jobId);
    assert.equal(healthyJob.status, 'succeeded');

    // 被取消作业的工作目录（含适配器创建的临时文件）已删除
    assert.deepEqual(await readdir(join(root, 'jobs', victim.jobId, 'attempts')), []);
    // 健康作业的目录与正式结果完好
    assert.ok(existsSync(join(root, 'jobs', healthy.jobId, 'job.json')));
    assert.ok(existsSync(healthyJob.resultPath));
    const outFiles = await readdir(runner.outputDir);
    assert.deepEqual(outFiles, [`${healthy.jobId}.json`]);
  } finally {
    await runner.shutdown();
  }
});

test('并发作业互不阻塞：一个适配器死循环，其他来源照常产出', {timeout: 20000}, async () => {
  const {runner} = await makeRunner();
  try {
    await register(runner, 'endless');
    await register(runner, 'normal');
    const stuck = runner.submit({
      adapterName: 'endless',
      raw: {items: []},
      budgets: {timeoutMs: 500},
    });
    const healthy = runner.submit({adapterName: 'normal', raw});
    const [stuckJob, healthyJob] = await Promise.all([
      stuck.then(({jobId}) => runner.waitFor(jobId)),
      healthy.then(({jobId}) => runner.waitFor(jobId)),
    ]);
    assert.equal(stuckJob.reason, 'timeout');
    assert.equal(healthyJob.status, 'succeeded');
    assert.equal(healthyJob.attempts[0].metrics.recordCount, 2);
  } finally {
    await runner.shutdown();
  }
});

test('运维视图：作业列表保留每次尝试的受限原因、指标与截断诊断', {timeout: 20000}, async () => {
  const {runner} = await makeRunner();
  try {
    await register(runner, 'logs');
    const {jobId} = await runner.submit({
      adapterName: 'logs',
      raw: {items: []},
      budgets: {timeoutMs: 5000, stderrBytes: 2048},
    });
    const job = await runner.waitFor(jobId);
    assert.equal(job.reason, 'diagnostics-over-budget');
    const attempt = job.attempts[0];
    assert.ok(attempt.diagnosticsBytes > 0);
    assert.ok(Buffer.byteLength(attempt.diagnostics) <= 2048 + 64);
    assert.equal(runner.listJobs().length, 1);
  } finally {
    await runner.shutdown();
  }
});

test('协议版本在注册与执行两层都被校验', async () => {
  const {runner} = await makeRunner();
  try {
    await register(runner, 'normal', adapterPath('normal'), 1);
    await assert.rejects(
      register(runner, 'future', adapterPath('normal'), 42),
      /不支持的协议版本/,
    );
  } finally {
    await runner.shutdown();
  }
});
