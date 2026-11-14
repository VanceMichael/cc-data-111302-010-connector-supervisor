import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {once} from 'node:events';
import {spawn} from 'node:child_process';
import {CollectionRunner} from '../src/runner.mjs';
import {createOpsServer} from '../src/ops.mjs';

const projectRoot = new URL('..', import.meta.url).pathname;
const adapterPath = name => join(projectRoot, 'adapters', `${name}.mjs`);
const raw = {items: [{id: 'a', name: '甲'}]};

async function withOps(callback) {
  const root = await mkdtemp(join(tmpdir(), 'ops-'));
  const runner = new CollectionRunner({root});
  await runner.ready();
  await runner.registerAdapter({name: 'normal', path: adapterPath('normal')});
  await runner.registerAdapter({name: 'sleep', path: adapterPath('sleep')});
  const server = await createOpsServer(runner);
  try {
    return await callback({runner, server});
  } finally {
    await runner.shutdown();
    await server.close();
  }
}

const request = async (server, path, options = {}) => {
  const response = await fetch(server.url + path, {
    method: options.method ?? 'GET',
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
    headers: {'Content-Type': 'application/json'},
    signal: AbortSignal.timeout(30000),
  });
  const value = await response.json();
  return {status: response.status, value};
};

test('运维入口：适配器清单、作业提交、状态查询与受限原因', async () => {
  await withOps(async ({server}) => {
    const health = await request(server, '/health');
    assert.equal(health.value.status, 'ok');

    const adapters = await request(server, '/adapters');
    assert.equal(adapters.value.adapters.length, 2);
    assert.ok(adapters.value.adapters[0].version.length === 64);

    const submitted = await request(server, '/jobs', {method: 'POST', body: {adapter: 'normal', raw}});
    assert.equal(submitted.status, 202);
    const {jobId} = submitted.value;

    let job;
    const deadline = Date.now() + 5000;
    do {
      job = (await request(server, `/jobs/${jobId}`)).value;
      await new Promise(resolve => setTimeout(resolve, 20));
    } while (job.status === 'queued' || job.status === 'running');
    assert.equal(job.status, 'succeeded');
    assert.equal(job.reason, null);
    assert.equal(job.attempts[0].metrics.recordCount, 1);

    const list = await request(server, '/jobs');
    assert.equal(list.value.jobs.length, 1);
  });
});

test('运维入口：取消与重试链路返回明确状态，坏请求被拒绝', {timeout: 30000}, async () => {
  await withOps(async ({server}) => {
    const missing = await request(server, '/jobs/nope');
    assert.equal(missing.status, 404);
    const badBody = await fetch(server.url + '/jobs', {method: 'POST', body: 'not-json'});
    assert.equal(badBody.status, 400);
    const incomplete = await request(server, '/jobs', {method: 'POST', body: {adapter: 'normal'}});
    assert.equal(incomplete.status, 400);
    const unknownAdapter = await request(server, '/jobs', {
      method: 'POST',
      body: {adapter: 'ghost', raw},
    });
    assert.equal(unknownAdapter.status, 400);

    const submitted = await request(server, '/jobs', {
      method: 'POST',
      body: {adapter: 'sleep', raw: {items: []}, budgets: {timeoutMs: 10000}},
    });
    const {jobId} = submitted.value;
    await new Promise(resolve => setTimeout(resolve, 300));

    const canceled = await request(server, `/jobs/${jobId}/cancel`, {method: 'POST'});
    assert.equal(canceled.value.status, 'canceled');
    assert.equal(canceled.value.reason, 'canceled');

    const retried = await request(server, `/jobs/${jobId}/retry`, {
      method: 'POST',
      body: {budgets: {timeoutMs: 15000}},
    });
    assert.equal(retried.status, 200);
    assert.equal(retried.value.attempts.length, 2);
    assert.equal(retried.value.attempts[0].reason, 'canceled');
  });
});

test('监督进程入口可独立启动并报告来源、运维地址，关闭时回收子进程', {timeout: 15000}, async () => {
  const child = spawn(process.execPath, ['src/supervisor.mjs'], {
    cwd: projectRoot,
    env: {...process.env, RUNS_DIR: join(tmpdir(), `supervisor-${Date.now()}`)},
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', chunk => { output += chunk.toString(); });
  child.stderr.on('data', chunk => { output += chunk.toString(); });
  try {
    const deadline = Date.now() + 8000;
    let info = null;
    while (Date.now() < deadline) {
      const match = output.match(/\{[^\n]*\}/);
      if (match) {
        info = JSON.parse(match[0]);
        break;
      }
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.ok(info, `监督进程未在时限内输出就绪信息: ${output}`);
    assert.equal(info.status, '采集监督进程就绪');
    assert.ok(info.adapters.length >= 6);
    const opsHealth = await fetch(info.ops + '/health').then(r => r.json());
    assert.equal(opsHealth.status, 'ok');
    const sourceHealth = await fetch(info.source + '/health').then(r => r.json());
    assert.equal(sourceHealth.status, 'ok');
  } finally {
    child.kill('SIGTERM');
    await once(child, 'exit');
  }
});
