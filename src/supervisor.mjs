import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {dirname, join} from 'node:path';
import {createSource} from './source.mjs';
import {CollectionRunner} from './runner.mjs';
import {createOpsServer} from './ops.mjs';

/**
 * 受控采集监督进程：来源服务、隔离执行器与运维入口在同一进程协作，
 * 但合作方适配器代码只存在于子进程中，主进程不加载它们。
 * HOST/PORT 控制来源服务，OPS_HOST/OPS_PORT 控制运维入口，
 * RUNS_DIR 控制作业/原文/正式结果的落盘根目录。
 */
const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..');

const source = await createSource({
  port: Number(process.env.PORT ?? 0),
  host: process.env.HOST ?? '127.0.0.1',
});

const runner = new CollectionRunner({root: process.env.RUNS_DIR || join(root, '.local', 'runs')});
await runner.ready();

const catalog = JSON.parse(await readFile(join(root, 'fixtures', 'source.json'), 'utf8'));
for (const name of catalog.adapters) {
  await runner.registerAdapter({name, path: join(root, 'adapters', `${name}.mjs`), protocol: catalog.protocol});
}

const ops = await createOpsServer(runner, {
  host: process.env.OPS_HOST ?? '127.0.0.1',
  port: Number(process.env.OPS_PORT ?? 0),
});

console.log(JSON.stringify({
  status: '采集监督进程就绪',
  source: source.url,
  ops: ops.url,
  runsRoot: runner.root,
  adapters: runner.listAdapters(),
}));

let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  await runner.shutdown().catch(() => {});
  await ops.close().catch(() => {});
  await source.close().catch(() => {});
  process.exit(0);
}
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
