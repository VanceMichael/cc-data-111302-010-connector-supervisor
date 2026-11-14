import {createHash} from 'node:crypto';
import {randomUUID} from 'node:crypto';
import {mkdir, writeFile, readFile, readdir, rm, rename} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {runAdapter, PROTOCOL_VERSION, SUPPORTED_PROTOCOLS} from './executor.mjs';

/**
 * 采集作业运行器：在执行器之上提供作业生命周期、原文与版本固化、
 * 临时/正式目录隔离、原子发布、取消清理与定位式重试。
 *
 * 目录布局（root 默认 .local/runs）：
 *   raw/<hash>.json          原文按内容哈希固化，重试复用同一字节，不重发上游请求
 *   jobs/<jobId>/job.json    作业记录（状态、受限原因、有限诊断、指标）
 *   jobs/<jobId>/attempts/<n>/        单次调用专属工作目录，失败/取消即整体删除
 *   jobs/<jobId>/result.json          仅在全部校验通过后原子发布的正式结果
 */

const JOB_TERMINAL = new Set(['succeeded', 'failed', 'canceled']);

function sha256(buffer) {
  return createHash('sha256').update(buffer).digest('hex');
}

/** 防止适配器把符号链接/逃逸路径塞进作业目录，删除只允许落在作业目录之内。 */
function assertInside(base, target) {
  const inside = resolve(target);
  const prefix = resolve(base);
  if (inside !== prefix && !inside.startsWith(prefix + '/')) {
    throw new Error(`拒绝清理作业目录之外的路径: ${inside}`);
  }
}

export class CollectionRunner {
  #root;
  #rawDir;
  #jobsDir;
  #outDir;
  #adapters = new Map();
  #jobs = new Map();
  #controllers = new Map();
  #ready;

  constructor({root} = {}) {
    this.#root = root ? resolve(root) : join(process.cwd(), '.local', 'runs');
    this.#rawDir = join(this.#root, 'raw');
    this.#jobsDir = join(this.#root, 'jobs');
    this.#outDir = join(this.#root, 'out');
    this.#ready = this.#init();
  }

  async #init() {
    await mkdir(this.#rawDir, {recursive: true});
    await mkdir(this.#jobsDir, {recursive: true});
    await mkdir(this.#outDir, {recursive: true});
    await this.#pruneStaging();
  }

  /** 清理上次异常退出留下的发布暂存文件；正式 result.json 永不触碰。 */
  async #pruneStaging() {
    for (const name of await readdir(this.#outDir, {withFileTypes: true})) {
      if (name.isFile() && name.name.startsWith('.staging-')) {
        await rm(join(this.#outDir, name.name), {force: true});
      }
    }
    for (const jobName of await readdir(this.#jobsDir, {withFileTypes: true})) {
      if (!jobName.isDirectory()) continue;
      const jobDir = join(this.#jobsDir, jobName.name);
      for (const name of await readdir(jobDir, {withFileTypes: true})) {
        if (name.isFile() && name.name.startsWith('.staging-')) {
          await rm(join(jobDir, name.name), {force: true});
        }
      }
    }
  }

  async ready() {
    return this.#ready;
  }

  /**
   * 注册本地适配器并固化版本指纹（脚本内容哈希）。
   * 重试时会重新读取当前脚本比对，适配器已变更则拒绝复用旧版本。
   */
  async registerAdapter({name, path, protocol = PROTOCOL_VERSION}) {
    await this.#ready;
    if (!Number.isInteger(protocol) || !SUPPORTED_PROTOCOLS.has(protocol)) {
      throw new Error(`不支持的协议版本: ${String(protocol)}`);
    }
    const absolute = resolve(path);
    if (!existsSync(absolute)) throw new Error(`适配器不存在: ${absolute}`);
    const source = await readFile(absolute);
    this.#adapters.set(name, {
      name,
      path: absolute,
      protocol,
      version: sha256(source),
      sourceBytes: source.length,
    });
    return {name, protocol, version: this.#adapters.get(name).version};
  }

  listAdapters() {
    return [...this.#adapters.values()].map(({name, protocol, version}) => ({name, protocol, version}));
  }

  #jobDir(jobId) {
    return join(this.#jobsDir, jobId);
  }

  #jobFile(jobId) {
    return join(this.#jobDir(jobId), 'job.json');
  }

  async #persist(jobId, record) {
    this.#jobs.set(jobId, record);
    await writeFile(this.#jobFile(jobId), JSON.stringify(record, null, 2));
  }

  getJob(jobId) {
    return this.#jobs.get(jobId);
  }

  listJobs() {
    return [...this.#jobs.values()].toSorted((a, b) => a.createdAt.localeCompare(b.createdAt));
  }

  /**
   * 提交一个采集作业。原文字节立即固化并返回作业 id；执行在后台进行，
   * 单个适配器被卡住或杀掉只影响它自己的作业，不会阻塞其他作业。
   */
  async submit({adapterName, raw, sourceUrl = null, sourcePath = null, budgets = {}}) {
    await this.#ready;
    const adapter = this.#adapters.get(adapterName);
    if (!adapter) throw new Error(`未注册的适配器: ${adapterName}`);

    const rawBuffer = Buffer.from(JSON.stringify(raw));
    const rawHash = sha256(rawBuffer);
    const rawPath = join(this.#rawDir, `${rawHash}.json`);
    await writeFile(rawPath, rawBuffer); // 同哈希即同字节，重复写无副作用

    const jobId = randomUUID();
    await mkdir(join(this.#jobDir(jobId), 'attempts'), {recursive: true});
    // 入队时即注册取消控制器，覆盖“已入队但子进程尚未启动”的窗口。
    const controller = new AbortController();
    this.#controllers.set(jobId, controller);
    const record = {
      jobId,
      adapter: adapter.name,
      adapterVersion: adapter.version,
      protocol: adapter.protocol,
      sourceUrl,
      sourcePath,
      rawHash,
      rawPath,
      status: 'queued',
      reason: null,
      message: '',
      resultPath: null,
      attempts: [],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await this.#persist(jobId, record);
    void this.#execute(jobId, {adapter, budgets});
    return {jobId, rawHash, rawPath};
  }

  /**
   * 用已经固化的原文执行一次调用（首次执行与重试共用）。
   * 全程不接触来源服务，因此重试不会重发任何上游请求。
   * 任何异常都收敛为作业记录，绝不向上抛出未处理拒绝。
   */
  async #runAttempt({jobId, raw, adapter, budgets, attemptNo, previous}) {
    const controller = this.#controllers.get(jobId) ?? new AbortController();
    this.#controllers.set(jobId, controller);
    const attemptDir = join(this.#jobDir(jobId), 'attempts', String(attemptNo));
    const staging = join(this.#jobDir(jobId), `.staging-${jobId}.json`);

    const attempt = {
      attempt: attemptNo,
      startedAt: new Date().toISOString(),
      status: 'running',
      reason: null,
      message: '',
      workDir: attemptDir,
      metrics: null,
      diagnosticsBytes: 0,
    };
    previous.attempts.push(attempt);
    previous.status = 'running';
    previous.reason = null;
    previous.message = '';
    previous.updatedAt = new Date().toISOString();
    await this.#persist(jobId, previous);

    try {
      // 排队阶段就被取消：不启动子进程，直接收敛为 canceled。
      if (controller.signal.aborted) {
        throw Object.assign(new Error('调用在启动前已被取消'), {canceledBeforeStart: true});
      }
      await mkdir(attemptDir, {recursive: true});
      const outcome = await runAdapter({
        adapterPath: adapter.path,
        raw,
        protocol: adapter.protocol,
        budgets,
        cwd: attemptDir,
        signal: controller.signal,
      });

      attempt.finishedAt = new Date().toISOString();
      attempt.metrics = outcome.metrics ?? null;
      attempt.diagnosticsBytes = Buffer.byteLength(outcome.diagnostics ?? '');
      attempt.diagnostics = outcome.diagnostics ?? ''; // 执行器已按预算截断

      if (outcome.ok) {
        // 先写作业目录内的暂存文件，再 rename 进正式目录：
        // 正式目录里永远只会出现完整且通过协议校验的 result.json。
        const result = {
          jobId,
          adapter: adapter.name,
          adapterVersion: adapter.version,
          protocol: adapter.protocol,
          rawHash: previous.rawHash,
          publishedAt: new Date().toISOString(),
          count: outcome.records.length,
          records: outcome.records,
        };
        await writeFile(staging, JSON.stringify(result, null, 2));
        const resultPath = join(this.#outDir, `${jobId}.json`);
        await rename(staging, resultPath);

        attempt.status = 'succeeded';
        attempt.resultPath = resultPath;
        Object.assign(previous, {
          status: 'succeeded',
          reason: null,
          message: '',
          resultPath,
          updatedAt: new Date().toISOString(),
        });
      } else {
        attempt.status = 'failed';
        attempt.reason = outcome.reason;
        attempt.message = outcome.message;
        Object.assign(previous, {
          status: controller.signal.aborted ? 'canceled' : 'failed',
          reason: outcome.reason,
          message: outcome.message,
          updatedAt: new Date().toISOString(),
        });
      }
    } catch (error) {
      await rm(staging, {force: true}).catch(() => {});
      attempt.finishedAt = new Date().toISOString();
      attempt.status = 'failed';
      if (error.canceledBeforeStart || controller.signal.aborted) {
        attempt.reason = 'canceled';
        attempt.message = error.message || '调用已被取消';
        Object.assign(previous, {
          status: 'canceled',
          reason: 'canceled',
          message: attempt.message,
          updatedAt: new Date().toISOString(),
        });
      } else {
        attempt.reason = 'executor-error';
        attempt.message = error.message;
        Object.assign(previous, {
          status: 'failed',
          reason: 'executor-error',
          message: error.message,
          updatedAt: new Date().toISOString(),
        });
      }
    } finally {
      await this.#persist(jobId, previous).catch(() => {});
      this.#controllers.delete(jobId);
      // 清理本次调用的子进程工作目录；删除范围被断言限制在本作业目录内。
      assertInside(this.#jobDir(jobId), attemptDir);
      await rm(attemptDir, {recursive: true, force: true});
    }
    return previous;
  }

  async #execute(jobId, {adapter, budgets}) {
    const record = this.#jobs.get(jobId);
    try {
      const raw = JSON.parse(await readFile(record.rawPath, 'utf8'));
      await this.#runAttempt({jobId, raw, adapter, budgets, attemptNo: 1, previous: record});
    } catch (error) {
      Object.assign(record, {
        status: 'failed',
        reason: 'executor-error',
        message: error.message,
        updatedAt: new Date().toISOString(),
      });
      await this.#persist(jobId, record).catch(() => {});
      this.#controllers.delete(jobId);
    }
  }

  /** 取消正在执行的作业：执行器杀整个进程组，这里负责删除其临时目录。 */
  async cancel(jobId) {
    await this.#ready;
    const record = this.#jobs.get(jobId);
    if (!record) throw new Error(`作业不存在: ${jobId}`);
    if (JOB_TERMINAL.has(record.status)) return record;
    this.#controllers.get(jobId)?.abort();
    return this.waitFor(jobId);
  }

  /**
   * 定位式重试：必须能读到同一原文，且适配器当前内容指纹与首次调用一致。
   * 不访问任何上游 URL，只重新执行本地子进程。
   */
  async retry(jobId, {budgets = {}} = {}) {
    await this.#ready;
    const record = this.#jobs.get(jobId);
    if (!record) throw new Error(`作业不存在: ${jobId}`);
    if (!JOB_TERMINAL.has(record.status)) throw new Error('作业仍在执行，不能重试');
    if (record.status === 'succeeded') throw new Error('作业已成功，无需重试');

    const adapter = this.#adapters.get(record.adapter);
    if (!adapter) throw new Error(`适配器未注册: ${record.adapter}`);
    const currentSource = await readFile(adapter.path);
    if (sha256(currentSource) !== record.adapterVersion) {
      throw new Error('适配器内容已变更，拒绝以不同版本重试；请注册新版本后新建作业');
    }
    if (!existsSync(record.rawPath)) throw new Error('固化的原文已丢失，无法在不重发上游请求的前提下重试');
    const raw = JSON.parse(await readFile(record.rawPath, 'utf8'));

    Object.assign(record, {
      status: 'queued',
      reason: null,
      message: '',
      updatedAt: new Date().toISOString(),
    });
    await this.#persist(jobId, record);
    this.#controllers.set(jobId, new AbortController());

    return this.#runAttempt({
      jobId,
      raw,
      adapter,
      budgets,
      attemptNo: record.attempts.length + 1,
      previous: record,
    });
  }

  async waitFor(jobId, {timeoutMs = 0} = {}) {
    const start = Date.now();
    for (;;) {
      const record = this.#jobs.get(jobId);
      if (!record) throw new Error(`作业不存在: ${jobId}`);
      if (JOB_TERMINAL.has(record.status)) return record;
      if (timeoutMs > 0 && Date.now() - start > timeoutMs) throw new Error('等待作业结束超时');
      await new Promise(resolve => setTimeout(resolve, 20));
    }
  }

  /** 仅供进程关闭时调用：终止全部仍在执行的子进程组。 */
  async shutdown() {
    for (const controller of this.#controllers.values()) controller.abort();
    await Promise.allSettled(
      [...this.#jobs.keys()].map(jobId => this.waitFor(jobId, {timeoutMs: 3000}).catch(() => {})),
    );
  }

  get root() {
    return this.#root;
  }

  get outputDir() {
    return this.#outDir;
  }
}

/**
 * 便捷组合：从来源读取一次原文、固化后提交作业。
 * 只有这一步会发起上游 HTTP 请求；重试路径不再调用它。
 */
export async function fetchAndSubmit(runner, {sourceClient, url, path, adapterName, budgets = {}}) {
  const buffer = await sourceClient.readRaw(url, path);
  let raw;
  try {
    raw = JSON.parse(buffer.toString('utf8'));
  } catch {
    throw new Error('上游原文不是合法 JSON，拒绝进入适配器');
  }
  return runner.submit({adapterName, raw, sourceUrl: url, sourcePath: path, budgets});
}
