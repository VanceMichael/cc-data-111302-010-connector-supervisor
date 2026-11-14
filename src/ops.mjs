import http from 'node:http';

/**
 * 运维只读/受控入口：暴露调用状态、受限原因与有限诊断，
 * 不暴露完整业务原文，诊断文本已经在执行器侧按预算截断。
 */
export async function createOpsServer(runner, {host = '127.0.0.1', port = 0} = {}) {
  const send = (response, status, value) => {
    response.writeHead(status, {'Content-Type': 'application/json; charset=utf-8'});
    response.end(JSON.stringify(value));
  };

  const summarize = job => ({
    jobId: job.jobId,
    adapter: job.adapter,
    adapterVersion: job.adapterVersion.slice(0, 12),
    protocol: job.protocol,
    rawHash: job.rawHash.slice(0, 12),
    source: job.sourcePath ? `${job.sourceUrl ?? ''}${job.sourcePath}` : null,
    status: job.status,
    reason: job.reason,
    message: job.message,
    resultPath: job.resultPath,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    attempts: job.attempts.map(attempt => ({
      attempt: attempt.attempt,
      status: attempt.status,
      reason: attempt.reason,
      message: attempt.message,
      metrics: attempt.metrics,
      diagnostics: attempt.diagnostics,
      diagnosticsBytes: attempt.diagnosticsBytes,
      startedAt: attempt.startedAt,
      finishedAt: attempt.finishedAt,
    })),
  });

  const server = http.createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://ops.local');
      let text = '';
      for await (const chunk of request) {
        text += chunk;
        if (text.length > 8 * 1024 * 1024) return send(response, 413, {error: '请求体过大'});
      }
      let body = {};
      if (text) {
        try { body = JSON.parse(text); } catch { return send(response, 400, {error: 'JSON 无效'}); }
      }

      if (request.method === 'GET' && url.pathname === '/health') {
        return send(response, 200, {status: 'ok'});
      }
      if (request.method === 'GET' && url.pathname === '/adapters') {
        return send(response, 200, {adapters: runner.listAdapters()});
      }
      if (request.method === 'GET' && url.pathname === '/jobs') {
        return send(response, 200, {jobs: runner.listJobs().map(summarize)});
      }
      const jobMatch = url.pathname.match(/^\/jobs\/([^/]+)$/);
      if (request.method === 'GET' && jobMatch) {
        const job = runner.getJob(jobMatch[1]);
        return job ? send(response, 200, summarize(job)) : send(response, 404, {error: '作业不存在'});
      }
      if (request.method === 'POST' && url.pathname === '/jobs') {
        if (!body.adapter || !body.raw) return send(response, 400, {error: '需要 adapter 与 raw 字段'});
        try {
          const submitted = await runner.submit({
            adapterName: body.adapter,
            raw: body.raw,
            sourceUrl: body.sourceUrl ?? null,
            sourcePath: body.sourcePath ?? null,
            budgets: body.budgets ?? {},
          });
          return send(response, 202, submitted);
        } catch (error) {
          return send(response, 400, {error: error.message});
        }
      }
      const cancelMatch = url.pathname.match(/^\/jobs\/([^/]+)\/cancel$/);
      if (request.method === 'POST' && cancelMatch) {
        try {
          return send(response, 200, summarize(await runner.cancel(cancelMatch[1])));
        } catch (error) {
          return send(response, 404, {error: error.message});
        }
      }
      const retryMatch = url.pathname.match(/^\/jobs\/([^/]+)\/retry$/);
      if (request.method === 'POST' && retryMatch) {
        try {
          const job = await runner.retry(retryMatch[1], {budgets: body.budgets ?? {}});
          return send(response, 200, summarize(job));
        } catch (error) {
          return send(response, 409, {error: error.message});
        }
      }
      return send(response, 404, {error: '资源不存在'});
    } catch (error) {
      if (!response.headersSent) send(response, 500, {error: error.message});
      else response.destroy();
    }
  });

  await new Promise(resolve => server.listen(port, host, resolve));
  const address = server.address();
  return {
    url: `http://${host}:${address.port}`,
    close: async () => {
      server.closeAllConnections?.();
      await new Promise(resolve => server.close(resolve));
    },
  };
}
