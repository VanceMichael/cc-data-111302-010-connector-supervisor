# 采集适配器的资源隔离与受控结果接收

这是用于内部数据接入的 Node.js 项目。当前提供可独立启动的本地来源、单次原文读取接口、来源契约测试，以及把合作方适配器限制在独立子进程中的执行与结果接收层；浏览器页面和仪器网关使用合成业务资料，不连接真实账户或设备。

## 环境与命令

Node.js 22.22.1 或更新的 22.x。依赖由 package-lock.json 锁定。

```sh
npm ci --ignore-scripts
npm run build
npm test
npm start          # 仅来源服务
npm run supervise  # 来源服务 + 适配器隔离执行层 + 运维入口
```

`npm start` 在标准输出报告来源地址。默认绑定本机回环地址并使用动态端口；可通过 `HOST` 与 `PORT` 指定监听地址和端口，例如 `HOST=127.0.0.1 PORT=8080 npm start`。按 Ctrl+C 关闭来源；动态端口避免与同机其他接入站冲突。`npm run demo` 在同一进程启动来源，读取一次目录后退出，不代表持续接收业务已经完成。

`npm run supervise` 额外读取 `OPS_HOST`/`OPS_PORT`（运维入口）与 `RUNS_DIR`（作业落盘根目录，默认 `.local/runs`）。运维入口提供 `GET /adapters`、`GET /jobs`、`GET /jobs/:id`、`POST /jobs`、`POST /jobs/:id/cancel`、`POST /jobs/:id/retry`。

```sh
docker build -t connector-supervisor:local .
docker run --rm --network none connector-supervisor:local
docker run --rm --network none connector-supervisor:local npm run demo
```

依赖在镜像构建阶段安装，运行阶段不需要公网。完整来源测试和一次目录演示通常数秒即可结束，适合在90秒以内查看真实输出。浏览器在无界面模式执行，页面交互不依赖人工鼠标。

## 目录

- `src/source.mjs`：本地来源生命周期与HTTP入口。
- `src/client.mjs`：目录读取及原始字节获取。
- `src/executor.mjs`：适配器隔离执行器——独立进程组、时间/输出/诊断/内存预算、输入输出协议校验、原因码与有限诊断。
- `src/runner.mjs`：作业运行器——原文与适配器版本固化、临时/正式目录隔离、原子发布、取消清理与定位式重试。
- `src/ops.mjs`：运维HTTP入口，暴露调用状态、受限原因、指标与截断诊断。
- `src/supervisor.mjs`：来源、执行层与运维入口的组合进程。
- `src/direct.mjs`：早期直跑 normal 适配器的实现，无隔离保障，仅保留给来源契约测试对照。
- `adapters/`：六种合成适配器（正常、睡眠、无终止计算、超量日志、半条输出、派生子进程）。
- `test/adapters/`：测试专用合成负载（内存增长、临时文件、非协议 stdout、count 不一致）。
- `fixtures/source.json`：来源初始状态。
- `docs/source-contract.md`：来源协议范围、消息契约与受限原因枚举。
- `docs/isolation-boundary.md`：隔离边界能保证与不能保证的事项、预算实现与运维说明。
- `test/executor.test.mjs`、`test/runner.test.mjs`、`test/ops.test.mjs`：实际子进程集成测试。

## 隔离要点（摘要）

每次适配器调用都是独立进程组中的独立 Node.js 子进程，主进程不加载合作方代码；时间、stdout、stderr、内存均有硬预算；stderr 诊断与 stdout 业务通道分离；结果只有在完整通过协议校验后才原子发布，半批资料不会进入正式目录；取消会终止整个进程组并只清理本作业的临时目录；重试复用按内容哈希固化的原文与适配器版本指纹，不重发上游请求。完整边界与明确不覆盖的威胁见 `docs/isolation-boundary.md`。

运行时状态仅用于来源模拟，重启来源会回到 fixtures 中的值。接入业务自己的持久状态请使用 `.local/` 或明确的外部路径。来源管理入口 `/state` 和 `/control` 是本地操作设施，不能作为绕过浏览器页面的业务写入捷径。
