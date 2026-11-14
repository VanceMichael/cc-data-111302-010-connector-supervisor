# 来源协议

适配器按JSON行协议从stdin读取一份{protocol,input}，输出{type:"record",value}及末尾{type:"end",count}，stderr为诊断。fixtures/raw.json为合成来源正文。adapters包含正常、睡眠、无终止计算、日志过量、半条输出及创建普通子进程六种行为。src/direct.mjs只执行normal适配器，无隔离保障；生产路径应使用src/executor.mjs与src/runner.mjs。/raw返回来源原文。测试资源治理只能使用这些良性负载，不生成攻击或漏洞利用。

## 适配器消息契约（协议版本 1）

- 输入：一行 JSON 对象 `{protocol:1, input:<结构化原文>}`，由执行器写入子进程 stdin。
- 输出（stdout，JSON Lines）：零或多条 `{type:"record", value:<任意 JSON 值>}`，最后必须恰好一条 `{type:"end", count:<非负整数>}`，count 必须等于 record 条数；不允许空行或非 JSON 文本。
- 诊断（stderr）：自由文本，与业务通道严格分离，仅按预算保留有限片段供排查，绝不并入记录。
- 协议版本由合作方在注册时声明；不在 `SUPPORTED_PROTOCOLS` 内的版本在启动子进程之前拒绝。

## 执行层受限原因（reason）

| reason | 触发条件 |
| --- | --- |
| `input-rejected` | 输入信封不是对象或 input 不是结构化对象 |
| `unsupported-protocol` | 声明的协议版本不受支持 |
| `adapter-launch-failed` | 子进程无法启动 |
| `timeout` | 超过时间预算，进程组被终止 |
| `output-over-budget` | stdout 字节数超过业务输出预算 |
| `diagnostics-over-budget` | stderr 字节数超过诊断预算，文本截断保留 |
| `memory-limit` | 触发 V8 老生代上限或 RSS 兜底预算 |
| `protocol-violation` | 非 JSON 行、未知消息类型、缺 end、count 不一致等 |
| `partial-record` | 进程退出时 stdout 残留没有换行结束的半条记录 |
| `nonzero-exit` | 协议完整但退出码非零 |
| `canceled` | 调用方通过 AbortSignal 取消 |
| `executor-error` | 执行器自身异常 |

## 通用控制

`GET /inventory` 返回本来源当前目录，`GET /health` 检查存活，`GET /state` 查看来源状态。`POST /control` 接收JSON对象，仅在来源支持的字段范围内操作。每次创建服务重新读取 fixtures/source.json；控制用于复现来源演进，不是采集业务的实现接口。

监督进程（src/supervisor.mjs）额外提供运维入口：`GET /adapters`、`GET /jobs`、`GET /jobs/:id`、`POST /jobs`、`POST /jobs/:id/cancel`、`POST /jobs/:id/retry`。作业状态取值为 queued、running、succeeded、failed、canceled；每次尝试都记录 reason、指标与截断诊断。

服务使用随机本地端口，测试启动与关闭均由代码负责。没有真实个人资料、凭据或付费调用。
