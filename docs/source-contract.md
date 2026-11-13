# 来源协议

适配器按JSON行协议从stdin读取一份{protocol,input}，输出{type:"record",value}及末尾{type:"end",count}，stderr为诊断。fixtures/raw.json为合成来源正文。adapters包含正常、睡眠、无终止计算、日志过量、半条输出及创建普通子进程六种行为。src/direct.mjs只执行normal适配器，无隔离保障。/raw返回来源原文。测试资源治理只能使用这些良性负载，不生成攻击或漏洞利用。

## 通用控制

`GET /inventory` 返回本来源当前目录，`GET /health` 检查存活，`GET /state` 查看来源状态。`POST /control` 接收JSON对象，仅在来源支持的字段范围内操作。每次创建服务重新读取 fixtures/source.json；控制用于复现来源演进，不是采集业务的实现接口。

服务使用随机本地端口，测试启动与关闭均由代码负责。没有真实个人资料、凭据或付费调用。
