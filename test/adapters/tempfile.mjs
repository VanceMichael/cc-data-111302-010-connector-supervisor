// 测试专用合成负载：在自己的工作目录留下临时文件后挂住，用于验证取消清理。
import {writeFileSync} from 'node:fs';
writeFileSync('job-artifact.tmp', 'synthetic');
setInterval(() => {}, 1000);
