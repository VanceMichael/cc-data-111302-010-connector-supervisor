// 测试专用合成负载：把调试文本写进 stdout 业务通道，应被协议校验拒绝。
process.stdout.write('DEBUG: adapter started without envelope\n');
process.stdout.write(JSON.stringify({type: 'end', count: 0}) + '\n');
