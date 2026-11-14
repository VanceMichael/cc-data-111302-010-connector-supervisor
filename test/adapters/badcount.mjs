// 测试专用合成负载：end.count 与实际记录数不一致，应被输出侧校验拒绝。
let text = '';
for await (const chunk of process.stdin) text += chunk;
const input = JSON.parse(text);
for (const item of input.input.items) {
  process.stdout.write(JSON.stringify({type: 'record', value: item}) + '\n');
}
process.stdout.write(JSON.stringify({type: 'end', count: input.input.items.length + 99}) + '\n');
