// 测试专用合成负载：持续持有 JS 对象直到老生代耗尽，用于验证内存预算。
const held = [];
setInterval(() => {
  held.push(new Array(200000).fill(0).map((_, index) => index));
}, 10);
