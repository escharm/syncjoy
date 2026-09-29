# 下一步方向

按优先级排列，每项落地时在 decisions 留档关键取舍。

1. **peer web 端收尾**：`web` feature 与 web_sys 双传输层已就位（编译级验证），剩余 wasm-bindgen 对外导出 JS API、浏览器内真实往返（Playwright 或 wasm-bindgen-test）、CI 落地，见 todos；
2. **relay 心跳与幽灵表项清理**：协议里 ping/pong 已就位，见 todos 与 [issues/ghost-session-keepalive.md](../issues/ghost-session-keepalive.md)；
3. **peer 链路自动重连与会话恢复**：信令断开后指数退避重连，重新注册并重新协商直连，对端感知语义跟随设计；
4. **同内网直连**：两个 peer 同内网时无需公网 relay 参与即可互相发现并直连（如 mDNS）；
5. **富状态层**：状态宿主 peer 的 presence、房间状态、快照与增量同步（需协议演进），这是"富状态"的本体，连接层稳定后启动；
6. **二进制序列化**：见 [issues/binary-serialization.md](../issues/binary-serialization.md)；
7. **发布**：LICENSE 选型、crates.io 发布三个 crate、CI（check/test/clippy，含 wasm target）。
