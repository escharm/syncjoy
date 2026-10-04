# 下一步方向

按优先级排列，每项落地时在 decisions 留档关键取舍。

1. **peer web 端收尾**：`web` feature 与 web_sys 双传输层已就位（编译级验证），剩余 wasm-bindgen 对外导出 JS API、浏览器内真实往返（Playwright 或 wasm-bindgen-test）、CI 落地，见 todos；
2. **同内网直连**：两个 peer 同内网时无需公网 relay 参与即可互相发现并直连（如 mDNS）；
3. **二进制序列化**：见 [issues/binary-serialization.md](../issues/binary-serialization.md)；
4. **发布**：LICENSE 选型、crates.io 发布三个 crate、CI（check/test/clippy 与 `cargo xtask features/e2e/verify`，含 wasm target）。
