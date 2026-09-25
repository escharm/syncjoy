# peer web 端

- [x] 平台绑定收拢到 `crates/peer/src/transport/`，native/web 按 feature 切分，共享层（signaling 泵、rtc 协商状态机、句柄层）零 cfg
- [x] `web` feature 下用 `web_sys::WebSocket` 实现信令、`web_sys::RtcPeerConnection` 实现同等协商语义，`cargo check --target wasm32-unknown-unknown --no-default-features --features web` 编译通过
- [ ] 浏览器内真实往返：wasm-bindgen-test 或 Playwright（本沙箱无浏览器，需 CI 落地）

实现备注（2026-09-26）：

- transport 两实现暴露同一组名字（`connect_signaling` 通道对、`PeerConnection`/`DataChannel` 包装、`spawn`/`timeout`/`new_peer_connection`、共享 `IceServer`），native 是 tokio-tungstenite + webrtc-rs，web 是 web_sys（`Closure::wrap` 回调经 forget 保活、promise 经 `JsFuture` await）
- workspace tokio 收窄到 `macros + sync`，`net`/`time`/`rt` 由各 crate 按需追加；peer 的平台依赖全部挂在 feature 门后
- web 侧 JS Closure 用 `forget` 保活：每条连接的回调闭窗在连接重建时会泄漏，浏览器往返验证时一并处理（改为 keepalive 容器）
