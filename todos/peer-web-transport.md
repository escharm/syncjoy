# peer web 端

- [x] 平台绑定收拢到 `crates/peer/src/transport/`，native/web 按 feature 切分，共享层（signaling 泵、rtc 协商状态机、句柄层）零 cfg
- [x] `web` feature 下用 `web_sys::WebSocket` 实现信令、`web_sys::RtcPeerConnection` 实现同等协商语义，`cargo check --target wasm32-unknown-unknown --no-default-features --features web` 编译通过
- [ ] 浏览器内真实往返：wasm-bindgen-test 或 Playwright（本沙箱无浏览器，需 CI 落地）

实现备注（2026-09-26，2026-10-01 更新）：

- transport 两实现暴露同一组名字（`connect_signaling` 通道对、`PeerConnection`/`DataChannel` 包装、`spawn`/`timeout`/`new_peer_connection`、共享 `IceServer`），native 是 tokio-tungstenite + webrtc-rs，web 是 web_sys，promise 经 `JsFuture` await
- workspace tokio 收窄到 `macros + sync`，`net`/`time`/`rt` 由各 crate 按需追加；peer 的平台依赖全部挂在 feature 门后
- web 信令连接已对齐 native 语义：`connect_signaling` 等 open 事件后再返回，首帧 hello 不会在 CONNECTING 状态发出；onerror/onclose 切断入站通道让信令泵退出
- web 侧 JS 回调不再 `forget`：闭包经 `into_js_value` 收进 keepalive 容器，挂在 `PeerConnection`/`DataChannel` 与信令发送句柄上，句柄 drop 后释放引用随 JS 对象被 GC 回收；Closure 本体不带 Send/Sync，容器存 `JsValue` 才能保持句柄 Send
- 回调闭包不得强引用 `PeerLinkInner` 或 `DataChannel`：它们持有同一 keepalive，强捕获构成引用环，句柄 drop 后整条对象图无法释放；链路回调一律经 `Weak` 捕获，upgrade 失败即链路已拆除、跳过本次回调
