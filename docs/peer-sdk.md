# peer SDK 使用方式

`connect_relay` 拿到信令链路 `RelayLink`，`connect_peer` 拿到与单个 peer 的直连 `PeerLink`。消息的收发都在 `PeerLink` 上：`send` 发送，来向消息用 `recv` 拉取或 `on_msg` 回调消费，两者是同一队列的视图，二选一。

消息内容是不透明字节：SDK 不解释、不编码，用 JSON、protobuf 还是私有格式由使用方决定。载荷载体是共享字节缓冲 `Bytes`（`syncjoy_peer::Bytes`，按 `&[u8]` 解引用，克隆是引用计数而非拷贝），接收路径对透传场景零拷贝。收发通道是与对端的 WebRTC DataChannel 直连，建立过程见 [architecture.md](architecture.md#msg-flow)。

<a id="connect"></a>
## 连接

```rust
let peer = Peer::builder().id("alice").build();
let mut relay_link = peer.connect_relay("ws://relay:7700").await?;      // 信令链路
let mut peer_link = relay_link.connect_peer("bob").await?;              // 与 bob 的直连
```

来向连接通过 `relay_link.next_event().await` 取得（`Event::Channel`，来向 DataChannel 打开时触发）。每对 peer 之间一条 DataChannel：`connect_peer` 对已连接的 peer 直接返回现有连接的句柄，不重复拨号；内置 15 秒拨号超时，穿透打不通时返回 `Timeout`。

relay 配置了注册准入时（见 [relay-sdk.md](relay-sdk.md#guard)），用 `relay_token(...)` 提供随 hello 帧发出的凭据；凭据只进信令通道，不进拨号 URL。relay 拒绝准入时 `connect_relay` 返回 `PeerError::Rejected`，携带 relay 给出的原因。

<a id="presence"></a>

## 在线状态

relay 在 peer 注册与退出时向其余 peer 广播，SDK 转为 `Event::PeerOnline { peer_id }` 与 `Event::PeerOffline { peer_id }`，与来向连接同从 `relay_link.next_event()` 取得：

```rust
while let Some(event) = relay_link.next_event().await {
    match event {
        Event::Channel(peer_link) => { /* 来向连接，见上节 */ }
        Event::PeerOnline { peer_id } => { /* 可向其 connect_peer */ }
        Event::PeerOffline { peer_id } => { /* 撤下该 peer 的在线状态 */ }
    }
}
```

边界：

- 广播不含当事人：自己不会收到自己的上下线；
- 没有初始在线列表：`connect_relay` 时已在场的 peer 不产生事件，此后各自的上下线才可见；
- 收到 `PeerOffline` 时，本端与该 peer 的连接已被 SDK 拆除，`on_close` 回调照常触发；
- 事件只投递给 `next_event` 的消费者，不消费则积压（无界），见 [issues/unbounded-queues.md](../issues/unbounded-queues.md)。

## ICE 配置（STUN/TURN）

三层合并生效：

```rust
// ① 静态预置：自建 STUN/TURN；配置后不再默认使用内置公共 STUN
let peer = Peer::builder()
    .id("alice")
    .ice_server(IceServer::stun("stun:stun.example.com:3478"))
    .ice_server(IceServer::turn("turn:turn.example.com:3478", "user", "secret"))
    .build();

// ② relay 下发：relay 配置了 TURN 时在注册后自动签发限时凭证并追加（见 relay-sdk.md）

// ③ 单次拨号追加：拨难达对端时临时补充，不持久化
let peer_link = relay_link
    .connect_peer_with(
        "bob",
        ConnectOptions {
            ice_servers: vec![IceServer::turn("turn:relay.example.com:3478", "u", "p")],
        },
    )
    .await?;
```

被叫方没有拨号选项入口，使用链路级配置（①+②）。

来向连接可被裁决：`relay_link.on_incoming(handler)` 在 offer 到达时询问（`incoming(&self, from) -> bool`），返回 `false` 拒绝，SDK 回 `decline`，发起方 `connect_peer` 立即收到 `Rejected("declined by peer")`。未注册 handler 时自动接受（旧行为不变）。不消费 `next_event` 的宿主注意：被接受的来向连接保持在线，其消息留在队列里无人读取。

## 连接生命周期

句柄可克隆共享同一连接。末位句柄被 drop、或任一句柄调用 `close`，连接即回收：本端表项清除、对端通道关闭。无句柄时可用 `relay_link.close_peer(to)` 显式拆除。`close` 与 drop 之后 `recv` 返回 `None`。

关闭可被感知：`peer_link.on_close(handler)` 在连接关闭时回调一次，远端关闭、本地 `close`/`close_peer`、对端下线、末位句柄 drop 都会触发，回调签名与 `MsgHandler` 同构（`on_close(&self, link, peer)`）。单槽语义：重复注册以最后一次为准，回调至多触发一次。

## 拉取式收发

```rust
peer_link.send("hi").await?;
while let Some(bytes) = peer_link.recv().await { /* 自行解析 */ }
peer_link.close();
```

## 回调式消费

```rust
peer_link.on_msg(Echo).await;    // 此后 recv 返回 None
```

```rust
struct Echo;

#[async_trait]
impl MsgHandler for Echo {
    async fn on_msg(&self, link: LinkHandle, from: PeerId, payload: Bytes) {
        let _ = link.send(from, payload).await;
    }
}
```

回调自身持有连接存活：注册后即可弃置句柄，回调随连接关闭而结束。回调在该连接内按到达顺序逐条执行；回信用参数里的 `LinkHandle::send`，它只对已建立的连接有效，未连接返回 `Rejected`，建立新连接显式走 `connect_peer`。

## 边界

- `on_msg` 与 `recv` 互斥：`on_msg` 取走来向队列，此后 `recv` 立即返回 `None`；
- 持有 `PeerLink` 而不消费时队列会积压（无界），见 [issues/unbounded-queues.md](../issues/unbounded-queues.md)；
- 回调是逐条顺序执行，慢回调推迟同连接的后续消息；需要并发时在回调内自行 spawn。
