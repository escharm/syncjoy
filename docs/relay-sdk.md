# relay SDK 使用方式

relay 的定位是"嵌进宿主服务的信令组件"，不是独立进程。它是 peer 们拨入的 WebSocket server，职责是注册表与连接协商路由；数据面在 peer 之间直连，relay 不转发应用数据：公网中继承担全部流量会把它变成带宽与延迟的瓶颈，而 NAT 打洞后 peer 本可直连。

## 起一个 relay

```rust
use syncjoy_relay::Relay;

let relay = Relay::new();
let app = relay.router();
```

`Relay::router()` 挂载两个端点：`GET /syncjoy/healthz` 探活，`GET /syncjoy/v1/peer` 信令升级。完整可运行版本见 `examples/relay-host`。

## TURN 协助

穿透失败的兜底由 TURN 承担。relay 配置了 coturn 的地址与 REST 密钥时，向每个注册成功的 peer 签发限时凭证（HMAC，默认 1 小时），peer 自动把它加进 ICE server：

```rust
use syncjoy_relay::{Relay, TurnConfig};

let relay = Relay::with_turn(TurnConfig {
    urls: vec!["turn:turn.example.com:3478".to_owned()],
    secret: std::env::var("SYNCJOY_TURN_SECRET").unwrap(),
});
```

TURN 服务本身由部署层的 coturn 提供，SDK 不内嵌：把有状态的运维组件编译进宿主进程，UDP 端口与带宽管理反而复杂化。

## 信令面做什么

| 消息 | 方向 | 行为 |
|---|---|---|
| `hello { peer_id }` | peer → relay | 注册（同 id 顶替旧连接） |
| `ack` / `turn` | relay → peer | 注册回执、TURN 凭证 |
| `offer` / `answer` / `ice` | peer → relay | 携带目标 peer id，relay 点对点路由 |
| `offer_relay` / `answer_relay` / `ice_relay` | relay → peer | 路由结果，附来源 peer id |
| `peer_online` / `peer_offline` | relay → peer | 上下线广播（不含当事 peer） |
| `rejected { to }` | relay → peer | offer 目标不在线 |

relay 不解读 SDP 与 ICE 内容，只按 peer id 转交；无鉴权、无准入：鉴权所需的业务上下文（谁认识谁、谁授权谁连谁）只存在于使用方，SDK 内置的授权点只能回放使用方自己注入的判断。

## 挂进既有服务

宿主已有 axum Router 时，把 relay 的路由合并进去即可，路径前缀（`/syncjoy/*`）不会与业务路由冲突：

```rust
let app = Router::new()
    .route("/my-own-api", get(my_handler))
    .merge(relay.router());
```

## 部署要点

- relay 应部署在有公网 IP 的主机上，进程只监听一个 TCP 端口，只承载低频信令流量；
- TLS 交给反代（如 nginx、caddy）或宿主自行处理，peer 用 `wss://` 地址接入；
- 需要穿透兜底时，同机或同内网部署 coturn，把 `turn:` 地址与 REST 密钥配给 `Relay::with_turn`；
- relay 无业务状态、可水平扩展，但 v0 的注册表在进程内存里，多副本需要粘性会话或共享注册表（未实现，见 roadmap）。
