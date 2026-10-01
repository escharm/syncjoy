# relay SDK 使用方式

relay 是零框架依赖的信令核心：注册表、TURN 凭证签发、信令会话状态机，不绑定任何 HTTP 库。宿主自己选框架、自己收 WebSocket，把连接折成一对通道交给会话入口，即获得完整信令语义（hello/ack 握手、注册与上下线广播、SDP/ICE 路由、TURN 下发）。数据面在 peer 之间直连，relay 不转发应用数据：公网中继承担全部流量会把它变成带宽与延迟的瓶颈，而 NAT 打洞后 peer 本可直连。

## 宿主接线

```rust
use syncjoy_relay::{session, Relay};

// inbound: 收到对端文本帧就送入；outbound: 收到 relay 文本帧就写回 WebSocket
session::run(relay, inbound_rx, outbound_tx).await;
```

契约只有两条：WebSocket 升级端点挂在 `/syncjoy/v1/peer`（peer SDK 的固定拨入路径）；文本帧按原样进出通道。探活等自有端点由宿主路由自理。

完整参考实现在 `examples/relay-host`（axum + healthz + TURN 环境变量），其他框架（actix、warp、裸 hyper）同构：收 ws、开通道对、spawn `session::run`。

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

<a id="guard"></a>
## 注册准入

relay 面向公网，谁都能发 hello 冒充任意 peer_id 顶替旧连接。宿主用 `Relay::with_guard` 注入准入回调后，每个 hello 先过 guard 再入注册表，返回 false 的连接收到 `rejected { reason: registration denied }` 并被断开，不触发注册与上下线广播；不配 guard 时所有 hello 放行，行为与从前一致：

```rust
let relay = Relay::new().with_guard(|peer_id, token| match token {
    Some(token) => check(peer_id, token),
    None => false,
});
```

guard 是纯同步判断，入参只有 hello 帧里的 peer_id 与可选 token；凭证的签发与验证属于使用方的业务上下文，SDK 不代劳。peer 侧凭据经 `Peer::builder().relay_token(...)` 配置，随 hello 帧发出（见 [peer-sdk.md](peer-sdk.md#connect)），不进拨号 URL，避免落入访问日志；被拒时 `connect_relay` 返回 `PeerError::Rejected`，携带 relay 给出的原因。

## 信令面做什么

| 消息 | 方向 | 行为 |
|---|---|---|
| `hello { peer_id, token? }` | peer → relay | 注册（同 id 顶替旧连接）；配置 guard 时先校验准入，不过则回 `rejected` 并断开 |
| `ack` / `turn` | relay → peer | 注册回执、TURN 凭证 |
| `offer` / `answer` / `ice` | peer → relay | 携带目标 peer id，relay 点对点路由 |
| `offer_relay` / `answer_relay` / `ice_relay` | relay → peer | 路由结果，附来源 peer id |
| `peer_online` / `peer_offline` | relay → peer | 上下线广播（不含当事 peer） |
| `rejected { to }` | relay → peer | offer 目标不在线，或 hello 未过准入 |

relay 不解读 SDP 与 ICE 内容，只按 peer id 转交；无鉴权，准入是使用方注入的回调（见 [注册准入](#guard)），不配 guard 则全部放行。鉴权所需的业务上下文（谁认识谁、谁授权谁连谁）只存在于使用方，SDK 内置的授权点只能回放使用方自己注入的判断。

## 部署要点

- relay 应部署在有公网 IP 的主机上，进程只监听一个 TCP 端口，只承载低频信令流量；
- TLS 交给反代（如 nginx、caddy）或宿主自行处理，peer 用 `wss://` 地址接入；
- 需要穿透兜底时，同机或同内网部署 coturn，把 `turn:` 地址与 REST 密钥配给 `Relay::with_turn`；
- relay 无业务状态、可水平扩展，但 v0 的注册表在进程内存里，多副本需要粘性会话或共享注册表（未实现，见 roadmap）。
