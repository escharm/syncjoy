# syncjoy

跨网络的多点对等协作 SDK。任意参与方都是 peer：向部署在公网的 relay 注册自己（`connect_relay`），即可与指定节点建立 WebRTC 直连（`connect_peer`）。peer 主动出站连接 relay 信令，因此不需要公网 IP；数据面在 peer 之间直连，不经过 relay。

## 功能模块

| crate | 部署位置 | 职责 |
|---|---|---|
| `syncjoy-relay` | 公网主机 | 信令与连接协助：注册表与 SDP/ICE 路由、上下线广播、来向 `decline` 回击、可选 TURN 凭证签发；零框架依赖，宿主自选 HTTP 库（axum 接线参考 `examples/relay-host`）；无头，不转发应用数据 |
| `syncjoy-peer` | 任意参与方 | 对等节点 SDK：`connect_relay(relay)` 注册自己，`connect_peer(to)` 得到与该 peer 的 `PeerLink`（WebRTC 直连，`send`/`recv`/`on_msg`/`on_close`）；`on_incoming` 裁决来向（`peer-demo` 以 `--reject-incoming` 切换固定拒绝，可交叉测试）；ICE 三层配置（builder 预置、relay 下发、单次拨号追加）；native 与 wasm 双传输层 |
| `syncjoy-protocol` | 内部依赖 | relay 与 peer 共用的信令消息类型与编解码（v6） |

SDK 都不是独立服务。可运行的服务由 `examples/` 下的宿主程序组合出来，集成测试复用同一套组合。

## QuickStart

```bash
# 终端一：起 relay（默认监听 0.0.0.0:7700）
cargo run -p relay-host

# 终端二：peer bob 上线，对话对象 alice
SYNCJOY_RELAY=ws://127.0.0.1:7700 cargo run -p peer-demo -- --id bob --to alice

# 终端三：peer alice 上线，对话对象 bob
SYNCJOY_RELAY=ws://127.0.0.1:7700 cargo run -p peer-demo -- --id alice --to bob
```

两端是 ratatui 聊天界面：身份与对话对象在启动参数给定；Ctrl-N 发起 WebRTC 连接（即时反馈 `connecting`），连接后输入一行回车发送；`/quit` 或 Ctrl-C 退出。

`--reject-incoming` 使该端固定拒绝来向连接（TUI 操作相同，只能自己拨出），与默认端交叉测试接受与拒绝两个方向：

```bash
SYNCJOY_RELAY=ws://127.0.0.1:7700 cargo run -p peer-demo -- --id alice --to bob --reject-incoming
```

## 开发命令

```bash
cargo check --workspace --all-targets
cargo test --workspace
cargo xtask features        # feature 矩阵：默认构建、relay 零框架断言、peer wasm、互斥护栏
cargo xtask e2e             # 多进程 e2e：pty 驱动 relay-host + 双 peer-demo TUI 往返
```

## 目录结构

```text
syncjoy/
├── crates/protocol          线上消息类型与编解码
├── crates/relay             公网信令 SDK（零框架核心：registry/session/turn）
├── crates/peer              对等节点 SDK（native/web 双传输层）
├── examples/relay-host      宿主示例：axum 接线参考，最小公网 relay 服务
├── examples/peer-demo       宿主示例：交互式 peer，连外部 relay 与指定 peer 聊天
├── xtask/                   任务编排：feature 矩阵与多进程集成验证
├── docs/                    设计文档与操作指南
├── roadmap/                 规划中的方向
├── todos/                   近期待办
└── issues/                  已知问题与 open questions
```

## 文档导读

| 想了解 | 去哪里 |
|---|---|
| 对等模型如何协作、连接策略 | [docs/architecture.md](docs/architecture.md) |
| peer 的收发方式与 ICE 配置 | [docs/peer-sdk.md](docs/peer-sdk.md) |
| 如何嵌入 relay（任意框架宿主） | [docs/relay-sdk.md](docs/relay-sdk.md) |
| relay 包内部机制（信令时序、顶替护栏、TURN） | [crates/relay/README.md](crates/relay/README.md) |
| 接下来做什么 | [roadmap/](roadmap/) |
