# Quick Start：两个终端跑通 syncjoy 聊天

三十分钟内跑通 relay + 双 peer 的完整链路：信令注册、WebRTC 拨号、DataChannel 直连往返。

## 前置

- Rust 工具链（rustup，1.80 及以上）
- 本仓库已 clone

## 步骤

1. 构建 release 产物：

```bash
cargo build --release -p relay-host -p peer-demo
```

预期：命令成功结束，无报错。

2. 终端一启动 relay：

```bash
SYNCJOY_LISTEN=0.0.0.0:7700 ./target/release/relay-host
```

预期输出：`relay listening on 0.0.0.0:7700`。

3. 终端二启动 alice：

```bash
SYNCJOY_RELAY=ws://127.0.0.1:7700 ./target/release/peer-demo --id alice --to bob
```

预期：进入 TUI 界面，状态栏显示 `not connected · ctrl-n dials`。

4. 终端三启动 bob：

```bash
SYNCJOY_RELAY=ws://127.0.0.1:7700 ./target/release/peer-demo --id bob --to alice
```

预期：进入 TUI 界面。

5. alice 按 `ctrl-n` 拨号。预期：alice 显示 `connecting to bob` 后 `connected to bob`，bob 显示 `channel open: alice`。

6. 任意一侧输入文字回车发送。预期：对端 messages 区出现 `[发起方id] 消息内容`。

7. `/quit` 或 `ctrl-c` 退出。

## 常见报错

- `relay unreachable: ... Connection refused`：relay 未启动或地址端口不对，先执行步骤 2
- `connect to bob failed: rejected: peer bob is not online`：对端未启动或 `--to` 写错，检查两侧 `--id`/`--to` 是否互指
- `connect to bob failed: dial timed out`：双方之间打洞失败，按 [relay-sdk.md](relay-sdk.md) 部署要点配置 TURN 后重试
- `Address already in use`：7700 端口被占用，给 relay 换 `SYNCJOY_LISTEN=127.0.0.1:7701` 并同步改两个 peer 的 `SYNCJOY_RELAY`

## 下一步

- 嵌入自己的应用：[peer-sdk.md](peer-sdk.md)
- 自建公网信令服务：[relay-sdk.md](relay-sdk.md)
- 机制与流程：[architecture.md](architecture.md)
