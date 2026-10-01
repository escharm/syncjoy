# relay 无心跳，半开连接占住 peer id

## 后果

peer 侧网络单断（进程未退出、TCP 未关闭）时，relay 注册表里的表项成为幽灵：该 peer id 看似在线，指向它的 `offer` 会被路由进黑洞，发起方等到的只有 15 秒拨号超时，直到同 id 新连接顶替才恢复。

## 状态

已解决：relay 对每个已注册连接定时发 `ping`（默认间隔 30s，超时 10s），超时未回 `pong` 即视为半开，断开并走既有清理路径（注销、广播 `peer_offline`），幽灵表项随连接一起被清除。间隔与超时经 `Relay::with_heartbeat(HeartbeatConfig)` 可调，宿主默认无需配置。

## 现状

- 协议定义 `ping`/`pong`（两个方向都有），relay 定时发送 `ping`，peer SDK 的信令泵收到即回 `pong`；
- 同 id 重复注册会顶替旧连接（代数校验保证旧连接退出时不误删新注册），这条路径能自愈部分场景；
- peer 的信令连接正常断开时 relay 会广播 `peer_offline`，对端据此拆除 DataChannel；心跳超时触发的断开同样广播 `peer_offline`。

## open questions

1. 探测间隔与超时阈值取多少（内网 peer 的出站长连，公网中间设备空闲超时常见的 5 分钟量级是否要主动避让）：当前默认 30s/10s，30s 的主动探测已避开常见空闲超时，如现网反馈再调；
2. peer 侧半开连接由谁负责断言：relay 侧超时踢除，还是 peer SDK 提供自己的心跳 API：已按 relay 侧踢除落地；
3. 同 id 顶替是自愈手段也是攻击面：relay 无鉴权，任何后来者都能顶掉先来者，是否需要顶替协商（如断开码、互斥凭证）。
