# relay 心跳

- [x] relay 对 peer 连接定时发 `ping`（间隔先取 30s），超时未回 pong 断开
- [x] 断开时走既有清理路径（unregister、对端通道补发 `closed`）
- [x] 补一条 relay 单测：半开 peer 的表项在超时后被清除，同 id 可重新注册
