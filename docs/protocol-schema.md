# 信令协议 schema

## 传输与帧格式

- 承载：WebSocket 文本帧，端点固定为 `/syncjoy/v1/peer`
- 帧：`{"v":<协议版本>,"msg":<消息>}`，当前 `v = 6`
- 版本不符的帧拒收（`UnsupportedVersion`），连接随之断开
- 消息以 `type` 字段区分，取值 snake_case

## PeerId

JSON 字符串。约束：可打印 ASCII（`0x21..=0x7E`，不含空格），长度不超过 128 字节。两侧在反序列化时强制校验，超限即坏帧。

## peer → relay

| type | 字段 | 语义 |
|---|---|---|
| `hello` | `peer_id: PeerId`，`token?: string` | 注册；同 id 顶替旧连接；`token` 供宿主 guard 校验 |
| `offer` | `to: PeerId`，`sdp: string` | 发起协商 |
| `answer` | `to: PeerId`，`sdp: string` | 应答协商 |
| `ice` | `to: PeerId`，`candidate: IceCandidate` | trickle 候选交换 |
| `decline` | `to: PeerId` | 拒绝来向连接请求 |
| `pong` | 无 | 心跳应答 |

`IceCandidate`：`{ candidate: string, sdp_mid?: string, sdp_mline_index?: u16 }`。

## relay → peer

| type | 字段 | 语义 |
|---|---|---|
| `ack` | 无 | 注册回执 |
| `ping` | 无 | 心跳探测 |
| `turn` | `urls: [string]`，`username: string`，`credential: string` | TURN 凭证；注册成功后下发，此后每半 TTL 重签 |
| `offer_relay` | `from: PeerId`，`sdp: string` | 路由的 offer |
| `answer_relay` | `from: PeerId`，`sdp: string` | 路由的 answer |
| `ice_relay` | `from: PeerId`，`candidate: IceCandidate` | 路由的候选 |
| `rejected` | `to: PeerId`，`reason: string` | 路由目标不在线（offer/answer/ice/decline），或 hello 未过准入 |
| `peer_online` / `peer_offline` | `peer_id: PeerId` | 兼容保留：类型可解码，relay 不再发送，peer SDK 收到即忽略 |

## TURN 凭证格式

coturn REST 式：`username = "{过期 epoch 秒}:{peer_id}"`，`credential = base64(HMAC-SHA1(secret, username))`。默认 TTL 3600 秒，每半 TTL 重签一次。

## 保活与超时

| 参数 | 默认 | 说明 |
|---|---|---|
| hello 超时 | 10s | 连接建立后首帧必须是 `hello`，超时断开 |
| 心跳间隔 | 30s | relay 对每个已注册连接发 `ping` |
| 心跳超时 | 10s | 未收到 `pong` 即断开并注销 |

帧与写缓冲上限属宿主接线配置（参考 relay-host 的 64KiB 帧上限、1MiB 写缓冲）。

## 示例帧

```json
{"v":6,"msg":{"type":"hello","peer_id":"alice"}}
{"v":6,"msg":{"type":"ack"}}
{"v":6,"msg":{"type":"offer","to":"bob","sdp":"v=0..."}}
{"v":6,"msg":{"type":"offer_relay","from":"alice","sdp":"v=0..."}}
{"v":6,"msg":{"type":"rejected","to":"bob","reason":"peer bob is not online"}}
```
