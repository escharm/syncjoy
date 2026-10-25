# TURN 凭证生命周期走宿主端口

## 结论

TURN 凭证的签发与撤销做成 `TurnProvisioner` 端口由宿主注入（`Relay::with_turn_provisioner`），SDK 只掌握三个时机：准入后签发、按签发返回的 `refresh_after` 续签、会话终结对最后一次成功签发撤销（异步派发不阻塞回收）。内置 REST 静态密钥实现收敛为该端口的内置适配器，对外行为不变。无 guard 不签发的规则对端口统一生效（背景见 turn-credentials-require-guard.md）。

## 理由

- 可撤销账号（coturn lt-cred-mech + userdb/Redis，每 peer 独立账号、断连即删）是凭证转发问题的现实解法，但它要求有人替 relay 触碰账号存储；
- SDK 直说 coturn 方言（SQL、turnadmin、schema）会把部署拓扑焊进核心，违背零框架依赖与水平扩展前提；端口把 syncjoy 对 TURN 的知识收窄为时机，适配器约二十行归属宿主；
- 撤销按签发实例而非 peer_id：同名顶替时旧会话退出触发的是自己那次签发的 revoke，宿主把用户名按签发实例唯一（如随机后缀）即可不误删新会话的账号。

## 否决过的方案

- SDK 直连 coturn userdb：耦合部署拓扑与 schema，否决；
- HTTP 签发服务进核心：多一个进程组件，宿主想走 HTTP 在适配器里自行请求即可，核心不需要知道，否决；
- 约定共享表名让双方解耦地读写同一张表：隐式耦合，schema 变更两头炸，显式 trait 才是缝，否决；
- 靠 peer 端删除凭证：对转发出去的副本无效，否决；
- RFC 7635（coturn 已实现 `--oauth`）：浏览器 WebRTC 与 webrtc-rs 客户端均不支持，现状走不通，留待客户端生态变化后重议。
