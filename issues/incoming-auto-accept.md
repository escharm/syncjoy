# 来向 offer 自动应答，被叫方无拒绝手段

## 后果

来向 `offer` 一到即自动创建 RTCPeerConnection 并应答，DataChannel 打开后被叫方才在 `Event::Channel` 里拿到通道，此前没有 accept/refuse 的机会。任何在线 peer 可以对任意在线 peer 发起直连协商，被叫方宿主只能接受后再断开（relay 侧无授权点可兜底）。

## 现状

- 应答是无条件的：被叫方 SDK 不做任何检查即 `answer`，没有等待被叫确认的中间态；
- 被叫方宿主在 `Event::Channel` 里拿到已开通的 DataChannel，语义是"已接通"而非"待接受"。

## open questions

1. 协议是否需要 `accept/refuse` 帧，还是被叫方接受后在业务层自行 `close` 即可；
2. 若引入确认环节，被叫无响应时的超时与发起方的失败语义；
3. 通道粒度的授权（允许 alice 连 bob，不允许连 carol）如何由被叫方宿主表达（accept/refuse 或收到后 close）。
