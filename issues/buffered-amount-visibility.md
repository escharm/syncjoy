# 发送侧积压不可见，buffered_amount 是否透传

## 后果

`PeerLink::send` 底层入队即返回（webrtc-sctp 的 `pending_queue`），网络慢时发送侧持续堆积，宿主无从感知，表现为内存上涨与消息延迟增大而无信号。

## 现状

- 源码验证：`dc.send()` 不阻塞，受 cwnd/rwnd 门控的只是从队列外发的速率；发送方积压量就是 SCTP 流的 `buffered_amount`；
- 浏览器以 `bufferedAmount` + `bufferedamountlow` 事件把该值暴露给应用，是暂停/恢复发送的标准依据；
- webrtc-rs 的 `Stream` 已有 `buffered_amount` 字段，peer SDK 的 `DataChannel` 封装未透传。

## open question

是否在 `PeerLink` 上暴露只读查询（如 `buffered_amount() -> usize`）：纯信息性 API，不含策略，与浏览器对齐；还是维持现状，需要背压信号的宿主自行在上层计量。
