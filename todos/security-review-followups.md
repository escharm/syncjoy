# 安全重审遗留跟进

来源：无权限前提下的安全重审（逐项重审 + 独立复核）。已落地的修复：hello 前超时、TURN 凭证重签、relay-host 帧与写缓冲上限、信令泵与用户回调解耦、DataChannel 关闭回收 PeerConnection、PeerId 边界、web 端 sdpMid、answer/ice/decline 路由失败回执、`connect_relay` 握手/应答超时、信令断开 `RelayClosed` 事件与在途拨号立即失败、`on_close` 末位句柄 drop 契约、连接代次与拨号合流、在线广播删除（presence 放大与在线 id 可见面随之消除，决策见 [decisions/no-presence-broadcast.md](../decisions/no-presence-broadcast.md)）。以下条目待明确。

## 待明确场景

- [ ] 通道队列背压：需先区分「宿主从不消费（更像代码错误，期望尽早暴露）」与「消费慢于对端（真实背压需求）」两个场景，场景细分清单见 [issues/unbounded-queues.md](../issues/unbounded-queues.md)

## 策略待定（已部分缓解）

- [ ] relay 信令面速率限制：帧上限（64KiB）、hello 超时、PeerId 边界、在线广播删除已落地；剩余暴露面是每连接对任意在线 peer 的路由帧洪泛（offer/answer/ice 直投无界 inbox）。需先定拒绝策略（同类问题的讨论见 [issues/unbounded-queues.md](../issues/unbounded-queues.md)）
