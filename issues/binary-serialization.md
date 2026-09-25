# 消息编码的剩余问题

payload 已定为不透明字节，载体是 WebRTC DataChannel 的原始字节，本 issue 只保留尚未拍板的部分。

## open questions

1. 信令面是否也二进制化：现保持 JSON 文本帧，调试友好；若未来信令频率升高（心跳、批量上下线事件），再评估紧凑编码；
2. DataChannel 载荷无帧内版本：依赖信令面 hello 时的版本协商，若未来出现"同进程多版本连接"再补帧内版本；
3. 是否为常见编码提供可选工具 crate（如 `syncjoy-codecs` 提供 JSON/protobuf 适配），当前倾向不提供，等真实使用方出现再定。
