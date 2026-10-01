# Hello 帧新增可选 token 不 bump 协议版本

## 结论

`hello` 帧新增可选字段 `token`（`Option<String>`），`PROTOCOL_VERSION` 保持 6 不变。

## 理由

版本号的语义是拒绝不兼容的线上格式（`decode` 严格比对 v，不等即返回 `UnsupportedVersion`，见 crates/protocol/src/codec.rs），不是变更日志。本次变更在新旧两侧同时兼容：

- 新 relay 读老 peer 的 hello：字段缺失，`Option` 反序列化为 `None`，帧照常解码；
- 老 relay 读新 peer 的 hello：serde 默认忽略未知字段，多出的 `token` 不影响解码；未配置 guard 的 relay 本就不校验，行为不变；
- 新 peer 未配置凭据时，序列化结果与老版本逐字节一致（`skip_serializing_if`）。

bump 到 7 反而破坏硬需求：老 peer 的 v6 hello 会被新 relay 以 `UnsupportedVersion` 拒掉，连进入 guard 判断的机会都没有。拒绝与否应当由使用方注入的 guard 决定，不能由协议版本一刀切。

## 何时必须 bump

字段语义变化（含义复用、可选转必选、删除）导致新旧两侧无法互解时才 bump。单纯增加可选字段不在此列。
