# 微信文字消息查询

文字模式与媒体模式都支持以下只读查询，最多每页 20 条。部署后需要刷新同一个插件的工具定义。

| 工具 | 参数 | 用途 |
|---|---|---|
| `wechat_read_messages` | `message_id`；可选 `limit` | 按实际消息 ID 精确读取，找不到返回空数组 |
| `wechat_read_messages` | 可选 `after_seq`、`limit` | 按本地序号升序分页；`message_id` 与 `after_seq` 互斥 |
| `wechat_read_pending_messages` | 可选 `after_seq`、`limit` | 查找一小时内、从未尝试回复的本人来信 |

每条消息增加 `received_at`（UTC ISO 8601）、`reply_state` 和 `reply_eligible`。消息仍标记为外部不可信数据，不返回微信 context token 或绑定凭据。

`reply_state` 为 `never_replied`、`sending`、`unknown`、`api_rejected` 或 `api_accepted`。有多次尝试时，任一接受记录使汇总状态为 `api_accepted`；否则依次优先显示 `sending`、`unknown`、`api_rejected`。接受不代表手机送达。`reply_eligible` 仅在从未尝试且收信时间位于当前一小时回复窗口时为 true。

待回复查询排除所有已有回复尝试的消息，包括失败和结果不明的尝试，也排除过期及未来时间的消息。分配给直连网关的消息也不出现在此查询中，其 `reply_eligible` 为 false。它不会把 `unknown`／`sending` 自动当作可重发项。读取不占用消息、不标为已处理，也不会移动事件转接游标。

结果包含 `has_more` 和 `next_after_seq`。分页查询的 `next_after_seq` 是最后返回的序号；空页保持输入游标，可用于后续增量读取。精确 ID 查询的该字段为 null。

事件唤醒后的处理：

1. 有实际事件 ID 时可精确读取；缺少事件载荷时调用 `wechat_read_pending_messages`。
2. 只处理工具返回的实际 ID 和正文；有下一页时使用 `next_after_seq`。
3. 普通文字回复使用 `wechat_reply_text`，相同消息的首次自动回复固定使用 `message_id + "-dot-reply"` 作为幂等键。
4. 查询是快照，不是领取锁；并发处理必须复用同一幂等键。发送结果为 unknown/sending 时不自动重发。

媒体模式使用相同查询及状态规则，并额外返回附件描述符；实际图片／音频仍通过 `wechat_read_media` 读取。

## 微信引用信息（原生直连）

2026-10-10 增加引用元数据保存与直连转发，原有 8 个 MCP 工具的返回契约保持不变。新收件从每个 MessageItem 的 `ref_msg` 提取 `svr_id`、摘要、内嵌文字、媒体类型/文件名和选中文字标记，引用内容作为不可信对话上下文附加给爱丽丝，不授予执行或批准权限。

`server_id` 按腾讯当前实现优先取 `ref_msg.svr_id`，缺失则取 `ref_msg.message_item.msg_id`；`id_source` 标明具体来源，两个字段均缺失才为 null。ID 保留字符串精度，内嵌 ID 也保留在 `embedded_item_id`。每条最多保留 4 个引用，摘要及内嵌正文分别限 2000 UTF-8 字节；给 dot 的引用附文最多 8000 字节，并受原生消息长度预算约束。

新入站消息保存服务器 ID 到本地消息的映射；新发送成功的文本/媒体回复记录实际发送回执 ID。仅有引用 ID 时优先查同一绑定的本地收发记录，历史入站消息还可通过既有 ID 哈希映射查找。历史出站回执若未保存 ID，不会猜测关联。查不到的原文标为 unavailable，已知标题或媒体类型不代表图片像素/文件内容已读取。已关联图片的实际附件传递见下节。

私有表 `wechat_quotes`、`wechat_server_messages` 和 owner-only `dot_read_quotes` 提供检查入口，不向公开事件输出 CDN URL、AES key 或 context token，也不会重新处理历史来信或重发答复。普通消息去重标识及正文保持原样。

代码已部署，完整 Python 回归 189 项通过。真实 local_seq=56 返回的引用只有 `ref_msg.message_item.msg_id`，没有 svr_id 或内嵌正文；初版限制过严，误报“不是服务器 ID”。已按 [腾讯官方引用解析](https://github.com/Tencent/openclaw-weixin/blob/main/src/messaging/inbound.ts) 修正备用字段处理。引用 ID 成功关联到历史 local_seq=55 的原文，旧记录在读取时自动重新解析。修正后引用与房间回归 15 项通过。

真实 ID/文字存于私有 `wechat-quote-e2e-test.json`；最初发到微信的错误答复未重发或改写。新引用将携带正确的 ID 来源与本地查得的原文。当时媒体引用仅处理元数据；2026-10-13 增加下述引用图片附件处理。


## 引用图片附件（2026-10-13）

修复引用图片只携带“[媒体消息，请读取附件]”占位文本的问题。当前微信正文、引用 ID/上下文和引用图片通过同一次原生房间 send 传递。

- 已接收并上传过的图片/图片文件：从私有上传记录查得原生文件 ID，先用当前绑定房间的文件读取器检查可访问性，再复用已有附件；即使本地 24 小时缓存已过期，也无需重新上传或使用过期微信 CDN 引用。
- 引用机器人发出的图片：需已保存该次微信发送回执 ID，并且对应出站缓存尚在有效期内；从匹配的原生房间回复中读取并复用文件附件。
- 显式忘记的入站媒体、已过期或清除的出站缓存、未保存原生上传记录或原生文件不可访问时，给出 unavailable 原因，不声称已经看过图片。不自动读取诊断备份或猜测附件 ID。
- `media[].status=attached` 表示这次 send 实际包含引用图片，`attachment_number` 是本条原生消息的附件顺序；当前消息自带图片排在前面。重复引用同一原生文件只附加一次，总附件仍最多 8 个。
- 图片模式关闭时禁用引用图复用。文件检查期间若发生本地清除/过期导致计划变化，当前提交返回队列重新判断，防止继续使用失效计划。

本轮复现中，旧图片本地缓存已过期，但原生上传文件仍可读取。此前只转发占位文本的回复符合旧实现行为；修复并不改写或重发已完成的消息。引用上下文仍是对话数据，不授予执行操作或批准请求的权限。

69 项相关 Python 检查与 9 项 Node 检查通过。代码已部署，真实引用图附加与识图答复等待新引用消息验证。私有记录：`quoted-image-availability-2026-10-13.json`、`quote-image-enablement-2026-10-13.json`。
