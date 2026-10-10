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
