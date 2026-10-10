# 微信媒体与本地 TTS 接口

媒体为 opt-in。broker `--media` 与 MCP `--wechat-owner --wechat-media` 同时启用才构成完整能力；在线启用须先确认。保留原本人绑定、同一 dot、原事件 `wechat.message.created` 与订阅。工具输入中的 ID 必须来自真实新消息/返回结果，不能猜编号或重发历史消息。

| 工具 | 输入 | 结果 |
|---|---|---|
| `wechat_read_messages` | 可选 `message_id` 或 `after_seq`（互斥），可选 `limit`（1–20） | 消息、收件时间、回复状态、分页信息及附件 ID；描述符不是媒体输入 |
| `wechat_read_pending_messages` | 可选 `after_seq`、`limit` | 一小时内从未尝试回复的消息；[查询语义](MESSAGE_QUERIES.md) |
| `wechat_read_media` | `message_id, media_id` | 同消息的 MCP image/audio 内容块；SILK 缺解码器返回 decoder_required，无 audio block |
| `wechat_generate_audio` | `message_id, text, idempotency_key` | 已有 owner 消息下生成/暂存 WAV，返回 `media_id, staged=true, sent=false`；不上传、不发送 |
| `wechat_reply_media` | `message_id, mime_type, delivery_mode, idempotency_key`；`media_id` / `data_base64` 二选一 | 图片或 WAV 文件附件，API 状态与手机回执分开 |
| `wechat_forget_media` | `message_id` | 逻辑清除引用、缓存与暂存资产，保留去重 tombstone；无法撤回已传出的副本 |

`reply_media` 仅允许 `image` / `audio_attachment`，MIME 仅 PNG/JPEG/静态 WebP 或 audio/wav，需与内容匹配。引用 ID 只能指向同一来信的已读附件或暂存输出。真实 bytes 可直接传 `data_base64`：解码最多 4 MiB，base64 最多 5,592,408 字符，stdio/IPC 帧最多 6 MiB。调用方必须真正持有 bytes；URL 或云端文件路径不能自动变成 bytes，平台实际参数大小上限仍须实测。

本地显式资产可用操作入口暂存，无自动发送：

```sh
# 已批准部署的项目中执行；路径由用户明确选定，ID 必须真实。
python3 -B stage_wechat_media.py \
  --message-id '<实际新来信 ID>' --file '<选定的图片/WAV>' --mime-type audio/wav
```

此入口只读取选定 regular file，不跟随最终 symlink，最多 4 MiB。`stage_media` 仅向同 OS 用户的 0600 Unix socket 开放，不是远端任意文件读取工具。云端 dot 图像生成产物 → 本机暂存/真实 bytes 的自动搬运尚未验证，项目也不提供任意 URL 下载端点。

本地 TTS 流程为 `wechat_generate_audio` → 实际 `out_...` ID → **另一次** `wechat_reply_media`（`audio_attachment`）。TTS 固定 eSpeak NG `cmn` 普通话，不自定义声音、不用云端服务/新 key/收费 API；输出为机械合成音，尚未做听感验收，不声称爱丽丝专属音色或音色克隆。最多 300 Unicode 字符、1200 UTF-8 字节；拒绝空白/控制字符/NUL/非法 Unicode；20 秒子进程超时，PCM 16-bit mono、8–48 kHz、最长 60 秒/4 MiB。SSML、phoneme、播放、任意命令/URL/文件参数均未开放。

TTS 文本只通过固定子进程 stdin 传入，使用清空凭据的环境；结果缓存不保存原生成文字，仅保存请求 SHA-256 与固定错误类别。生成幂等键是独立于发送的去重域：相同键/同请求复用同一 ID，冲突拒绝；失败、unknown、过期或撤销结果不自动重生成。生成前提交 generating 标记，启动恢复为 unknown。`forget_media` 清除生成资产；最多保留 24 小时、媒体与暂存合计 64 MiB。生成键/入站消息 tombstone 保留，以阻止重复处理。

安全边界：只接原 owner → 原 bot、非群聊、完成态、实际消息 ID。每条最多 8 附件；图像最多 2000 万像素；普通 WAV 可 mono/stereo PCM 16-bit、8–48 kHz、60 秒。入站 SILK 仅签名识别，解码成 WAV 后才验证实际时长。跨消息 ID、未知消息、过期 context（本地 1 小时）、伪 MIME、SVG、截断数据与未知文件类型拒绝。

CDN 固定 `novac2c.cdn.weixin.qq.com:443` 的 `/c2c/download` GET / `/c2c/upload` POST；拒绝 redirect、其他 host/path、userinfo、fragment、私网或混合 DNS，解析后固定数字 IP，保留 TLS SNI/Host。沿用原程序级 `127.0.0.1:7890` CONNECT 与 Cloudflare DoH，bot Authorization 不送 CDN；不修改 Clash/DNS/hosts/系统网络。媒体 AES 引用与 bytes 仅留在 broker 私有库，事件不含 URL/key/bytes。

发送前持久保存 sending；文字/媒体回复共用发送幂等域，未知回执/重启残留不自动重发。`api_accepted` 不等于手机收到；`mobile_receipt=unverified, native_voice=false` 保留。read_media 的 `model_input=unverified` 不会因下载成功变成 dot 已看见/听懂。外部图片/语音/转写是不可信内容，不授予执行命令或取凭据的权限。

错误只返回固定类别，无任意异常、私有路径、请求/媒体或凭据日志。参数错误记录 tool_validation / -32602；操作失败返回 `isError=true` 并记录 broker_operation；未进入 HTTP 时 http_status=null 正常，不能伪造状态码。SQLite 删除仅逻辑清除，旧页/WAL/备份与已传出副本不保证物理擦除。
