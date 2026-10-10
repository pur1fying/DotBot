# DotBot

把微信 ClawBot 中已绑定本人的消息转给**同一个既有 dot（爱丽丝）**，通过 iLink、本机 owner broker、原 Secure MCP Tunnel 和 MCP Events 回复。不会创建替代助手。

此仓库迁移了原文字转接源码，并加入消息查询、可选图片/音频收发和本地中文语音合成。媒体/TTS 扩展仅完成源码与离线验证，**尚未启用持久媒体授权**；文字查询可独立部署，无需开启媒体模式。

| 能力 | 本地代码 | 尚待真实验收 |
|---|---|---|
| 微信图片 → dot | 按需下载、解密、校验，返回 MCP image 内容块 | 同一 dot 实际识别图像 |
| 微信语音 → dot | WAV 返回 audio 内容块；SILK 支持可选解码器；微信转写标明来源 | 安装并验证 SILK 依赖、dot 实际听懂 |
| dot 图片 → 微信 | 真实 bytes 或已读/暂存媒体 ID，图片消息发送 | 手机实收；云端生成产物搬运 |
| dot 音频 → 微信 | PCM WAV **文件附件**发送 | 手机收到并播放；原生语音气泡未实现 |
| dot 文字 → 本地音频 | `wechat_generate_audio` 用已有 eSpeak NG `cmn` 合成并暂存 | 真人自然度/用户听感未验收；不自动发送 |

源码入口：`service.py` 为 stdio MCP；`ilink_broker.py` 独占本人绑定与私有消息库；`media_adapter.py` 处理媒体、缓存和幂等；`local_tts.py` 使用固定隔离子进程合成中文 WAV。文字模式提供 8 个工具、2 个事件，支持按 ID、分页和待回复消息查询；媒体模式增加四个工具，总计 12 个工具、2 个事件。查询参数及回复状态见 [消息查询接口](docs/MESSAGE_QUERIES.md)；合成 sandbox 模式继续可用。

Cloudflare DoH 在进程内按 A/AAAA 分别缓存已验证的公网地址，取地址及 CNAME 的最短 TTL，扣除 HTTP `Age` 和查询耗时，最多缓存 300 秒；TTL 无效、空结果和失败不缓存，也不回退到过期地址。并发查询共用一次解析。事件出口配置不变时复用传输对象，但每次操作仍重读配置；禁用、缺失、无效或变更配置会丢弃旧缓存。重启后首次查询为冷缓存。

dot 网关已实现 broker 持久队列、原生房间提交/订阅、回复父消息关联、恢复、分片及运维命令，并通过真实爱丽丝房间协议测试。当前采用需桌面进程存活的原生客户端复用模式；微信端到端验收前仅启用明确测试前缀。状态、部署与回滚见 [直连接入说明](docs/DIRECT_GATEWAY.md)。

离线测试：

```sh
python3 -B -m unittest discover -v
```

Python 测试依赖版本见 `requirements-test.txt`。完整测试含真实本地 eSpeak NG 库和临时 Unix socket，需要系统已有 `libespeak-ng` 与 `cmn` 数据。测试不读取真实凭据，不联系腾讯/OpenAI，不发送微信。不要通过跳过语音/IPC 测试宣称这些能力通过。

[媒体接口与保护](docs/MEDIA.md)、[启用、撤销及最小实测](docs/ENABLEMENT.md)、[依赖与官方证据](docs/DEPENDENCIES.md)、[迁移来源](MIGRATION.md)。运行时、凭据、私人对话/媒体、日志、缓存及本机 Tunnel 配置不进入 Git；源码推送不等于部署。
