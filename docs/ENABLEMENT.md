# 启用与最小真实验收

本次仅源码迁移和离线开发，未部署、重启、发送测试微信、安装 SILK 包或扩展持久访问授权。克隆目录没有真实 runtime；不要直接以它重新登录、另建助手或替换原绑定。以下在线动作须由用户确认后完成。

1. 先复核原源码是否有新修改，对审查过的补丁 dry-run，再在批准的位置部署代码。保留原私有库、binding、订阅、Tunnel 和 callback policy；运行时不进 Git。若要把在线运行目录迁往新仓库，需单独给出备份和 runtime/launch 路径迁移方案。
2. 媒体新增网络为 `novac2c.cdn.weixin.qq.com:443`：GET `/c2c/download`、POST `/c2c/upload`；原 iLink API 新增 POST `/ilink/bot/getuploadurl`。既有 Cloudflare DoH/TLS bootstrap、localhost 代理、原 Tunnel/callback 范围沿用，不新增媒体接收方。MCP 媒体块经原 Tunnel 到同一个 dot；只有附件 ID 进入事件。
3. 批准后，没有运行 broker 时用 `python3 -B start_wechat_broker.py --media`；已运行时用 `python3 -B restart_wechat_broker.py --media`。restart 检查实际锁持有者、发送/生成在途状态，拒绝强制 kill。TTS 随媒体模式配置，需本机已有库及 cmn 数据；status 的 local_tts_configured 不代表动态库可用，真实生成测试才验证。
4. SILK 尚未安装/验证，不加 `--silk-node`。依赖批准并用非私人 SILK 样本实测 helper 后，才选择本地绝对 Node 路径：`--media --silk-node '<Node 绝对路径>'`。解码不等于 ASR 或 dot 听懂。
5. 批准后让**同一个原 Tunnel**改用项目相对模板 `/bin/sh launch_mcp_media.sh`，保留实际原 `--db` / policy 参数。原文字模板为 `launch_mcp.sh`；两者清空环境，用 `/usr/bin/python3`。刷新同一 Plugin 的工具及事件 schema，核对版本 0.4.3、12 个工具/2 事件、四个媒体/TTS 工具及 reply source 二选一。文字模式为 8 个工具，包括独立可用的待回复查询。`ttlMs=0` 不证明在线缓存已刷新。
6. 经授权更新原 dot 的既有事件处理指令，保留所有原 trigger。使用事件实际 data.message_id 精确读取；payload 缺失则调用 wechat_read_pending_messages 并按 next_after_seq 分页，只处理返回的实际消息。查询规则见 [消息查询接口](MESSAGE_QUERIES.md)。附件须 read_media 后再理解；明确 decoder_required/平台音频不支持。只回复原 owner 对应实际消息，unknown 不重试；生成/暂存不自动发送。

最小真实测试由用户主动在**同一 ClawBot 对话**发起，每次使用新收到的实际 ID，不操作其他会话：

1. 非私人图片含随机数字/形状，同一 dot 读取媒体块并说出细节；只下载不算通过。
2. 3–5 秒非敏感语音含随机词。记录真实 SILK 解码与平台 audio 块保留，区分微信自带转写与 dot 对音频的理解；缺依赖/理解支持则记未通过。
3. 用户明确要求回传该测试图片，同一 dot 使用实际 media_id，手机实收才通过下行。用户明确要求短中文语音回复时，先 generate_audio，再独立 reply_media，手机收到并播放 **WAV 文件附件**才通过。原生语音气泡另列未实现。
4. 用户明确请求一次完成通知，绑定此次真实 message_id，核对工具阶段、HTTP/API 结果和手机实收；同时验收 event payload/message_id。缺 payload 不猜号、不重发旧通知。

上述在线环节当前全部未完成。云端图像生成产物 → 本机真实 bytes 的搬运仍待接口验证；本地 TTS 已绕开云端音频资产搬运，但不解决 dot 音频理解。缓存可按实际消息调用 forget_media。撤销媒体时，先清理需要清理的缓存/引用，再经批准以不带 --media/--silk-node 的 broker 和原文字 stdio 命令重启、刷新同一 Plugin，收回新增媒体持续授权。保留文字绑定/订阅；不自行删 key/token 或改系统网络。
