# 已核实的依赖与协议依据

2026-10-09 本机已存在并用于验证的 Python 包：cryptography 46.0.3、Pillow 12.0.0、jsonschema 4.25.0。版本记录在 requirements-media.txt / requirements-test.txt。本次未执行安装。文本 broker/MCP 不需要外部 TTS key。

本地语音生成使用已安装 `libespeak-ng.so.1` 与 `cmn` 普通话数据。真实隔离 worker 测试生成了 22,050 Hz、mono、16-bit PCM WAV，固定测试句约 6.1 秒、PCM 非零，无播放/网络。API 依据 [eSpeak NG speak_lib.h](https://github.com/espeak-ng/espeak-ng/blob/master/src/include/espeak-ng/speak_lib.h)，固定同步 retrieval 模式，SSML/phoneme 标志未开启。机械声音，不保证自然音质。

SILK helper 需要 Node 与 `silk-wasm`。仅只读查询[公共 npm 元数据](https://registry.npmjs.org/silk-wasm)，未下载安装：版本 **3.7.1**、MIT、Node **>=16.11.0**、无 JavaScript dependencies，源码为 [idranme/silk-wasm](https://github.com/idranme/silk-wasm)。tarball：`https://registry.npmjs.org/silk-wasm/-/silk-wasm-3.7.1.tgz`；integrity：`sha512-mXPwLRtZxrYV3TZx41jMAeKc80wvmyrcXIcs8HctFxK15Ahz2OJQENYhNgEPeCEOdI6Mbx1NxQsqxzwc3DKerw==`。项目内目前无法 resolve 该包，因此不声称真实 SILK 已解码。

批准后可将该固定版本安装至 helper 可解析的项目位置（node_modules 被忽略），先非私人样本验收，再允许 broker --silk-node。当前 helper 将 SILK 转 24 kHz、mono PCM WAV，有输入大小/输出时长/20 秒超时限制；缺包/坏输出固定失败。没有本地 ASR，微信提供的转写明确标注来源；解码器不产生转写。

官方参考：[Tencent/openclaw-weixin 协议](https://github.com/Tencent/openclaw-weixin/blob/main/docs/protocol.md)、[发送器](https://github.com/Tencent/openclaw-weixin/blob/main/src/messaging/send.ts)、[CDN 上传](https://github.com/Tencent/openclaw-weixin/blob/main/src/cdn/upload.ts)、[媒体下载](https://github.com/Tencent/openclaw-weixin/blob/main/src/media/media-download.ts)、[SILK 转换](https://github.com/Tencent/openclaw-weixin/blob/main/src/media/silk-transcode.ts)、[OpenClaw 微信 channel](https://docs.openclaw.ai/zh-CN/channels/wechat)。此处沿用原 channel_version；没有因迁移自行升级登录协议。

腾讯当前发送器 image/video/file 构造不能证明原生 voice 发送可用。本项目图片为 upload media_type=1、消息 item type=2，WAV 附件为 upload media_type=3、消息 item type=4；不是 item type=3 原生 voice。AES-128-ECB/PKCS#7，明文 MD5，随机 16 字节 key，CDN 成功并返回 x-encrypted-param 后才发送消息。

[MCP Tools 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28/server/tools) 规定 image/audio 内容块使用 type/data/mimeType。离线测试验证实际字节、schema 和完整 IPC/stdio 帧，但实际 Plugin/Tunnel 是否保留这些块与同一 dot 是否理解仍须真实测试。
