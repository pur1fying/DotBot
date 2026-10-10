# 图片直连

2026-10-10 用户选择先完成图片收发，音频暂不接入。运行中的 owner broker 已增加 `--images`，gateway 保持原生桌面房间模式和原有爱丽丝绑定。原 MCP/Tunnel 的 8 个工具、2 个事件保持不变；未开启完整媒体工具或语音/TTS。

微信文字、PNG/JPEG/静态 WebP 图片 → broker 原 owner 校验与私有缓存 → 原生房间上传文件并带附件发送 → 同一爱丽丝回复 → 通过原生客户端读取图片附件 → broker 暂存、腾讯 CDN 上传与微信图片消息。

`--images` 接受文字、图片消息以及 PNG/JPEG/静态 WebP 图片文件，语音、ZIP 和其他文件不进入这条路径。上限沿用媒体校验：每条 8 个附件、单图 4 MiB、2000 万像素，缓存总额 64 MiB、保留 24 小时；微信回复 context 本地限 1 小时。

## 恢复与关联

- 输入上传前保存 `uploading`。返回文件 ID 后保存 `ready`；确定没有发送应用消息的失败可复用该文件 ID。上传结果未知或进程中断不重新上传，需人工处理。
- 应用消息继续使用持久 UUID，回复关联通过房间、dot、对外角色、固定根消息和实际父消息检查，沿用文字去重与未知回执语义。
- 输出图片独立以消息 ID 和附件 ID 去重，支持图片单独回复、文字之后附图、生成中图片稍后就绪。图片准备完成前持久排队，下载可重试；微信发送开始后未知结果不重发。
- 仅原生房间客户端读取已关联附件，网关不接收任意 URL，也不输出签名下载链接、AES key、token 或私有上下文。
- `dot.schema_version=3` 增加输入上传状态表与出站媒体列，原消息路由和既有发送记录保留。源码回滚须考虑新出站媒体，不能把已提交或未知的图片消息交回旧 webhook 重发。

## 已验证与待确认

原生输入测试：640×480 PNG，只在图中包含图形和数字；爱丽丝准确回复 3 个红色圆形、2 个蓝色正方形、1 个绿色三角形、6935。请求文字未提供答案。

原生回图测试：爱丽丝发回一张无文字图片附件，原生下载得到 PNG；与输入图解码后的 RGB 像素完全一致。PNG 文件编码不同，不声称原始文件字节相同。

离线检查：177 项 Python 与 8 项 Node 通过，包含原文字回归、图片路由、MCP 契约、上传复用/未知恢复、无文字图片输出、延迟附件、父链与微信图片未知回执去重。

实际微信测试：用户自选 JPEG 在 local_seq=51 入库并经原生上传，local_seq=52 的原样回传请求得到原生图片回复。微信文字和图片 API 各接受一次，无重复。用户反馈手机已拿到约 400 KB 的回图，确认实收；用户另指出手机原文件约 3 MB，暴露原图保留未通过。图片来信到首条文字 API 接受 28.37 秒，回图请求入库到图片 API 接受 33.58 秒；这是两个单次样本，不是性能分位数。

图像质量边界：broker 下载的 JPEG 已为 352,797 字节、1206×2622；爱丽丝原生附件下载得到 437,366 字节、942×2048，broker 原字节上传微信，无本地 resize/re-encode。PNG 小图的像素一致结果不能推广为任意图无损回传。此 JPEG 测试收发通过，手机 3 MB 原文件保留和原分辨率保留未通过。

微信下载采用 `image_item.media`，没有采用 `thumb_media`，与 [腾讯官方媒体下载实现](https://github.com/Tencent/openclaw-weixin/blob/main/src/media/media-download.ts) 相同。官方 [类型定义](https://github.com/Tencent/openclaw-weixin/blob/main/src/api/types.ts) 将 media 标作原图引用，但本轮实际返回的媒体已经小于用户报告的本地原文件。未拿到手机原始文件，不能确定第一次变化的具体压缩参数或 PNG/JPEG 格式转换情况；未验证存在另一个可取回 3 MB 源文件的接口。

原图勾选复测（2026-10-10）：用户确认勾选微信“原图”后重新发送同一张图，local_seq=53 入站仍为 352,797 字节、1206×2622 的 JPEG，与 local_seq=51 缓存逐字节相同。该结果证明当前下载路径在勾选原图后仍未取得用户报告的约 3 MB 手机源文件；尚不能区分微信/iLink 只暴露处理后的版本与 CDN 默认下载选择了该版本。腾讯仓库的 [issue #241](https://github.com/Tencent/openclaw-weixin/issues/241) 有同类用户复现报告，当前为 Open；这是用户报告，不能当作腾讯已确认的服务端策略。对应私有记录为 `wechat-original-image-retest.json`。

## 原图下载接口核查（2026-10-10）

本轮检索腾讯项目 issue/PR、媒体下载实现及提交历史，并对照当前 npm 2.4.9。未找到已确认、可复现的图片消息原图下载修复或质量选择参数。#28、#241 均为 Open 且无评论；#235 的关闭原因是转为英文 #241，并非问题修复。公开用户报告支持存在同类现象，不构成腾讯对服务端策略的确认。

| 项目 | 本地与官方对照 |
|---|---|
| 入站接口 | POST `/ilink/bot/getupdates`，完成态 type=2 的 `image_item` |
| 引用选择 | 使用 `image_item.media`，不选择 `thumb_media`；优先 `media.full_url` |
| 无完整 URL 时 | GET `/c2c/download?encrypted_query_param=...`，与官方 URL builder 相同 |
| 解密 | 优先 `image_item.aeskey` hex，兼容 `media.aes_key` 两种 base64 编码；AES-128-ECB / PKCS#7 |
| 图像处理 | 下载完整 bytes 后校验和缓存，不缩放、不重编码；4 MiB 上限超出即拒绝，不生成压缩替代品 |
| 协议版本 | 本地声明 2.4.9，与本次 npm latest 相同；未发现升级即可恢复原图的依据 |

真实下载对照使用 local_seq=53 的既有图片引用：服务器完整 URL 含 `encrypted_query_param` 和 `taskid`；另构造相同授权参数、去除 taskid 的官方单参数 URL。两次下载均得到 352,797 字节、1206×2622 JPEG，与既有缓存逐字节相同。该读操作没有改变收件游标或发送消息；不声称比较了原始消息内另一条未保存的 CDN 引用。结果在私有 `image-download-form-comparison.json`。

发现并修正一个独立兼容性问题：本地此前仅接受 `encrypt_type=1`，现在支持官方定义的 0/1 两种元数据模式，拒绝未知值和错误类型。该修正已部署，媒体与图片直连相关 44 项检查通过，运行模式仍为 direct。该字段不是图像质量选择器，官方下载器不据此切换高清版本。local_seq=53 在旧校验下已成功接收，说明本次 353 KB 现象并非该拒绝造成。

文件通道：官方 type=4 的 `file_item.media` 有独立原字节下载分支。后续已启用 PNG/JPEG/静态 WebP 图片文件入站，见下节；ZIP 和其他文件仍未开启，图片下行的无损文件回传尚未验证。

依据：[官方收件下载](https://github.com/Tencent/openclaw-weixin/blob/main/src/media/media-download.ts)、[CDN URL 构造](https://github.com/Tencent/openclaw-weixin/blob/main/src/cdn/cdn-url.ts)、[解密与 URL 优先级](https://github.com/Tencent/openclaw-weixin/blob/main/src/cdn/pic-decrypt.ts)、[字段定义](https://github.com/Tencent/openclaw-weixin/blob/main/src/api/types.ts)、[issue #28](https://github.com/Tencent/openclaw-weixin/issues/28)、[issue #241](https://github.com/Tencent/openclaw-weixin/issues/241)。

## 图片文件接收（2026-10-10）

用户随后发送约 2 MB 的新 PNG 截图。本地此前的图片模式过滤 type=4，未将这条消息入库。现已支持 `.png/.jpg/.jpeg/.webp` 文件，经 `file_item.media` 下载/解密后使用原生图片直连。保留文件名、声明 MIME 和声明大小作为私有收件元数据；实际 MIME 必须与扩展名一致，若给出 len，解密字节数必须与其一致。单文件仍限制 4 MiB，不接语音、PDF、SVG、ZIP 或任意文件。

通过 owner-only 操作 `dot_recover_image_file`，从之前的收件 checkpoint 读取并恢复了这一条实际文件消息，未让用户重发。操作不回退实时收件游标，只允许在指定已知消息之后、一小时内、原 owner/bot 的最新合格图片文件；先 preview 后 commit，已入库的最新文件不会重复入队或继续向前回放其他历史文件。此操作不开放为 MCP 工具，也不返回 key、URL、context 或 cursor。

实测 local_seq=54：声明与实际下载均为 **2,115,564 字节**，PNG、1206×2622；原生上传成功，爱丽丝的文字描述与本地看到的截图内容一致，两条不同的文字回复均被微信 API 接受。手机源文件哈希未提供，因此不宣称与手机原文件逐字节等价；该样本确认文件入站保留了约 2 MB 的 PNG，未在本地接收时压成几百 KB。这张新图与先前普通图片测试不是同一截图，不能作为同图压缩 A/B 对照。

检查覆盖图片文件直连、实际 bytes、文件长度/扩展名伪造拒绝、语音/其他文件拒绝、恢复 preview/commit/去重与实时游标保留；媒体检查 47 项、文字/查询回归 13 项通过，恢复去重变更另以 11 项图片直连检查复核。

本轮仅实现并验证图片文件**入站**。爱丽丝图片输出仍走微信图片消息，不能据此宣称原文件下行回传无损。私有记录：`image-file-enablement.json`、`latest-image-file-recovery.json`、`wechat-image-file-e2e-test.json`。

本机私有记录位于独立运行目录的 `image-gateway-enablement.json`、`native-image-capability-probe.json`、`native-image-return-probe.json`、`wechat-image-e2e-test.json`，图片、绑定、实际 ID、缓存和数据库不进入 Git。

## 启用方式

在既有 broker unit 的 `ExecStart` 末尾增加 `--images`，重新加载用户级 systemd 配置并依次启动 broker、gateway。需要已部署本版源码及原媒体校验依赖；无需重启桌面或 MCP/Tunnel。桌面仍须在本机调试端口可用且登录同一账号。

停用新图片接收应撤去 `--images` 并重启两个服务；先检查是否有已领取、准备中或未知状态的图片任务。此开关不撤回已发送图片，已有缓存遵循保留/清除规则。
