# dot 文件回传

broker 启动参数 `--files` 启用 dot → 微信通用文件回传，可与 `--images` 同时使用。2026-10-23 已部署本机；离线链路测试通过，用户已确认微信收到回传文件。手机打开情况及下载后文件哈希尚未核验。

用户可从微信要求：“生成一个 txt 文件，并作为附件发回微信。”爱丽丝必须将文件附在同一原生回复链上；仅输出电脑路径、Markdown 文件链接或网页链接不会触发文件发送。此功能不增加微信普通文档入站处理能力。

原生客户端通过 `media.content(roomId, messageId, attachmentId, 'document', signal)` 下载文件。桥接层核验绑定房间、dot 作者和回复根，broker 通过回复父消息关联本人微信请求。文件作为不透明字节传递，不解压、不执行、不转换，文件名保留；PNG/JPEG/WebP 仍走既有图片通道。

限制：单个文件 1 字节至 4 MiB；文件名最多 240 UTF-8 字节，不含路径分隔符或控制字符；每条原生消息最多处理 8 个普通文件附件。沿用 1 小时回复窗口、24 小时缓存有效期和 64 MiB 共享媒体缓存上限。

文件进入持久化 outbox，下载后暂存，再使用 iLink file 类型上传和发送。幂等指纹包含模式、MIME、内容 SHA-256 和文件名。发送前持久化状态；发送结果不明时不自动重发。超限、失效或无法暂存的附件转为文字提示，后续回复继续处理。暂未完成上传的附件等待就绪；临时网络故障随网关恢复重试下载。

`build` IPC 返回 `output_files_enabled` 与 `output_file_max_bytes`。公共 MCP 工具不增加文件下载或任意路径读取接口。`api_accepted` 只表示微信接口接受，不能当作手机已收到或成功打开。

验证：`python3 -m unittest test_dot_files test_dot_images test_media test_dot_room_gateway test_dot_store test_dot_desktop -q` 和 `node --test test_desktop_room_bridge.mjs`。测试涵盖二进制保真、中文名、大小限制、父消息隔离、重复事件、重启、发送未知不重发和失败提示后继续发送。
