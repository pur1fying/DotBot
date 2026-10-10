# 复用 Codex 桌面消息房间的微信网关

网关使用既有爱丽丝、既有消息房间和桌面的原生发送/订阅客户端。broker 独占微信凭据及发送操作，网关通过 owner Unix socket 处理持久队列。代码不创建替代助手。

## 已验证的实际协议

桌面安装版 `26.928.21956` 的房间客户端由 `app-initial` 导出的 `CN` 创建；发送经 `httpFetch`、窗口 MessagePort 和 Electron 主进程，保留原鉴权、Sentinel/设备校验及线程路由。接收复用原生 `/celsius/ws/user` pubsub 和消息投影。

2026-10-10 完成原生房间只读检查和两条明确标记的消息测试：

- 当前账号、爱丽丝及既有房间相符。
- `request_id` 在提交确认中回传，提交往返分别约 2.11 秒、1.26 秒。
- 普通房间消息的答复不含可用于绑定的父消息。`turn_trigger=chatgpt_messaging` 只表示来源类型。
- 以固定入口消息为父消息提交时，答复明确引用本次来信的房间消息 ID，并具有相同回复根。第二次测试从 API 接受至本机读取完整答复约 7.02 秒。
- dot 的对外答复在此协议中是 `role=user`、`self=false`、`senderAeonId` 为指定 dot。旧设计中的 `agentMessage` / `delivery=async` 不能直接用于房间投影。

这些是协议测试，尚不是手机端到端性能验收。主 dot 根线程仍返回 `canAcceptDirectInput=false`；网关不向它调用旧输入 RPC。独立 Python HTTP 请求曾收到浏览器校验挑战，当前实现调用已经登录的桌面原生客户端。

## 运行依赖与边界

此模式需要 Codex 桌面进程存活，并开启仅本机使用的调试入口。它替代最初“桌面完全退出后独立运行”的假设。网关与 broker 是独立用户级服务，桌面退出时保留队列并重连。

```sh
/usr/lib/chatgpt/ChatGPT --remote-debugging-address=127.0.0.1 --remote-debugging-port=9229
```

后续已支持的文字消息固定默认使用直连，故障仅排队重连；只有用户明确要求时才执行人工回滚。broker 路由保存在私有数据库，重启会保留；两个用户级后台服务已启用。

本机已为安装版 ChatGPT 的普通图标配置用户级启动覆盖：`~/.local/share/applications/chatgpt.desktop`，自动带上上述两个调试参数，保留 URL 打开参数 `%U`。当前进程无需为此再次重启。

本机已实际核对监听地址为 `127.0.0.1`。调试端口可控制该应用，应仅用于本机。说明见 [Electron 官方文档](https://www.electronjs.org/docs/latest/api/command-line-switches#--remote-debugging-portport)。

`dot_desktop.py` 仅连接 loopback，选取主窗口，排除头像窗口、独立窗口及嵌入网页；多个主窗口时要求 target ID。固定本地桥接代码导入已安装的两个 bundle，不修改 `app.asar`。桌面升级后需重新核对接口；它不是稳定公开 API。

令牌和校验材料留在原客户端。`dot_auth.py` 登录助手仅用于独立只读预检，不创建对话或调用模型。服务日志不保存聊天正文；正文及出站分片保存在 broker 私有 SQLite 中。运行绑定和协议报告不进入 Git。

## 持久队列与回复安全

每条微信首次入队生成稳定 request UUID。native send 前持久化 `submitting`；明确接受后保存房间消息 ID。没有收到确认时标为 `submission_unknown`，通过历史里的 request ID 核对，不重新提交。

消息以固定入口为父消息发送。回复必须同时匹配房间、dot、对外角色、回复根及已知父消息。`turn_trigger` 仅为诊断标签，`chatgpt_messaging` 与 `persistent_mode` 都可能产生合法对外答复。已关联的后续消息可作为新的父节点，仍归到原微信来信。父节点暂缺时保存未匹配消息，历史补齐后再关联。

只发送原客户端投影出的完整对外文本；追问和确认表单转换为文字及选项。网关不选择选项、不调用批准接口。微信文本标注为外部来信，需要执行或批准操作时要求在 Codex 中确认。

同一时间只提交一条新的来信，首条完整答复的所有分片被微信 API 接受后才处理下一条。后续同链答复仍归原来信。UTF-8 分片不超过 2,000 字节，首段沿用原幂等键；重放不重复发送。

微信发送结果不明、超过一小时回复窗口、明确拒绝及需用户处理的情况分别记录。普通故障排队并退避重连，最长 30 秒，不自动切换 webhook。收取和发送分别复用 HTTPS 连接，收取结束及时续接。

## 运维命令

使用安装了 `requirements-direct.txt` 的 Python 3.12。以下命令默认读取同一源码目录下的 `runtime`：

```sh
python -B dot_gateway.py preflight
python -B dot_gateway.py preflight-desktop
python -B dot_gateway.py bind --verification /private/path/desktop-room-verification.json
python -B dot_gateway.py serve
python -B dot_gateway.py status
python -B dot_gateway.py reconcile
python -B dot_gateway.py metrics
python -B dot_gateway.py canary
python -B dot_gateway.py activate
python -B dot_gateway.py rollback
```

`bind` 需要已验证的 `desktop_room_reply_v1` 私有报告，并再次读取实际房间。`preflight-desktop` 仅检查读路径，其本身不授予生产启用资格。

`canary` 只将 `【DotBot直连测试】` 开头的纯文字归直连，其余保持 webhook。`activate` 将切换序号之后的纯文字归直连；图片和语音继续原路径。切换要求网关健康、协议已验证、旧链路待回复及发送完成。

`rollback` 停止新领取，仅明确未提交的 queued 消息可归回 webhook。已经提交或结果不明的消息保留直连归属。broker 与 MCP 有持久回滚补投/确认机制，转发游标已经越过的未提交消息也不会遗漏。

`reconcile` 只读取房间记录并补齐持久事实，不主动发送微信。常驻服务负责发送已确认的出站分片。

## 后台托管与检查

`ops/dotbot-broker.service.in`、`ops/dotbot-gateway.service.in` 为用户级 service 模板。替换 `@SOURCE@`、`@PYTHON@` 后安装到 `~/.config/systemd/user/`。模板不含凭据。旧 MCP/Tunnel 保留查询与回滚能力。

```sh
systemctl --user status dotbot-broker.service dotbot-gateway.service
python -B -m unittest discover
node --test test_desktop_room_bridge.mjs
```

检查覆盖 FIFO、分片、父节点补齐、别的 dot/房间隔离、断线未知结果、重复消息、过期、确认选项和回滚游标。实际性能由 `metrics` 的入库、领取、native send 开始、API 接受、完整回复入队、微信发送开始与 API 接受时间计算。native send 开始是客户端发起 HTTP 发送的回调，不能称为 WebSocket 写出。

真实微信测试已由用户确认手机实收，新纯文字在 local_seq=26 之后切换为 direct。broker 路由使新文字不再进入旧 webhook 投递，旧订阅仍可用于回滚。修复后本轮对比已完成：各 10 条同组数字请求，中位数降低 62.5%，无重复，全部手机实收。本轮样本达到延迟目标；输入空格差异、顺序采集及样本量限制详见 [实测报告](LATENCY_COMPARISON_2026-10-10.md)。

2026-10-10 的一条真实微信测试已被新链路处理，答复内容符合要求，微信 API 接受。入库至 native send 开始 113.17 ms，完整答复入队至微信发送开始 33.63 ms，入库至微信 API 接受 12.374 s。broker/gateway 重启后，已接受发送记录仍为一次。该样本不能用于宣称 P95 或正式 40% 改善；手机实收另行由用户确认。

实际旧链路测试暴露了发送连接闲置约 117 秒后立即失败的问题。传输现在在写入前回收闲置超过 15 秒、已关闭或出现可读状态的旧连接；保持短时间热连接复用。已发生的结果不明发送不重试。

正式 A/B 初测第 4 条暴露了来源标签筛选过严的问题：爱丽丝已生成 `persistent_mode` 答复，网关误过滤并推进了游标。已取消来源枚举限制，仍保留身份/房间/对外角色/父链检查；重启与 reconcile 从最早未完成的自有输入回填，支持恢复已越过全局游标的答复。原始异常数据保留，修复后的新链路另起完整测试组。

最终回归 169 项 Python、6 项 Node 通过。新链路以修复后的 10 条重新验证，入库到 native send 的样本 P95 为 119.3 ms，完整答复入队到微信发送开始为 43.5 ms。当前模式为 direct，普通新文字无需测试前缀。
