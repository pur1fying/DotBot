# dot 直连接入：预检与队列基础

当前分支完成的是可验证的基础组件，**不是已上线的 dot 直连服务**。现有部署保持原 webhook/MCP 路径。

## 真实协议结果

2026-10-10 使用桌面客户端 `26.928.21956` 的打包代码作为参考，使用本机 `codex-cli 0.159.2` 完成独立鉴权辅助进程验证。

- `getAuthStatus` 能获取现有 ChatGPT 登录授权，不创建线程或调用模型。
- `wss://codex-cloud-backend.chatgpt.com/` 的握手、初始化与只读 RPC 可用。
- 本机账户范围内的 dot 元数据与已认证的云端线程读取相符。
- 主 dot 的根线程明确返回 `canAcceptDirectInput=false`。代码不会向它发送 `turn/start`、`turn/steer` 或 `turn/addUserMessage`。
- 当前主 dot 的桌面入口实际使用消息房间：`/messaging/rooms/{room_id}/messages`、`/messaging/rooms/{room_id}/live`，以及用户级消息订阅。
- 独立访问身份 HTTP 接口返回 HTTP 403、`cf-mitigated: challenge`。没有尝试绕过浏览器验证。
- 随后对既有消息房间做了一次只读 GET，同样返回浏览器验证挑战；没有发送消息或创建新房间。

因此，WebSocket 初始化成功不能当作主 dot 可写入的证明。原设计中使用旧线程输入 API 的假设尚不成立；需完成真实消息房间适配与授权访问验证后，才能实现生产常驻网关和切换流程。

## 已实现

- `dot_auth.py`：stdio 登录助手、令牌内存管理、过期刷新、账号变化检查；不输出令牌或原始鉴权响应。
- `dot_transport.py`：固定域名的 CONNECT/TLS WebSocket 客户端、独立接收循环、RPC 响应分发、输入能力检查及只读身份请求。
- `dot_gateway.py preflight`：可重复的真实协议检查。身份 HTTP 不可用时，仅从当前账号对应的桌面 dot 元数据中取候选 ID，再通过已认证的云端只读请求核对线程；不把缓存当成写入授权。
- `desktop_room_bridge.mjs` / `dot_desktop.py`：调用**已运行桌面进程内**的原生消息房间客户端。包含固定身份检查、消息提交、历史查询、现有 pubsub 订阅及账号变更关闭；仅完成离线验证，尚未接通真实桌面实例。
- broker 的路由归属、FIFO 队列、稳定提交/回复链 ID、回复 item 去重、UTF-8 分片、未知发送结果及重启恢复。
- broker 的 `wait_inbox` 在消息事务提交后唤醒，不持有发送锁等待。
- webhook 转发与待回复查询排除分配给直连的消息，空的直连分页也会推进转发游标。
- iLink 收取和发送使用独立、可复用的 HTTPS 连接；发送异常不自动重试。正常长轮询结束后及时续接，快速空响应保留短退避。

内部 broker RPC 仅为后续网关提供基础，不新增对外 MCP 工具；它们不代表当前账号已获得生产直连能力。

## 预检

在 Python 3.12 环境安装 `requirements-direct.txt` 后运行：

```sh
python -B dot_gateway.py preflight --output /private/path/dot-preflight.json
```

退出码 `2` 表示没有满足启用条件。当前预期包含：

```json
{
  "auth": "ok",
  "websocket": "ok",
  "authenticated_thread_read": "ok",
  "can_accept_direct_input": false,
  "required_transport": "messaging_room",
  "protocol_verified": false,
  "ready_to_activate": false,
  "application_message_sent": false
}
```

报告仅保存必要的账户/dot/线程关联信息与检查状态，不保存头像签名 URL、令牌、Cookie 或聊天正文。报告保留在本机私有目录，不进入 Git。

## 复用桌面房间链路

用户要求直接复用桌面发送链路后，新增了桌面适配器。它依赖桌面进程存活，取代最初“桌面完全退出后独立运行”的假设；独立鉴权助手不能代替原生房间客户端。

已核对的调用顺序是：

```text
DesktopRoomClient（本地 CDP 连接）
  → 已有 AppScope 中的房间客户端 c0o / 导出 CN
  → 账号核对 + 原客户端的 Sentinel / 设备校验准备
  → _f.safePost(/messaging/rooms/{room_id}/messages)
  → 主窗口 MessagePort → httpFetch → Electron performDesktopFetch
```

客户端自行填写 `request_id` / `idempotency_token`、保留线程路由缓存，并处理发送结果不明。适配器只传稳定 request ID、文字和可选回复消息 ID，不读取/复制 Cookie、令牌或校验材料。不会调用旧根线程输入 RPC。

接收复用桌面的账户级消息缓存及 `/celsius/ws/user` pubsub。唤醒消息仅保留已绑定房间的 ID；正文通过原客户端的消息投影读取。断线或缓冲溢出会要求重新读取历史。**房间消息的实际回复关联和完成边界尚待真实测试**，因此此适配器不会令 `protocol_verified` 变为 true，也尚未驱动 broker 出站队列。

当前桌面安装版只把 `httpFetch` 暴露给受信任窗口，现有 MCP 和本地 IPC 没有房间发送方法。适配器使用 Electron 调试入口执行固定的本地桥接代码，不修改 `app.asar`，不自动打开调试端口、终止进程或重启桌面。

在允许中断桌面会话的时间，完整退出 Codex，再以以下参数启动同一安装程序：

```sh
/usr/lib/chatgpt/ChatGPT --remote-debugging-address=127.0.0.1 --remote-debugging-port=9229
```

调试端口可控制应用，应仅绑定本机。适配器只连接 `127.0.0.1`，只选择 `app://-` / `app://.` 主窗口；有多个候选窗口时要求显式 target ID，不猜测。真实启动后须核对监听地址。调试参数见 [Electron 官方说明](https://www.electronjs.org/docs/latest/api/command-line-switches#--remote-debugging-portport)。

随后使用已有 Python 3.12 环境执行只读检查：

```sh
python -B dot_gateway.py preflight-desktop --debug-port 9229 \
  --output /private/path/desktop-room-preflight.json
```

该命令检查已有账号与 dot 元数据、房间归属和一次历史读取，不发送测试文字。关闭连接时释放该桥接实例的订阅，不影响桌面自己的订阅。导出符号固定对应 `26.928.21956` 的两个 bundle；应用升级后模块不存在或接口变更会报错，需重新核对，不能作为稳定公开 API 使用。

新增检查：`node --test test_desktop_room_bridge.mjs`。这些测试覆盖账号/dot 不匹配、原生发送结果不明、不重试、固定房间、队列溢出及正文投影，不证明真实服务已接受消息。

## 后续完成条件

需要完成消息房间协议的消息提交、回复订阅、关联及重放验证。此后才能提供 `bind/status/reconcile/activate/rollback` 运维入口、常驻服务及用户级托管配置，并按同一批实际微信输入进行旧/新链路对比。

在这些条件满足前，不切换流量，不宣称端到端性能改善，也不把接口替换为新建模型会话或另一位助手。

离线回归：`python -B -m unittest discover`。测试不读取真实授权，不发送微信或 dot 消息。
