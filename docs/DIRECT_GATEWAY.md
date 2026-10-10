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

因此，WebSocket 初始化成功不能当作主 dot 可写入的证明。原设计中使用旧线程输入 API 的假设尚不成立；需完成真实消息房间适配与授权访问验证后，才能实现生产常驻网关和切换流程。

## 已实现

- `dot_auth.py`：stdio 登录助手、令牌内存管理、过期刷新、账号变化检查；不输出令牌或原始鉴权响应。
- `dot_transport.py`：固定域名的 CONNECT/TLS WebSocket 客户端、独立接收循环、RPC 响应分发、输入能力检查及只读身份请求。
- `dot_gateway.py preflight`：可重复的真实协议检查。身份 HTTP 不可用时，仅从当前账号对应的桌面 dot 元数据中取候选 ID，再通过已认证的云端只读请求核对线程；不把缓存当成写入授权。
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

## 后续完成条件

需要完成消息房间协议的消息提交、回复订阅、关联及重放验证。此后才能提供 `bind/status/reconcile/activate/rollback` 运维入口、常驻服务及用户级托管配置，并按同一批实际微信输入进行旧/新链路对比。

在这些条件满足前，不切换流量，不宣称端到端性能改善，也不把接口替换为新建模型会话或另一位助手。

离线回归：`python -B -m unittest discover`。测试不读取真实授权，不发送微信或 dot 消息。
