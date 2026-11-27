# JINX 多平台采集与动作执行协议（Connector Protocol v1）

本文说明 JINX 公开核心中多来源消息接入（Relay Webhook）与通用环回动作连接器（Action Connector HTTP v1）的通信协议、确认语义、安全边界，以及首个非微信实现 `cmd/bilibili-connector` 的公私隔离配置方式。

> 所有监听与调用必须限定在本机回环地址（`127.0.0.1`、`::1` 或 `localhost`）。

---

## 1. Relay 多平台入站字段（`POST /api/v1/messages`）

连接器向 Relay 推送入站事件时，除现有微信兼容字段（`id`、`group_id`、`group_name`、`sender_id`、`sender_name`、`type`、`content`、`timestamp`、`is_echo`）外，还可使用以下可选多平台字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `source` | `string` | 来源标识，如 `bilibili:ego`、`web:<adapter>` 或空/微信默认值 |
| `platform` | `string` | 平台名：`wechat`，或任意命名空间平台（见 §1.1，如 `bilibili`、`web`、`zhihu`）；省略时从 `source` 推导 |
| `account_id` | `string` | 发送或监听账号标识（需满足契约 ID 字符约束） |
| `group_id` / `container_id` | `string` | 容器标识，格式如 `bilibili:BV1example0001`、`web:demo-thread` 或 `<digits>@chatroom` |
| `parent_id` | `string` | 回复父节点 ID（顶层评论为空） |
| `root_id` | `string` | 根评论 / 根主题 ID（顶层评论为空） |
| `reply_context` | `JSON` | 不透明结构化回复上下文（Relay 校验并压缩为紧凑 JSON，最大 8 KiB，原样透传给 Dispatcher 与 Connector） |
| `is_echo` | `bool` | 是否为本账号自身发出的回声事件 |

Relay 保留 `containerId` / `container_id` 作为 `group_id` 的兼容别名，并在外层包裹 `{"message": {...}}` 结构时自动向下继承上述外层元数据。

### 1.1 平台命名与接入新论坛

平台列表不是写死的枚举。除 `wechat`（使用 `<digits>@chatroom` 容器格式）外，任何满足 `^[a-z][a-z0-9-]{1,31}$`、且不是 `http`/`https`/`ws`/`wss`/`file` 的名字都是合法平台，容器 ID 写作 `<platform>:<local>`（如 `zhihu:question-123`），`source` 写作 `<platform>` 或 `<platform>:<adapter>`。`bilibili`、`web` 只是内置示例。

**语法合法 ≠ 已接入。** 平台真正可用仍取决于显式配置：Dispatcher 的 `action_endpoints.<platform>`、control-plane 的 `-connector <platform>=<origin>`，以及该连接器 `/health` 声明的 `platform` 与 `capabilities`。未配置的平台消息可以进入 Relay，但不会产生任何外部动作。

接入一个新论坛（例如知乎）不需要改公开核心代码，只需要：

1. **写连接器**（独立进程）：用 `internal/connector.NewServer` 获得 `/health`、`/v1/control/dry-run`、`/v1/actions`、幂等账本与过期校验；只实现采集与回复的平台细节（参考 `internal/connector/bilibili` 的 `Driver` 接口）。
2. **采集时填好回复坐标**：每条评论推到 Relay 时，`id` 填评论自身 ID，`parent_id` / `root_id` 填楼中楼关系，回复该评论所需的平台私有参数全部放进 `reply_context`（≤ 8 KiB JSON）。Relay 与 Dispatcher 只校验其为合法 JSON 并原样透传，只有同平台连接器解析它；AI 既看不到也改不了实际投递用的 `reply_context`。
3. **配置路由**：Relay `group_control` 与 Dispatcher `group_whitelist` / `group_rules` 加入 `zhihu:*` 容器；Dispatcher 配置 `action_endpoints.zhihu`；control-plane 加 `-connector zhihu=http://127.0.0.1:<port>`。
4. **先 `dry_run: true` 演练**，在控制台核对草稿后再经两步确认关闭 dry-run。

### 1.2 多条评论时选择回复目标（`REPLY_TO`）

非微信平台一个批次可能聚合多条评论。Dispatcher 在提示词的 `pending_messages` 中给每条评论一个批内编号 `ref`（1..N），模型若决定回复，第一行写 `REPLY_TO: <ref>`，其后为回复正文（也接受同一行 `REPLY_TO: 2 正文`）。

- Dispatcher 用 `ref` 取回**自己已保存**的那条消息，其 `content_id` / `parent_id` / `root_id` / `reply_context` 作为动作目标；模型输出中的任何平台 ID 都不会被采信。
- 批次只有 1 条时可省略 `REPLY_TO`。
- 多条却缺少 `REPLY_TO` → `blocked / reply_target_missing`；编号越界、写成平台 ID 或格式错误 → `blocked / reply_target_invalid`。两者都**不发送**，不会回退到"最后一条"。
- 一个批次最多回复一条；`REPLY_TO: <ref>` 后接 `NONE` 等同静默。
- 微信路径（整群回复，无内容级目标）不受影响，提示词也不变。

---

## 2. Connector HTTP Action Protocol v1

Dispatcher 通过 `action_endpoints.<platform>` 配置连接非微信平台的动作连接器（键为任意命名空间平台名，见 §1.1）。连接器必须仅监听本机回环地址，并拒绝非回环 `Host`、`Sec-Fetch-Site: cross-site` 或跨源 `Origin`。

### 2.1 `GET /health`

在跨越持久化提交边界（`submission_started`）前，Dispatcher 先请求连接器的 `/health`：

```json
{
  "status": "ready",
  "protocol_version": 1,
  "platform": "bilibili",
  "ready": true,
  "dry_run": false,
  "capabilities": ["reply_text", "thread_reply"],
  "controls": ["dry_run"],
  "targets": [
    {
      "container_id": "bilibili:BV1example0001",
      "label": "Example Video",
      "enabled": true
    }
  ],
  "last_poll_at": "2026-11-24T00:00:00Z",
  "last_poll_error_code": "",
  "account_id": "bilibili-ego",
  "connector_name": "bilibili-ego-connector",
  "reason": ""
}
```

准入要求：
- HTTP `200`
- `protocol_version >= 1`、`ready == true`、`platform` 与目标容器匹配、`status` 为 `ready`/`ok`/`healthy`
- `dry_run == false`（否则 Dispatcher 以 `connector_dry_run` 在提交前阻断，不触发任何外部写入）
- 若声明 `capabilities`，需包含本次 `ActionKind`（如 `thread_reply` 或 `reply_text`）
- `controls`（如 `["dry_run"]`）、`targets`、`last_poll_at` 与枚举型 `last_poll_error_code`（仅 bounded token，如 `fetch_failed` / `relay_push_failed`）供 `control-plane` 控制台只读健康观测与确认切换使用。

### 2.2 `POST /v1/control/dry-run`

用于受控切换运行期 `dry_run` 开关（受环回 `Host`、`Sec-Fetch-Site`、`Origin` 校验，仅接受单个 `application/json` 对象，禁止未知字段）：

```json
{
  "dry_run": false,
  "request_id": "req-toggle-1"
}
```

- 按 `request_id` 提供幂等重放；同一 `request_id` 对应不同 `dry_run` 目标返回 HTTP `409 idempotency_conflict`。
- 默认仅修改运行期内存状态（重启连接器后恢复启动配置），除非宿主注入 `OnDryRunChange` 并返回 `persisted: true`。
- 在控制台 (`cmd/control-plane`) 中，可通过 `-bilibili-connector` / `-web-connector`，或可重复的 `-connector <platform>=<origin>` 接入该连接器，并经由两步确认接口 `/console/v1/connectors/{platform}/dry-run/plan` -> 显式确认后 `/apply` -> 强制 `/health` 回读校验来安全切换。

### 2.3 `POST /v1/actions`

通过健康检查并写入 `submission_started` 与 `ActionRef` 后，Dispatcher 发起**单次无自动重试**请求：

```json
{
  "protocol_version": 1,
  "action_id": "act-...",
  "run_id": "run-...",
  "idempotency_key": "...",
  "platform": "bilibili",
  "kind": "thread_reply",
  "text": "回复内容",
  "expires_at": "2026-11-24T00:00:00Z",
  "expires_at_unix_ms": 1700000000000,
  "target": {
    "platform": "bilibili",
    "account_id": "bilibili-ego",
    "container_id": "bilibili:BV1example0001",
    "container_label": "Example Video",
    "content_id": "rpid-123",
    "parent_id": "rpid-100",
    "root_id": "rpid-100",
    "target_label": "Example Video"
  },
  "reply_context": {
    "oid": "88001",
    "bvid": "BV1example0001",
    "comment_type": 1
  }
}
```

服务端 (`internal/connector.Server`) 强制执行：
1. **环回与来源保护**：校验 `Host`、`Sec-Fetch-Site`、`Origin`，`Content-Type` 必须为 `application/json`。
2. **字段与时效校验**：拒绝未知字段、`ProtocolVersion < 1`、过期请求（返回 HTTP `409` / `submitted: false` / `reason_code: "expired"`）。
3. **Dry-Run 双重保险**：`dry_run: true` 时拒绝执行并返回 HTTP `409` / `submitted: false` / `reason_code: "dry_run"`。
4. **持久幂等账本**（`idempotency.json`）：
   - 同一 `idempotency_key` + 相同请求摘要：直接重放已持久化的响应，不重复执行外部动作。
   - 同一 `idempotency_key` + 不同请求摘要：返回 HTTP `409` `idempotency_conflict`。
   - 并发进行中：返回 HTTP `409` `status: "unknown"` / `action_in_flight`。

---

## 3. 确认语义与失败分类（绝不盲目重试）

| 响应状态 | HTTP 状态码 | 含义 | Dispatcher 状态迁移 |
| --- | --- | --- | --- |
| `confirmed`（含合法 `external_id`） | `200` | 外部平台已返回确认 ID，完成发送 | 记录回声并去重，`PhaseTerminal / OutcomeReplied / DeliveryConfirmed`（`submittedAtSource: "bridge_ack"`） |
| `submitted` | `200` | 外部请求已受理，待回声确认 | 进入 `PhaseConfirming / OutcomePending / DeliverySubmitted`，等待 Relay 后续自身回声 |
| `failed`（显式 `submitted: false` + `reason_code`） | `4xx`（通常 `422`/`409`） | **已证明**未跨过外部发送边界（如未登录、评论已删除、触发验证码或 dry-run） | `PhaseTerminal / OutcomeFailed / DeliveryFailed`（记录 `FailureEvidence`） |
| `unknown`、缺少合法 `external_id`、网络错误、超时或 `5xx` | `502` / `5xx` / IO 错误 | 外部提交结果不确定 | **禁止自动重试**，立即转为 `PhaseUnknown / OutcomeUnknown / DeliveryUnknown` |

---

## 4. `cmd/bilibili-connector` 运行与公私隔离

`cmd/bilibili-connector` 是 JINX 面向创作者自托管自有视频评论区的**参考连接器（Reference Connector）**，通过 `ego-browser nodejs` 在用户已登录的真实浏览器会话中轮询评论并发表回复，仅依赖公开核心契约，不含任何业务提示词或私有视频/账号信息：

- **定位、驱动可替换性与合规边界**：
  - **预期用途**：面向创作者或授权运维人员在本地自托管管理本人有权限的视频评论与回复，**不提供**批量营销、多账号轮换、群控刷屏或任何反风控能力；使用者须自行遵守目标平台的用户协议（ToS）与当地法律法规。
  - **可插拔 `Driver` 接口**：`ego-browser` 仅是内置默认参考驱动（`EgoDriver`），核心采集与回复动作通过 [`internal/connector/bilibili/driver.go`](../internal/connector/bilibili/driver.go) 中的 `Driver` 接口解耦，传入 `NewConnector` 即可替换为其他浏览器驱动或官方 API 客户端，无协议级强绑定。
- 公开配置示例见 [`cmd/bilibili-connector/config.example.json`](../cmd/bilibili-connector/config.example.json)，全部使用合成占位值（如 `bilibili:BV1example0001`）且默认 `"dry_run": true`。
- 真实的视频目标列表（`bvid`/`oid`）、`self_mids`、运行时 `state_dir` 及独立 Dispatcher 配置应保存在私有配置目录，运行前通过 `-config /absolute/path/to/private-config.json` 传入。
- **人机校验、限频与控制权保护（Fail-Closed）**：
  - 当 B 站返回需要验证（如 code `12015` `captcha_required`）、未登录（`-101`/`-111` `not_logged_in`）、频控限制（`12025`/`12035` `rate_limited`）、评论关闭/删除（`12002`/`12022`）或当前 `taskSpace` 处于用户人工接管（`agentDelegatedToUser` -> `user_control_active`）时，连接器明确返回 `status: "failed", submitted: false`，绝不尝试绕过验证码、对抗风控或抢占用户控制权。
- 单次诊断采集（不修改实时发送状态）：
  ```sh
  go run -ldflags=-linkmode=external ./cmd/bilibili-connector -config /absolute/path/to/private-config.json -once
  ```
- 支持通过 `search_targets` 配置关键词自动发现视频，或使用 `-search "<关键词>"` 检索候选视频、`-skip-relay -show-comments` 进行只读采集诊断；完整「指定视频」与「搜关键词选视频」双场景示例见 [`examples/bilibili-reply/README.md`](../examples/bilibili-reply/README.md)。
- 若配置了 `action_endpoints.bilibili`，当且仅当同一配置文件中不存在微信群路由（`<digits>@chatroom`）时，Dispatcher 允许省略 `bridge.url`。修改 `action_endpoints` 需重启 Dispatcher 进程生效（热更新接口拒绝变更服务端点）。

### 4.1 `cmd/v2ex-connector` 论坛主题参考连接器

`cmd/v2ex-connector` 是基于同一套 `internal/connector.Server` 与可插拔 `Driver` 接口（[`internal/connector/v2ex/driver.go`](../internal/connector/v2ex/driver.go)）实现的 V2EX 论坛帖子参考连接器，同样默认 `"dry_run": true`：

- 容器格式为 `v2ex:t-<topic_id>`（示例见 [`cmd/v2ex-connector/config.example.json`](../cmd/v2ex-connector/config.example.json)），多楼层批次下由 Dispatcher 的 `REPLY_TO: <ref>` 机制选定回复目标并透传 `reply_context`（`topic_id`、`reply_id`、`reply_no`、`reply_to_member`）。
- 启用 `auto_mention: true`（默认）且针对特定楼层（`thread_reply`）回复时，连接器会自动在正文前补全 `@<reply_to_member> `（若正文已含相同 `@用户名` 前缀则不重复添加）。
- 在遇到未登录 (`not_logged_in`)、人机验证 (`captcha_required`)、频控 (`rate_limited`)、主题已关闭 (`replies_closed`) 或 `taskSpace` 人工接管 (`user_control_active`) 时 fail-closed 返回 `status: "failed", submitted: false`。
- 路由接入时在 Dispatcher 中配置 `action_endpoints.v2ex`，并在 `control-plane` 启动参数附加 `-connector v2ex=http://127.0.0.1:18094`。
