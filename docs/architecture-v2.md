# 三层架构 V2：当前实现与扩展协议

本页对应 Go 1.22.5、module `jinx`。公共协议位于
`internal/contracts/architecture.go`（含 `container.go`），扩展端口位于 `internal/ports/ports.go`。
公开 core 包含通用 Relay、多平台 Dispatcher、Scenario Playbooks 组装、Engine、发送编排、Action Connector HTTP v1 (`internal/connector`) 及 `cmd/bilibili-connector` 示例连接器。
微信本地采集及其实现和部署细节位于独立私有模块，仅在本机配合 core 运行，
通过 `pkg/relay` 来源扩展共享同一消息管线、Hub 与 HTTP/SSE 监听；非微信来源也可通过 `POST /api/v1/messages` 多平台入站字段接入。公共协议不依赖该私有实现。
`cmd/control-plane` 已向 Console 注入同一个真实 Engine，入站引用与 `ActionRef`
通过 Dispatcher 的持久 trace 关联到运行与事件；控制面对非微信连接器的只读健康投影、`source`/`action` 独立事件流及动态 Agent 注册表尚未接入。

## 边界与数据流

```text
SourceAdapter → InboundEnvelope → AgentRuntime.Execute → ProcessingResult
                                        │                     │
                              ProcessingRequest          requestedAction
                                                              ↓
                                  ActionAdapter.Validate → Send → Confirm
                                                              ↓
                                                        ActionReceipt
```

来源层负责账号/容器/内容标识、去重、来源顺序、订阅恢复和来源原生能力。
`SourceRef` 使用 `platform/accountId/containerId/containerLabel/contentId/parentId/rootId`，
平台白名单当前为 `wechat/bilibili/web`。枚举中存在一个平台不代表已接入。
`InboundEnvelope` 包含 `envelopeId/dedupKey/source/author/content/receivedAt/replyTarget/`
`capabilities/metadataAllowlist`。`SourceCapabilities` 是
`canReply/canEdit/canDelete/canThreadReply/canAttach`，必须与实际目标和适配器一致。
账号、容器与内容标识均为平台内标识；跨平台去重必须同时使用来源命名空间。

处理层负责准入策略、Agent 路由、后端/模型选择、allowlist 引用解析、执行及证据。
`ProcessingRequest` 固定 `runId/envelopeId/agentId/policyRevision/backendId/modelId/`
`skills/tools/knowledgeBases`；后三项只能引用受信任注册表 ID，不能携带路径、
凭证、命令或原始 prompt。运行时还需检查引用确实在当前策略 allowlist 中，
`ValidateProcessingRequest` 的语法检查不能代替注册表授权。
`ProcessingResult{runId,reply,requestedAction,attempts,evidence}` 保留失败、超时和远端
可能仍运行的状态。模型生成的“已发送/已调用”文本不建立动作事实或工具能力。
证据仅允许 `backend_result/adapter_receipt/policy_check/external_confirmation` 的结构化
引用及时间、确定性；没有自由文本摘要证据字段。

动作层负责目标校验、发送时再验策略与过期、持久幂等准入、外部提交和确认。
`ActionRequest` 固定 `actionId/runId/target/kind/text/expiresAt/idempotencyKey`；动作白名单为
`reply_text/edit_text/delete_content/thread_reply`。Validate 必须无发送副作用，Send 在
外部 I/O 前持久记录幂等键；同键同请求返回原准入，不同请求冲突拒绝。
`ActionReceipt` 的 `admission{status,admissionId,recordedAt}` 单独记录持久准入；外部
`status/externalId/submittedAt/confirmedAt/reasonCode/platform` 记录平台证据。
`accepted` 不表示已提交，`submitted` ACK 不表示已确认。unknown 不自动重发；Confirm
只查询关联证据，禁止再次发送。取消调用也不能删除已经产生的准入回执。
真实微信提交已把 ActionRef 连同 idempotencyKey/admissionId/admittedAt 与提交边界一起
持久化；actionId 使用真实 Bridge request ID，admissionId 引用该持久 run 准入记录。
旧记录没有该证据时保持缺失，不能把旧 run ID 改名为 actionId。

SourceAdapter 的 Open/Stream/Close 管理已配置来源生命周期，不枚举其他容器；
context 取消/Close 关闭 channel 并释放资源。AgentRuntime 的 Open/Execute/Close 管理
运行时生命周期；取消本地调用不证明远端停止。ActionAdapter 是无会话调用边界，
调用者持有其宿主生命周期。除 context/channel/error 外端口参数和结果只依赖 contracts。

## 校验与存储规则

所有新增 JSON 字段 camelCase；Validate 函数校验枚举白名单、受限 ID、文本长度、
时间关系、结果关联和回执证据。Source 的原始内容与回复文本属于不可信内容，不作为
prompt 指令或事实摘要；console 的架构投影不输出这些文本，也不写入 Console journal。
协议没有 secret/rawPrompt/任意 JSON 扩展字段。`metadataAllowlist` 仅允许
`locale=zh-CN|en|und` 与 `ingress=realtime|replay`，未知键和值都拒绝。
禁止把凭证、原始 prompt 或自由文本放入 ID、label、refs、metadata 和 evidence。
实际凭证仅由宿主运行时管理；适配器负责输入 JSON 的严格解码及来源权限校验。
文本结构校验无法识别所有凭证语义，不能用它替代凭证隔离。
现有 run/policy 字段及 null/[] 行为保持兼容。新增的输入与动作关联在旧记录中允许缺失；
Console 将缺失关联保留为 null，不从当前配置或最新运行推断历史。

## 多平台运行时：Action Connector v1 与 B 站连接器

除微信 Bridge 路径外，Relay 与 Dispatcher 运行时已落地通用非微信接入与动作投递链路（详见 [connector-protocol.md](connector-protocol.md)）：

- **容器与入站协议**：`contracts.ParseContainerID` 校验 `<digits>@chatroom`（微信）、`bilibili:<local>` 与 `web:<local>`；Relay Webhook 接受 `source`、`platform`、`account_id`、`parent_id`、`root_id` 及不透明紧凑 JSON `reply_context`（上限 8 KiB），通过既有 SSE 流向 Dispatcher 投递并由 `ActionTargetForBatch` 推导目标评论/楼中楼动作（`thread_reply` 或 `reply_text`）。
- **Action Connector HTTP v1 (`internal/connector`)**：Dispatcher 对非微信容器路由至 `action_endpoints.<platform>`。跨过持久提交边界前先校验环回连接器 `GET /health`（要求 `protocol_version >= 1`、`ready == true`、`dry_run == false` 及能力匹配；`dry_run == true` 直接在准入前以 `connector_dry_run` 阻断）。跨过 `submission_started` 与 WAL 持久 `ActionRef` 后执行单次无自动重试 `POST /v1/actions`，连接器服务端强制校验环回 `Host`/`Origin`、请求时效、持久幂等账本 (`idempotency.json`) 与状态分类：`confirmed`（带合法 `external_id`）直接完成确认，`submitted` 等待后续回声，`failed (submitted: false)` 明确失败，`unknown` 或超时保持人工核验且严禁自动重发。
- **`cmd/bilibili-connector`（首个参考实现）**：基于 `ego-browser` 在操作者已登录浏览器会话中轮询视频顶层评论及预览楼中楼，维护断点游标 (`cursor.json`)，默认开启 `dry_run: true`，遇未登录、人机验证或用户接管标记 fail-closed 返回显式失败，不绕过任何风控。

## 声明式场景策略与受控知识挂载

Dispatcher 调度层 (`internal/dispatcher/scenario.go`、`local_policy.go`) 已落地多场景提示词组装与只读上下文注入：

- **公开路由层**：`group_rules` 支持 `scenario`、`response_goal`、`tone`、`skills`、`tools` 与 `knowledge_bases` 逻辑 ID；公开配置仅校验标识合法性与文本长度，不包含文件路径或私有提示词。内置 7 个通用场景：`chat`、`tech_community`、`product_support`、`soft_promo`、`public_comment`、`feedback_intake`、`test_echo`。
- **私有策略层 (`-local-policy`)**：通过严格 `0600` 文件加载 `communication_mode`、`support.instructions_mode` 与 `scenarios.<id>.prompt_mode`（`append` 或 `replace`）。`replace` 仅替换模板人设段落，公共核心的防注入、明确受话人与权限底线规则始终保留。
- **只读知识与源码事实预算**：`knowledge_bases` 与 `support.*_files` 仅从本地策略显式列出的绝对路径只读加载（总预算 48,000 bytes），缺失时 fail-closed 向模型注入知识缺口提示；`support.code_facts_file` + `code_fact_rules` 仅输出单捕获组正则提取的参数值。开启 `support_mode` 后强制禁用自主技能挂载并将工具提示词收敛到安全子集。
- **Console 规则对齐边界**：当前运行时与 `-local-policy` 已执行上述场景字段，但 `internal/console/rules.go` 的规则编辑器 schema 尚未暴露 `scenario`/`response_goal`/`tone`/`skills`/`tools`/`knowledge_bases` 字段，由后续控制台对齐工作补齐。

## 实际只读路由

### GET /console/v1/architecture

返回 `{ok:true,value:ArchitectureSnapshot}`，value 是直接快照，不再套 Envelope：

| 字段 | 当前含义 |
| --- | --- |
| revision | 公共投影内容的 SHA-256；不包含 observedAt，也不是配置 CAS 或跨服务原子版本 |
| observedAt | 本次聚合读完成时间；各上游不是同一个事务 |
| layers | 固定 `["source","processing","action"]` |
| sources[] | 平台、state、availability；accountId/containerIds 未取得配置证据时为 null；capabilities 含 ingest、adapter_contract |
| processing.agents | 当前 []；没有可验证的 Agent 目录，agent_catalog unsupported |
| processing.backends | control-plane 注入真实 Engine catalog 的受限投影；console-api 未注入时 []，不推断内部 profile 为 Agent |
| processing.capabilities | run_snapshot、agent_catalog、allowlist_refs、backend_catalog、skills_catalog、tools_catalog、knowledge_catalog |
| actions[] | 平台×动作、state、availability；capabilities 含 send、durable_admission、confirmation、adapter_contract |
| eventStreams[] | 三个域独立支持信息；有真实 replay 能力才提供 streamId/cursor/lastSeq/coverageSince |

能力统一为 `{id,supported,enforcement,source,reason}`。enforcement 白名单为
`hard/soft/host/unverified`，source 是固定来源枚举，reason 为有限安全机器码。
不透传上游自由文本原因；未知原因映射为 `upstream_reason_redacted`。

Wechat 的来源 state 由现有 `Facade.Services` 的 Relay `/health` 投影决定。
它描述宿主接入状态，不表示公开 core 包含本地采集或已经配置私有来源。
`ingest.supported` 与 `availability.supported` 表示已配置宿主接入；
`availability.enabled` 仅在 ready 时为 true。unreachable/not_ready 对应 reason=unavailable，
未知健康协议对应 unverified，不因宿主暂不可达改称平台未实现。
没有读取已登录微信账号或枚举容器；accountId/containerIds 为 null 表示配置身份未知，
不能把平台枚举、健康成功或历史事件中的来源命名空间当成登录账号清单。
Wechat `reply_text` 的 send 同样只证明宿主 Bridge 接入，证据来源为 bridge_health、
enforcement=host；ready 要求 protocol_version>=2、dry_run=false，且既有 health 检查的
group_routing/serial_ui/权限均为 true；缺字段为 unknown。
Bridge 授权不建立通用 ActionAdapter 契约，adapter_contract 继续 unsupported。
没有试发消息。在当前 Console 投影 (`internal/console/architecture.go`) 中，通用 `durable_admission` 和独立 `confirmation` 字段、微信编辑/删除/线程回复，以及 Bilibili/Web 的架构只读视图仍报告 `unsupported`：也就是说，虽然 Relay / Dispatcher 运行时与 `cmd/bilibili-connector` 已能执行 B 站评论采集、`dry_run` 门禁与 Action Connector v1 动作投递，但控制面的 `/console/v1/architecture` 尚未注入非微信连接器 `/health` 探测。

仅调用已配置上游固定 `/health`，Dispatcher `/api/trace/snapshot` 和
`/api/trace/events?cursor=...&limit=100`。不读取 Relay `/groups`、`/stats`、`/history`、
`/sources`，不枚举来源存储，不读 Dispatcher 旧日志或聊天正文。

Engine 接入点是 `console.Options.ExecutionCatalog`，只要求
`Catalog(context.Context) ([]execution.Backend,error)`，现有 `*execution.Engine` 直接满足。
`cmd/control-plane.assemble` 先创建 Engine，再以 `ExecutionCatalog: engine` 创建 Facade，
并将同一个 Engine 交给 Gateway；其 `/backends` 与 `/architecture` 使用共享目录。
初始化失败时释放已获得的 journal，退出按 supervisor、Console 顺序关闭；Engine
目前没有长期持有的锁或 Close 方法。独立 `cmd/console-api` 没有 Engine，仍明确返回
backend_catalog unsupported 与 backends=[]。目录查询不执行模型、不访问后端网络。
投影仅保留 backend ID、kind、available、agentBinding、受限 modelIds 和 capability
evidence；不返回 model label、命令、路径、URL、profile、环境变量和任意上游错误。
modelIds 使用与共享目录一致的受限模型 ID 字符集，保留合法 `/`、`+`、`@` 模型名称；
绝对路径不符合此格式。前端不能将架构列表作为完整模型路由编辑器。
Engine 的 soft 提示词/CLI 工具集约束保留 soft，effective 未验证时 supported=false；
hard 禁用能力也保持 supported=false。配置引用和模型回答都不会扩大 supported。

### GET /console/v1/architecture/events?cursor=...&domain=processing

domain 可省略，当前默认 processing；source/action 返回 501 unsupported JSON，
无 business event。非法 domain、未知/重复/空参数或缺 cursor 返回 400。
`Last-Event-ID` 优先于 query cursor。客户端必须把 cursor 当不透明字符串原样保存。
初始失效游标、流切换、过期或不连续返回 409 reset_required；尚未进入 SSE 时仍为 JSON。

```text
id: demo-cursor-001
event: architecture
data: {"eventId":"demo-event-001","domain":"processing","kind":"run_changed","occurredAt":"2026-01-01T00:00:00Z","runId":"demo-run-001","envelopeId":"","source":null,"action":null,"phase":"executing","outcome":"pending","certainty":"observed","reasonCode":""}

: keepalive
```

以上是 synthetic demo，不是实际会话证据。真实运行的 SSE id 使用原始 opaque cursor，
eventId 使用该 cursor 的稳定摘要，occurredAt 使用持久事件时间。

data 始终通过 `contracts.ValidateArchitectureEvent`。每条事件仅使用持久 Event 自身的
envelopeId/source/action；旧事件无关联时使用空 envelopeId、null source/action。
不读取最新 Run 补造旧事件关联，不把批次 run 假装为单条来源消息。
存在 ActionRef 的处理事件仍为 processing/run_changed，它只关联动作身份，
不表示独立动作流或送达确认。source 的 containerLabel 在公共投影中清空，避免
标签自由文本泄露；其余字段仅保留通过契约校验的身份。
真实 run 更新统一投影为白名单 kind `run_changed`，eventId
来自该事件真实 cursor 的稳定摘要；SSE id 原样沿用 cursor，不用 eventId 恢复。
unknown phase/outcome 的 certainty=unknown；其余 observed 表示状态变更确实被 trace
记录，不能据此证明动作送达。未知 reason 被裁剪，不会输出 prompt、路径或错误正文。

RunView 新增 `inputs/inputsMissingReason/action/receipt`，TS 对旧服务器保持 optional。inputs 仅投影持久
`InboundReference{envelopeId,dedupKey,source,receivedAt}`，不返回正文；旧记录 inputs=null、
action=null；ReceivedAt 未记录时不补造。ActionRef 保留真实持久幂等键、准入 ID 与时间。
receipt 为处理层 `{status,evidence,receipt,missingReason}`，从已有 DeliveryEvidence 与
delivery 状态映射；仅在持久 ActionRef 存在时投影并校验嵌套 ActionReceipt admission。
缺失准入时嵌套 receipt=null、missingReason=action_admission_not_recorded，不能用健康
或最新配置建立准入。无关联及证据的旧记录外层 receipt=null。
`submittedAtSource=echo_inferred` 的确认保留 submittedAt=null；不为 UI 补造 ACK 时间，
失败详情只输出固定摘要，unknown 不改为成功。

复用原 `/console/v1/events` 的 replay/longpoll：严格同 stream、连续 seq、页末 cursor
一致；limit=100，hasMore 先排空，再 wait=1。15 秒注释 keepalive 不含业务时间或 id。
客户端断连取消上游请求；每帧有写超时。流中不一致发 reset_required，其他失败发
unavailable 后关闭。reset 后重读 architecture（和需要的 runs/bootstrap），以新 cursor
重新订阅；架构 snapshot 不包含完整 run 状态。禁止将旧日志、统计、健康轮询编成事件。
原 `/console/v1/events` 的 event:run 数据和行为兼容保留。

## 前端映射与当前交付

`internal/console/client-contract.ts` 定义 SourceView/ProcessingView/ActionView/
ArchitectureSnapshot/ArchitectureEvent；`CurrentArchitectureEvent` 将当前实现限制为
processing/run_changed，envelopeId 为实际 ID 或空，source/action 为实际关联或 null。页面三列对应 layers：来源展示
健康与接入能力；处理展示 trace、真实目录、soft/hard 和缺失 Agent 目录；动作展示平台
动作矩阵、发送与确认能力。状态渲染先区分 supported，再读取 availability.enabled、
state 和 reason；unavailable 不应显示为“平台未实现”。unsupported 项显示 reason，
不生成可执行按钮。Agent 与动态 skills/tools/knowledge 注册表尚未接入，明确显示
unsupported，不复制 profile 名作为 Agent。

`architecture-client.ts` 提供同源 `createArchitectureClient()`，实现
`getArchitecture()` 与 `subscribeArchitecture({cursor,domain,onEvent,onResetRequired,`
`onUnavailable,onConnection})`，返回取消函数。onEvent 单独传 opaque SSE cursor。
reset/unavailable/网络错误关闭 EventSource 并回调；不使用浏览器无限自动重试过期位置。
EventSource 初始 JSON 错误不能读取 Problem body，网络回调统一要求重读快照，具体能力
先看 snapshot。此客户端已实现，三层页面本身未实现。原 CurrentConsoleHTTPClient
仍为整体接口交接，不能因新增方法就宣称完整 Console 客户端已经交付。

## 后续平台与控制面演进

为 B 站/Web 等平台补齐控制面可观测性与更多连接器时，继续遵循以下边界：

1. **Control-Plane 与 Console 对齐**：在 `cmd/control-plane` 与 `internal/console/architecture.go` 中注入非微信 `action_endpoints.<platform>` `/health` 探测，分别投影为对应平台的 Source/Action `host` availability（不可达记为 `unavailable`，而非与 `unsupported` 混淆）；并在 `internal/console/rules.go` 与只读目录下仅暴露逻辑 ID（绝不回传本地策略路径或私有提示词文本）。
2. **新平台连接器扩展**：扩充 `contracts` 平台白名单和校验后，参照 `internal/connector` + `cmd/bilibili-connector` 实现已授权容器订阅、断点去重、Action HTTP v1 持久幂等准入及未知结果防重发；`source`/`action` 独立 SSE 域仅在连接器具备真实持久化事件流后开放，各自维护独立 `streamId`/`cursor`/保留窗口与 `reset_required`，不复用 `processing` 游标声称跨源全序。
3. **动态 Agent 与工具隔离**：仅在运行时真正实现请求级硬工具沙箱与可验证 Agent 注册表后，才更新 `agent_catalog`/`skills_catalog`/`tools_catalog`/`knowledge_catalog` 及能力 `enforcement` 级别；`alma_personal` 仍保留 `soft`。

既有离线测试覆盖微信与 B 站全链路（`TestBilibiliConnectorChain`、`internal/connector/...`、`internal/dispatcher/multi_platform_test.go`、`scenario_prompt_test.go` 等），使用 `httptest` 上游、临时 trace/Engine/journal，不调用现役服务、不发送真消息、不读取本机私有库。

`cmd/control-plane/main_test.go` 通过真实 assemble handler 对照 `/backends` 与
`/architecture` 的模型和权限证据，并断言零 provider 请求、退出释放锁及错误清理。
`architecture_handler_test.go` 经持久 trace 重开和真实 Dispatcher handler 验证事件
原有 cursor、输入/动作关联及旧事件缺失事实；Node 测试以实际 TS 编译结果验证
可空关联、opaque cursor、reset 和断连回调。
