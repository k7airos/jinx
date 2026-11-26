# JINX 后端对接说明

公开项目根目录记为 `<PROJECT_ROOT>`，独立本机私有项目记为 `<PRIVATE_ROOT>`。源码更新不替换正在运行的服务、配置、状态或 LaunchAgent。公开验证方法与合成证据见 [verification.md](verification.md)；原始运行证据、账号与本地策略不属于公开文档。

## 公开边界

core 包含群聊处理、发送编排、adapter 接口，以及通用 Webhook/SSE 入口。微信本地采集及其实现和部署细节不在公开模块；私有模块只在本机配合 core 运行，通过通用消息契约对接。core 不要求私有来源存在。私有源码必须实际移出公开项目，不能只靠 `.gitignore` 隐藏。

## 工程与入口

module 为 `jinx`。Relay、Dispatcher 和控制面保留独立生命周期。兼容原入口 `cmd/relay`、`cmd/dispatcher`；使用新结构化执行器时改用 `cmd/jinx-dispatcher`，由 `cmd/control-plane` 提供完整控制接口。两种 Dispatcher 入口互斥，不能双开接同一条消息流。

`cmd/control-plane` 与 `cmd/jinx-dispatcher` 共用同一份 `schemaVersion: "1"` 后端定义和同一个 execution state 文件。配置文件只由操作者本机提供；前端可以选择后端/模型、调整路由和权限，不能提交任意 URL、命令、环境变量或私有路径。

## 对接入口

HTTP 根路径为 `/console/v1`，没有 `/api` 前缀。原始 upstream URL 由服务端固定配置，不由页面拼接。

| 功能 | 接口/契约 |
| --- | --- |
| 规则、配置发布与历史、任务列表/详情 | [基础 API](../internal/console/API.md)、[基础 TS](../internal/console/client-contract.ts) |
| 暂停、恢复、排空、取消、后端与服务操作 | [Gateway API](../internal/gateway/README.md)、[Gateway TS](../internal/gateway/gateway-contract.ts) |
| Dispatcher 持久控制 | [CONTROL.md](../internal/dispatcher/CONTROL.md) |
| 执行策略与权限 | [execution](../internal/execution/README.md)、[runtimebridge](../internal/runtimebridge/README.md) |
| Relay 原子公开规则 | [relay](../internal/relay/README.md) |
| 白名单服务管理 | [supervisor](../internal/supervisor/README.md) |

`/bootstrap` 用于能力发现。任务事件流仅涵盖 run 域，配置/服务/控制操作须独立读回；不要把全局配置发布混进任务 SSE。缺失历史、不确定远端执行或未支持能力必须按实际 reason 展示，不能填 0/false/成功。

## 已接入的业务边界

暂停和提交共用串行准入边界，先成功写入持久 submission admission 才允许外部 POST。暂停只阻止尚未跨边界的任务；已提交任务继续等待回声。恢复不回放暂停期间的输入。

排空允许维护边界前队列和已受理工作结束，新输入仍被观测但不受理。未知交付、远端未证明停止、用户暂停或重启恢复未确认均阻止 ready。缺少安全字段的维护响应被拒绝。启动服务不依赖已停止 Dispatcher 的 HTTP，但停止/重启仍必须获得有效排空证明。

执行策略在模型调用与提交前实际校验。真实 backend/model/external run ID 和版本保存为 attempts，原始受理配置版本不被事后改写。策略收紧会阻止旧结果投递，权限放宽不追溯授权旧结果。未确认远端结束时不自动 fallback，防止重复工具与副作用。

全局/群规则保留 null 与空集合语义。Dispatcher 显式规则 map 按替换解码；Relay 公开配置提供 ETag/If-Match CAS，只返回 group_control 与 filters，不暴露凭据或私有路径。跨 Dispatcher/Relay 联合发布按步骤读回并记录 partial，不宣称分布式原子事务。回滚是基于当前版本的新发布，不撤销既有外部副作用。

新任务保留允许字段的不可变历史快照；旧任务没有的历史不从当前配置重建。WAL 持久化、重启恢复、迟到回声和提交去重保留原事实，不自动重发 unknown。

## Alma 上游事实

- `POST /api/personal/dispatch` 接受 task/title/model/sourceThreadId/sourceMessageId，直接返回 run。
- `GET /api/personal/runs/{id}` 的 running/waiting 继续轮询；done 必须有有效 summary，failed/skipped 为明确终态；GET 核对同一 run ID。
- 指定任务停止 API 为 `POST /api/personal/stop {runId}`。当前执行器取消只证明停止本地轮询，无远端停止证据时保持 may_be_running，不使用 stop all。
- personal dispatch 没有有效 agent_id 或请求级工具硬隔离，可能继承个人上下文，权限证据为 soft；不能冒充绑定指定 Agent 或硬沙箱。
- 受控纯模型走 `/anthropic-proxy/<providerId>/v1/messages`，不注册/执行工具，拒绝工具调用输出。`/api/chat/completions` 的空 tools 不能被当作所有 provider 的硬隔离证明。
- 模型目录来自 `GET /api/models`，personal model 为 providerID:modelID；纯代理 model 使用对应 provider 的裸模型 ID。源码不包含 provider 凭据。

## 验证范围与剩余产品边界

隔离验证覆盖 fake inbound → 实际 Relay/Dispatcher/runtimebridge/Engine → mock Alma → fake Bridge → 回声 → Console，以及不可达失败、权限拦截、版本回滚与 fake launchd 生命周期。真实后端冒烟为单独 opt-in 测试，必须保持假出口与独立状态，不与默认 mock 回归混为一次验证。

后端提供权限控制和证据，不会凭空新增执行器能力。网页工具、宿主写入、提醒同步和远端委派若没有受控实现就明确不可启用；旧外部适配分支保持原有能力。MyGo 桌面窗口和新前端仍由界面阶段实现。

首次生产切换应检查配置与状态归属、单实例锁、在途提交和可回退二进制。Relay 内存回放不等于持久消息队列，重启期间来源缺口不能被健康接口掩盖。私有配置和状态由操作者独立保存；公开示例默认不采集、不受理、不发送。本轮未做生产切换，也未 Git commit/push。


## 三层架构接入（2026-11-20）

业务架构确定为信息接入、处理与编排、动作执行。公开的微信消息处理和发送路径接入 `InboundEnvelope`、`ProcessingRequest/Result` 和 `ActionRequest/Receipt` 公共协议；适配端口位于 `internal/ports`，来源采集由外部宿主提供。多平台标识与支持能力不等价，B 站/Web 尚无真实适配器。

当前三层界面使用 `GET /console/v1/architecture` 读取快照，`GET /console/v1/architecture/events?domain=processing&cursor=...` 订阅真实 processing 变化。来源、动作独立事件流没有可靠事实源时仍返回 unsupported；旧 run SSE 保持兼容。具体字段、当前接线与缺失事实见 [三层方案](architecture-v2.md)、[微信映射](platform-mapping.md)；前端页面方案已同步到 [1.4 版](target-page-spec.md)。

本轮不会新增真实平台账号、切换现役服务、调用私有来源或发送测试群消息。动态 Agent 目录和能力引用挂载仍为 unsupported，不能仅因公共协议存在就宣称已经支持。


三层实际微信接线已验证：受理入口记录实际 ReceivedAt，统一 envelope 的安全引用进入持久 Run.Inputs；最终提交前转换并校验 ActionRequest，ActionRef 与提交边界同次 WAL 持久化。NONE、演练、权限拦截不创建 Action。Run 详情新增 inputs/action/receipt；架构 SSE 从事件自身读取关联，旧记录缺失保持缺失。真实 Echo 推断确认保留 SubmittedAt=null，不编造 ACK 时间。

`cmd/control-plane` 已注入与 Gateway 共用的 Engine，架构快照可读真实后端目录。`supported` 与 `availability.enabled` 分开；Bridge 动作启用需数字协议版本至少 2、非演练且权限证据齐全。前端同源客户端位于 `internal/console/architecture-client.ts`，提供 getArchitecture/subscribeArchitecture。独立 SourceAdapter/AgentRuntime/ActionAdapter 的完整生命周期未全部抽取，动态 Agent/skills/tools/知识库注册与挂载仍未实现；此范围明确保留，不以 DTO 存在冒充能力。
