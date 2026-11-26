# JINX 多来源自动处理控制台：页面规则、业务流程与对接规格

文档版本：1.4（补充公开核心／私有来源与本地策略边界）<br>
核对日期：2026-11-22（Asia/Shanghai）<br>
目标平台：macOS；MyGo Go 主进程 + WebView 前端<br>
状态：统一后端、三层协议、微信消息处理/发送接线和前端接口已完成隔离验证。2026-11-22 公开核心全工程 race 和私有来源组合链路通过；新前端尚未实施，现役服务未切换。本文仍含开发基线和目标规格，当前能力以运行入口、能力接口和对接说明为准。

公开 core 包含群聊处理、发送编排、adapter 接口及通用 Webhook/SSE 入口。微信本地采集及其实现和部署细节不在公开模块，独立私有模块只在本机配合 core 运行。页面仅消费公共接口和受限能力投影，不提供私有采集配置或部署向导。示例身份统一使用 demo/test，真实会话证据仅保留在私有文档。

## 0. 当前方案修订：来源、处理、动作（2026-11-20）

本版将业务架构确定为三层，微信消息处理和发送是首个集成路径，来源由宿主提供。既有五页按公共消息契约设计，但后续界面不能将所有来源都画成微信群或将 Agent、执行后端、模型合为一个对象。完整公共字段及适配边界见 [三层架构方案](architecture-v2.md)，实际后端入口见 [后端对接说明](backend-integration.md)。

| 层 | 职责 | 前端应具象化的内容 |
| --- | --- | --- |
| 信息接入 source | 接收已授权来源消息、规范化、去重、来源及回复定位；技术过滤 | 平台/接入实例、账号身份是否已验证、容器（群/视频/帖子）、健康/时效、支持动作和不支持原因 |
| 处理与编排 processing | 准入、聚合、决策、Agent/后端/模型选择、能力引用、执行和兜底 | 计划与实际选择、过滤/决策状态、尝试与兜底、skills/tools/知识库引用及实施强度、输入到任务的关联 |
| 动作执行 action | 可信目标解析、投递前权限复核、平台发送、幂等、确认与未知结果 | 平台/目标位置、动作种类、准入/提交/确认分离、外部 ID 与证据、失败/未知原因；禁止默认重发 |

控制面贯穿三层：配置、权限、暂停/维护、健康、审计和事件恢复不另算一个业务处理阶段。来源消息、执行任务、动作有各自身份，不能把 run ID 改名冒充 source message 或 action ID。批次可含多条来源消息；应展示 inputs[]，不能暗示每个 run 只对应一条消息。

### 0.1 界面与接口

新增 `GET /console/v1/architecture` 返回 `layers/sources/processing/actions/eventStreams/revision/observedAt`。前端按三层展示现有能力，并保留每项 `supported/enforcement/source/reason`。健康不可达与适配器未实现是不同状态，不能用一个灰色开关混在一起。revision 是聚合投影指纹，不是跨服务 CAS 事务版本。

新增 `GET /console/v1/architecture/events?cursor=...&domain=processing`，事件名 `architecture`，使用真实 Dispatcher 原有 cursor 恢复。source/action 独立事件域在没有持久适配器事实源前明确 unsupported，不能把健康轮询伪装成实时来源/发送事件。processing 事件可以携带当时已持久的 envelope/source/action 关联，但不能将最新 run 的确认反填进更早的事件。

前端使用 `internal/console/client-contract.ts` 的三层 DTO 及 `internal/console/architecture-client.ts` 的同源客户端；原 `GET /console/v1/events` 和既有列表接口兼容保留。事件只触发需要的实体重读，不抹除未保存配置草稿；cursor reset 后重新读取架构快照和所需列表。

### 0.2 三层在界面中的位置

顶层保留总览与运行追踪，并将配置入口按「信息来源」「处理编排」「回复动作」分组；服务与维护作为贯穿三层的独立管理页。旧五页的字段按下面归属迁移，不需要为了兼容旧导航继续把所有内容塞进“群配置”。

| 入口 | 主要内容 | 当前可用数据 |
| --- | --- | --- |
| 总览 | 三层连线、真实依赖健康、事件覆盖范围、未知结果与注意事项 | architecture、services、attention、runs；节点不可用不等于消息丢失已被证明 |
| 信息来源 | 来源类型/接入状态；来源详情展示已配置容器与来源原生能力 | architecture.sources；容器仅使用获准接口返回的身份，不扫描私有来源补全 |
| 处理编排 | 规则/决策、Agent 与后端/模型区分、路由/兜底、权限及能力引用 | architecture.processing、现有 rules/routing/permissions/backends；动态能力目录未实现项只读说明 |
| 回复动作 | 平台×动作能力矩阵、可信目标、提交/确认和未知结果 | architecture.actions、run 详情与真实 action 关联；本轮不新增通用手工发送按钮 |
| 运行追踪 | 一次任务的多个 inputs、处理尝试和动作；点击来源/动作互相定位 | runs、run detail、processing SSE；旧记录缺失关联显示“历史未记录” |
| 服务与维护 | 现有白名单服务、暂停/排空/操作结果 | services/control/maintenance 原接口；实际部署版本单独展示 |

微信群与 B 站视频在界面中统一称“来源容器”，详情页可以按平台补充群 ID、视频 ID、根/父评论等专属字段。没有接入 B 站时仅展示接入说明，不让用户填写一套永远无法保存或执行的假表单。Agent 目录为空时显示缺少目录能力，不把可用后端行改名成 Agent 行。

事件订阅状态与业务状态分开：连接 fresh 表示流已连通；某项来源能力 supported 不表示每个容器可见；confirmed 需要实际外部证据；unknown 无自动重试按钮。source/action 流 unsupported 时仍可从 run 的真实关联展示已记录的动作，不宣称拥有平台全部活动记录。

### 0.3 平台与能力事实

B 站/Web 目前仅有平台协议接点，没有采集和回复适配器；界面只能展示未接入与原因，不能出现可工作的启用按钮。微信账号若只有 `wechat-relay` 本地适配器范围标识，不可显示为已核实登录账号。引用没有真实父/根消息 ID 时留空，不从引用正文猜位置。

Agent 身份、执行后端、模型和能力包分开。当前有后端/模型路由及权限执行，尚无完整动态 Agent 目录和 skill/tool/知识库挂载运行时；空数组不能被解释成已完成动态挂载。引用语法校验不等于允许列表授权。hard/soft/host/unverified 证据与旧规格一致，页面不能把 Alma personal 的提示词约束当硬隔离。

新的微信输入/动作映射必须在实际受理和提交点使用并留存安全关联；正文和完整 prompt 不进入架构事件。历史记录缺元数据时保留缺失，不从当前配置补造。详见三层方案的当前实现段和本轮验证记录。

## 1. 目的、标记与交接分工

统一控制台要回答：当前哪些群可以自动回复、消息走到了哪一步、为什么没有回复、实际使用什么后端及权限、修改什么时候生效、服务能否安全维护。融合的是管理入口；Relay、Dispatcher、Decision、Bridge 继续作为独立 launchd 服务运行，关闭窗口不停止它们。JINX 是群内身份，不是一个可统一 kill 的常驻进程。

全文使用三种标记：**【当前】**为编写该章节时核实的行为；**【目标】**为必须有实现和验证才能开放的契约；**【建议默认】**为仍可由产品负责人审校的产品决策。未标记的页面行为均属于目标。第 2–12 章保留开发基线，不代表最终实现清单；当前能力以第 0 章、架构方案、后端对接说明和运行时 capability 为准。V1/V2 是实施里程碑，不可把“V1 目标”读作“今天可用”。

交接责任：

| 角色 | 交付与责任 |
| --- | --- |
| 设计与前端开发 | 按本规格输出视觉设计文档、交互稿、前端代码；自行决定视觉语言、颜色、像素、字体及响应布局 |
| 产品/业务维护 | 维护字段、流程、状态、版本边界和验收标准，评审设计是否误导 |
| 业务与对接开发 | 负责 Go Console Facade、Dispatcher 埋点与控制、routing 配置、权限校验、持久记录及具体适配；统一后端与三层增量已实施，继续负责审查和真实前端对接 |
| 代码审查 | 检查前端及集成代码，核对 DTO、实际调用、并发与发布语义、安全边界、模拟与真实能力区别 |
| 后续运行维护负责人 | 验证 launchd 白名单和运行健康，按另行授权执行部署；本文不是操作授权 |

本稿无需其他上下文即可用于设计。不得要求设计 AI 推断现有接口、读取真实聊天或实现后端。首版只面向本机单用户，不建账号/RBAC、云平台或远程控制入口。权限编辑指自动执行与工具能力策略，不是用户角色管理。

## 2. 统一架构与通用交互规则

### 2.1 数据与控制边界

业务链路：已授权来源 → core Relay 通用 Webhook/SSE → Dispatcher（聚合、过滤、Decision、队列）→ runtimebridge/执行后端 → Bridge 提交 → Relay 接收自身消息回声 → Dispatcher 确认。普通 Alma 分支调用 personal dispatch，兼容外部适配器按自身能力接入。所有服务地址由操作者配置；当前实际投递能力只有文本。

【目标】WebView 只调用 MyGo 主进程提供的 `ConsoleClient`。Go Facade 聚合只读状态、脱敏、版本控制和白名单动作，作为唯一控制入口；不能让页面分别跨端口请求现役服务，也不能给页面 shell、文件路径、任意 URL 或原始脚本执行能力。Facade 不访问私有来源补足观测，不伪造结构化 run。Dispatcher 需在实际受理、执行、提交和确认处产生事件。

【当前】loopback Host + Origin 同源校验不是登录/RBAC，不能因为都在 localhost 就直接跨端口调用。目标 WebView 采用受限本机绑定或受限宿主 RPC，拒绝非预期来源和远程页面调用；CORS 不得开放 `*`。前端 DTO 与现役配置 JSON 分离，ETag、内部路径、提示词和凭据由 Go 管理。

### 2.1.1 已确认的统一 Go 工程组织（2026-11-19）

Relay 与 Dispatcher 合为同一仓库、同一个 Go module，仍保留独立程序入口和 launchd 生命周期。目标 module 为 `jinx`，目录分为 `cmd/relay`、`cmd/dispatcher`，共享业务协议放 `internal/contracts`；任务记录和事件放 `internal/observability`，前端聚合与配置适配放 `internal/console`。MyGo 桌面入口后续接入；开发期可提供受限本机 `cmd/console-api` 方便联调，不代表新增常驻服务已部署。

core 保留通用 Relay SSE、Dispatcher API 语义。源码统一不自动迁移运行状态，不自动更改现役 plist、单实例锁或数据目录。前端仍只调用 ConsoleClient/受限 Facade，由适配层屏蔽进程与传输细节。公开工程根目录记为 `<PROJECT_ROOT>`，独立本机私有工程根目录记为 `<PRIVATE_ROOT>`。

后端优先建立真实健康、任务事实、配置发布和控制边界。视觉设计与 mock 前端可以并行；任何新增后台功能只有实际测试与能力发现确认后，前端才能开放。MyGo 官方文档工具链需求高于现有服务 Go1.22，因此 MyGo 依赖版本与工具链须在接入桌面入口时单独验证，不能让未固定的框架升级阻塞服务迁移。

### 2.2 全站状态、时效与导航

固定五个导航：总览、运行记录、群配置、服务管理、配置变更。页内深链接使用群 ID、run ID、change ID 等稳定标识；回退保留筛选和滚动位置。顶部持续展示连接状态、全局自动回复控制状态、维护状态及最后同步时间。页面标题、按钮和字段须可键盘操作，状态不能只靠颜色表达。

每个数据块均携带 `observedAt`、`freshness`、`source`、`completeness`。时间用带时区 ISO 时间传输，界面显示本地日期时间和时区；相对时间同时支持查看绝对时间。来源可为现役累计统计、目标结构化记录或旧日志；不可混算。建议普通快照 5 秒刷新，15 秒未获新快照标记 stale，30 秒标记 disconnected；阈值为控制台连接策略，不是服务死亡判据。SSE 心跳只能证明通道活跃，不能刷新业务进展时间。

| 状态 | 展示及允许动作 |
| --- | --- |
| loading | 首次加载占位；不得闪现零计数或默认“正常” |
| empty | 请求成功且确实无记录；说明当前过滤条件，可清空筛选 |
| stale | 保留最近快照与采集时间，禁止基于陈旧状态提交控制操作，提供刷新 |
| unknown | 事实不足或结果不确定，给出缺失来源；不得显示为失败、零或未发生 |
| error | 区分连接、权限、校验、服务错误；可重试读取，保留已编辑内容 |
| conflict | 版本过期，展示基准/本地/服务器差异，刷新后重新预览，不自动覆盖 |
| pending / verifying | 请求已发出或结果核验中，锁定重复提交；超时不推断失败 |
| unsaved | 标签及离页提醒；可留在页面、丢弃，或在表单有效时保存 |
| unsupported | 说明未接入的能力和替代查看位置；不显示可点击的假按钮 |
| partial | 一些来源成功、一些不可用；逐项标记，不能整页称为健康 |

筛选无结果与数据获取失败必须不同。切换群/后端时不丢草稿；建议草稿仅存在当前 UI 内存，退出明确提示，不自动写磁盘。服务推送更新不覆盖表单；提示“服务器已有新版本”，由用户比较。前端乐观更新只用于筛选和展开；配置生效、暂停、发送结果、服务停止等必须等待后端事实。

### 2.3 数据最小化、确认与并发

列表只展示群别名、消息数量、触发原因和脱敏摘要，不展示全群正文。详情的摘录也由后端按策略裁剪；默认不返回完整 prompt、系统指令、token、API key、用户目录、原始异常堆栈及可含聊天内容的日志。可编辑的规则文本是用户配置，不是实际拼接 prompt。无“导出全部原始日志”入口。后续若需诊断导出，须另定脱敏契约。

读取和无副作用筛选无需确认。保存配置前必须看字段差异、有效值和影响范围；停止/重启服务、修改路由目标、扩大可执行权限、全局暂停需说明影响后确认。恢复自动回复须说明恢复水位；不要求输入固定口令。禁止重复弹窗，最终确认只针对已生成的预览。暂不提供强制重启、unknown 重发、手动发送测试消息。

所有 mutation 都有 `requestId`（防重复）和版本前置条件；服务动作还绑定计划版本及运行实例。后端重新读取状态并验证能力、版本、并发冲突与允许动作。前端 disabled 只是提示，不构成权限校验。网络超时后按 requestId 查结果，不盲目重试；查询仍无事实则显示未知并回读。

## 3. 总览：逐区字段、状态和点击

### 3.1 自动回复控制区

显示全局状态（运行中/暂停中/恢复中/未知/未接入）、各群暂停数量、维护状态、最后控制人（本机操作）及时间。全局与群级任一暂停都阻止该群新执行和未提交自动发送。按钮能力来自后端；当前无 pause API 时展示“暂停控制未接入”，不能以 `dry_run` 或 `enabled=false` 实现。

点击暂停打开影响预览：作用域、当前 Pending/执行/提交待确认数量、暂停边界、远端执行可能继续、已提交仅确认。点击恢复展示新水位、被保留边界内工作的处理结果及“不自动回放”；确认后等待后端水位与状态读回。暂停/维护冲突规则见第 8 章。

### 3.2 链路与计数区

链路节点为 Relay、Dispatcher、Decision、执行分支、Bridge、回声确认。每个节点分别展示进程状态、探针状态、最后业务事件及观测时间；“执行分支”显示近期实际分支分布，不伪装为单一进程。点击节点进入服务详情或对应运行筛选。Bridge 授权正常不代表投递已确认；Relay 连通不代表所有群均可见。

【当前】统计卡逐项标明“持久累计”：`total_received` 收到消息、`total_triggered` 触发、`total_submitted` 提交、`total_replied` 已确认回复、`total_dry_run` 演练、`total_send_failed` 发送失败、`total_unknown` 交付未知、`replay_gaps` 回放缺口。`pending_messages` 显示为“待处理及处理中消息”，因为包含 Pending + InFlight；不可写成纯队列长度。`relay_connected` 单列连接状态。

累计计数可能重叠，unknown/failed 不是互斥桶；不得由它们推导成功率、单群数量或今日数据。没有结构化记录时，今日触发、平均时延、当前 run 数显示“尚无可靠数据”。【目标】结构化指标只有在记录覆盖完整时才按所选时间窗计算，明确统计起点；延迟只统计对应阶段均有时间戳的样本并标注样本量。

### 3.3 需要关注、群摘要与最近运行

“需要关注”按 unknown 交付、权限不满足、服务失联、配置核验失败、事件缺口排序；显示发生时间、范围、原因和查看入口，不默认提供重试发送。条目可标记本机已读，已读不改变业务结果。

群摘要每行：群别名/ID（可复制 ID）、Relay 可见性、规则已建/启用/白名单、全局与群暂停、有效 dry-run、配置目标、最近实际后端、最近确认、关注项。点击进入群详情。没有观测不能显示“该群没有消息”。最近运行展示 5–10 条可配置密度的记录，点 run 进入详情，“查看全部”保留当前时间窗。

## 4. 运行记录：列表、详情与旧记录

### 4.1 列表

每行字段：受理时间、run ID、来源群、触发类型（@、普通门禁、推广关键词、受限客服、未知）、消息数量、阶段、处理结果、交付状态、配置版本、计划后端/实际后端、耗时及完整性。群显示源群，跨群目标额外标记，不能只显示目标群。运行中耗时从受理起算；缺起点显示未知。

筛选：时间范围、群、阶段、处理结果、交付、后端、是否降级、配置版本、完整性；搜索仅匹配 ID/脱敏原因及别名，不做全群正文检索。分页游标由服务器生成，切换筛选重置游标；新记录到达显示“有新记录”再刷新，避免正在查看的行跳动。默认最近 24 小时，但覆盖不足要提示“仅自记录启用时可查”。

只进入受理流程的工作单元称 run。消息在受理前被过滤，如未来提供独立过滤观测，可单列 `decisionRecord`；不能为历史每条消息臆造 run。一个 batch 对应一个 run，多个 backend attempt 仍属于同一 run。

### 4.2 详情

详情顶栏展示：run ID、源/目标群、受理时间、处理阶段、结果、交付、记录完整性、配置快照版本。关联触发消息 ID、批次数和水位；默认不展示正文。脱敏 excerpt 缺失显示“未保留”，不补抓历史。点击配置版本只读查看当时规则，不把当前配置当旧快照。

时间线阶段：受理 → 聚合完成 → 门禁 → 排队 → 后端尝试 → 输出策略检查 → 提交 → 回声确认。每项有时间、原因码、来源和不确定性；未经过的阶段标“未进入”，观测缺失标“未知”，两者不同。跨重启显示恢复节点与缺口，不连成虚假的连续执行。

后端尝试区：attempt ID、分支、模型展示名、route/policy 版本、开始/结束、外部 run/request ID（若存在）、调用结果、是否可能仍在远端执行、降级原因。计划选择与实际调用并列。工具事件只有结构化证据时展示，不能从文本“我搜索了”推导工具调用。

交付区：发送前检查结果、提交时间、Bridge 关联 ID（若有）、回声消息 ID、确认时间、超时及不确定原因。`confirmed` 才用“已确认回复”；`submitted` 用“已提交，等待确认”；`unknown` 用“交付结果未知，禁止自动重发”。不提供撤回或重发入口。

【目标】取消动作只在后端返回允许且未越过提交边界时出现。无法取消远端时写“已阻止后续投递；远端仍可能运行”，不能写“已终止”。静默是模型/业务决定不回复；拦截是规则/权限不允许；失败是明确错误；未知是证据不够；四者必须独立可筛选。用户暂停造成的输出丢弃与用户取消分别记原因。

### 4.3 legacy 降级展示

【当前】`/api/events` 是 `{time,level,message}` 日志 SSE，最近 100 条、拥塞可丢、15 秒 keepalive，message 可能含正文。只能经脱敏后放“旧日志片段”，不能生成可靠 run ID、完整时间线、精确耗时和分群成功率。legacy 条目标为 `legacy_partial`，不提供取消等控制。无法关联时保持独立，不用相近时间强行拼接。切换“仅完整记录”时说明旧记录被隐藏。

## 5. 群配置：规则、权限与后端

### 5.1 索引与详情布局

索引列出已配置群及来源已授权暴露的群，不调用 Relay groups/history/stats 扫描补全。展示群 ID、别名、路由就绪性、有效启用、有效演练、后端、权限风险、当前版本和未保存标记。支持按 ID/别名、启用、后端和异常筛选。新增群要求明确输入 ID 与目标搜索名；“未在 Relay 可见”是独立阻碍，配置存在不等于能采集。

详情分“路由与触发”“批次与内容规则”“权限”“后端与会话”“变更预览”，同一个草稿整体校验。全局规则入口也在本页，明确全局作用域；不得把 `bot_names` 放成群级编辑。表单每项展示原始配置值、有效值、来源（本群/继承/默认/宿主）和可编辑状态，折叠高级字段不能抹去其存在。

【当前】有效群路由同时满足：存在 group_rule、`enabled=true`、在 `group_whitelist`；Relay 暴露可能另有限制。索引分别显示这三个条件，不能以一个开关掩盖。首版“启用此群”可由 Facade 生成同一次 Dispatcher 配置写入，补齐规则与白名单；预览仍显示实际两处字段。删除规则先移除白名单并校验引用；禁用现役群会清 Pending，必须提示不可恢复。删除不删除历史运行记录。

### 5.2 群字段完整表

以下字段为【当前】`GroupRule`。bool 未配置按 false；除明确继承的字段外，不发明通用继承规则。表单允许“使用默认/继承”时须保存对应零值或 nil，而不是把有效值写死。

| 字段 | 页面输入及作用域 | 有效值、校验与副作用 |
| --- | --- | --- |
| `enabled` | 群启用开关 | 还需白名单；禁用清 Pending，非暂停 |
| `target_group_id` | 目标群 ID | 空取源群；有效值须数字 `@chatroom`；跨群显著确认 |
| `target` | GUI 搜索目标名 | 必须明确、非空、无换行/NUL；配置校验不能证明 GUI 唯一性，不自动发测试消息 |
| `hermes_profile` | 会话隔离标识 | 空按群 ID 生成；有效值字母/数字开头，可含 `_`、`-`；不得共用或等于全局 Hermes profile |
| `dry_run` | 群演练 | 与全局 `bridge.dry_run` 任一 true 则不投递；仍生成，不是暂停 |
| `mention_only` | 仅 @ 触发 | 与有效 bot_names/门禁规则共同作用，不能承诺每次 @ 都回复 |
| `allowed_types` | 消息类型多选及高级数字类型 | nil 默认 `[1,49,81]`；显式 `[]` 不等于 nil；未知类型保留数字，不声称支持音视频生成 |
| `self_sender_ids` | 自身发送者 ID 列表 | nil 继承全局，显式 `[]` 覆盖；往返 DTO 不得因 omitempty 丢失区别 |
| `blocked_sender_ids` | 被屏蔽发送者 ID 列表 | 不输入姓名代替 ID；去空项和重复项后由后端校验 |
| `keyword_blocklist` | 群关键词屏蔽列表 | 展示群配置及全局约束，不能用清空本群绕过全局 |
| `regex_blocklist` | 正则列表 | Go regexp 服务端编译；前端 JS 测试不能当有效性结论；错误定位到条目 |
| `batch_size` | 批次条数 | 0 继承全局；有效 1–50 |
| `idle_timeout_sec` | 空闲聚合秒数 | 0 继承；有效至少 1，且不大于有效 max wait |
| `max_batch_wait_sec` | 最长聚合秒数 | 0 继承；有效不小于 idle 且不超过 300 |
| `max_reply_runes` | 回复字符上限 | 0 默认 500；有效 1–2000，按 Unicode rune，不是字节 |
| `reply_window_sec` | 限流窗口秒数 | 0 默认 45；有效 1–3600 |
| `max_replies_in_window` | 窗口回复上限 | 0 默认 2；有效 1–20 |
| `decision_context` | 内容门禁背景文本 | 最多 4000 rune；属于规则编辑，非完整系统 prompt |
| `sensitive_criteria` | 敏感判断标准 | 最多 2000 rune |
| `noise_criteria` | 噪音判断标准 | 最多 2000 rune |
| `safe_criteria` | 安全判断标准 | 最多 2000 rune；不能理解为绕过其他权限 |
| `decision_threshold` | 判定阈值 | 0 按 0.5；非零有效 0.5–1；显示实际阈值 |
| `promo_keywords` | 推广路径关键词 | 最多 200 个，每项 trim 后 2–40 rune；命中可跳过 noise 判定，仍拦敏感，不保证回复 |
| `promo_shadow` | 推广观察模式 | 运行推广生成但不投递；与群/全局 dry-run 分别标记 |
| `promo_daily_cap` | 推广每日上限 | 配置 0 实际 1；合法 0–20，按本地自然日；不得写“0 不限量” |
| `support_mode` | 受限产品客服状态 | 仅特定产品群合法，首版只读或走专用受限流程；不是通用客服开关 |
| `support_since_ms` | 客服生效水位 | 毫秒时间戳，support 模式要求正数；展示日期与原始单位，不随意回溯 |

support 模式还受固定群、已核验自身身份、非 mention_only、无 promo shadow/keywords 等代码条件限制。通用群页应给出“仅专用路由支持”，不得复制真实群 ID 或身份到示例，不把这些限制去掉以方便设计。

### 5.3 全局字段与保留原则

【当前】`Rules` 还包括全局 `batch_size`（1–50）、`idle_timeout_sec`（1–300）、`max_batch_wait_sec`（0 规范化为 15，最终 idle≤max≤300）、`max_message_age_sec`（0→180，有效 1–3600）、`laya_url`、`laya_filter`、`bypass_decision_on_mention`、`bot_names`、`self_sender_ids`、`group_whitelist`、`group_profiles`、`group_other_bots`、`group_rules`、`default_profile`、`keyword_blocklist`、`ignore_xml`、`session_max_turns`（0 关闭，0–1000）、`session_max_age_sec`（0 关闭，0–30 天）。

全局表单分聚合、门禁、身份、会话。`bot_names` 明确全局；`bypass_decision_on_mention` 的预览需提示扩大哪些消息绕过 Decision，但不能绕过发送权限。`laya_url` 作为基础设施来源首版只读，不开放任意 URL；`laya_filter` 可按能力编辑。`group_profiles`、`group_other_bots`、`default_profile` 的兼容映射由 Facade 解析并展示解释，未完成作用优先级核验前只读保留，不能因群表单没显示就删掉。`ignore_xml` 不等于所有 XML 类型都不可触发，以 Dispatcher 实际规则为准。

`server`、`relay`、`hermes` 是现役重启类配置；群后端页不能直接编辑其原始路径、命令、URL。`bridge` 可热加载，但首版常规编辑仅开放 dry_run；URL/timeout 的变更走受控基础设施方案，不能给前端自由路径输入框。Go 持有完整配置，按字段保留未编辑部分；目标 DTO 不复刻完整内存 JSON。

### 5.4 权限表单

每项权限展示：能力名称、配置意图、实际有效约束、证据来源、强制级别、可编辑性、后备行为。建议能力目录：执行生成、自动文本投递、受控网络检索、资料持久化、提醒同步、宿主工具、远端委派；不把尚无代码边界的能力标为“关闭即禁止”。

级别分 `hard`（代码或隔离环境强制）、`soft`（提示词约定）、`host`（宿主提供及管控）、`unverified`（未核验）。对 soft/unverified 项首版只能展示约定文本与风险，不能做声称强制生效的开关。host 项必须说明本机 Console 不能代表宿主全部行为。支持编辑的 hard 项必须关联实施点和策略版本，后端验证后才保存；V1 至少实现发送门禁与后端能力准入，否则对应编辑能力不得宣告验收。

Alma personal 的权限证据为 soft；纯模型执行器不注册工具并拒绝工具调用输出。兼容外部 adapter 的工具约束和宿主行为需按实际实现核验，不能将某个工具白名单当作整链只读证明。页面按 capability 展示，不开放尚无安全执行器的网页工具、宿主写入、提醒同步或远端委派。有效权限按约束交集求得，后备不得扩大；无法证明同等约束则阻止降级并留下原因。

### 5.5 后端与会话表单

展示后端类型（Alma personal、纯模型、兼容外部执行器）、是否已接入、模型展示名、连接健康、工具约束、会话策略和降级链。只有已接入且符合群模式及权限的选项可选；被禁选项有原因。模型选择来自后端 catalog，不允许任意字符串/URL/脚本。profile 只说明本机配置范围，不证明 Agent 绑定或真实会话复用。

兼容外部 adapter 的分支/profile 映射不能呈现为真实 Agent 绑定，也不公开操作者的映射集合、模型路由或地址。结构化执行器的模型来自受信 catalog，合成示例仅使用 `demo-provider:demo-model`；配置变化不改写当前 run 的历史尝试。独立会话以外的复用模式尚未提供，不得把 session 字符串或 profile 名当成复用证明。

【目标 V1】新增结构化 routing 配置，至少含 schemaVersion、route ID、分支、catalog model ID、策略版本、允许的降级路线和会话轮换策略。Facade 校验 catalog 及模式后发布，由 adapter 新执行读取；前端不修改脚本。若某分支仍硬编码模型，显示只读实际模型，不能假装可切换。切换模型/后端时预览哪些新工作生效，必要时新会话；旧 run 保留原尝试。真实 Agent ID 绑定属于 V2。

### 5.6 保存预览

点击“预览变更”生成：字段 old/new、有效值 old/new、影响群、配置组件、热加载/新执行/需重启、可能失效的旧批次、权限变化、会话轮换及告警。全局字段变化即使当前群表单无差异，也要列出继承它的群。无实际差异则不发布。

保存结果分已核验生效、部分应用、核验中、冲突、拒绝、失败且确认未生效、结果未知。配置更新会使持旧完整 config hash 的批次输出在发送前失效，即使只改其他群，也需显著提醒；不能承诺只影响所编辑群。dry-run 开关改变同样是配置变更。发布与核验详见第 7 章。

## 6. 服务管理：进程、健康与维护动作

### 6.1 服务列表与详情

只展示后端固定白名单中的可管理服务。进程状态来自 launchd，不通过进程名猜测；label 不由前端自由输入。下表 label 均为 demo 名称，不代表已配置或可以控制的本机服务；实际能力以服务接口为准。

| 服务 | demo launchd Label | 业务依赖 |
| --- | --- | --- |
| Relay | `com.demo.jinx.relay` | 通用 Webhook/SSE；不允许控制台探针触发来源采集 |
| Dispatcher | `com.demo.jinx.dispatcher` | 路由、聚合、门禁编排、交付确认 |
| Decision | `com.demo.jinx.decision` | 内容门禁；是否必需取决于有效规则 |
| Bridge | `com.demo.jinx.bridge` | 提交与宿主授权 |

每行分别展示：launchd 已注册/运行/停止/未知，实例标识和启动时间（可得时）、最近退出码（可得时）、探针状态/延迟/时间、业务状态、依赖阻碍、允许动作。`PID` 仅作诊断，不能据此提供任意 kill。详情显示脱敏错误与修复建议；服务未运行时不自动替用户启动。KeepAlive 可能拉起被 kill 的进程，受控停止必须具有明确的 launchd 管理语义及读回验证，不能把 kill 成功当停止成功。

【当前】Decision `GET :8765/health` 提供 status/service/model/protocol_version，未就绪可返回 503。Bridge `GET :5033/health` 提供 protocol_version/group_routing/serial_ui/accessibility_trusted/screen_capture_authorized/dry_run。分别展示协议兼容性、路由支持、串行 GUI、辅助功能/屏幕捕获授权及演练状态；缺字段显示未报告，不能默认为 false。Bridge health 不证明实际发送成功。

Relay、Dispatcher 使用只读内存 health（进程/协议/内存连接/水位/最近进展）；不以 Relay `/api/v1/groups`、history、stats 作健康探针，也不访问私有来源补齐健康。Dispatcher stats 只证明其接口可读及 relay_connected 报告，不替代完整 health。微信客户端单独列为外部依赖，只展示宿主可安全获取的可见/授权状态，不 kill/restart、不发送测试消息。

### 6.2 动作流程

【目标】启动：后端白名单校验 → 检查注册与依赖 → 执行受控启动 → 等待新实例/健康 → 读回；启动进程但不就绪显示“已启动，未就绪”。停止/重启：生成影响计划 → 进入维护排空 → 处理已受理工作与确认 → 满足边界 → 受控 launchd 动作 → 读回。重启必须验证实例变化，不能只看接口又能访问。

动作状态为 planned、draining、blocked、executing、verifying、succeeded、failed、unknown。按钮一旦提交即锁定同服务重复操作，后端再校验 planVersion、服务实例和 drain 条件。依赖变化或出现未知交付时阻止执行，给出原因及“继续等待/取消维护”；不提供“忽略并强制”。多个服务联动只接受后端生成的依赖计划，不让前端硬编码顺序。

服务停止期间保持控制台可读；关闭 UI 不停止服务、不取消 run、不自动恢复暂停。再次打开从快照恢复，不能由前端默认值重置控制态。权限缺失时显示系统授权说明，不自动改变系统配置。

## 7. 配置变更：版本、发布与核验

配置变更是独立第五个导航页。列表字段：change ID、时间、组件、范围、基准/目标/读回版本、字段数量、状态、是否来自外部、影响 run 数（有证据时）、本机操作标记。筛选按组件、群、状态、日期。详情显示字段 diff、发布步骤、核验事实、相关 run 和错误；不保存 secret 或完整 prompt。历史快照按允许字段保存，不直接开放任意 JSON 下载。

【目标】Facade 分别管理 Dispatcher、routing、权限等组件版本，组合版本是版本向量及指纹，不是单一事务号。Dispatcher ETag 作为 opaque 值原样保留在 Go，不去引号、不据显示 hash 重算；前端只持脱敏版本 token。发布流程：

1. 读取最新实际配置与版本，构造保留未知兼容字段的草稿；只允许明确支持的 schema 字段参与编辑。
2. 服务端校验全部依赖、权限与有效值，生成字段 diff、风险、组件顺序和短期有效 plan token。
3. 用户确认后，再校验基准、plan token、全局控制态及 requestId；任何漂移均拒绝旧计划。
4. 各组件按计划写入并逐个读回，记录 applied/verified/failed/unknown；V1 跨组件发布不能称原子。
5. 全部读回一致才显示“已核验生效”；API 返回成功但读回不一致仍为核验异常。明确每个组件何时生效，关联受影响工作。

【当前】Dispatcher 保存顺序是文件原子替换 → 内存更新 → timer 重排/禁用群清 Pending → 持久 state。末步失败返回 500 时，配置可能已经改变，不具备全程事务。无论 500、连接断开还是超时，先 GET 回读：与目标相同则“配置已应用，状态持久化待核验”；与基准相同且有足够证据才称未应用；其他值显示冲突/漂移。不要自动回写旧配置来“补偿”，那会再次产生副作用。

冲突视图保留基准、本地、服务器三列，支持逐字段重新选取后生成新计划；不提供跳过 If-Match 的强制覆盖。map/array 在底层全量替换，Facade 必须基于最新完整值合并，不能因修改一群删掉其他群。可行时界面按条目 diff，执行仍遵守现役替换语义。

【当前】文件无 watch；外部改文件不代表内存已生效。页面分别标“运行内存版本”和“磁盘漂移状态”；磁盘检查仅在后端获准的固定配置范围内进行，不暴露路径。无法安全读取时显示“未核验”，不能靠 GET config 声称文件一致。下次重启可能采用磁盘版本，维护预览要纳入此风险。

【目标】回滚是基于当前版本创建一次新发布，恢复允许字段的历史值；不是倒退历史、回滚已发送消息、复活旧 run 或恢复旧会话。先生成差异并重新校验现有 schema/权限/目标依赖，不能恢复不再支持的字段。未完成历史持久化与发布实现前，回滚显示“不支持”。建议 V1 历史保留最近 100 次变更和 30 天元数据，run 元数据保留 7 天，均为目标默认且须审校；不保留全群正文。清理不得抹掉仍待确认的交付和必要去重/水位事实，未决数据需保留到终结策略明确。

## 8. 自动回复暂停、维护排空、取消：独立控制契约

【当前】没有真实 pause/resume、drain、按 run 可靠取消 API；Bridge `/action/park`、`unpark` 也不是暂停。下列均为目标。任何一项未实施并验证时，页面只能展示 unsupported。

### 8.1 暂停自动回复

【建议默认】全局/群级暂停在 Dispatcher 原子写入持久边界：停止接受该作用域的新自动执行，同时阻止尚未提交的自动发送；继续采集、追踪和确认已提交工作。暂停与进入发送提交的判定必须共享服务端串行化边界；UI 点击时间不是边界。若发送已经在边界前提交，仅继续确认，不撤回。

暂停期收到消息不自动回放，记录受限水位/计数，不积存无限正文。暂停前受理而尚未提交的工作保留其受理和暂停关联，但在恢复时按 paused_boundary 终结并丢弃输出，不偷偷补发。正在执行的远端任务可能继续，结果返回仍被门禁拦截。恢复建立新的受理水位，只接受其后的新触发；已有提交的回声确认不受新水位限制。过期工作标 expired；被暂停边界终结标 suppressed + paused_boundary，两者分开。

暂停状态由服务持久化，服务重启及 UI 重开不能自动恢复。状态读回包含 revision、scope、effectiveAt、boundary、未决提交数及原因。对于“是否可能已提交”不能证明的工作，进入 unknown 并继续确认，禁止恢复时重试。

### 8.2 维护排空

【建议默认】维护 drain 停止接受新工作，允许维护边界前已受理的工作按原许可继续生成和交付，并等待必要确认；它不是暂停发送。排空范围覆盖待聚合/排队/执行/提交待确认工作，不能只检查 `pending_messages=0`。进入前显示预期处理范围及每阶段数量；进行中显示剩余数量、最久工作、阻碍和期限。

超时只允许继续等待或取消维护；取消维护恢复新工作受理，不取消已受理 run、不撤回已提交消息。drain 超时不自动 kill，不把交付 unknown 当排空成功。未决远端可能继续执行时由计划判定是否阻碍安全维护，不能抹掉它的事实。

若暂停和 drain 并存，暂停优先阻止交付，维护显示 `blocked_by_pause`。用户可选择继续等待、取消维护，或明确解除相应暂停；解除暂停时仍遵守暂停边界终结规则，不能借维护把已拦输出补发。终结后的未提交工作可从 drain 活动计数移除，但保留远端未决标记；是否可停服务由安全计划重新判定。

### 8.3 取消单次运行

取消分“阻止后续投递”和“确认后端执行已停止”两个事实。未执行工作可可靠终结；已执行未提交工作可先持久化投递拦截，再尝试后端取消；远端无可靠取消能力时明确提示执行可能继续。已进入提交边界不允许取消交付，只继续确认。取消请求必须绑定 run revision，跟发送门禁并发校验；若竞态中先提交，返回状态冲突及新交付状态，不显示取消成功。

【当前】adapter timeout 后可能回落 Hermes，远端 Alma 仍运行，无可靠取消。目标不得沿用“超时即任务已停止”的假设。对于存在重复远端执行风险的降级，必须满足策略、权限和会话要求并记录两次 attempt；不能启动第二次投递，最终仅允许一次发送边界提交。

## 9. Run 状态模型与恢复规则

单一 status 无法表达“生成失败但另一尝试成功”或“提交后结果未知”。目标数据分以下正交维度；前端徽标组合表达，不自行改状态。

| 维度 | 允许值 | 含义 |
| --- | --- | --- |
| phase | accepted / aggregating / gating / queued / executing / checking / submitting / confirming / terminal / unknown | 当前可证实的阶段；unknown 表示阶段事实不足 |
| outcome | pending / replied / silent / blocked / suppressed / expired / canceled / failed / unknown | run 的业务结果；replied 仅在确认后成立 |
| delivery | not_started / blocked / submitting / submitted / confirmed / failed / unknown | 交付事实；silent/expired 通常 not_started，权限/暂停可 blocked |
| attempt status | queued / running / succeeded / failed / timed_out / cancel_requested / canceled / unknown | 后端尝试事实；succeeded 只代表生成完成 |
| remoteExecution | stopped / may_be_running / unknown / not_applicable | 避免把本地超时当远端停止 |
| completeness | complete / partial / legacy_partial | 是否足够支持完整追踪 |

正常路径是 accepted→aggregating→gating→queued→executing→checking→submitting→confirming→terminal；实际没有经过的聚合/门禁允许跳过并带原因。静默、拦截、过期、取消、明确失败可从相应阶段到 terminal。配置 hash 过期输出为 suppressed / config_changed，dry-run 输出为 suppressed / dry_run，promo shadow 为 suppressed / promo_shadow；都不算 replied。

发送前需要一起复核配置 hash、权限版本、路由目标、暂停/维护边界、取消 revision、过期及有效 dry-run。仅普通轮询检查再异步发送会存在竞态，需持久化一次性的提交准入事实，并在提交组件执行时验证边界。配置改变使旧输出失效不等于主动取消后端。

以下组合是服务端输出、前端显示和 mock 共用的强制不变量，不能仅作为文案建议：

| 条件 | 必须同时成立的状态 | 禁止组合 |
| --- | --- | --- |
| `delivery=confirmed` | `phase=terminal`、`outcome=replied`，有可靠确认依据 | replied + submitted；confirmed + pending |
| `delivery=submitted` | `phase=confirming`、`outcome=pending`；超时转 unknown | submitted + replied/failed |
| `delivery=submitting` | `phase=submitting`、`outcome=pending`；尚不能证明 Bridge 已接受 | submitting + canceled |
| `delivery=unknown` | `phase=unknown`、`outcome=unknown`；仍可被可靠新证据推进 | unknown + failed/replied |
| `delivery=failed` | 已证明未交付，`phase=terminal`、`outcome=failed` | 有可能提交时只因 HTTP 错误标 failed |
| `outcome=silent/blocked/suppressed/expired/canceled/failed` | `phase=terminal`，交付须是与原因一致的 not_started/blocked/failed | 已提交后再把旧 run 标 canceled/suppressed |

`outcome=unknown` 且交付未开始可用于执行事实不足，此时 `phase=unknown`、`delivery=not_started`，不得推断有交付。`delivery=blocked` 配合 blocked/suppressed/canceled，必要时 expired；必须有未越过提交边界的证据。`outcome=replied` 当且仅当 `delivery=confirmed`。

迟到的可靠回声可使 `unknown/unknown/unknown → terminal/replied/confirmed`；需记录新证据与修订序号，保留先前未知事件，不删除历史。一次生成或请求错误只是一条事件，不能覆盖后续更强的交付证据。

每个 run 关联源消息 ID、受理水位、配置向量及指纹、计划路由、实际 attempts、外部 ID（可空并附缺失原因）和投递关联事实。跨重启先恢复持久记录：已 confirmed 不再发；已 submitted/submitting 且无确定结论进入确认或 unknown；队列/执行是否可继续必须有持久所有权与去重证据，否则终结或标未知，不自动重新生成。重启不能从旧日志推断已发送。旧统计计数不反向生成 run。

## 10. 权限、版本与生效范围契约

策略记录至少包含 capability、requested、effective、enforcement、provenance、policyVersion、enforcedAt、editable、reason。hard 要有明确实施点，如 Dispatcher 发送门禁或 adapter 工具白名单；soft 不可被 UI 宣称为安全隔离；host 依赖的实际能力未知时不升级为 hard。

V1 的结构化路由与权限编辑必须有运行期读取者，不能只把 JSON 保存到磁盘就称生效。若后端 catalog 不能证明满足有效策略，前端禁选且后端拒绝发布。降级到 Hermes 或其他后端也要重新做能力准入；约束不满足则 blocked / fallback_policy_mismatch。不得因为主后端失败就默认放宽工具或宿主能力。

版本生效说明：

| 变化 | 生效范围 | UI 必须提示 |
| --- | --- | --- |
| Dispatcher rules/bridge 热加载 | 当前配置立即替换；旧 batch 发送前 hash 不同将丢弃 | 可能影响其他群已受理输出、timer、Pending |
| server/relay/hermes | 现役 API 拒绝，需另行受控重启方案 | 未实施该方案时只读，不承诺点保存立即生效 |
| routing/adapter 模型或映射 | 目标新执行/新进程读取；当前 attempt 保持原值 | 旧 run 可能仍在远端运行；模型切换可能轮换会话 |
| 权限收紧 | 目标立即拦截未提交发送；工具执行边界依各实施点 | 无法追溯撤销已经发生的工具行为 |
| 权限扩大 | 新执行按新版本生效 | 不提升旧 run 权限，不让既有输出借新权限补发 |
| pause/cancel | 持久边界后阻止未提交交付 | 远端可继续；已提交只确认 |

各 run 保留固定执行快照；实时控制和权限收紧是附加拒绝条件，不能用当前规则覆盖历史记录。V1 跨组件阶段性状态可能使新工作暂时不满足一致策略，应拒绝受理或阻止执行并说明原因，不能带不完整策略执行。

## 11. 前端统一 DTO、ConsoleClient 与模拟契约

### 11.1 约束

以下 TypeScript 是【目标、待实现】契约，不是现役 HTTP API；传输可用 MyGo RPC 或同源 facade，设计 AI 不自行发明 HTTP 路径。业务字段以 DTO 为准，框架、组件库及状态库可自行选择。所有 ID/token 均为不透明字符串。时间为带时区 ISO 字符串。`null` 表示未知/无事实，必须附原因或由状态解释；数字 0 是已知零。`undefined` 只用于明确可选部分，不能自动解释成禁用。

```ts
type ID = string;
type ISOTime = string;
type Freshness = 'fresh' | 'stale' | 'disconnected' | 'unknown';
type Completeness = 'complete' | 'partial' | 'legacy_partial';
type Component = 'dispatcher' | 'routing' | 'policy';
interface Availability { supported: boolean; enabled: boolean; reason: string | null }
interface Envelope<T> {
  data: T; observedAt: ISOTime; source: string;
  freshness: Freshness; completeness: Completeness;
  revision: string; // opaque，只用于相等性与并发条件，不能比较大小
  entitySeq: number; // 在 streamId 内单调；快照包含已处理事件位置
  streamId: ID;
}
interface Page<T> { items: T[]; nextCursor: string | null; coverageSince: ISOTime | null }
interface Problem {
  code: 'validation' | 'conflict' | 'unsupported' | 'stale' | 'forbidden'
    | 'unavailable' | 'too_large' | 'partial_apply' | 'unknown' | 'internal';
  message: string;
  fieldErrors?: { path: string; message: string }[];
  requestId: ID; retry: 'read' | 'replan' | 'none';
  currentRevision?: string; // 冲突后仍须读取相同 schema 的完整最新 DTO
}
type Result<T> = { ok: true; value: T } | { ok: false; problem: Problem };
interface Versions {
  dispatcher: string; routing: string | null; policy: string | null;
  fingerprint: string;
}
interface ActionContext { requestId: ID; expectedRevision: string }
interface GroupRef { id: ID; label: string; visibility: 'visible' | 'not_observed' | 'unknown' }
interface Watermark {
  token: string; // 后端不透明边界；不能用浏览器时间代替
  recordedAt: ISOTime;
  sourceSequence: string | null;
}
interface StageCounts {
  aggregating: number | null; queued: number | null; executing: number | null;
  submitting: number | null; confirming: number | null;
  unknownDelivery: number | null; remoteUnsettled: number | null;
}
interface MaintenanceView {
  id: ID; operationId: ID; revision: string;
  status: 'draining' | 'ready' | 'blocked_by_pause' | 'blocked' | 'executing'
    | 'verifying' | 'succeeded' | 'canceled' | 'failed' | 'unknown';
  admissionBoundary: Watermark; remaining: StageCounts;
  oldestWorkAt: ISOTime | null; deadlineAt: ISOTime | null;
  blockers: { code: string; summary: string; runId: ID | null }[];
  actions: { continueWaiting: Availability; cancelMaintenance: Availability };
}
interface ControlState {
  revision: string;
  scope: { kind: 'global' } | { kind: 'group'; groupId: ID };
  pause: 'active' | 'paused' | 'transitioning' | 'unknown' | 'unsupported';
  effectiveAt: ISOTime | null;
  pauseBoundary: Watermark | null;
  resumeWatermark: Watermark | null;
  awaitingConfirmation: number | null;
  maintenance: MaintenanceView | null;
  actions: { pause: Availability; resume: Availability };
}
interface AttentionItem {
  id: ID; code: string; summary: string; occurredAt: ISOTime;
  acknowledged: boolean;
  target: { kind: 'run' | 'group' | 'service' | 'change' | 'operation'; id: ID } | null;
}
interface Overview {
  controls: ControlState[]; services: ServiceView[];
  groups: GroupView[]; currentWork: StageCounts;
  legacyCounters: Record<'total_received' | 'total_triggered' | 'total_submitted'
    | 'total_replied' | 'total_dry_run' | 'total_send_failed' | 'total_unknown'
    | 'replay_gaps' | 'pending_messages', number | null>;
  relayConnected: boolean | null; attention: AttentionItem[]; recentRuns: RunView[];
}
type Phase = 'accepted' | 'aggregating' | 'gating' | 'queued' | 'executing'
  | 'checking' | 'submitting' | 'confirming' | 'terminal' | 'unknown';
type Outcome = 'pending' | 'replied' | 'silent' | 'blocked' | 'suppressed'
  | 'expired' | 'canceled' | 'failed' | 'unknown';
type Delivery = 'not_started' | 'blocked' | 'submitting' | 'submitted'
  | 'confirmed' | 'failed' | 'unknown';
type RemoteExecution = 'stopped' | 'may_be_running' | 'unknown' | 'not_applicable';
type CancelMode = 'block_delivery' | 'stop_execution';
interface AttemptView {
  id: ID; backendId: ID; modelLabel: string | null;
  status: 'queued' | 'running' | 'succeeded' | 'failed' | 'timed_out'
    | 'cancel_requested' | 'canceled' | 'unknown';
  remoteExecution: RemoteExecution;
  startedAt: ISOTime | null; endedAt: ISOTime | null;
  externalId: ID | null; missingExternalIdReason: string | null;
  fallbackReason: string | null; versions: Versions;
}
interface RunView {
  id: ID; revision: string; source: GroupRef; target: GroupRef;
  acceptedAt: ISOTime | null; updatedAt: ISOTime; lastProgressAt: ISOTime | null;
  admissionWatermark: Watermark | null;
  trigger: string; messageCount: number | null; messageIds: ID[];
  phase: Phase; outcome: Outcome; delivery: Delivery;
  reasonCode: string | null; summary: string;
  completeness: Completeness; versions: Versions;
  configSnapshotId: ID | null; missingSnapshotReason: string | null;
  plannedBackendId: ID | null; attempts: AttemptView[];
  excerpt: string | null;
  sideEffects: { kind: string; status: 'confirmed' | 'failed' | 'unknown';
    occurredAt: ISOTime | null; summary: string }[];
  deliveryEvidence: { submittedAt: ISOTime | null; confirmedAt: ISOTime | null;
    bridgeRequestId: ID | null; echoMessageId: ID | null };
  actions: { blockDelivery: Availability; stopExecution: Availability };
}
interface TimelineItem {
  id: ID; runId: ID; occurredAt: ISOTime | null; source: string;
  kind: string; reasonCode: string | null; summary: string;
  certainty: 'observed' | 'missing' | 'not_entered'; attemptId: ID | null;
}
interface FieldValue<T> {
  configured: T | null; effective: T | null;
  origin: 'group' | 'global' | 'default' | 'host' | 'unknown';
  editable: boolean; reason: string | null;
}
type FieldMap<T> = { [K in keyof T]: FieldValue<T[K]> };
interface GroupRuleValues {
  support_mode: boolean; support_since_ms: number; self_sender_ids: string[] | null;
  enabled: boolean; target_group_id: string; target: string; hermes_profile: string;
  dry_run: boolean; mention_only: boolean; allowed_types: number[] | null;
  blocked_sender_ids: string[]; keyword_blocklist: string[]; regex_blocklist: string[];
  batch_size: number; idle_timeout_sec: number; max_batch_wait_sec: number;
  max_reply_runes: number; reply_window_sec: number; max_replies_in_window: number;
  decision_context: string; sensitive_criteria: string; noise_criteria: string;
  safe_criteria: string; decision_threshold: number; promo_keywords: string[];
  promo_shadow: boolean; promo_daily_cap: number;
}
interface GlobalRuleValues {
  batch_size: number; idle_timeout_sec: number; max_batch_wait_sec: number;
  max_message_age_sec: number; laya_filter: boolean; laya_url: string;
  bypass_decision_on_mention: boolean; bot_names: string[]; self_sender_ids: string[];
  keyword_blocklist: string[]; ignore_xml: boolean;
  session_max_turns: number; session_max_age_sec: number; dry_run: boolean;
}
interface GlobalRulesView {
  fields: FieldMap<GlobalRuleValues>; versions: Versions;
  // 兼容映射只读，群规则通过 getGroup 获取，白名单通过 admission 操作管理
  compatibility: { groupWhitelist: ID[]; groupProfiles: Record<ID, string>;
    groupOtherBots: Record<ID, string[]>; defaultProfile: string };
}
interface SchemaEntry {
  label: string; help: string;
  widget: 'switch' | 'integer' | 'decimal' | 'text' | 'multiline' | 'string_list'
    | 'number_list' | 'readonly';
  unit: 'seconds' | 'milliseconds' | 'runes' | 'count' | 'none';
  nullable: boolean; nullMeaning: 'inherit' | 'default' | 'not_allowed';
  minimum: number | null; maximum: number | null;
  maxRunes: number | null; effect: 'hot_reload' | 'new_execution' | 'restart_required';
  editable: boolean; readOnlyReason: string | null;
}
interface FormSchemas {
  schemaVersion: string;
  group: { [K in keyof GroupRuleValues]: SchemaEntry };
  global: { [K in keyof GlobalRuleValues]: SchemaEntry };
}
interface PermissionView {
  capability: string; requested: boolean | null; effective: boolean | null;
  enforcement: 'hard' | 'soft' | 'host' | 'unverified'; provenance: string;
  policyVersion: string | null; enforcedAt: string[]; editable: boolean;
  reason: string | null;
}
interface SessionPolicy {
  mode: 'independent_run' | 'isolated_session';
  rotateAfterTurns: number | null; rotateAfterSeconds: number | null;
}
interface RouteAttemptPolicy {
  backendId: ID; modelId: ID;
  timeoutSec: number; session: SessionPolicy;
}
interface RoutingView {
  routeId: ID; schemaVersion: string;
  primary: RouteAttemptPolicy; fallbacks: RouteAttemptPolicy[];
  editable: Availability; agentBinding: 'branch_only' | 'verified_agent' | 'unverified';
  agentLabel: string | null;
}
interface BackendOption {
  id: ID; label: string; branch: 'alma' | 'support_web' | 'recharge' | 'hermes';
  models: { id: ID; label: string; selectable: Availability }[];
  timeoutBounds: { minSec: number; maxSec: number } | null;
  sessionModes: SessionPolicy['mode'][]; selectable: Availability;
  agentBinding: 'branch_only' | 'verified_agent' | 'unverified';
  permissionEvidence: PermissionView[];
}
interface GroupView {
  group: GroupRef; revision: string; versions: Versions;
  routeExists: boolean; enabled: boolean; whitelisted: boolean;
  effectiveDryRun: boolean | null; fields: FieldMap<GroupRuleValues>;
  permissions: PermissionView[]; routing: RoutingView | null;
  missingRoutingReason: string | null; backendId: ID | null;
  latestConfirmedAt: ISOTime | null; attentionCodes: string[]; controls: ControlState;
}
interface ConfigSnapshot {
  id: ID; versions: Versions; capturedAt: ISOTime;
  group: GroupRef; fields: FieldMap<GroupRuleValues>;
  globalFields: FieldMap<GlobalRuleValues>;
  routing: RoutingView | null; permissions: PermissionView[];
  // 均为当时脱敏快照；不包含模型系统提示、密钥、全群正文
}
interface ServiceView {
  id: 'relay' | 'dispatcher' | 'decision' | 'bridge';
  process: 'running' | 'stopped' | 'unregistered' | 'unknown';
  instanceId: string | null; startedAt: ISOTime | null; lastExitCode: number | null;
  health: 'ready' | 'not_ready' | 'unreachable' | 'unsupported' | 'unknown';
  healthLatencyMs: number | null; healthObservedAt: ISOTime | null;
  lastProgressAt: ISOTime | null;
  blockers: { code: string; summary: string }[];
  facts: { key: string; value: boolean | number | string | null; reason: string | null }[];
  maintenance: MaintenanceView | null;
  actions: Record<'start' | 'stop' | 'restart', Availability>;
}
interface FieldChange {
  component: Component; path: string;
  before: unknown; after: unknown; effectiveBefore: unknown; effectiveAfter: unknown;
}
interface ChangePlan {
  id: ID; token: string; expiresAt: ISOTime; base: Versions;
  changes: FieldChange[]; affectedGroups: ID[];
  effect: ('hot_reload' | 'new_execution' | 'restart_required')[];
  warnings: { code: string; message: string }[]; componentOrder: Component[];
}
type ComponentApplyState = 'not_started' | 'applying' | 'applied'
  | 'verified' | 'applied_persistence_unverified' | 'failed_not_applied' | 'unknown';
interface Operation {
  id: ID; requestId: ID; revision: string;
  changeId: ID | null; maintenanceId: ID | null; runId: ID | null;
  status: 'planned' | 'pending' | 'draining' | 'blocked' | 'executing'
    | 'verifying' | 'succeeded' | 'partial_apply' | 'canceled' | 'failed' | 'unknown';
  summary: string; resultRevision: string | null;
  components: { id: string; state: ComponentApplyState;
    readbackVersion: string | null; persistenceVerified: boolean | null }[];
  cancelResult: { mode: CancelMode; deliveryBlockPersisted: boolean;
    remoteExecution: RemoteExecution } | null;
}
interface ChangeRecord {
  id: ID; createdAt: ISOTime; origin: 'console' | 'external' | 'unknown';
  base: Versions; target: Versions | null; readback: Versions | null;
  changes: FieldChange[]; operation: Operation;
}
interface RunFilter {
  from: ISOTime; to: ISOTime; groupId?: ID; phases?: Phase[];
  outcomes?: Outcome[]; deliveries?: Delivery[]; backendId?: ID;
  fallback?: boolean; version?: string; completeness?: Completeness;
  query?: string; cursor?: string;
}
type FieldEdit<T, S> = { [K in keyof T]:
  { kind: 'field'; scope: S; field: K; value: T[K] } }[keyof T];
type GlobalEditableValues = Omit<GlobalRuleValues, 'laya_url'>;
type GroupEditableValues = Omit<GroupRuleValues,
  'support_mode' | 'support_since_ms' | 'hermes_profile'>;
type Edit = FieldEdit<GroupEditableValues, { kind: 'group'; groupId: ID }>
  | FieldEdit<GlobalEditableValues, { kind: 'global' }>
  | { kind: 'group_admission'; groupId: ID; enabled: boolean }
  | { kind: 'create_group'; groupId: ID; targetGroupId: ID; targetLabel: string }
  | { kind: 'delete_group'; groupId: ID }
  | { kind: 'routing'; groupId: ID; primary: RouteAttemptPolicy;
      fallbacks: RouteAttemptPolicy[] }
  | { kind: 'permission'; groupId: ID; capability: string; requested: boolean };
// capability 只能来自 getGroup.permissions 且 editable=true；不能新造任意权限名。
// TS 只约束形状；具体群和 schema 的只读/区间约束仍由 Go 二次验证。
type ActionRequest =
  | { kind: 'pause' | 'resume'; scope: ControlState['scope'] }
  | { kind: 'start' | 'stop' | 'restart'; serviceId: ServiceView['id'] }
  | { kind: 'cancel_run'; runId: ID; cancelMode: CancelMode };
interface ActionPlan {
  id: ID; token: string; expiresAt: ISOTime; expectedRevision: string;
  action: ActionRequest; warnings: string[]; eligible: Availability;
  impact: StageCounts; boundary: Watermark | null;
  expectedInstanceId: string | null;
}
interface EventEnvelope {
  streamId: ID; seq: number; cursor: string; occurredAt: ISOTime;
  entity: 'run' | 'group' | 'service' | 'control' | 'change' | 'operation';
  entityId: ID; revision: string; kind: string;
}
interface Bootstrap {
  streamId: ID; cursor: string; lastSeq: number;
  overview: Envelope<Overview>; activeOperations: Operation[];
  capabilities: Record<string, Availability>; schemas: FormSchemas;
}
interface ConsoleClient {
  bootstrap(): Promise<Result<Bootstrap>>;
  getOverview(): Promise<Result<Envelope<Overview>>>;
  listRuns(filter: RunFilter): Promise<Result<Envelope<Page<RunView>>>>;
  getRun(id: ID): Promise<Result<Envelope<{ run: RunView; timeline: TimelineItem[] }>>>;
  getConfigSnapshot(id: ID): Promise<Result<Envelope<ConfigSnapshot | null>>>;
  listGroups(filter: { query?: string; enabled?: boolean; backendId?: ID;
    attentionOnly?: boolean; cursor?: string }): Promise<Result<Envelope<Page<GroupView>>>>;
  getGroup(id: ID): Promise<Result<Envelope<GroupView>>>;
  getGlobalRules(): Promise<Result<Envelope<GlobalRulesView>>>;
  getBackends(groupId: ID): Promise<Result<Envelope<BackendOption[]>>>;
  planChange(input: { requestId: ID; base: Versions; schemaVersion: string;
    edits: Edit[] }): Promise<Result<ChangePlan>>;
  publishChange(input: { requestId: ID; planId: ID; token: string }): Promise<Result<Operation>>;
  listChanges(filter: { from?: ISOTime; to?: ISOTime; groupId?: ID;
    component?: Component; status?: Operation['status']; cursor?: string }): Promise<Result<Envelope<Page<ChangeRecord>>>>;
  getChange(id: ID): Promise<Result<Envelope<ChangeRecord>>>;
  planRollback(input: { requestId: ID; changeId: ID; base: Versions }): Promise<Result<ChangePlan>>;
  listServices(): Promise<Result<Envelope<ServiceView[]>>>;
  planAction(input: ActionContext & { action: ActionRequest }): Promise<Result<ActionPlan>>;
  executeAction(input: { requestId: ID; planId: ID; token: string }): Promise<Result<Operation>>;
  continueMaintenance(input: ActionContext & { maintenanceId: ID }): Promise<Result<Operation>>;
  cancelMaintenance(input: ActionContext & { maintenanceId: ID }): Promise<Result<Operation>>;
  getOperation(id: ID): Promise<Result<Envelope<Operation>>>;
  getOperationByRequest(requestId: ID): Promise<Result<Envelope<Operation | null>>>;
  acknowledgeAttention(input: ActionContext & { attentionId: ID }): Promise<Result<Envelope<AttentionItem>>>;
  subscribe(input: { cursor: string; lastSeq: number;
    onEvent: (event: EventEnvelope) => void;
    onResetRequired: (reason: string) => void;
    onConnection: (state: Freshness) => void }): () => void;
}
```

目标 schema 对群 `allowed_types` 与 `self_sender_ids` 保留 null/[] 差别：field edit 的 value=null 表示对应 nil 语义；不适用 nil 的字段收到 null 必须拒绝。schema 描述零值继承与默认时，仍遵守第 5 章，不用通用 nullable 推断。字段注册表由 bootstrap.schemas 返回，包含所有可见项及只读原因，设计 AI 不自行补字段。群表单加载时完整保留初始 GroupView/GlobalRulesView 及 Versions 作为基准；遇到冲突回读同结构最新 DTO，以基准/草稿/最新三方比较，未渲染字段不丢失，最终重新 plan。

routing 的 primary/fallbacks 每项统一读写 backendId、modelId、timeoutSec、session；只有 catalog 提供的模式和范围能编辑。null routing 表示尚未接入并说明原因，不用硬编码默认后端代替。permission edit 使用目录里的 capability、requested；生效值、强制级别及证据不可由前端提交。请求 model/timeout 修改不代表当前硬编码分支已可写，须 capability+运行读取者都实现。

schemaVersion 不匹配必须拒绝发布并重新 bootstrap。所有计划 token 包含实际版本、作用域、服务实例和影响计数的校验信息；预览 boundary 只是候选，最终以操作成功后的持久控制边界为准。

历史配置通过 run.configSnapshotId → getConfigSnapshot 读取，只展示当时脱敏快照；缺失/清理时返回 null 并明确“历史快照未保留”，禁止拿当前设置代替。异步 operation 按 ID 或 requestId 查询；bootstrap.activeOperations 与各 MaintenanceView.operationId 负责 UI 重开恢复。一次操作的 succeeded 仅表示该 action 定义的目标完成；block_delivery 成功只保证 deliveryBlockPersisted，不能写成远端停止。stop_execution 仅 remoteExecution=stopped 才能显示执行已停止；否则继续 pending/unknown 或明确返回阻止投递已完成、执行未确认。

组件读回目标值一致但状态文件持久化未核验时，用 applied_persistence_unverified；operation 不能整体 succeeded。通过明确持久化证据后才转 verified。历史“未在活动操作列表”也不能当成功，必须读取已有 operation 事实。

### 11.2 最少代表性 JSON（全部虚构）

以下是独立契约片段，ID、群、时间、版本和内容全部虚构；不是采集样本。完整 mock 必须补齐对应 DTO 必填字段。

```json
{
  "exampleOnly": true,
  "runState": {
    "id": "demo-run-001",
    "phase": "confirming",
    "outcome": "pending",
    "delivery": "submitted",
    "deliveryEvidence": {
      "submittedAt": "2026-11-19T10:00:02+08:00",
      "confirmedAt": null,
      "bridgeRequestId": "demo-bridge-001",
      "echoMessageId": null
    }
  },
  "inheritedField": {
    "configured": 0, "effective": 8, "origin": "global",
    "editable": true, "reason": null
  },
  "emptyOverride": {
    "configured": [], "effective": [], "origin": "group",
    "editable": true, "reason": "显式空列表，与继承不同"
  },
  "permission": {
    "capability": "host_tools", "requested": false, "effective": null,
    "enforcement": "soft", "provenance": "adapter_prompt",
    "policyVersion": null, "enforcedAt": [], "editable": false,
    "reason": "提示词约定不能证明宿主工具被禁止"
  }
}
```

```json
{
  "exampleOnly": true,
  "conflict": {
    "ok": false,
    "problem": {
      "code": "conflict", "message": "运行配置已变化，请重新比较并预览",
      "requestId": "demo-request-002", "retry": "replan"
    }
  },
  "partialPublish": {
    "id": "demo-op-003", "requestId": "demo-request-003",
    "revision": "demo-op-r2", "changeId": "demo-change-003",
    "maintenanceId": null, "runId": null, "cancelResult": null,
    "status": "partial_apply", "summary": "Dispatcher 已读回，routing 尚未核验",
    "resultRevision": null,
    "components": [
      { "id": "dispatcher", "state": "verified", "readbackVersion": "demo-d-v2", "persistenceVerified": true },
      { "id": "routing", "state": "unknown", "readbackVersion": null, "persistenceVerified": null }
    ]
  },
  "event": {
    "streamId": "demo-stream-a", "seq": 102, "cursor": "demo-cursor-102",
    "occurredAt": "2026-11-19T10:00:03+08:00",
    "entity": "run", "entityId": "demo-run-001",
    "revision": "demo-run-r4", "kind": "delivery_changed"
  }
}
```

### 11.3 事件恢复与 mock 验收

【目标】事件流采用 streamId + 单调 seq + opaque cursor。bootstrap 必须原子关联快照、lastSeq 和可续传 cursor，避免读快照与订阅之间丢事件；订阅首条应是 lastSeq+1，订阅接口须保留该快照之后的事件。重连按 cursor/lastSeq 请求补流；重复 seq 去重，乱序短暂缓存后按顺序处理，无法连续则 reset。cursor 过期、日志截断、streamId 变更或跨重启不能证明连续时触发 onResetRequired，全量 bootstrap 并保留当前筛选/草稿；草稿标需重新对比。事件是实体失效通知。revision 是 opaque 字符串，只能做相等比较；先后关系用 streamId+seq/entitySeq 判断。收到事件后给该实体及依赖列表记录最低失效 seq，读取响应的 entitySeq 低于它则丢弃并重读；同实体请求还需本地 request generation 防旧请求覆盖新结果。streamId 改变则重置这些序号并 bootstrap，不能跨 stream 比 seq。

事件日志与业务 run 持久事实分开；事件可重建显示、不能替代已提交去重记录。当前 `/api/events` 没有这套 cursor/seq，不可包装一个本地序号就声称可靠恢复。目标事件保留窗口可先 24 小时，过期走快照恢复；服务端时间和控制边界均以事实源为准。

前端需提供可切换 mock fixtures：首次 loading、全空、筛选空、服务部分失联、陈旧快照、legacy、执行降级远端未决、静默、权限拦截、演练、配置过期输出、submitted 待确认、unknown 后迟到确认、版本 409、500 后读回已改、跨组件 partial_apply、nil/[] 区别、暂停/drain 冲突、取消与提交竞态、订阅缺口、UI 重开恢复。mock 必须显著标识，不能连接真实服务或自带“演示发送”副作用；禁用能力与真实模式共享同一 Availability 分支。

## 12. 现有 API、适配映射与源码附录

### 12.1 Dispatcher 准确接口

【当前】只有三个 `/api` 路径，不能假设 `/api/runs`、`/pause`、`/health`、`/rollback` 或权限接口已存在。

| 接口 | 精确契约及控制台映射 |
| --- | --- |
| `GET /api/config` | 完整内存配置 JSON + ETag；Go 持有原始值，映射为脱敏表单和版本，不原样发前端 |
| `POST /api/config` | 扁平 patch keys；Rules 子字段放顶层，不包 `rules`；`dry_run` 映射 `bridge.dry_run`；map/array 按字段整体替换 |
| `PUT /api/config` | 完整配置替换；未知字段严格拒绝；首版 UI 不开放原始 JSON 替换 |
| `GET /api/stats` | 第 3 章十项字段；累计、可能重叠、pending 包含处理中；无可靠单群/当日口径 |
| `GET /api/events` | 日志 SSE `{time,level,message}`，keepalive 15 秒，最近 100 条且拥塞可丢；只作脱敏 legacy 展示 |

写请求必须 `Content-Type: application/json`，请求体上限 256 KiB，`If-Match` 原样传 GET 的 ETag。缺失或冲突返回 409；400 校验错误；415 类型错误；413 过大；成功 `{ok:true}` 并返回新 ETag。对 400/415/413 不盲重试，保留草稿并给出相应提示。`server`、`relay`、`hermes` 变化 API 拒绝 require restart；rules/bridge 可热加载。成功响应不是跨组件原子承诺。

Go 适配规则：修改某群须读取完整 `group_rules`，在 Go 内合并该群的完整规则对象，再把保留其他群的整张 map 放在 POST 顶层 `group_rules` 字段。严禁前端只发单个 map 项导致整图被替换。实际请求由 Go schema 生成，表单不得拼接底层 JSON。群白名单和列表也使用相同的“读全量、合并、带版本替换”规则。

现役 GET 返回规范化内存配置不保证还原原文件字段存在性；`omitempty` 可能使显式空列表在序列化后消失。Facade 若无法获得 nil/[] 保真信息，须把该字段标不可安全编辑并要求先修复 DTO/读取契约，不能猜。首次改造需为配置读写往返补足明确存在性标记或无损内部读取。

### 12.2 其他现役边界

Decision 和 Bridge health 如第 6 章；Bridge `POST /send`、`POST /` 是发送入口，`/action/park`、`unpark` 是 GUI 行为，不准用于监看探针或暂停模拟。dry-run 发送调用也不作为监看探针。健康聚合不调用 Relay groups/history/stats，只读已获准内存数据，缺能力就显示 unsupported。

兼容外部 adapter 的分支映射不能证明真实 Agent 指定；其宿主行为应按实际执行器核验，整链只能声明已验证的权限。实际只有文本投递，不显示可用图像、语音或附件按钮。动态 Agent/skills/tools/知识库注册与挂载仍为 unsupported；trace、取消、权限和版本回滚以公开对接说明和运行能力为准。

### 12.3 源码定位与核对方法

以下路径均相对 `<PROJECT_ROOT>`，只说明公开 core 的契约与实现定位；私有来源的库结构和部署路径不属于公开页面规格。源码行为不等于运行二进制包含相同修订，部署版本仍需核验。

| 公开位置 | 核对重点 |
| --- | --- |
| `internal/dispatcher/` | 群规则、准入与聚合、后端调用、发送边界、回声确认、unknown 与重启恢复 |
| `internal/relay/` | 通用 Webhook/SSE、公开规则、过滤与内存健康 |
| `internal/contracts/`、`internal/ports/` | 输入/处理/动作协议、身份校验、适配器生命周期 |
| `internal/console/`、`internal/gateway/` | 脱敏 DTO、能力发现、版本发布、任务投影与维护控制 |
| `internal/execution/`、`internal/runtimebridge/` | 后端 catalog、模型与权限准入、attempt 和远端执行证据 |
| `internal/supervisor/` | 固定服务白名单、计划与排空验证 |

配置、状态、锁与审计目录由操作者在本机显式指定，不根据源码位置猜测生产路径。使用独立 demo/test 配置和合成消息核验；不共享生产状态或消息流。core 构建方法见根 README；私有模块在独立根目录构建 `./cmd/relay`，本机通过私有 README 中的显式配置入口启动；群处理入口用 `-local-policy` 保留本机原身份与业务策略。

## 13. 两版范围、执行顺序与验收交接

### 13.1 V1：可验证的统一入口

V1 最终范围包括五页、可信健康/累计统计、结构化新 run、现有规则编辑、结构化后端选择、可强制的权限编辑、安全维护控制和版本读回。V1 可分增量交付；每个增量独立标能力，未完不得用 mock 填充真实入口。暂停/维护为产品完整目标，只有后端边界实现后才能开放按钮。

| 顺序 | 负责人 | 交付与依赖/验收门槛 |
| --- | --- | --- |
| 1 契约与设计 | 设计开发 + 代码审查 | 五页设计、全部状态、ConsoleClient mock；前端仅依赖契约，无真实跨端口调用 |
| 2 只读整合 | 业务开发 | MyGo Facade、同源边界、能力发现、现役统计脱敏；不探测私有来源；进程/健康/时效分开 |
| 3 run 事实 | 业务开发 | Dispatcher 阶段埋点、持久 run/attempt/交付、cursor 恢复和跨重启 unknown；设计评审可按真实状态联调 |
| 4 配置发布 | 业务开发 + 审查 | 无损字段读取、ETag/字段 diff、回读核验、变更持久化；测试 409、500 已改、map 保留、nil/[] |
| 5 routing/权限 | 业务开发 | 结构化配置与实际运行读取者、catalog、后备准入、发送门禁；普通 soft 权限只读解释 |
| 6 控制/维护 | 业务开发 + 运行维护 | 持久暂停边界、发送并发闸门、排空及服务白名单动作；unknown 不重发、不强停 |
| 7 联调验收 | 前端 + 代码审查 | 完整 mock 与真实能力逐项比对，验证 UI 关闭重开/服务重启/事件断线，移除一切假可用入口 |

真正可强制的单 run 后端取消并非 V1 全分支承诺；V1 可交付“阻止未提交投递”并展示远端未决，只有 backend capability 支持时才提供执行取消。版本历史与“回滚为新发布”属于 V1 配置闭环；若基础历史未完成则增量阶段可只读，但不得称 V1 完整验收。跨组件一致发布的原子化不属于 V1。

### 13.2 V2：更深的执行治理

V2 扩展真实 Agent ID 绑定、宿主与细粒度工具隔离、可靠远端取消、丰富工具结构事件、规则模板、关联配置协调发布与更完善历史检索。图像/语音投递不因 V2 名称自动纳入，需单独需求与链路验证。V2 不改变已提交只确认、unknown 不重发、本机来源校验等基本契约。

### 13.3 设计与前端交接验收

- 五页逐区具备默认、loading、empty、stale、partial、unknown、error、conflict、unsupported 和草稿状态；不写死视觉颜色像素。
- 字段可看到作用域、有效值、继承来源、合法范围及副作用；允许类型和自身 ID 的 nil/[] 往返不丢。
- 总览不显示伪单群/当日成功率；pending 不称纯等待队列；生成完成不称已回复。
- 群路由三条件分别可见；目标名可校验但不发测试；专用 support 不包装通用开关；bot_names 仅全局。
- 权限四级证据可见；soft 不能表现为硬禁止；后端计划与实际尝试可区分，agent 映射不冒充绑定。
- 前端只调用 ConsoleClient；mock 与真实入口清晰；无 shell、任意路径、原始 JSON 或 secret 输入/输出。
- 表单冲突保留草稿，返回页面保留筛选；新事件不覆盖编辑；重复点击受前后端共同约束。
- 暂停/drain/取消/停止含义不同；提交/确认/unknown 明确；不可用动作有理由且不可执行。

### 13.4 业务与代码审查验收

- 配置写遵守 Content-Type、256 KiB、原样 ETag、全量 map/array 替换及未知字段拒绝；500/断线后回读，不自动补偿。
- 配置更新使旧 hash 回复失效有可追踪原因，不误报后端已取消；跨组件 partial_apply 可恢复核验。
- pause 与提交竞争、cancel 与提交竞争由后端原子边界裁决；提交已发生只确认，无任何路径自动重发 unknown。
- drain 不等价 pending=0，含排队/执行/待确认及远端未决风险；pause 阻断时不强制停服务。
- 每次降级重新验证权限与会话 fingerprint；超时保留远端可能运行事实，不扩大能力。
- 服务 label 白名单、动作计划/实例版本二次校验；KeepAlive 停止语义实际验证；关闭 UI 不停止服务。
- 事件重复/缺口/过期/跨重启通过 snapshot 恢复；日志不能伪装 trace，统计不能伪造历史 run。
- 不 kill/restart 微信、不访问私有来源、不发测试消息，不用 Bridge send/park 或 Relay groups/history/stats 接口作探针。

### 13.5 联调异常矩阵与可执行验收样例

以下场景先由 mock 验证交互，再在隔离测试环境通过模拟适配器验证业务；不在真实微信群发送测试消息。每条验收同时检查界面文案、返回 DTO 和持久事实，界面画面本身不证明后端正确。

| 输入场景 | 预期事实与界面结果 | 禁止行为 |
| --- | --- | --- |
| 另一窗口先修改同一配置 | 本次发布冲突，草稿保留，重新比较三方字段 | 自动覆盖、去掉版本条件 |
| 配置写文件和内存成功，state 保存失败 | 返回错误后回读确认目标配置，状态显示已应用但持久化待核验 | 显示完全失败后自动再写 |
| 只修改甲群规则，乙群已有旧快照输出 | 若完整 config hash 变化，乙群输出也可能失效，记录 config_changed | 宣称只影响甲群或把丢弃记为发送失败 |
| Bridge 提交响应丢失 | delivery unknown，继续被动确认；迟到回声可确认 | 再次发送同一输出 |
| 后端生成超时，远端未确认结束 | attempt timed_out、remoteExecution may_be_running；按权限决定是否降级 | 展示执行已停止或放宽降级权限 |
| 暂停和提交同时发生 | 由持久提交边界决定：边界前已提交继续确认，否则阻止 | 仅凭按钮点击顺序判定 |
| 维护期间又暂停同一作用域 | 维护 blocked_by_pause，提供等待、取消维护或明确解除暂停 | 自动解除暂停以完成维护 |
| UI 断开后服务持续运行 | 重开读取持久快照，不重发控制请求或重置开关 | 用本地默认状态恢复服务 |
| 事件游标过期且存在未保存草稿 | 重新 bootstrap，保留草稿并要求重新比较版本 | 静默丢草稿或沿旧快照发布 |
| 显式清空自身身份列表再读回 | 必须保留空覆盖语义；无法保真则禁止发布并报告契约缺口 | 把空数组变成继承全局 |

审查记录应逐条注明“通过/失败/尚未接入”和证据来源。尚未接入不能按通过计分；前端交互通过也不代表控制语义通过。只有对应能力实际读写者、并发边界、持久恢复和错误核验均完成，才能把 Availability.supported 设为 true。

### 13.6 待用户审校的关键决策

本稿已给出可设计默认，无需等待答复才能阅读或产出前端。用户重点审校：第一，接受本机单用户、MyGo 统一入口与服务独立 launchd；第二，暂停恢复从新水位开始、不回放暂停期/暂停前未提交输出；第三，维护允许旧工作完成交付但暂停优先、阻碍时只等待或取消维护；第四，V1 需要真实 run 与配置/routing/权限后端改造，不能只合并现有网页即称完成；第五，soft/host 未证实的权限只读，后备不满足约束则阻断；第六，历史默认 100 次变更/30 天元数据、7 天 run 元数据及 24 小时事件补流窗口；第七，不开放强制重启、unknown 重发、任意脚本/JSON 编辑。

上述审校可以修改产品默认，但不能将未实现能力改成“现有”，也不能把没有证据的交付写成成功。文档、设计、前端和后端始终以同一能力发现与版本读回契约对齐。


### 13.7 可直接交给设计与前端 AI 的工作指令

> 请以本文件 1.2 为业务与交互依据，先输出五个页面的设计文档，再实现可运行的前端。目标是嵌入 MyGo WebView 的 macOS 本机桌面控制台；前端框架与视觉风格自行选择，业务状态、字段名和规则以本文为准。
>
> 页面必须覆盖总览、运行记录、群配置、服务管理、配置变更。群配置要可设计规则、权限与后端的完整编辑体验；待接入能力保留清晰的不可用状态。对“提交成功/回声确认/结果未知”、权限强制级别、暂停/排空/取消、配置冲突和部分应用给出完整交互。
>
> 实现 ConsoleClient 抽象和独立 mock adapter，页面组件不得直接调用 Relay/Dispatcher/Bridge、读磁盘、执行命令或连接真实群聊。使用本文的注册字段、DTO 和模拟场景；不要自行发明生产 API、假装 Agent 绑定已完成或把提示词限制画成硬权限。真实 MyGo 生成客户端由对接开发编写适配器接入。
>
> 交付设计说明、源码、运行说明、mock 场景切换方式，以及所需但本文未定义的契约问题清单。提供基础响应布局与键盘操作；无需承担现役服务启动、部署或权限隔离实现。若设计确实需要新增业务字段，先在问题清单中说明用途，不在页面里静默硬编码。
>
> 项目维护者审查页面与业务契约一致性、代码结构和副作用边界，并负责业务后端与集成实现。
