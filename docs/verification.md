# 独立集成验收记录

核心验证环境为 Go 1.22.5、darwin/arm64。验证范围包含全工程竞态、静态检查、命令编译、TypeScript strict 及三层架构隔离链路。历史结果仅适用于对应版本，代码或依赖变更后应重新执行相关检查；不能据此推断现役服务或未来发行包已验证。

公开文档仅保留验证方法、覆盖范围和 synthetic demo evidence。真实运行记录、身份策略与原始资料应由操作者保存在公开仓库之外，不属于测试夹具或发布内容。

默认测试只启动自建 loopback 进程，使用合成来源、mock 后端与假出口；不读取真实微信库、不向真实群发送消息，也不切换现役服务。真实后端冒烟必须单独显式启用，不能与 mock 验证混为一谈。

## 验证边界

公开 core 包含群聊处理、发送编排、adapter 接口与通用 Webhook/SSE 入口。微信本地采集及其实现和部署细节不在公开模块；私有模块只在本机配合 core 运行。公开核心的离线验证使用合成来源，不依赖私有模块；私有组合验收另外覆盖本机来源实现。两类验证均不发送真实群消息，不操作生产服务或真实 launchd。

| 测试 | 覆盖 |
| --- | --- |
| `TestRelayDispatcherConsoleLifecycle` | 合成入站 → 实际 Relay Webhook/SSE → Dispatcher → fixture 执行器 → fake Bridge → 回声 → Console；覆盖 dry-run、NONE、断连恢复、重启去重和发布幂等。 |
| `TestPersonalHTTPRealModuleChain` | 正式 runtimebridge/Engine 配合 mock Alma HTTP；验证外部任务关联、执行版本和请求计数。 |
| `TestPersonalHTTPWaitingThenDone` | `running → waiting → done` 持续轮询；只有有效 summary 才能进入假出口。 |
| `TestUnreachablePersonalDoesNotFallbackOrEcho` | 无 fallback 的不可达后端明确失败，无成功 attempt、无结果回显、sink=0。 |
| `TestControlPauseResumeDrainThroughHTTP` | pause/resume、drain/cancel、持久操作读回与 requestId 幂等；恢复不回放旧输入。 |
| `TestControlCancelRunningBackendThroughHTTP` | block_delivery 与停止本地调用；无远端取消证据时保持 may_be_running，不宣称远端已停。 |
| `TestRelayDispatcherPublicationRollback` | 双组件发布、读回、冲突、partial 与回滚新发布；只使用合成目标。 |
| `TestRoutingFallbackAndPermissionBlock`、`TestRoutingRejectsPermissionWeakeningFallback` | 失败/成功 attempts、生成与发送权限门禁、禁止扩大权限的降级。 |
| `TestAnthropicProxyTextOnlyContract` | 纯模型代理不注册 tools，拒绝 tool_use 输出；provider/model 使用测试目录。 |
| `TestFakeLaunchdLifecycleWithRealDispatcherDrain` | fake launchctl 配合实际 Gateway/Supervisor/Dispatcher 排空，操作读回与重开幂等；不调用系统 launchctl。 |
| `TestAlmaTransportGuard*`、`TestLiveAlmaOriginRejectsBusinessEndpoints` | 源地址/方法/路径限域、自有任务取消及拒绝越界请求。 |
| `TestBilibiliConnectorChain` | 合成 B 站评论与楼中楼 → `bilibili-connector` (`ego` fake driver) → 实际 Relay → Dispatcher (`bilibili` 动作端点) → 幂等账本与 `confirmed`/`unknown` 分支（无真实外发）。 |
| `internal/connector/...`、`internal/connector/bilibili/...` | 游标持久化、防历史洪泛、自回复过滤、环回/`Origin`/`Sec-Fetch-Site` 门禁、`dry_run` 拒绝、持久幂等冲突 (`idempotency_conflict`)、`confirmed`/`failed`/`unknown` 语义及人机校验/人工接管 fail-closed。 |
| `TestActionEndpointsConfigValidationAndHotReloadGuard`、`TestActionTargetForBatchAndPlatformActionRequest`、`TestConfirmDirectLockedAndFailSubmissionLockedValidation`、`TestUnknownActionCannotSubmitAgain` | `contracts.ParseContainerID` 多平台验证、`action_endpoints` 热更新守卫、`ActionTargetForBatch` 与 `PlatformActionRequest` 校验、直接确认 vs `unknown` 禁止重发。 |
| `TestBuiltinScenarioPresetsAndGoalToneRendering`、`TestLocalPolicyScenariosModesPermissionsAndNoLeakage`、`TestKnowledgeBasesFailClosedAndSupportRestrictions`、`local_policy_test.go` | 7 套内建场景、`append`/`replace` 模式与安全规则不丢失、`0600`/符号链接拒绝、只读知识库字节预算与缺失 fail-closed、`code_fact_rules` 单捕获组提取及 `support_mode` 隔离限制。 |

三层增量及多平台扩展还覆盖：来源与目录投影裁剪、capability/availability 分离、Bridge 协议与授权完整性、输入/动作持久关联、回执准入和 echo_inferred、历史事件不含未来证据、旧 trace 缺失事实、opaque cursor、reset、断连与 TypeScript 客户端。

## core 可复现命令

从 `<PROJECT_ROOT>` 执行，使用 Go 1.22.5 或兼容的更新版本。macOS Go 1.22 使用 external linker；测试需要允许临时 loopback 监听。独立性验证应在没有私有兄弟目录的公开源码副本中，使用 `GOWORK=off` 检查全工程 race、vet、所有入口编译、三套 TypeScript strict 和静态边界。可复现命令如下：

```sh
JINX_LIVE_ALMA=0 GOPROXY=off GOSUMDB=off go test -race -ldflags=-linkmode=external ./internal/integration -count=1 -timeout=240s
JINX_LIVE_ALMA=0 GOPROXY=off GOSUMDB=off go test -race -ldflags=-linkmode=external ./... -count=1 -timeout=240s
go vet ./...
go build -ldflags=-linkmode=external ./cmd/...
# 或逐一显式列出全部 7 个入口：
# go build -ldflags=-linkmode=external ./cmd/relay ./cmd/dispatcher ./cmd/jinx-dispatcher ./cmd/bilibili-connector ./cmd/control-plane ./cmd/console-api ./cmd/backend-adapter
tsc --strict --noEmit --lib ES2020,DOM internal/console/client-contract.ts internal/console/architecture-client.ts internal/gateway/gateway-contract.ts
```

离线执行要求依赖已缓存在本机；缺依赖时不能把未执行记为通过。普通测试使用临时目录、随机 loopback 端口、假模型、fake Bridge 和 fake launchd。多 package build 仅检查编译，不在源码目录写入二进制。

来源扩展的组合验收由其维护者单独执行，不是公开核心测试的前置条件。建议使用合成来源、固定测试脚本或 mock 后端、假动作出口和关联回声，要求每次只执行一次、只提交一次，最终达到 `terminal/replied/confirmed`。操作者本地策略应先只读校验，再在独立配置与状态目录中验证；不得为测试调用真实知识源、提醒事项或业务出口。

采集源码、专属配置、旧资料与原始测试语料实际保存在公开目录之外。公开 `Config` JSON 不含本地策略字段；本地身份与规则通过 `-local-policy` 重新注入。测试副本可运行不等于现役服务已部署，生产切换仍需单独验证状态、锁与在途任务。

边界复核使用 `scripts/check-public-boundary.py` 和显式私有身份基线，仅输出分类、公开相对路径、行号和数量。来源不明的对话不得作为公开夹具，应改写为明确虚构场景。扫描器只识别有限秘密格式，基线提取和 synthetic 判断也有局限；零命中不等于完整安全证明，任何通用词重叠或告警都应结合上下文复核。自有代码与文档采用 Apache-2.0，第三方材料按各自许可保留。

## 真实 Alma 验证方法与证据边界

真实 Alma 冒烟默认关闭，只能在专用隔离环境中显式启用；本次未执行。显式提供 loopback 地址和操作者从模型目录选择的测试模型，公开文档不记录实际 provider alias 或模型路由。

验证使用合成入站、独立任务、正式 runtimebridge/Engine 与 fake Bridge，禁止真实平台出口。客户端仅访问模型目录、单次任务提交、自有 run 查询及必要的自有 run 停止；禁止全局停止和读取凭据配置。personal 请求不携带真实来源 thread、工作目录或业务上下文，fallback 为空。

成功条件同时包括：上游任务身份关联一致、执行结果与 Console attempt 外部 ID 对齐、独立查询 done.summary 精确匹配测试标记、恰好一次假出口提交、关联回声确认。超时或缺任一证据均不能称链路通过。真实验证原始 run/thread/trace/随机标记及模型路由只留私有记录。

以下是 **synthetic demo evidence**，仅说明记录格式，不对应真实会话，也不是本次执行结果：

```json
{
  "exampleOnly": true,
  "runId": "demo-run-001",
  "threadId": "demo-thread-001",
  "traceId": "demo-trace-001",
  "marker": "demo-marker-001",
  "backendId": "demo-personal",
  "modelId": "demo-provider:demo-model",
  "dispatchCount": 1,
  "fakeSinkCount": 1,
  "fallback": false,
  "delivery": "confirmed"
}
```

Alma personal 权限证据仍为 soft，此验证不证明请求级硬工具隔离或指定 Agent 绑定。运行时已支持 B 站动作连接器 (`cmd/bilibili-connector` + `internal/connector`) 及声明式 Scenario Playbooks / 只读 `knowledge_bases` 挂载；但控制台 `/console/v1/architecture` 的非微信连接器健康投影、`source`/`action` 独立事件域、规则编辑器场景字段绑定、Web 连接器、动态 Agent 注册与硬工具沙箱，以及桌面/Web 前端界面仍属下一阶段待实现范围。未支持项不得因配置、DTO 或示例存在而标为已实现。
