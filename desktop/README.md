# JINX Console Desktop UI (`jinx-desktop`)

JINX 统一控制面的原生桌面控制端，基于 **MyGo Native UI (`github.com/egoist/mygo v0.10.0`, `package ui`)** 构建（零 WebView / HTML / JavaScript 运行时依赖）。

## 许可与第三方材料

JINX 自有代码采用根目录 [Apache License 2.0](../LICENSE)。桌面端依赖、内联图标、实际随附资源及 Go 运行时保留各自的版权与许可，集中列于根目录 [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md)，完整文本与来源记录位于 [third_party/](../third_party/)。分发桌面端源码或二进制时，应一并保留适用的第三方许可材料。品牌标识不因代码许可而自动获得额外的第三方授权。

## 模块隔离与技术栈

- **独立 Go Module**：`jinx-desktop` 独立声明于 [go.mod](go.mod)（`go 1.27.3`），使桌面端能够使用 `mygo v0.10.0` 原生渲染管线，同时保持根目录公开核心模块（`go 1.22.5`）的构建与依赖完全不受影响。
- **高对比度紧凑遥测视觉系统**：位于 [theme/theme.go](theme/theme.go)，采用中性石墨灰阶表面配合 JINX 晴空钴蓝主色、发丝分割线与极简矢量图标集（[components/icons.go](components/icons.go)），支持默认浅色与实时深色切换；窗口在 macOS 下集成 `TitleBarHiddenInset` 无界标题栏与圆角应用 LOGO（[assets/assets.go](assets/assets.go)）。
- **严谨状态管理与异步线程安全**：[app/store.go](app/store.go) 与 [main.go](main.go) 在 `mygo.App.WhenReady` 附着窗口后设置 Dock/窗口图标，并通过 `store.Start(ctx)` 启动后台拉取与可恢复 SSE 订阅，经 `win.Update` 回到 UI 线程刷帧及调度 Toast；在无头 `ui.NewTester` 单元测试下切换为同步 `SyncOps` 以保证确定性与竞态安全。

---

## 快速启动

从 `desktop/` 目录运行：

### 1. Mock 沙盒模式（离线演练与 UI 验证，不连接任何实盘）

```sh
cd <PROJECT_ROOT>/desktop

# 完整多源场景（默认 Light 主题）
go run . -mock -scenario full

# 其他内置合成场景：
# - degraded         （维护排空 blocker、服务运行态与配置漂移）
# - conflict         （变更发布触发 409 并发冲突与重新拉取流程）
# - unknown-delivery （attempt=succeeded 但 delivery=unknown、echo_inferred 无伪造 ACK 时间、may_be_running）
go run . -mock -scenario unknown-delivery -dark
```

### 2. 连接本地 Control-Plane Loopback API

控制面 HTTP 客户端强制校验目标地址必须为 loopback（`127.0.0.1`、`::1` 或 `localhost`，且仅允许 `http://` 协议；拒绝非回环远端主机），保护控制面管理端口边界：

```sh
go run . -api http://127.0.0.1:8090
# 或启动深色主题：
go run . -api http://127.0.0.1:8090 -dark
```

### CLI 参数说明

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-api` | `http://127.0.0.1:8090` | 本地回环 `control-plane` / gateway HTTP 根地址 |
| `-mock` | `false` | 使用完全内存隔离的合成沙盒数据源运行 |
| `-scenario` | `full` | `-mock` 模式下的场景预设：`full`、`degraded`、`conflict`、`unknown-delivery` |
| `-dark` | `false` | 启动时默认使用高对比度深色主题 |

---

## 6 个全中文控制台视图与功能分布

1. **系统总览（[app/view_overview.go](app/view_overview.go)）**
   - 顶部 4 项核心 KPI 指标卡片（`当前活跃任务`、`投递状态未知`、`远端状态未定`、`在途与待处理消息`），严格区分实时快照与持久化累计口径。
   - **消息处理漏斗**原生柱状图（基于 `github.com/egoist/mygo/ui/chart` 绘制 `已接收 → 已触发 → 已提交 → 已回复 / 演练 / 失败`）与右侧`核心服务健康`列表、`待处理关注事项`确认队列及`最近运行记录`。
2. **来源与动作（[app/view_sources.go](app/view_sources.go)）**
   - 展示「第一层：信息来源矩阵」、「第二层：处理后端与模型目录」、「第三层：平台动作能力矩阵」及 SSE 域支持边界。
   - **平台连接器与确认式演练开关**：执行「预览变更计划 → 实盘风险警告与人工勾选确认 → 应用生效 → 回读校验（`ReadbackVerified`）」闭环，明确标示 `persisted: false`。
3. **容器与规则（[app/view_containers.go](app/view_containers.go)）**
   - 左侧容器列表展示三标志准入徽章（`路由` / `启用` / `白名单`）与`演练中`状态。
   - 右侧 5 个标签页：`基础与过滤`、`场景与挂载`（接入 `/console/v1/catalog` 逻辑目录，支持 `scenario`、`response_goal`、`tone`、`skills`、`tools`、`knowledge_bases`、`promo_keywords`）、`路由与权限`、`全局与中继规则`、`直通转发`（跨平台直通转发规则与跳过过滤/AI 的手动直发测试）。
   - 全部变更通过 Diff 预览模态框提交，遭遇 `409 版本冲突` 时引导重新拉取最新配置后再计划。
4. **运行与追踪（[app/view_runs.go](app/view_runs.go)）**
   - 全中文正交状态筛选（`运行阶段` / `执行结果` / `投递状态` / `容器 ID` / `运行或消息 ID`，不展示聊天原文）。
   - 支持手动加载到达的 SSE 新状态或响应 `reset_required` 重新同步全量快照。
   - 只读查看`历史配置快照`模态框（附带不变量范围说明）。
5. **服务与维护（[app/view_services.go](app/view_services.go)）**
   - `全局准入闸门`暂停 / 恢复接收（按 `controls` 与 capability 门控）。
   - `活动维护排空会话`横幅，逐项展示活跃协程 / 投递未知阻塞原因及`继续维护` / `取消维护`。
   - 服务`启动` / `停止` / `重启`先生成计划（展示可执行条件与阻塞项）再执行。
6. **变更与漂移（[app/view_changes.go](app/view_changes.go)）**
   - 对比 `Dispatcher` 与 `Relay` 内存/磁盘版本漂移（附带私有本地策略不纳入对比的声明）。
   - `部分生效` / `状态未知`明确警告不做静默自动补偿；`生成回滚计划`创建显式回滚计划供确认发布。

---

## 安全与真实性不变量

本桌面端严格贯彻 [../docs/target-page-spec.md](../docs/target-page-spec.md) 的数据真实性与操作安全规范：

- **`attempt succeeded != replied`**：当模型尝试已成功但投递未 `confirmed` 时明确提示，防止误把模型产出当作用户可见送达。
- **`echo_inferred` 不伪造时间**：仅在存在真实提交回执时间戳时展示 `SubmittedAt`；当来源为 `echo_inferred` 且 `submittedAt == null` 时明确显示提交回执时间未记录，绝不填补虚假时间。
- **`delivery: unknown` 禁止盲目重发**：面板呈现手工核对警告，且**不提供**自动或一键 Resend 按钮，避免重复发信。
- **遥测未停稳提示（`may_be_running` / `unknown`）**：明确提示停止控制面跟踪无法保证外部远端 Agent 进程已终止。
- **有损与无损三态区分**：过滤列表（如 `self_sender_ids`、`blocked_sender_ids`、`keyword_blocklist`、`regex_blocklist`、`allowed_types`）在序列化中严格保留 `null`（继承/默认）与 `[]`（显式清空）语义；挂载列表（`skills`、`tools`、`knowledge_bases`、`promo_keywords`）如实提示 dispatcher 回读时 `[]` 规范化为 `null`。
- **四级权限证据（4-Level Enforcement）**：路由权限视图以不同语义区分 `hard`、`host`、`soft (read-only)`、`unverified`，并禁止在前端修改软约束规则。
- **Capabilities 与 Controls 严格门控**：任何 `supported=false` 或 `enabled=false` 的动作保持禁用并保留原因提示；不把未支持的能力伪造为可用或 `0`。

---

## 测试与构建验证

从 `desktop/` 目录运行：

```sh
cd <PROJECT_ROOT>/desktop

# 全量客户端协议测试 + MyGo 无头 ui.NewTester 交互回归测试（含 Go race detector）
go test -race -v ./... -count=1 -timeout=180s

# 静态检查
go vet ./...

# 编译可执行验证（产物建议输出至临时目录，勿留在公开仓库树中）
go build -o /tmp/jinx-desktop-verify . && rm -f /tmp/jinx-desktop-verify
```

### UI 自动化驱动与画面巡检（`e2e` 构建标签）

桌面端通过 [automation_e2e.go](automation_e2e.go) 在 `//go:build e2e` 构建标签下按需接入 MyGo 官方 `github.com/egoist/mygo/plugins/automation` 插件；正式发布构建不包含该标签，因此不会对外暴露自动化套接字。

```sh
cd <PROJECT_ROOT>/desktop

# 1. 安装与当前 go.mod 版本（v0.10.0）匹配的 mygo-automation CLI
go install github.com/egoist/mygo/plugins/automation/cmd/mygo-automation

# 2. 编译带自动化能力的临时诊断版本，并以仅当前用户可访问的 0600 套接字启动
mkdir -p /tmp/jinx-e2e && chmod 700 /tmp/jinx-e2e
go build -tags e2e -o /tmp/jinx-e2e/jinx-desktop-e2e .
MYGO_AUTOMATION=/tmp/jinx-e2e/app.sock /tmp/jinx-e2e/jinx-desktop-e2e -mock -scenario full &

# 3. 使用 mygo-automation 查看界面树、操作控件或导出窗口图像
mygo-automation open --socket /tmp/jinx-e2e/app.sock
mygo-automation snapshot -i
mygo-automation screenshot /tmp/jinx-e2e/overview.png
mygo-automation close
```

从仓库根目录运行全仓开源边界检查，确保不包含任何私有标识、实盘路径或残余二进制产物：

```sh
cd <PROJECT_ROOT>
python3 scripts/check-public-boundary.py .
```
