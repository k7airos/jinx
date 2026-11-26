# JINX：三层架构的多来源消息编排与自动化控制平台（Open-Core）

JINX 是一个面向多平台消息接入、场景化智能决策与受控动作执行的 Go 后端控制系统。系统采用 **「信息来源 → 处理与编排 → 回复动作」** 三层解耦架构：公开核心 (`<PROJECT_ROOT>`) 提供完整协议契约、通用 Webhook/SSE 中继、多源聚合与场景调度、执行权限网关、动作连接器协议及统一控制面 API；本机私有采集器、真实账号标识、专属提示词与业务知识库则严格隔离在独立的私有目录 (`<PRIVATE_ROOT>`) 中，运行时通过扩展包或受控策略注入。

> 架构整理或文档更新不会自动替换现役服务、运行配置、状态锁或 `launchd` 任务。

---

## 核心特性与三层数据流

```text
+-----------------------------------------------------------------------------------+
| 1. 信息来源层 (Source Connectors)                                                   |
|   - 微信本地采集器 (私有模块通过 pkg/relay 扩展接入)                                |
|   - cmd/bilibili-connector (基于 ego-browser 与游标幂等的 B 站评论/楼中楼轮询)       |
|   - 通用 Webhook: POST /api/v1/messages (support: wechat / bilibili / web)         |
+-----------------------------------------+-----------------------------------------+
                                          | 标准化入站 / 过滤 / 序列号 SSE
                                          v
+-----------------------------------------------------------------------------------+
| 2. 处理与编排层 (Relay -> jinx-dispatcher -> execution.Engine)                     |
|   - 跨平台容器路由 (<digits>@chatroom | bilibili:<local> | web:<local>)             |
|   - 防抖聚合、内容/关键字/正则过滤、可选 Laya 前置门禁、历史关键词召回                 |
|   - 场景策略 (Scenario Playbooks) + 分层提示词 + 只读知识库/代码事实提取             |
|   - 结构化 Execution Engine: 模型目录路由、四级权限门禁与失败不扩权降级               |
+------------------------+-----------------------------------------+----------------+
                         | 微信路径                                 | Connector HTTP v1
                         v                                         v
+----------------------------------------+   +--------------------------------------+
| 3a. 微信 Bridge 动作出口                |   | 3b. 平台 Action Connector 动作出口   |
|   - /health 权限校验 (Acc / ScreenCap)  |   |   - GET /health (dry_run / ready)    |
|   - 持久提交边界 -> POST -> 自身回声确认 |   |   - POST /v1/actions + 持久幂等账本  |
+----------------------------------------+   +--------------------------------------+
                         \                                         /
                          +-------------------+-------------------+
                                              | WAL 持久 Trace / SSE / 控制 API
                                              v
                               +-----------------------------+
                               | cmd/control-plane           |
                               | /console/v1/* & 服务白名单   |
                               +-----------------------------+
```

---

## 开源核心 vs. 私有运营层边界

为了兼顾框架复用性与私域资产安全，仓库在代码与配置层面执行严格的 **Open-Core 物理拆分**：

| 维度 | 开源核心 (`<PROJECT_ROOT>`) | 私有运营层 (`<PRIVATE_ROOT>`, 运行时外置注入) |
| --- | --- | --- |
| **协议与端口契约** | `InboundEnvelope`、`ProcessingRequest`、`ActionRequest`、`ActionReceipt`、`internal/ports` | 无私有协议修改；直接实现或对接公开接口 |
| **来源采集实现** | 通用 Relay Webhook (`POST /api/v1/messages`)、SSE、过滤链及 `cmd/bilibili-connector` 示例 | 本地桌面/数据库私有采集器（如 `sqlcipher` 轮询、`pkg/relay` 进程内组合） |
| **容器与身份映射** | 平台感知正则校验、`container:<id>` 隔离默认规则与合成示例 `configs/*.example.json` | 真实群/频道 ID、机器人/自身账号 ID、专属 `hermes_profile`、记忆与向量库映射 |
| **提示词与业务知识** | 7 套通用内建场景模板、`append`/`replace` 组装引擎、只读 `knowledge_bases` 与 `code_fact_rules` 框架 | 品牌沟通规范、私有场景细则、产品知识文档、特定发行说明与运营规则（`-local-policy` 注入） |
| **边界门禁工具** | `scripts/check-public-boundary.py` 静态与私有基线双扫描 | 真实线上状态、会话证据、发信配置与服务部署脚本 |

> 公开目录禁止通过 `.gitignore` 掩盖私有文件；私有源码、真实标识及原始对话证据必须保存在独立目录之外。

---

## 运行入口 (`cmd/*`)

所有可执行入口均位于 `jinx` Go Module 下，职责清晰且保留独立进程边界：

| 入口 | 职责说明 |
| --- | --- |
| `cmd/relay` | 通用多平台消息接入网关：提供 Webhook 入站、去重过滤、SSE 消息流与 ETag/If-Match 配置 CAS；私有模块可通过 `pkg/relay` 组合自定义来源 |
| `cmd/jinx-dispatcher` | **主调度入口**：注入结构化 `execution.Engine`，负责防抖聚合、门禁过滤、Scenario Playbooks 提示词组装、模型与权限校验、多平台动作投递及 WAL 持久 trace |
| `cmd/dispatcher` | 兼容传统外部 `stream-json` adapter 协议的调度入口；与 `cmd/jinx-dispatcher` 为替代关系，共享配置目录 `.dispatcher.lock`，**严禁对同一消息流双开** |
| `cmd/bilibili-connector` | 首个非微信端到端参考连接器：通过 `ego-browser` 轮询 B 站视频评论及楼中楼、维护断点游标并暴露 Action Connector HTTP v1 动作服务（默认 `dry_run: true`） |
| `cmd/control-plane` | 完整统一控制面（`/console/v1/*`）：提供运行追踪、规则与执行配置预览/发布/回滚、全局/容器暂停与维护排空、白名单 `launchd` 管理及 `/architecture` 只读投影 |
| `cmd/console-api` | 轻量级控制台 Facade 兼容入口（不注入 `Engine` 与 `supervisor` 服务控制；`/backends` 与 `backend_catalog` 显式标记为 `unsupported`） |
| `cmd/backend-adapter` | 兼容旧外部 `stream-json` 协议的独立执行器适配器入口 |

---

## 场景策略（Scenario Playbooks）与受控挂载

`internal/dispatcher/scenario.go` 支持为每个容器路由声明意图与受控能力边界，而不暴露底层私有路径：

- **7 个开源内建场景标识**：`chat`（日常社群）、`tech_community`（技术交流）、`product_support`（客服反馈）、`soft_promo`（克制推荐）、`public_comment`（公开评论区）、`feedback_intake`（需求/缺陷收集）、`test_echo`（测试验证）。
- **路由级声明字段**（公开 `group_rules` 中仅存逻辑 ID）：`scenario`、`response_goal`、`tone`、`skills`、`tools`、`knowledge_bases`。
- **私有策略模式（`append` / `replace`）**：通过 `-local-policy`（或 `JINX_LOCAL_POLICY`）加载严格的 `0600` 本地 JSON 策略（拒绝符号链接与未知字段）。`prompt_mode`、`communication_mode`、`support.instructions_mode` 设为 `replace` 时替换默认人设话术，但**始终保留核心安全防注入、明确受话人与权限边界规则**。
- **只读知识库与源码事实提取**：`knowledge_bases` 绑定受字节预算保护（总量上限 48,000 bytes）的绝对只读文件；`support.code_facts_file` + `code_fact_rules` 仅允许按显式单捕获组正则提取指标数值，绝不向模型暴露原始代码上下文；文件缺失时 fail-closed 并明确告知知识缺口。
- **`support_mode` 隔离限制**：开启后禁用自主切换会话复用与普通技能挂载，工具集合收窄为受控白名单，并与全局/群其它路由保持严格隔离。

---

## 安全契约与能力边界

1. **四级权限与诚实降级**：执行引擎将能力证据严格区分为 `hard`、`soft`、`host`、`unverified`。受控纯模型路径（`text_only`，`noTools: true, allowSoft: false`）禁止注册工具并拒绝工具调用输出；`alma_personal` 明确标记为 `soft`；禁止扩大权限的模型降级，远端状态未确认时禁止自动切换模型重试。
2. **提交与确认分离（绝不盲目重试）**：
   - 微信 `Bridge ACK` 仅证明已跨过提交边界 (`submitted`)，最终确认依赖匹配关联的自身回声 (`confirmed`)。
   - 非微信 Action Connector HTTP v1 区分 `confirmed`（含合法 `external_id`）、`submitted`（待回声）、`failed`（显式 `submitted: false` + 机器码）与 `unknown`；任何网络中断、超时、缺失 ID 或 `unknown` **绝不自动重发**，防止重复发信。
3. **默认演练、可插拔驱动与人机校验保护**：`cmd/bilibili-connector` 作为面向创作者自托管自有视频的参考连接器（可通过 `internal/connector/bilibili/driver.go` 的 `Driver` 接口替换默认 `ego-browser` 驱动），与所有示例配置一样默认开启 `"dry_run": true`；当外部页面出现未登录 (`not_logged_in`)、人机验证 (`captcha_required`)、触发限频 (`rate_limited`) 或用户正在人工接管 (`user_control_active`) 时连接器立即失败并拒绝继续操作，绝不包含任何反风控、绕过验证码或多账号刷屏逻辑。
4. **控制面如实表达未支持项**：前端或外部集成必须先读 `/console/v1/bootstrap` 与 `Availability.supported`，禁止将未接入的来源、动作、Agent 目录或事件域展示为可用或伪造为 `false`/`0`。

---

## 快速开始（隔离示例环境）

> 环境要求：Go 1.22.5 或兼容更新版本（macOS 推荐附加 `-ldflags=-linkmode=external`）。测试与试用务必使用独立临时运行目录及示例配置，勿直接挂载现役生产状态目录。

### 0. 准备隔离运行目录

示例配置复制到临时目录后使用，避免运行状态写回仓库。本地策略文件必须是 `0600` 权限的普通文件，而 git clone 下来的文件通常是 `0644`，所以复制后要 `chmod 600`：

```sh
cd <PROJECT_ROOT>
export JINX_RUN=$(mktemp -d)
cp configs/*.example.json "$JINX_RUN"/
cp internal/dispatcher/local-policy.example.json "$JINX_RUN"/local-policy.json
chmod 600 "$JINX_RUN"/*.json
```

### 1. 编译检查与配置校验

```sh
go build -ldflags=-linkmode=external ./cmd/...

# 只读校验 Dispatcher 配置与私有本地策略（不创建锁、日志、状态文件，不连接后端）
go run -ldflags=-linkmode=external ./cmd/jinx-dispatcher \
  -check-config \
  -config "$JINX_RUN"/dispatcher.example.json \
  -local-policy "$JINX_RUN"/local-policy.json
```

### 2. 单次诊断轮询 B 站评论（`dry_run: true`）

先启动通用 Relay（默认监听 `127.0.0.1:18080`）：

```sh
go run -ldflags=-linkmode=external ./cmd/relay -config "$JINX_RUN"/relay.example.json
```

然后按 `cmd/bilibili-connector/config.example.json` 配置目标视频（默认 `dry_run: true`，需本机已配置 `ego-browser` 并登录有效会话），运行单次只读采集验证：

```sh
cp cmd/bilibili-connector/config.example.json "$JINX_RUN"/bilibili.json   # 修改 targets 为自有视频
go run -ldflags=-linkmode=external ./cmd/bilibili-connector \
  -config "$JINX_RUN"/bilibili.json \
  -once
```

### 3. 启动完整新架构调度链与控制面

`-execution-config` 指向的是 Execution Engine 的**持久 store 文件**（`{revision, config, receipts}`），不是 `configs/execution-plan.example.json` 那种裸 `Config`。可以直接用 `configs/execution-store.example.json`（第 0 步已复制），也可以先启动 control-plane，让它在 `-state-dir` 内初始化一个空 store，再让 dispatcher 指向同一个文件。

在 Relay 已启动的前提下：

```sh
cp "$JINX_RUN"/execution-store.example.json "$JINX_RUN"/execution.json

go run -ldflags=-linkmode=external ./cmd/control-plane \
  -dispatcher http://127.0.0.1:18088 -relay http://127.0.0.1:18080 \
  -state-dir "$JINX_RUN" -execution-config "$JINX_RUN"/execution.json \
  -backend-definitions "$JINX_RUN"/backends.example.json

go run -ldflags=-linkmode=external ./cmd/jinx-dispatcher \
  -config "$JINX_RUN"/dispatcher.example.json \
  -execution-config "$JINX_RUN"/execution.json \
  -backends-json "$JINX_RUN"/backends.example.json \
  -local-policy "$JINX_RUN"/local-policy.json
```

此时三个服务的 `/health` 应为 `ready`。示例配置没有监控任何群，也没有可用模型后端，所以入站消息会被判定为 `blocked_unmonitored`。要真正生成回复，还需要：

- 按 `configs/backends.example.json` 与 `docs/backend-integration.md` 接入可用的模型后端（Alma personal / text_only 或 Hermes）；
- 在 relay 的 `group_control` 与 dispatcher 的 `group_whitelist` / `group_rules` 中填入容器 ID；
- 动作出口：微信 Bridge（私有，不在本仓库），或 `cmd/bilibili-connector` 的 HTTP 动作端点（加 `-bilibili-connector` 传给 control-plane）。

### 4. 启动原生桌面控制台 (`desktop/`)

`desktop/` 目录为独立隔离的 Go `1.27.3` + MyGo 原生桌面 UI 模块（避免影响核心模块 `go 1.22.5` 依赖）：

```sh
cd desktop
go run . -mock -scenario full              # 离线 Mock 沙盒体验
go run . -api http://127.0.0.1:8090        # 连接本地回环 control-plane
```

详情、CLI 参数与测试方式见 [desktop/README.md](desktop/README.md)。

---

## 验证与边界检查

从 `<PROJECT_ROOT>` 执行：

```sh
# 公开核心全量竞态回归（离线、随机 loopback、假模型/假 Bridge/假 launchd）
# GOPROXY=off 要求本机模块缓存里已有依赖；首次运行请先执行 go mod download
JINX_LIVE_ALMA=0 GOPROXY=off GOSUMDB=off go test -race -ldflags=-linkmode=external ./... -count=1 -timeout=240s

# 静态检查与全部入口编译
go vet ./...
go build -ldflags=-linkmode=external ./cmd/...

# 前端类型契约严格校验（需要 TypeScript：npm i -g typescript）
tsc --strict --noEmit --lib ES2020,DOM \
  internal/console/client-contract.ts \
  internal/console/architecture-client.ts \
  internal/gateway/gateway-contract.ts

# 开源边界静态审计（可附加 --private-baseline 校验无私有信息泄露）
python3 scripts/check-public-boundary.py .
```

详细测试覆盖、隔离实测规范与历史校验边界见 [docs/verification.md](docs/verification.md)。

---

## 文档导航

| 文档 | 内容 |
| --- | --- |
| [docs/architecture-v2.md](docs/architecture-v2.md) | 三层协议 (`contracts`/`ports`)、连接器运行时边界与 `/console/v1/architecture` 投影语义 |
| [docs/connector-protocol.md](docs/connector-protocol.md) | Relay 多平台入站、Action Connector HTTP v1 (`POST /v1/actions`) 与 B 站示例说明 |
| [docs/platform-mapping.md](docs/platform-mapping.md) | 容器 ID、入站字段、ActionRef 与回声确认到三层协议的精确映射 |
| [docs/backend-integration.md](docs/backend-integration.md) | 控制面 HTTP 对接契约、执行器安全基线与边界说明 |
| [docs/target-page-spec.md](docs/target-page-spec.md) | 统一控制台页面规格 v1.4（信息架构、状态机、字段规则与交互边界） |
| [desktop/README.md](desktop/README.md) | 原生 MyGo 桌面控制台 (`jinx-desktop`) 启动方式、视图说明、真实性不变量与测试指南 |
| [docs/verification.md](docs/verification.md) | 全栈回归矩阵、可复现命令与验证边界 |
| [internal/dispatcher/LOCAL_POLICY.md](internal/dispatcher/LOCAL_POLICY.md) | 外置本地策略 (`-local-policy`)、场景模式、知识库与代码事实规则 |
| [internal/dispatcher/CONTROL.md](internal/dispatcher/CONTROL.md) | 暂停、维护排空、单任务取消及持久化边界语义 |
| [internal/console/API.md](internal/console/API.md) & [internal/gateway/README.md](internal/gateway/README.md) | 控制面 `/console/v1/*` 精确 HTTP 接口与 TypeScript 契约索引 |
| [internal/execution/README.md](internal/execution/README.md) & [internal/runtimebridge/README.md](internal/runtimebridge/README.md) | Execution Engine 目录路由与运行时桥接协议 |
| [internal/relay/README.md](internal/relay/README.md) & [internal/supervisor/README.md](internal/supervisor/README.md) | Relay 原子配置 CAS 与 `launchd` 白名单服务管理器 |

---

## 演进路线图

- [x] **Phase 1–2**：多平台容器/协议抽象 (`contracts` / `ports`)、Action Connector HTTP v1 及 `cmd/bilibili-connector` 端到端链路
- [x] **Phase 3**：场景策略（7 套内建 + `append`/`replace` 私有注入）、声明式能力挂载与只读知识库/代码事实提取
- [x] **Phase 4a（控制台对齐）**：将非微信连接器 `/health`、confirmed `dry_run` 开关、`/console/v1/catalog` 逻辑目录及 `/console/v1/architecture` 投影纳入控制面（未持久化的 `source`/`action` SSE 域按真实契约保持 `unsupported`）
- [x] **Phase 4b（统一桌面前端）**：基于 `docs/target-page-spec.md` 使用 MyGo Native UI 构建跨来源原生桌面控制台 [desktop/](desktop/README.md)（总览、来源与连接器、容器与场景编排、运行追踪、服务维护、变更回滚）
- [ ] **后续扩展**：Web/论坛连接器适配器、动态 `AgentRuntime` 真实绑定与更细粒度的工具沙箱隔离

---

## 使用边界与免责声明

- 本项目公开仓库中的所有平台连接器与示例适配器（包括 `cmd/bilibili-connector`）均定位于**个人及创作者在本地显式授权下自托管管理自有内容/社群**的参考实现。
- 仓库不提供、也不支持任何绕过认证、破解风控/人机验证码、多账号轮换、未经授权的大规模采集或批量骚扰营销能力。
- 部署与运行前请确保已获得目标容器/账号的合法授权，并严格遵守各平台服务条款（Terms of Service）及相关法律法规，一切外部运行行为及合规责任由部署运营方自行承担。

---

## 许可证

本项目的自有代码与文档基于 [Apache License 2.0](LICENSE) 发布，版权声明见 [NOTICE](NOTICE)。第三方代码、依赖与图标保留各自许可，详见 [第三方材料说明](THIRD_PARTY_NOTICES.md)。

私有提示词、个人知识库、运营身份与本地配置不属于公开仓库。`scripts/check-public-boundary.py` 提供有限格式的静态检查和显式私有基线比对，不能替代发布清单、人工复核和发行包检查。
