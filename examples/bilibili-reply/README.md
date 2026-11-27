# B 站评论采集与 AI 回复示例

本目录提供两个开箱可跑的参考例子，展示 JINX 如何通过 `cmd/bilibili-connector`、`cmd/relay` 与 `cmd/jinx-dispatcher` 实现 **「B 站评论采集 → 场景编排 → AI 生成回复 → Action Connector 回复投递」** 闭环。

## 两个场景

- [`fixed-video/`](fixed-video/): **指定视频采集与回复**。配置目标视频的 `BVID`，按时间顺序持续轮询新评论和楼中楼，推送到 Relay 并交由 Dispatcher 调度 AI 回复。
- [`keyword-search/`](keyword-search/): **关键词搜索视频再采集与回复**。先按关键词搜索 B 站视频，筛选出有评论的目标视频，再将其显式绑定到 `bilibili-connector`、`relay` 与 `dispatcher` 的容器白名单中进行评论采集和回复。

## 前置准备

1. 本机已安装 Go（1.22.5+）与 `ego-browser`。
2. 在 `ego-browser` 中登录你的 B 站账号（只读采集也需要有效会话；实际发表评论必须在本人授权/自有视频下进行，并遵守平台规范）。
3. **默认安全状态**：两个示例中的所有配置均默认开启 `"dry_run": true`：
   - `--inspect` 模式仅执行单次只读采集（`-once -skip-relay -show-comments`），不写游标也不推 Relay。
   - `--start` 模式启动完整三件套（`relay` + `bilibili-connector` + `jinx-dispatcher`），可观测入站评论和 Dispatcher 决策，但不会向 B 站真实发评。
   - 如需真正启用 AI 生成并真实发评，请将临时运行目录中的 `backends.json` 替换为真实的本地模型后端（参考 [`../../docs/backend-integration.md`](../../docs/backend-integration.md)），并确认草稿无误后通过控制面两步确认或修改运行配置关闭 `dry_run`。

## 快速运行

### 示例 1：指定视频采集评论

```sh
./examples/bilibili-reply/fixed-video/run.sh BV1example0001 --inspect
```

启动完整本地调度链（`dry_run: true`）：

```sh
./examples/bilibili-reply/fixed-video/run.sh BV1example0001 --start
```

### 示例 2：按关键词搜索视频并采集评论

```sh
./examples/bilibili-reply/keyword-search/run.sh "AI Agent" --inspect
```

可选参数：

- `--order totalrank|pubdate|click|dm|stow`：搜索排序方式（默认 `totalrank`）。
- `--pick N`：选择搜索结果中的第 `N` 个视频（默认自动选第一个有评论的视频）。
- `--start`：将选中的视频写入临时隔离运行目录并启动 `relay` + `bilibili-connector` + `jinx-dispatcher`。
