# 第三方材料来源与复核

本目录集中保留 JINX 核心与桌面端所用第三方许可。总说明见 [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md)。

[`SOURCES.json`](SOURCES.json) 为逐文件来源及校验清单：

- `modules` 记录精确模块版本、许可边界和模块校验值。
- `files` 记录保存的上游原文、原始字节数、SHA-256 和精确来源。`LICENSE`、`PATENTS` 均未改写。
- `generated_notice_indexes` 是本项目生成的原始版权行与归属行摘录，不冒充上游 `NOTICE` 文件。
- `dependency_scans` 保留各平台、标签及项目测试的实际外部包与 `EmbedFiles`，不含本机绝对路径。

## 来源模板

`${GOMODCACHE}` 指 `go env GOMODCACHE`；缓存源路径使用 `<module>@<version>/<relative-file>`，无需依赖整理者的本机路径。

`${GOROOT_CORE}` 指实际核心 `go1.22.5` 可执行文件的 `go env GOROOT`；`${GOROOT_DESKTOP}` 指实际桌面 `go1.27.3` 可执行文件的 `go env GOROOT`。本次分别读取这些发行版自身的 `LICENSE`、`PATENTS` 和实际使用的标准库内置第三方副本，不能仅根据 `go.mod` 中的最低版本推定最终二进制采用的工具链。

Lucide 从官方 `0.468.0` 标签取得；Feather MIT 补充文本从官方 `v4.29.2` 标签取得。MyGo 未标注内联 MingCute 图标的原始版本，因此这里只将官方固定提交作为许可文本快照。Public Suffix List 的提交号来自精确 `x/net` 版本的 `publicsuffix/table.go`。完整 URL 均保留在 `SOURCES.json`。

## 离线枚举

使用已安装且满足对应模块要求的 Go 可执行文件，从仓库根目录及 `desktop/` 目录分别执行：

```sh
GOWORK=off GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go list -deps -json ./...
```

桌面端额外检查 `-tags e2e`、`GOOS=linux GOARCH=amd64 CGO_ENABLED=0`、`GOOS=windows GOARCH=amd64 CGO_ENABLED=0` 以及 `-test -deps -json ./...` 的项目测试依赖。本次均使用已有缓存，没有下载模块、执行 `go mod tidy` 或改动项目模块文件。

Windows 首次枚举在原有 `desktop/go.sum` 处缺少 `golang.org/x/net/publicsuffix` 的校验项。随后将 `desktop/go.mod` 和 `desktop/go.sum` 复制到临时目录，通过 `-modfile=<temporary-go.mod> -mod=mod` 离线补全临时副本后，完整枚举到精确 `golang.org/x/net v0.59.0`。原有项目文件保持不变。清单同时保留原始失败范围和完成后的枚举范围，不能将其当作 Windows 构建已通过。

MyGo 的 Ebitengine 编译器及 typesetting 的 HarfBuzz 包不在本次实际包清单中，但其独立许可仍随该精确模块的材料保留。实际 shader 资源按 `EmbedFiles` 逐项记录，没有复制整个依赖缓存或未使用的测试字体。

## 文件完整性复核

从仓库根目录运行以下只读检查，验证所有随附原文及生成的归属索引与清单中的 SHA-256 一致：

```sh
python3 - <<'PY'
import hashlib
import json
from pathlib import Path

manifest = json.loads(Path("third_party/SOURCES.json").read_text())
for record in manifest["files"] + manifest["generated_notice_indexes"]:
    content = Path(record["file"]).read_bytes()
    assert hashlib.sha256(content).hexdigest() == record["sha256"], record["file"]
print("Third-party material hashes match.")
PY
```

已有对应缓存时，还可按 `files[].source` 模板定位原文，使用 `cmp` 逐字节比较。例如：

```sh
module_cache=$(GOWORK=off GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local go env GOMODCACHE)
cmp third_party/mygo/LICENSE "$module_cache/github.com/egoist/mygo@v0.10.0/LICENSE"
cmp third_party/mygo/ebitengine-shader/LICENSE "$module_cache/github.com/egoist/mygo@v0.10.0/internal/shader/LICENSE"
cmp third_party/typesetting/harfbuzz/LICENSE "$module_cache/github.com/go-text/typesetting@v0.3.5/harfbuzz/LICENSE"
```
