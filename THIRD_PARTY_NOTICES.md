# 第三方许可与声明

JINX 自有代码采用根目录 [`LICENSE`](LICENSE) 中的 Apache License 2.0。第三方代码、图标、数据及运行时保留各自的版权与许可，本项目的主许可不改变下列材料的许可条件。

完整许可集中放在根目录 `third_party/`。其中的上游 `LICENSE`、`PATENTS` 均按原始字节保存，未替换版权人、年份或许可中的占位文本。精确版本、来源模板、上游链接、SHA-256、模块校验值及实际依赖包和内嵌资源清单见 [`third_party/SOURCES.json`](third_party/SOURCES.json)，复核方法见 [`third_party/README.md`](third_party/README.md)。

## 核心与桌面端 Go 依赖

| 模块 | 版本 | 适用许可 | 随附材料 |
| --- | --- | --- | --- |
| `github.com/gorilla/websocket` | `v1.5.3` | BSD-2-Clause | [LICENSE](third_party/gorilla-websocket/LICENSE) |
| `github.com/egoist/mygo` | `v0.10.0` | MIT；下述子组件单独许可 | [LICENSE](third_party/mygo/LICENSE)、[源码版权与归属摘录](third_party/mygo/SOURCE_NOTICES.txt) |
| `github.com/ebitengine/purego` | `v0.11.1` | Apache-2.0；Go 派生部分为 BSD-3-Clause | [LICENSE](third_party/purego/LICENSE)、[源码版权摘录](third_party/purego/SOURCE_NOTICES.txt)、[Go LICENSE](third_party/go/go1.27.3/LICENSE) |
| `github.com/go-text/typesetting` | `v0.3.5` | `Unlicense OR BSD-3-Clause`；`harfbuzz` 子目录为 MIT | [LICENSE（含两个可选许可全文）](third_party/typesetting/LICENSE)、[HarfBuzz LICENSE](third_party/typesetting/harfbuzz/LICENSE) |
| `golang.org/x/image` | `v0.46.0` | BSD-3-Clause | [LICENSE](third_party/x-image/LICENSE)、[PATENTS](third_party/x-image/PATENTS) |
| `golang.org/x/text` | `v0.43.0` | BSD-3-Clause | [LICENSE](third_party/x-text/LICENSE)、[PATENTS](third_party/x-text/PATENTS) |
| `golang.org/x/net` | `v0.59.0` | BSD-3-Clause；Public Suffix List 数据为 MPL-2.0 | [LICENSE](third_party/x-net/LICENSE)、[PATENTS](third_party/x-net/PATENTS)、[数据 LICENSE](third_party/x-net/publicsuffix/LICENSE)、[数据原始声明](third_party/x-net/publicsuffix/SOURCE_NOTICE.txt) |

核心当前实际导入 Gorilla WebSocket。macOS、Linux 桌面端及 macOS `e2e` 构建实际导入 MyGo、purego、typesetting、`x/image`、`x/text`；Windows 桌面端另外通过 MyGo 的 `internal/contentrules` 导入 `x/net/publicsuffix`。MyGo 模块中未被本项目导入的音频、打包及其他插件依赖，不因出现在上游 `go.mod` 中而列为当前应用依赖。

当前 `desktop/go.sum` 缺少 Windows 所需的 `x/net` 校验项。Windows 的完整依赖清单使用临时模块文件副本和已存在的精确版本缓存离线取得；本次许可整理未修改项目 `go.mod` 或 `go.sum`，也未验证 Windows 可执行文件构建。

## 单独许可的子组件与图标

### MyGo 中的 Ebitengine 派生编译器

MyGo `internal/shader` 的原始说明将 `internal/shader`、`internal/shaderir` 标为派生自 Ebitengine 提交 `16f18b0` 的 Kage 编译器，采用 Apache-2.0，版权行包括 `Copyright 2020 The Ebiten Authors`、`Copyright 2022 The Ebiten Authors` 和 `Copyright 2023 The Ebitengine Authors`。随附其原始 [LICENSE](third_party/mygo/ebitengine-shader/LICENSE) 与 [源码归属摘录](third_party/mygo/SOURCE_NOTICES.txt)。这些编译器包未出现在本次桌面端依赖枚举中；材料保留用于明确该精确模块的多许可边界。实际内嵌的 Metal、GLSL、HLSL 渲染器资源另按 MyGo 的 MIT 许可保留，资源路径见来源清单。

### purego 中的 Go 派生代码

purego 的原始说明明确指出 `abi_*.h`、`wincallback.go` 和 `internal/fakecgo` 内多份文件源自 Go runtime/cgo，采用 Go 的 BSD-3-Clause 许可。这些文件的原始版权行保存在 [SOURCE_NOTICES.txt](third_party/purego/SOURCE_NOTICES.txt)，许可全文见 [Go LICENSE](third_party/go/go1.27.3/LICENSE)。purego 其余适用 Apache-2.0 的代码不因此改为 BSD。

### typesetting 中的 HarfBuzz 派生代码

`harfbuzz/LICENSE` 独立采用 MIT，保留 B. KUGLER、Google、Ebrahim Byagowi、Facebook、Mozilla Foundation、Codethink、Nokia、Keith Stribley、Martin Hosken and SIL International、Chris Wilson、Behdad Esfahbod、David Turner、Red Hat、Werner Lemberg 的原始版权声明，全文见 [LICENSE](third_party/typesetting/harfbuzz/LICENSE)。该包未出现在本次桌面端依赖枚举中，不能把它的许可与 typesetting 根目录的双选许可混为一谈。

### JINX 内联 Lucide / Feather 图标

`desktop/components/icons.go` 中至少 `Moon`、`Search`、`ChevronRight` 的几何路径已与 Lucide `0.468.0` 官方标签逐项核对。项目统一将描边宽度由上游的 `2` 调整为 `1.6`，并以 Go 字符串组合 SVG；这些调整不改变原有许可。

随附 Lucide 该精确标签的完整 [ISC LICENSE](third_party/lucide/LICENSE)，其中原文保留 Feather 派生部分的 `Cole Bemis 2013-2022` 与其余部分的 `Lucide Contributors 2022` 归属。另随附 Feather 官方 `v4.29.2` 的完整 [MIT LICENSE](third_party/lucide/feather/LICENSE)，其自身版权行 `Copyright (c) 2013-2023 Cole Bemis` 原样保留。Feather 标签是补充 MIT 原文的来源，并不声称 JINX 图标取自该 Feather 版本；Lucide 标签的归属也未被后者替换。

### MyGo 内联 MingCute 搜索图标

MyGo `ui/combobox.go` 将其内联 `searchIcon` 明确归属为 MingCute 搜索图标、Apache-2.0。随附 [归属摘录](third_party/mygo/SOURCE_NOTICES.txt) 与从官方固定提交 `9dc7d3ad074fb16477096a6a8109a6467d0e2fe3` 保存的完整 [LICENSE](third_party/mygo/mingcute/LICENSE)。MyGo 没有标注所复制图标的上游版本；该提交仅固定许可文本的获取来源，不推定图标最初来源版本。

### Windows Public Suffix List 数据

`x/net@v0.59.0/publicsuffix/table.go` 标明数据来自 Public Suffix List 提交 `d6c92f1bbb7433e5db7b8405c25d4035fb8ff376`（`2026-02-06T07:36:33Z`），并内嵌 `data/children`、`data/nodes`、`data/text`。随附该提交的完整 [MPL-2.0 LICENSE](third_party/x-net/publicsuffix/LICENSE) 与[原始声明](third_party/x-net/publicsuffix/SOURCE_NOTICE.txt)。数据源可从[对应提交](https://github.com/publicsuffix/list/blob/d6c92f1bbb7433e5db7b8405c25d4035fb8ff376/public_suffix_list.dat)取得，Go 派生表的源码可从 [`x/net v0.59.0`](https://go.googlesource.com/net/+/refs/tags/v0.59.0/publicsuffix/) 取得；分发包含这些数据的 Windows 二进制时应保留 MPL 声明和相应源码获取方式。

## Go 运行时与标准库

分别从实际执行的核心 Go `go1.22.5` 与桌面 Go `go1.27.3` 的 `go env GOROOT` 读取并保存 [核心 LICENSE](third_party/go/go1.22.5/LICENSE)、[核心 PATENTS](third_party/go/go1.22.5/PATENTS)、[桌面 LICENSE](third_party/go/go1.27.3/LICENSE)、[桌面 PATENTS](third_party/go/go1.27.3/PATENTS)。两个版本的原文分别保留，没有合并或改写版权声明。

标准库实际使用的工具链内置 `vendor/golang.org/x/crypto`、`x/net`、`x/text` 另分别保留各工具链的 `LICENSE`、`PATENTS`，位于 [`third_party/go/go1.22.5/vendor/`](third_party/go/go1.22.5/vendor/) 与 [`third_party/go/go1.27.3/vendor/`](third_party/go/go1.27.3/vendor/)。这些是工具链内置副本，不是桌面模块所选的同名依赖版本。更换构建工具链后，应按实际工具链更新随附材料。

## 资源与分发

依赖枚举覆盖 macOS、Linux、Windows 桌面端、macOS `e2e` 标签和 macOS/Linux 项目测试的依赖清单；只枚举依赖，不执行测试。MyGo 实际内嵌 JavaScript bridge、语言消息和平台渲染器 shader，Windows 还内嵌键盘处理脚本，详见来源清单。本次清单未发现内嵌字体或 `gofont` 包，因此没有收录未使用的上游测试字体。系统运行时加载的字体没有复制进仓库。

分发源码或二进制时，应随附本文件及 `third_party/` 中适用的完整许可、版权、专利与数据来源声明。依赖或构建标签改变后，应重新枚举实际依赖、检查新子组件及资源许可并更新来源清单。

`desktop/assets/jinx-logo.png` 是应用内嵌的品牌图像；当前材料未提供原图作者及授权来源。本文件不推定其作者或所有权，项目代码许可也不自动扩大任何第三方对该品牌资源的权利。发布前需由维护者补充原图来源和可公开分发的授权依据。
