# 持久 run / event store 集成契约

本模块只存储受理后的结构化元数据；不读取微信、不调用后端、不发送消息，不把旧日志或累计统计转为 run。`contracts.Run` 是内部持久事实，Console Facade 仍须映射为页面 `RunView`；不能原样当作前端 DTO。

## 精确接口

```go
// package contracts
func ValidateRun(run Run) error
func ValidateTransition(previous, next Run) error

// package observability
func Open(path string) (*Store, error)
func (store *Store) Close() error
func (store *Store) CreateRun(run contracts.Run) (contracts.Run, error)
func (store *Store) UpdateRun(run contracts.Run, expectedRevision, kind string) (contracts.Run, error)
func (store *Store) GetRun(id string) (contracts.Run, error)
func (store *Store) RunEvents(id string) ([]contracts.Event, error)
func (store *Store) Snapshot() (Snapshot, error)
func (store *Store) EventsAfter(after string, limit int) (EventPage, error)
func (store *Store) WaitEvents(ctx context.Context, after string, limit int) (EventPage, error)
func (store *Store) PruneEvents(before time.Time) error
func (store *Store) Checkpoint() error
```

`Snapshot` 字段为 `StreamID string`、`LastSeq uint64`、`Cursor string`、`CoverageSince time.Time`、`Runs []contracts.Run`。所有字段来自同一个锁内快照，runs 按 ID 排序；Facade 自行按业务时间筛选、排序、分页，不应将 ID 顺序当时间顺序。

`EventPage` 字段为 `Events []contracts.Event`、`Cursor string`、`HasMore bool`。limit 范围 1..1000。空页 cursor 保持不变。`WaitEvents` 在无新事件时等待，支持 context 取消、Close 唤醒；消费者每次用返回 cursor 继续读取，没有易丢事件的内存订阅队列。事件既是实体失效通知，也保留当时 phase/outcome/delivery/reasonCode；详情用 `RunEvents` 展示历史证据，不能把当前状态覆盖到旧事件上。

## Dispatcher 写入规则

1. 在实际 batch 受理点生成稳定 run ID，填写源/目标群 ID、非空且不重复的 message IDs、受理时间/水位、trigger、固定配置 Versions 和计划后端。未保留配置快照时必须提供 `MissingSnapshotReason`。新 run 只能是 `accepted/pending/not_started`，不得带 attempts 或 store revision。
2. 保存 `CreateRun` 返回的完整 run；后续修改该副本，调用 `UpdateRun(next, previous.Revision, kind)`。副本上的 `Revision` 和 `EntitySeq` 必须仍是上次读取值。修订号由 store 生成，是 opaque 字符串；只有相等比较。缺失、过时或重复创建返回 `ErrConflict`，失败不会产生事件。
3. 在后端实际启动/结束处追加或更新 `Attempts`，不能删除旧 attempt。timeout 必须显式记录 `timed_out` 及 `may_be_running/unknown/stopped`；取消远端仅在 `remoteExecution=stopped` 有证据时记录 `canceled`。终结 run 可以补录远端停止证据，但不能改写既有业务结果。
4. 进入提交前先生成稳定 `BridgeRequestID`，持久化 `SubmissionStartedAt` 和 `submitting/pending/submitting`。必须先成功持久化才允许外部提交。Bridge 明确接受后填精确 `SubmittedAt` 和 `SubmittedAtSource="bridge_ack"`，记录 `confirming/pending/submitted`；响应丢失/超时用 `unknown/unknown/unknown`，不能因 HTTP 错误直接当作已证明失败。
5. 可靠回声确认更新为 `terminal/replied/confirmed`。精确 ACK 路径需要 `SubmittedAt`、不早于它的 `ConfirmedAt`、非空 `EchoMessageID` 及提交关联。Bridge 响应丢失、但目标群新回声证明真实发送时，设置 `SubmittedAtSource="echo_inferred"`，保持 `SubmittedAt=nil`，必须有 `SubmissionStartedAt`、严格晚于该边界的 `ConfirmedAt`、非空 `EchoMessageID` 和 `BridgeRequestID`。允许从 submitting/unknown 直接确认，保留原未知事件；调用方负责验证回声的新鲜性、目标群和内容关联，store 不读取消息内容。不得把确认时间伪造成 ACK 时间。
6. 未越过提交边界的静默、抑制、拦截、过期、取消或执行失败，设置 `NoSubmission=true` 与原因码。`delivery=failed` 另外要求 `FailureEvidence` 表明已证明未交付；普通异常文本不能代替调用方的事实判断。
7. 正常阶段允许同阶段补证据或向前推进；跳过中间阶段必须带原因码。固定身份/配置/消息/受理水位不可变，证据时间不能倒退，提交后的 run 不允许取消、抑制、回到执行或再次提交。

`NoSubmission` 是调用方提供的事实证明，不是 store 探测结果。这里的串行写入和 expectedRevision 只保护记录；并未替代 Dispatcher 的暂停/取消/配置门禁锁。Dispatcher 必须把门禁验证和持久提交准入放在同一串行边界内，并确保 store 成功后提交组件验证该边界。只有集成后才可开放控制能力。

`contracts.DeliveryEvidence.SubmittedAtSource` 的 Go 类型是 `string`，JSON 名为 `submittedAtSource`，合法值只有空字符串、`bridge_ack`、`echo_inferred`。空字符串兼容旧记录，继续要求原精确提交时间；旧 ACK 可仅补标 `bridge_ack`。非空来源不可清空或替换；既有 ACK 时间、提交边界、确认时间、关联 ID 和回声 ID 均不能擦除或改写。`echo_inferred` 不允许携带推测的 `SubmittedAt`，后续也不能改成 ACK 来源。

## 恢复与故障

- 路径必须是隔离工程自行配置的数据文件，不能指向现役状态。`path` 为 JSON checkpoint，`path.wal` 为追加事务日志，`path.lock` 通过 OS 文件锁限制同一路径单写者。必须一起保管 checkpoint 和 WAL；不能单独拷贝 checkpoint 当成最新事实，不能手动删除锁文件或 WAL。文件权限 0600，新建父目录权限 0700。
- 每次 mutation 只序列化当前 run 和对应 event（prune 只写 floor），不复制全库。WAL frame 含 magic、长度、SHA-256 payload 校验和、commit 标记及重复长度；成功完整写入并 `File.Sync` 后才更新内存、发布事件。WAL 事务序号与事件 seq 分开，prune 不制造业务事件。重开先逐 frame 校验、重放并同步，再执行恢复。
- `Open` 加载并校验已有事实。confirmed 和其他 terminal 结果不变；所有未终结 run 保守转为 `unknown`、标记 `partial/restart_recovery` 并持久化恢复事件。原 submitting/submitted 转为 delivery unknown；原未提交仍是 not_started。不会重新生成或发送。
- 完整 journal 跨重启保留同一 streamId 和单调 seq。合法 cursor 在恢复事件前后连续。损坏/截断/未知 schema 拒绝打开并保留文件，不静默创建空库；WAL 校验、commit、序号或尾部不完整返回 `ErrWALCorrupt`，错误含失败 frame 的 byte offset。缺少 schema 2 必需 WAL 同样拒绝打开。没有自动截断尾部或自动修复 API；保留原文件，离线核对事务是否真实提交并从经过验证的备份恢复后才能重开。禁止为了打开成功而直接丢弃尾部。
- bootstrap 用 `Snapshot.Cursor` 接 `EventsAfter` / `WaitEvents`。错误 cursor、外来 stream、未来 seq 和已过期 cursor 返回 `ErrResetRequired`，Facade 必须要求全量快照。新库的空游标也应先取 Snapshot，不自行构造 cursor。
- `PruneEvents(before)` 只删除补流 journal 的连续过期前缀，并推进游标下界；永久 run 事实及各 run 历史事件仍保留。因此即使事件补流过期，提交去重和 unknown 历史仍存在。调用方可按 24 小时安排 prune；没有后台自动清理。
- append 写错误、短写或 fsync 失败一律返回 `ErrPersistenceUnknown`，不修改内存事实、不广播事件，后续读写拒绝执行。已有 `WaitEvents` 通过独立故障信号返回该错误；`Close` 仍可调用。必须关闭后重新打开核对磁盘：完整 frame 会重放并按原规则恢复 unknown，零字节失败无新增事务，残缺 frame 拒绝打开。不得据错误自动重发外部请求。上述错误及 `ErrWALCorrupt` 均支持 `errors.Is`。

## 旧 JSON 迁移与 checkpoint

`Open(path)` 原位兼容旧 schema 1 JSON，先校验其全部 run/history/journal，再创建并同步带原 streamId 的 WAL，原子写成 schema 2 checkpoint，之后才允许增量写入。迁移保留所有 run、历史、floor、cursor 和 coverage；恢复事件仍按原规则追加。迁移中断留下空/残缺 WAL 时拒绝打开，不能把它当作新的空日志。schema 2 明确要求 WAL 存在，避免误删 WAL 后悄悄回退到旧 checkpoint。旧二进制不支持 schema 2，不能直接回退运行。

`Checkpoint()` 是唯一的运行期全量重写入口，无后台或每 mutation checkpoint。持锁原子写新 JSON（临时文件、完整写入、文件同步、rename、目录同步），成功后原子替换 WAL 为带已提交事务水位的 header，再切换 append 文件句柄。JSON 先提交，WAL 后旋转；中断后可读取旧 WAL 并跳过已 checkpoint 的事务，也可读取新 WAL，均不重复事件、不改 cursor、不删 run 历史。checkpoint rename 前失败保留旧事实和 WAL；rename 后结果不确定或旋转失败 fail closed，重开核对。checkpoint 会在锁内扫描全库，应由调用方在可接受的维护时机显式执行。

## 边界与验证

这是 stdlib 同步单写者 WAL store。mutation 的序列化/写入成本随当前 run 大小增长，不随全部 run/history 增长；每次仍等待物理同步，无异步 ACK 或 group commit。run 及其历史仍全部留内存且未按 7 天自动清理，也没有配置快照内容存储。未知/待确认去重事实不会被事件保留策略删除；完整产品 retention、自动 checkpoint 调度和吞吐优化留给明确的后续策略。

离线验证命令（只使用虚构 ID、临时文件，无网络和真实群内容）：

```sh
go test -race -ldflags=-linkmode=external -count=1 ./internal/contracts ./internal/observability
go vet ./internal/contracts ./internal/observability
go test -ldflags=-linkmode=external ./internal/observability -run '^TestIncrementalWALAtTenThousandRuns$' -v -count=1
go test -ldflags=-linkmode=external ./internal/observability -run '^$' -bench '^BenchmarkCreateTenThousandRunsCountedSync$' -benchtime=1x
```

测试涵盖 880 种 phase/outcome/delivery 组合、新旧 ACK/echo 来源、缺少证据、不可变配置、跳阶段原因、attempt 状态/远端停止、已提交禁止重发、迟到回声、重启、未知历史保留、单写者、并发 revision 冲突、快照/补流、过期 cursor、等待取消/关闭/故障唤醒、写错误/短写/fsync 失败不发布、损坏文件拒绝覆盖、旧 JSON 迁移、checkpoint 旋转前后及失败恢复。

10k 规模测试/benchmark 真实追加文件，但用可计数同步替身创建 10k runs，断言每个 mutation 恰好一次 write+sync，隔离 macOS `File.Sync` 使用 `F_FULLFSYNC` 的磁盘抖动，避免基准准备耗时数分钟。测试另外在 1k/10k 各执行 11 次真实 `File.Sync` 更新并报告耗时与追加字节；这些真实同步样本与全部故障/重启测试共同验证持久路径。计数同步的创建时间不能当作真实每次 fsync 吞吐承诺。临时数据及缓存可通过 `TMPDIR`、`GOTMPDIR`、`GOCACHE` 指向 workspace `tmp`。
