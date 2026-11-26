# Supervisor

Go 1.22.5, standard library only. This package manages exactly four launchd
services: `relay`, `dispatcher`, `decision`, and `bridge`. Labels are compiled
into the package; plist paths are `<trusted PlistDir>/<fixed label>.plist`.
Neither service IDs, UIDs nor filesystem paths come from an HTTP request.
WeChat is an external dependency and has no control API here.

## API

```go
func New(Options) (*Manager, error)
func (*Manager) Close() error
func (*Manager) ListServices(context.Context) (ServiceList, error)
func (*Manager) ControlCapabilities(context.Context) Capabilities
func (*Manager) Plan(context.Context, Kind, ServiceID, string) (ActionPlan, error)
func (*Manager) Execute(context.Context, ExecuteInput) (Operation, error)
func (*Manager) GetOperation(context.Context, string) (Operation, error)
func (*Manager) GetOperationByRequest(context.Context, string) (Operation, error)
func (*Manager) ContinueMaintenance(context.Context, string) (Operation, error)
func (*Manager) CancelMaintenance(context.Context, string) (Operation, error)

type Commander interface {
    Run(context.Context, string, ...string) (stdout, stderr string, exit int, err error)
}

type DrainCoordinator interface {
    Snapshot(context.Context) (ControlSnapshot, error)
    Drain(context.Context, Scope) (string, error)
    Poll(context.Context, string) (DrainStatus, error)
    Cancel(context.Context, string) error
    Continue(context.Context, string) error
}
```

The fourth argument to Plan is the revision returned by ListServices. Execute
accepts `{PlanID, Token, RequestID}`. The plan token is only returned by Plan;
the journal retains a payload fingerprint for request idempotency, not the token.
Operation polling and manager lifetime belong to Console, not to a browser tab.
Successful Execute persists the pending operation before starting its worker;
canceling the HTTP request afterward does not cancel that worker.

## Console integration

Console supplies an observer and, for stop/restart, a DrainCoordinator. The observer must accept
the Commander signature and return cached or otherwise authorized launchd print
facts for the requested fixed target. It must not execute arbitrary inputs.
This separation makes the strict policy possible: built-in ExecCommander only
executes inside an explicitly enabled Execute worker, including its validation
and readback; New, Plan, ListServices and recovery never grant exec permission.
Without an observer, capabilities are unsupported and observations are unknown.
An explicitly supplied Commander is also used as the observer when Observer is
absent, which is convenient for offline runners. Injected implementations are
trusted and must honor cancellation and the same read-only observation policy.

```go
uid := uint32(operatorUID)
manager, err := supervisor.New(supervisor.Options{
    UID:              &uid,
    PlistDir:         trustedLaunchAgentsDirectory,
    DataPath:         independentControlJournalPath,
    EnableControl:    operatorServiceControlEnabled,
    Observer:         consoleLaunchdObserver,
    DrainCoordinator: consoleDrainCoordinator,
})
if err != nil {
    return err
}
defer manager.Close()

services, err := manager.ListServices(ctx)
if err != nil {
    return err
}
plan, err := manager.Plan(ctx, supervisor.Restart, supervisor.Relay, services.Revision)
if err != nil {
    return err
}
if !plan.Eligible {
    return showPlanBlockers(plan.Blockers)
}
operation, err := manager.Execute(ctx, supervisor.ExecuteInput{
    PlanID: plan.ID, Token: plan.Token, RequestID: consoleRequestID,
})
```

The example describes integration and is not executed by tests. Keep the manager
in the Console backend process, separate from Dispatcher. Pass only structured
allowlisted fields to clients. Capability readiness is a current infrastructure
check; per-service action eligibility is determined by Plan.

### Dispatcher gateway contract

Dispatcher's `internal/dispatcher/control.go` now declares ControlSnapshot,
MaintenanceView, ControlRequest and ControlOperation. Console should adapt these
through `GET /api/control/snapshot` and `POST /api/control/action`; the supervisor
does not guess a JSON protocol or import Dispatcher and its dependencies.

The adapter must:

- Preserve Dispatcher revision as an opaque token, send `expectedRevision` and a
  stable request ID on each action, and verify action persistence. POST uses
  `action.kind=drain|cancel_maintenance|continue_maintenance`, global scope, and
  the exact maintenance ID for cancel/continue. Never invoke `resume`.
- Expose any global or group pause as Paused. Expose unresolved deliveries as
  UnknownDelivery. If these facts cannot be obtained, return an error.
- Supply `ControlSnapshot.DependencyVersion` from the effective configuration
  dependency fingerprint, and `DecisionRequired` from effective rules. The
  Dispatcher control snapshot alone does not provide these configuration facts.
  A missing dependency fingerprint blocks stop/restart. Start treats an
  unavailable snapshot as unknown configuration and requires Decision for
  Dispatcher; Decision is not silently assumed optional.
- Map MaintenanceView.Stage `blocked_by_pause` to Paused, `deadline_exceeded`
  to `timedout`, and preserve `draining`, `blocked` and `ready`. Unknown stages,
  a different maintenance ID, or unverifiable persistence must fail closed.
- Set Global only for active global maintenance. Set Persisted only after a
  verified successful persistent action and when RecoveryRequired is false.
  Set ConfirmedSettled only with zero Pending, ActiveWorkers, UnresolvedRuns and
  UnknownDelivery; set RemoteSettled only with zero RemoteUnsettled. Missing
  fields are unknown, not zero. `ready` alone is insufficient evidence.
- Keep the global maintenance gate closed and stable from final Poll until the
  service action/readback ends. Snapshot/Poll must remain truthful under
  concurrent Console actions. Versions detect observed drift, but do not supply
  an atomic lock across independently running controllers.

Dispatcher stop/restart writes the ready proof to the manager journal before
the command. Console remains readable while Dispatcher is down. A gateway that
cannot read Dispatcher after stop must report unknown; it must not invent a new
snapshot from stale data to authorize subsequent stop/restart actions. Start
does not require a reachable Dispatcher or a configured DrainCoordinator. It
uses verified launchd observations, requires Relay/Bridge for Dispatcher, and
requires Decision when its configuration requirement is unknown. Dispatcher
then recovers its own durable state; start never resumes a pause or releases
maintenance.

## State and safety

- Options' zero value is read-only and launches no subprocess. Real control
  requires `EnableControl: true`, an explicit trusted UID, an absolute plist
  directory, an independent absolute journal path, and verified observations.
- Plans are read-only, short-lived and single-use. They include the target
  instance/version, observed service versions, dependencies and global drain
  requirement. No action starts another service implicitly. Dispatcher starts
  require Relay/Bridge and conditionally Decision; other stop/restart actions
  require a running Dispatcher for global drain. Dispatcher stop lists its
  dependencies without requiring them to be running.
- Start selects `bootstrap gui/<uid> <fixed plist>` for unregistered targets or
  `kickstart gui/<uid>/<label>` for registered targets. Stop uses `bootout` and
  succeeds only after an unregistered readback. Registered waiting/exited
  KeepAlive jobs are not stopped. Restart uses `kickstart -k` only after a durable
  global ready drain and succeeds only after an observed instance change.
- Start skips drain and does not use pause/delivery facts as blockers. It leaves
  existing durable pauses and maintenance untouched. Stop/restart require a
  current control snapshot and a verified global drain boundary.
- All commands are `/bin/launchctl` with distinct arguments. No shell, kill,
  arbitrary label or force bypass exists. Command output, environment,
  arguments, injected errors and secret text are never included in responses
  or the journal. Launchd unknown/error output never proves unregistration.
- Service, dependency, plan expiry and revision are checked again before every
  command; stop/restart also recheck pause, delivery and drain evidence. Command
  intent and any required ready proof are synced
  before invocation. A command is invoked at most once per operation. A nonzero
  exit with unchanged observed version is failed; ambiguous execution/readback
  is unknown. A verified target state can succeed even after a command error.
- One manager holds an OS file lock for its data path. Journal and lock are
  mode 0600; updates use sync, atomic rename and directory sync. The journal is
  bound to UID/plist directory. Store it in an operator-owned directory distinct
  from Dispatcher/Relay business data and plists. Trusted parent directories
  must not be changed by another principal. Journal errors poison control.
- Same request ID and same payload returns the original operation across
  reopen; a different payload is rejected. Reusing a plan under a new request
  is rejected. Only one unfinished/unknown operation can control the domain.
- Reopen converts interrupted states to unknown without executing commands or
  polling services in the constructor. GetOperation may read back an unknown
  target but never retries the command or claims retrospective success.
- A blocked operation can explicitly ContinueMaintenance while its original
  plan/deadline and dependencies remain valid. A ready continue records
  `execute_required` and never launches a service command. Explicit Execute
  with the original plan/token/request resumes that operation after revalidation;
  it keeps the same operation ID and still invokes the command at most once.
  Expiry never schedules a delayed action.
  CancelMaintenance before the command ends the operation without service
  control. Neither path automatically resumes a user pause. CancelMaintenance
  can also explicitly release maintenance after a succeeded/failed action while
  preserving that result; Dispatcher must be available for gateway cleanup.
- Unknown operations deliberately block further control and cannot be forced,
  resumed or canceled into apparent success. Operator reconciliation is needed.
  Closing the manager cancels and waits for workers; it never silently cancels
  maintenance, resumes users, or retries commands. Keep it alive while UI closes.

## Offline verification

```sh
go test -race -count=1 ./internal/supervisor
go vet ./internal/supervisor
```

Tests inject fake Commander and DrainCoordinator implementations. They never
invoke real launchctl, change live plists/data, or control the WeChat client.
