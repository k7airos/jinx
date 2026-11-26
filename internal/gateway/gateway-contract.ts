import type { Bootstrap, Envelope, ServiceView } from '../console/client-contract';

export type Result<T> =
  | { ok: true; value: T }
  | { ok: false; problem: { code: string; message: string; currentRevision?: string } };

export type Capability = 'text_generation' | 'auto_text_delivery' | 'web_search'
  | 'web_fetch' | 'host_write' | 'reminder_sync' | 'host_tools' | 'remote_delegation';
export type Enforcement = 'hard' | 'soft' | 'host' | 'unverified';
export type BackendKind = 'alma_personal' | 'text_only' | 'legacy_hermes';

export interface Permission {
  capability: Capability;
  requested: boolean | null;
  effective: boolean | null;
  enforcement: Enforcement;
  provenance: string;
  policyVersion: string;
  enforcedAt: string[];
  editable: boolean;
  reason: string;
}

export interface Backend {
  id: string;
  kind: BackendKind;
  models: { id: string; label: string }[];
  available: boolean;
  reason: string;
  agentBinding: 'branch_only' | 'unverified';
  sessionModes: ['independent_run'];
  minTimeoutSec: number;
  maxTimeoutSec: number;
  permissionEvidence: Permission[];
}

export interface RouteAttempt {
  backendId: string;
  modelId: string;
  timeoutSec: number;
  session: { mode: 'independent_run'; rotateAfterTurns: 0; rotateAfterSeconds: 0 };
}

export interface Routing {
  routeId: string;
  policyId: string;
  primary: RouteAttempt;
  fallbacks: RouteAttempt[];
}

export interface Policy {
  capabilities: Record<Capability, boolean>;
  noTools: boolean;
  allowSoft: boolean;
}

export interface ExecutionConfig {
  schemaVersion: '1';
  groups: Record<string, Routing>;
  profiles: Record<string, string>;
  policies: Record<string, Policy>;
}

export interface ExecutionSnapshot { revision: string; config: ExecutionConfig }
export interface ExecutionPlanInput { expectedRevision: string; config: ExecutionConfig }
export interface ExecutionApplyInput extends ExecutionPlanInput { requestId: string }
export interface ExecutionPlan {
  expectedRevision: string;
  configDigest: string;
  affectedGroups: string[];
  effects: string[];
  warnings: string[];
}
export interface ExecutionApplyResult {
  snapshot: ExecutionSnapshot;
  appliedRevision: string;
  replayed: boolean;
}
export interface GroupRouting { groupId: string; revision: string; routing: Routing }
export interface GroupPermissions {
  groupId: string;
  revision: string;
  policy: Policy;
  permissions: Permission[];
}

export type Scope = { kind: 'global'; groupId?: never } | { kind: 'group'; groupId: string };
export interface ScopeControl {
  paused: boolean;
  revision: string;
  boundary: number;
  changedAt: string;
}
export interface ControlCounts {
  pending: number;
  phases: Record<string, number>;
  activeWorkers: number;
  unknownDelivery: number;
  remoteUnsettled: number;
  unresolvedRuns: number;
}
export interface Maintenance {
  maintenanceId: string;
  revision: string;
  scope: Scope;
  boundary: number;
  startedAt: string;
  deadline: string;
  active: boolean;
  recoveryRequired: boolean;
  stage: 'draining' | 'ready' | 'blocked' | 'blocked_by_pause' | 'canceled';
  counts: ControlCounts;
  blockers: string[];
}
export interface ControlSnapshot {
  revision: string;
  global: ScopeControl;
  groups: Record<string, ScopeControl>;
  maintenance?: Maintenance;
}
export type ControlAction =
  | { kind: 'pause' | 'resume' | 'drain'; scope: Scope }
  | { kind: 'cancel_maintenance' | 'continue_maintenance'; scope: Scope; maintenanceId: string }
  | { kind: 'cancel_run'; scope: Scope; runId: string; cancelMode: 'block_delivery' | 'stop_execution' };
export interface ControlRequest {
  requestId: string;
  expectedRevision: string;
  action: ControlAction;
}
export interface ControlOperation {
  operationId: string;
  requestId: string;
  revision: string;
  status: string;
  control?: ScopeControl;
  maintenanceId?: string;
  runId?: string;
  runRevision?: string;
  remoteExecution?: string;
  fingerprint: string;
  action: ControlAction;
}

export type ServiceID = 'relay' | 'dispatcher' | 'decision' | 'bridge';
export type ServiceKind = 'start' | 'stop' | 'restart';
export interface ServiceProblem { code: string; message: string }
export interface RuntimeService {
  id: ServiceID;
  label: string;
  registration: string;
  state: string;
  pid?: number;
  instanceId?: string;
  lastExit?: number;
  version: string;
  problem?: ServiceProblem;
}
export interface RuntimeControl {
  version: string;
  dependencyVersion: string;
  decisionRequired: boolean;
  paused: boolean;
  unknownDelivery: boolean;
}
export interface RuntimeServices {
  revision: string;
  services: RuntimeService[];
  control?: RuntimeControl;
  problem?: ServiceProblem;
}
export interface ServicePlanInput {
  expectedRevision: string;
  kind: ServiceKind;
  serviceId: ServiceID;
}
export interface ServicePlan extends ServicePlanInput {
  id: string;
  token?: string;
  expiresAt: string;
  expectedInstanceId?: string;
  targetVersion: string;
  dependencies: {
    serviceId: ServiceID; version: string; instance?: string; required: boolean; ready: boolean;
  }[];
  drainRequired: boolean;
  drainScope?: 'global';
  eligible: boolean;
  blockers: ServiceProblem[];
  base: RuntimeServices;
}
export interface ServiceExecuteInput { planId: string; token: string; requestId: string }
export interface DrainStatus {
  maintenanceId: string;
  state: string;
  global: boolean;
  persisted: boolean;
  confirmedSettled: boolean;
  remoteSettled: boolean;
  unknownDelivery: boolean;
  paused: boolean;
}
export interface ServiceOperation {
  id: string;
  requestId: string;
  planId: string;
  serviceId: ServiceID;
  kind: ServiceKind;
  state: string;
  revision: number;
  createdAt: string;
  deadline: string;
  maintenanceId?: string;
  drain?: DrainStatus;
  steps: { name: string; state: string }[];
  readback?: RuntimeService;
  problem?: ServiceProblem;
}

export interface GatewayRoutes {
  'GET /console/v1/bootstrap': { response: Result<Bootstrap> };
  'GET /console/v1/controls': { response: Result<ControlSnapshot> };
  'POST /console/v1/controls/actions': { input: ControlRequest; response: Result<ControlOperation> };
  'GET /console/v1/controls/operations/{id}': { response: Result<ControlOperation> };
  'GET /console/v1/backends': { response: Result<Backend[]> };
  'GET /console/v1/groups/{id}/routing': { response: Result<GroupRouting> };
  'GET /console/v1/groups/{id}/permissions': { response: Result<GroupPermissions> };
  'GET /console/v1/execution/snapshot': { response: Result<ExecutionSnapshot> };
  'POST /console/v1/execution/plan': { input: ExecutionPlanInput; response: Result<ExecutionPlan> };
  'POST /console/v1/execution/apply': { input: ExecutionApplyInput; response: Result<ExecutionApplyResult> };
  'GET /console/v1/services': { response: Result<Envelope<ServiceView[]>> };
  'GET /console/v1/services/runtime': { response: Result<RuntimeServices> };
  'POST /console/v1/services/plan': { input: ServicePlanInput; response: Result<ServicePlan> };
  'POST /console/v1/services/execute': { input: ServiceExecuteInput; response: Result<ServiceOperation> };
  'GET /console/v1/services/operations/{id}': { response: Result<ServiceOperation> };
  'POST /console/v1/services/maintenance/{id}/continue': { input: Record<string, never>; response: Result<ServiceOperation> };
  'POST /console/v1/services/maintenance/{id}/cancel': { input: Record<string, never>; response: Result<ServiceOperation> };
}
