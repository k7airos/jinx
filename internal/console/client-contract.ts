export type ID = string;
export type ISOTime = string;
export type Freshness = 'fresh' | 'stale' | 'disconnected' | 'unknown';
export type Completeness = 'complete' | 'partial' | 'legacy_partial';
export type Phase = 'accepted' | 'aggregating' | 'gating' | 'queued' | 'executing'
  | 'checking' | 'submitting' | 'confirming' | 'terminal' | 'unknown';
export type Outcome = 'pending' | 'replied' | 'silent' | 'blocked' | 'suppressed'
  | 'expired' | 'canceled' | 'failed' | 'unknown';
export type Delivery = 'not_started' | 'blocked' | 'submitting' | 'submitted'
  | 'confirmed' | 'failed' | 'unknown';
export type AttemptStatus = 'queued' | 'running' | 'succeeded' | 'failed'
  | 'timed_out' | 'cancel_requested' | 'canceled' | 'unknown';
export type RemoteExecution = 'stopped' | 'may_be_running' | 'unknown' | 'not_applicable';

export interface Availability {
  supported: boolean;
  enabled: boolean;
  reason: string;
}

export interface Problem {
  code: 'validation' | 'conflict' | 'unsupported' | 'stale' | 'forbidden'
    | 'unavailable' | 'too_large' | 'unknown' | 'internal' | 'reset_required' | 'not_found';
  message: string;
  requestId: ID;
  retry: 'read' | 'replan' | 'none';
}

export type Result<T> = { ok: true; value: T } | { ok: false; problem: Problem };

export interface Envelope<T> {
  data: T;
  observedAt: ISOTime;
  source: string;
  freshness: Freshness;
  completeness: Completeness;
  revision: string;
  streamId?: ID;
  entitySeq?: number;
  missingReason?: string;
}

export interface Versions {
  dispatcher: string;
  relay?: string;
  routing: string | null;
  policy: string | null;
  fingerprint: string;
}

export interface GroupRef {
  id: ID;
  label: string;
  visibility: 'visible' | 'not_observed' | 'unknown';
}

export interface Watermark {
  token: string;
  recordedAt: ISOTime;
  sourceSequence: string | null;
}

export interface AttemptView {
  id: ID;
  backendId: ID;
  modelLabel: string | null;
  status: AttemptStatus;
  remoteExecution: RemoteExecution;
  startedAt: ISOTime | null;
  endedAt: ISOTime | null;
  externalId: ID | null;
  missingExternalIdReason: string | null;
  fallbackReason: string | null;
  versions: Versions;
}

export interface DeliveryEvidence {
  submissionStartedAt: ISOTime | null;
  submittedAt: ISOTime | null;
    submittedAtSource: 'bridge_ack' | 'echo_inferred' | null;
  confirmedAt: ISOTime | null;
  bridgeRequestId: ID | null;
  echoMessageId: ID | null;
  noSubmission: boolean;
  failureEvidence: string | null;
}

export interface RunView {
  id: ID;
  revision: string;
  entitySeq: number;
  source: GroupRef;
  target: GroupRef;
  acceptedAt: ISOTime | null;
  updatedAt: ISOTime;
  lastProgressAt: ISOTime | null;
  admissionWatermark: Watermark | null;
  trigger: string;
  messageCount: number | null;
  messageIds: ID[];
  phase: Phase;
  outcome: Outcome;
  delivery: Delivery;
  reasonCode: string | null;
  summary: string;
  completeness: Completeness;
  versions: Versions;
  configSnapshotId: ID | null;
  missingSnapshotReason: string | null;
  plannedBackendId: ID | null;
  attempts: AttemptView[];
  excerpt: string | null;
  sideEffects: SideEffect[];
  deliveryEvidence: DeliveryEvidence;
  inputs?: InboundReference[] | null;
  inputsMissingReason?: string | null;
  action?: ActionRef | null;
  receipt?: ProcessingReceipt | null;
  actions: { blockDelivery: Availability; stopExecution: Availability };
}

export interface SideEffect {
  kind: string;
  status: 'confirmed' | 'failed' | 'unknown';
  occurredAt: ISOTime | null;
  summary: string;
}

export interface TimelineItem {
  id: ID;
  runId: ID;
  occurredAt: ISOTime | null;
  source: string;
  kind: string;
  reasonCode: string | null;
  summary: string;
  certainty: 'observed' | 'missing' | 'not_entered';
  attemptId: ID | null;
}

export interface RunDetail {
  run: RunView;
  timeline: TimelineItem[];
}

export interface RunFilter {
  from?: ISOTime;
  to?: ISOTime;
  groupId?: ID;
  phases?: Phase[];
  outcomes?: Outcome[];
  deliveries?: Delivery[];
  backendId?: ID;
  fallback?: boolean;
  version?: string;
  completeness?: Completeness;
  query?: string;
  limit?: number;
  cursor?: string;
}

export interface RunPage {
  items: RunView[];
  nextCursor: string | null;
  coverageSince: ISOTime;
  watermark: string;
}

export interface TraceState extends Availability {
  domain: 'run';
  streamId: ID | null;
  cursor: string | null;
  lastSeq: number | null;
  coverageSince: ISOTime | null;
}

export interface RunEvent {
  streamId: ID;
  seq: number;
  cursor: string;
  occurredAt: ISOTime;
  entity: 'run';
  entityId: ID;
  revision: string;
  kind: string;
  reasonCode: string;
  phase: Phase;
  outcome: Outcome;
  delivery: Delivery;
  envelopeId?: ID;
  source?: SourceRef | null;
  action?: ActionRef | null;
}

export type RunSSEFrame =
  | { event: 'run'; id: string; data: RunEvent }
  | { event: 'reset_required' | 'unavailable'; data: Problem };

export interface FieldValue {
  configured: unknown;
  effective: unknown;
  origin: string;
  editable: boolean;
  reason: string;
}

export interface GroupView {
  id: ID;
  group: GroupRef;
  labelSource: 'config_target_ui_only' | 'unverified';
  uiTargetLabel: string;
  routing: null;
  missingRoutingReason: string;
  permissions: unknown[];
  backendId: ID | null;
  latestConfirmedAt: ISOTime | null;
  attentionCodes: string[];
  revision: string;
  versions: Versions;
  routeExists: boolean;
  enabled: boolean;
  whitelisted: boolean;
  effectiveDryRun: boolean;
  visibility: string;
  fields: Record<string, FieldValue>;
}

export interface GlobalRulesView {
  fields: Record<string, FieldValue>;
  versions: Versions;
  compatibility: {
    groupWhitelist: ID[] | null;
    groupProfiles: Record<ID, string> | null;
    groupOtherBots: Record<ID, string[]> | null;
    defaultProfile: string | null;
    editable: false;
    semantics: Record<'groupWhitelist' | 'groupProfiles' | 'groupOtherBots' | 'defaultProfile', string>;
  };
}

export interface ForwardRule {
  id: ID;
  enabled: boolean;
  source_container_id: ID;
  target_container_id: ID;
  dry_run?: boolean;
  include_echo?: boolean;
  template?: string;
}

export interface DirectDeliverInput {
  target_container_id: ID;
  text: string;
  idempotency_key: ID;
  source?: string;
  dry_run?: boolean;
  content_id?: ID;
  parent_id?: ID;
  root_id?: ID;
  reply_context?: unknown;
}

export interface DirectDeliverResult {
  run_id: ID;
  replayed: boolean;
  dry_run: boolean;
  target_container_id: ID;
  trigger: string;
  phase: Phase;
  outcome: Outcome;
  delivery: Delivery;
  reason_code?: string;
  delivery_evidence: DeliveryEvidence;
}

export interface SchemaEntry {
  type: 'boolean' | 'string' | 'integer' | 'decimal' | 'string_list' | 'number_list' | 'forward_rule_list';
  nullable: boolean;
  minimum: number;
  maximum: number;
  maxRunes: number;
  editable: boolean;
  inherited: boolean;
  default: unknown;
}

export interface FormSchemas {
  schemaVersion: 'console-rules-v1';
  group: Record<string, SchemaEntry>;
  global: Record<string, SchemaEntry>;
}

export interface ServiceView {
  id: 'relay' | 'dispatcher' | 'decision' | 'bridge';
  process: 'unknown';
  health: 'ready' | 'not_ready' | 'unreachable' | 'unsupported' | 'unknown';
  healthLatencyMs: number;
  healthObservedAt: ISOTime;
  facts: Record<string, boolean | number | string | null>;
  actions: Record<'start' | 'stop' | 'restart', Availability>;
  startedAt: ISOTime | null;
  lastProgressAt: ISOTime | null;
}

export interface Overview {
  stats: Partial<Record<'total_received' | 'total_triggered' | 'total_submitted'
    | 'total_replied' | 'total_dry_run' | 'total_send_failed' | 'total_unknown'
    | 'replay_gaps' | 'pending_messages', number>> & { relay_connected?: boolean };
  statsSource: 'persistent_cumulative';
  statsAvailable: boolean;
  pendingMeaning: 'pending_and_in_flight_messages';
  services: ServiceView[];
  controls: Availability;
  recentRuns: RunView[];
  trace: TraceState;
  currentWork: StageCounts;
  workSource: 'retained_trace_snapshot';
  legacyCounters: Record<string, number | null>;
  relayConnected: boolean | null;
  attention: AttentionItem[];
}

export interface StageCounts {
  aggregating: number | null;
  queued: number | null;
  executing: number | null;
  submitting: number | null;
  confirming: number | null;
  unknownDelivery: number | null;
  remoteUnsettled: number | null;
}

export interface AttentionItem {
  id: ID;
  revision: string;
  code: string;
  summary: string;
  occurredAt: ISOTime;
  acknowledged: boolean;
  target: { kind: 'run' | 'operation'; id: ID } | null;
}

export interface Scope {
  kind: 'global' | 'group';
  groupId?: ID;
}

export type Edit =
  | { kind: 'field'; scope: Scope; field: string; value: unknown }
  | { kind: 'group_admission'; groupId: ID; enabled: boolean }
  | { kind: 'create_group'; groupId: ID; targetGroupId: ID; targetLabel: string }
  | { kind: 'delete_group'; groupId: ID };

export type RelayEdit =
  | { kind: 'relay_field'; scope: { kind: 'relay_group_control' }; field: keyof RelayGroupControl; value: unknown }
  | { kind: 'relay_field'; scope: { kind: 'relay_filters' }; field: keyof RelayFilters; value: unknown }
  | { kind: 'group_collection'; groupId: ID; enabled: boolean };

export interface PlanInput {
  requestId: ID;
  base: Versions;
  schemaVersion: 'console-rules-v1';
  edits: (Edit | RelayEdit)[];
}

export interface PublishInput {
  requestId: ID;
  planId: ID;
  token: string;
}

export interface FieldChange {
  component: string;
  path: string;
  before: unknown;
  after: unknown;
  effectiveBefore: unknown;
  effectiveAfter: unknown;
}

export interface ChangePlan {
  id: ID;
  token: string;
  expiresAt: ISOTime;
  base: Versions;
  changes: FieldChange[];
  affectedGroups: ID[];
  effect: string[];
  warnings: { code: string; message: string }[];
  componentOrder: string[];
}

export interface ComponentResult {
  id: string;
  state: string;
  readbackVersion: string | null;
  persistenceVerified: boolean | null;
}

export interface Operation {
  id: ID;
  requestId: ID;
  revision: string;
  changeId: ID;
  status: 'pending' | 'unknown' | 'partial_apply' | 'succeeded' | 'failed';
  summary: string;
  resultRevision: string | null;
  components: ComponentResult[];
  maintenanceId: ID | null;
  runId: ID | null;
  cancelResult: null;
}

export interface ChangeRecord {
  id: ID;
  createdAt: ISOTime;
  origin: string;
  base: Versions;
  readback: Versions | null;
  changes: FieldChange[];
  operation: Operation;
  target: Versions | null;
  beforeSnapshot: ConfigurationSnapshot | null;
  targetSnapshot: ConfigurationSnapshot | null;
  readbackSnapshot: ConfigurationSnapshot | null;
  rollbackOf: ID | null;
}

export interface RelayGroupControl {
  monitor_all: boolean;
  monitored_groups: ID[] | null;
  exposed_groups: ID[] | null;
  group_aliases: Record<ID, string> | null;
  drop_unexposed_completely: boolean;
}

export interface RelayFilters {
  keyword_blocklist: string[] | null;
  regex_blocklist: string[] | null;
  sender_blocklist: string[] | null;
  allowed_types: number[] | null;
  clean_content: boolean;
  ignore_echo: boolean;
}

export interface RelayRules {
  group_control: RelayGroupControl;
  filters: RelayFilters;
}

export interface RelayRulesView {
  rules: RelayRules;
  versions: Versions;
  writeConcurrency: 'digest_preflight_local_mutex' | 'provider_remote_cas' | 'remote_cas';
  warnings: { code: string; message: string }[];
}

export interface ConfigurationSnapshot {
  capturedAt: ISOTime;
  versions: Versions;
  source: string;
  scope: string[];
  dispatcher: { rules: Record<string, unknown>; bridge: { dry_run: boolean } } | null;
  relay: RelayRules | null;
}

export interface ConfigSnapshot {
  id: ID;
  versions: Versions;
  capturedAt: ISOTime;
  group: GroupRef;
  fields: Record<string, FieldValue>;
  globalFields: Record<string, FieldValue>;
  routing: null;
  permissions: unknown[];
  source: 'dispatcher_trace_config_snapshot';
  scope: string[];
}

export interface ConfigurationDrift {
  component: 'dispatcher' | 'relay';
  available: boolean;
  reason: string;
  drift: boolean | null;
  memoryVersion: string | null;
  diskVersion: string | null;
  memory: unknown;
  disk: unknown;
}

export interface ChangeFilter {
  from?: ISOTime;
  to?: ISOTime;
  origin?: 'console' | 'external' | 'unknown';
  status?: Operation['status'];
  component?: 'dispatcher' | 'relay';
  groupId?: ID;
  query?: string;
  limit?: number;
  cursor?: string;
}

export interface ChangePage {
  items: ChangeRecord[];
  nextCursor: string | null;
}

export interface Unpaginated<T> {
  items: T[];
  nextCursor: null;
}

export interface GroupPage extends Unpaginated<GroupView> {
  coverageSince: null;
}

export interface Bootstrap {
  streamId: ID | null;
  cursor: string | null;
  lastSeq: number | null;
  overview: Envelope<Overview>;
  activeOperations: Operation[];
  capabilities: Record<string, Availability>;
  schemas: FormSchemas;
  eventStream: TraceState;
}

export interface CatalogScenario {
  id: ID;
  builtin: boolean;
  hasLocalPolicy: boolean;
}

export interface CatalogKnowledgeBase {
  id: ID;
}

export interface CatalogTool {
  id: ID;
  supportAllowed: boolean;
}

export interface CatalogSkill {
  id: ID;
}

export interface CatalogView {
  scenarios: CatalogScenario[];
  knowledgeBases: CatalogKnowledgeBase[];
  tools: CatalogTool[];
  skills: CatalogSkill[];
}

export interface ConnectorTargetView {
  containerId: ID;
  label: string | null;
  enabled: boolean;
}

export interface ConnectorView {
  platform: ConnectorPlatform;
  configured: boolean;
  state: ServiceView['health'];
  availability: Availability;
  dryRun: boolean | null;
  capabilities: ArchitectureActionKind[];
  controls: string[];
  accountId: ID | null;
  connectorName: ID | null;
  targets: ConnectorTargetView[];
  lastPollAt: ISOTime | null;
  lastPollErrorCode: string | null;
  reason: string | null;
  protocolVersion: number | null;
  discoverySource?: 'static' | 'dropin' | 'dynamic' | string | null;
  heartbeatState?: 'alive' | 'stale' | 'expired' | 'untracked' | string | null;
  lastHeartbeatAt?: ISOTime | null;
  heartbeatTtlSec?: number | null;
  registeredAt?: ISOTime | null;
  observedAt: ISOTime;
}

export interface ConnectorDryRunPlanInput {
  requestId: ID;
  expectedDryRun: boolean;
  targetDryRun: boolean;
}

export interface ConnectorDryRunPlan {
  id: ID;
  token: string;
  platform: ConnectorPlatform;
  currentDryRun: boolean;
  targetDryRun: boolean;
  persisted: boolean;
  expiresAt: ISOTime;
  warnings: { code: string; message: string }[];
}

export interface ConnectorDryRunApplyInput {
  requestId: ID;
  planId: ID;
  token: string;
  confirm: boolean;
}

export interface ConnectorDryRunApplyResult {
  ok: boolean;
  platform: ConnectorPlatform;
  previousDryRun: boolean;
  dryRun: boolean;
  persisted: boolean;
  readbackVerified: boolean;
  status: 'succeeded';
  requestId: ID;
  observedAt: ISOTime;
}

export interface CurrentConsoleHTTPClient {
  getArchitecture(): Promise<Result<ArchitectureSnapshot>>;
  subscribeArchitecture(input: ArchitectureSubscription): () => void;
  bootstrap(): Promise<Result<Bootstrap>>;
  getOverview(): Promise<Result<Envelope<Overview>>>;
  getCatalog(): Promise<Result<Envelope<CatalogView>>>;
  listConnectors(): Promise<Result<Envelope<ConnectorView[]>>>;
  planConnectorDryRun(
    platform: ConnectorPlatform,
    input: ConnectorDryRunPlanInput,
  ): Promise<Result<ConnectorDryRunPlan>>;
  applyConnectorDryRun(
    platform: ConnectorPlatform,
    input: ConnectorDryRunApplyInput,
  ): Promise<Result<ConnectorDryRunApplyResult>>;
  listRuns(filter: RunFilter): Promise<Result<Envelope<RunPage>>>;
  getRun(id: ID): Promise<Result<Envelope<RunDetail>>>;
  listServices(): Promise<Result<Envelope<ServiceView[]>>>;
  listGroups(): Promise<Result<Envelope<GroupPage>>>;
  getGroup(id: ID): Promise<Result<Envelope<GroupView>>>;
  getGlobalRules(): Promise<Result<Envelope<GlobalRulesView>>>;
  getRelayRules(): Promise<Result<Envelope<RelayRulesView>>>;
  planRelayRules(input: PlanInput): Promise<Result<ChangePlan>>;
  getConfigurationDrift(component: 'dispatcher' | 'relay'): Promise<Result<Envelope<ConfigurationDrift>>>;
  getConfigSnapshot(id: ID): Promise<Result<Envelope<ConfigSnapshot | null>>>;
  planChange(input: PlanInput): Promise<Result<ChangePlan>>;
  publishChange(input: PublishInput): Promise<Result<Operation>>;
  listChanges(filter?: ChangeFilter): Promise<Result<Envelope<ChangePage>>>;
  getChange(id: ID): Promise<Result<Envelope<ChangeRecord>>>;
  planRollback(input: { requestId: ID; changeId: ID; base: Versions }): Promise<Result<ChangePlan>>;
  directDeliver(input: DirectDeliverInput): Promise<Result<DirectDeliverResult>>;
  listAttention(): Promise<Result<Envelope<{ items: AttentionItem[]; runsAvailable: boolean }>>>;
  acknowledgeAttention(input: { requestId: ID; expectedRevision: string; attentionId: ID }): Promise<Result<Envelope<AttentionItem>>>;
  getOperation(id: ID): Promise<Result<Operation>>;
  getOperationByRequest(requestId: ID): Promise<Result<Operation | null>>;
  subscribeRuns(input: {
    cursor: string;
    onEvent: (event: RunEvent) => void;
    onResetRequired: (problem: Problem) => void;
    onUnavailable: (problem: Problem) => void;
    onConnection: (state: Freshness) => void;
  }): () => void;
}

export type UnsupportedTargetMethod = 'getBackends'
  | 'planAction' | 'executeAction' | 'continueMaintenance'
  | 'cancelMaintenance' | 'subscribeGlobalEvents';

export type MockRequiredTargetFeature = 'overview-controls'
  | 'run-cancel' | 'group-routing-permissions-controls'
  | 'service-management-maintenance';

export type ArchitectureDomain = 'source' | 'processing' | 'action';
/** Any lowercase namespaced platform (`^[a-z][a-z0-9-]{1,31}$`) except URL schemes; bilibili/web are built in. */
export type ConnectorPlatform = 'bilibili' | 'web' | (string & {});
export type Platform = 'wechat' | ConnectorPlatform;
export type ArchitectureReason = '' | 'unsupported' | 'unavailable' | 'unverified'
  | 'policy_blocked' | 'expired' | 'canceled' | 'failed' | 'unknown' | 'upstream_reason_redacted';
export type ArchitectureEnforcement = 'hard' | 'soft' | 'host' | 'unverified';
export type ArchitectureProvenance = 'source_adapter' | 'agent_runtime' | 'action_adapter'
  | 'dispatcher_trace' | 'execution_catalog' | 'relay_health' | 'bridge_health' | 'console';
export type ArchitectureActionKind = 'reply_text' | 'edit_text' | 'delete_content' | 'thread_reply';

export interface ArchitectureCapability {
  id: string;
  supported: boolean;
  enforcement: ArchitectureEnforcement;
  source: ArchitectureProvenance;
  reason: ArchitectureReason;
}

export interface SourceRef {
  platform: Platform;
  accountId: ID;
  containerId: ID;
  containerLabel: string;
  contentId: ID;
  parentId: ID;
  rootId: ID;
}

export interface SourceView {
  platform: Platform;
  state: ServiceView['health'];
  availability?: Availability;
  accountId?: ID | null;
  containerIds?: ID[] | null;
  capabilities: ArchitectureCapability[];
}

export interface ProcessingView {
  agents: { id: ID }[];
  backends: {
    id: ID;
    kind: 'alma_personal' | 'text_only' | 'legacy_hermes' | 'agy_cli';
    available: boolean;
    modelIds: ID[];
    agentBinding: 'branch_only' | 'unverified';
    capabilities: ArchitectureCapability[];
  }[];
  capabilities: ArchitectureCapability[];
}

export interface ActionView {
  platform: Platform;
  kind: ArchitectureActionKind;
  state: ServiceView['health'];
  availability?: Availability;
  capabilities: ArchitectureCapability[];
}

export interface ArchitectureStream extends ArchitectureCapability {
  domain: ArchitectureDomain;
  streamId: ID | null;
  cursor: string | null;
  lastSeq: number | null;
  coverageSince: ISOTime | null;
}

export interface ArchitectureSnapshot {
  revision: string;
  observedAt: ISOTime;
  layers: ['source', 'processing', 'action'];
  sources: SourceView[];
  processing: ProcessingView;
  actions: ActionView[];
  eventStreams: ArchitectureStream[];
}

export interface ArchitectureEvent {
  eventId: ID;
  domain: ArchitectureDomain;
  kind: 'source_received' | 'run_changed' | 'action_changed';
  occurredAt: ISOTime;
  runId: ID;
  envelopeId: ID;
  source: SourceRef | null;
  action: ActionRef | null;
  phase: Phase;
  outcome: Outcome;
  certainty: 'observed' | 'unknown';
  reasonCode: ArchitectureReason;
}

// Current SSE implementation emits processing/run_changed only. Source/action
// domains return unsupported; their event shapes reserve the adapter boundary.
export type CurrentArchitectureEvent = ArchitectureEvent & {
  domain: 'processing';
  kind: 'run_changed';
};

export interface InboundReference {
  envelopeId: ID;
  dedupKey: ID;
  source: SourceRef;
  receivedAt?: ISOTime | null;
}

export interface ActionRef {
  actionId: ID;
  kind: ArchitectureActionKind;
  platform: Platform;
  idempotencyKey?: ID;
  admissionId?: ID;
  admittedAt?: ISOTime | null;
}

export interface ProcessingReceipt {
  status: 'not_started' | 'submitted' | 'confirmed' | 'failed' | 'unknown';
  evidence: DeliveryEvidence;
  receipt: ActionReceipt | null;
  missingReason: string | null;
}

export interface ActionReceipt {
  actionId: ID;
  platform: Platform;
  admission: { status: 'accepted' | 'rejected' | 'unknown'; admissionId: ID; recordedAt: ISOTime | null };
  status: ProcessingReceipt['status'];
  externalId: ID;
  submittedAt: ISOTime | null;
  submittedAtSource?: '' | 'bridge_ack' | 'echo_inferred';
  confirmedAt: ISOTime | null;
  confirmation?: { requestId: ID; echoMessageId: ID } | null;
  reasonCode: ArchitectureReason;
}

export type ArchitectureSSEFrame =
  | { event: 'architecture'; id: string; data: CurrentArchitectureEvent }
  | { event: 'reset_required' | 'unavailable'; data: Problem };

export interface ArchitectureSubscription {
  cursor: string;
  domain?: ArchitectureDomain;
  onEvent: (event: CurrentArchitectureEvent, cursor: string) => void;
  onResetRequired: (problem: Problem) => void;
  onUnavailable: (problem: Problem) => void;
  onConnection: (state: Freshness) => void;
}
