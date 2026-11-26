import type {
  ArchitectureSnapshot, ArchitectureSubscription, CurrentArchitectureEvent, Problem, Result,
} from './client-contract';

export interface ArchitectureClient {
  getArchitecture(): Promise<Result<ArchitectureSnapshot>>;
  subscribeArchitecture(input: ArchitectureSubscription): () => void;
}

function problem(code: 'reset_required' | 'unavailable', message: string): Problem {
  return { code, message, requestId: '', retry: 'read' };
}

function identity(value: unknown, optional = false): value is string {
  return typeof value === 'string' && ((optional && value === '') || /^[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,255}$/.test(value));
}

const reservedPlatforms = new Set(['wechat', 'http', 'https', 'ws', 'wss', 'file']);

function platform(value: unknown): boolean {
  if (value === 'wechat') return true;
  return typeof value === 'string' && /^[a-z][a-z0-9-]{1,31}$/.test(value) && !reservedPlatforms.has(value);
}

function sourceReference(value: unknown): boolean {
  if (value === null) return true;
  if (typeof value !== 'object') return false;
  const source = value as Record<string, unknown>;
  return platform(source.platform) && identity(source.accountId) && identity(source.containerId)
    && identity(source.contentId, true) && identity(source.parentId, true) && identity(source.rootId, true)
    && (!(source.parentId || source.rootId) || source.contentId !== '')
    && typeof source.containerLabel === 'string' && [...source.containerLabel].length <= 128
    && !/[\u0000-\u001f\u007f-\u009f]/.test(source.containerLabel);
}

function actionReference(value: unknown): boolean {
  if (value === null) return true;
  if (typeof value !== 'object') return false;
  const action = value as Record<string, unknown>;
  return platform(action.platform) && identity(action.actionId)
    && (action.idempotencyKey === undefined || identity(action.idempotencyKey, true))
    && (action.admissionId === undefined || identity(action.admissionId, true))
    && (action.admittedAt === undefined || action.admittedAt === null
      || (typeof action.admittedAt === 'string' && Number.isFinite(Date.parse(action.admittedAt))))
    && typeof action.kind === 'string' && ['reply_text', 'edit_text', 'delete_content', 'thread_reply'].includes(action.kind);
}

function currentEvent(value: unknown): value is CurrentArchitectureEvent {
  if (typeof value !== 'object' || value === null) return false;
  const event = value as Record<string, unknown>;
  const phases = ['accepted', 'aggregating', 'gating', 'queued', 'executing', 'checking',
    'submitting', 'confirming', 'terminal', 'unknown'];
  const outcomes = ['pending', 'replied', 'silent', 'blocked', 'suppressed', 'expired',
    'canceled', 'failed', 'unknown'];
  const reasons = ['', 'unsupported', 'unavailable', 'unverified', 'policy_blocked',
    'expired', 'canceled', 'failed', 'unknown', 'upstream_reason_redacted'];
  const stateValid = event.outcome === 'pending' ? event.phase !== 'terminal' && event.phase !== 'unknown'
    : event.outcome === 'unknown' ? event.phase === 'unknown' : event.phase === 'terminal';
  return identity(event.eventId) && identity(event.runId)
    && typeof event.occurredAt === 'string' && Number.isFinite(Date.parse(event.occurredAt))
    && event.domain === 'processing' && event.kind === 'run_changed'
    && identity(event.envelopeId, true) && sourceReference(event.source) && actionReference(event.action)
    && typeof event.phase === 'string' && phases.includes(event.phase)
    && typeof event.outcome === 'string' && outcomes.includes(event.outcome)
    && stateValid
    && (event.certainty === 'observed' || event.certainty === 'unknown')
    && typeof event.reasonCode === 'string' && reasons.includes(event.reasonCode);
}

export function createArchitectureClient(): ArchitectureClient {
  return {
    async getArchitecture() {
      const response = await fetch('/console/v1/architecture', { credentials: 'same-origin', cache: 'no-store' });
      return response.json() as Promise<Result<ArchitectureSnapshot>>;
    },
    subscribeArchitecture(input) {
      const query = new URLSearchParams({ cursor: input.cursor });
      if (input.domain) query.set('domain', input.domain);
      const stream = new EventSource(`/console/v1/architecture/events?${query}`);
      let closed = false;
      const close = () => {
        if (closed) return;
        closed = true;
        stream.close();
        input.onConnection('disconnected');
      };
      stream.onopen = () => { if (!closed) input.onConnection('fresh'); };
      stream.addEventListener('architecture', (frame) => {
        if (closed) return;
        const message = frame as MessageEvent<string>;
        let value: unknown;
        try { value = JSON.parse(message.data); } catch { value = null; }
        if (!currentEvent(value) || !message.lastEventId) {
          close();
          input.onResetRequired(problem('reset_required', 'Invalid architecture event; read architecture again'));
          return;
        }
        input.onEvent(value, message.lastEventId);
      });
      stream.addEventListener('reset_required', () => {
        if (closed) return;
        close();
        input.onResetRequired(problem('reset_required', 'Stream position expired; read architecture again'));
      });
      stream.addEventListener('unavailable', () => {
        if (closed) return;
        close();
        input.onUnavailable(problem('unavailable', 'Architecture stream is unavailable'));
      });
      stream.onerror = () => {
        if (closed) return;
        close();
        input.onUnavailable(problem('unavailable', 'Read architecture before reconnecting'));
      };
      return close;
    },
  };
}
