const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { mkdtempSync, rmSync } = require('node:fs');
const { tmpdir } = require('node:os');
const { join } = require('node:path');
const { after, test } = require('node:test');

const output = mkdtempSync(join(tmpdir(), 'jinx-architecture-client-'));
after(() => rmSync(output, { recursive: true, force: true }));
execFileSync('tsc', ['--strict', '--target', 'ES2020', '--module', 'commonjs', '--outDir', output,
  join(__dirname, 'client-contract.ts'), join(__dirname, 'architecture-client.ts')]);
const { createArchitectureClient } = require(join(output, 'architecture-client.js'));

class FakeEventSource {
  constructor(url) {
    this.url = url;
    this.listeners = new Map();
    this.closed = false;
    FakeEventSource.latest = this;
  }
  addEventListener(name, listener) { this.listeners.set(name, listener); }
  close() { this.closed = true; }
  emit(name, data, lastEventId = 'opaque-cursor') {
    this.listeners.get(name)({ data: JSON.stringify(data), lastEventId });
  }
}
global.EventSource = FakeEventSource;

const source = { platform: 'wechat', accountId: 'relay-default', containerId: 'group@chatroom',
  containerLabel: '', contentId: 'message-1', parentId: '', rootId: '' };
const action = { actionId: 'action-1', kind: 'reply_text', platform: 'wechat',
  admissionId: 'admission-1', idempotencyKey: 'dedup-1', admittedAt: '2026-11-20T00:00:00Z' };
const event = { eventId: 'event-1', domain: 'processing', kind: 'run_changed',
  occurredAt: '2026-11-20T00:00:00Z', runId: 'run-1', envelopeId: 'envelope-1', source, action,
  phase: 'submitting', outcome: 'pending', certainty: 'observed', reasonCode: '' };

function subscription() {
  const received = [], resets = [], unavailable = [], connections = [];
  const close = createArchitectureClient().subscribeArchitecture({ cursor: 'opaque+cursor/==', domain: 'processing',
    onEvent: (value, cursor) => received.push({ value, cursor }),
    onResetRequired: value => resets.push(value), onUnavailable: value => unavailable.push(value),
    onConnection: value => connections.push(value) });
  return { stream: FakeEventSource.latest, close, received, resets, unavailable, connections };
}

test('consumes actual nullable event references and preserves the opaque cursor', () => {
  const state = subscription();
  assert.equal(new URL(state.stream.url, 'http://localhost').searchParams.get('cursor'), 'opaque+cursor/==');
  state.stream.onopen();
  state.stream.emit('architecture', event, 'opaque+cursor/next==');
  state.stream.emit('architecture', { ...event, envelopeId: '', source: null, action: null });
  assert.deepEqual(state.received[0], { value: event, cursor: 'opaque+cursor/next==' });
  assert.equal(state.received[1].value.source, null);
  assert.equal(state.resets.length, 0);
  state.close();
  state.close();
  state.stream.emit('architecture', event);
  assert.equal(state.received.length, 2);
  assert.deepEqual(state.connections, ['fresh', 'disconnected']);
  assert.equal(state.stream.closed, true);
});

test('invalid references or state reset without advancing the cursor', () => {
  for (const invalid of [
    { ...event, source: { ...source, accountId: '/private/account' } },
    { ...event, action: { ...action, kind: 'unknown' } },
    { ...event, action: { ...action, admissionId: '/private/admission' } },
    { ...event, action: { ...action, admittedAt: 'invalid' } },
    { ...event, envelopeId: '/private/envelope' },
    { ...event, source: undefined },
    { ...event, phase: 'terminal', outcome: 'pending' },
    { ...event, domain: 'action', kind: 'action_changed' },
  ]) {
    const state = subscription();
    state.stream.emit('architecture', invalid);
    assert.equal(state.received.length, 0);
    assert.equal(state.resets[0].code, 'reset_required');
    assert.equal(state.stream.closed, true);
  }
});

test('reset, unavailable and disconnection close once and suppress later frames', () => {
  for (const kind of ['reset_required', 'unavailable', 'network']) {
    const state = subscription();
    if (kind === 'network') state.stream.onerror();
    else state.stream.emit(kind, { code: kind });
    state.stream.onerror();
    state.stream.emit('architecture', event);
    assert.equal(state.stream.closed, true);
    assert.equal(state.received.length, 0);
    assert.equal(state.resets.length + state.unavailable.length, 1);
    assert.deepEqual(state.connections, ['disconnected']);
  }
});
