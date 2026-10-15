'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { scoutRampDiagnostic, controlCycleDiagnostic, discoveryDiagnostic } = require('../services/outboundControlDiagnostics');
const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
const cycleId = 'moc_13_1234567890abcdef';

test('ramp inventory is unknown even when legacy yield defaults are zero', () => {
  const result = scoutRampDiagnostic({ tenantId: '13', cycleId, admission: { discovered: 0 }, enrichment: { considered: 0 } });
  assert.equal(result.preCycleCleanInventory, null);
  assert.equal(result.postCycleCleanInventory, null);
  assert.equal(result.inventoryMeasured, false);
  assert.equal(result.admission.discovered, 0);
  assert.equal(result.admission.fit, null);
});
test('measured genuine zeros stay distinct from unavailable preparation or sends', () => {
  const event = { cycleId, cleanInventoryBefore: 0, cleanInventoryAfter: 0, pendingPrepared: 0, sentToday: 0 };
  assert.equal(controlCycleDiagnostic('13', event).cleanInventoryAfter, 0);
  assert.equal(controlCycleDiagnostic('13', event).pendingPrepared, null);
  assert.equal(controlCycleDiagnostic('13', event).sentToday, null);
  assert.equal(controlCycleDiagnostic('13', { ...event, pendingPreparedMeasured: true, sentTodayMeasured: true }).pendingPrepared, 0);
  assert.equal(controlCycleDiagnostic('13', { ...event, cleanInventoryAfter: NaN }).cleanInventoryAfter, null);
});
test('only fixed categories survive malicious provider payloads', () => {
  const secret = 'secret@example.com https://evil.test/?token=secret\nFORGED LOG';
  const result = { status: secret, errors: [{ code: 'REQUEST_DENIED', message: secret }], payload: { providerExecution: [{ name: secret, execution: { errors: [{ code: secret }] } }] } };
  const d = discoveryDiagnostic({ kind: 'investigated', result });
  assert.deepEqual(d, { kind: 'investigated', outcome: 'unknown', blocker: 'provider_denied' });
  assert.ok(!JSON.stringify(d).includes('secret'));
  const a = scoutRampDiagnostic({ tenantId: secret, cycleId: secret, admission: { decisions: [secret], discovered: secret } });
  assert.equal(a.tenantId, null); assert.equal(a.cycleId, null); assert.ok(!JSON.stringify(a).includes('secret'));
  assert.equal(discoveryDiagnostic({ result: { errors: [{ code: secret }] } }).blocker, 'unknown');
});
test('provider category and backoff are deterministic', () => {
  for (const [code, category] of [['OVER_QUERY_LIMIT', 'provider_quota'], ['provider_unavailable', 'provider_unavailable'], ['provider_error', 'provider_failed'], ['insufficient_coverage', 'insufficient_coverage'], ['invalid_target', 'invalid_target']]) {
    assert.equal(discoveryDiagnostic({ result: { errors: [{ code }] } }).blocker, category);
  }
  assert.equal(discoveryDiagnostic({ kind: 'backoff' }).blocker, 'backoff');
});
async function run(tenantId, failEvent = false, rawSentToday = 0, failEnvelope = false) {
  const logs = []; let persisted = false;
  const result = await runMaxOutboundControlLoop({
    cycleId: `moc_${tenantId}_1234567890abcdef`, tenantId,
    pool: { query: async () => { throw new Error('Unexpected database call'); } },
    store: { tenantId, clientId: Number(tenantId), envelope: async () => { if (failEnvelope) throw new Error('fixture read unavailable'); return null; }, event: async () => { if (failEvent) throw new Error('event_failure'); persisted = true; } },
    program: { id: 'outbound_fixture', tenant_id: tenantId, mode: 'active', policy: { dailyCap: 2 } },
    source: { id: 'fixture', payload: { structuredMission: { market: { segment: 'small_business_owners' }, geography: { region: 'United States', scope: 'nationwide' } } } },
    infrastructure: { cap: 2, snapshot: { sentToday: rawSentToday }, assessed: { governor: { outcome: 'proceed' } }, operating: { planningDailyCapacity: 2, dispatchCapacityNow: 2, authorizationLimitedCapacity: 2 } },
    inventory: { clean: [], excluded: [], exclusionCounts: {}, scope: {} },
    timestamps: {}, funnel: {}, execute: false,
    logger: { log: (label, value) => { assert.equal(persisted, true); logs.push([label, JSON.parse(value)]); } },
  });
  return { result, logs };
}
test('actual measured summary follows successful event write and stays tenant scoped', async () => {
  const [a, b] = await Promise.all([run('13'), run('10')]);
  for (const [data, tenant] of [[a, '13'], [b, '10']]) {
    assert.equal(data.logs.length, 1);
    const d = data.logs[0][1]; assert.equal(d.tenantId, tenant);
    assert.equal(d.cycleId, `moc_${tenant}_1234567890abcdef`);
    assert.equal(d.eventWriteCompleted, true); assert.equal(Object.hasOwn(d, 'eventPersisted'), false);
    assert.equal(d.cleanInventoryAfter, 0); assert.equal(d.pendingPrepared, 0); assert.equal(d.sentToday, 0);
    assert.equal(data.result.inventoryGrowth.postCycleCleanInventory, 0);
  }
});
test('failed durable event does not produce a success summary', async () => {
  await assert.rejects(run('13', true), /event_failure/);
});
test('logging failures cannot change execution and malformed numeric values remain unknown', () => {
  const { emitDiagnostic } = require('../services/outboundControlDiagnostics');
  assert.doesNotThrow(() => emitDiagnostic({ log() { throw new Error('logger unavailable'); } }, 'fixture', {}));
  const event = { pendingPreparedMeasured: true, sentTodayMeasured: true, pendingPrepared: Infinity, sentToday: '0', cleanInventoryBefore: -1, cleanInventoryAfter: 0.5 };
  const d = controlCycleDiagnostic('13', event);
  for (const field of ['pendingPrepared', 'sentToday', 'cleanInventoryBefore', 'cleanInventoryAfter']) assert.equal(d[field], null);
});

test('raw malformed sentToday never becomes measured zero after control coercion', async () => {
  for (const raw of [NaN, Infinity, -1, 0.5, '0', null]) {
    const { logs } = await run('13', false, raw);
    assert.equal(logs[0][1].sentToday, null);
  }
});

test('resolved no-op event write reports completion without claiming insertion', () => {
  const d = controlCycleDiagnostic('13', { cycleId });
  assert.equal(d.eventWriteCompleted, true);
  assert.equal(Object.hasOwn(d, 'eventPersisted'), false);
});

test('failed preparation read logs unknown without changing legacy fallback control count', async () => {
  const { result, logs } = await run('13', false, 0, true);
  assert.equal(result.pendingPrepared, 0);
  assert.equal(logs[0][1].pendingPrepared, null);
});
