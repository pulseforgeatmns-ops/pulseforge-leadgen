'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { admissionReceiptDiagnostic, providerReceiptDiagnostic, scoutRampDiagnostic, controlCycleDiagnostic, discoveryDiagnostic } = require('../services/outboundControlDiagnostics');
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
async function run(tenantId, failEvent = false, rawSentToday = 0, failEnvelope = false, discovery, admissionFixture = {}, exclusionCounts = {}, postInventory = {}) {
  const logs = []; let persisted = false; let event;
  const result = await runMaxOutboundControlLoop({
    cycleId: `moc_${tenantId}_1234567890abcdef`, tenantId,
    pool: { query: async () => { throw new Error('Unexpected database call'); } },
    store: { tenantId, clientId: Number(tenantId), envelope: async () => { if (failEnvelope) throw new Error('fixture read unavailable'); return null; }, event: async (_type, _key, payload) => { event = payload; if (failEvent) throw new Error('event_failure'); persisted = true; } },
    program: { id: 'outbound_fixture', tenant_id: tenantId, mode: 'active', policy: { dailyCap: 2 } },
    source: { id: 'fixture', payload: { structuredMission: { market: { segment: 'small_business_owners' }, geography: { region: 'United States', scope: 'nationwide' } } } },
    infrastructure: { cap: 2, snapshot: { sentToday: rawSentToday }, assessed: { governor: { outcome: 'proceed' } }, operating: { planningDailyCapacity: 2, dispatchCapacityNow: 2, authorizationLimitedCapacity: 2 } },
    inventory: { clean: [], excluded: [], exclusionCounts, scope: {} },
    timestamps: {}, funnel: {}, execute: discovery ? true : false,
    ...(discovery ? { scoutRamp: async () => ({ discovery, ...admissionFixture }), inventoryAfter: { clean: [], excluded: [], exclusionCounts, scope: {}, ...postInventory } } : {}),
    logger: { log: (label, value) => { assert.equal(persisted, true); logs.push([label, JSON.parse(value)]); } },
  });
  return { result, logs, event };
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


test('provider receipts preserve unknown versus zero and never infer network from executed', () => {
  const unknown = providerReceiptDiagnostic('13', cycleId, null);
  assert.equal(unknown.reportsAvailable, false);
  assert.deepEqual(unknown.providers, []);
  const p = providerReceiptDiagnostic('13', cycleId, { result: { payload: { providerExecution: [{ providerId: 'google_maps', status: 'failed', rawResultCount: 0, execution: { executed: true, queries: [{ googleStatus: 'REQUEST_DENIED', httpStatus: 200, resultCount: 0 }] } }] } } }).providers[0];
  assert.equal(p.rawResultCount, 0); assert.equal(p.mappedCandidateCount, null);
  assert.equal(p.queries[0].origin, 'unknown'); assert.equal(p.queries[0].cacheHit, null);
});
test('provider receipt bounds and sanitizes malicious fields and explicit cache origins', () => {
  const secret = 'secret@example.com https://evil.test/token';
  const report = { providerId: secret, status: secret, rawResultCount: -1, executedAt: secret, error: secret, execution: { executed: secret, queries: Array.from({ length: 25 }, (_, i) => ({ query: secret, googleError: secret, googleStatus: secret, httpStatus: 900, resultCount: NaN, cacheHit: i === 0 ? true : false })), errors: [{ code: secret, message: secret }] } };
  const result = providerReceiptDiagnostic('13', cycleId, { result: { startedAt: secret, payload: { providerExecution: Array(25).fill(report) } } });
  assert.equal(result.providers.length, 20); assert.equal(result.reportsTruncated, true);
  assert.equal(result.providers[0].queries.length, 20); assert.equal(result.providers[0].queriesTruncated, true);
  assert.equal(result.providers[0].queries[0].origin, 'cache');
  assert.equal(result.providers[0].queries[1].origin, 'uncached');
  assert.equal(result.providers[0].rawResultCount, null);
  assert.ok(!JSON.stringify(result).includes('secret'));
});
test('actual control event durably includes sanitized tenant/cycle provider receipt', async () => {
  const discovery = { kind: 'investigated', result: { status: 'partial', startedAt: '2026-10-15T15:45:10.000Z', errors: [{ code: 'insufficient_coverage', message: 'private' }], payload: { providerExecution: [{ providerId: 'google_maps', status: 'failed', execution: { errors: [{ code: 'google_places_status_REQUEST_DENIED', message: 'private' }] } }] } } };
  const { event } = await run('13', false, 0, false, discovery, { admissionMeasured: true, admission: { evaluated: 92, fit: 0, rejected: { outside_geography: 92 } } });
  assert.equal(event.scoutAdmissionReceipt.measured, true);
  assert.equal(event.scoutAdmissionReceipt.tenantId, '13');
  assert.equal(event.scoutAdmissionReceipt.cycleId, cycleId);
  assert.equal(event.scoutAdmissionReceipt.rejected.outside_geography, 92);
  assert.equal(event.scoutAdmissionReceipt.recordedRejectedTotal, 92);
  assert.equal(event.scoutProviderReceipt.tenantId, '13');
  assert.equal(event.scoutProviderReceipt.cycleId, cycleId);
  assert.equal(event.scoutProviderReceipt.providers[0].errors[0].code, 'google_places_status_REQUEST_DENIED');
  assert.ok(!JSON.stringify(event.scoutProviderReceipt).includes('private'));
  const other = providerReceiptDiagnostic('10', 'moc_10_1234567890abcdef', { kind: 'backoff' });
  assert.equal(other.tenantId, '10'); assert.deepEqual(other.providers, []);
});

test('canonical SPEC-184 provider counts and empty status survive projection', () => {
  const { normalizeProviderExecutionRecord } = require('../packages/scout/coverage/ProviderExecution');
  const canonical = normalizeProviderExecutionRecord({ providerId: 'google_maps', status: 'empty', rawResultCount: 0, mappedCandidateCount: 0, execution: { executed: true, totals: { queries: 1, results: 0 }, queries: [{ googleStatus: 'ZERO_RESULTS', httpStatus: 200 }] } });
  const p = providerReceiptDiagnostic('13', cycleId, { result: { payload: { providerExecution: [canonical] } } }).providers[0];
  assert.equal(p.rawResultCount, 0); assert.equal(p.mappedCandidateCount, 0);
  assert.equal(p.status, 'empty'); assert.equal(p.queries[0].origin, 'unknown');
});

test('admission receipt preserves unavailable versus measured zero and reconciles recorded reasons', () => {
  const { createReplenishmentAdmissionCounters } = require('../utils/replenishmentVertical');
  const a = createReplenishmentAdmissionCounters();
  const unavailable = admissionReceiptDiagnostic('13', cycleId, a, false);
  assert.equal(unavailable.counts.evaluated, null); assert.equal(unavailable.rejected.outside_geography, null);
  assert.equal(unavailable.recordedRejectedTotal, null);
  a.discovered = 92; a.evaluated = 92; a.rejected.outside_geography = 90; a.rejected.segment_mismatch = 2;
  const measured = admissionReceiptDiagnostic('13', cycleId, a, true);
  assert.equal(measured.recordedRejectedTotal, 92); assert.equal(measured.counts.fit, 0);
  assert.equal(measured.rejected.outside_geography, 90); assert.equal(measured.unrecognizedReasonCount, 0);
  a.rejected['secret@example.com'] = 3;
  const unknownReason = admissionReceiptDiagnostic('13', cycleId, a, true);
  assert.equal(unknownReason.recordedRejectedTotal, 95); assert.equal(unknownReason.unrecognizedReasonCount, 3);
  assert.ok(!JSON.stringify(unknownReason).includes('secret'));
  a.rejected.segment_mismatch = 'private';
  assert.equal(admissionReceiptDiagnostic('13', cycleId, a, true).recordedRejectedTotal, null);
});
test('actual Scout ramp marks admission measured only when persistence callback completed', async () => {
  const { defaultScoutRamp } = require('../services/maxOutboundControlLoop');
  const program = { id: 'fixture', tenant_id: '13', policy: {} };
  const source = { payload: { structuredMission: { market: { segment: 'small_business_owner' }, geography: { region: 'United States', scope: 'nationwide', cities: [] } } } };
  for (const invoke of [false, true]) {
    const result = await defaultScoutRamp({ cycleId, pool: { query: async () => { throw Error('unexpected DB'); } }, store: { tenantId: '13', clientId: 13 }, program, source, plan: { deficit: 1 }, skipVerificationRetry: true, enrichment: { run: async () => ({ considered: 0 }) }, recoverExistingInventory: async () => ({}), loadDiscoveryBackoff: async () => null, runDiscovery: async (_input, options) => { if (invoke) await options.persistCompanies({ companies: [] }); return { kind: 'investigated', result: { status: 'partial' } }; }, logger: { log() {} } });
    assert.equal(result.admissionMeasured, invoke);
    assert.equal(admissionReceiptDiagnostic('13', cycleId, result.admission, result.admissionMeasured).counts.evaluated, invoke ? 0 : null);
  }
});

test('natural control logs expose only the sanitized provider receipt after event write', async () => {
  const privateText = 'secret@example.com https://private.test/?token=secret';
  const discovery = { kind: 'investigated', result: { status: 'partial', payload: { providerExecution: [{
    providerId: 'google_maps', status: 'failed', execution: { queries: [{
      query: privateText, googleStatus: 'REQUEST_DENIED', googleError: privateText,
      httpStatus: 200, resultCount: 0, cacheHit: true, cacheStrategy: 'memory_ttl',
    }] },
  }] } } };
  const { logs, event } = await run('13', false, 0, false, discovery);
  const receipts = logs.filter(([label]) => label === '[max-outbound-control] Scout provider receipt');
  assert.equal(receipts.length, 1);
  assert.deepEqual(receipts[0][1], event.scoutProviderReceipt);
  assert.equal(receipts[0][1].providers[0].queries[0].origin, 'cache');
  assert.ok(!JSON.stringify(logs).includes('secret'));
  await assert.rejects(run('13', true, 0, false, discovery), /event_failure/);
  const malformed = structuredClone(discovery);
  malformed.result.payload.providerExecution[0].execution.queries[0].cacheStrategy = privateText;
  assert.equal(providerReceiptDiagnostic('13', cycleId, malformed).providers[0].queries[0].cacheStrategy, 'unknown');
});

test('natural control logs measured admission rejection buckets after the durable event write', async () => {
  const privateText = 'secret@example.com https://private.test/?token=secret';
  const admission = {
    discovered: 92, evaluated: 92, fit: 0, admittedToEnrichment: 0,
    rejected: { outside_geography: 70, segment_mismatch: 15, unclassifiable_vertical: 7, [privateText]: 0 },
    decisions: [{ name: privateText, domain: privateText, reason: privateText }],
  };
  const discovery = { kind: 'investigated', result: { status: 'completed' } };
  for (const tenant of ['10', '13']) {
    const fixture = { admission, admissionMeasured: true };
    const { logs, event } = await run(tenant, false, 0, false, discovery, fixture);
    const receipts = logs.filter(([label]) => label === '[max-outbound-control] Scout admission receipt');
    assert.equal(receipts.length, 1);
    assert.deepEqual(receipts[0][1], event.scoutAdmissionReceipt);
    assert.equal(receipts[0][1].tenantId, tenant);
    assert.equal(receipts[0][1].cycleId, `moc_${tenant}_1234567890abcdef`);
    assert.equal(receipts[0][1].measured, true);
    assert.equal(receipts[0][1].recordedRejectedTotal, 92);
    assert.equal(receipts[0][1].rejected.segment_mismatch, 15);
    assert.equal(receipts[0][1].counts.fit, 0);
    assert.ok(!JSON.stringify(logs).includes('secret'));
    await assert.rejects(run(tenant, true, 0, false, discovery, fixture), /event_failure/);
  }
});

test('natural admission receipt logs unavailable data as unknown, never measured zero', async () => {
  const discovery = { kind: 'backoff' };
  const { logs } = await run('10', false, 0, false, discovery, {
    admission: { discovered: 0, evaluated: 0, fit: 0, rejected: {} }, admissionMeasured: false,
  });
  const receipt = logs.find(([label]) => label === '[max-outbound-control] Scout admission receipt')?.[1];
  assert.ok(receipt);
  assert.equal(receipt.measured, false);
  assert.equal(receipt.counts.evaluated, null);
  assert.equal(receipt.recordedRejectedTotal, null);
});

test('provider receipt reports bounded cache freshness and rejects malformed age metadata', () => {
  const receipt = value => providerReceiptDiagnostic('10', 'moc_10_1234567890abcdef', {
    result: { payload: { providerExecution: [{ providerId: 'google_maps', execution: { queries: [value] } }] } },
  }).providers[0].queries[0];
  const historical = receipt({ cacheHit: true, googleStatus: 'REQUEST_DENIED', cacheAgeMs: 60000, cacheExpiresInMs: 840000 });
  assert.equal(historical.origin, 'cache');
  assert.equal(historical.cacheAgeMs, 60000);
  assert.equal(historical.cacheExpiresInMs, 840000);
  const live = receipt({ cacheHit: false, googleStatus: 'OK' });
  assert.equal(live.origin, 'uncached'); assert.equal(live.cacheAgeMs, null);
  for (const value of ['secret@example.com', -1, Infinity, 1.5]) {
    const invalid = receipt({ cacheAgeMs: value, cacheExpiresInMs: value });
    assert.equal(invalid.cacheAgeMs, null); assert.equal(invalid.cacheExpiresInMs, null);
    assert.ok(!JSON.stringify(invalid).includes('secret'));
  }
});


test('clean exclusion projection is bounded, numeric and distinct from unavailable', () => {
  const project = value => controlCycleDiagnostic('13', { cycleId, cleanInventoryExclusions: value }).cleanInventoryExclusions;
  for (const value of [undefined, null, [], 'private@example.test']) {
    const d = project(value);
    assert.equal(d.available, false);
    assert.equal(d.counts.business_fit, null);
    assert.equal(d.recordedExcludedTotal, null);
  }
  const empty = project({});
  assert.equal(empty.available, true);
  assert.equal(empty.recordedExcludedTotal, 0);
  assert.equal(empty.counts.business_fit, 0);
  const secret = 'private@example.test https://private.test/token\nFORGED';
  const value = { business_fit: 8, contact_classification_not_authorized: 2, [secret]: 3 };
  const d = project(value);
  assert.equal(d.scope, 'loaded_inventory_first_failure');
  assert.equal(d.recordedExcludedTotal, 13);
  assert.equal(d.unrecognizedReasonCount, 3);
  assert.equal(d.counts.contact_classification_not_authorized, 2);
  assert.ok(!JSON.stringify(d).includes('private'));
  assert.ok(JSON.stringify(d).length < 2500);
  for (const invalid of [NaN, Infinity, -1, 0.5, '2', { email: secret }]) {
    const malformed = project({ business_fit: invalid });
    assert.equal(malformed.counts.business_fit, null);
    assert.equal(malformed.recordedExcludedTotal, null);
    assert.equal(malformed.unrecognizedReasonCount, null);
    assert.ok(!JSON.stringify(malformed).includes('private'));
  }
  assert.equal(project({ business_fit: Number.MAX_SAFE_INTEGER, email_not_verified: 1 }).recordedExcludedTotal, null);
});

test('persisted post-ramp exclusions reach the measured summary without prospect details', async () => {
  const exclusions = { contact_classification_not_authorized: 2, business_fit: 7, 'private@example.test': 1 };
  const discovery = { kind: 'investigated', result: { status: 'completed' } };
  const { logs, event, result } = await run('13', false, 0, false, discovery, { promoted: 2 }, exclusions);
  const d = logs.find(([label]) => label === '[max-outbound-control] measured summary')[1];
  assert.equal(d.tenantId, '13');
  assert.equal(d.cycleId, cycleId);
  assert.equal(d.cleanInventoryAfter, 0);
  assert.equal(d.cleanInventoryExclusions.counts.contact_classification_not_authorized, 2);
  assert.equal(d.cleanInventoryExclusions.recordedExcludedTotal, 10);
  assert.equal(d.cleanInventoryExclusions.unrecognizedReasonCount, 1);
  assert.deepEqual(event.cleanInventoryExclusions, exclusions);
  assert.deepEqual(result.cleanInventoryExclusions, exclusions);
  assert.ok(!JSON.stringify(d).includes('private'));
});


test('exact promotion cohort survives the real control loop into sanitized post-reload summary', async () => {
  const { retainPromotionIds } = require('../services/scoutPromotionCohort');
  const batches = [retainPromotionIds({ promoted: 2, recovered: 0 }, ['a1b2c3d4-1111-4222-8333-123456789abc', 'b2c3d4e5-2222-4333-8444-23456789abcd'])];
  const { logs, event } = await run('13', false, 0, false,
    { kind: 'investigated', result: { status: 'completed' } },
    { promoted: 2, enrichmentBatches: batches }, { business_fit: 40, contact_classification_not_authorized: 1 },
    { excluded: [{ prospectId: 'a1b2c3d4-1111-4222-8333-123456789abc', reason: 'contact_classification_not_authorized' },
      { prospectId: '999', reason: 'business_fit' }] });
  const receipt = logs.find(([label]) => label === '[max-outbound-control] measured summary')[1];
  const cohort = receipt.scoutPromotionCleanCohort;
  assert.equal(cohort.available, true);
  assert.equal(cohort.outcomes, 2);
  assert.equal(cohort.clean, 0);
  assert.equal(cohort.unmatched, 1);
  assert.equal(cohort.exclusions.counts.contact_classification_not_authorized, 1);
  assert.equal(cohort.exclusions.counts.business_fit, 0);
  assert.equal(receipt.cleanInventoryExclusions.counts.business_fit, 40);
  assert.ok(!JSON.stringify(receipt).includes('123456789abc'));
  assert.ok(!JSON.stringify(event.scoutPromotionCleanCohort).includes('123456789abc'));
});
