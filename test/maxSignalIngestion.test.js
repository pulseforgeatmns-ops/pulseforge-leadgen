const assert = require('node:assert/strict');
const test = require('node:test');
const {
  ingestNormalizedSignal,
  normalizeBrevoSignal,
  rileyReplyEventType,
  safeIngestEnrichmentOutcome,
  safeIngestBrevoSignal,
  safeIngestIcpScoreChange,
  safeIngestNormalizedSignal,
  safeIngestRileyReplySignal,
} = require('../utils/maxSignalIngestion');

function memoryDb() {
  const signalKeys = new Set();
  const calls = [];
  return {
    calls,
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (/INSERT INTO prospect_signal_events/.test(sql)) {
        const id = params[0];
        if (signalKeys.has(id)) return { rows: [] };
        signalKeys.add(id);
        return { rows: [{ id, created_at: new Date() }] };
      }
      if (/SELECT id FROM max_decisions/.test(sql)) return { rows: [] };
      if (/SELECT id, vertical_tiers/.test(sql)) return { rows: [{ id: 1, vertical_tiers: {}, max_orchestration_config: {} }] };
      return { rows: [] };
    },
  };
}

const brevoResult = overrides => ({
  prospect_id: '1000c166-c9c3-4bab-adef-d4cbdf14ab18',
  client_id: 1,
  event_id: `evt-${overrides.event_type}-${overrides.open_source || 'none'}`,
  has_corresponding_send: true,
  ...overrides,
});

test('Brevo delivery, human/proxy/unknown opens, click, unsubscribe, and invalid normalize distinctly', () => {
  assert.equal(normalizeBrevoSignal(brevoResult({ event_type: 'delivered' }), {}).event_type, 'email_delivered');
  assert.equal(normalizeBrevoSignal(brevoResult({ event_type: 'opened', open_source: 'human' }), {}).event_type, 'email_human_opened');
  assert.equal(normalizeBrevoSignal(brevoResult({ event_type: 'opened', open_source: 'proxy' }), {}).event_type, 'email_proxy_opened');
  assert.equal(normalizeBrevoSignal(brevoResult({ event_type: 'opened', open_source: 'unknown' }), {}).event_type, 'email_unknown_opened');
  assert.equal(normalizeBrevoSignal(brevoResult({ event_type: 'clicked' }), {}).event_type, 'email_clicked');
  assert.equal(normalizeBrevoSignal(brevoResult({ event_type: 'unsubscribed' }), {}).event_type, 'email_unsubscribed');
  assert.equal(normalizeBrevoSignal(brevoResult({ event_type: 'invalid' }), {}).event_type, 'email_hard_bounced_confirmed_invalid');
});

test('Riley classifications preserve meaningful and OOO distinctions', () => {
  assert.equal(rileyReplyEventType('interested'), 'email_positive_reply');
  assert.equal(rileyReplyEventType('not_now'), 'email_meaningful_reply');
  assert.equal(rileyReplyEventType('negative'), 'email_negative_reply');
  assert.equal(rileyReplyEventType('unsubscribe'), 'email_unsubscribed');
  assert.equal(rileyReplyEventType('out_of_office'), 'email_out_of_office');
});

test('live Brevo unsubscribe canonicalizes epoch seconds once before persistence and evaluation', async () => {
  const db = memoryDb();
  let triggerTimestamp = null;
  const result = await safeIngestBrevoSignal(brevoResult({ event_type: 'unsubscribed' }), { ts: '1784131374' }, {
    db,
    evaluateProspectFn: async args => {
      triggerTimestamp = args.triggerEvent.event_timestamp;
      return { decision: { id: 'decision-unsubscribe' }, score: { score: 10 } };
    },
  });
  assert.equal(result.primary.failed, undefined);
  assert.ok(triggerTimestamp instanceof Date);
  assert.equal(triggerTimestamp.toISOString(), '2026-07-15T16:02:54.000Z');
  const insert = db.calls.find(call => /INSERT INTO prospect_signal_events/.test(call.sql));
  assert.ok(insert.params[5] instanceof Date);
  assert.notEqual(insert.params[5], '1784131374');
  assert.equal(JSON.parse(insert.params[8]).raw_source_timestamp, '1784131374');
});

test('Brevo epoch milliseconds and Riley ISO timestamps reach evaluators as canonical Dates', async () => {
  const db = memoryDb();
  const seen = [];
  const evaluateProspectFn = async args => {
    seen.push(args.triggerEvent.event_timestamp);
    return { decision: { id: `decision-${seen.length}` }, score: { score: 1 } };
  };
  await safeIngestBrevoSignal(brevoResult({ event_type: 'opened', open_source: 'human' }), { ts: 1784131374000 }, { db, evaluateProspectFn });
  await safeIngestRileyReplySignal({
    prospect: { id: brevoResult({}).prospect_id },
    email: { id: 'gmail-iso', date: '2026-07-15T12:02:54-04:00' },
    classification: 'interested', clientId: 1,
  }, { db, evaluateProspectFn });
  assert.deepEqual(seen.map(value => value.toISOString()), [
    '2026-07-15T16:02:54.000Z', '2026-07-15T16:02:54.000Z',
  ]);
});

test('invalid required timestamp is isolated, structured, and original caller can continue', async () => {
  const db = memoryDb();
  let originalHandlerCompleted = false;
  const result = await safeIngestBrevoSignal(brevoResult({ event_type: 'unsubscribed' }), { ts: 'not-a-date' }, { db });
  originalHandlerCompleted = true;
  assert.equal(result.primary.failed, true);
  assert.equal(result.primary.code, 'MAX_TIMESTAMP_INVALID');
  assert.equal(originalHandlerCompleted, true);
  assert.equal(db.calls.some(call => /INSERT INTO prospect_signal_events/.test(call.sql)), false);
  assert.equal(db.calls.some(call => /INSERT INTO max_decisions/.test(call.sql)), false);
  const failure = db.calls.find(call => /INSERT INTO agent_log/.test(call.sql));
  const payload = JSON.parse(failure.params[2]);
  assert.equal(payload.source, 'brevo');
  assert.equal(payload.raw_timestamp, 'not-a-date');
  assert.equal(payload.normalization_error_code, 'MALFORMED_TIMESTAMP');
  assert.equal(payload.original_handler_status, 'continues_after_isolated_max');
  assert.ok(payload.occurred_at);
});

test('duplicate normalized delivery is suppressed idempotently', async () => {
  const db = memoryDb();
  const signal = {
    client_id: 1,
    prospect_id: '1000c166-c9c3-4bab-adef-d4cbdf14ab18',
    event_type: 'email_delivered',
    event_timestamp: new Date(),
    source: 'brevo',
    source_record_id: 'delivery-1',
  };
  const first = await ingestNormalizedSignal(signal, { db, evaluate: false });
  const second = await ingestNormalizedSignal(signal, { db, evaluate: false });
  assert.equal(first.inserted, true);
  assert.equal(second.duplicate, true);
  assert.equal(db.calls.filter(call => /INSERT INTO prospect_signal_events/.test(call.sql)).length, 2);
  assert.ok(db.calls.some(call => /max_duplicate_events_suppressed_total/.test(JSON.stringify(call.params))));
});

test('meaningful signal invokes one shadow evaluator and records metrics', async () => {
  const db = memoryDb();
  let evaluations = 0;
  const transactionContext = { client: db, transactionManagedByCaller: true };
  const result = await ingestNormalizedSignal({
    client_id: 1,
    prospect_id: '1000c166-c9c3-4bab-adef-d4cbdf14ab18',
    event_type: 'email_positive_reply',
    event_timestamp: new Date(),
    source: 'riley_gmail',
    source_record_id: 'gmail-1',
  }, {
    db,
    transactionContext,
    evaluateProspectFn: async args => {
      evaluations++;
      assert.equal(args.db, db);
      assert.equal(args.transactionContext, transactionContext);
      return { decision: { id: 'decision-1', transition_recommended: true, actions: ['stop_automated_sequences'] } };
    },
  });
  assert.equal(evaluations, 1);
  assert.equal(result.evaluated, true);
  assert.ok(db.calls.some(call => call.params?.includes('signal_to_decision_duration')));
});

test('signal ingestion fails closed when transaction context does not match db', async () => {
  const db = memoryDb();
  await assert.rejects(ingestNormalizedSignal({
    client_id: 1,
    prospect_id: '1000c166-c9c3-4bab-adef-d4cbdf14ab18',
    event_type: 'email_positive_reply',
    source: 'test',
    source_record_id: 'mismatch',
  }, {
    db,
    transactionContext: { client: memoryDb(), transactionManagedByCaller: true },
  }), /same client as db/);
  assert.equal(db.calls.length, 0);
});

test('ICP and enrichment adapters emit traceable normalized records', async () => {
  const db = memoryDb();
  await safeIngestIcpScoreChange({
    prospectId: '1000c166-c9c3-4bab-adef-d4cbdf14ab18', clientId: 1,
    historyId: 99, oldScore: 50, newScore: 70, createdAt: new Date(),
  }, { db, evaluate: false });
  await safeIngestEnrichmentOutcome({
    prospectId: '1000c166-c9c3-4bab-adef-d4cbdf14ab18', clientId: 1,
    sourceRecordId: 'enrich-1', status: 'success',
    payload: { resolved: true, verified_email: true, phone_found: true, provider: 'test' },
  }, { db, evaluate: false });
  const eventTypes = db.calls.filter(call => /INSERT INTO prospect_signal_events/.test(call.sql)).map(call => call.params[4]);
  assert.deepEqual(eventTypes, ['icp_score_changed', 'enrichment_succeeded', 'phone_found', 'email_verified']);
  assert.ok(db.calls.filter(call => /INSERT INTO prospect_signal_events/.test(call.sql)).every(call => call.params[5] instanceof Date));
});

test('safe ingestion failure never throws into the operational caller', async () => {
  const db = { async query() { throw new Error('orchestration unavailable'); } };
  const result = await safeIngestNormalizedSignal({
    client_id: 1, prospect_id: 'p', event_type: 'email_clicked', source: 'brevo', source_record_id: 'e1',
  }, { db });
  assert.equal(result.failed, true);
});
