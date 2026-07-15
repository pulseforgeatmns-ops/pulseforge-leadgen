const assert = require('node:assert/strict');
const test = require('node:test');
const {
  ingestNormalizedSignal,
  normalizeBrevoSignal,
  rileyReplyEventType,
  safeIngestEnrichmentOutcome,
  safeIngestIcpScoreChange,
  safeIngestNormalizedSignal,
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
});

test('safe ingestion failure never throws into the operational caller', async () => {
  const db = { async query() { throw new Error('orchestration unavailable'); } };
  const result = await safeIngestNormalizedSignal({
    client_id: 1, prospect_id: 'p', event_type: 'email_clicked', source: 'brevo', source_record_id: 'e1',
  }, { db });
  assert.equal(result.failed, true);
});
