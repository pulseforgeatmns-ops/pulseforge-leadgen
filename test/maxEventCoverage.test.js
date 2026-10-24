'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { REQUIRED_EVENT_TYPES, buildEventCoverageReport } = require('../utils/maxEventCoverage');

test('coverage keeps live, historical, and synthetic evidence separate', async () => {
  let call = 0;
  const db = { async query() {
    call++;
    if (call === 1) return { rows: [
      { client_id: 10, event_type: 'email_human_opened', provenance: 'live', first_observed_at: '2026-07-15T12:00:00Z', most_recent_observed_at: '2026-07-15T12:00:00Z', event_count: 1, successful_ingestion_count: 1, decision_count: 1, last_source_record_id: 'live-1' },
      { client_id: 10, event_type: 'email_human_opened', provenance: 'historical_backfill', first_observed_at: '2026-07-15T13:00:00Z', most_recent_observed_at: '2026-07-15T13:00:00Z', event_count: 2, successful_ingestion_count: 2, decision_count: 2, last_source_record_id: 'history-2' },
      { client_id: 10, event_type: 'email_unsubscribed', provenance: 'synthetic_smoke', first_observed_at: '2026-07-15T14:00:00Z', most_recent_observed_at: '2026-07-15T14:00:00Z', event_count: 1, successful_ingestion_count: 1, decision_count: 1, last_source_record_id: 'smoke-1' },
    ] };
    if (call === 2) return { rows: [{ client_id: 10, event_type: 'email_clicked', provenance: 'live', isolated_failure_count: 1 }] };
    return { rows: [{ client_id: 10, event_type: 'email_human_opened', provenance: 'historical_backfill', duplicate_count: 3 }] };
  } };
  const report = await buildEventCoverageReport({ clientId: 10 }, db);
  const live = report.rows.find(row => row.event_type === 'email_human_opened' && row.provenance === 'live');
  const historical = report.rows.find(row => row.event_type === 'email_human_opened' && row.provenance === 'historical_backfill');
  const synthetic = report.rows.find(row => row.event_type === 'email_unsubscribed' && row.provenance === 'synthetic_smoke');
  assert.equal(live.live_event_count, 1);
  assert.equal(historical.event_count, 2);
  assert.equal(historical.live_event_count, 0);
  assert.equal(historical.duplicate_count, 3);
  assert.equal(synthetic.event_count, 1);
  assert.equal(synthetic.live_event_count, 0);
  assert.equal(report.rows.find(row => row.event_type === 'email_clicked' && row.provenance === 'live').isolated_failure_count, 1);
  assert.ok(REQUIRED_EVENT_TYPES.every(type => report.rows.some(row => row.event_type === type && row.provenance === 'live')));
});
