'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { parseArgs, run } = require('../scripts/configureMaxReadiness');

test('Anchor readiness config records evidence while forcing Phase 3 allowlist false', async () => {
  const options = parseArgs(['--client-id=10','--updated-by=test']);
  let captured;
  const db = { async query(sql, params) {
    captured = { sql, params };
    return { rows: [{ client_id: 10, phase3_allowlisted: false, minimum_total_reviews: 100, recovery_snapshot_verified: false }] };
  } };
  const row = await run(options, db);
  assert.equal(row.phase3_allowlisted, false);
  assert.equal(row.minimum_total_reviews, 100);
  assert.equal(row.recovery_snapshot_verified, false);
  assert.match(captured.sql, /phase3_allowlisted=FALSE/);
  assert.match(captured.sql, /terminal_review_requirement='every'/);
});
