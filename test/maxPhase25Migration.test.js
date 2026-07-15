'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { MIGRATIONS, inspectMigrationSafety, migrationSql } = require('../scripts/validateMaxMigrations');

test('migration order preserves Phase 1, Phase 2, Phase 2.5, then shadow hardening', () => {
  assert.deepEqual(MIGRATIONS, [
    '2026-07-15-max-prospect-orchestration-v1.sql',
    '2026-07-15-max-prospect-orchestration-phase2.sql',
    '2026-07-15-max-prospect-orchestration-phase2_5.sql',
    '2026-07-15-max-shadow-operations-hardening.sql',
  ]);
});

test('all Max migrations are status-safe and Phase 2.5 rerun operations are guarded', () => {
  for (const filename of MIGRATIONS) assert.equal(inspectMigrationSafety(migrationSql(filename), filename).safe, true);
  const phase25 = fs.readFileSync(path.join(__dirname,'../migrations/2026-07-15-max-prospect-orchestration-phase2_5.sql'),'utf8');
  assert.match(phase25,/DROP CONSTRAINT IF EXISTS prospect_signal_events_client_fk/);
  assert.match(phase25,/CREATE TABLE IF NOT EXISTS max_recommendation_reviews/);
  assert.doesNotMatch(phase25,/UPDATE\s+prospects/i);
});
