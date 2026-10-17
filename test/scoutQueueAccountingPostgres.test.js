'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { ELIGIBILITY_SQL } = require('../services/scoutQueueAccounting');

test('queue eligibility matches actual PostgreSQL selector at null and microsecond boundaries', async t => {
  const pg = await startDisposablePostgres('scout-queue-accounting-');
  const db = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 1 });
  t.after(() => closePostgresFixture(db, pg));
  const { rows } = await db.query(`WITH u(label, vertical, enrichment_attempts, last_attempt_at, expected) AS (VALUES
    ('null attempts', 'painting', NULL::integer, NULL::timestamptz, 'eligible_now'),
    ('two null', 'painting', 2, NULL, 'eligible_now'),
    ('three null', 'painting', 3, NULL, 'attempt_limit_hold'),
    ('before hour', 'painting', 2, NOW()-INTERVAL '1 hour'+INTERVAL '1 microsecond', 'retry_cooldown'),
    ('at hour', 'painting', 2, NOW()-INTERVAL '1 hour', 'eligible_now'),
    ('after hour', 'painting', 2, NOW()-INTERVAL '1 hour'-INTERVAL '1 microsecond', 'eligible_now'),
    ('before week', 'painting', 3, NOW()-INTERVAL '7 days'+INTERVAL '1 microsecond', 'attempt_limit_hold'),
    ('at week', 'painting', 3, NOW()-INTERVAL '7 days', 'eligible_now'),
    ('after week', 'painting', 3, NOW()-INTERVAL '7 days'-INTERVAL '1 microsecond', 'eligible_now'),
    ('unsupported', 'Painting', 3, NOW(), 'unsupported_vertical'),
    ('null vertical', NULL, 0, NULL, 'unsupported_vertical'),
    ('overlap', 'painting', 3, NOW(), 'attempt_limit_hold')
  ) SELECT label, expected, ${ELIGIBILITY_SQL.replace('$5', '$1')} AS bucket,
    ((COALESCE(enrichment_attempts,0)<3 OR last_attempt_at<=NOW()-INTERVAL '7 days')
      AND COALESCE(last_attempt_at,NOW()-INTERVAL '100 years')<=NOW()-INTERVAL '1 hour'
      AND vertical=ANY($1::text[])) IS TRUE AS selector_eligible
    FROM u`, [['painting']]);
  for (const row of rows) {
    assert.equal(row.bucket, row.expected, row.label);
    assert.equal(row.bucket === 'eligible_now', row.selector_eligible, row.label);
  }
});
