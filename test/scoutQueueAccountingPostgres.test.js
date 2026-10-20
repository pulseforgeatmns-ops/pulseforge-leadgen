'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { _test: { ENRICHMENT_ELIGIBILITY_SQL } } = require('../scoutUnenrichedEnrichmentAgent');
const { ELIGIBILITY_SQL, RETRY_AFTER_SQL } = require('../services/scoutQueueAccounting');

test('queue eligibility matches actual PostgreSQL selector at null and microsecond boundaries', async t => {
  const pg = await startDisposablePostgres('scout-queue-accounting-');
  const db = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 1 });
  t.after(() => closePostgresFixture(db, pg));
  const { rows } = await db.query(`WITH u(label, vertical, enrichment_attempts, last_attempt_at, expected) AS (VALUES
    ('null attempts', 'painting', NULL::integer, NULL::timestamptz, 'eligible_now'),
    ('two null', 'painting', 2, NULL, 'eligible_now'),
    ('three null', 'painting', 3, NULL, 'eligible_now'),
    ('before hour', 'painting', 2, NOW()-INTERVAL '1 hour'+INTERVAL '1 microsecond', 'retry_cooldown'),
    ('at hour', 'painting', 2, NOW()-INTERVAL '1 hour', 'eligible_now'),
    ('after hour', 'painting', 2, NOW()-INTERVAL '1 hour'-INTERVAL '1 microsecond', 'eligible_now'),
    ('three before day', 'painting', 3, NOW()-INTERVAL '24 hours'+INTERVAL '1 microsecond', 'retry_cooldown'),
    ('three at day', 'painting', 3, NOW()-INTERVAL '24 hours', 'eligible_now'),
    ('five before day', 'painting', 5, NOW()-INTERVAL '24 hours'+INTERVAL '1 microsecond', 'retry_cooldown'),
    ('five at day', 'painting', 5, NOW()-INTERVAL '24 hours', 'eligible_now'),
    ('six null', 'painting', 6, NULL, 'eligible_now'),
    ('six before week', 'painting', 6, NOW()-INTERVAL '7 days'+INTERVAL '1 microsecond', 'attempt_limit_hold'),
    ('six at week', 'painting', 6, NOW()-INTERVAL '7 days', 'eligible_now'),
    ('six after week', 'painting', 6, NOW()-INTERVAL '7 days'-INTERVAL '1 microsecond', 'eligible_now'),
    ('unsupported', 'Painting', 6, NOW(), 'unsupported_vertical'),
    ('null vertical', NULL, 0, NULL, 'unsupported_vertical'),
    ('null vertical held', NULL, 6, NOW(), 'unsupported_vertical'),
    ('unsupported cooldown', 'Painting', 3, NOW(), 'unsupported_vertical'),
    ('overlap', 'painting', 6, NOW(), 'attempt_limit_hold')
  ) SELECT label, expected, ${ELIGIBILITY_SQL} AS bucket,
    (${ENRICHMENT_ELIGIBILITY_SQL}) = 'eligible_now' AS selector_eligible
    FROM u WHERE $1::integer = 10 AND $4::integer = 5`, [10, 6, 1, 5, ['painting'], 24, 168]);
  for (const row of rows) {
    assert.equal(row.bucket, row.expected, row.label);
    assert.equal(row.bucket === 'eligible_now', row.selector_eligible, row.label);
  }
});

test('production selector honors configured cooldowns and stricter attempt ceilings', async t => {
  const pg = await startDisposablePostgres('scout-retry-policy-');
  const db = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 1 });
  t.after(() => closePostgresFixture(db, pg));
  const { rows } = await db.query(`WITH u(label, vertical, enrichment_attempts, last_attempt_at, expected) AS (VALUES
    ('configured early cooldown', 'painting', 1, NOW()-INTERVAL '24 hours', false),
    ('configured early boundary', 'painting', 1, NOW()-INTERVAL '7 days', true),
    ('strict max holds early tier', 'painting', 2, NOW()-INTERVAL '24 hours', false),
    ('strict max boundary', 'painting', 2, NOW()-INTERVAL '7 days', true),
    ('unrestricted vertical preserved', NULL, 0, NULL, true)
  ) SELECT label, expected, (${ENRICHMENT_ELIGIBILITY_SQL}) = 'eligible_now' AS eligible
    FROM u WHERE $1::integer = 10 AND $4::integer = 5`, [10, 2, 168, 5, null, 24, 168]);
  for (const row of rows) assert.equal(row.eligible, row.expected, row.label);
});


test('durable queue tickets wake at the same retry tier as production selection', async t => {
  const pg = await startDisposablePostgres('scout-ticket-retry-');
  const db = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 1 });
  t.after(() => closePostgresFixture(db, pg));
  const { rows } = await db.query(`WITH u(label, vertical, enrichment_attempts, last_attempt_at, expected_ms) AS (VALUES
    ('early', 'painting', 2, NOW(), 3600000::bigint),
    ('daily first', 'painting', 3, NOW(), 86400000),
    ('daily last', 'painting', 5, NOW(), 86400000),
    ('long hold', 'painting', 6, NOW(), 604800000),
    ('daily due', 'painting', 3, NOW()-INTERVAL '24 hours', 0),
    ('long due', 'painting', 6, NOW()-INTERVAL '7 days', 0),
    ('null timestamp', 'painting', 6, NULL, 0),
    ('unsupported', 'unknown', 3, NOW(), NULL),
    ('null vertical', NULL, 3, NOW(), NULL)
  ) SELECT label, expected_ms, ${RETRY_AFTER_SQL.replaceAll('$5', '$1')} AS retry_ms,
    ${ELIGIBILITY_SQL.replaceAll('$5', '$1')} AS bucket FROM u`, [['painting']]);
  for (const row of rows) {
    assert.equal(row.retry_ms, row.expected_ms, row.label);
    assert.equal(row.bucket === 'eligible_now', row.retry_ms === '0', row.label);
  }
});
