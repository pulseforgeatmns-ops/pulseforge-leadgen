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
    ('three null', 'painting', 3, NULL, 'attempt_limit_hold'),
    ('before hour', 'painting', 2, NOW()-INTERVAL '1 hour'+INTERVAL '1 microsecond', 'retry_cooldown'),
    ('at hour', 'painting', 2, NOW()-INTERVAL '1 hour', 'eligible_now'),
    ('after hour', 'painting', 2, NOW()-INTERVAL '1 hour'-INTERVAL '1 microsecond', 'eligible_now'),
    ('three before day', 'painting', 3, NOW()-INTERVAL '24 hours'+INTERVAL '1 microsecond', 'attempt_limit_hold'),
    ('three at day', 'painting', 3, NOW()-INTERVAL '24 hours', 'attempt_limit_hold'),
    ('five before day', 'painting', 5, NOW()-INTERVAL '24 hours'+INTERVAL '1 microsecond', 'attempt_limit_hold'),
    ('five at day', 'painting', 5, NOW()-INTERVAL '24 hours', 'attempt_limit_hold'),
    ('six null', 'painting', 6, NULL, 'attempt_limit_hold'),
    ('three before week', 'painting', 3, NOW()-INTERVAL '7 days'+INTERVAL '1 microsecond', 'attempt_limit_hold'),
    ('three at week', 'painting', 3, NOW()-INTERVAL '7 days', 'eligible_now'),
    ('three after week', 'painting', 3, NOW()-INTERVAL '7 days'-INTERVAL '1 microsecond', 'eligible_now'),
    ('unsupported', 'Painting', 6, NOW(), 'unsupported_vertical'),
    ('null vertical', NULL, 0, NULL, 'unsupported_vertical'),
    ('null vertical held', NULL, 6, NOW(), 'unsupported_vertical'),
    ('unsupported cooldown', 'Painting', 3, NOW(), 'unsupported_vertical'),
    ('overlap', 'painting', 6, NOW(), 'attempt_limit_hold')
  ) SELECT label, expected, ${ELIGIBILITY_SQL} AS bucket,
    (${ENRICHMENT_ELIGIBILITY_SQL}) = 'eligible_now' AS selector_eligible
    FROM u WHERE $1::integer = 10 AND $4::integer = 5`, [10, 3, 1, 5, ['painting']]);
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
    FROM u WHERE $1::integer = 10 AND $4::integer = 5`, [10, 2, 168, 5, null]);
  for (const row of rows) assert.equal(row.eligible, row.expected, row.label);
});


test('durable queue tickets wake at the same unchanged retry boundary as production selection', async t => {
  const pg = await startDisposablePostgres('scout-ticket-retry-');
  const db = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 1 });
  t.after(() => closePostgresFixture(db, pg));
  const { rows } = await db.query(`WITH u(label, vertical, enrichment_attempts, last_attempt_at, expected_ms) AS (VALUES
    ('early', 'painting', 2, NOW(), 3600000::bigint),
    ('third attempt', 'painting', 3, NOW(), 604800000),
    ('fifth attempt', 'painting', 5, NOW(), 604800000),
    ('long hold', 'painting', 6, NOW(), 604800000),
    ('one day is not due', 'painting', 3, NOW()-INTERVAL '24 hours', 518400000),
    ('long due', 'painting', 6, NOW()-INTERVAL '7 days', 0),
    ('null timestamp remains held', 'painting', 3, NULL, NULL),
    ('unsupported', 'unknown', 3, NOW(), NULL),
    ('null vertical', NULL, 3, NOW(), NULL)
  ) SELECT label, expected_ms, ${RETRY_AFTER_SQL.replaceAll('$5', '$1')} AS retry_ms,
    ${ELIGIBILITY_SQL.replaceAll('$5', '$1')} AS bucket FROM u`, [['painting']]);
  for (const row of rows) {
    assert.equal(row.retry_ms, row.expected_ms, row.label);
    assert.equal(row.bucket === 'eligible_now', row.retry_ms === '0', row.label);
  }
});

test('shared selector preserves legacy eligibility across configured retry policies', async t => {
  const pg = await startDisposablePostgres('scout-policy-parity-');
  const db = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 1 });
  t.after(() => closePostgresFixture(db, pg));
  assert.equal(require('../scoutUnenrichedEnrichmentAgent')._test.DEFAULT_MAX_ATTEMPTS, 3);
  const legacy = `(COALESCE(enrichment_attempts, 0) < $2
    OR last_attempt_at <= NOW() - INTERVAL '7 days')
    AND COALESCE(last_attempt_at, NOW() - INTERVAL '100 years') <= NOW() - ($3::numeric * INTERVAL '1 hour')
    AND ($5::text[] IS NULL OR vertical = ANY($5::text[]))`;
  for (const ceiling of [1, 2, 3, 6]) {
    for (const hours of [1, 168, 192]) {
      for (const verticals of [null, ['painting'], []]) {
        const { rows } = await db.query(`WITH u AS (
          SELECT enrichment_attempts, vertical, last_attempt_at FROM
          unnest(ARRAY[NULL,0,1,2,3,4,5,6,10]::integer[]) AS a(enrichment_attempts)
          CROSS JOIN unnest(ARRAY[NULL,'painting','unknown']::text[]) AS v(vertical)
          CROSS JOIN unnest(ARRAY[NULL,NOW(),NOW()-INTERVAL '1 hour',
            NOW()-INTERVAL '24 hours',NOW()-INTERVAL '7 days'+INTERVAL '1 microsecond',
            NOW()-INTERVAL '7 days',NOW()-INTERVAL '8 days']::timestamptz[]) AS d(last_attempt_at)
        ) SELECT count(*)::int AS mismatches FROM u
          WHERE $1::integer=10 AND $4::integer=5
          AND ((${legacy}) IS TRUE) <> ((${ENRICHMENT_ELIGIBILITY_SQL})='eligible_now')`,
        [10, ceiling, hours, 5, verticals]);
        assert.equal(rows[0].mismatches, 0, JSON.stringify({ ceiling, hours, verticals }));
      }
    }
  }
});
