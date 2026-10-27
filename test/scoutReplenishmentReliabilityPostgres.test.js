'use strict';

// SPEC-SCOUT-P0-001 against disposable PostgreSQL: hold deduplication, the
// enforced agent_log status constraint and the read-only scorecard.
const test = require('node:test');
const assert = require('node:assert/strict');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');

const SCHEMA = `
  CREATE TABLE agent_actions (id SERIAL PRIMARY KEY, created_by TEXT, action_type TEXT, title TEXT,
    description TEXT, payload JSONB, status TEXT, client_id INTEGER, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE agent_log (id SERIAL PRIMARY KEY, agent_name TEXT, action TEXT, payload JSONB, status TEXT,
    error_msg TEXT, ran_at TIMESTAMPTZ, client_id INTEGER,
    CONSTRAINT agent_log_status_check
      CHECK (status IN ('success', 'failed', 'skipped', 'pending', 'completed', 'posted', 'in_progress')));
  CREATE TABLE prospects (id SERIAL PRIMARY KEY, client_id INTEGER, email TEXT, email_verified BOOLEAN,
    do_not_contact BOOLEAN, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE touchpoints (id SERIAL PRIMARY KEY, client_id INTEGER, prospect_id INTEGER, channel TEXT,
    action_type TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE scout_unenriched (id SERIAL PRIMARY KEY, client_id INTEGER, source TEXT,
    enrichment_attempts INTEGER, last_attempt_at TIMESTAMPTZ);
`;

test('replenishment reliability against PostgreSQL', { timeout: 60000 }, async t => {
  const instance = await startDisposablePostgres('scout-p0-');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString });
  t.after(() => closePostgresFixture(pool, instance));
  await pool.query(SCHEMA);
  const { _test } = require('../services/maxOutboundControlLoop');
  const quiet = { warn() {} };

  await t.test('one pending hold per reason; a reviewed hold allows a new alert', async () => {
    const hold = reason => _test.recordReplenishmentHold(pool, { tenantId: '10', clientId: 10, programId: 'p', reason, logger: quiet });
    await hold('anchor_research_provider_paused');
    await hold('anchor_research_provider_paused');
    await hold('anchor_recovery_separate_preparation_approval_required');
    const count = async () => (await pool.query(
      "SELECT payload->>'reason' AS reason, count(*)::int AS n FROM agent_actions WHERE client_id=10 GROUP BY 1 ORDER BY 1")).rows;
    assert.deepEqual(await count(), [
      { reason: 'anchor_recovery_separate_preparation_approval_required', n: 1 },
      { reason: 'anchor_research_provider_paused', n: 1 },
    ]);
    await pool.query("UPDATE agent_actions SET status='dismissed' WHERE payload->>'reason'='anchor_research_provider_paused'");
    await hold('anchor_research_provider_paused');
    assert.equal((await count()).find(row => row.reason === 'anchor_research_provider_paused').n, 2);
  });

  await t.test('governed block record satisfies the production status constraint', async () => {
    const { logResearchProviderBlock } = require('../utils/researchProviderBlockLog');
    const error = Object.assign(new Error('anchor_research_provider_paused'), { code: 'anchor_research_provider_paused' });
    const id = await logResearchProviderBlock({ agentName: 'scout', action: 'scrape', clientId: 10, error, trigger: 'cron', db: pool });
    assert.ok(id);
    await assert.rejects(pool.query(
      "INSERT INTO agent_log (agent_name, action, status, ran_at, client_id) VALUES ('x','y','partial',NOW(),10)"),
    { code: '23514' }, 'the former partial status is rejected by the constraint');
  });

  await t.test('scorecard is read-only and reports missing tables instead of failing', async () => {
    await pool.query("INSERT INTO prospects (client_id, email, email_verified, do_not_contact) VALUES (10,'a@x.example',true,false),(13,'b@y.example',true,false)");
    await pool.query("INSERT INTO touchpoints (client_id, prospect_id, action_type) VALUES (10,1,'email_sent'),(10,1,'email_sent'),(13,2,'email_sent')");
    const { collectScorecard } = require('../scripts/anchorScoutScorecard');
    const before = (await pool.query('SELECT count(*)::int AS n FROM agent_actions')).rows[0].n;
    const report = await collectScorecard(pool, { days: 7, env: { ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true', CRON_SECRET: 'never' } });
    assert.equal(report.flags.ANCHOR_RESEARCH_PROVIDERS_PAUSED, 'true');
    assert.equal(Object.hasOwn(report.flags, 'CRON_SECRET'), false, 'no secrets reported');
    assert.equal(report.sections.inventory[0].verified_contactable, 1, 'tenant 13 excluded');
    assert.equal(report.sections.firstTouches[0].first_touches, 1, 'repeat touch is not a first touch');
    assert.equal(report.sections.agentRuns[0].status, 'skipped');
    assert.equal(report.sections.openHolds.length, 3, 'pending and reviewed holds are reported separately');
    assert.equal(report.sections.recoveryControl.unavailable, '42P01');
    assert.equal(report.sections.controlEvents.unavailable, '42P01');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM agent_actions')).rows[0].n, before);
  });
});
