'use strict';

// The production validation preflight is read-only and names the exact gate
// that blocks each authorized step. Native disposable PostgreSQL, no providers.
require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: { query: async () => ({ rows: [] }) } };

const LEGACY_MISSION = { structuredMission: {
  market: { segment: 'commercial_cleaning_buyers', industry: 'property_and_office_operations',
    eligibleSubsegments: ['property_manager', 'str_manager', 'realtor', 'commercial_office'] },
  geography: { region: 'Greater Manchester', scope: null,
    cities: ['Manchester', 'Hooksett', 'Bedford', 'Auburn', 'Goffstown', 'Londonderry'] } } };
const CLOSED_ENV = { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true', ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true',
  ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false', RAILWAY_GIT_COMMIT_SHA: 'a2d32225a5426aae46873a148811b8fbd0cd6d15' };

async function schema(db) {
  await db.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-14-api-usage-control.sql'), 'utf8'));
  for (const file of ['2026-10-17-scout-query-checkpoints.sql', '2026-10-19-scout-query-pages.sql',
    '2026-10-19-scout-candidate-tickets.sql', '2026-10-27-scout-tenant-search-memory.sql']) {
    await db.query(fs.readFileSync(path.join(__dirname, '../migrations', file), 'utf8'));
  }
  await db.query(`
    CREATE TABLE acquisition_outbound_programs(id text PRIMARY KEY, tenant_id text, mode text, source_mission_id text,
      policy jsonb, policy_hash text, scope_hash text, authorized_at timestamptz DEFAULT now());
    CREATE TABLE acquisition_missions(id text PRIMARY KEY, tenant_id text, client_id int, stage text, status text, objective text,
      target_segment text, campaign text, title text, priority text, confidence text, owner text, created_by text,
      orchestration_mission_id text, payload jsonb, created_at timestamptz, updated_at timestamptz);
    CREATE TABLE acquisition_outbound_envelopes(id text PRIMARY KEY, program_id text);
    CREATE TABLE acquisition_outbound_items(id text PRIMARY KEY, tenant_id text, envelope_id text, status text);`);
  await db.query(`INSERT INTO acquisition_missions(id, tenant_id, client_id, objective, payload)
    VALUES ('mission_anchor_weekend_20261003_approved_market', '10', 10, 'Anchor acquisition', $1)`, [JSON.stringify(LEGACY_MISSION)]);
  await db.query(`INSERT INTO acquisition_outbound_programs(id, tenant_id, mode, source_mission_id, policy)
    VALUES ('outbound_0e6e8d823ee3428405d90815', '10', 'active', 'mission_anchor_weekend_20261003_approved_market', '{}')`);
}

test('validation preflight reads production-shaped state without writing and names each blocking gate', { timeout: 90000 }, async t => {
  const pg = await startDisposablePostgres('anchor-preflight-');
  const db = new Pool({ connectionString: pg.connectionString, max: 4 });
  t.after(async () => { await db.end(); await pg.stop(); });
  await schema(db);
  const { run } = require('../scripts/anchorScoutValidationPreflight');
  const snapshot = async () => (await db.query(`SELECT
    (SELECT count(*) FROM acquisition_missions) AS missions,
    (SELECT source_mission_id FROM acquisition_outbound_programs) AS source,
    (SELECT count(*) FROM api_spend_policies) AS policies`)).rows[0];

  await t.test('closed production state: mission may be activated, the discovery window is blocked by governance', async () => {
    await db.query(`CREATE TABLE anchor_email_recovery_control(singleton boolean PRIMARY KEY, maintenance_armed boolean, manifest jsonb);
      INSERT INTO anchor_email_recovery_control VALUES (true, true, NULL)`);
    const before = await snapshot();
    const report = await run(db, { env: { ...CLOSED_ENV }, expectRevision: CLOSED_ENV.RAILWAY_GIT_COMMIT_SHA });
    assert.deepEqual(await snapshot(), before, 'read-only');
    const byName = Object.fromEntries(report.gates.map(g => [g.gate, g]));
    assert.equal(byName.deployed_revision.status, 'pass');
    assert.equal(byName.successor_scope.status, 'pass');
    assert.equal(byName.prepared_envelope_drained.status, 'pass');
    assert.equal(byName.no_recovery_manifest_bound_to_program.status, 'pass');
    assert.equal(report.mayApplyMission, true);
    assert.equal(report.facts.successor.cities.length, 8);
    assert.equal(report.facts.currentScope.fingerprint, '22d50dc07f997a3a811368a7e8b1de5a3211df5bd555260c3d42c4e780a960b7');
    assert.notEqual(report.facts.successor.fingerprint, report.facts.currentScope.fingerprint);
    assert.equal(report.facts.recovery.mode, 'maintenance');
    assert.equal(report.facts.recovery.reason, 'recovery_kill_switch');
    assert.equal(byName.recovery_mode_allows_scheduled_research.status, 'blocked');
    assert.match(byName.recovery_mode_allows_scheduled_research.detail, /legacy unreachable/);
    assert.equal(byName.places_spend_policy.status, 'blocked');
    assert.match(byName.places_spend_policy.detail, /uncapped/);
    assert.equal(byName.discovery_memory_flag.status, 'blocked');
    assert.equal(byName.discovery_memory_schema.status, 'pass');
    assert.equal(byName.research_providers_unpaused.status, 'blocked');
    assert.equal(byName.governed_outbound_disabled.status, 'pass');
    assert.equal(report.mayStartWindow, false);
    assert.equal(report.verdict, 'blocked_at_step_5');
  });

  await t.test('an installed recovery manifest blocks activation because the policy hash would change', async () => {
    await db.query(`UPDATE anchor_email_recovery_control SET manifest='{"kind":"scoped_resume"}'`);
    const report = await run(db, { env: { ...CLOSED_ENV } });
    assert.equal(report.gates.find(g => g.gate === 'no_recovery_manifest_bound_to_program').status, 'blocked');
    assert.equal(report.mayApplyMission, false);
    await db.query('UPDATE anchor_email_recovery_control SET manifest=NULL');
  });

  await t.test('a pending prepared envelope blocks activation', async () => {
  await db.query(`INSERT INTO acquisition_outbound_envelopes VALUES ('env-1','outbound_0e6e8d823ee3428405d90815');
      INSERT INTO acquisition_outbound_items VALUES ('item-1','10','env-1','pending')`);
    const pending = await run(db, { env: { ...CLOSED_ENV } });
    assert.equal(pending.gates.find(g => g.gate === 'prepared_envelope_drained').status, 'blocked');
    assert.equal(pending.mayApplyMission, false);
    await db.query(`DELETE FROM acquisition_outbound_items; DELETE FROM acquisition_outbound_envelopes`);
  });

  await t.test('every gate passes only with legacy recovery, memory, a capped policy and outbound off', async () => {
    await db.query('DROP TABLE anchor_email_recovery_control');
  await db.query(`INSERT INTO api_spend_policies(provider, daily_request_limit, daily_usd_limit, paused, updated_by)
      VALUES ('google_places', 170, 5, false, 'fixture')`);
    const report = await run(db, { env: { ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false', SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED: 'true',
      ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED: 'true' } });
    assert.deepEqual(report.gates.filter(g => g.status === 'blocked').map(g => g.gate), [], JSON.stringify(report.gates));
    assert.equal(report.mayStartWindow, true);
    await db.query(`UPDATE api_spend_policies SET daily_usd_limit=50 WHERE provider='google_places'`);
    const loose = await run(db, { env: { ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false', SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED: 'true',
      ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED: 'true' } });
    assert.equal(loose.gates.find(g => g.gate === 'places_spend_policy').status, 'blocked', 'a loose ceiling is not the approved ceiling');
    const sending = await run(db, { env: { ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'true', SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED: 'true',
      ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED: 'true' } });
    assert.equal(sending.gates.find(g => g.gate === 'governed_outbound_disabled').status, 'blocked');
  });
});
