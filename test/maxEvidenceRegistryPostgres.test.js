'use strict';

/**
 * P0-MAX-REASONING-001 PR 4 — EvidenceRegistry against disposable PostgreSQL.
 * Synthetic fixtures only. Every statement the registry sends is recorded and
 * must be a SELECT/WITH; every table is hashed before and after and must be
 * unchanged.
 */

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { ensureAcquisitionMissionSchema } = require('../services/acquisitionMissionPersistence');
const { EvidenceRegistry } = require('../packages/max/evidence/EvidenceRegistry');
const { diagnoseScout, diagnoseEmmett, rankOpportunities, opportunityStaleness } = require('../packages/max/evidence/EvidenceDiagnosis');
const { revalidateRemembered } = require('../packages/max/evidence/LedgerReferentContract');

const NOW = new Date('2026-11-18T12:00:00.000Z');
const days = (n) => new Date(NOW.getTime() - n * 86400000).toISOString();
const L = (n) => `00000000-0000-4000-8000-0000000021${String(n).padStart(2, '0')}`;
const A = (n) => `00000000-0000-4000-8000-0000000010${String(n).padStart(2, '0')}`;
const LEVI = '21';
const ANCHOR = '10';
const TABLES = ['clients', 'companies', 'prospects', 'agent_log', 'touchpoints', 'acquisition_missions', 'acquisition_mission_events', 'acquisition_outbound_programs'];

function recordingPool(pool) {
  const statements = [];
  return { statements, query: (sql, params) => { statements.push(typeof sql === 'string' ? sql : sql.text); return pool.query(sql, params); } };
}
async function hashTables(db) {
  const out = {};
  for (const t of TABLES) out[t] = (await db.query(`SELECT md5(COALESCE(string_agg(x::text, '|' ORDER BY x::text), '')) AS h FROM ${t} x`)).rows[0].h;
  return out;
}

async function seed(db) {
  await db.query(`
    CREATE TABLE clients(id INTEGER PRIMARY KEY, name TEXT, slug TEXT);
    CREATE TABLE companies(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL REFERENCES clients(id), name TEXT);
    CREATE TABLE prospects(
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL REFERENCES clients(id),
      company_id UUID REFERENCES companies(id), first_name TEXT, last_name TEXT, email TEXT, phone TEXT, job_title TEXT,
      linkedin_url TEXT, source TEXT, icp_score INTEGER, status TEXT, notes TEXT, vertical TEXT, service_area_match TEXT,
      setter_visible BOOLEAN DEFAULT false, setter_status TEXT, is_hot BOOLEAN DEFAULT false, do_not_contact BOOLEAN DEFAULT false,
      ao_outreach_review_required BOOLEAN NOT NULL DEFAULT false, discovery_method TEXT, acquisition_metadata JSONB,
      last_contacted_at TIMESTAMPTZ, closed_at TIMESTAMPTZ, booked_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE agent_log(id SERIAL PRIMARY KEY, client_id INTEGER, agent_name TEXT, ran_at TIMESTAMPTZ, status TEXT);
    CREATE TABLE touchpoints(id SERIAL PRIMARY KEY, client_id INTEGER, channel TEXT, action_type TEXT, outcome TEXT, created_at TIMESTAMPTZ);
    INSERT INTO clients VALUES (21, 'Levi Lending', 'levi-lending'), (10, 'Anchor Cleaning', 'cleaning-co');
  `);
  await ensureAcquisitionMissionSchema(db);
  await db.query(fs.readFileSync('migrations/2026-09-18-anchor-daily-outbound.sql', 'utf8'));
  const md = (extra = {}) => JSON.stringify({ category: 'borrower', reviewState: 'contact_now', ...extra });
  const p = (id, client, f) => db.query(`INSERT INTO prospects (id, client_id, first_name, last_name, email, phone, source, icp_score, status, notes,
      service_area_match, setter_status, is_hot, acquisition_metadata, last_contacted_at, closed_at, created_at, updated_at)
    VALUES ($1,$2,$3,'Synthetic',$4,$5,'fixture',$6,$7,'synthetic note',$8,$9,$10,$11::jsonb,$12,$13,$14,$15)`,
  [id, client, f.first || 'Pat', 'email' in f ? f.email : 'pat@example.test', 'phone' in f ? f.phone : '555-0100', f.icp ?? 85, f.status, f.area ?? 'manchester',
    f.setter ?? 'new', f.hot ?? false, f.md ?? md(), f.contacted ?? days(10), f.closed ?? null, days(200), f.updated ?? days(3)]);
  // Levi: five borrower prospects with distinct situations, plus one more.
  await p(L(1), 21, { status: 'hot', hot: true });
  await p(L(2), 21, { status: 'warm', email: null, phone: null });
  await p(L(3), 21, { status: 'closed', closed: days(20), setter: 'follow_up' });
  await p(L(4), 21, { status: 'cold', updated: days(90), contacted: days(120) });
  await p(L(5), 21, { status: 'warm', md: md({ expiresAt: days(5) }) });
  await p(L(6), 21, { status: 'warm' });
  // Anchor: three open, eligible opportunities and one closed.
  await p(A(1), 10, { status: 'hot', hot: true, md: md({ category: 'facility' }) });
  await p(A(2), 10, { status: 'warm', md: md({ category: 'facility' }) });
  await p(A(3), 10, { status: 'cold', md: md({ category: 'facility' }) });
  await p(A(4), 10, { status: 'dead', md: md({ category: 'facility' }) });
  await db.query(`INSERT INTO agent_log (client_id, agent_name, ran_at, status) VALUES
    (10, 'scout', $1, 'error'), (10, 'scout', $2, 'error'), (21, 'paige', $3, 'ok')`, [days(3), days(20), days(2)]);
  await db.query(`INSERT INTO touchpoints (client_id, channel, action_type, created_at) VALUES (10, 'email', 'sent', $1)`, [days(40)]);
  const mission = (id, tenant, status, updated, payload = {}) => db.query(
    `INSERT INTO acquisition_missions (id, tenant_id, client_id, stage, status, objective, title, payload, created_at, updated_at)
     VALUES ($1,$2,$3,'discovery',$4,'Synthetic objective','Synthetic mission',$5::jsonb,$6,$6)`,
    [id, tenant, Number(tenant), status, JSON.stringify(payload), updated]);
  await mission('m_anchor_live', ANCHOR, 'active', days(30), { pendingOperatorDecision: { kind: 'plan_approval', prompt: 'Approve?' } });
  await mission('m_anchor_idle', ANCHOR, 'active', days(40));
  await db.query(`INSERT INTO acquisition_mission_events (id, mission_id, tenant_id, kind, specialist, label, at) VALUES
    ('e1', 'm_anchor_live', '10', 'emmett_dispatch_completed', 'emmett', 'Emmett dispatch completed', $1),
    ('e2', 'm_anchor_live', '10', 'plan_presented', null, 'Plan presented', $2)`, [days(2), days(1)]);
  await db.query(`INSERT INTO acquisition_outbound_programs (id, tenant_id, source_mission_id, policy, policy_hash, scope_hash, authorized_by, authorized_at, mode)
    VALUES ('prog1', '10', 'm_anchor_live', $1::jsonb, 'h', 's', 'operator', $2, 'paused')`, [JSON.stringify({ expiresAt: days(26) }), days(60)]);
}

test('PostgreSQL: EvidenceRegistry scenarios, SELECT-only, nothing written', { timeout: 180000 }, async (t) => {
  const instance = await startDisposablePostgres('max-evidence-');
  const db = new Pool({ connectionString: instance.connectionString });
  t.after(async () => { await db.end(); await instance.stop(); });
  await seed(db);
  const before = await hashTables(db);
  const pool = recordingPool(db);
  // The Anchor outbound gate is read by the existing recovery-state loader;
  // here it is a synthetic fixture (its own suites cover the real store).
  const gate = { loadOutboundGate: async ({ tenantId, asOf }) => (tenantId === ANCHOR
    ? { source: 'outbound_gate', available: true, applicable: true, provenance: 'fixture', asOf, mode: 'maintenance', reason: 'recovery_kill_switch' }
    : { source: 'outbound_gate', available: true, applicable: false, provenance: 'none', asOf }) };
  const registry = () => new EvidenceRegistry({ pool, now: () => NOW, env: {}, loaders: { snapshot: gate } });
  const FIVE = [L(1), L(2), L(3), L(4), L(5)];
  const memory = { referent: { kind: 'prospect_set', memberIds: FIVE }, requiresRevalidation: true, verified: false };

  await t.test("1/3: Levi's five borrower prospects: current evidence, missing contact info, closed, expired, stale, contradictions", async () => {
    const { report } = await revalidateRemembered(registry(), { tenantId: LEVI, memory });
    const byId = Object.fromEntries(report.revalidation.results.map((r) => [r.id, r]));
    assert.deepEqual(FIVE.map((id) => byId[id].status), FIVE.map(() => 'current'), JSON.stringify(report.domains.crm_pipeline && report.domains.crm_pipeline.status) + JSON.stringify(report.unknowns));
    const ev = (n) => byId[L(n)].evidence;
    assert.ok(ev(2).missing.includes('contact channel'));
    assert.deepEqual(ev(2).facts.contactChannels, { email: false, phone: false, linkedin: false });
    assert.equal(ev(3).facts.opportunityState, 'closed');
    assert.equal(ev(5).facts.opportunityState, 'expired');
    assert.equal(opportunityStaleness(ev(4)).stale, true);
    assert.equal(ev(4).freshness.stale, true);
    assert.equal(opportunityStaleness(ev(1)).stale, false);
    assert.ok(report.contradictions.some((c) => c.kind === 'status_conflict' && c.refs.includes(`crm_pipeline:prospect_record:${L(3)}`) && c.resolution === 'unresolved'));
    for (const n of [1, 2, 3, 4, 5]) {
      assert.equal(ev(n).tenantId, LEVI);
      assert.equal(ev(n).kind, 'observed');
      assert.ok(ev(n).observedAt && ev(n).retrievedAt === NOW.toISOString() && ev(n).source.provenance);
    }
    assert.doesNotMatch(JSON.stringify(report), /pat@example|555-0100|synthetic note|Synthetic/);
  });

  await t.test('2: after a restart, the same remembered ids are rechecked; deleted and transferred records are reported, never replaced', async () => {
    await db.query('DELETE FROM prospects WHERE id = $1', [L(4)]);
    await db.query('UPDATE prospects SET client_id = 10 WHERE id = $1', [L(5)]);
    const fresh = new EvidenceRegistry({ pool: recordingPool(db), now: () => NOW, env: {}, loaders: { snapshot: gate } });
    const { report } = await revalidateRemembered(fresh, { tenantId: LEVI, memory });
    const status = Object.fromEntries(report.revalidation.results.map((r) => [r.id, r.status]));
    assert.equal(status[L(4)], 'not_found_in_tenant');
    assert.equal(status[L(5)], 'not_found_in_tenant');
    assert.equal(report.revalidation.results.filter((r) => r.status === 'current').length, 3);
    assert.ok(!report.revalidation.results.some((r) => r.id === L(6)), 'no replacement record is offered');
    assert.ok(report.unknowns.some((u) => u.domain === 'revalidation' && u.what.includes(L(4))));
    // Restore fixtures for the remaining subtests.
    await db.query('UPDATE prospects SET client_id = 21 WHERE id = $1', [L(5)]);
  });

  await t.test("4: Anchor's highest-priority active opportunities", async () => {
    const report = await registry().retrieve({ tenantId: ANCHOR, domains: ['crm_pipeline'] });
    const ranked = rankOpportunities(report, { top: 2 });
    assert.deepEqual(ranked.ranked.map((r) => [r.ref.split(':').pop(), r.position]), [[A(1), 1], [A(2), 2]]);
    assert.ok(ranked.excluded.some((e) => e.ref.endsWith(A(4)) && e.reason === 'opportunity closed'));
    assert.equal(ranked.complete, true);
    assert.ok(ranked.ranked.every((r) => r.basis.startsWith('recorded engagement tier')));
  });

  await t.test('5: why Scout has not produced qualified prospects', async () => {
    const report = await registry().retrieve({ tenantId: ANCHOR, domains: ['scout_execution'] });
    const d = diagnoseScout(report);
    assert.equal(d.verdict, 'candidate_explanations');
    assert.ok(d.inferences.some((i) => /failed/.test(i.claim) && i.certainty === 'inferred' && i.basis.includes('scout_execution:agent_run:scout')));
    assert.ok(d.unknowns.some((u) => u.what === 'Scout acquisition state'));
  });

  await t.test('6: why Emmett has not sent outreach', async () => {
    const report = await registry().retrieve({ tenantId: ANCHOR, domains: ['outbound_authorization', 'outbound_delivery', 'crm_pipeline'] });
    const d = diagnoseEmmett(report);
    const claims = d.inferences.map((i) => i.claim).join(' | ');
    assert.match(claims, /maintenance/);
    assert.match(claims, /authorization expired/);
    assert.match(claims, /"paused" mode/);
    assert.ok(d.unknowns.some((u) => /no emmett runs recorded/.test(u.reason) && /does not establish/.test(u.reason)));
    assert.ok(d.inferences.every((i) => i.basis.length > 0));
  });

  await t.test('7/10: contradictory mission and agent records; a pending approval is reported, not consumed', async () => {
    const report = await registry().retrieve({ tenantId: ANCHOR, domains: ['mission_lifecycle', 'agent_execution', 'operator_decisions'] });
    const kinds = report.contradictions.map((c) => c.kind);
    assert.ok(kinds.includes('mission_vs_agent_log'), 'mission says Emmett dispatched 2 days ago; agent_log has no Emmett run');
    assert.ok(report.contradictions.some((c) => c.kind === 'status_vs_activity' && c.refs[0].endsWith('m_anchor_idle')));
    assert.ok(report.contradictions.every((c) => c.resolution === 'unresolved'));
    const pending = report.domains.operator_decisions.items.find((i) => i.subject.type === 'pending_decision');
    assert.deepEqual([pending.facts.state, pending.facts.kind], ['pending', 'plan_approval']);
    const live = report.domains.mission_lifecycle.items.find((i) => i.subject.id === 'm_anchor_live');
    assert.equal(live.facts.pendingDecisionKind, 'plan_approval');
    assert.ok(report.domains.operator_decisions.items.some((i) => i.facts.eventKind === 'emmett_dispatch_completed'));
    const row = (await db.query("SELECT payload FROM acquisition_missions WHERE id = 'm_anchor_live'")).rows[0];
    assert.equal(row.payload.pendingOperatorDecision.kind, 'plan_approval', 'still pending');
  });

  await t.test('8: cross-tenant access is refused or yields nothing', async () => {
    const report = await registry().retrieve({ tenantId: LEVI, domains: ['crm_pipeline'], referents: [{ kind: 'prospect_set', memberIds: [A(1), A(2)] }, { kind: 'mission', missionId: 'm_anchor_live' }] });
    assert.ok(report.revalidation.results.every((r) => r.status === 'not_found_in_tenant'));
    assert.ok(!report.domains.crm_pipeline.items.some((i) => i.subject.id.startsWith('00000000-0000-4000-8000-0000000010')));
    const named = recordingPool(db);
    const refused = await new EvidenceRegistry({ pool: named, now: () => NOW, env: {} }).retrieve({ tenantId: LEVI, domains: ['crm_pipeline', 'mission_lifecycle'], question: "Show me Anchor Cleaning's prospects" });
    assert.equal(refused.refused, 'tenant_mismatch');
    assert.deepEqual(refused.domains, {});
    assert.ok(named.statements.every((s) => /FROM clients/.test(s)), 'only the client directory was read');
  });

  await t.test('9: missing, malformed or unavailable evidence is reported, never invented', async () => {
    const report = await registry().retrieve({ tenantId: ANCHOR, domains: ['campaign_performance'] });
    assert.equal(report.domains.campaign_performance.status, 'unavailable');
    assert.ok(report.unknowns.some((u) => u.reason === 'email_performance_not_present_in_this_deployment'));
    const dead = new EvidenceRegistry({ pool: new Pool({ connectionString: 'postgresql://postgres@127.0.0.1:1/none', connectionTimeoutMillis: 500 }), now: () => NOW, env: {} });
    const down = await dead.retrieve({ tenantId: LEVI, domains: ['crm_pipeline', 'mission_lifecycle', 'agent_execution'], referents: [{ kind: 'prospect', id: L(1) }] });
    assert.equal(down.domains.crm_pipeline.status, 'failed');
    assert.equal(down.domains.crm_pipeline.items.length, 0);
    assert.equal(down.revalidation.results[0].status, 'retrieval_failed');
    assert.ok(down.unknowns.length >= 3);
  });

  await t.test('every statement was a read and no table changed beyond the test\'s own fixture edits', async () => {
    assert.ok(pool.statements.length > 10);
    for (const s of pool.statements) assert.match(s, /^\s*(SELECT|WITH)\b/i, s.slice(0, 80));
    await db.query(`INSERT INTO prospects (id, client_id, status, created_at, updated_at) VALUES ($1, 21, 'cold', $2, $2)`, [L(4), days(90)]);
    const after = await hashTables(db);
    for (const t of TABLES.filter((x) => x !== 'prospects')) assert.equal(after[t], before[t], t);
  });
});
