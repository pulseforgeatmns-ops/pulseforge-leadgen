'use strict';

/**
 * P0-MAX-REASONING-001 PR 4 — EvidenceRegistry (hermetic). No network, no
 * database: injected loaders and a real in-memory acquisition mission engine.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const amo = require('../../../acquisition-mission');
const { EvidenceRegistry, MAX_REFERENT_IDS } = require('../EvidenceRegistry');
const { rankOpportunities, diagnoseScout } = require('../EvidenceDiagnosis');
const { referentsFromMemory, revalidateRemembered } = require('../LedgerReferentContract');
const { validateTurnFrame } = require('../../interpretation/TurnFrame');

const NOW = new Date('2026-11-18T12:00:00.000Z');
const days = (n) => new Date(NOW.getTime() - n * 86400000).toISOString();
const row = (id, client, f = {}) => ({
  id, client_id: client, first_name: 'Pat', email: 'x@example.test', phone: null, linkedin_url: null, source: 'fixture',
  icp_score: 85, status: 'warm', notes: 'n', service_area_match: 'yes', setter_status: 'new', is_hot: false, do_not_contact: false,
  ao_outreach_review_required: false, acquisition_metadata: { category: 'c', reviewState: 'contact_now' },
  last_contacted_at: days(5), created_at: days(100), updated_at: days(2), ...f,
});
const emptySnapshot = {
  loadClientDirectory: async () => ({ available: true, rows: [{ id: 10, name: 'Anchor Cleaning', slug: 'cleaning-co' }, { id: 21, name: 'Levi Lending', slug: 'levi-lending' }] }),
  loadAcquisitionMissions: async () => ({ source: 'acquisition_missions', available: true, provenance: 'fixture', rows: [] }),
  loadEngineMissions: async () => ({ source: 'missions', available: true, provenance: 'fixture', rows: [] }),
  loadProspecting: async () => ({ source: 'prospects', available: true, provenance: 'fixture', counts: { total: 0, qualified: 0, setterVisible: 0, doNotContact: 0 }, lastAddedAt: null }),
  loadOutboundActivity: async () => ({ source: 'touchpoints', available: true, provenance: 'fixture', channels: [] }),
  loadAgentRuns: async () => ({ source: 'agent_log', available: true, provenance: 'fixture', agents: [] }),
  loadOutboundGate: async () => ({ source: 'outbound_gate', available: true, applicable: false }),
};
function registry(over = {}) {
  return new EvidenceRegistry({ pool: null, now: () => NOW, env: {}, ...over, loaders: { snapshot: emptySnapshot, ...(over.loaders || {}) } });
}

describe('pending approval', () => {
  it('reports a pending decision without resolving, executing or changing anything', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const m = engine.create({ tenantId: '10', objective: 'Acquire recurring commercial cleaning customers for Anchor Cleaning in Greater Manchester.', targetSegment: 'Facility buyers' });
    const fp = () => JSON.stringify({ m: engine.get(m.id, '10'), e: engine.store.listEvents(m.id), c: engine.store.listContributions(m.id) });
    const before = fp();
    amo.clearExecutionRouterAudit();
    const reg = new EvidenceRegistry({ pool: null, now: () => NOW, env: {}, acquisitionMissionRuntime: { engine: () => engine } });
    const report = await reg.retrieve({ tenantId: '10', domains: ['mission_lifecycle', 'operator_decisions'], referents: [{ kind: 'mission', missionId: m.id }] });
    const pending = report.domains.operator_decisions.items.find((i) => i.subject.type === 'pending_decision');
    assert.equal(pending.facts.state, 'pending');
    assert.equal(pending.facts.kind, 'plan_approval');
    assert.equal(report.revalidation.results[0].status, 'current');
    assert.equal(fp(), before);
    assert.deepEqual(amo.listExecutionRouterAudit(), []);
    assert.equal(engine.get(m.id, '10').pendingOperatorDecision.kind, 'plan_approval');
    assert.deepEqual(report.authority, { grantsAuthority: false, executionPermitted: false });
  });
});

describe('frame-directed retrieval and enum domains', () => {
  const ctx = { tenantId: '21', candidates: [], pendingDecisionId: null };
  const frame = (over = {}) => validateTurnFrame({
    intent: 'question', subjectKind: 'crm_pipeline', relationToPrior: 'new_topic', referents: [], evidenceNeeds: ['crm_pipeline'], facets: ['status'],
    requestedOutcome: 'status', requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' }, scope: 'focused',
    foreignTenantReference: false, ambiguities: [], confidence: 0.9, ...over,
  }, { ...ctx, producer: 'model' });

  it('only TurnFrame evidence domains are read; anything else is rejected and reported', async () => {
    const calls = [];
    const reg = registry({ loaders: { loadProspectRecords: async () => { calls.push(1); return { status: 'empty', rows: [], complete: true }; } } });
    const report = await reg.retrieve({ tenantId: '21', domains: ['crm_pipeline', 'prospects; DROP TABLE prospects', 'raw_sql'] });
    assert.deepEqual(report.rejectedDomains, ['prospects; DROP TABLE prospects', 'raw_sql']);
    assert.deepEqual(Object.keys(report.domains), ['crm_pipeline']);
    assert.equal(calls.length, 1);
  });

  it('a foreign-tenant frame is refused before any read; a frame for another tenant is invalid', async () => {
    let reads = 0;
    const pool = { query: async () => { reads += 1; return { rows: [] }; } };
    const reg = new EvidenceRegistry({ pool, now: () => NOW, env: {} });
    assert.equal((await reg.retrieveForFrame(frame({ foreignTenantReference: true }), { tenantId: '21' })).refused, 'foreign_tenant_reference');
    assert.equal((await reg.retrieveForFrame(frame(), { tenantId: '10' })).refused, 'invalid_frame');
    assert.equal((await reg.retrieveForFrame({ ...frame(), authority: { grantsAuthority: true } }, { tenantId: '21' })).refused, 'invalid_frame');
    assert.equal(reads, 0);
  });

  it('an invalid tenant is refused', async () => {
    assert.equal((await registry().retrieve({ tenantId: "21' OR 1=1", domains: ['crm_pipeline'] })).refused, 'invalid_tenant');
  });
});

describe('read-only boundary', () => {
  it('a loader that attempts a write is blocked by the guard and reported as failed', async () => {
    const writes = [];
    const pool = { query: async (sql) => { writes.push(sql); return { rows: [] }; } };
    const reg = new EvidenceRegistry({ pool, now: () => NOW, env: {}, loaders: { snapshot: emptySnapshot, loadProspectRecords: async ({ db }) => { await db.query("UPDATE prospects SET status = 'hot'"); return { status: 'found', rows: [] }; } } });
    const report = await reg.retrieve({ tenantId: '21', domains: ['crm_pipeline'] });
    assert.equal(report.domains.crm_pipeline.status, 'failed');
    assert.equal(report.readOnly.writesBlocked, 1);
    assert.ok(!writes.some((s) => /UPDATE/i.test(s)), 'the write never reached the pool');
  });

  it('registry sources contain no write SQL and nothing in production imports the registry', () => {
    const dir = path.join(__dirname, '..');
    for (const f of ['EvidenceRegistry.js', 'EvidenceDiagnosis.js', 'LedgerReferentContract.js']) {
      const src = fs.readFileSync(path.join(dir, f), 'utf8');
      assert.doesNotMatch(src, /\b(INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE|TRUNCATE|FOR\s+UPDATE)\b/i, f);
      assert.doesNotMatch(src, /routeExecutionRequest|executeMissionStage|resolvePendingOperatorDecision|sendEmail|runScout|advance\w*AfterApproval/, f);
    }
    const root = path.resolve(__dirname, '../../../..');
    const importers = [];
    const walk = (d) => {
      for (const e of fs.readdirSync(d, { withFileTypes: true })) {
        const p = path.join(d, e.name);
        if (/node_modules|\/tests?\b|\/evidence\b/.test(p)) continue;
        if (e.isDirectory()) walk(p);
        else if (e.name.endsWith('.js')) {
          // Resolve every require() and flag only those that land in packages/max/evidence.
          const target = path.join(root, 'packages/max/evidence');
          for (const [, spec] of fs.readFileSync(p, 'utf8').matchAll(/require\(\s*['"]([^'"]+)['"]\s*\)/g)) {
            if (spec.startsWith('.') && path.resolve(path.dirname(p), spec).startsWith(target)) importers.push(path.relative(root, p));
          }
        }
      }
    };
    for (const d of ['packages', 'routes', 'services', 'utils']) walk(path.join(root, d));
    assert.deepEqual(importers, []);
  });
});

describe('malformed and unavailable evidence', () => {
  it('malformed loader output is treated as failed; another client\'s rows are dropped', async () => {
    const odd = await registry({ loaders: { loadProspectRecords: async () => ({ status: 'great', rows: [row('p1', 21)] }) } }).retrieve({ tenantId: '21', domains: ['crm_pipeline'] });
    assert.equal(odd.domains.crm_pipeline.status, 'failed');
    const mixed = await registry({ loaders: { loadProspectRecords: async () => ({ status: 'found', complete: true, rows: [row('p1', 21), row('p2', 10), { client_id: 21 }] }) } }).retrieve({ tenantId: '21', domains: ['crm_pipeline'] });
    assert.deepEqual(mixed.domains.crm_pipeline.items.map((i) => i.subject.id), ['p1']);
    const thrown = await registry({ loaders: { loadProspectRecords: async () => { throw new Error('boom'); } } }).retrieve({ tenantId: '21', domains: ['crm_pipeline'], referents: [{ kind: 'prospect', id: 'p1' }] });
    assert.equal(thrown.domains.crm_pipeline.status, 'failed');
    assert.equal(thrown.revalidation.results[0].status, 'retrieval_failed');
  });

  it('with no database every domain reports unavailability, never emptiness as fact', async () => {
    const report = await new EvidenceRegistry({ pool: null, now: () => NOW, env: {} }).retrieve({ tenantId: '21', domains: ['crm_pipeline', 'mission_lifecycle', 'agent_execution', 'outbound_authorization', 'campaign_performance'] });
    for (const d of ['crm_pipeline', 'mission_lifecycle', 'agent_execution', 'campaign_performance']) assert.equal(report.domains[d].status, 'unavailable', d);
    assert.ok(report.unknowns.length >= 5);
  });

  it('an incomplete bounded read never declares a remembered id deleted', async () => {
    const report = await registry({ loaders: { loadProspectRecords: async () => ({ status: 'found', complete: false, rows: [row('p1', 21)] }) } }).retrieve({ tenantId: '21', domains: ['crm_pipeline'], referents: [{ kind: 'prospect_set', memberIds: ['p1', 'p9'] }] });
    const status = Object.fromEntries(report.revalidation.results.map((r) => [r.id, r.status]));
    assert.deepEqual(status, { p1: 'current', p9: 'not_verifiable_within_bound' });
  });

  it('missing agent logs are unknowns, and a diagnosis without evidence says so', async () => {
    const report = await registry().retrieve({ tenantId: '10', domains: ['scout_execution'] });
    assert.ok(report.unknowns.some((u) => /does not establish that scout never ran/.test(u.reason)));
    const d = diagnoseScout(report);
    assert.ok(d.inferences.every((i) => i.certainty === 'inferred'));
    assert.ok(!JSON.stringify(d).includes('"verified"'));
  });
});

describe('ledger contract and ranking', () => {
  it('accepts the ledger focus shape without depending on ledger code, and bounds ids', async () => {
    const focus = { referent: { id: 'r1', kind: 'prospect_set', memberIds: ['p1', 'bad id!', 'p2'], scope: 'existing' }, remembered: true, verified: false, requiresRevalidation: true };
    assert.deepEqual(referentsFromMemory(focus), [{ kind: 'prospect_set', memberIds: ['p1', 'p2'] }]);
    assert.deepEqual(referentsFromMemory({ referent: { kind: 'mission', missionId: 'm1' } }), [{ kind: 'mission', missionId: 'm1' }]);
    assert.deepEqual(referentsFromMemory(null), []);
    const many = Array.from({ length: 40 }, (_, i) => `p${i}`);
    const reg = registry({ loaders: { loadProspectRecords: async () => ({ status: 'found', complete: true, rows: [] }) } });
    const { report } = await revalidateRemembered(reg, { tenantId: '21', memory: { referent: { kind: 'prospect_set', memberIds: many } } });
    assert.equal(report.revalidation.results.length, MAX_REFERENT_IDS);
    assert.deepEqual((await revalidateRemembered(reg, { tenantId: '21', memory: null })).reason, 'nothing_remembered_to_revalidate');
  });

  it('ties share a priority position; closed, expired and ineligible records are excluded with reasons', async () => {
    const rows = [row('a', 10, { status: 'hot', is_hot: true }), row('b', 10, { status: 'warm' }), row('c', 10, { status: 'warm' }),
      row('d', 10, { status: 'dead' }), row('e', 10, { acquisition_metadata: { category: 'c', reviewState: 'contact_now', expiresAt: days(1) } }), row('f', 10, { do_not_contact: true })];
    const report = await registry({ loaders: { loadProspectRecords: async () => ({ status: 'found', complete: true, rows }) } }).retrieve({ tenantId: '10', domains: ['crm_pipeline'] });
    const ranked = rankOpportunities(report, { top: 2 });
    assert.deepEqual(ranked.ranked.map((r) => [r.ref.split(':').pop(), r.position]), [['a', 1], ['b', 2], ['c', 2]]);
    assert.deepEqual(ranked.excluded.map((e) => [e.ref.split(':').pop(), e.reason]), [['d', 'opportunity closed'], ['e', 'opportunity expired'], ['f', 'does not meet recorded outreach gates']]);
  });
});
