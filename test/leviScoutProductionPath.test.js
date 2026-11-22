'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { runLeviScoutProductionCycle, authorityFor } = require('../services/leviScoutProductionRunner');
const { createProductionDeps } = require('../services/leviScoutProductionDeps');
const { loadApprovedCriteria, fromUnderstanding, proposedRangeIsNotAuthority } = require('../services/leviScoutApprovedCriteria');
const { tick, cycleIdFor, startLeviScoutWeeklyClock } = require('../services/leviScoutSchedule');
const { compactResult } = require('../services/leviScoutMaxDelivery');
const { researchEnabled, productionLimits, productionPermissions, AGENT } = require('../services/leviScoutEnablement');
const { smoke } = require('../scripts/leviScoutProductionSmoke');
const { TENANT_ID } = require('../services/leviScoutProductionContract');
const { shouldHandleScoutAcquisition } = require('../packages/max/workspace/ScoutAcquisitionContext');

const nycRow = {
  permit_si_no: 'NYC-100', owner_s_first_name: 'Ada', owner_s_last_name: 'Borrower',
  owner_s_business_name: 'Ada Holdings LLC', filing_date: '2026-11-02T00:00:00.000',
  borough: 'MANHATTAN', permit_type: 'NB', owner_s_house__: '10', owner_s_house_street_name: 'WALL STREET',
};
const chicagoRow = {
  permit_: 'CHI-200', contact_1_name: 'Ben Operator', issue_date: '2026-11-03T00:00:00.000',
  street_number: '100', street_name: 'MICHIGAN', permit_type: 'PERMIT',
};

const understanding = (services = 'Bridge loans. Financing range: $5,000–$20,000.') => ({
  _bundle: {
    tenantId: '21',
    knowledgeState: 'available',
    summary: {
      clientId: 21, approved: true, semanticAuthority: 'CANONICAL',
      canonicalSnapshotId: 'synthetic-approved-snapshot',
    },
    contract: { services },
  },
});

function fetchFor() {
  const calls = [];
  const fetchImpl = async (url) => {
    calls.push(String(url));
    if (/googleapis|serpapi|prospeo|hunter/i.test(url)) throw new Error(`forbidden_provider:${url}`);
    const rows = /cityofnewyork/.test(url) ? [nycRow] : /cityofchicago/.test(url) ? [chicagoRow] : [];
    return { ok: true, json: async () => rows };
  };
  fetchImpl.calls = calls;
  return fetchImpl;
}

test('production enablement is fail-closed and paid providers stay unused', () => {
  assert.equal(researchEnabled({}), false);
  assert.equal(AGENT, 'scout_levi');
  assert.deepEqual(productionLimits(), { maxCandidates: 20, maxProviderRequests: 0 });
  assert.equal(productionPermissions().outreach, false);
  assert.equal(productionPermissions().campaignEnrollment, false);
  const env = { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' };
  assert.equal(researchEnabled(env), true);
});

test('Blueprint criteria are used only when tenantCriteria trusts a single range; the proposed band is not invented', async () => {
  const proposed = proposedRangeIsNotAuthority();
  assert.equal(proposed.authorized, false);
  assert.equal(fromUnderstanding(undefined), null);
  assert.equal(fromUnderstanding({ _bundle: { tenantId: '21', knowledgeState: 'never_learned' } }), null);
  const loaded = await loadApprovedCriteria({ understanding: understanding() });
  assert.deepEqual(loaded, { approved: true, source: 'approved_blueprint', minAmount: 5000, maxAmount: 20000 });
  assert.equal(await loadApprovedCriteria({ understanding: understanding('Lending to investors.') }), null);
});

test('disabled production cycle never discovers, persists or sends outreach', async () => {
  await assert.rejects(runLeviScoutProductionCycle({ env: {}, fetch: fetchFor() }), { code: 'levi_research_disabled' });
});

test('missing Blueprint range blocks qualification without inventing $5k–$20k', async () => {
  const result = await runLeviScoutProductionCycle({
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' },
    fetch: fetchFor(),
    understanding: understanding('Lending to investors.'),
    sink: async () => {},
  });
  assert.equal(result.status, 'blocked_financing_criteria_unverified');
  assert.equal(result.qualified, 0);
  assert.equal(result.discovered, 0);
  assert.equal(result.outreachAttempted, false);
});

test('a production cycle discovers NYC and Chicago, persists review-only, and never qualifies a permit', async () => {
  const cards = [];
  const fetchImpl = fetchFor();
  const result = await runLeviScoutProductionCycle({
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' },
    fetch: fetchImpl,
    understanding: understanding(),
    now: new Date('2026-11-18T12:00:00Z'),
    sink: async card => cards.push(card),
  });
  assert.equal(result.tenantId, String(TENANT_ID));
  assert.equal(result.status, 'completed');
  assert.equal(result.discovered, 2);
  assert.deepEqual([...result.cities].sort(), ['Chicago', 'New York']);
  assert.equal(result.qualified, 0);
  assert.equal(result.outreachAttempted, false);
  assert.equal(result.paidProvidersUsed, false);
  assert.ok(fetchImpl.calls.every(url => /cityofnewyork|cityofchicago/.test(url)));
  assert.equal(cards.length, 1);
  assert.equal(cards[0].payload.outreachAttempted, false);
  assert.match(cards[0].description, /outbound stayed off/i);
});

test('weekly clock is idempotent for the same cycle id', async () => {
  const now = new Date('2026-11-18T15:00:00Z');
  const env = { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' };
  let runs = 0;
  const run = async () => { runs += 1; return { status: 'completed' }; };
  const first = await tick({ now, env, run });
  const second = await tick({ now, env, run, lastCycleId: first.cycleId });
  assert.equal(first.cycleId, cycleIdFor(now));
  assert.equal(first.ran, true);
  assert.equal(second.ran, false);
  assert.equal(second.status, 'already_ran');
  assert.equal(runs, 1);
  assert.equal(first.outreachAttempted, false);
});

test('the weekly clock does not arm when research is disabled', async () => {
  const started = await startLeviScoutWeeklyClock({ env: {}, run: async () => { throw new Error('should not run'); } });
  assert.equal(started.started, false);
  assert.equal(started.reason, 'levi_research_disabled');
});

test('in-memory production deps claim once, skip tenant 10, and keep review-only flags', async () => {
  const deps = createProductionDeps({ fetch: fetchFor(), env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' } });
  const first = await deps.claimRun({ cycleId: 'levi-week-2026-11-16', authorityHash: 'abc' });
  const replay = await deps.claimRun({ cycleId: 'levi-week-2026-11-16', authorityHash: 'abc' });
  assert.equal(first.claimed, true);
  assert.equal(replay.claimed, false);
  const rows = await deps.discover({ maxCandidates: 20, maxProviderRequests: 0 });
  assert.equal(rows.length, 2);
  const persisted = await deps.persistReviewOnly({ records: rows.map(candidate => ({ candidate, qualified: false, blockers: ['borrower_intent_unverified'], outreachAuthorized: false, campaignEnrollmentAuthorized: false, setterVisible: false, aoOutreachReviewRequired: true })), cycleId: 'levi-week-2026-11-16' });
  assert.equal(persisted.persisted, 2);
  assert.equal(persisted.records[0].setterVisible, false);
  assert.equal(persisted.records[0].aoOutreachReviewRequired, true);
});

test('authority for production never enables outreach or paid provider requests', () => {
  const authority = authorityFor({ cycleId: 'levi-week-2026-11-16', criteria: { approved: true, source: 'approved_blueprint', minAmount: 5000, maxAmount: 20000 } });
  assert.equal(authority.tenantId, '21');
  assert.equal(authority.limits.maxProviderRequests, 0);
  assert.equal(authority.permissions.outreach, false);
  assert.equal(authority.kind, 'levi_scout_research_only');
});

test('Max compact result always reports outbound and paid providers off', () => {
  const snapshot = compactResult({ discovered: 3, qualified: 0, cities: ['New York'] });
  assert.equal(snapshot.outreachAttempted, false);
  assert.equal(snapshot.paidProvidersUsed, false);
  assert.equal(snapshot.tenantId, '21');
});

test('smoke reports discovered count, cities, qualified and outbound=false', async () => {
  const disabled = await smoke({ env: {} });
  assert.deepEqual(disabled, { discovered: 0, cities: [], qualified: 0, outbound: false, status: 'disabled' });
  const enabled = await smoke({
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' },
    fetch: fetchFor(),
    now: new Date('2026-11-18T12:00:00Z'),
    understanding: understanding(),
  });
  assert.equal(enabled.outbound, false);
  assert.equal(enabled.qualified, 0);
  assert.equal(enabled.discovered, 2);
  assert.deepEqual([...enabled.cities].sort(), ['Chicago', 'New York']);
});

test('Max workspace can route tenant 21 Levi Scout without enabling Places scout', () => {
  assert.equal(shouldHandleScoutAcquisition({
    question: 'What did Levi Scout find this week?',
    context: { enabledAgents: ['scout_levi', 'max'], tenantId: '21' },
  }), true);
  assert.equal(shouldHandleScoutAcquisition({
    question: 'Who should we call in Manchester?',
    context: { enabledAgents: ['max'], tenantId: '10' },
  }), false);
});
