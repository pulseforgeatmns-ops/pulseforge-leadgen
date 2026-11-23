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
const { shouldHandleScoutAcquisition, maybeHandleScoutAcquisitionTurn } = require('../packages/max/workspace/ScoutAcquisitionContext');
const { inspectApprovedBlueprint } = require('../services/leviScoutBlueprintReadiness');

const NOW = new Date('2026-11-22T15:00:00Z');
const nycRow = {
  job_filing_number: 'B00-TEST-1', job_type: 'Alteration', first_permit_date: '2026-11-18T00:00:00.000',
  owner_s_business_name: 'Ada Holdings LLC', house_no: '10', street_name: 'WALL STREET', borough: 'Manhattan',
  job_description: 'Interior renovation of dwelling units', sidewalk_shed_work_type_: 'NO',
};
const chicagoRow = {
  permit_: '100519123', permit_type: 'PERMIT - RENOVATION/ALTERATION', issue_date: '2026-11-18T00:00:00.000',
  contact_1_type: 'OWNER', contact_1_name: 'Ben Operator LLC', contact_1_city: 'SKOKIE',
  street_number: '100', street_direction: 'N', street_name: 'MICHIGAN', suffix: 'AVE',
  work_description: 'Interior renovation of a two-flat dwelling',
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
    if (/googleapis|serpapi|prospeo|hunter|apollo/i.test(url)) throw new Error(`forbidden_provider:${url}`);
    const id = (/resource\/([a-z0-9]{4}-[a-z0-9]{4})/.exec(String(url)) || [])[1];
    const rows = id === 'w9ak-ipjd' ? [nycRow]
      : id === 'ydr8-5enu' ? [chicagoRow]
        : [];
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

test('missing Blueprint range still discovers and persists Qualify First without inventing $5k–$20k', async () => {
  const result = await runLeviScoutProductionCycle({
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' },
    fetch: fetchFor(),
    understanding: understanding('Lending to investors.'),
    now: NOW,
    sink: async () => {},
  });
  assert.equal(result.status, 'completed');
  assert.ok(result.discovered >= 1);
  assert.equal(result.qualified, 0);
  assert.equal(result.outreachAttempted, false);
  assert.equal(authorityFor({ cycleId: 'levi-week-2026-11-16' }).criteria.source, 'approved_blueprint');
  assert.equal(authorityFor({ cycleId: 'levi-week-2026-11-16' }).criteria.approved, false);
  assert.equal(Object.prototype.hasOwnProperty.call(authorityFor({ cycleId: 'levi-week-2026-11-16' }).criteria, 'minAmount'), false);
});

test('a production cycle discovers NYC and Chicago, persists review-only, and never qualifies a permit', async () => {
  const cards = [];
  const fetchImpl = fetchFor();
  const result = await runLeviScoutProductionCycle({
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' },
    fetch: fetchImpl,
    understanding: understanding(),
    now: NOW,
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

test('the weekly clock starts at boot even when research is disabled, and ticks no-op', async () => {
  const timers = { setTimeout() { return 0; } };
  const started = await startLeviScoutWeeklyClock({
    env: {},
    timers,
    run: async () => { throw new Error('should not run'); },
  });
  assert.equal(started.started, true);
  assert.equal(started.armed, false);
  const idle = await tick({ env: {}, run: async () => { throw new Error('should not run'); } });
  assert.equal(idle.ran, false);
  assert.equal(idle.status, 'disabled');
  assert.equal(idle.outreachAttempted, false);
});

test('in-memory production deps claim once, skip tenant 10, and keep review-only flags', async () => {
  const deps = createProductionDeps({ fetch: fetchFor(), env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' }, now: NOW });
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

test('smoke discovers with persist dry-run even when the research flag is unset', async () => {
  const disabled = await smoke({ env: {}, fetch: fetchFor(), now: NOW });
  assert.equal(disabled.outbound, false);
  assert.equal(disabled.qualified, 0);
  assert.equal(disabled.persist, 'dry-run');
  assert.ok(disabled.discovered >= 1);
  assert.deepEqual([...disabled.cities].sort(), ['Chicago', 'New York']);
});

test('Max tenant-21 find/discover/prospects/borrowers questions do not require the words levi scout', () => {
  assert.equal(shouldHandleScoutAcquisition({
    question: 'Find borrowers in New York this week',
    context: { enabledAgents: ['max'], tenantId: '21' },
  }), true);
  assert.equal(shouldHandleScoutAcquisition({
    question: 'Discover prospects for the Chicago book',
    context: { enabledAgents: ['max'], tenantId: '21' },
  }), true);
  assert.equal(shouldHandleScoutAcquisition({
    question: 'Who should we call in Manchester?',
    context: { enabledAgents: ['max'], tenantId: '10' },
  }), false);
});

test('tenant 21 Max review does not call Places or the general Scout loop', async () => {
  let loopCalled = 0;
  const turn = await maybeHandleScoutAcquisitionTurn({
    question: 'Discover prospects for the Chicago book',
    context: { enabledAgents: ['max'], tenantId: '21' },
    runLoop: async () => { loopCalled += 1; throw new Error('general_scout_loop_forbidden'); },
    reviewRecords: [],
    understanding: understanding('Lending to investors.'),
  });
  assert.equal(loopCalled, 0);
  assert.equal(turn.reason, 'levi_scout_research_review');
  assert.equal(turn.structured.metadata.operationalRead.paidProvidersUsed, false);
  const missing = inspectApprovedBlueprint(understanding('Lending to investors.')).questionsForLevi;
  assert.ok(missing.length >= 1);
  assert.match(turn.prose, /Missing Blueprint decisions/);
});

test('property-record identity keys stay entity-based after public phone enrichment', () => {
  const { identityKey } = require('../services/leviScoutResearchOnlyExecution');
  const row = {
    name: 'Ben Operator LLC',
    company: 'Ben Operator LLC',
    phone: '(312) 555-0199',
    service_area_match: '100 N MICHIGAN AVE, Chicago, IL',
    acquisition_metadata: { sourceType: 'property_record_signal' },
  };
  assert.equal(
    identityKey(row),
    identityKey({ ...row, phone: null })
  );
  assert.match(identityKey(row), /^entity:ben operator llc\|/);
});
