'use strict';

/**
 * P0 — Max question-directed reasoning over operational evidence
 * (follow-up to #1089 / PR #1090).
 *
 * Production (Nov 8): after a read-only inspection of Anchor Cleaning and an
 * analytical summary, the operator asked
 *
 *   "Why is the CRM preparation mission classified as stale, and what evidence
 *    would justify treating it as current again? Do not approve, reject,
 *    cancel, resume, or modify that mission. Explain only, using the evidence
 *    from your previous answer."
 *
 * and Max returned the whole portfolio analysis again (canonical objective,
 * 48 stale missions, 8 ambiguous, outbound gate, pending decisions, limits)
 * without answering the question.
 *
 * Every test drives the real WorkspaceEngine.ask() path against a
 * production-shaped persisted portfolio and asserts response CONTENT, not
 * only routing labels, plus the absence of any mutation.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const A = require('../AmoOperatorApproval');
const { createHermeticWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime, liveScoutStub } = require('./amoTestRuntime');
const { buildAnalyticalFrame } = require('../OperationalQuestionFrame');

const TENANT = '10';
// Late on Nov 8 (UTC): the CRM mission's last activity (Oct 25 22:41:53Z) is
// just over 14 days old, as in production.
const NOW = new Date('2026-11-08T23:30:00Z');
const daysAgo = (n) => new Date(NOW.getTime() - n * 24 * 60 * 60 * 1000).toISOString();

const CRM_ID = 'mission_crm_ddf50ad75bfdde8f77d7ed9b8b3bd83a';
const CRM_TITLE = 'CRM preparation — facility buyer records';
const CRM_LAST = '2026-10-25T22:41:53.000Z';
const GOV_ID = 'am_governed_outbound_6c1e';
const GOV_TITLE = 'Governed outbound — facility buyers';
const GOV_LAST = '2026-10-18T09:12:00.000Z';
const OTHER_TENANT_ID = 'am_probate_partners_77';

const INSPECT =
  'Give me a read-only operational status report for Anchor Cleaning. Identify the current mission state, active execution blockers, and supporting evidence. Distinguish active business missions from completed, stale, or development/test missions, but do not archive, delete, or modify anything.';
const SUMMARY =
  'Using the operational evidence you just retrieved for Anchor Cleaning, identify the single canonical business objective, the missions genuinely relevant to that objective, and the historical or ambiguous missions that should not be treated as current execution work. Explain why outbound is intentionally paused, distinguish the safety gate from actual execution failures, and recommend the next reliability validation step. Do not change session settings, approve or resume missions, alter the outbound gate, create new missions, or execute anything. Cite persisted evidence and explicitly identify anything you cannot verify.';
// Verbatim production follow-up.
const CRM_QUESTION =
  'Why is the CRM preparation mission classified as stale, and what evidence would justify treating it as current again? Do not approve, reject, cancel, resume, or modify that mission. Explain only, using the evidence from your previous answer.';

const BLUEPRINT = {
  id: 'bp_anchor',
  clientId: 10,
  status: 'approved',
  summary: { campaignGoals: 'Win recurring commercial cleaning contracts with facility buyers in Greater Manchester.' },
};

function amoRow(id, f) {
  const tenant = f.tenant || TENANT;
  return {
    id,
    tenant_id: tenant,
    client_id: Number(tenant),
    stage: f.stage,
    status: f.status,
    title: f.title,
    objective: f.objective || f.title,
    payload: { id, tenantId: tenant, ...(f.payload || {}) },
    created_at: f.created_at || daysAgo(60),
    updated_at: f.updated_at,
  };
}

const EXEC_PENDING = { pendingOperatorDecision: { kind: 'execution_approval', prompt: 'Authorize external execution of prepared outreach?' } };
const STALE_COUNT = 5;

/** Production-shaped: no active non-synthetic mission; stale CRM + governed outbound; stale with pending decisions. */
function fixtureRows() {
  return {
    acquisition_missions: [
      amoRow(CRM_ID, {
        stage: 'ready', status: 'Ready', title: CRM_TITLE,
        objective: 'Prepare CRM records for Greater Manchester facility buyers ahead of governed outreach.',
        created_at: '2026-10-14T10:05:00.000Z', updated_at: CRM_LAST, payload: EXEC_PENDING,
      }),
      amoRow(GOV_ID, {
        stage: 'execute', status: 'Executing', title: GOV_TITLE,
        objective: 'Send governed first-touch outreach to approved facility buyers.',
        created_at: '2026-10-16T15:02:00.000Z', updated_at: GOV_LAST, payload: EXEC_PENDING,
      }),
      ...Array.from({ length: STALE_COUNT }, (_, i) => amoRow(`am_hist_${i}`, {
        stage: 'discover', status: 'Discovering', title: `Historical segment ${i}`,
        updated_at: daysAgo(30 + i),
        payload: { pendingOperatorDecision: { kind: 'discovery_approval', prompt: 'Approve discovery?' } },
      })),
      amoRow('am_done', { stage: 'improve', status: 'Completed', title: 'Accounting pilot', updated_at: daysAgo(20) }),
      amoRow('am_untimed', { stage: 'discover', status: 'Discovering', title: 'Imported segment', updated_at: null }),
      amoRow('am_probe', { stage: 'discover', status: 'Discovering', title: 'Probe mission', updated_at: daysAgo(3) }),
      amoRow(OTHER_TENANT_ID, { tenant: '2', stage: 'execute', status: 'Executing', title: 'Probate partners', updated_at: daysAgo(1) }),
    ],
    acquisition_mission_events: [
      { mission_id: CRM_ID, tenant_id: TENANT, kind: 'stage_committed', label: 'Preparation committed', at: CRM_LAST },
    ],
    clients: [
      { id: 1, name: 'Pulseforge', slug: 'pulseforge' },
      { id: 2, name: 'MSHI', slug: 'mshi' },
      { id: 10, name: 'Anchor Cleaning', slug: 'cleaning-co' },
    ],
    prospects: { 10: { total: 211, qualified: 64, setter_visible: 40, do_not_contact: 5, last_added_at: daysAgo(3), last_contacted_at: daysAgo(12) }, 2: { total: 777 } },
    touchpoints: { 10: [{ channel: 'email', last_7d: 0, last_30d: 18, last_at: daysAgo(12) }] },
    agent_log: { 10: [{ agent_name: 'scout', last_run_at: daysAgo(1), runs_7d: 6, errors_7d: 0 }] },
  };
}

function createFixturePool() {
  const rows = fixtureRows();
  const statements = [];
  return {
    rows,
    statements,
    async query(sql, params = []) {
      const text = String(sql);
      statements.push(text.replace(/\s+/g, ' ').trim());
      const key = String(params[0]);
      const id = params[1] != null ? String(params[1]) : null;
      if (/FROM clients/.test(text)) return { rows: rows.clients };
      if (/FROM acquisition_mission_events/.test(text)) {
        return { rows: rows.acquisition_mission_events.filter((r) => r.tenant_id === key && (!id || r.mission_id === id)) };
      }
      if (/FROM acquisition_missions/.test(text)) {
        return { rows: rows.acquisition_missions.filter((r) => r.tenant_id === key && (!id || r.id === id)) };
      }
      if (/FROM missions/.test(text)) return { rows: [] };
      if (/FROM prospects/.test(text)) return { rows: [rows.prospects[key] || { total: 0 }] };
      if (/FROM touchpoints/.test(text)) return { rows: rows.touchpoints[key] || [] };
      if (/FROM agent_log/.test(text)) return { rows: rows.agent_log[key] || [] };
      if (/to_regclass/.test(text)) return { rows: [{ control_table: 'anchor_email_recovery_control' }] };
      if (/FROM anchor_email_recovery_control/.test(text)) return { rows: [{ singleton: true, maintenance_armed: true, manifest: null }] };
      throw new Error(`unexpected statement: ${text.slice(0, 80)}`);
    },
  };
}

/** In-process mission at execution_approval, bound to the session. */
async function readyMission(engine = amo.createAcquisitionMissionEngine()) {
  const mission = engine.create({ tenantId: TENANT, objective: 'Acquire recurring commercial cleaning customers for Anchor Cleaning in Greater Manchester.', targetSegment: 'Facility buyers' });
  const g = () => engine.get(mission.id, TENANT);
  await A.advancePlanAfterApproval({ engine, mission, tenantId: TENANT, question: 'Approved.' });
  await A.advanceDiscoveryAfterApproval({ engine, mission, tenantId: TENANT, question: 'Approved. Begin Discovery.', runScout: liveScoutStub });
  await A.advancePrioritizationAfterApproval({ engine, mission: g(), tenantId: TENANT, question: 'Approved prioritization.' });
  await A.advanceMaxPrioritization({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true });
  await A.advanceAcquisitionApproach({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true });
  await A.advancePaigeVariants({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true, question: 'Generate variants.' });
  await A.advanceEmmettCapacity({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true, question: 'Plan capacity.' });
  assert.equal(g().pendingOperatorDecision.kind, 'execution_approval');
  return { engine, mission };
}

async function openWorkspace({ engine = amo.createAcquisitionMissionEngine(), pool = createFixturePool(), bindMissionId = null } = {}) {
  const ws = createHermeticWorkspaceEngine({
    disableLlm: true,
    missionsEnabled: true,
    resolverEnabled: true,
    clientIntelligenceService: { getApprovedClientBlueprint: async () => BLUEPRINT },
    acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    operationalInspectionOpts: { pool, now: NOW, env: { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true' } },
  });
  const opened = await ws.open({ tenantId: TENANT, page: 'command-deck' });
  const session = ws._sessions.get(opened.sessionId);
  if (bindMissionId) {
    session.context.missionId = bindMissionId;
    session.context.acquisitionMissionId = bindMissionId;
  }
  return { ws, session, pool, engine, ask: (question) => ws.ask({ sessionId: opened.sessionId, question }) };
}

function fingerprint(engine) {
  return JSON.stringify(engine.list(TENANT).map((m) => ({ m, e: engine.store.listEvents(m.id), c: engine.store.listContributions(m.id) })));
}

function assertOnlyReads(pool) {
  for (const s of pool.statements) assert.match(s, /^(SELECT|WITH)\b/i, `non-read statement reached the database: ${s}`);
}

function assertMissionAnswer(result, missionIds) {
  assert.equal(result.workspaceOwnership.owner, 'operational_analysis');
  assert.equal(result.resolution.action, 'operational_analyzed');
  assert.equal(result.mission, null);
  assert.deepEqual(result.recommendedActions, []);
  const frame = result.operationalEvidence.analyticalFrame;
  assert.equal(frame.subject, 'mission');
  assert.equal(frame.responseScope, 'focused');
  assert.equal(frame.actionAuthorized, false);
  assert.deepEqual(frame.missionIds, missionIds);
  assert.doesNotMatch(result.prose, /This approval is the next operator gate|## Execution Ready|Authorize external execution of (?:this )?prepared outreach\?\s*$/m);
}

/** The full portfolio report must not be repeated for a narrow question. */
function assertNotPortfolioReport(prose) {
  assert.doesNotMatch(prose, /^Operational analysis —/m);
  assert.doesNotMatch(prose, /Canonical business objective/);
  assert.doesNotMatch(prose, /Stale \(\d+\)|Historical — should not be treated/);
  assert.doesNotMatch(prose, /Historical segment \d|Accounting pilot|Imported segment|Probe mission/);
  assert.doesNotMatch(prose, /Email touchpoints|outbound gate is in|recovery_kill_switch/);
  assert.doesNotMatch(prose, /\b211\b|prospects?\b/i);
  assert.doesNotMatch(prose, /Recommended next reliability validation step/);
  assert.doesNotMatch(prose, /hold a pending operator decision \(/);
  assert.ok(prose.length < 3200, `focused answer is too long (${prose.length} chars)`);
}

beforeEach(() => amo.clearExecutionRouterAudit());

describe('A — exact production reproduction', { concurrency: false }, () => {
  it('inspection → summary → CRM follow-up answers the CRM question, not the portfolio', async () => {
    const { engine, mission } = await readyMission();
    const { ask, pool, session } = await openWorkspace({ engine, bindMissionId: mission.id });
    const first = await ask(INSPECT);
    assert.equal(first.workspaceOwnership.owner, 'operational_inspection');
    const summary = await ask(SUMMARY);
    assert.equal(summary.workspaceOwnership.owner, 'operational_analysis');
    assert.match(summary.prose, /^1\. Canonical business objective$/m, 'the summary is still the full report');
    const before = fingerprint(engine);
    const policyBefore = JSON.stringify(session.context.sessionState || null);

    const result = await ask(CRM_QUESTION);
    assertMissionAnswer(result, [CRM_ID]);
    const p = result.prose;
    // Direct conclusion first, from the retained evidence.
    assert.match(p, new RegExp(`^"${CRM_TITLE}" \\[${CRM_ID}\\] is classified as stale because its last recorded activity was 2026-10-25 22:41:53Z, and the inspection ran at 2026-11-08 23:30:00Z — 14\\.0 days later, just past the 14-day inactivity threshold`));
    assert.match(p, /the classification is marginal and turns entirely on that one timestamp/);
    assert.match(p, /Observed fact: last recorded activity 2026-10-25 22:41:53Z \(latest mission event \("Preparation committed", "stage_committed"\)\); stage ready \/ status Ready/);
    assert.match(p, /Classification rule: a non-terminal mission with no recorded activity for more than 14 days is stale/);
    // Classification is not a business conclusion.
    assert.match(p, /does not show the mission is abandoned, invalid, or superseded/);
    // Mission-specific criteria for reconsidering it.
    assert.match(p, /^What would justify treating it as current again$/m);
    assert.match(p, /Relationship to the present business objective\. Its persisted objective: "Prepare CRM records for Greater Manchester facility buyers/);
    assert.match(p, /No active, non-synthetic mission carries a persisted objective/);
    assert.match(p, /Its latest persisted lifecycle state: stage ready \/ status Ready/);
    assert.match(p, /Whether the work it prepared is still valid/);
    assert.match(p, /A deliberate operator decision to retain it as active work: none is persisted/);
    assert.match(p, /^Not sufficient on its own$/m);
    assert.match(p, /Its pending decision \(Authorize external execution of prepared outreach\? \(execution_approval\)\) has stayed open with no recorded activity since 2026-10-25 22:41:53Z\. That makes it historical state: an open approval request is not evidence that the mission should resume\./);
    assert.match(p, /New activity alone[^\n]*does not establish business relevance/);
    // Evidence basis and constraints.
    assert.match(p, /Evidence: the read-only inspection retrieved earlier in this conversation \(as of 2026-11-08 23:30:00Z\)/);
    assert.match(p, /Your constraints were honoured: "Do not approve, reject, cancel, resume, or modify that mission"/);
    assert.match(p, /No changes were made\.$/);
    assertNotPortfolioReport(p);
    assert.doesNotMatch(p, new RegExp(GOV_TITLE));

    // Structured contract.
    const ev = result.operationalEvidence;
    assert.equal(ev.evidenceUse, 'reused');
    assert.deepEqual(ev.analyticalFrame.facets, ['classification', 'reconsideration']);
    assert.equal(ev.analyticalFrame.reference, 'description');
    assert.ok(ev.uncertainties.some((u) => /parked, abandoned, or superseded/.test(u)));
    assert.deepEqual(ev.pendingDecisions, { present: STALE_COUNT + 2, requested: 0, consumed: 0 });

    // Nothing changed.
    assert.equal(fingerprint(engine), before);
    assert.equal(JSON.stringify(session.context.sessionState || null), policyBefore);
    assert.equal(engine.get(mission.id, TENANT).pendingOperatorDecision.kind, 'execution_approval');
    assert.deepEqual(amo.listExecutionRouterAudit(), []);
    assertOnlyReads(pool);
  });
});

describe('B–D — different questions over the same evidence', { concurrency: false }, () => {
  it('B: relevance — separates supporting evidence from missing evidence', async () => {
    const { ask } = await openWorkspace();
    await ask(INSPECT);
    const r = await ask("What evidence suggests the CRM preparation mission is still relevant to Anchor's current objective?");
    assertMissionAnswer(r, [CRM_ID]);
    assert.deepEqual(r.operationalEvidence.analyticalFrame.facets, ['relevance']);
    assert.equal(r.operationalEvidence.analyticalFrame.intent, 'evaluation');
    const p = r.prose;
    assert.match(p, /^The persisted evidence neither establishes that "CRM preparation — facility buyer records" is relevant to the current objective nor that it is irrelevant\./);
    assert.match(p, /^Evidence that supports relevance:$/m);
    assert.match(p, /It is non-terminal: no completed, cancelled, or archived status is persisted/);
    assert.match(p, /shares terms with the declared business goal \([^)]*facility[^)]*\)\. That is textual overlap, not proof that it is the same work/);
    assert.match(p, /^Evidence that is missing$/m);
    assert.match(p, /Recent activity: none for 14\.0 days/);
    assert.match(p, /A persisted current objective: no active, non-synthetic mission carries one/);
    assert.match(p, /Whether the work it prepared is still valid/);
    assert.doesNotMatch(p, /^What would justify treating it as current again$/m, 'not asked');
    assertNotPortfolioReport(p);
  });

  it('C: business judgment versus classification — stale is not abandonment', async () => {
    const { ask } = await openWorkspace();
    await ask(INSPECT);
    const r = await ask('Does stale mean we should abandon the CRM preparation mission?');
    assertMissionAnswer(r, [CRM_ID]);
    assert.deepEqual(r.operationalEvidence.analyticalFrame.facets, ['abandonment']);
    const p = r.prose;
    assert.match(p, /^No\. Stale means "CRM preparation — facility buyer records" has had no recorded activity for more than 14 days; that alone does not establish that it should be abandoned\./);
    assert.match(p, /Observed fact: last recorded activity 2026-10-25 22:41:53Z/);
    assert.match(p, /Classification rule: inactivity beyond 14 days on a non-terminal mission → stale/);
    assert.match(p, /Business interpretation: the mission may be parked, abandoned, superseded, or still relevant\. Persisted state does not distinguish these\./);
    assert.match(p, /Decision requirement: abandoning it \(closing or cancelling it\) is an operator decision[^\n]*I have not changed it\./);
    assertNotPortfolioReport(p);
  });

  it('D: comparison — actual timestamps, no unsupported business conclusion', async () => {
    const { ask } = await openWorkspace();
    await ask(INSPECT);
    const r = await ask('Compare the CRM preparation mission with the October 16 governed outbound mission. Which has more recent recorded activity, and what can we conclude?');
    assertMissionAnswer(r, [CRM_ID, GOV_ID]);
    assert.equal(r.operationalEvidence.analyticalFrame.intent, 'comparison');
    const p = r.prose;
    assert.match(p, /^"CRM preparation — facility buyer records" has the more recent recorded activity: 2026-10-25 22:41:53Z, 7\.6 days after "Governed outbound — facility buyers" \(2026-10-18 09:12:00Z\)\./);
    assert.match(p, new RegExp(`"${GOV_TITLE}" \\[${GOV_ID}\\]: last recorded activity 2026-10-18 09:12:00Z \\(acquisition_missions\\.updated_at\\); created 2026-10-16 15:02:00Z; stage execute / status Executing; stale`));
    assert.match(p, /Both are stale — both are beyond the 14-day threshold, so the gap does not change either classification/);
    assert.match(p, /What this establishes: only which mission was touched more recently/);
    assert.match(p, /What it does not establish: which is more valuable or more relevant to the current objective/);
    assert.doesNotMatch(p, /should (?:prioriti[sz]e|resume|abandon|keep)/i);
    assertNotPortfolioReport(p);
  });

  it('an ambiguous description asks which mission is meant instead of guessing', async () => {
    const { ask } = await openWorkspace();
    await ask(INSPECT);
    const r = await ask('Why is the facility buyers mission stale?');
    assert.equal(r.workspaceOwnership.owner, 'operational_analysis');
    assert.deepEqual(r.operationalEvidence.analyticalFrame.missionIds, []);
    assert.match(r.prose, /"the facility buyers mission" matches 2 missions in the evidence; which one do you mean\?/);
    assert.match(r.prose, new RegExp(`${CRM_ID}[\\s\\S]*${GOV_ID}|${GOV_ID}[\\s\\S]*${CRM_ID}`));
    assert.doesNotMatch(r.prose, /is classified as stale because/);
  });
});

describe('E — mission absent from the evidence record', { concurrency: false }, () => {
  it('a mission created after the inspection is read read-only and disclosed as such', async () => {
    const { ask, pool } = await openWorkspace();
    await ask(INSPECT);
    pool.rows.acquisition_missions.push(amoRow('am_foh_research_1107', {
      stage: 'discover', status: 'Discovering', title: 'Restaurant FOH research', updated_at: '2026-11-07T16:00:00.000Z',
    }));
    const r = await ask('What happened to mission am_foh_research_1107?');
    assertMissionAnswer(r, ['am_foh_research_1107']);
    assert.match(r.prose, /"Restaurant FOH research" \[am_foh_research_1107\]/);
    assert.match(r.prose, /currently active/);
    assert.match(r.prose, /was not in that evidence; I read its record just now, read-only and scoped to this tenant/);
    assert.deepEqual(r.operationalEvidence.analyticalFrame.lookups, [{ found: true }]);
    assert.ok(pool.statements.some((s) => /FROM acquisition_missions WHERE tenant_id = \$1 AND id = \$2/.test(s)));
    assertOnlyReads(pool);
  });

  it('an unknown mission id is disclosed as unverifiable, never invented', async () => {
    const { ask, pool } = await openWorkspace();
    await ask(INSPECT);
    const r = await ask('Why is mission am_does_not_exist_42 stale?');
    assert.equal(r.workspaceOwnership.owner, 'operational_analysis');
    assert.match(r.prose, /I cannot find a mission with id am_does_not_exist_42 in Anchor Cleaning \(tenant 10\)'s persisted mission stores \(acquisition_missions, missions\), so I cannot verify anything about it/);
    assert.doesNotMatch(r.prose, /is classified as|last recorded activity was/);
    assert.deepEqual(r.operationalEvidence.analyticalFrame.lookups, [{ found: false }]);
    assertOnlyReads(pool);
  });

  it('an unmatched description is disclosed with a way forward', async () => {
    const { ask } = await openWorkspace();
    await ask(INSPECT);
    const r = await ask('Why is the dental referral mission stale?');
    assert.equal(r.workspaceOwnership.owner, 'operational_analysis');
    assert.match(r.prose, /I cannot match the dental referral mission to a mission in the evidence \(as of 2026-11-08 23:30:00Z\)\. Give me its mission id/);
  });
});

describe('F — conversational continuity', { concurrency: false }, () => {
  it('three successive questions keep the referent and answer each new question', async () => {
    const { ask } = await openWorkspace();
    await ask(INSPECT);
    const one = await ask('Why is the CRM preparation mission classified as stale?');
    assertMissionAnswer(one, [CRM_ID]);
    assert.deepEqual(one.operationalEvidence.analyticalFrame.facets, ['classification']);
    assert.doesNotMatch(one.prose, /^What would justify/m, 'not asked yet');

    const two = await ask('What would justify treating it as current again?');
    assertMissionAnswer(two, [CRM_ID]);
    assert.equal(two.operationalEvidence.analyticalFrame.reference, 'focus');
    assert.deepEqual(two.operationalEvidence.analyticalFrame.facets, ['reconsideration']);
    assert.match(two.prose, /^To determine whether "CRM preparation — facility buyer records" should be treated as current again/);
    assert.doesNotMatch(two.prose, /is classified as stale because/, 'does not repeat the previous answer');

    const three = await ask('Does stale mean we should abandon it?');
    assertMissionAnswer(three, [CRM_ID]);
    assert.deepEqual(three.operationalEvidence.analyticalFrame.facets, ['abandonment']);
    assert.match(three.prose, /^No\. Stale means "CRM preparation — facility buyer records"/);

    const four = await ask('How does it compare with the October 16 governed outbound mission?');
    assertMissionAnswer(four, [GOV_ID, CRM_ID]);
    assert.match(four.prose, /has the more recent recorded activity/);
  });

  it('the referent does not survive a turn owned by someone else', async () => {
    const { ask } = await openWorkspace();
    await ask(INSPECT);
    await ask('Why is the CRM preparation mission classified as stale?');
    await ask('What do you understand about my business?');
    const r = await ask('Why is it stale?');
    assert.notDeepEqual(r.operationalEvidence && r.operationalEvidence.analyticalFrame && r.operationalEvidence.analyticalFrame.missionIds, [CRM_ID]);
  });
});

describe('G — approval isolation on the named mission', { concurrency: false }, () => {
  it('a pending execution approval on the named mission is never requested, approved, rejected, or consumed', async () => {
    const { engine, mission } = await readyMission();
    const { ask, pool, session } = await openWorkspace({ engine, bindMissionId: mission.id });
    await ask(INSPECT);
    const before = fingerprint(engine);
    for (const q of [
      CRM_QUESTION,
      'Why does the CRM preparation mission still have a pending decision? Do not approve it, do not reject it, and do not cancel it.',
      'Is it still relevant? Do not resume or execute it.',
    ]) {
      const r = await ask(q);
      assert.equal(r.workspaceOwnership.owner, 'operational_analysis', q);
      assert.deepEqual(r.operationalEvidence.analyticalFrame.missionIds, [CRM_ID], q);
      assert.deepEqual(r.operationalEvidence.pendingDecisions, { present: STALE_COUNT + 2, requested: 0, consumed: 0 }, q);
      assert.doesNotMatch(r.prose, /Authorize external execution of (?:this )?prepared outreach\?\s*$|## Execution Ready/m, q);
      assert.notEqual(r.resolution.action, 'cancelled', q);
    }
    assert.equal(fingerprint(engine), before);
    assert.equal(engine.get(mission.id, TENANT).pendingOperatorDecision.kind, 'execution_approval');
    assert.notEqual(engine.get(mission.id, TENANT).planCancelled, true);
    assert.deepEqual(amo.listExecutionRouterAudit(), []);
    assert.equal(session.context.sessionState && session.context.sessionState.executionPolicy, undefined);
    assertOnlyReads(pool);
  });

  it('action language about the named mission is not analysis', () => {
    const record = { missions: [{ id: CRM_ID, title: CRM_TITLE, category: 'stale' }] };
    const { detectOperationalAnalysisFollowUp } = require('../OperationalEvidenceContinuity');
    const session = { context: { tenantId: TENANT }, operationalEvidence: { ...record, tenantId: TENANT, recordedAt: NOW.toISOString() } };
    for (const q of ['Resume the CRM preparation mission.', 'Approve the CRM preparation mission.', 'Cancel the CRM preparation mission', 'Run the CRM preparation mission now']) {
      assert.equal(detectOperationalAnalysisFollowUp(q, session, { now: NOW }).matched, false, q);
    }
    assert.equal(detectOperationalAnalysisFollowUp('Why is the CRM preparation mission stale?', session, { now: NOW }).matched, true);
  });
});

describe('H — legitimate execution routing is not intercepted', { concurrency: false }, () => {
  it('explicit execution approval after mission-specific analysis uses the existing approval workflow', async () => {
    const { engine, mission } = await readyMission();
    const { ask } = await openWorkspace({ engine, bindMissionId: mission.id });
    await ask(INSPECT);
    await ask(CRM_QUESTION);
    await ask('Does stale mean we should abandon it?');

    const shown = await ask("Let's run it");
    assert.notEqual(shown.workspaceOwnership && shown.workspaceOwnership.owner, 'operational_analysis');
    assert.match(shown.prose, /## Execution Ready/);

    amo.clearExecutionRouterAudit();
    const approved = await ask('Approve');
    assert.notEqual(approved.workspaceOwnership && approved.workspaceOwnership.owner, 'operational_analysis');
    assert.equal(approved.structured.metadata.executionState, 'committed_internal');
    assert.deepEqual(amo.listExecutionRouterAudit().map((a) => [a.intent, a.outcome]), [['APPROVE_EXECUTION', 'dispatched']]);
  });

  it('"this mission" without an analytical referent keeps mission ownership', async () => {
    const { engine, mission } = await readyMission();
    const { ask } = await openWorkspace({ engine, bindMissionId: mission.id });
    await ask(INSPECT);
    const r = await ask("What's the status of this mission?");
    assert.notEqual(r.workspaceOwnership && r.workspaceOwnership.owner, 'operational_analysis');
  });
});

describe('I — portfolio overview remains available', { concurrency: false }, () => {
  it('a full portfolio request after mission questions still gets the comprehensive report', async () => {
    const { ask } = await openWorkspace();
    await ask(INSPECT);
    await ask(CRM_QUESTION);
    const r = await ask('Using the evidence you just retrieved, give me the full operational analysis of the portfolio.');
    assert.equal(r.workspaceOwnership.owner, 'operational_analysis');
    assert.equal(r.operationalEvidence.analyticalFrame.subject, 'portfolio');
    assert.equal(r.operationalEvidence.analyticalFrame.responseScope, 'portfolio');
    for (const h of ['1. Canonical business objective', '2. Missions relevant to that objective', '3. Why outbound is paused — safety gate vs. execution failures', '4. Recommended next reliability validation step', '5. Evidence and limits']) {
      assert.match(r.prose, new RegExp(`^${h.replace(/[.()]/g, '\\$&')}$`, 'm'), h);
    }
    assert.match(r.prose, new RegExp(`Stale \\(${STALE_COUNT + 2}\\)`));
    // A portfolio answer ends the mission referent.
    const next = await ask('Why is it stale?');
    assert.notDeepEqual(next.operationalEvidence && next.operationalEvidence.analyticalFrame && next.operationalEvidence.analyticalFrame.missionIds, [CRM_ID]);
  });

  it('the production summary request is still the full report', async () => {
    const { ask } = await openWorkspace();
    await ask(INSPECT);
    const r = await ask(SUMMARY);
    assert.equal(r.operationalEvidence.analyticalFrame.responseScope, 'portfolio');
    assert.match(r.prose, /^1\. Canonical business objective$/m);
    assert.match(r.prose, /^5\. Evidence and limits$/m);
  });
});

describe('J — tenant isolation', { concurrency: false }, () => {
  it("another tenant's mission id is indistinguishable from a missing one", async () => {
    const { ask, pool } = await openWorkspace();
    await ask(INSPECT);
    const r = await ask(`Why is mission ${OTHER_TENANT_ID} stale?`);
    assert.equal(r.workspaceOwnership.owner, 'operational_analysis');
    assert.match(r.prose, new RegExp(`I cannot find a mission with id ${OTHER_TENANT_ID} in Anchor Cleaning \\(tenant 10\\)'s persisted mission stores`));
    assert.doesNotMatch(r.prose, /Probate partners|Executing|MSHI|tenant 2\b/);
    // Every mission read was scoped to the session tenant.
    const lookups = pool.statements.filter((s) => /AND id = \$2/.test(s));
    assert.ok(lookups.length >= 1);
    assertOnlyReads(pool);
  });

  it('a question naming another client is refused before any mission detail is read', async () => {
    const { ask } = await openWorkspace();
    await ask(INSPECT);
    const r = await ask('Why is the MSHI probate partners mission stale?');
    assert.match(r.prose, /I did not read another tenant's data/);
    assert.doesNotMatch(r.prose, new RegExp(`${OTHER_TENANT_ID}|is classified as stale because|Executing`));
    assert.notEqual(r.operationalEvidence && r.operationalEvidence.analyticalFrame && r.operationalEvidence.analyticalFrame.missionIds && r.operationalEvidence.analyticalFrame.missionIds.length, 1);
  });
});

describe('frame — routing ownership is separate from answer construction', () => {
  const record = {
    missions: [
      { id: CRM_ID, title: CRM_TITLE, createdAt: '2026-10-14T10:05:00Z', lastActivityAt: CRM_LAST },
      { id: GOV_ID, title: GOV_TITLE, createdAt: '2026-10-16T15:02:00Z', lastActivityAt: GOV_LAST },
    ],
  };
  it('extracts the question, subject, facets, and scope', () => {
    const f = buildAnalyticalFrame(CRM_QUESTION, record);
    assert.equal(f.subject, 'mission');
    assert.deepEqual(f.subjectIds, [CRM_ID]);
    assert.deepEqual(f.facets, ['classification', 'reconsideration']);
    assert.equal(f.responseScope, 'focused');
    assert.equal(f.actionAuthorized, false);
    assert.doesNotMatch(f.affirmative, /approve|reject|cancel|resume/i, 'negated constraints are not part of the question');
  });
  it('a portfolio question is focused on the facets it names; an overview is portfolio-wide', () => {
    const narrow = buildAnalyticalFrame('Why is outbound paused?', record);
    assert.equal(narrow.subject, 'portfolio');
    assert.deepEqual(narrow.facets, ['safety']);
    assert.equal(narrow.responseScope, 'focused');
    assert.equal(buildAnalyticalFrame(SUMMARY, record).responseScope, 'portfolio');
  });
  it('a date in the description must match a persisted date or the title', () => {
    assert.deepEqual(buildAnalyticalFrame('Why is the October 16 governed outbound mission stale?', record).subjectIds, [GOV_ID]);
    assert.deepEqual(buildAnalyticalFrame('Why is the October 17 governed outbound mission stale?', record).subjectIds, []);
  });
});
