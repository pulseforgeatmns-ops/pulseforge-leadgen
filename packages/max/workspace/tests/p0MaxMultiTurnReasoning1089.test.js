'use strict';

/**
 * P0 #1089 — Max multi-turn reasoning and routing.
 *
 * Production (Nov 8): turn 1, a read-only operational status report for Anchor
 * Cleaning, was correctly owned by OperationalInspection (#1088). Turn 2, an
 * analytical follow-up over that evidence, was claimed by the hydrated
 * mission's pending execution_approval and answered:
 *
 *   "This approval is the next operator gate before the mission can advance.
 *    Authorize external execution of prepared outreach?"
 *
 * Every test drives the real WorkspaceEngine.ask() path and asserts routing,
 * response content, and the absence of mutations.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const A = require('../AmoOperatorApproval');
const { createHermeticWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime, liveScoutStub } = require('./amoTestRuntime');
const { resolvePendingOperatorDecision, RESOLUTION_OUTCOMES } = require('../PendingDecisionResolver');
const { safeClassifyPendingDecisionCaptureIntent, CAPTURE_INTENTS } = require('../pendingDecisionCaptureGuard');
const { contextualPendingAnswer } = require('../PendingDecisionTurn');
const { resolveOperatorObjective } = require('../OperatorObjectiveResolutionEngine');
const { PRIMARY_OBJECTIVES } = require('../PrimaryObjective');
const { isConstraintQualifiedRequest } = require('../NegatedDirective');
const { detectOperationalAnalysisFollowUp } = require('../OperationalEvidenceContinuity');

// Verbatim from Issue #1089.
const TURN_1 =
  'Give me a read-only operational status report for Anchor Cleaning. Identify the current mission state, active execution blockers, and supporting evidence. Distinguish active business missions from completed, stale, or development/test missions, but do not archive, delete, or modify anything.';
const TURN_2 =
  'Using the operational evidence you just retrieved for Anchor Cleaning, identify the single canonical business objective, the missions genuinely relevant to that objective, and the historical or ambiguous missions that should not be treated as current execution work. Explain why outbound is intentionally paused, distinguish the safety gate from actual execution failures, and recommend the next reliability validation step. Do not change session settings, approve or resume missions, alter the outbound gate, create new missions, or execute anything. Cite persisted evidence and explicitly identify anything you cannot verify.';

const TENANT = '10';
const NOW = new Date('2026-11-08T12:00:00Z');
const daysAgo = (n) => new Date(NOW.getTime() - n * 24 * 60 * 60 * 1000).toISOString();
const CANONICAL = 'Acquire recurring commercial cleaning customers for Anchor Cleaning in Greater Manchester.';

const BLUEPRINT = {
  id: 'bp_anchor',
  clientId: 10,
  status: 'approved',
  summary: {
    identity: 'Anchor Cleaning — commercial cleaning for professional offices.',
    campaignGoals: 'Win recurring commercial cleaning contracts in Greater Manchester.',
  },
};

function amoRow(id, fields) {
  return {
    id,
    tenant_id: fields.tenant || TENANT,
    client_id: Number(fields.tenant || TENANT),
    stage: fields.stage,
    status: fields.status,
    title: fields.title,
    objective: fields.objective || fields.title,
    payload: { id, tenantId: fields.tenant || TENANT, ...(fields.payload || {}) },
    created_at: daysAgo(60),
    updated_at: fields.updated_at,
  };
}

const STALE_COUNT = 6;

/** Production-shaped portfolio: one active mission, many stale with pending approvals. */
function fixtureRows({ scoutErrors = 0 } = {}) {
  const stale = Array.from({ length: STALE_COUNT }, (_, i) =>
    amoRow(`am_stale_${i}`, {
      stage: i % 2 ? 'ready' : 'discover',
      status: i % 2 ? 'Ready' : 'Discovering',
      title: `Historical segment ${i}`,
      updated_at: daysAgo(30 + i),
      payload: {
        pendingOperatorDecision: i % 2
          ? { kind: 'execution_approval', prompt: 'Authorize external execution of prepared outreach?' }
          : { kind: 'discovery_approval', prompt: 'Approve discovery?' },
      },
    })
  );
  return {
    acquisition_missions: [
      amoRow('am_canonical', {
        stage: 'ready', status: 'Ready', title: 'Facility buyers — Greater Manchester', objective: CANONICAL,
        updated_at: daysAgo(2),
        payload: { pendingOperatorDecision: { kind: 'execution_approval', prompt: 'Authorize external execution of prepared outreach?' } },
      }),
      ...stale,
      amoRow('am_done', { stage: 'improve', status: 'Completed', title: 'Accounting pilot', updated_at: daysAgo(20) }),
      amoRow('am_untimed', { stage: 'discover', status: 'Discovering', title: 'Imported segment', updated_at: null }),
      amoRow('am_probe', { stage: 'discover', status: 'Discovering', title: 'Probe mission', updated_at: daysAgo(3) }),
      amoRow('am_mshi', { tenant: '2', stage: 'execute', status: 'Executing', title: 'MSHI probate partners', updated_at: daysAgo(1) }),
    ],
    acquisition_mission_events: [
      { mission_id: 'am_canonical', tenant_id: TENANT, kind: 'stage_committed', label: 'Ready committed', at: daysAgo(1) },
    ],
    clients: [
      { id: 1, name: 'Pulseforge', slug: 'pulseforge' },
      { id: 2, name: 'MSHI', slug: 'mshi' },
      { id: 10, name: 'Anchor Cleaning', slug: 'cleaning-co' },
    ],
    prospects: {
      10: { total: 211, qualified: 64, setter_visible: 40, do_not_contact: 5, last_added_at: daysAgo(3), last_contacted_at: daysAgo(12) },
      2: { total: 777 },
    },
    touchpoints: { 10: [{ channel: 'email', last_7d: 0, last_30d: 18, last_at: daysAgo(12) }], 2: [{ channel: 'sms', last_7d: 90, last_30d: 90, last_at: daysAgo(0) }] },
    agent_log: { 10: [{ agent_name: 'scout', last_run_at: daysAgo(1), runs_7d: 6, errors_7d: scoutErrors }] },
  };
}

function createFixturePool({ failMissionsTable = false, scoutErrors = 0 } = {}) {
  const rows = fixtureRows({ scoutErrors });
  const statements = [];
  return {
    statements,
    async query(sql, params = []) {
      const text = String(sql);
      statements.push(text.replace(/\s+/g, ' ').trim());
      const key = String(params[0]);
      if (/FROM clients/.test(text)) return { rows: rows.clients };
      if (/FROM acquisition_mission_events/.test(text)) return { rows: rows.acquisition_mission_events.filter((r) => r.tenant_id === key) };
      if (/FROM acquisition_missions/.test(text)) return { rows: rows.acquisition_missions.filter((r) => r.tenant_id === key) };
      if (/FROM missions/.test(text)) {
        if (failMissionsTable) throw new Error('connection terminated');
        return { rows: [] };
      }
      if (/FROM prospects/.test(text)) return { rows: [rows.prospects[key] || { total: 0 }] };
      if (/FROM touchpoints/.test(text)) return { rows: rows.touchpoints[key] || [] };
      if (/FROM agent_log/.test(text)) return { rows: rows.agent_log[key] || [] };
      if (/to_regclass/.test(text)) return { rows: [{ control_table: 'anchor_email_recovery_control' }] };
      if (/FROM anchor_email_recovery_control/.test(text)) return { rows: [{ singleton: true, maintenance_armed: true, manifest: null }] };
      throw new Error(`unexpected statement: ${text.slice(0, 80)}`);
    },
  };
}

/** In-process mission at execution_approval — the decision that hijacked production. */
async function readyMission(engine = amo.createAcquisitionMissionEngine()) {
  const mission = engine.create({ tenantId: TENANT, objective: CANONICAL, targetSegment: 'Facility buyers' });
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

async function openWorkspace({ engine, pool, tenantId = TENANT, bindMissionId = null }) {
  const ws = createHermeticWorkspaceEngine({
    disableLlm: true,
    missionsEnabled: true,
    resolverEnabled: true,
    clientIntelligenceService: { getApprovedClientBlueprint: async () => BLUEPRINT },
    acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    operationalInspectionOpts: { pool, now: NOW, env: { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true' } },
  });
  const opened = await ws.open({ tenantId, page: 'command-deck' });
  const session = ws._sessions.get(opened.sessionId);
  if (bindMissionId) {
    session.context.missionId = bindMissionId;
    session.context.acquisitionMissionId = bindMissionId;
  }
  const ask = (question) => ws.ask({ sessionId: opened.sessionId, question });
  return { ws, session, ask };
}

function fingerprint(engine, tenantId = TENANT) {
  return JSON.stringify(
    engine.list(tenantId).map((m) => ({
      m,
      events: engine.store.listEvents(m.id),
      contributions: engine.store.listContributions(m.id),
    }))
  );
}

function isApproved(engine, mission) {
  return amo.isExecutionApproved(engine.inspect(mission.id, { tenantId: TENANT }).contributions, mission.id);
}

function fingerprintMissionPending(engine, mission) {
  return engine.get(mission.id, TENANT).pendingOperatorDecision.kind;
}

function sessionPolicy(session) {
  return JSON.stringify((session.context && session.context.sessionState) || null);
}

function assertOnlyReads(pool) {
  for (const s of pool.statements) {
    assert.match(s, /^(SELECT|WITH)\b/i, `non-read statement reached the database: ${s}`);
  }
}

function assertNotPendingDecisionSubstitute(result) {
  assert.doesNotMatch(result.prose, /This approval is the next operator gate/);
  assert.doesNotMatch(result.prose, /Authorize external execution of (?:this )?prepared outreach\?/);
  assert.doesNotMatch(result.prose, /## Execution Ready/);
  assert.doesNotMatch(result.prose, /Mission Updated|Approve discovery\?\s*$/m);
  assert.notEqual(result.resolution && result.resolution.reason, 'pending_decision_turn_ownership');
  assert.equal(result.structured.metadata.pendingDecisionTurnOwnership, undefined);
  assert.equal(result.mission, null);
  assert.deepEqual(result.recommendedActions, []);
}

function assertAnalysisOwner(result) {
  assert.equal(result.workspaceOwnership.owner, 'operational_analysis');
  assert.equal(result.resolution.action, 'operational_analyzed');
  assert.equal(result.structured.metadata.operationalAnalysis, true);
  assert.equal(result.presentation, 'operational_analysis');
  assertNotPendingDecisionSubstitute(result);
}

/** Grounded content the production answer was missing. */
function assertGroundedAnalysis(prose, { staleCount = STALE_COUNT } = {}) {
  assert.match(prose, /^Operational analysis — Anchor Cleaning \(tenant 10\)/);
  assert.match(prose, /Nothing was approved, resumed, created, executed, or changed/);
  // Canonical objective with its source.
  assert.match(prose, /1\. Canonical business objective\n- Acquire recurring commercial cleaning customers for Anchor Cleaning in Greater Manchester\./);
  assert.match(prose, /Source: objective persisted on the only active, non-synthetic mission, Facility buyers — Greater Manchester \[am_canonical\] \(acquisition_missions\)/);
  assert.match(prose, /Declared business goal \(approved Client Blueprint — operator-authored intent, not execution evidence\): Win recurring commercial cleaning contracts/);
  // Relevance.
  assert.match(prose, /Plausibly current work \(1\)[\s\S]*Facility buyers — Greater Manchester \[am_canonical\][^\n]*carries the canonical objective/);
  assert.match(prose, new RegExp(`Historical — should not be treated as current execution work \\(${staleCount + 1}\\)`));
  assert.match(prose, new RegExp(`Stale \\(${staleCount}\\): non-terminal, no recorded activity for more than 14 days`));
  assert.match(prose, /Completed \/ closed \(1\)/);
  assert.match(prose, /Ambiguous — needs operator confirmation before being treated either way \(2\)[\s\S]*Imported segment[\s\S]*Probe mission/);
  // Safety gate vs failure, with the two evidence types distinguished.
  assert.match(prose, /The outbound gate is in maintenance \(recovery_kill_switch\)\. This is an armed safety control that holds sends by design — it is a deliberate hold, not an execution error\./);
  assert.match(prose, /What the database establishes: the gate is engaged[\s\S]*that rationale is your operator directive — I treat it as operator-declared, not as persisted evidence/);
  assert.match(prose, /Actual execution failures \(kept separate from the gate\):/);
  assert.match(prose, /Email touchpoints in the last 7 days: 0[^\n]*consistent with the gate holding outbound; on its own it is neither evidence of a failure nor of healthy delivery/);
  // Pending approvals are context, not the answer.
  assert.match(prose, new RegExp(`${staleCount + 1} mission\\(s\\) hold a pending operator decision \\(1 on active missions, ${staleCount} on historical or ambiguous missions\\)\\. These are approval gates, not failures\\. I did not request, approve, or consume any of them\\.`));
  // Recommendation is a validation step, not an approval or launch.
  assert.match(prose, /4\. Recommended next reliability validation step\n- Run one controlled Max reliability check with the outbound gate left exactly as it is/);
  assert.match(prose, /this is not a recommendation to approve, resume, or launch anything/);
  // Evidence and limits.
  for (const src of ['acquisition_missions', 'missions', 'prospects', 'touchpoints', 'agent_log', 'outbound_gate']) {
    assert.match(prose, new RegExp(`✓ ${src}:`), src);
  }
  assert.match(prose, /Cannot verify: the business rationale for the outbound pause[\s\S]*whether the \d+ stale mission\(s\) are abandoned or deliberately parked[\s\S]*the true status of the 2 uncertain mission\(s\)/);
  // Tenant isolation in content.
  assert.doesNotMatch(prose, /MSHI probate|777|\bsms\b/);
}

beforeEach(() => amo.clearExecutionRouterAudit());

describe('P0 #1089 — the exact two-turn production sequence', { concurrency: false }, () => {
  it('turn 2 is analysed from turn-1 evidence; the pending execution approval is untouched', async () => {
    const { engine, mission } = await readyMission();
    const pool = createFixturePool();
    const { session, ask } = await openWorkspace({ engine, pool, bindMissionId: mission.id });

    const first = await ask(TURN_1);
    assert.equal(first.workspaceOwnership.owner, 'operational_inspection');
    assert.ok(session.operationalEvidence, 'inspection evidence is recorded on the session');
    assert.equal(session.operationalEvidence.tenantId, TENANT);

    const before = fingerprint(engine);
    const policyBefore = sessionPolicy(session);
    const readsBefore = pool.statements.length;

    const second = await ask(TURN_2);

    assertAnalysisOwner(second);
    assert.equal(second.resolution.reason, 'explicit_prior_evidence_reference');
    assertGroundedAnalysis(second.prose);
    assert.match(second.prose, /Basis: the read-only inspection already retrieved in this conversation \(as of 2026-11-08 12:00:00Z\)/);
    assert.match(second.prose, /Your constraints were honoured: "Do not change session settings, approve or resume missions, alter the outbound gate, create new missions, or execute anything"/);

    // Evidence continuity: reused, not re-read (only the client directory for authorization).
    const continuity = second.operationalEvidence;
    assert.equal(continuity.evidenceUse, 'reused');
    assert.equal(continuity.evidenceState, 'current');
    assert.equal(continuity.canonicalObjectiveSource, 'acquisition_mission:am_canonical');
    assert.deepEqual(continuity.pendingDecisions, { present: STALE_COUNT + 1, requested: 0, consumed: 0 });
    assert.deepEqual(pool.statements.slice(readsBefore).map((s) => /FROM clients/.test(s)), [true]);

    // No mutation, approval, dispatch, or session-policy change.
    assert.equal(fingerprint(engine), before, 'acquisition mission state must not change');
    assert.equal(engine.get(mission.id, TENANT).pendingOperatorDecision.kind, 'execution_approval');
    assert.equal(isApproved(engine, mission), false);
    assert.deepEqual(amo.listExecutionRouterAudit(), [], 'nothing reaches the execution router');
    assert.equal(sessionPolicy(session), policyBefore, 'session settings unchanged');
    assert.notEqual(session.context.sessionState && session.context.sessionState.executionPolicy, 'read_only');
    assert.equal(session.context.missionId, mission.id, 'mission context preserved, not cleared');
    assertOnlyReads(pool);
  });

  it('the same sequence with a hydrated (not explicitly bound) mission', async () => {
    const { engine, mission } = await readyMission();
    const { ask } = await openWorkspace({ engine, pool: createFixturePool() });
    await ask(TURN_1);
    const before = fingerprint(engine);
    const second = await ask(TURN_2);
    assertAnalysisOwner(second);
    assertGroundedAnalysis(second.prose);
    assert.equal(fingerprint(engine), before);
    assert.equal(isApproved(engine, mission), false);
    assert.deepEqual(amo.listExecutionRouterAudit(), []);
  });

  it('real execution failures are reported separately from the safety gate', async () => {
    const { engine } = await readyMission();
    const { ask } = await openWorkspace({ engine, pool: createFixturePool({ scoutErrors: 3 }) });
    await ask(TURN_1);
    const second = await ask(TURN_2);
    assertAnalysisOwner(second);
    const [, failures] = second.prose.split('Actual execution failures (kept separate from the gate):');
    assert.match(failures, /^\n- agent_log: scout recorded 3 error run\(s\) in the last 7 days\./);
    assert.match(second.prose, /deliberate hold, not an execution error/);
    assert.match(second.prose, /review the scout \(3\) error run\(s\) in agent_log before relying on that agent/);
  });
});

describe('P0 #1089 — evidence survives envelope context switches', { concurrency: false }, () => {
  it('a context envelope on every ask does not drop the evidence; it is not echoed in the response context', async () => {
    const { engine, mission } = await readyMission();
    const { ws, session } = await openWorkspace({ engine, pool: createFixturePool(), bindMissionId: mission.id });
    const envelope = { tenantId: TENANT, page: 'command-deck' };
    await ws.ask({ sessionId: session.id, question: TURN_1, context: envelope });
    const result = await ws.ask({
      sessionId: session.id,
      question: 'Which missions are genuinely current work, and why is outbound paused?',
      context: envelope,
    });
    assertAnalysisOwner(result);
    assert.equal(result.operationalEvidence.evidenceUse, 'reused');
    assert.equal(result.context.operationalEvidence, undefined);
    assert.equal(fingerprintMissionPending(engine, mission), 'execution_approval');
  });
});

describe('P0 #1089 — session shapes', { concurrency: false }, () => {
  it('session without any mission: analysis still works, no mission is created', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const { ask } = await openWorkspace({ engine, pool: createFixturePool() });
    await ask(TURN_1);
    const second = await ask(TURN_2);
    assertAnalysisOwner(second);
    assertGroundedAnalysis(second.prose);
    assert.equal(engine.list(TENANT).length, 0, 'no acquisition mission may be created');
    assert.deepEqual(amo.listExecutionRouterAudit(), []);
  });

  it('many stale missions with pending approvals are historical context, never the answer', async () => {
    const { engine } = await readyMission();
    const { ask } = await openWorkspace({ engine, pool: createFixturePool() });
    await ask(TURN_1);
    const second = await ask('Based on that report, which missions are genuinely current and which pending approvals are historical?');
    assertAnalysisOwner(second);
    assert.match(second.prose, /Stale \(6\)[\s\S]*Any pending decision on them is historical state, not a current request/);
    assert.match(second.prose, /6 on historical or ambiguous missions/);
  });
});

describe('P0 #1089 — follow-ups that do not repeat "operational status report"', { concurrency: false }, () => {
  // Question-directed reasoning: each narrower follow-up keeps the operational
  // owner and gets the sections it asks about (plus evidence), not the whole
  // five-section report.
  const FOLLOW_UPS = [
    ['Which missions are genuinely current work, and why is outbound paused?', ['relevance', 'safety', 'evidence']],
    ['Explain why outbound is paused and distinguish the safety gate from actual execution failures.', ['safety', 'evidence']],
    ['Given the evidence you just retrieved, what is the canonical business objective?', ['objective', 'evidence']],
    ['Recommend the next reliability validation step based on that report.', ['recommendation', 'evidence']],
  ];
  const HEADINGS = {
    objective: /^Canonical business objective$/m,
    relevance: /^Missions relevant to the canonical objective$/m,
    safety: /^Why outbound is paused — safety gate vs\. execution failures$/m,
    recommendation: /^Recommended next reliability validation step$/m,
    evidence: /^Evidence and limits$/m,
  };
  for (const [q, sections] of FOLLOW_UPS) {
    it(`"${q}" keeps the operational owner and answers only ${sections.join(' + ')}`, async () => {
      const { engine, mission } = await readyMission();
      const { ask } = await openWorkspace({ engine, pool: createFixturePool(), bindMissionId: mission.id });
      await ask(TURN_1);
      const before = fingerprint(engine);
      const result = await ask(q);
      assertAnalysisOwner(result);
      for (const [section, re] of Object.entries(HEADINGS)) {
        if (sections.includes(section)) assert.match(result.prose, re, `${q} → ${section}`);
        else assert.doesNotMatch(result.prose, re, `${q} must not include ${section}`);
      }
      assert.deepEqual(result.operationalEvidence.analyticalFrame.sections, sections);
      assert.equal(result.operationalEvidence.analyticalFrame.responseScope, 'focused');
      assert.equal(fingerprint(engine), before);
      assert.deepEqual(amo.listExecutionRouterAudit(), []);
    });
  }

  it('the contract is stateful: without session evidence or an explicit reference the turn keeps normal routing', () => {
    const q = 'Which missions are genuinely current work, and why is outbound paused?';
    assert.equal(detectOperationalAnalysisFollowUp(q, { context: { tenantId: TENANT } }).matched, false);
    assert.equal(detectOperationalAnalysisFollowUp('What do you understand about my business?', {
      context: { tenantId: TENANT },
      operationalEvidence: { tenantId: TENANT, recordedAt: new Date().toISOString(), missions: [] },
    }).matched, false);
  });
});

describe('P0 #1089 — negated approval and execution instructions', { concurrency: false }, () => {
  const NEGATED_ANALYSIS = 'Explain the outbound pause and separate the safety gate from execution failures. Do not approve or resume missions.';
  const PENDING = { kind: 'execution_approval', prompt: 'Authorize external execution of prepared outreach?' };

  it('a negated "approve / resume" constraint is never a reject, approve, or decision question', () => {
    for (const q of [TURN_2, NEGATED_ANALYSIS, 'Do not send anything. Summarize the outbound evidence.']) {
      const r = resolvePendingOperatorDecision(q, { id: 'm', pendingOperatorDecision: PENDING });
      assert.equal(r.resolved, false, q);
      assert.equal(r.outcome, RESOLUTION_OUTCOMES.UNRELATED, q);
      assert.notEqual(r.action, 'cancel', q);
      assert.equal(isConstraintQualifiedRequest(q), true, q);
    }
    assert.notEqual(
      safeClassifyPendingDecisionCaptureIntent({ message: TURN_2, pendingDecision: PENDING }),
      CAPTURE_INTENTS.PENDING_DECISION_CLARIFICATION
    );
    assert.doesNotMatch(
      contextualPendingAnswer(TURN_2, { decisionKind: 'execution_approval', prompt: PENDING.prompt }, null),
      /next operator gate/
    );
  });

  it('a genuine reply to the decision keeps its meaning', () => {
    for (const [q, outcome] of [
      ["Don't send it.", RESOLUTION_OUTCOMES.REJECT],
      ['No, do not approve.', RESOLUTION_OUTCOMES.REJECT],
      ['Why should I approve this?', RESOLUTION_OUTCOMES.QUESTION],
      ['Approved.', RESOLUTION_OUTCOMES.AFFIRM],
    ]) {
      const r = resolvePendingOperatorDecision(q, { id: 'm', pendingOperatorDecision: PENDING });
      assert.equal(r.outcome, outcome, q);
      assert.equal(isConstraintQualifiedRequest(q), false, q);
    }
  });

  it('a prohibition-only segment never selects mission execution', () => {
    const r = resolveOperatorObjective({ question: 'Do not change session settings, approve or resume missions, or execute anything.', hasActiveMission: true });
    assert.notEqual(r.primaryObjective, PRIMARY_OBJECTIVES.MISSION_EXECUTION);
    const positive = resolveOperatorObjective({ question: 'Resume the mission.', hasActiveMission: true });
    assert.equal(positive.primaryObjective, PRIMARY_OBJECTIVES.MISSION_EXECUTION);
  });

  it('end to end: negated instructions after an inspection produce analysis, not a cancel', async () => {
    const { engine, mission } = await readyMission();
    const { ask } = await openWorkspace({ engine, pool: createFixturePool(), bindMissionId: mission.id });
    await ask(TURN_1);
    const before = fingerprint(engine);
    const result = await ask('Do not approve, resume, or execute anything. Explain which missions are stale and why outbound is paused.');
    assertAnalysisOwner(result);
    assert.equal(fingerprint(engine), before);
    assert.equal(engine.get(mission.id, TENANT).pendingOperatorDecision.kind, 'execution_approval');
    assert.notEqual(engine.get(mission.id, TENANT).planCancelled, true);
  });

  it('end to end WITHOUT prior evidence: a constraint-qualified question does not cancel the mission', async () => {
    const { engine, mission } = await readyMission();
    const { ask } = await openWorkspace({ engine, pool: createFixturePool(), bindMissionId: mission.id });
    const before = fingerprint(engine);
    const result = await ask(NEGATED_ANALYSIS);
    assert.notEqual(result.resolution && result.resolution.action, 'cancelled');
    assert.equal(fingerprint(engine), before);
    assert.equal(engine.get(mission.id, TENANT).pendingOperatorDecision.kind, 'execution_approval');
    assert.deepEqual(amo.listExecutionRouterAudit().filter((a) => a.outcome === 'dispatched'), []);
  });
});

describe('P0 #1089 — explicit authority and mission ownership are preserved', { concurrency: false }, () => {
  it('explicit execution approval still works after an analytical turn — and still sends nothing', async () => {
    const { engine, mission } = await readyMission();
    const { ask } = await openWorkspace({ engine, pool: createFixturePool(), bindMissionId: mission.id });
    await ask(TURN_1);
    await ask(TURN_2);
    assert.equal(isApproved(engine, mission), false);

    const shown = await ask("Let's run it");
    assert.equal(shown.resolution.action, 'clarify');
    assert.match(shown.prose, /## Execution Ready/);

    amo.clearExecutionRouterAudit();
    const approved = await ask('Approve');
    assert.notEqual(approved.workspaceOwnership && approved.workspaceOwnership.owner, 'operational_analysis');
    assert.equal(approved.structured.metadata.executionState, 'committed_internal');
    assert.equal(isApproved(engine, mission), true);
    assert.deepEqual(amo.listExecutionRouterAudit().map((a) => [a.intent, a.outcome]), [['APPROVE_EXECUTION', 'dispatched']]);
    assert.equal(engine.get(mission.id, TENANT).stage, amo.STAGES.READY, 'approval does not send');
  });

  it('explicit plan approval still advances a correctly selected mission after an analytical turn', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({ tenantId: TENANT, objective: CANONICAL, targetSegment: 'Facility buyers' });
    const { ask } = await openWorkspace({ engine, pool: createFixturePool(), bindMissionId: mission.id });
    await ask(TURN_1);
    await ask(TURN_2);
    assert.equal(engine.get(mission.id, TENANT).pendingOperatorDecision.kind, 'plan_approval');
    const approved = await ask('Approved.');
    assert.notEqual(approved.workspaceOwnership && approved.workspaceOwnership.owner, 'operational_analysis');
    assert.notEqual(engine.get(mission.id, TENANT).pendingOperatorDecision?.kind, 'plan_approval');
  });

  for (const q of ["What's the status of this mission?", 'Why is this mission blocked?', 'What exactly am I approving?']) {
    it(`"${q}" keeps mission ownership after an inspection`, async () => {
      const { engine, mission } = await readyMission();
      const { ask } = await openWorkspace({ engine, pool: createFixturePool(), bindMissionId: mission.id });
      await ask(TURN_1);
      const result = await ask(q);
      assert.notEqual(result.workspaceOwnership && result.workspaceOwnership.owner, 'operational_analysis', q);
      assert.notEqual(result.resolution && result.resolution.action, 'operational_analyzed', q);
    });
  }

  it('ordinary conversation after an inspection is untouched', async () => {
    const { ask } = await openWorkspace({ engine: amo.createAcquisitionMissionEngine(), pool: createFixturePool() });
    await ask(TURN_1);
    const result = await ask('What do you understand about my business?');
    assert.notEqual(result.workspaceOwnership && result.workspaceOwnership.owner, 'operational_analysis');
  });

  it('a session-policy directive after an inspection is still a session directive', async () => {
    const { session, ask } = await openWorkspace({ engine: amo.createAcquisitionMissionEngine(), pool: createFixturePool() });
    await ask(TURN_1);
    const result = await ask('Use read-only mode.');
    assert.notEqual(result.workspaceOwnership && result.workspaceOwnership.owner, 'operational_analysis');
    assert.equal(session.context.sessionState.executionPolicy, 'read_only');
  });
});

describe('P0 #1089 — missing, stale, or unavailable evidence', { concurrency: false }, () => {
  it('turn 2 in a fresh session refreshes evidence read-only and says so', async () => {
    const { engine, mission } = await readyMission();
    const pool = createFixturePool();
    const { session, ask } = await openWorkspace({ engine, pool, bindMissionId: mission.id });
    const before = fingerprint(engine);
    const result = await ask(TURN_2);
    assertAnalysisOwner(result);
    assertGroundedAnalysis(result.prose);
    assert.match(result.prose, /Basis: a fresh read-only inspection as of 2026-11-08 12:00:00Z \(no inspection evidence was available in this conversation, so I retrieved it now\)/);
    assert.equal(result.operationalEvidence.evidenceUse, 'refreshed');
    assert.equal(result.operationalEvidence.evidenceState, 'missing');
    assert.ok(session.operationalEvidence);
    assert.equal(fingerprint(engine), before);
    assertOnlyReads(pool);
  });

  it('stale session evidence is re-read before analysis', async () => {
    const { engine } = await readyMission();
    const { session, ask } = await openWorkspace({ engine, pool: createFixturePool() });
    await ask(TURN_1);
    session.operationalEvidence.recordedAt = new Date(NOW.getTime() - 2 * 60 * 60 * 1000).toISOString();
    const result = await ask(TURN_2);
    assertAnalysisOwner(result);
    assert.equal(result.operationalEvidence.evidenceState, 'stale');
    assert.match(result.prose, /the earlier evidence was older than 30 minutes, so I re-read it/);
  });

  it('unavailable sources are disclosed, never assumed healthy', async () => {
    const { ask } = await openWorkspace({ engine: amo.createAcquisitionMissionEngine(), pool: createFixturePool({ failMissionsTable: true }) });
    await ask(TURN_1);
    const result = await ask(TURN_2);
    assertAnalysisOwner(result);
    assert.match(result.prose, /✗ missions: UNAVAILABLE \(mission_store_unavailable\) — nothing in this answer relies on it\./);
    assert.match(result.prose, /Cannot verify:[^\n]*anything that depends on missions/);
    assert.ok(result.structured.metadata.unavailable.includes('missions: mission_store_unavailable'));
  });

  it('with no database, gate and activity are unavailable and the answer says so', async () => {
    const { engine } = await readyMission();
    const { ask } = await openWorkspace({ engine, pool: null });
    await ask(TURN_1);
    const result = await ask(TURN_2);
    assertAnalysisOwner(result);
    assert.match(result.prose, /Gate state UNAVAILABLE — I cannot confirm from persisted state whether outbound is intentionally held\./);
    assert.match(result.prose, /agent_log was unavailable, so agent errors cannot be ruled out\./);
    assert.doesNotMatch(result.prose, /deliberate hold/);
  });
});

describe('P0 #1089 — tenant isolation', { concurrency: false }, () => {
  it('a follow-up naming another tenant is refused without reading its data', async () => {
    const pool = createFixturePool();
    const { ask } = await openWorkspace({ engine: amo.createAcquisitionMissionEngine(), pool });
    await ask(TURN_1);
    const readsBefore = pool.statements.length;
    const result = await ask(TURN_2.replace('for Anchor Cleaning', 'for MSHI'));
    assert.equal(result.workspaceOwnership.owner, 'operational_analysis');
    assert.equal(result.structured.metadata.operationalInspectionRefused, 'tenant_mismatch');
    assert.match(result.prose, /did not read another tenant's data/);
    assert.deepEqual(pool.statements.slice(readsBefore).filter((s) => !/FROM clients/.test(s)), []);
    assert.doesNotMatch(result.prose, /MSHI probate|777/);
  });

  it('evidence recorded for another tenant is never reused', async () => {
    const pool = createFixturePool();
    const { session, ask } = await openWorkspace({ engine: amo.createAcquisitionMissionEngine(), pool });
    session.operationalEvidence = {
      tenantId: '2', recordedAt: NOW.toISOString(), asOf: NOW.toISOString(), missions: [], sources: [], missionCounts: {},
    };
    const result = await ask(TURN_2);
    assertAnalysisOwner(result);
    assert.equal(result.operationalEvidence.evidenceState, 'other_tenant');
    assert.equal(result.operationalEvidence.tenantId, TENANT);
    assert.equal(session.operationalEvidence.tenantId, TENANT);
    assertGroundedAnalysis(result.prose);
  });
});
