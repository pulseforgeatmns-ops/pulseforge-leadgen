'use strict';

/**
 * P0 review corrections for PR #1127 (H1, M1, M2, M4).
 *
 *  H1 — provider-research prohibition is not review-only intent: "No paid ads"
 *       and "No spending" missions keep ordinary acquisition planning, and
 *       restriction text is derived from the actual constraints.
 *  M1 — reopening research planning through an amendment bumps the mission
 *       version and records the reopen; replay and stale approvals are safe.
 *  M2 — an approved review-only plan has one non-executing classification
 *       across status, progression and workspace mode.
 *  M4 — guard refusals are audited with correlation identifiers.
 *
 * Every refusal is checked for zero specialist, provider and network calls.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const { createHermeticWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const amo = require('../../../acquisition-mission');
const {
  listExecutionRouterAudit,
  clearExecutionRouterAudit,
} = require('../../../acquisition-mission/ExecutionRouter');
const { planFromObjective } = require('../../../acquisition-mission/MissionPlanner');
const { deriveProgressionStage, PROGRESSION_STAGES, runAutonomousProgression } =
  require('../../../acquisition-mission/MissionProgression');
const { deriveWorkspaceMode, WORKSPACE_MODES } = require('../../../acquisition-mission/WorkspaceMode');
const svc = require('../../../../services/acquisitionMission');

const TENANT = '25';
const REVIEW_ONLY =
  'Set up a review-only contact intake mission for an existing customer. ' +
  'No outreach, calls, content, or campaign activation.';
const NO_PAID_ADS =
  'Create a new acquisition mission. Objective: Acquire one recurring commercial cleaning client ' +
  'from a property management company in Nashua NH. No paid ads.';
const NO_SPENDING =
  'Create a new acquisition mission. Objective: Acquire one recurring commercial cleaning client ' +
  'from a property management company in Nashua NH. No spending.';
const ADMIN = { id: 'admin-1', role: 'admin', name: 'Admin' };

function harness() {
  const runtime = createTestAmoRuntime();
  const engine = runtime.engine();
  const ws = createHermeticWorkspaceEngine({
    acquisitionMissionRuntime: runtime,
    clientIntelligenceService: { getApprovedClientBlueprint: async () => null },
  });
  let sessionId = null;
  const ask = async (question) => {
    const result = await ws.ask({
      sessionId,
      question,
      context: sessionId ? undefined : { tenantId: TENANT, page: 'command-deck' },
    });
    sessionId = result.sessionId;
    return result;
  };
  return { runtime, engine, ask, mission: () => engine.list(TENANT)[0] || null, sessionId: () => sessionId };
}

function fingerprint(engine, mission) {
  const row = engine.get(mission.id, TENANT);
  return JSON.stringify({
    stage: row.stage,
    version: row.version,
    pending: row.pendingOperatorDecision && row.pendingOperatorDecision.kind,
    locked: Boolean(row.structuredMission && row.structuredMission.immutable),
    contributions: engine.store.listContributions(mission.id).length,
  });
}

function amend(engine, mission, change, amendmentId, hash) {
  const expectedConstraintsHash = hash ||
    amo.previewAuthorization({ engine, missionId: mission.id, tenantId: TENANT }).constraintsHash;
  return amo.applyAuthorizationAmendment({
    engine, missionId: mission.id, tenantId: TENANT, approver: ADMIN, amendmentId,
    expectedConstraintsHash, change: { add: [], lift: [], reason: 'acceptance', ...change },
  });
}

function dispatched() {
  return listExecutionRouterAudit().filter((row) => row.outcome !== 'blocked_by_constraint');
}

let fetchCalls;
let realFetch;
let spies;
beforeEach(() => {
  clearExecutionRouterAudit();
  fetchCalls = 0;
  realFetch = globalThis.fetch;
  globalThis.fetch = async () => { fetchCalls += 1; throw new Error('network is not allowed in this test'); };
  spies = { scoutCalls: 0, sendCalls: 0 };
  spies.scout = async () => { spies.scoutCalls += 1; return { status: 'completed', payload: {} }; };
  spies.send = async () => { spies.sendCalls += 1; return {}; };
});
afterEach(() => { globalThis.fetch = realFetch; });

function assertNoDispatch() {
  assert.equal(spies.scoutCalls, 0, 'no Scout call');
  assert.equal(spies.sendCalls, 0, 'no send');
  assert.equal(fetchCalls, 0, 'no provider or network call');
}

describe('H1 — research prohibition without review-only intent keeps ordinary planning', () => {
  for (const [label, text] of [['No paid ads', NO_PAID_ADS], ['No spending', NO_SPENDING]]) {
    it(`"${label}": ordinary planning, success metric and truthful restriction text`, async () => {
      const constraints = amo.extractAuthorizationConstraints(text);
      assert.equal(constraints.reviewOnly, false);
      assert.ok(constraints.prohibited.includes('provider_research'), 'pre-existing mapping (reported separately)');

      const objective = 'Acquire one recurring commercial cleaning client from a property management company in Nashua NH.';
      const baseline = planFromObjective(objective, {});
      const planned = planFromObjective(objective, { authorizationConstraints: constraints });
      assert.deepEqual(planned.ambiguities, baseline.ambiguities);
      assert.deepEqual(planned.draft.successMetric, baseline.draft.successMetric);
      assert.notEqual(planned.draft.successMetric.type, 'operator_review');
      assert.equal('researchPlanning' in planned.draft, false);

      const h = harness();
      const created = await h.ask(text);
      assert.doesNotMatch(created.prose, /Review only/i);
      const mission = h.mission();
      assert.equal(mission.authorizationConstraints.reviewOnly, false);
      assert.notEqual(mission.status, 'Plan approved (review only)');

      const approved = await h.ask('Approve the plan.');
      const after = h.mission();
      assert.equal(after.pendingOperatorDecision.kind, 'discovery_approval');
      assert.equal(after.status, 'Discovery not authorized');
      assert.equal(deriveProgressionStage(h.engine.inspect(after.id, { tenantId: TENANT })), PROGRESSION_STAGES.DISCOVERY_APPROVAL);
      assert.doesNotMatch(approved.prose, /Approve discovery to begin Scout|Review only/);
      assert.match(approved.prose, /Not authorized: paid media, discovery \(provider research\)\./);

      clearExecutionRouterAudit();
      const before = fingerprint(h.engine, after);
      const discovery = await h.ask('Approve discovery.');
      assert.match(discovery.prose, /forbids provider research\./);
      assert.doesNotMatch(discovery.prose, /review-only/);
      assert.equal(fingerprint(h.engine, after), before);
      assert.deepEqual(dispatched(), []);
      assertNoDispatch();
    });
  }

  it('"Approve the plan and run discovery" on a "No paid ads" mission is refused, not read as plan approval', async () => {
    const h = harness();
    await h.ask(NO_PAID_ADS);
    const mission = h.mission();
    assert.equal(mission.pendingOperatorDecision.kind, 'plan_approval');
    const before = fingerprint(h.engine, mission);
    const result = await h.ask('Approve the plan and run discovery.');
    assert.equal(result.structured.metadata.blocker, amo.AUTHORIZATION_CONSTRAINT_BLOCKED);
    assert.equal(fingerprint(h.engine, mission), before, 'plan not approved by prohibited wording');
    assertNoDispatch();
  });
});

describe('Review-only creation and approval (truthful, non-executing)', () => {
  it('scope lists exactly the mission\'s prohibitions; approval rests in one consistent state', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    // The plan confirmation the operator approves states the actual restrictions.
    const confirmation = h.mission().pendingOperatorDecision.missionUnderstanding;
    assert.match(confirmation, /Discovery \(provider research\) is not authorized\.\nAlso not authorized: content, outreach, outreach approval, paid media\./);
    assert.doesNotMatch(confirmation, /\bcustomers\b|Which market/);
    const approved = await h.ask('Approve the plan.');
    const mission = h.mission();
    const snapshot = h.engine.inspect(mission.id, { tenantId: TENANT });
    assert.equal(mission.pendingOperatorDecision, null);
    assert.equal(mission.status, 'Plan approved (review only)');
    assert.equal(deriveProgressionStage(snapshot), PROGRESSION_STAGES.REVIEW_ONLY);
    assert.equal(deriveWorkspaceMode({ missionId: mission.id, snapshot }), WORKSPACE_MODES.REVIEW_ONLY);
    assert.match(approved.prose, /Not authorized: content, outreach, outreach approval, paid media, discovery \(provider research\)\./);
    assert.doesNotMatch(approved.prose, /Approve discovery/);

    // No hidden progression: autonomous continuation has nothing to run.
    const before = fingerprint(h.engine, mission);
    const progressed = await runAutonomousProgression({
      engine: h.engine, missionId: mission.id, tenantId: TENANT, maxSteps: 3, runScout: spies.scout,
    }).catch((err) => ({ error: err }));
    assert.equal((progressed.transitions || []).length, 0);
    assert.equal(fingerprint(h.engine, mission), before);
    assertNoDispatch();
  });

  it('lifting a non-research restriction ends review-only without reopening, with a truthful label', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    await h.ask('Approve the plan.');
    const mission = h.mission();
    const versionBefore = h.engine.get(mission.id, TENANT).version;
    await amend(h.engine, mission, { lift: ['outreach'] }, 'p0-corr-lift-outreach');
    const after = h.mission();
    assert.equal(after.authorizationConstraints.reviewOnly, false);
    assert.equal(after.structuredMission.immutable, true, 'no reopen while research is still prohibited');
    assert.equal(after.version, versionBefore, 'constraints-only amendment does not bump the version');
    assert.equal(after.status, 'Plan approved (discovery not authorized)');
    assertNoDispatch();
  });
});

describe('M1 — amendment reopen: versioning, audit, replay and stale approvals', () => {
  it('during pending plan approval: reopens, bumps the version and records the reopen', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    const mission = h.mission();
    assert.equal(mission.pendingOperatorDecision.kind, 'plan_approval');
    const versionBefore = mission.version;
    const result = await amend(h.engine, mission, { lift: ['provider_research'] }, 'p0-corr-pre-approval');
    const after = h.mission();
    assert.equal(after.pendingOperatorDecision.kind, 'plan_clarification');
    assert.equal(after.pendingOperatorDecision.field, 'geography.region');
    assert.equal(after.version, versionBefore + 1);
    assert.equal(after.lastTransactionId, result.transactionId);
    const record = after.authorizationConstraints.amendments.at(-1);
    assert.equal(record.planningReopened, true);
    assert.equal(record.supersededPlan.approved, false);
    assert.equal(record.pendingDecisionAfter, 'plan_clarification');
    assert.equal(record.missionVersionBefore, versionBefore);
    assertNoDispatch();
  });

  it('after approval: supersedes the approved plan; replay is idempotent; stale reviews and approvals are refused', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    await h.ask('Approve the plan.');
    const mission = h.mission();
    const approvedHash = mission.structuredMission.contractHash;
    const versionBefore = mission.version;
    const staleHash = amo.previewAuthorization({ engine: h.engine, missionId: mission.id, tenantId: TENANT }).constraintsHash;

    const result = await amend(h.engine, mission, { lift: ['provider_research'] }, 'p0-corr-post-approval', staleHash);
    const after = h.mission();
    assert.equal(after.version, versionBefore + 1);
    assert.equal(after.lastTransactionId, result.transactionId);
    const record = after.authorizationConstraints.amendments.at(-1);
    assert.equal(record.planningReopened, true);
    assert.deepEqual(record.supersededPlan, { contractHash: approvedHash, approved: true });
    const contribution = h.engine.store.listContributions(mission.id)
      .find((row) => row.payload && row.payload.amendmentId === 'p0-corr-post-approval');
    assert.equal(contribution.payload.planningReopened, true, 'audit contribution records the reopen');

    // Replay: same amendment id is a no-op (no second bump, no second reopen).
    const replay = await amend(h.engine, mission, { lift: ['provider_research'] }, 'p0-corr-post-approval', staleHash);
    assert.equal(replay.alreadyApplied, true);
    assert.equal(h.mission().version, after.version);

    // Stale review: another amendment bound to the pre-amendment hash is refused.
    await assert.rejects(
      amend(h.engine, mission, { lift: ['content'] }, 'p0-corr-stale-review', staleHash),
      (err) => err.code === 'authorization_amendment_stale'
    );

    // Stale approvals: the superseded approval cannot lock or start anything.
    for (const intent of ['APPROVE_DISCOVERY', 'START_DISCOVERY', 'APPROVE_PLAN']) {
      const before = fingerprint(h.engine, mission);
      const routed = await svc.executeCanonical({
        tenantId: TENANT, missionId: mission.id, intent, role: 'operator', operatorId: 'op',
        runScout: spies.scout, sendEmail: spies.send,
      }, { acquisitionMissionRuntime: h.runtime, persist: false }).catch((err) => ({ error: err }));
      const completed = routed.execution && routed.execution.state === 'completed_verified';
      assert.equal(completed, false, `${intent} must not complete on the reopened mission`);
      const now = h.engine.get(mission.id, TENANT);
      assert.equal(now.structuredMission, null, `${intent} did not lock a plan`);
      assert.equal(fingerprint(h.engine, mission), before, `${intent} changed nothing`);
    }
    assertNoDispatch();
  });
});

describe('Mixed-intent and ambiguous approval commands (review-only, plan approval pending)', () => {
  for (const command of [
    'Approve the plan and send the emails.',
    'Approve the plan and start discovery.',
    'Yes. Send the emails.',
    'Approve everything and launch the campaign.',
  ]) {
    it(`"${command}" is refused as a whole; the plan is not approved`, async () => {
      const h = harness();
      await h.ask(REVIEW_ONLY);
      const mission = h.mission();
      const before = fingerprint(h.engine, mission);
      const result = await h.ask(command);
      assert.equal(result.structured.metadata.blocker, amo.AUTHORIZATION_CONSTRAINT_BLOCKED, command);
      assert.equal(fingerprint(h.engine, mission), before);
      assert.deepEqual(dispatched(), []);
      assertNoDispatch();
    });
  }

  it('"Yes, but don\'t send anything." is a conditional approval: nothing is approved', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    const mission = h.mission();
    const before = fingerprint(h.engine, mission);
    const result = await h.ask("Yes, but don't send anything.");
    assert.notEqual(result.structured.metadata.prohibitedCommandGuard, true, 'the negated send is not read as a command');
    assert.match(result.prose, /^Not executed: .*Nothing was approved, sent, or cancelled\./s);
    assert.equal(fingerprint(h.engine, mission), before);
    assertNoDispatch();
  });

  it('"Approve it?" is a question, not an approval', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    const mission = h.mission();
    const before = fingerprint(h.engine, mission);
    const result = await h.ask('Approve it?');
    assert.notEqual(result.structured.metadata.prohibitedCommandGuard, true);
    assert.equal(fingerprint(h.engine, mission), before);
    assertNoDispatch();
  });
});

describe('M4 — guard refusals are audited with correlation identifiers', () => {
  it('records request, operator, runtime owner and session; never the message text', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    clearExecutionRouterAudit();
    const result = await h.ask('Send the emails and draft the content.');
    const ids = result.structured.metadata.auditRequestIds;
    assert.ok(Array.isArray(ids) && ids.length >= 2);
    const rows = listExecutionRouterAudit();
    assert.deepEqual(rows.map((row) => row.requestId).sort(), [...ids].sort());
    for (const row of rows) {
      assert.match(row.requestId, /^cer_guard_[0-9a-f-]{36}$/);
      assert.equal(row.outcome, 'blocked_by_constraint');
      assert.equal(row.action, 'prohibited_command');
      assert.ok(row.operatorId, 'operator recorded');
      assert.ok(row.runtimeOwner, 'runtime owner recorded');
      assert.equal(row.request.sessionId, h.sessionId());
      assert.doesNotMatch(JSON.stringify(row), /Send the emails|draft the content/i, 'no message text in audit');
    }
    assertNoDispatch();
  });
});

describe('L1–L4 — classification follows current authorization after amendments', () => {
  const { restingPlanState, RESTING_PLAN_STATES, RESEARCH_PLANNING_REQUIRED } =
    require('../../../acquisition-mission/ResearchPlanning');
  const { routeExecutionRequest } = require('../../../acquisition-mission/ExecutionRouter');
  const { freezeStructuredMission, formatMissionUnderstandingProse } =
    require('../../../acquisition-mission/StructuredMission');

  it('lifting outreach ends review-only: status, progression and workspace mode all say discovery not authorized', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    const approved = await h.ask('Approve the plan.');
    assert.match(approved.prose, /No pending decision/);
    assert.match(approved.prose, /Discovery cannot run, so nothing that depends on it can start/);
    assert.doesNotMatch(approved.prose, /no action is authorized/i);
    const mission = h.mission();
    const versionBefore = mission.version;

    await amend(h.engine, mission, { lift: ['outreach'] }, 'p0-l1-lift-outreach');
    const after = h.mission();
    const snapshot = h.engine.inspect(after.id, { tenantId: TENANT });
    assert.equal(after.authorizationConstraints.reviewOnly, false);
    assert.ok(after.authorizationConstraints.prohibited.includes('provider_research'));
    assert.equal(restingPlanState(after), RESTING_PLAN_STATES.DISCOVERY_NOT_AUTHORIZED);
    assert.equal(after.status, 'Plan approved (discovery not authorized)');
    assert.equal(deriveProgressionStage(snapshot), PROGRESSION_STAGES.DISCOVERY_NOT_AUTHORIZED);
    assert.equal(deriveWorkspaceMode({ missionId: after.id, snapshot }), WORKSPACE_MODES.DISCOVERY_NOT_AUTHORIZED);
    assert.notEqual(deriveProgressionStage(snapshot), PROGRESSION_STAGES.REVIEW_ONLY);

    // Authorization, approval and dispatch behaviour unchanged.
    assert.equal(after.pendingOperatorDecision, null);
    assert.equal(after.structuredMission.immutable, true, 'no reopen: research is still prohibited');
    assert.equal(after.version, versionBefore, 'constraints-only amendment does not bump the version');

    const scope = formatMissionUnderstandingProse(after.structuredMission, { authorizationConstraints: after.authorizationConstraints });
    assert.doesNotMatch(scope, /Review only/);
    assert.match(scope, /Discovery \(provider research\) is not authorized\.\nAlso not authorized: content, outreach approval, paid media\./);
    assert.doesNotMatch(scope, /Also not authorized:[^\n]*\boutreach,/, 'lifted outreach is not listed');

    const before = fingerprint(h.engine, after);
    const bare = await h.ask('approved');
    assert.match(bare.prose, /^Not executed: this mission's authorization forbids provider research\./);
    assert.doesNotMatch(bare.prose, /review-only mission/);
    assert.equal(fingerprint(h.engine, after), before);

    for (const intent of ['APPROVE_DISCOVERY', 'START_DISCOVERY']) {
      await assert.rejects(
        svc.executeCanonical({ tenantId: TENANT, missionId: after.id, intent, role: 'operator', operatorId: 'op', runScout: spies.scout },
          { acquisitionMissionRuntime: h.runtime, persist: false }),
        (err) => err.code === amo.AUTHORIZATION_CONSTRAINT_BLOCKED, intent);
    }
    // Outreach is lifted, but with no approved package nothing can be sent.
    const send = await svc.executeCanonical({
      tenantId: TENANT, missionId: after.id, intent: 'EXECUTE_OUTBOUND', role: 'operator', operatorId: 'op',
      canonicalSender: { tenantId: TENANT, senderEmail: 'ops@example.invalid', senderName: 'Ops', sendingDomain: 'example.invalid' },
      sendEmail: spies.send,
    }, { acquisitionMissionRuntime: h.runtime, persist: false }).catch((err) => ({ error: err }));
    assert.ok(send.error || !(send.execution && send.execution.state === 'completed_verified'));
    const progressed = await runAutonomousProgression({
      engine: h.engine, missionId: after.id, tenantId: TENANT, maxSteps: 3, runScout: spies.scout,
    }).catch((err) => ({ error: err }));
    assert.equal((progressed.transitions || []).length, 0);
    assert.equal(fingerprint(h.engine, after), before);
    assertNoDispatch();
  });

  it('a deferred plan with research permitted reads "research planning required" and discovery stays gated', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const planned = planFromObjective('Set up a contact intake mission for an existing customer.', {
      authorizationConstraints: amo.extractAuthorizationConstraints('Review-only.'),
    });
    const mission = engine.create({
      tenantId: TENANT, clientId: Number(TENANT), createdBy: 'test',
      objective: 'Set up a contact intake mission for an existing customer.',
      structuredMission: freezeStructuredMission(planned.draft, { approvedBy: 'test' }),
    });
    const snapshot = engine.inspect(mission.id, { tenantId: TENANT });
    assert.equal(restingPlanState(snapshot.mission), RESTING_PLAN_STATES.RESEARCH_PLANNING_REQUIRED);
    assert.equal(snapshot.mission.status, 'Research planning required');
    assert.equal(deriveProgressionStage(snapshot), PROGRESSION_STAGES.MISSION_PLANNING);
    // L4 — no false claim that discovery is unauthorized.
    const scope = formatMissionUnderstandingProse(snapshot.mission.structuredMission, { authorizationConstraints: { prohibited: [] } });
    assert.match(scope, /Research planning has not been completed\.\nMarket and region must be collected and the plan approved again before discovery\./);
    assert.doesNotMatch(scope, /not authorized/i);
    // Execution behaviour unchanged: the research-planning gate still refuses discovery.
    const request = amo.createExecutionRequestFromApi({
      intent: amo.EXECUTION_INTENTS.APPROVE_DISCOVERY, missionId: mission.id, mission, operatorId: 'op',
      stage: mission.stage, objective: mission.objective, runtimeOwner: amo.resolveMissionRuntimeOwner(mission),
      permissions: { canExecute: true }, pendingOperatorDecision: mission.pendingOperatorDecision, payload: {}, metadata: {},
    });
    await assert.rejects(
      routeExecutionRequest(request, { engine, tenantId: TENANT, persist: false, runScout: spies.scout }),
      (err) => err.code === RESEARCH_PLANNING_REQUIRED
    );
    assertNoDispatch();
  });

  it('still review-only after a non-external lift (content): stays Review Only', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    await h.ask('Approve the plan.');
    await amend(h.engine, h.mission(), { lift: ['content'] }, 'p0-l1-lift-content');
    const after = h.mission();
    const snapshot = h.engine.inspect(after.id, { tenantId: TENANT });
    assert.equal(after.authorizationConstraints.reviewOnly, true);
    assert.equal(after.status, 'Plan approved (review only)');
    assert.equal(deriveProgressionStage(snapshot), PROGRESSION_STAGES.REVIEW_ONLY);
    assert.equal(deriveWorkspaceMode({ missionId: after.id, snapshot }), WORKSPACE_MODES.REVIEW_ONLY);
    assertNoDispatch();
  });
});
