'use strict';

/**
 * P0 — execution approval authorization binding.
 *
 *   - Phrases ("launch the campaign", "send the emails", "run Scout", "go ahead")
 *     never authorize a broader or different action than the pending decision.
 *   - Preparation, authorization and actual sending stay distinct: an execution
 *     approval makes the prepared sends eligible; it never sends.
 *   - An approval is bound to tenant, mission, action and the exact prepared
 *     package revision the operator was shown. A stale or unreviewed approval
 *     is refused and the current package is shown again.
 *
 * Deterministic: no network, no database, no provider. Scout and the outbound
 * transport are stubs; the transport must never be called.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const A = require('../AmoOperatorApproval');
const { createHermeticWorkspaceEngine: createWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime, liveScoutStub } = require('./amoTestRuntime');
const {
  presentedExecutionRevision,
  recordPresentedExecutionDecision,
} = require('../AcquisitionMissionExecution');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';
const TENANT = '10';

async function readyMission() {
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: TENANT, objective: OBJECTIVE, targetSegment: 'Law Firms' });
  const g = () => engine.get(mission.id, TENANT);
  await A.advancePlanAfterApproval({ engine, mission, tenantId: TENANT, question: 'Approved.' });
  await A.advanceDiscoveryAfterApproval({
    engine, mission, tenantId: TENANT, question: 'Approved. Begin Discovery.', runScout: liveScoutStub,
  });
  await A.advancePrioritizationAfterApproval({ engine, mission: g(), tenantId: TENANT, question: 'Approved prioritization.' });
  await A.advanceMaxPrioritization({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true });
  await A.advanceAcquisitionApproach({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true });
  await A.advancePaigeVariants({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true, question: 'Generate variants.' });
  await A.advanceEmmettCapacity({ engine, mission: g(), tenantId: TENANT, allowFixtureFallback: true, question: 'Plan capacity.' });
  return { engine, mission };
}

function contributions(engine, mission) {
  return engine.inspect(mission.id, { tenantId: TENANT }).contributions;
}
function approved(engine, mission) {
  return amo.isExecutionApproved(contributions(engine, mission), mission.id);
}
function revision(engine, mission) {
  return amo.computePreparedArtifactRevision(mission.id, contributions(engine, mission));
}
function contentDigest(engine, mission) {
  return amo.computePreparedContentDigest(mission.id, contributions(engine, mission));
}
function changePackage(engine, mission) {
  engine.contribute(mission.id, {
    specialist: amo.SPECIALISTS.PAIGE,
    kind: amo.CONTRIBUTION_KINDS.VARIANTS,
    payload: { variants: [{ label: 'Changed', subject: 'Changed subject', body: 'Changed body', cta: 'Reply' }] },
  }, { tenantId: TENANT });
}
function routedIntents() {
  return amo.listExecutionRouterAudit().map((a) => [a.intent, a.outcome]);
}

async function chat(engine, mission) {
  const ws = createWorkspaceEngine({
    missionsEnabled: true,
    resolverEnabled: true,
    acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
  });
  const opened = ws.open({ tenantId: TENANT, page: 'command-deck' });
  const session = ws._sessions.get(opened.sessionId);
  session.context.missionId = mission.id;
  session.context.acquisitionMissionId = mission.id;
  const ask = (question) => ws.ask({ sessionId: opened.sessionId, question });
  return { ws, session, ask };
}

/** Show the package in this session (an ambiguous reply re-renders it). */
async function showPackage(ask) {
  const shown = await ask("Let's run it");
  assert.equal(shown.resolution.action, 'clarify');
  assert.match(shown.prose, /## Execution Ready/);
  return shown;
}

describe('P0 approval binding — phrase scope at the execution decision', () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  for (const phrase of ['Go ahead and launch the campaign', 'Go ahead and send the emails']) {
    it(`"${phrase}" after review authorizes only the pending execution approval — nothing is sent`, async () => {
      const { engine, mission } = await readyMission();
      const { ask } = await chat(engine, mission);
      await showPackage(ask);
      amo.clearExecutionRouterAudit();
      const result = await ask(phrase);
      assert.equal(result.resolution.action, 'execution_approved');
      assert.deepEqual(routedIntents(), [['APPROVE_EXECUTION', 'dispatched']], 'no EXECUTE_OUTBOUND from chat');
      assert.equal(engine.get(mission.id, TENANT).stage, amo.STAGES.READY, 'approval does not advance to EXECUTE');
      assert.equal(result.structured.metadata.executionState, 'committed_internal');
      assert.match(result.prose, /Prepared — not sent/);
      assert.equal(approved(engine, mission), true);
    });
  }

  for (const [phrase, named] of [
    ['Go ahead and run Scout', 'discovery'],
    ['Approve the plan', 'plan'],
    ['Approve prioritization', 'prioritization'],
  ]) {
    it(`"${phrase}" cannot approve the pending execution decision`, async () => {
      const { engine, mission } = await readyMission();
      const { ask } = await chat(engine, mission);
      const result = await ask(phrase);
      assert.equal(result.resolution.action, 'clarify');
      assert.match(result.prose, new RegExp(`^Not executed: you asked for ${named}`));
      assert.deepEqual(routedIntents(), []);
      assert.equal(approved(engine, mission), false);
    });
  }

  it('a bare "Go ahead" does not authorize external execution', async () => {
    const { engine, mission } = await readyMission();
    const { ask } = await chat(engine, mission);
    await ask('Go ahead');
    assert.deepEqual(routedIntents(), []);
    assert.equal(approved(engine, mission), false);
  });
});

describe('P0 approval binding — reviewed package, current decision, no reuse', () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  it('an approval without a package shown in this conversation is refused and the package is shown', async () => {
    const { engine, mission } = await readyMission();
    const { ask } = await chat(engine, mission);
    const refused = await ask('Go ahead and send the emails');
    assert.match(refused.prose, /^Not executed: The prepared outreach has not been shown/);
    assert.match(refused.prose, /Nothing was approved or sent/);
    assert.match(refused.prose, /## Execution Ready/);
    assert.equal(refused.structured.metadata.executionState, 'blocked');
    assert.equal(refused.structured.metadata.blocker, 'tme_execution_approval_unreviewed');
    assert.deepEqual(routedIntents(), [['APPROVE_EXECUTION', 'rolled_back']]);
    assert.equal(approved(engine, mission), false);
    assert.equal(engine.get(mission.id, TENANT).pendingOperatorDecision.kind, 'execution_approval');

    amo.clearExecutionRouterAudit();
    const second = await ask('Approve');
    assert.equal(second.structured.metadata.executionState, 'committed_internal');
    assert.equal(approved(engine, mission), true);
  });

  it('a package changed after review makes the approval stale; it is refused and re-shown', async () => {
    const { engine, mission } = await readyMission();
    const { ask } = await chat(engine, mission);
    await showPackage(ask);
    changePackage(engine, mission);
    amo.clearExecutionRouterAudit();
    const refused = await ask('Approve');
    assert.match(refused.prose, /^Not executed: The prepared outreach changed after it was shown/);
    assert.match(refused.prose, /Changed subject/, 'the current package is shown');
    assert.equal(refused.structured.metadata.blocker, 'tme_execution_approval_stale');
    assert.equal(approved(engine, mission), false);

    const after = await ask('Approve');
    assert.equal(after.structured.metadata.executionState, 'committed_internal');
    const approval = amo.findValidExecutionApproval(contributions(engine, mission), mission.id);
    assert.equal(approval.payload.preparedArtifactRevision, revision(engine, mission));
    assert.equal(approval.payload.missionId, mission.id);
  });

  it('a consumed approval cannot be reused once the package changes, and sending is refused', async () => {
    const { engine, mission } = await readyMission();
    const { ask } = await chat(engine, mission);
    await showPackage(ask);
    await ask('Approve');
    assert.equal(approved(engine, mission), true);
    changePackage(engine, mission);
    assert.equal(approved(engine, mission), false);

    let sends = 0;
    const routed = await amo.routeExecutionRequest(
      amo.createExecutionRequest({
        source: amo.EXECUTION_SOURCES.API,
        intent: amo.EXECUTION_INTENTS.EXECUTE_OUTBOUND,
        missionId: mission.id,
        operatorId: 'operator-1',
        stage: amo.STAGES.READY,
      }),
      { engine, tenantId: TENANT, maxSends: 1, sendEmail: async () => { sends += 1; return { messageId: 'never' }; } },
    );
    assert.equal(routed.executionResult.rolledBack, true);
    assert.equal(routed.executionResult.error.code, 'tme_execution_not_approved');
    assert.equal(sends, 0);
  });

  it('presented revision is per mission: a package shown for one mission does not bind another', () => {
    const session = { context: { missionId: 'm_a', acquisitionMissionId: 'm_a' } };
    recordPresentedExecutionDecision(session, {
      mission: { id: 'm_a', pendingOperatorDecision: { kind: 'execution_approval', preparedArtifactRevision: 'rev_a' } },
      prose: '## Execution Ready\n...',
      structured: { metadata: { acquisitionMission: true } },
    });
    assert.equal(presentedExecutionRevision(session, 'm_a'), 'rev_a');
    assert.equal(presentedExecutionRevision(session, 'm_b'), null);
  });

  it('a turn that only mentions the decision (no package rendered) does not count as review', () => {
    const session = { context: { missionId: 'm_a', acquisitionMissionId: 'm_a' } };
    recordPresentedExecutionDecision(session, {
      mission: { id: 'm_a', pendingOperatorDecision: { kind: 'execution_approval', preparedArtifactRevision: 'rev_a' } },
      prose: 'You are deciding: Authorize external execution of prepared outreach?',
      structured: { metadata: { pendingDecisionTurnOwnership: true } },
    });
    assert.equal(presentedExecutionRevision(session, 'm_a'), null);
  });
});

describe('P0 approval binding — API surface and tenant', () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  function approveRequest(mission, payload) {
    return amo.createExecutionRequest({
      source: amo.EXECUTION_SOURCES.APPROVAL_BUTTON,
      intent: amo.EXECUTION_INTENTS.APPROVE_EXECUTION,
      missionId: mission.id,
      operatorId: 'operator-1',
      stage: amo.STAGES.READY,
      ...(payload ? { payload } : {}),
    });
  }

  it('API/button approval with the revision but no content digest is refused', async () => {
    const { engine, mission } = await readyMission();
    const routed = await amo.routeExecutionRequest(
      approveRequest(mission, { expectedPreparedArtifactRevision: revision(engine, mission) }), { engine, tenantId: TENANT });
    assert.equal(routed.executionResult.error.code, 'tme_execution_approval_unreviewed');
    assert.equal(approved(engine, mission), false);
  });

  it('API/button approval without the reviewed revision is refused', async () => {
    const { engine, mission } = await readyMission();
    const routed = await amo.routeExecutionRequest(approveRequest(mission), { engine, tenantId: TENANT });
    assert.equal(routed.executionResult.rolledBack, true);
    assert.equal(routed.executionResult.error.code, 'tme_execution_approval_unreviewed');
    assert.equal(approved(engine, mission), false);
  });

  it('API/button approval naming an outdated revision is refused', async () => {
    const { engine, mission } = await readyMission();
    const reviewed = { expectedPreparedArtifactRevision: revision(engine, mission), expectedPreparedContentDigest: contentDigest(engine, mission) };
    changePackage(engine, mission);
    const routed = await amo.routeExecutionRequest(
      approveRequest(mission, reviewed), { engine, tenantId: TENANT });
    assert.equal(routed.executionResult.error.code, 'tme_execution_approval_stale');
    assert.equal(approved(engine, mission), false);
  });

  it('API/button approval naming the current revision is committed and bound to it', async () => {
    const { engine, mission } = await readyMission();
    const current = revision(engine, mission);
    const routed = await amo.routeExecutionRequest(
      approveRequest(mission, { expectedPreparedArtifactRevision: current, expectedPreparedContentDigest: contentDigest(engine, mission) }),
      { engine, tenantId: TENANT });
    assert.equal(routed.action, 'execution_approved');
    const approval = amo.findValidExecutionApproval(contributions(engine, mission), mission.id);
    assert.equal(approval.payload.preparedArtifactRevision, current);
    assert.ok(approval.payload.plannedSendCount >= 0, 'scope: planned sends recorded');
  });

  it('an approval routed under another tenant is refused before anything runs', async () => {
    const { engine, mission } = await readyMission();
    await assert.rejects(
      amo.routeExecutionRequest(
        approveRequest(mission, { expectedPreparedArtifactRevision: revision(engine, mission), expectedPreparedContentDigest: contentDigest(engine, mission) }),
        { engine, tenantId: '2' },
      ),
      /does not belong to this tenant/,
    );
    assert.equal(approved(engine, mission), false);
  });

  it('a chat session for another tenant cannot approve this tenant\'s execution', async () => {
    const { engine, mission } = await readyMission();
    const ws = createWorkspaceEngine({
      missionsEnabled: true, resolverEnabled: true, acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    });
    const opened = ws.open({ tenantId: '2', page: 'command-deck' });
    const session = ws._sessions.get(opened.sessionId);
    session.context.missionId = mission.id;
    session.context.acquisitionMissionId = mission.id;
    await assert.rejects(ws.ask({ sessionId: opened.sessionId, question: 'Go ahead and send the emails' }), /does not belong to this tenant/);
    assert.equal(approved(engine, mission), false);
  });
});
