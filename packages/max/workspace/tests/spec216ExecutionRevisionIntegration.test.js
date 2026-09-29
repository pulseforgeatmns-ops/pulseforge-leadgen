'use strict';

/**
 * SPEC-217 — Prepared outreach revision reaches canonical execution safely.
 * Canonical execution router integration: REQUEST_REVISION reaches advancePreparedOutreachRevision.
 * 
 * Key acceptance:
 * - executionRequested = true for REVISE_PREPARED_OUTREACH (SPEC-217 fix)
 * - Ownership remains on ACTIVE_MISSION after resolution
 * - Pending decision does NOT re-intercept (resolved, not unresolved)
 * - maybeHandleAcquisitionMissionExecution proceeds (gate check uses executionRequested)
 * - Execution request routes to canonical handler (advancePreparedOutreachRevision)
 * - No execution/send/approve side effects
 */

const { describe, it, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  createExecutionRequest,
  routeExecutionRequest,
  getExecutionRequestAudit,
} = amo;

const {
  analyzeOperatorIntent,
} = require('../OperatorIntent');

const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');

const {
  resolvePendingOperatorDecision,
} = require('../PendingDecisionResolver');

const {
  resetOperatorIntentAudit,
} = require('../audit/OperatorIntentAudit');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advanceAcquisitionApproach,
  advancePaigeVariants,
  advanceEmmettCapacity,
  advanceExecutionAfterApproval,
  advancePreparedOutreachRevision,
  buildPaigeRevisionFailureDetails,
} = require('../AmoOperatorApproval');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH.';

function executionApprovalMission(extra = {}) {
  return {
    id: 'm-revision-workflow',
    stage: STAGES.READY,
    objective: OBJECTIVE,
    pendingOperatorDecision: {
      stage: STAGES.READY,
      kind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
      prompt: 'Approve execution of the prepared outbound plan?',
    },
    ...extra,
  };
}

async function preparedRuntime() {
  const engine = amo.createAcquisitionMissionEngine();
  const created = engine.create({ tenantId: '10', objective: OBJECTIVE, targetSegment: 'Law Firms' });
  await advancePlanAfterApproval({ engine, mission: created, tenantId: '10', question: 'Approved.' });
  await advanceDiscoveryAfterApproval({
    engine, mission: created, tenantId: '10', question: 'Approved. Begin Discovery.', allowFixtureFallback: true,
  });
  await advancePrioritizationAfterApproval({
    engine, mission: engine.get(created.id, '10'), tenantId: '10', question: 'Approved prioritization.',
  });
  await advanceMaxPrioritization({
    engine, mission: engine.get(created.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advanceAcquisitionApproach({
    engine, mission: engine.get(created.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advancePaigeVariants({
    engine, mission: engine.get(created.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advanceEmmettCapacity({
    engine, mission: engine.get(created.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advanceExecutionAfterApproval({
    engine,
    mission: engine.get(created.id, '10'),
    tenantId: '10',
    operatorId: 'operator-1',
    question: 'Authorize the prepared bundle.',
  });
  return { engine, mission: engine.get(created.id, '10') };
}

describe('SPEC-217 — Prepared outreach revision executes in the canonical path', () => {
  afterEach(() => {
    resetOperatorIntentAudit();
  });

  it('exact production utterance: executionRequested = true (KEY FIX)', async () => {
    const operatorUtterance = 'regenerate the messaging before execution';
    const mission = executionApprovalMission();

    const intent = await analyzeOperatorIntent({
      question: operatorUtterance,
      mission,
      resolveMission: false,
      session: { id: 's-spec216-fix', context: { tenantId: '10', missionId: mission.id } },
    });

    // Verify REQUEST_REVISION intent resolution
    assert.equal(intent.pendingDecisionResolution.outcome, 'request_revision', 'outcome is request_revision');
    assert.equal(intent.pendingDecisionResolution.executionIntent, EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH, 'executionIntent is REVISE_PREPARED_OUTREACH');
    assert.equal(intent.pendingDecisionResolution.resolved, true, 'decision is resolved');
    assert.equal(intent.pendingDecisionResolution.resolvedFromPendingDecision, true, 'marked as executable');

    // Verify conversationIntent preserves the structured intent
    assert.equal(intent.conversationIntent.via, 'pending_decision_revision_request', 'via is pending_decision_revision_request');
    assert.equal(intent.conversationIntent.executionIntent, EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH, 'conversationIntent.executionIntent is REVISE_PREPARED_OUTREACH');

    // SPEC-217: executionRequested must be true so maybeHandleAcquisitionMissionExecution proceeds.
    assert.equal(intent.executionRequested, true, 'SPEC-217: executionRequested MUST be true for REVISE_PREPARED_OUTREACH');
  });

  it('workspace ownership after resolution: ACTIVE_MISSION', async () => {
    const operatorUtterance = 'rewrite the outreach package';
    const mission = executionApprovalMission();

    const intent = await analyzeOperatorIntent({
      question: operatorUtterance,
      mission,
      resolveMission: false,
      session: { id: 's-spec216-owner', context: { tenantId: '10', missionId: mission.id } },
    });

    const ownership = await resolveWorkspaceOwner({
      question: operatorUtterance,
      session: { id: 's-spec216-owner', context: { tenantId: '10', missionId: mission.id } },
      context: { tenantId: '10', activeMissionId: mission.id },
      operatorIntent: intent,
      missionEngine: {
        activeMissionResolver: {
          resolveActiveMission: async () => mission,
        },
      },
      missionsEnabled: true,
      resolverEnabled: true,
    });

    assert.equal(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION, 'owner is ACTIVE_MISSION');
    assert.equal(ownership.reason, 'pending_decision_turn_ownership', 'resolved pending decision remains owned by ACTIVE_MISSION');
  });

  it('resolved pending decision does not retain turn via pending-decision handler', async () => {
    const operatorUtterance = 'regenerate and resend';
    const mission = executionApprovalMission();

    const intent = await analyzeOperatorIntent({
      question: operatorUtterance,
      mission,
      resolveMission: false,
      session: { id: 's-spec216-pending', context: { tenantId: '10', missionId: mission.id } },
    });

    // Resolved pending decisions should NOT pass the pendingDecisionOwnsTurn gate
    // (which would handle unresolved decisions requiring clarification)
    const pendingDecisionOwnsTurn = intent.pendingDecisionResolution.resolved === false &&
                                   intent.pendingDecisionResolution.pending === true;
    assert(!pendingDecisionOwnsTurn, 'resolved pending decision should not retain turn via pending-decision handler');

    // But the decision should still be resolved
    assert.equal(intent.pendingDecisionResolution.resolved, true, 'decision is resolved');
  });

  it('execution gate passes: executionRequested OR planningRequested is true', async () => {
    const operatorUtterance = 'prepare a new version';
    const mission = executionApprovalMission();

    const intent = await analyzeOperatorIntent({
      question: operatorUtterance,
      mission,
      resolveMission: false,
      session: { id: 's-spec216-gate', context: { tenantId: '10', missionId: mission.id } },
    });

    // The gate in maybeHandleAcquisitionMissionExecution checks:
    // if (!question || (!executionRequested && !planningTurn)) return null;
    const wouldProceed = intent.executionRequested || intent.planningRequested;
    assert(wouldProceed, 'SPEC-217: maybeHandleAcquisitionMissionExecution gate should pass (not short-circuited)');
  });

  it('structuring is preserved end-to-end', async () => {
    const operatorUtterance = 'The messaging needs revision. Regenerate the outreach.';
    const mission = executionApprovalMission();

    const intent = await analyzeOperatorIntent({
      question: operatorUtterance,
      mission,
      resolveMission: false,
      session: { id: 's-spec216-struct', context: { tenantId: '10', missionId: mission.id } },
    });

    // Verify executionIntent is preserved at every level
    assert.equal(
      intent.pendingDecisionResolution.executionIntent,
      EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      'pendingDecisionResolution.executionIntent is REVISE_PREPARED_OUTREACH'
    );
    assert.equal(
      intent.conversationIntent.executionIntent,
      EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      'conversationIntent.executionIntent is REVISE_PREPARED_OUTREACH'
    );
    assert.equal(
      intent.executionRequested,
      true,
      'executionRequested is true'
    );
    assert.equal(
      intent.mutatesMission,
      true,
      'mutatesMission is true'
    );
  });

  it('exact production revision utterance reaches the revision handler and creates a new approval boundary', async () => {
    const operatorUtterance =
      'Do not authorize this outreach. The prepared messaging needs to be regenerated before execution. ' +
      'Regenerate the outreach messaging and outbound plan for these prospects, then return the updated package for my approval. ' +
      'Do not send anything.';
    const { engine, mission } = await preparedRuntime();
    const before = engine.inspect(mission.id, { tenantId: '10' });
    const currentMission = before.mission;
    const intentMission = {
      ...currentMission,
      pendingOperatorDecision: {
        stage: STAGES.READY,
        kind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
        prompt: 'Approve execution of the prepared outbound plan?',
      },
    };
    const p1Id = before.contributions.findLast(
      (row) => row.specialist === SPECIALISTS.PAIGE && row.kind === CONTRIBUTION_KINDS.VARIANTS
    ).id;
    const e1Id = before.contributions.findLast(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    ).id;
    const approval1 = before.contributions.find(
      (row) => row.payload?.decisionKind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL
    );
    const sent = [];

    const operatorIntent = await analyzeOperatorIntent({
      question: operatorUtterance,
      mission: intentMission,
      resolveMission: false,
      session: { id: 's-spec217-lifecycle', context: { tenantId: '10', missionId: currentMission.id } },
    });
    const ownership = await resolveWorkspaceOwner({
      question: operatorUtterance,
      session: { id: 's-spec217-lifecycle', context: { tenantId: '10', missionId: currentMission.id } },
      context: { tenantId: '10', activeMissionId: currentMission.id },
      operatorIntent,
      missionEngine: { activeMissionResolver: { resolveActiveMission: async () => intentMission } },
      missionsEnabled: true,
      resolverEnabled: true,
    });
    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.CHAT,
      intent: operatorIntent.pendingDecisionResolution.executionIntent,
      missionId: currentMission.id,
      mission: currentMission,
      operatorId: 'operator-1',
      stage: STAGES.READY,
      question: operatorUtterance,
    });
    const routed = await routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
      sendEmail: async (...args) => sent.push(args),
    });
    const after = routed.executionResult.snapshot;
    const p2 = after.contributions.find(
      (row) => row.id === after.mission.revisionState.paigeContributionId
    );
    const e2 = after.contributions.find(
      (row) => row.id === after.mission.revisionState.emmettContributionId
    );
    const routerAudit = getExecutionRequestAudit(request.id);

    assert.equal(operatorIntent.executionRequested, true);
    assert.equal(
      operatorIntent.pendingDecisionResolution.executionIntent,
      EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH
    );
    assert.equal(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
    assert.equal(request.intent, EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH);
    assert.equal(routed.action, 'revise_prepared_outreach');
    assert.equal(routed.audit.intent, EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH);
    assert.equal(routerAudit.length, 1);
    assert.equal(routerAudit[0].action, 'revise_prepared_outreach');
    assert.notEqual(p2.id, p1Id);
    assert.notEqual(e2.id, e1Id);
    assert.equal(after.contributions.find((row) => row.id === p1Id).payload.superseded, true);
    assert.equal(after.contributions.find((row) => row.id === p1Id).payload.supersededBy, p2.id);
    assert.equal(after.contributions.find((row) => row.id === e1Id).payload.superseded, true);
    assert.equal(after.contributions.find((row) => row.id === e1Id).payload.supersededBy, e2.id);
    assert.equal(after.executionReview.artifactBinding.paigeContributionId, p2.id);
    assert.equal(after.executionReview.artifactBinding.emmettContributionId, e2.id);
    assert.equal(after.mission.revisionState.paigeContributionId, p2.id);
    assert.equal(after.mission.revisionState.emmettContributionId, e2.id);
    assert.equal(after.mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL);
    assert.equal(
      after.contributions.find((row) => row.id === approval1.id).payload.invalidated,
      true
    );
    assert.equal(request.intent === EXECUTION_INTENTS.APPROVE_EXECUTION, false);
    assert.equal(request.intent === EXECUTION_INTENTS.EXECUTE_OUTBOUND, false);
    assert.equal(sent.length, 0);
  });

  it('Paige non-success exposes blocker details for rollback diagnostics', () => {
    const details = buildPaigeRevisionFailureDetails({
      status: amo.EXECUTION_STATUSES.BLOCKED,
      blocked: {
        reason: 'Paige generated customer-facing copy containing internal mission or scoring language.',
        requiredPrecondition: 'internal_reasoning_leakage',
      },
      contributions: {
        variants: [{
          candidateId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
          subject: 'Cleaning for Blue Door Living Property Management',
          body: 'Mission focus: Achieve 1 recurring_clients',
          cta: 'Reply if a written quote would be useful',
        }],
      },
    }, {
      stage: STAGES.PREPARE,
      revisionState: { status: 'running' },
    });

    assert.equal(details.status, amo.EXECUTION_STATUSES.BLOCKED);
    assert.equal(details.blocker, 'internal_reasoning_leakage');
    assert.match(details.reason, /customer-facing copy/i);
    assert.equal(details.variantCount, 1);
    assert.equal(details.candidateCount, 1);
    assert.equal(details.missionStage, STAGES.PREPARE);
    assert.equal(details.missionState, 'running');
    assert.equal(details.copySafetyViolations.length, 1);
  });

  it('failed Paige revision rolls back without outbound side effects', async () => {
    const { engine, mission } = await preparedRuntime();
    const before = engine.inspect(mission.id, { tenantId: '10' });
    const beforeIds = before.contributions.map((row) => row.id);
    const sent = [];

    await assert.rejects(
      () => advancePreparedOutreachRevision({
        engine,
        mission: before.mission,
        tenantId: '10',
        operatorId: 'operator-1',
        question: 'Regenerate the outreach before execution.',
        sendEmail: async (...args) => sent.push(args),
        runPaige: async () => ({
          variants: [{
            candidateId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
            companyId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
            placeId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
            companyName: 'Blue Door Living Property Management',
            bindingScope: 'prospect',
            variantId: 'paige_v_blue_door',
            label: 'Primary - Blue Door Living Property Management',
            subject: 'Cleaning for Blue Door Living Property Management',
            body: 'Mission focus: Achieve 1 recurring_clients',
            cta: 'Reply if a written quote would be useful',
          }],
          subjects: ['Cleaning for Blue Door Living Property Management'],
          messaging: 'Mission focus: Achieve 1 recurring_clients',
          cta: 'Reply if a written quote would be useful',
          hypotheses: ['Prospect-bound messages with prospect-specific intelligence increase engagement.'],
          experiments: [],
          bindingScope: 'prospect',
        }),
      }),
      (err) => {
        assert.equal(err.code, 'internal_reasoning_leakage');
        assert.equal(err.rollback, true);
        assert.equal(err.commitStatus, 'rolled_back');
        assert.match(err.message, /customer-facing copy/i);
        return true;
      }
    );

    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.deepEqual(after.contributions.map((row) => row.id), beforeIds);
    assert.equal(sent.length, 0);
  });
});
