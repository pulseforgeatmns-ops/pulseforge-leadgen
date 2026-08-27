'use strict';

/**
 * Canonical EXECUTE outbound adapter — end-to-end mission send path.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  EXECUTION_RECORD_STATUS,
  createExecutionRequest,
  routeExecutionRequest,
  clearExecutionRouterAudit,
  specialistContext,
  canEnter,
  deriveExecutionIdempotencyKey,
  buildExecutionBundle,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advancePaigeVariants,
  advanceEmmettCapacity,
  advanceExecutionAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function mockProvider() {
  const calls = [];
  return {
    calls,
    provider: {
      sendEmail: async (input) => {
        calls.push(input);
        return {
          success: true,
          status: 'sent',
          provider: 'brevo',
          providerMessageId: `brevo_msg_${calls.length}`,
          attemptedAt: new Date().toISOString(),
          sentAt: new Date().toISOString(),
        };
      },
    },
  };
}

function failingProvider() {
  return {
    provider: {
      sendEmail: async () => ({
        success: false,
        status: 'failed',
        provider: 'brevo',
        providerErrorCode: '550',
        providerErrorMessage: 'Mailbox unavailable',
        attemptedAt: new Date().toISOString(),
      }),
    },
  };
}

describe('Canonical EXECUTE outbound adapter', () => {
  let engine;
  let mission;
  let mock;

  beforeEach(() => {
    clearExecutionRouterAudit();
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    mock = mockProvider();
  });

  async function throughExecutionReady() {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });
    await advancePrioritizationAfterApproval({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      question: 'Approved prioritization.',
    });
    await advanceMaxPrioritization({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
    });
    await advancePaigeVariants({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
      question: 'Generate variants.',
    });
    await advanceEmmettCapacity({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
      question: 'Plan capacity.',
    });
    await advanceExecutionAfterApproval({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      operatorId: 'operator-1',
      question: 'Authorize execution.',
    });
    return engine.inspect(mission.id, { tenantId: '10' });
  }

  const executionContext = () => ({
    engine,
    tenantId: '10',
    provider: mock.provider,
    resolveEmail: async (prospectId) => `${prospectId}@law.example.com`,
    senderIdentity: { name: 'Anchor Cleaning', email: 'outreach@example.com' },
  });

  it('stage entry: READY + valid EXECUTION_APPROVAL → EXECUTE on EXECUTE_OUTBOUND', async () => {
    await throughExecutionReady();
    const before = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(before.mission.stage, STAGES.READY);
    const ctx = specialistContext(before.contributions || [], { missionId: mission.id });
    assert.equal(ctx.executionApproved, true);
    assert.equal(canEnter(STAGES.EXECUTE, ctx).ok, true);

    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
      missionId: mission.id,
      operatorId: 'operator-1',
      stage: STAGES.READY,
    });

    const routed = await routeExecutionRequest(request, executionContext());
    assert.equal(routed.action, 'execute_outbound');
    assert.equal(routed.specialist, 'operator');

    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(after.mission.stage, STAGES.EXECUTE);
    assert.ok(routed.executionResult.summary.sent >= 1);
  });

  it('artifact revision: matching revision allows execution', async () => {
    await throughExecutionReady();
    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
      missionId: mission.id,
      operatorId: 'operator-1',
    });
    await routeExecutionRequest(request, executionContext());
    assert.ok(mock.calls.length >= 1);
  });

  it('artifact revision: changed Paige blocks execution with no provider call', async () => {
    await throughExecutionReady();
    engine.contribute(mission.id, {
      specialist: SPECIALISTS.PAIGE,
      kind: CONTRIBUTION_KINDS.VARIANTS,
      payload: {
        variants: [{
          label: 'Revised',
          subject: 'Updated subject',
          body: 'Updated body',
          cta: 'Reply',
        }],
      },
    }, { tenantId: '10' });

    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
      missionId: mission.id,
      operatorId: 'operator-1',
    });

    const routed = await routeExecutionRequest(request, executionContext());
    assert.equal(routed.executionResult.rolledBack, true);
    const errCode = routed.executionResult.error?.code
      || routed.executionResult.error?.cause?.code;
    assert.ok(
      errCode === 'amo_execution_artifact_stale'
        || errCode === 'amo_execution_not_approved'
    );
    assert.equal(mock.calls.length, 0);
  });

  it('recipient integrity: only mission queue targets enter execution bundle', async () => {
    const snapshot = await throughExecutionReady();
    const bundle = await buildExecutionBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      resolveEmail: async (id) => `${id}@law.example.com`,
      senderIdentity: { name: 'Test', email: 'test@example.com' },
    });
    const capacity = snapshot.contributions.find(
      (r) => r.specialist === SPECIALISTS.EMMETT && r.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    const queueIds = new Set((capacity.payload.queue?.items || []).map((i) => String(i.prospectId || i.id)));
    for (const send of bundle.sends) {
      if (send.prospectId) assert.ok(queueIds.has(String(send.prospectId)) || send.status === 'blocked');
    }
    assert.ok(bundle.sends.length >= 1);
  });

  it('Paige integrity: subject/body originate from Paige contribution', async () => {
    const snapshot = await throughExecutionReady();
    const paige = snapshot.contributions.find(
      (r) => r.specialist === SPECIALISTS.PAIGE && r.kind === CONTRIBUTION_KINDS.VARIANTS
    );
    const variant = paige.payload.variants[0];

    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
      missionId: mission.id,
      operatorId: 'operator-1',
    });
    await routeExecutionRequest(request, executionContext());

    assert.ok(mock.calls.length >= 1);
    assert.equal(mock.calls[0].subject, variant.subject);
    assert.ok(mock.calls[0].body.includes(variant.body.split('\n')[0].slice(0, 20)));
  });

  it('governor integrity: paused governor blocks bundle construction', async () => {
    const contributions = [
      { id: 'max1', specialist: SPECIALISTS.MAX, kind: CONTRIBUTION_KINDS.PRIORITIZATION, payload: { rankedTargets: [{ prospectId: 'p1', name: 'Co' }] } },
      { id: 'paige1', specialist: SPECIALISTS.PAIGE, kind: CONTRIBUTION_KINDS.VARIANTS, payload: { variants: [{ label: 'Primary', subject: 'S', body: 'B', cta: 'C' }] } },
      {
        id: 'emmett1',
        specialist: SPECIALISTS.EMMETT,
        kind: CONTRIBUTION_KINDS.CAPACITY,
        payload: {
          capacity: { recommended: 0, remaining: 0 },
          queue: { items: [{ prospectId: 'p1', position: 1, email: 'p1@example.com' }] },
          governor: { outcome: 'pause', reason: 'Reputation risk.', halt: true },
        },
      },
      {
        id: 'ap1',
        specialist: SPECIALISTS.OPERATOR,
        kind: CONTRIBUTION_KINDS.APPROVAL,
        payload: { decisionKind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL },
      },
    ];
    const mission = { id: 'm1', tenantId: '10' };
    const revision = amo.computePreparedArtifactRevision(mission.id, contributions);
    contributions[3].payload.preparedArtifactRevision = revision;

    await assert.rejects(
      () => buildExecutionBundle({
        mission,
        contributions,
        resolveEmail: async (id) => `${id}@example.com`,
      }),
      (err) => err.code === 'amo_governor_blocked'
    );
  });

  it('provider success: message ID persisted as mission-bound evidence', async () => {
    await throughExecutionReady();
    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
      missionId: mission.id,
      operatorId: 'operator-1',
    });
    const routed = await routeExecutionRequest(request, executionContext());
    const records = engine.store.listExecutionRecords(mission.id);
    assert.ok(records.length >= 1);
    const sent = records.find((r) => r.status === EXECUTION_RECORD_STATUS.SENT);
    assert.ok(sent);
    assert.equal(sent.provider, 'brevo');
    assert.ok(sent.providerMessageId);
    assert.ok(sent.preparedArtifactRevision);
    assert.ok(sent.executionApprovalContributionId);
    assert.equal(routed.executionResult.results[0].missionId, mission.id);
  });

  it('provider failure: explicit failed result without false sent state', async () => {
    await throughExecutionReady();
    const failMock = failingProvider();
    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
      missionId: mission.id,
      operatorId: 'operator-1',
    });
    const routed = await routeExecutionRequest(request, {
      ...executionContext(),
      provider: failMock.provider,
    });
    const records = engine.store.listExecutionRecords(mission.id);
    const failed = records.filter((r) => r.status === EXECUTION_RECORD_STATUS.FAILED);
    assert.ok(failed.length >= 1);
    assert.ok(failed[0].providerErrorMessage);
    assert.equal(routed.executionResult.summary.sent, 0);
    assert.ok(!records.some((r) => r.status === EXECUTION_RECORD_STATUS.SENT && r.prospectId === failed[0].prospectId));
  });

  it('idempotency: same mission+prospect+revision calls provider once', async () => {
    const snapshot = await throughExecutionReady();
    const revision = snapshot.contributions.find(
      (r) => r.specialist === SPECIALISTS.OPERATOR
        && r.payload?.decisionKind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL
    ).payload.preparedArtifactRevision;

    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
      missionId: mission.id,
      operatorId: 'operator-1',
    });

    await routeExecutionRequest(request, executionContext());
    const firstCallCount = mock.calls.length;
    assert.ok(firstCallCount >= 1);

    await routeExecutionRequest(request, executionContext());
    assert.equal(mock.calls.length, firstCallCount);

    const queueItem = snapshot.contributions.find(
      (r) => r.specialist === SPECIALISTS.EMMETT
    ).payload.queue.items[0];
    const key = deriveExecutionIdempotencyKey(
      mission.id,
      queueItem.prospectId,
      revision
    );
    const record = engine.store.getExecutionRecordByKey(key);
    assert.equal(record.status, EXECUTION_RECORD_STATUS.SENT);
  });

  it('fail closed without execution approval', async () => {
    await advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
    await advanceDiscoveryAfterApproval({
      engine, mission, tenantId: '10', question: 'Approved.', allowFixtureFallback: true,
    });
    await advancePrioritizationAfterApproval({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', question: 'Approved.',
    });
    await advanceMaxPrioritization({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
    });
    await advancePaigeVariants({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
    });
    await advanceEmmettCapacity({
      engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
    });

    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
      missionId: mission.id,
      operatorId: 'operator-1',
    });

    const routed = await routeExecutionRequest(request, executionContext());
    assert.equal(routed.executionResult.rolledBack, true);
    const errCode = routed.executionResult.error?.code
      || routed.executionResult.error?.cause?.code;
    assert.ok(
      errCode === 'amo_execution_not_approved'
        || routed.executionResult.error?.tmeClass === 'precondition'
    );
    assert.equal(mock.calls.length, 0);
  });
});
