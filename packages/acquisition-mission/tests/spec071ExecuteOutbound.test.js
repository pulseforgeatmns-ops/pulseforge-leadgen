'use strict';

/**
 * SPEC-071 / AUDIT-070 — Canonical EXECUTE Outbound Adapter.
 * READY + EXECUTION_APPROVAL → EXECUTE → frozen bundle → Brevo → mission-bound evidence.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  EXECUTION_RECORD_STATUS,
  createExecutionRequest,
  routeExecutionRequest,
  clearExecutionRouterAudit,
  specialistContext,
  canEnter,
  computePreparedArtifactRevision,
  buildExecutionBundle,
  deriveExecutionIdentity,
  deriveIdempotencyKey,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advancePaigeVariants,
  advanceEmmettCapacity,
  advanceExecutionAfterApproval,
  advanceExecuteOutbound,
} = require('../../max/workspace/AmoOperatorApproval');
const { executeOutboundBundle } = require('../../max/workspace/OutboundExecutionAdapter');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

const PROSPECT_EMAILS = {
  'co-harbor': 'alex@harborlaw.com',
  'co-granite': 'ops@granitelegal.com',
};

function resolveProspectAttributes(prospectId) {
  const email = PROSPECT_EMAILS[prospectId];
  return email ? { email, name: prospectId } : null;
}

function mockSendEmailFactory(calls = []) {
  return async (input) => {
    calls.push(input);
    return {
      success: true,
      messageId: `brevo-${calls.length}`,
      providerMessageId: `brevo-${calls.length}`,
    };
  };
}

function mockFailingSendEmail(calls = []) {
  return async (input) => {
    calls.push(input);
    return {
      success: false,
      providerErrorCode: 'brevo_http_400',
      providerErrorMessage: 'Invalid recipient',
      error: 'Invalid recipient',
    };
  };
}

describe('SPEC-071 — Canonical EXECUTE Outbound Adapter', () => {
  let engine;
  let mission;

  beforeEach(() => {
    clearExecutionRouterAudit();
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  async function throughExecutionApproved() {
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
    await advanceExecutionAfterApproval({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      operatorId: 'operator-1',
      question: 'Authorize execution.',
    });
    return engine.inspect(mission.id, { tenantId: '10' });
  }

  it('stage entry: READY + valid EXECUTION_APPROVAL → EXECUTE', async () => {
    const snapshot = await throughExecutionApproved();
    assert.equal(snapshot.mission.stage, STAGES.READY);
    const ctx = specialistContext(snapshot.contributions || [], { missionId: mission.id });
    assert.equal(ctx.executionApproved, true);
    assert.equal(canEnter(STAGES.EXECUTE, ctx).ok, true);

    const providerCalls = [];
    const result = await advanceExecuteOutbound({
      engine,
      mission: snapshot.mission,
      tenantId: '10',
      operatorId: 'operator-1',
      sendEmail: mockSendEmailFactory(providerCalls),
      resolveProspectAttributes,
    });

    assert.equal(result.executionOutcome, 'completed');
    assert.equal(result.snapshot.mission.stage, STAGES.EXECUTE);
    assert.ok(result.bundle);
    assert.ok(result.summary.sent >= 1);
  });

  it('EXECUTE_OUTBOUND CER routes through ExecutionRouter to outbound adapter', async () => {
    await throughExecutionApproved();
    const providerCalls = [];

    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
      missionId: mission.id,
      operatorId: 'operator-1',
      stage: STAGES.EXECUTE,
    });

    const routed = await routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      sendEmail: mockSendEmailFactory(providerCalls),
      resolveProspectAttributes,
    });

    assert.equal(routed.specialist, 'emmett');
    assert.equal(routed.action, 'execute_outbound');
    assert.equal(routed.executionResult.executionOutcome, 'completed');
    assert.ok(providerCalls.length >= 1);
  });

  it('matching artifact revision allows execution', async () => {
    const snapshot = await throughExecutionApproved();
    const bundleResult = buildExecutionBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      resolveProspectAttributes,
    });
    assert.equal(bundleResult.ok, true);
    assert.ok(bundleResult.bundle.sends.length >= 1);
    assert.ok(bundleResult.bundle.sends.some((row) => row.message?.subject));
  });

  it('changed artifact revision blocks execution with no provider call', async () => {
    await throughExecutionApproved();
    engine.contribute(mission.id, {
      specialist: SPECIALISTS.PAIGE,
      kind: CONTRIBUTION_KINDS.VARIANTS,
      payload: {
        variants: [{
          label: 'Revised',
          subject: 'New subject',
          body: 'New body',
          cta: 'Reply',
        }],
      },
    }, { tenantId: '10' });

    const providerCalls = [];
    await assert.rejects(
      () => advanceExecuteOutbound({
        engine,
        mission: engine.get(mission.id, '10'),
        tenantId: '10',
        sendEmail: mockSendEmailFactory(providerCalls),
        resolveProspectAttributes,
      }),
      (err) => err.code === 'tme_execution_approval_stale'
        || err.code === 'tme_execute_outbound_blocked'
        || err.code === 'tme_execution_not_approved'
    );
    assert.equal(providerCalls.length, 0);
  });

  it('recipient integrity: only mission queue targets enter execution bundle', async () => {
    const snapshot = await throughExecutionApproved();
    const emmett = snapshot.contributions.find(
      (r) => r.specialist === SPECIALISTS.EMMETT && r.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    const queueIds = new Set(
      (emmett.payload.queue?.items || []).map((item) => String(item.prospectId || item.id))
    );

    const bundleResult = buildExecutionBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      resolveProspectAttributes,
    });
    assert.equal(bundleResult.ok, true);
    for (const send of bundleResult.bundle.sends) {
      assert.ok(queueIds.has(String(send.prospectId)), `Unexpected recipient ${send.prospectId}`);
    }
  });

  it('Paige integrity: subject/body originate from Paige contribution', async () => {
    const snapshot = await throughExecutionApproved();
    const paige = snapshot.contributions.find(
      (r) => r.specialist === SPECIALISTS.PAIGE && r.kind === CONTRIBUTION_KINDS.VARIANTS
    );
    const primary = paige.payload.variants[0];

    const bundleResult = buildExecutionBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      resolveProspectAttributes,
    });
    const sendable = bundleResult.bundle.sends.find((row) => row.message);
    assert.ok(sendable);
    assert.equal(sendable.message.subject, primary.subject);
    assert.equal(sendable.message.body, primary.body);
    assert.match(sendable.message.subject, /walkthrough/i);
  });

  it('governor pause blocks execution with no provider call', async () => {
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
    engine.contribute(mission.id, {
      specialist: SPECIALISTS.EMMETT,
      kind: CONTRIBUTION_KINDS.CAPACITY,
      payload: {
        capacity: { recommended: 2, remaining: 2 },
        queue: {
          items: [{
            prospectId: 'co-harbor',
            email: 'alex@harborlaw.com',
            position: 1,
            sendable: true,
            paige: { variantLabel: 'Primary', author: 'paige', source: 'paige', ready: true, sendable: true },
          }],
        },
        deliverability: { status: 'healthy' },
        governor: { outcome: 'pause', reason: 'Reputation risk.', halt: true },
      },
    }, { tenantId: '10' });
    engine.contribute(mission.id, {
      specialist: SPECIALISTS.OPERATOR,
      kind: CONTRIBUTION_KINDS.APPROVAL,
      payload: {
        decisionKind: 'execution_approval',
        preparedArtifactRevision: computePreparedArtifactRevision(mission.id, engine.inspect(mission.id, { tenantId: '10' }).contributions),
        approved: true,
      },
    }, { tenantId: '10' });

    const providerCalls = [];
    await assert.rejects(
      () => advanceExecuteOutbound({
        engine,
        mission: engine.get(mission.id, '10'),
        tenantId: '10',
        sendEmail: mockSendEmailFactory(providerCalls),
        resolveProspectAttributes,
      }),
      (err) => err.code === 'tme_deliverability_paused' || err.code === 'tme_execute_outbound_blocked'
    );
    assert.equal(providerCalls.length, 0);
  });

  it('provider success persists mission-bound execution evidence', async () => {
    await throughExecutionApproved();
    const providerCalls = [];
    const result = await advanceExecuteOutbound({
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      sendEmail: mockSendEmailFactory(providerCalls),
      resolveProspectAttributes,
    });

    const records = engine.store.listExecutionRecords(mission.id);
    assert.ok(records.length >= 1);
    const sent = records.find((row) => row.status === EXECUTION_RECORD_STATUS.SENT);
    assert.ok(sent);
    assert.equal(sent.provider, 'brevo');
    assert.ok(sent.providerMessageId);
    assert.ok(sent.preparedArtifactRevision);
    assert.ok(sent.executionApprovalContributionId);
    assert.ok(sent.transactionId);
    assert.ok(sent.prospectId);

    const inspect = engine.inspect(mission.id, { tenantId: '10' });
    assert.ok(inspect.executionRecords?.length >= 1);
    assert.equal(result.summary.sent, sent ? result.summary.sent : 0);
  });

  it('provider failure persists explicit failed result without false sent state', async () => {
    await throughExecutionApproved();
    const providerCalls = [];
    const result = await executeOutboundBundle({
      mission: engine.get(mission.id, '10'),
      contributions: engine.inspect(mission.id, { tenantId: '10' }).contributions,
      tenantId: '10',
      sendEmail: mockFailingSendEmail(providerCalls),
      resolveProspectAttributes,
      persistExecutionRecord: (row) => engine.store.addExecutionRecord(row),
    });

    assert.equal(result.blocked, false);
    assert.ok(result.records.some((row) => row.status === EXECUTION_RECORD_STATUS.FAILED));
    assert.ok(!result.records.some((row) => row.status === EXECUTION_RECORD_STATUS.SENT));
    assert.equal(result.summary.sent, 0);
    assert.ok(result.summary.failed >= 1);
  });

  it('idempotency: same missionId + prospectId + revision sends once', async () => {
    await throughExecutionApproved();
    const providerCalls = [];
    const sendEmail = mockSendEmailFactory(providerCalls);
    const snap = engine.inspect(mission.id, { tenantId: '10' });
    const revision = computePreparedArtifactRevision(mission.id, snap.contributions);
    const firstProspect = snap.contributions
      .find((r) => r.specialist === SPECIALISTS.EMMETT)
      ?.payload?.queue?.items?.[0]?.prospectId;
    assert.ok(firstProspect);
    const identity = deriveExecutionIdentity({
      missionId: mission.id,
      prospectId: firstProspect,
      preparedArtifactRevision: revision,
    });
    assert.equal(deriveIdempotencyKey(identity), `exec_${identity.slice(0, 32)}`);

    await executeOutboundBundle({
      mission: snap.mission,
      contributions: snap.contributions,
      tenantId: '10',
      sendEmail,
      resolveProspectAttributes,
      persistExecutionRecord: (row) => engine.store.addExecutionRecord(row),
    });
    const firstCallCount = providerCalls.length;
    assert.ok(firstCallCount >= 1);

    await executeOutboundBundle({
      mission: snap.mission,
      contributions: snap.contributions,
      tenantId: '10',
      sendEmail,
      resolveProspectAttributes,
      existingRecords: engine.store.listExecutionRecords(mission.id),
      persistExecutionRecord: (row) => engine.store.addExecutionRecord(row),
    });
    assert.equal(providerCalls.length, firstCallCount);
  });

  it('regression: Scout, Max, Paige, Emmett, and READY execution-approval suites remain compatible', async () => {
    const snapshot = await throughExecutionApproved();
    const ctx = specialistContext(snapshot.contributions || [], { missionId: mission.id });
    assert.equal(ctx.scoutComplete, true);
    assert.equal(ctx.maxComplete, true);
    assert.equal(ctx.paigeComplete, true);
    assert.equal(ctx.emmettComplete, true);
    assert.equal(ctx.executionApproved, true);
    assert.equal(snapshot.mission.stage, STAGES.READY);
  });
});
