'use strict';

/**
 * AUDIT-085 / SPEC-186 — Canonical single-sender enforcement on AMO EXECUTE.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  createExecutionRequest,
  routeExecutionRequest,
  clearExecutionRouterAudit,
  computePreparedArtifactRevision,
  computePreparedArtifactBinding,
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
  advanceExecuteOutbound,
} = require('../../max/workspace/AmoOperatorApproval');
const { executeOutboundBundle } = require('../../max/workspace/OutboundExecutionAdapter');
const { FIXTURE_CANONICAL_SENDER } = require('../../max/workspace/EmmettCapacityExecution');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

const ANCHOR_SENDER = {
  tenantId: '10',
  clientId: 10,
  senderEmail: 'anchor@domain.com',
  senderName: 'Anchor Cleaning',
  sendingDomain: 'domain.com',
};

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

function infrastructureFor(sender) {
  return {
    tenantId: String(sender.tenantId || sender.clientId || '10'),
    clientId: sender.clientId || 10,
    inboxId: sender.senderEmail,
    domain: sender.sendingDomain,
    senderEmail: sender.senderEmail,
    senderName: sender.senderName,
    sendingDomain: sender.sendingDomain,
    inboxAgeDays: 45,
    providerCeiling: 50,
    authentication: { spf: 'pass', dkim: 'pass', dmarc: 'none' },
    warmup: { status: 'healthy', dailyCap: 50, activeSendDays: 14, reset: false },
    bounceRate: 0,
    replyRate: 0.08,
    openRate: 0.35,
    complaintRate: 0,
    blacklist: { listed: false, sources: [] },
    sentToday: 0,
    sentYesterday: 5,
    historicalDailyAvg: 8,
    recentSends: 40,
    replyByWeekday: { Tue: 0.12, Fri: 0.06 },
  };
}

describe('SPEC-186 — Canonical single-sender enforcement', () => {
  let engine;
  let mission;
  const priorFrom = process.env.FROM_EMAIL;

  beforeEach(() => {
    clearExecutionRouterAudit();
    process.env.FROM_EMAIL = 'hello@gopulseforge.com';
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  afterEach(() => {
    if (priorFrom == null) delete process.env.FROM_EMAIL;
    else process.env.FROM_EMAIL = priorFrom;
  });

  async function throughExecutionApproved(sender = ANCHOR_SENDER) {
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
      engine,
      mission: engine.get(mission.id, '10'),
      tenantId: '10',
      allowFixtureFallback: true,
      infrastructureSnapshot: infrastructureFor(sender),
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

  it('EXECUTE bundle uses clients.sender_email regardless of FROM_EMAIL', async () => {
    const snapshot = await throughExecutionApproved(ANCHOR_SENDER);
    const bundleResult = buildExecutionBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      resolveProspectAttributes,
      canonicalSender: ANCHOR_SENDER,
    });
    assert.equal(bundleResult.ok, true);
    assert.equal(bundleResult.bundle.provider.senderIdentity, 'anchor@domain.com');
    assert.equal(bundleResult.bundle.provider.senderName, 'Anchor Cleaning');
    assert.equal(bundleResult.bundle.provider.sendingDomain, 'domain.com');
    assert.notEqual(bundleResult.bundle.provider.senderIdentity, process.env.FROM_EMAIL);
  });

  it('blocks execution when tenant sender configuration is missing', () => {
    const snapshot = {
      mission,
      contributions: [],
    };
    const bundleResult = buildExecutionBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      canonicalSender: {
        tenantId: '10',
        clientId: 10,
        senderEmail: '',
        senderName: '',
        sendingDomain: '',
      },
    });
    assert.equal(bundleResult.ok, false);
    assert.match(bundleResult.blockReason, /incomplete|required|approval/i);
    assert.notEqual(bundleResult.blockReason, process.env.FROM_EMAIL);
  });

  it('blocks when sender email domain does not match sending_domain', async () => {
    const snapshot = await throughExecutionApproved(ANCHOR_SENDER);
    const bundleResult = buildExecutionBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      resolveProspectAttributes,
      canonicalSender: {
        ...ANCHOR_SENDER,
        senderEmail: 'anchor@other.com',
      },
    });
    assert.equal(bundleResult.ok, false);
    assert.match(bundleResult.blockReason, /domain/i);
  });

  it('blocks canonical AMO send when the provider sender is inactive', async () => {
    const snapshot = await throughExecutionApproved(ANCHOR_SENDER);
    const providerCalls = [];
    const result = await executeOutboundBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      tenantId: '10',
      sendEmail: mockSendEmailFactory(providerCalls),
      resolveProspectAttributes,
      canonicalSender: ANCHOR_SENDER,
      brevoState: {
        domain: { verified: true, authenticated: false },
        sender: { email: ANCHOR_SENDER.senderEmail, active: false },
        errors: [],
      },
    });
    assert.equal(result.blocked, true);
    assert.match(result.blockReason, /Sender email|Sending domain|ready/i);
    assert.equal(providerCalls.length, 0);
  });

  it('cannot send under an approval prepared for a different CAPACITY identity', async () => {
    const snapshot = await throughExecutionApproved(ANCHOR_SENDER);
    const providerCalls = [];
    const result = await executeOutboundBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      tenantId: '10',
      sendEmail: mockSendEmailFactory(providerCalls),
      resolveProspectAttributes,
      canonicalSender: {
        tenantId: '10',
        clientId: 10,
        senderEmail: 'other@changed.com',
        senderName: 'Changed',
        sendingDomain: 'changed.com',
      },
      senderReadiness: { ready: true },
    });
    assert.equal(result.blocked, true);
    assert.match(result.blockReason, /stale|changed since CAPACITY/i);
    assert.equal(providerCalls.length, 0);
  });

  it('sender/domain change alters prepared artifact revision', () => {
    const empty = computePreparedArtifactBinding(mission.id, []);
    const withA = computePreparedArtifactBinding(mission.id, [{
      id: 'cap-a',
      specialist: SPECIALISTS.EMMETT,
      kind: CONTRIBUTION_KINDS.CAPACITY,
      payload: {
        capacity: { recommended: 1 },
        queue: { items: [] },
        senderIdentity: {
          senderEmail: 'a@domain-a.com',
          sendingDomain: 'domain-a.com',
        },
      },
    }]);
    const withB = computePreparedArtifactBinding(mission.id, [{
      id: 'cap-a',
      specialist: SPECIALISTS.EMMETT,
      kind: CONTRIBUTION_KINDS.CAPACITY,
      payload: {
        capacity: { recommended: 1 },
        queue: { items: [] },
        senderIdentity: {
          senderEmail: 'b@domain-b.com',
          sendingDomain: 'domain-b.com',
        },
      },
    }]);
    assert.notEqual(withA.senderEmail, withB.senderEmail);
    assert.notEqual(
      computePreparedArtifactRevision(mission.id, [{
        id: 'cap-a',
        specialist: SPECIALISTS.EMMETT,
        kind: CONTRIBUTION_KINDS.CAPACITY,
        payload: { capacity: { recommended: 1 }, queue: { items: [] }, senderIdentity: { senderEmail: 'a@domain-a.com', sendingDomain: 'domain-a.com' } },
      }]),
      computePreparedArtifactRevision(mission.id, [{
        id: 'cap-a',
        specialist: SPECIALISTS.EMMETT,
        kind: CONTRIBUTION_KINDS.CAPACITY,
        payload: { capacity: { recommended: 1 }, queue: { items: [] }, senderIdentity: { senderEmail: 'b@domain-b.com', sendingDomain: 'domain-b.com' } },
      }])
    );
    assert.equal(empty.senderEmail, null);
  });

  it('provider receives the exact canonical email and name', async () => {
    const snapshot = await throughExecutionApproved(ANCHOR_SENDER);
    const providerCalls = [];
    const result = await executeOutboundBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      tenantId: '10',
      sendEmail: mockSendEmailFactory(providerCalls),
      resolveProspectAttributes,
      canonicalSender: ANCHOR_SENDER,
      senderReadiness: { ready: true },
    });
    assert.equal(result.blocked, false);
    assert.ok(providerCalls.length >= 1);
    assert.deepEqual(providerCalls[0].sender, {
      email: 'anchor@domain.com',
      name: 'Anchor Cleaning',
    });
    assert.equal(providerCalls[0].requireExplicitSender, true);
  });

  it('READY → approval → EXECUTE still correlates with canonical sender binding', async () => {
    await throughExecutionApproved(FIXTURE_CANONICAL_SENDER);
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
      canonicalSender: FIXTURE_CANONICAL_SENDER,
      senderReadiness: { ready: true },
    });
    assert.equal(routed.executionResult.executionOutcome, 'completed');
    assert.ok(providerCalls.length >= 1);
    assert.equal(providerCalls[0].sender.email, FIXTURE_CANONICAL_SENDER.senderEmail);
  });
});
