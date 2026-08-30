'use strict';

/**
 * AUDIT-085 — Canonical Single-Sender Enforcement.
 * one tenant → one canonical sender identity across CAPACITY → EXECUTE → Brevo → webhooks.
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
  buildExecutionBundle,
  computePreparedArtifactRevision,
  findValidExecutionApproval,
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
const {
  fixtureCanonicalSender,
  fixtureInfrastructureSnapshot,
} = require('../../max/workspace/EmmettCapacityExecution');
const {
  resolveCanonicalSenderIdentity,
  validateCanonicalSenderConfiguration,
  assertCapacitySenderBinding,
  classifyProviderEventSenderIdentity,
  evaluateCanonicalSenderReadiness,
  loadCanonicalSenderIdentity,
} = require('../../../utils/canonicalSenderIdentity');
const { sendEmail } = require('../../providers/brevo/sendEmail');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

const ANCHOR_SENDER = {
  tenantId: '10',
  clientId: 10,
  senderEmail: 'anchor@goanchorcleaning.com',
  senderName: 'Anchor Cleaning',
  sendingDomain: 'goanchorcleaning.com',
};

describe('AUDIT-085 — Canonical Single-Sender Enforcement', () => {
  let engine;
  let mission;
  let prevFrom;

  beforeEach(() => {
    clearExecutionRouterAudit();
    prevFrom = process.env.FROM_EMAIL;
    process.env.FROM_EMAIL = 'hello@gopulseforge.com';
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  afterEach(() => {
    if (prevFrom === undefined) delete process.env.FROM_EMAIL;
    else process.env.FROM_EMAIL = prevFrom;
  });

  async function throughExecutionApproved(sender = fixtureCanonicalSender('10')) {
    const infrastructureSnapshot = {
      ...fixtureInfrastructureSnapshot('10'),
      inboxId: sender.senderEmail,
      domain: sender.sendingDomain,
    };
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
      infrastructureSnapshot,
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

  it('canonical sender resolution: tenant 10 uses clients.sender_email regardless of FROM_EMAIL', () => {
    const identity = resolveCanonicalSenderIdentity({
      tenantId: 10,
      clientId: 10,
      client: {
        id: 10,
        sender_email: ANCHOR_SENDER.senderEmail,
        sender_name: ANCHOR_SENDER.senderName,
        sending_domain: ANCHOR_SENDER.sendingDomain,
      },
    });
    assert.equal(identity.senderEmail, 'anchor@goanchorcleaning.com');
    assert.notEqual(identity.senderEmail, process.env.FROM_EMAIL);
    const validated = validateCanonicalSenderConfiguration(identity);
    assert.equal(validated.ok, true);
  });

  it('no global fallback: missing tenant sender blocks execution', async () => {
    const snapshot = await throughExecutionApproved();
    const bundleResult = buildExecutionBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      tenantId: '10',
    });
    assert.equal(bundleResult.ok, false);
    assert.match(bundleResult.blockReason, /canonical sender/i);
    assert.ok(!String(bundleResult.blockReason || '').includes('gopulseforge'));
  });

  it('domain validation: senderEmail domain != sending_domain blocks', () => {
    const validated = validateCanonicalSenderConfiguration({
      tenantId: '10',
      clientId: 10,
      senderEmail: 'anchor@other-domain.com',
      senderName: 'Anchor',
      sendingDomain: 'goanchorcleaning.com',
    });
    assert.equal(validated.ok, false);
    assert.ok(validated.failures.some((row) => row.code === 'canonical_sender_domain_mismatch'));
  });

  it('EXECUTE bundle uses anchor@domain.com when that is the canonical sender', async () => {
    const snapshot = await throughExecutionApproved(ANCHOR_SENDER);
    const calls = [];
    const result = await advanceExecuteOutbound({
      engine,
      mission: snapshot.mission,
      tenantId: '10',
      canonicalSender: ANCHOR_SENDER,
      skipProviderReadiness: true,
      resolveProspectAttributes: (id) => ({ email: `${id}@harborlaw.com`, name: id }),
      sendEmail: async (input) => {
        calls.push(input);
        return { success: true, messageId: 'm1', providerMessageId: 'm1' };
      },
    });
    assert.equal(result.executionOutcome, 'completed');
    assert.equal(result.bundle.provider.senderIdentity, ANCHOR_SENDER.senderEmail);
    assert.equal(result.bundle.provider.sendingDomain, ANCHOR_SENDER.sendingDomain);
    assert.equal(calls[0].sender.email, ANCHOR_SENDER.senderEmail);
    assert.equal(calls[0].sender.name, ANCHOR_SENDER.senderName);
    assert.notEqual(calls[0].sender.email, process.env.FROM_EMAIL);
  });

  it('provider readiness: inactive sender / unauthenticated domain blocks AMO send', async () => {
    const snapshot = await throughExecutionApproved(ANCHOR_SENDER);
    const result = await executeOutboundBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      tenantId: '10',
      canonicalSender: ANCHOR_SENDER,
      skipProviderReadiness: false,
      brevoState: {
        domain: { verified: false, authenticated: false },
        sender: { email: ANCHOR_SENDER.senderEmail, active: false },
        errors: [],
      },
      sendEmail: async () => ({ success: true, messageId: 'x' }),
      resolveProspectAttributes: (id) => ({ email: `${id}@x.com`, name: id }),
    });
    assert.equal(result.blocked, true);
    assert.match(result.blockReason, /Brevo|authenticated|active/i);
  });

  it('CAPACITY binding: sender change after CAPACITY blocks existing approval', async () => {
    const snapshot = await throughExecutionApproved(ANCHOR_SENDER);
    const changed = {
      ...ANCHOR_SENDER,
      senderEmail: 'new@goanchorcleaning.com',
    };
    const bundleResult = buildExecutionBundle({
      mission: snapshot.mission,
      contributions: snapshot.contributions,
      tenantId: '10',
      canonicalSender: changed,
    });
    assert.equal(bundleResult.ok, false);
    assert.match(bundleResult.blockReason, /different sender identity|Re-approve/i);
  });

  it('prepared revision: sender/domain change alters artifact authorization', async () => {
    const snapshot = await throughExecutionApproved(ANCHOR_SENDER);
    const before = computePreparedArtifactRevision(mission.id, snapshot.contributions);
    const approval = findValidExecutionApproval(snapshot.contributions, mission.id);
    assert.ok(approval);
    assert.equal(approval.payload.preparedArtifactRevision, before);
    assert.equal(approval.payload.senderEmail, ANCHOR_SENDER.senderEmail);
    assert.equal(approval.payload.sendingDomain, ANCHOR_SENDER.sendingDomain);

    const emmett = snapshot.contributions.find(
      (r) => r.specialist === SPECIALISTS.EMMETT && r.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    emmett.payload.senderIdentity = {
      inboxId: 'drifted@goanchorcleaning.com',
      senderEmail: 'drifted@goanchorcleaning.com',
      sendingDomain: 'goanchorcleaning.com',
    };
    const after = computePreparedArtifactRevision(mission.id, snapshot.contributions);
    assert.notEqual(after, before);
    assert.equal(findValidExecutionApproval(snapshot.contributions, mission.id), null);
  });

  it('explicit provider sender required — omission errors for AMO', async () => {
    const result = await sendEmail({
      toEmail: 'a@b.com',
      subject: 'x',
      body: 'y',
      requireExplicitSender: true,
      apiKey: 'test-key',
    });
    assert.equal(result.success, false);
    assert.equal(result.providerErrorCode, 'missing_explicit_sender');
  });

  it('evaluateCanonicalSenderReadiness shares Brevo gates with legacy readiness', async () => {
    const ready = await evaluateCanonicalSenderReadiness({
      identity: ANCHOR_SENDER,
      brevoState: {
        domain: { verified: true, authenticated: true },
        sender: { email: ANCHOR_SENDER.senderEmail, active: true },
        errors: [],
      },
    });
    assert.equal(ready.sendable, true);

    const blocked = await evaluateCanonicalSenderReadiness({
      identity: {
        ...ANCHOR_SENDER,
        senderEmail: 'wrong@elsewhere.com',
      },
      brevoState: {
        domain: { verified: true, authenticated: true },
        sender: { email: 'wrong@elsewhere.com', active: true },
        errors: [],
      },
    });
    assert.equal(blocked.sendable, false);
    assert.ok(blocked.failures.some((row) => row.code === 'client_sender_domain_matches'));
  });

  it('foreign-domain webhook classification excludes reputation', () => {
    const mismatch = classifyProviderEventSenderIdentity({
      eventSendingDomain: 'domain-b.com',
      tenantSendingDomain: 'domain-a.com',
    });
    assert.equal(mismatch.status, 'mismatch');
    assert.equal(mismatch.reputationExcluded, true);
    assert.equal(mismatch.eventDomain, 'domain-b.com');
  });

  it('matching webhook domain processes as matched', () => {
    const matched = classifyProviderEventSenderIdentity({
      eventSenderEmail: 'ops@domain-a.com',
      tenantSendingDomain: 'domain-a.com',
    });
    assert.equal(matched.status, 'matched');
    assert.equal(matched.reputationExcluded, false);
  });

  it('unknown domain is distinct from confirmed mismatch', () => {
    const unknown = classifyProviderEventSenderIdentity({
      eventSendingDomain: null,
      eventSenderEmail: null,
      tenantSendingDomain: 'domain-a.com',
    });
    assert.equal(unknown.status, 'unknown');
    assert.equal(unknown.reputationExcluded, false);
    assert.match(unknown.reason, /lacks sender-domain/i);
  });

  it('CAPACITY → EXECUTE identity binding asserts inboxId/domain match', () => {
    const ok = assertCapacitySenderBinding({
      capacityPayload: {
        senderIdentity: {
          inboxId: ANCHOR_SENDER.senderEmail,
          senderEmail: ANCHOR_SENDER.senderEmail,
          sendingDomain: ANCHOR_SENDER.sendingDomain,
        },
      },
      canonicalSender: ANCHOR_SENDER,
    });
    assert.equal(ok.ok, true);

    const drift = assertCapacitySenderBinding({
      capacityPayload: {
        senderIdentity: {
          inboxId: ANCHOR_SENDER.senderEmail,
          senderEmail: ANCHOR_SENDER.senderEmail,
          sendingDomain: ANCHOR_SENDER.sendingDomain,
        },
      },
      canonicalSender: { ...ANCHOR_SENDER, sendingDomain: 'other.com', senderEmail: 'x@other.com' },
    });
    assert.equal(drift.ok, false);
    assert.equal(drift.code, 'canonical_sender_identity_drift');
  });

  it('loadCanonicalSenderIdentity fails closed without pool/client', async () => {
    const missing = await loadCanonicalSenderIdentity({ tenantId: 10 });
    assert.equal(missing.ok, false);
  });

  it('AMO regression: READY → approval → EXECUTE uses canonical sender binding', async () => {
    const snapshot = await throughExecutionApproved(ANCHOR_SENDER);
    assert.equal(snapshot.mission.stage, STAGES.READY);
    const request = createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.EXECUTE_OUTBOUND,
      missionId: mission.id,
      operatorId: 'operator-1',
      stage: STAGES.EXECUTE,
    });
    const calls = [];
    const routed = await routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      canonicalSender: ANCHOR_SENDER,
      skipProviderReadiness: true,
      resolveProspectAttributes: (id) => ({ email: `${id}@harborlaw.com`, name: id }),
      sendEmail: async (input) => {
        calls.push(input);
        return { success: true, messageId: `id-${calls.length}`, providerMessageId: `id-${calls.length}` };
      },
    });
    assert.equal(routed.executionResult.executionOutcome, 'completed');
    assert.ok(calls.length >= 1);
    assert.equal(calls[0].sender.email, ANCHOR_SENDER.senderEmail);
  });
});
