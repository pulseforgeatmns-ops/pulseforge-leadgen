'use strict';

/**
 * P0 Max reliability — remaining gaps:
 *   1. durable authorization constraints
 *   2. verified execution outcomes (internal commit vs provider evidence)
 *   3. routing fallback integrity
 *
 * Deterministic: no network, no database, no provider. Tenants are
 * interchangeable fixtures.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const { executeMissionStage } = require('../../../acquisition-mission/TransactionalExecution');
const { runAutonomousProgression } = require('../../../acquisition-mission/MissionProgression');
const approval = require('../AmoOperatorApproval');
const { createWorkspaceEngine } = require('..');
const { applyExecutionTruth } = require('../AcquisitionMissionExecution');
const { assessFallback } = require('../RoutingFallbackIntegrity');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const {
  extractAuthorizationConstraints: extract,
  mergeAuthorizationConstraints: merge,
  routeExecutionRequest,
  createExecutionRequest,
  classifyExecutionEvidence,
  EXECUTION_EVIDENCE_STATES: STATES,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  ACTION_CLASSES,
  clearExecutionRouterAudit,
  listExecutionRouterAudit,
} = amo;

const OBJECTIVE = 'Acquire one recurring commercial cleaning client from a law firm in the Greater Manchester area.';

function missionWith(engine, constraintText, tenantId = '10') {
  return engine.create({
    tenantId,
    objective: OBJECTIVE,
    targetSegment: 'Law Firms',
    authorizationConstraints: constraintText ? extract(constraintText) : null,
  });
}

function cer(mission, intent, extra = {}) {
  return createExecutionRequest({
    source: EXECUTION_SOURCES.CHAT,
    missionId: mission.id,
    mission,
    operatorId: 'operator_test',
    intent,
    ...extra,
  });
}

describe('Gap 1 — durable authorization constraints', () => {
  beforeEach(() => clearExecutionRouterAudit());

  it('derives tenant-independent constraints from operator language', () => {
    const reviewOnly = extract('Set up a review-only contact intake. No outreach, calls, content, or campaign activation.');
    assert.equal(reviewOnly.reviewOnly, true);
    for (const cls of [ACTION_CLASSES.OUTREACH, ACTION_CLASSES.OUTREACH_AUTHORIZATION, ACTION_CLASSES.PROVIDER_RESEARCH,
      ACTION_CLASSES.PAID_MEDIA, ACTION_CLASSES.CONTENT]) {
      assert.ok(reviewOnly.prohibited.includes(cls), cls);
    }
    assert.deepEqual(extract('Find dental offices. No spending.').prohibited, ['paid_media', 'provider_research']);
    // Conditional language keeps the existing approval gate; it is not a ban.
    assert.equal(extract('Create a new mission. Do not send anything without my approval.'), null);
    assert.equal(extract('Do not create a mission. Do not resume any existing mission.'), null);
  });

  it('merging is monotonic: restrictions can only be added', () => {
    const a = extract('No outreach.');
    const b = extract('No spending.');
    const merged = merge(a, b, null);
    assert.deepEqual(merged.prohibited, ['outreach', 'outreach_authorization', 'paid_media', 'provider_research']);
    assert.deepEqual(merge(merged, null).prohibited, merged.prohibited);
    assert.equal(amo.tightens(merged, a), false);
    assert.equal(amo.tightens(a, b), true);
  });

  it('constraints are stored on the mission and survive a store snapshot/restore', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = missionWith(engine, 'Review only. No outreach.');
    assert.equal(mission.authorizationConstraints.reviewOnly, true);
    const snap = engine.store.snapshot();
    const fresh = amo.createAcquisitionMissionEngine();
    fresh.store.restore(snap);
    assert.deepEqual(fresh.get(mission.id, '10').authorizationConstraints.prohibited, mission.authorizationConstraints.prohibited);
  });

  it('router refuses a prohibited intent before any handler runs, and audits it', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = missionWith(engine, 'Review only.');
    let invoked = 0;
    const handlers = { [EXECUTION_INTENTS.EXECUTE_OUTBOUND]: () => { invoked += 1; return {}; },
      [EXECUTION_INTENTS.APPROVE_DISCOVERY]: () => { invoked += 1; return {}; } };
    for (const intent of [EXECUTION_INTENTS.EXECUTE_OUTBOUND, EXECUTION_INTENTS.APPROVE_DISCOVERY]) {
      await assert.rejects(
        routeExecutionRequest(cer(mission, intent), { engine, tenantId: '10', handlers, persist: false }),
        (err) => err.code === 'authorization_constraint_blocked'
      );
    }
    assert.equal(invoked, 0);
    const audit = listExecutionRouterAudit();
    assert.equal(audit.length, 2);
    assert.ok(audit.every((row) => row.outcome === 'blocked_by_constraint' && row.executionState === 'blocked'));
  });

  it('retrying or replaying the same request stays blocked', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = missionWith(engine, 'No outreach.');
    const request = cer(mission, EXECUTION_INTENTS.EXECUTE_OUTBOUND);
    for (let i = 0; i < 3; i += 1) {
      await assert.rejects(
        amo.replayExecutionRequest(request, { engine, tenantId: '10', handlers: {}, persist: false }),
        (err) => err.code === 'authorization_constraint_blocked'
      );
    }
  });

  it('permitted intents still run on a constrained mission', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = missionWith(engine, 'No outreach.');
    const routed = await routeExecutionRequest(cer(mission, EXECUTION_INTENTS.GENERATE_VARIANTS), {
      engine, tenantId: '10', persist: false,
      handlers: { [EXECUTION_INTENTS.GENERATE_VARIANTS]: () => ({ transactionId: 'tx_v' }) },
    });
    assert.equal(routed.execution.state, STATES.COMMITTED_INTERNAL);
  });

  it('TME blocks the stage even when the router is bypassed, and rolls back', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = missionWith(engine, 'Review only.');
    const before = engine.get(mission.id, '10');
    let executed = false;
    await assert.rejects(
      executeMissionStage({
        engine, missionId: mission.id, tenantId: '10',
        specialist: 'emmett', stage: 'execute',
        execute: async () => { executed = true; return {}; },
        commit: () => ({}),
      }),
      (err) => amo.isAuthorizationConstraintError(err) && err.commitStatus === 'rolled_back'
    );
    assert.equal(executed, false, 'specialist never ran');
    assert.equal(engine.get(mission.id, '10').version, before.version, 'mission unchanged');
  });

  it('a rolled-back constraint violation is classified blocked, not failed', () => {
    const err = new Error('forbidden');
    err.code = 'authorization_constraint_blocked';
    err.actionClass = 'outreach';
    const execution = classifyExecutionEvidence({
      request: { intent: 'EXECUTE_OUTBOUND' },
      executionResult: { rolledBack: true, error: { code: 'tme_x', cause: err } },
      specialist: 'emmett',
    });
    assert.equal(execution.state, STATES.BLOCKED);
    assert.equal(execution.reason, 'authorization_constraint');
    assert.equal(execution.evidence.actionClass, 'outreach');
  });

  it('chat approval on a review-only mission is refused explicitly and Scout never runs', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = missionWith(engine, 'Review only.');
    await approval.advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
    assert.equal(engine.get(mission.id, '10').pendingOperatorDecision.kind, 'discovery_approval');
    let scoutCalls = 0;
    const ws = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: true,
      resolverEnabled: true,
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
      scoutAcquisitionOpts: { runScout: async () => { scoutCalls += 1; return { status: 'completed', payload: {} }; } },
    });
    const opened = ws.open({ tenantId: '10', page: 'command-deck' });
    const session = ws._sessions.get(opened.sessionId);
    session.context.missionId = mission.id;
    session.context.acquisitionMissionId = mission.id;
    const result = await ws.ask({ sessionId: opened.sessionId, question: 'approved' });
    assert.equal(scoutCalls, 0);
    assert.match(result.prose, /forbids provider research/);
    assert.equal(engine.get(mission.id, '10').pendingOperatorDecision.kind, 'discovery_approval', 'approval not consumed');
  });

  it('autonomous progression cannot reach a prohibited specialist', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = missionWith(engine, 'No spending.');
    await approval.advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
    let scoutCalls = 0;
    await runAutonomousProgression({
      engine, missionId: mission.id, tenantId: '10', maxSteps: 3,
      runScout: async () => { scoutCalls += 1; return { status: 'completed', payload: {} }; },
    }).catch(() => {});
    assert.equal(scoutCalls, 0);
    assert.equal(engine.store.listContributions(mission.id).filter((c) => c.specialist === 'scout').length, 0);
  });
});

describe('Gap 2 — verified execution outcomes', () => {
  const outbound = (records) => classifyExecutionEvidence({
    request: { id: 'cer_o', intent: 'EXECUTE_OUTBOUND' },
    executionResult: { transactionId: 'tx_o', executionOutcome: 'completed', records },
    specialist: 'emmett',
  });

  it('outbound is completed_verified only with provider acceptance for every send', () => {
    const ok = outbound([
      { status: 'sent', providerMessageId: '<a@brevo>' },
      { status: 'sent', providerMessageId: '<b@brevo>' },
    ]);
    assert.equal(ok.state, STATES.COMPLETED_VERIFIED);
    assert.equal(ok.verification.levelAchieved, 'provider_accepted');
    assert.deepEqual(ok.evidence.providerMessageIds, ['<a@brevo>', '<b@brevo>']);
  });

  it('a committed outbound transaction without provider records is outcome_unknown', () => {
    assert.equal(outbound([]).state, STATES.OUTCOME_UNKNOWN);
    assert.equal(outbound([]).reason, 'no_provider_records');
    assert.equal(outbound([{ status: 'sent' }]).reason, 'provider_acceptance_unconfirmed');
    assert.equal(outbound([{ status: 'attempted' }]).reason, 'provider_outcome_pending');
  });

  it('mixed provider results are partial; all-rejected is failed; all-blocked is blocked', () => {
    const partial = outbound([{ status: 'sent', providerMessageId: 'x' }, { status: 'failed' }]);
    assert.equal(partial.state, STATES.PARTIALLY_COMPLETED);
    assert.equal(partial.verified, false);
    assert.equal(outbound([{ status: 'failed' }, { status: 'failed' }]).state, STATES.FAILED);
    assert.equal(outbound([{ status: 'blocked' }]).state, STATES.BLOCKED);
    const response = applyExecutionTruth({ prose: 'Sent', structured: { answer: 'Sent', metadata: {} } }, partial);
    assert.match(response.prose, /^Emmett partially completed: the provider accepted 1 of 2/);
  });

  it('internal work is committed_internal, distinct from provider-verified completion', () => {
    const internal = classifyExecutionEvidence({
      request: { intent: 'GENERATE_VARIANTS' },
      executionResult: { transactionId: 'tx_p' },
      specialist: 'paige',
    });
    assert.equal(internal.state, STATES.COMMITTED_INTERNAL);
    assert.equal(internal.verification.external, false);
  });

  it('Scout fixture data is synthetic_result, never completed', () => {
    const fixture = approval.fixtureScoutDiscoveryResult();
    assert.equal(fixture.provenance, 'synthetic_fixture');
    const execution = classifyExecutionEvidence({
      request: { intent: 'APPROVE_DISCOVERY' },
      executionResult: { transactionId: 'tx_s', executionOutcome: 'completed', discovery: { id: 'd1' }, scoutResult: fixture },
      specialist: 'scout',
    });
    assert.equal(execution.state, STATES.SYNTHETIC_RESULT);
    assert.equal(execution.verified, false);
    const response = applyExecutionTruth({ prose: 'Mission Updated', structured: { answer: 'Mission Updated', metadata: {} } }, execution);
    assert.match(response.prose, /^Scout returned synthetic fixture data/);
  });

  it('live discovery committed with a discovery record is completed_verified; without one it is unknown', () => {
    const base = { request: { intent: 'START_DISCOVERY' }, specialist: 'scout' };
    assert.equal(classifyExecutionEvidence({ ...base, executionResult: { transactionId: 't', discovery: { id: 'd' } } }).state,
      STATES.COMPLETED_VERIFIED);
    assert.equal(classifyExecutionEvidence({ ...base, executionResult: { transactionId: 't' } }).reason, 'no_discovery_record');
  });

  it('autonomous progression no longer opts into fixture data by default', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = missionWith(engine, null);
    await approval.advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
    const seen = [];
    await runAutonomousProgression({
      engine, missionId: mission.id, tenantId: '10', maxSteps: 1,
      deps: {
        advanceDiscoveryAfterApproval: async (input) => {
          seen.push(input.allowFixtureFallback);
          throw new Error('stop');
        },
      },
    }).catch(() => {});
    assert.deepEqual(seen, [false]);
  });
});

describe('Gap 3 — routing fallback integrity', () => {
  it('flags expected execution only for actionable owners and affirmative commands', () => {
    const cases = [
      ['specialist_scout', 'Find 20 law firms in Manchester.', true],
      ['specialist_scout', 'Should we find more law firms?', false],
      ['active_mission', 'Run Scout discovery now.', true],
      ['active_mission', 'Should we run discovery?', false],
      ['active_mission', 'Launch the campaign.', true],
      ['active_mission', 'Do not launch the campaign.', false],
      ['knowledge_retrieval', 'Run Scout discovery now.', false],
      ['mission_inspection', 'Launch the campaign.', false],
    ];
    for (const [claimedOwner, question, expected] of cases) {
      assert.equal(assessFallback({ claimedOwner, reason: 'x_unhandled', question }).executionExpected, expected,
        `${claimedOwner}: ${question}`);
    }
  });

  it('an unhandled execution command returns an explicit, traced "not executed" turn', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const ws = createWorkspaceEngine({
      disableLlm: true,
      clientIntelligenceService: { getApprovedClientBlueprint: async () => null },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    });
    const opened = await ws.open({ tenantId: '10' });
    const result = await ws.ask({ sessionId: opened.sessionId, question: 'Approve discovery.' });
    assert.match(result.prose, /^Not executed: there is no mission in this workspace/m);
    assert.equal(result.structured.metadata.executionState, 'blocked');
    assert.equal(result.structured.metadata.routingFallback.executionExpected, true);
    assert.equal(engine.list('10').length, 0);
  });

  it('informational turns keep the reasoning answer but carry the fallback trail', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const ws = createWorkspaceEngine({
      disableLlm: true,
      clientIntelligenceService: { getApprovedClientBlueprint: async () => null },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    });
    const opened = await ws.open({ tenantId: '10' });
    const result = await ws.ask({ sessionId: opened.sessionId, question: 'Should we run discovery this week?' });
    assert.doesNotMatch(result.prose || '', /Not executed:/);
  });
});
