'use strict';

/**
 * P0 Max reliability — authorization-aware planning for review-only missions.
 *
 * A mission that prohibits provider research is not asked which market or
 * region Scout should pursue and gets no customer-acquisition success metric.
 * It reaches plan approval directly, never dispatches discovery, and keeps
 * every restriction. If an admin later permits research, planning reopens in
 * the amendment commit and the research plan must be approved before
 * discovery can run. Ordinary acquisition planning is unchanged.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const { createHermeticWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const amo = require('../../../acquisition-mission');
const {
  listExecutionRouterAudit,
  clearExecutionRouterAudit,
  routeExecutionRequest,
} = require('../../../acquisition-mission/ExecutionRouter');
const { planFromObjective } = require('../../../acquisition-mission/MissionPlanner');
const {
  isReadyForLock,
  formatMissionUnderstandingProse,
  hashStructuredMission,
} = require('../../../acquisition-mission/StructuredMission');
const { RESEARCH_PLANNING_REQUIRED } = require('../../../acquisition-mission/ResearchPlanning');
const svc = require('../../../../services/acquisitionMission');

const TENANT = '25';
const REVIEW_ONLY =
  'Set up a review-only contact intake mission for an existing customer. ' +
  'No outreach, calls, content, or campaign activation.';
const ALL_PROHIBITED = ['content', 'outreach', 'outreach_authorization', 'paid_media', 'provider_research'];
const SENDER = { tenantId: TENANT, senderEmail: 'ops@example.invalid', senderName: 'Ops', sendingDomain: 'example.invalid' };

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
  return { runtime, engine, ask, mission: () => engine.list(TENANT)[0] || null };
}

function planOf(mission) {
  return mission.structuredMission || mission.missionPlanDraft || {};
}

function liftResearch(engine, mission, amendmentId = 'p0-review-only-lift-1') {
  const preview = amo.previewAuthorization({ engine, missionId: mission.id, tenantId: TENANT });
  return amo.applyAuthorizationAmendment({
    engine,
    missionId: mission.id,
    tenantId: TENANT,
    approver: { id: 'admin-1', role: 'admin', name: 'Admin' },
    amendmentId,
    expectedConstraintsHash: preview.constraintsHash,
    change: { add: [], lift: ['provider_research'], reason: 'Acceptance: allow research' },
  });
}

async function rest(h, mission, intent, spies) {
  return svc.executeCanonical({
    tenantId: TENANT, missionId: mission.id, intent, role: 'operator', operatorId: 'op',
    canonicalSender: SENDER, runScout: spies.scout, sendEmail: spies.send,
  }, { acquisitionMissionRuntime: h.runtime, persist: false });
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
  spies.scout = async () => { spies.scoutCalls += 1; return {}; };
  spies.send = async () => { spies.sendCalls += 1; return {}; };
});
afterEach(() => { globalThis.fetch = realFetch; });

describe('P0 review-only planning — planner', () => {
  const reviewOnly = amo.extractAuthorizationConstraints(REVIEW_ONLY);

  it('defers Scout targeting and uses an operator-review success metric', () => {
    const planned = planFromObjective('Set up a review-only contact intake mission for an existing customer.', {
      authorizationConstraints: reviewOnly,
    });
    const fields = (planned.ambiguities || []).map((row) => row.field);
    assert.equal(fields.includes('market.segment'), false);
    assert.equal(fields.includes('geography.region'), false);
    assert.equal(planned.draft.successMetric.type, 'operator_review');
    assert.equal(planned.draft.researchPlanning.deferred, true);
    assert.deepEqual(planned.draft.researchPlanning.missing.sort(), ['geography.region', 'market.segment']);
    assert.deepEqual(
      planned.draft.researchPlanning.deferredAmbiguities.map((row) => row.field).sort(),
      ['geography.region', 'market.segment']
    );
    assert.equal(isReadyForLock(planned.draft), true, 'review-only plan can be approved without market/region');
    // Without constraints only the plan's history is stated (no authorization claim).
    const prose = formatMissionUnderstandingProse(planned.draft);
    assert.match(prose, /Scope\nPlanned as review-only\.\nMarket and region were not collected\./);
    assert.doesNotMatch(prose, /not authorized/i);
    // With constraints: discovery called out, other prohibitions listed exactly.
    const scoped = formatMissionUnderstandingProse(planned.draft, { authorizationConstraints: reviewOnly });
    assert.match(scoped, /Review only\.\nDiscovery \(provider research\) is not authorized\.\nAlso not authorized: content, outreach, outreach approval, paid media\./);
    assert.match(prose, /Operator review complete \(no acquisition target\)/);
    assert.doesNotMatch(prose, /\bcustomers\b|Market\nUnknown|Buyer\nUnknown/);
  });

  it('leaves ordinary acquisition planning unchanged', () => {
    for (const objective of [
      'Acquire one recurring commercial cleaning client from a property manager in Nashua NH.',
      'Find law firms in Manchester NH.',
      'Set up a contact intake mission for an existing customer.',
    ]) {
      const baseline = planFromObjective(objective, {});
      for (const constraints of [null, amo.extractAuthorizationConstraints('No content.'), amo.extractAuthorizationConstraints('No outreach.')]) {
        const planned = planFromObjective(objective, { authorizationConstraints: constraints });
        assert.deepEqual(planned.ambiguities, baseline.ambiguities, objective);
        assert.deepEqual(planned.draft.successMetric, baseline.draft.successMetric, objective);
        assert.equal('researchPlanning' in planned.draft, false, objective);
        assert.equal(hashStructuredMission(planned.draft), hashStructuredMission(baseline.draft), objective);
      }
    }
  });
});

describe('P0 review-only planning — creation and approval flow', () => {
  it('creates the mission at plan approval with every restriction preserved', async () => {
    const h = harness();
    const created = await h.ask(REVIEW_ONLY);
    const mission = h.mission();
    assert.equal(created.workspaceOwnership.owner, 'mission_creation');
    assert.equal(h.engine.list(TENANT).length, 1);
    assert.equal(mission.authorizationConstraints.reviewOnly, true);
    assert.deepEqual(mission.authorizationConstraints.prohibited, ALL_PROHIBITED);
    assert.equal(mission.pendingOperatorDecision.kind, 'plan_approval');
    assert.equal(mission.status, 'Plan ready for approval');
    assert.equal(planOf(mission).successMetric.type, 'operator_review');
    assert.doesNotMatch(created.prose, /Which market should Scout pursue|Which region should this mission cover/);
    assert.deepEqual(h.engine.store.listContributions(mission.id), []);
    assert.deepEqual(listExecutionRouterAudit(), [], 'nothing dispatched at creation');
  });

  it('plan approval locks a review-only plan and never offers or runs discovery', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    const approved = await h.ask('Approve the plan.');
    const mission = h.mission();
    assert.equal(mission.structuredMission.immutable, true);
    assert.equal(mission.structuredMission.researchPlanning.deferred, true);
    assert.equal(mission.pendingOperatorDecision, null, 'durable resting state: nothing actionable advertised');
    assert.equal(mission.status, 'Plan approved (review only)');
    assert.doesNotMatch(approved.prose, /Approve discovery to begin Scout/);
    assert.match(approved.prose, /review-only/i);
    assert.deepEqual(listExecutionRouterAudit().map((row) => row.intent), ['APPROVE_PLAN']);
    assert.equal(h.engine.store.listContributions(mission.id).some((row) => row.specialist === 'scout'), false);

    clearExecutionRouterAudit();
    const discovery = await h.ask('Approve discovery.');
    assert.equal(discovery.structured.metadata.blocker, amo.AUTHORIZATION_CONSTRAINT_BLOCKED);
    assert.match(discovery.prose, /forbids provider research \(review-only mission\)/);
    const send = await h.ask('Send the emails.');
    assert.equal(send.structured.metadata.blocker, amo.AUTHORIZATION_CONSTRAINT_BLOCKED);
    assert.match(send.prose, /forbids outreach \(review-only mission\)/);
    assert.ok(listExecutionRouterAudit().every((row) => row.outcome === 'blocked_by_constraint'));
    assert.equal(fetchCalls, 0);
  });

  it('REST enforces every prohibited class on the locked review-only plan', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    await h.ask('Approve the plan.');
    const mission = h.mission();
    for (const intent of ['APPROVE_DISCOVERY', 'START_DISCOVERY', 'GENERATE_VARIANTS', 'ASSESS_PAID_ACQUISITION',
      'APPROVE_EXECUTION', 'EXECUTE_OUTBOUND']) {
      await assert.rejects(rest(h, mission, intent, spies), (err) => err.code === amo.AUTHORIZATION_CONSTRAINT_BLOCKED, intent);
    }
    assert.equal(spies.scoutCalls, 0);
    assert.equal(spies.sendCalls, 0);
    assert.equal(fetchCalls, 0);
  });
});

describe('P0 review-only planning — authorization amendment', () => {
  it('permitting research reopens planning; discovery waits for an approved research plan', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    await h.ask('Approve the plan.');
    const mission = h.mission();

    const amended = await liftResearch(h.engine, mission);
    assert.equal(amended.loosens, true);
    const reopened = h.mission();
    assert.equal(reopened.structuredMission, null, 'the review-only lock no longer stands');
    assert.equal(reopened.pendingOperatorDecision.kind, 'plan_clarification');
    assert.equal(reopened.pendingOperatorDecision.field, 'geography.region');
    assert.equal(reopened.status, 'Clarification required');
    assert.equal(planOf(reopened).successMetric.type, 'customers');
    assert.equal('researchPlanning' in planOf(reopened), false);
    assert.deepEqual(
      reopened.authorizationConstraints.prohibited,
      ['content', 'outreach', 'outreach_authorization', 'paid_media'],
      'only provider research was lifted'
    );

    // Discovery before the research plan is approved never reaches Scout.
    for (const intent of ['APPROVE_DISCOVERY', 'START_DISCOVERY']) {
      const routed = await rest(h, reopened, intent, spies).catch((err) => ({ error: err }));
      const failed = routed.error || (routed.execution && routed.execution.state !== 'completed_verified');
      assert.ok(failed, `${intent} must not complete before plan approval`);
    }
    assert.equal(spies.scoutCalls, 0);

    // Outreach stays prohibited throughout.
    await assert.rejects(rest(h, reopened, 'EXECUTE_OUTBOUND', spies), (err) => err.code === amo.AUTHORIZATION_CONSTRAINT_BLOCKED);

    await h.ask('Nashua NH.');
    assert.equal(h.mission().pendingOperatorDecision.field, 'market.segment');
    await h.ask('Property managers');
    assert.equal(h.mission().pendingOperatorDecision.kind, 'plan_approval');
    await h.ask('Approve the plan.');
    const relocked = h.mission();
    assert.equal(relocked.structuredMission.immutable, true);
    assert.equal(relocked.structuredMission.geography.region, 'Nashua NH');
    assert.equal(relocked.structuredMission.market.segment, 'property_management');
    assert.equal(relocked.pendingOperatorDecision.kind, 'discovery_approval');
    assert.equal(relocked.status, 'Awaiting discovery approval');
    assert.equal(spies.scoutCalls, 0);
    assert.equal(fetchCalls, 0);
  });

  it('an amendment that keeps research prohibited does not reopen planning', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    await h.ask('Approve the plan.');
    const mission = h.mission();
    const preview = amo.previewAuthorization({ engine: h.engine, missionId: mission.id, tenantId: TENANT });
    await amo.applyAuthorizationAmendment({
      engine: h.engine, missionId: mission.id, tenantId: TENANT,
      approver: { id: 'admin-1', role: 'admin', name: 'Admin' },
      amendmentId: 'p0-review-only-lift-content',
      expectedConstraintsHash: preview.constraintsHash,
      change: { add: [], lift: ['content'], reason: 'Acceptance: allow drafts' },
    });
    const after = h.mission();
    assert.equal(after.structuredMission.immutable, true);
    assert.equal(after.structuredMission.researchPlanning.deferred, true);
    assert.equal(after.pendingOperatorDecision, null);
    assert.equal(after.status, 'Plan approved (review only)');
  });

  it('the router refuses discovery on a deferred research plan even when research is permitted', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const planned = planFromObjective('Set up a review-only contact intake mission for an existing customer.', {
      authorizationConstraints: amo.extractAuthorizationConstraints('Review-only.'),
    });
    const { freezeStructuredMission } = require('../../../acquisition-mission/StructuredMission');
    const mission = engine.create({
      tenantId: TENANT,
      clientId: Number(TENANT),
      objective: 'Set up a contact intake mission for an existing customer.',
      createdBy: 'test',
      structuredMission: freezeStructuredMission(planned.draft, { approvedBy: 'test' }),
    });
    assert.equal(mission.pendingOperatorDecision, null, 'a locked deferred plan advertises no discovery approval');
    assert.equal(mission.authorizationConstraints, null, 'no prohibition: only the planning gate stands');
    const request = amo.createExecutionRequestFromApi({
      intent: amo.EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: mission.id,
      mission,
      operatorId: 'op',
      stage: mission.stage,
      objective: mission.objective,
      runtimeOwner: amo.resolveMissionRuntimeOwner(mission),
      permissions: { canExecute: true },
      pendingOperatorDecision: mission.pendingOperatorDecision,
      payload: {},
      metadata: {},
    });
    await assert.rejects(
      routeExecutionRequest(request, { engine, tenantId: TENANT, persist: false, runScout: spies.scout }),
      (err) => err.code === RESEARCH_PLANNING_REQUIRED
    );
    assert.equal(spies.scoutCalls, 0);
    assert.ok(listExecutionRouterAudit().some((row) => row.outcome === 'blocked_research_planning'));
  });
});

describe('P0 sender validation ordering (executeCanonical)', () => {
  const INCOMPLETE = { id: 25, sender_email: 'ops@example.invalid', sender_name: 'Ops', sending_domain: null };
  const MISMATCH = { id: 25, sender_email: 'ops@example.invalid', sender_name: 'Ops', sending_domain: 'other.invalid' };

  function loader(client) {
    const calls = [];
    const loadClient = async (id) => { calls.push(id); return client; };
    return { calls, loadClient };
  }

  async function unrestrictedMission() {
    const runtime = createTestAmoRuntime();
    const engine = runtime.engine();
    const mission = engine.create({
      tenantId: TENANT, clientId: Number(TENANT), createdBy: 'test',
      objective: 'Acquire one recurring commercial cleaning client from a property manager in Nashua NH.',
    });
    return { runtime, engine, mission };
  }

  function run(ctx, intent, loadClient, extra = {}) {
    return svc.executeCanonical({
      tenantId: TENANT, missionId: ctx.mission.id, intent, role: 'operator', operatorId: 'op',
      runScout: spies.scout, sendEmail: spies.send, ...extra,
    }, { acquisitionMissionRuntime: ctx.runtime, persist: false, loadClient });
  }

  it('a prohibited action is refused before the sender is read, and audited once', async () => {
    const h = harness();
    await h.ask(REVIEW_ONLY);
    const ctx = { runtime: h.runtime, mission: h.mission() };
    for (const intent of ['EXECUTE_OUTBOUND', 'APPROVE_EXECUTION', 'APPROVE_DISCOVERY']) {
      clearExecutionRouterAudit();
      const { calls, loadClient } = loader(INCOMPLETE);
      await assert.rejects(run(ctx, intent, loadClient), (err) => err.code === amo.AUTHORIZATION_CONSTRAINT_BLOCKED, intent);
      assert.deepEqual(calls, [], `${intent}: sender never read`);
      const audit = listExecutionRouterAudit();
      assert.equal(audit.length, 1, intent);
      assert.equal(audit[0].outcome, 'blocked_by_constraint');
      assert.equal(audit[0].intent, intent);
    }
    assert.equal(spies.sendCalls, 0);
    assert.equal(spies.scoutCalls, 0);
  });

  it('intents that do not send never read the sender configuration', async () => {
    const ctx = await unrestrictedMission();
    const { calls, loadClient } = loader(INCOMPLETE);
    const routed = await run(ctx, 'APPROVE_PLAN', loadClient).catch((err) => ({ error: err }));
    assert.ok(!routed.error || !String(routed.error.code || '').startsWith('canonical_sender_'),
      routed.error && routed.error.code);
    assert.deepEqual(calls, []);
  });

  it('outbound intents still fail closed on missing or mismatched sender identity', async () => {
    for (const [client, code] of [[INCOMPLETE, 'canonical_sender_incomplete'], [MISMATCH, 'canonical_sender_domain_mismatch'], [null, 'canonical_sender_client_not_found']]) {
      for (const intent of ['EXECUTE_OUTBOUND', 'APPROVE_EXECUTION']) {
        const ctx = await unrestrictedMission();
        const { calls, loadClient } = loader(client);
        await assert.rejects(run(ctx, intent, loadClient), (err) => err.code === code, `${intent} ${code}`);
        assert.equal(calls.length, 1, `${intent}: sender resolved once`);
      }
    }
    assert.equal(spies.sendCalls, 0, 'nothing sent');
  });
});
