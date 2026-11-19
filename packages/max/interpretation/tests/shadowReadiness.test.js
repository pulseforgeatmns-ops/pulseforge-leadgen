'use strict';

/**
 * P0-MAX-REASONING-001 PR 2: semantic-shadow readiness.
 * Explicit interpreter model, bound pending-decision context without
 * authority, truthful degradation/truncation/model reporting, and no change to
 * production behaviour. No network, no database, no paid call.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { ShadowInterpreter, pendingDecisionCandidate } = require('../ShadowInterpreter');
const { ModelFrameProducer } = require('../producers/ModelFrameProducer');
const { resolveInterpreterModel, servedModelMatches } = require('../interpreterModel');
const { observerHealthSnapshot, resetObserverHealth } = require('../../observability/ObserverHealth');

const MODEL = 'claude-sonnet-5-5';
const ON = { MAX_INTERPRETATION_SHADOW: 'true', MAX_INTERPRETATION_MODEL: MODEL };
const PROMPT_TEXT = 'Proceed? Approve Edit Cancel — confidential plan content';

function rawFrame(over = {}) {
  return {
    intent: 'question', subjectKind: 'mission', relationToPrior: 'new_topic', referents: [],
    evidenceNeeds: [], facets: ['status'], requestedOutcome: 'status',
    requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' },
    scope: 'focused', foreignTenantReference: false, ambiguities: [], confidence: 0.9, ...over,
  };
}
const response = (frame, over = {}) => ({
  stop_reason: 'end_turn', model: MODEL, content: [{ type: 'text', text: JSON.stringify(frame) }],
  usage: { input_tokens: 900, output_tokens: 80 }, ...over,
});
function producerReturning(fn) {
  return new ModelFrameProducer({ model: MODEL, client: { messages: { create: async (params) => fn(params) } } });
}
const mission = (over = {}) => ({
  id: 'm_pd1', tenantId: '10', stage: 'plan',
  pendingOperatorDecision: { kind: 'plan_approval', stage: 'plan', prompt: PROMPT_TEXT, actions: ['Approve'] },
  ...over,
});
function contextFor(tenantId = '10') {
  return { tenantId, candidates: [], focusIds: [], pendingDecisionId: null, pendingDecisionKind: null };
}
async function observeOnce(observer, context, text = 'Go ahead with it.', result = { route: 'approval' }) {
  observer.observe({ context, text, result });
  await observer.drain();
}

describe('interpreter model: one explicit source, no implicit defaults', () => {
  it('resolves only MAX_INTERPRETATION_MODEL; MAX_WORKSPACE_MODEL is not a fallback', () => {
    assert.deepEqual(resolveInterpreterModel({}), { configured: false, model: null, reason: 'interpreter_model_not_configured' });
    assert.equal(resolveInterpreterModel({ MAX_WORKSPACE_MODEL: 'claude-opus-5-5' }).configured, false);
    assert.equal(resolveInterpreterModel({ MAX_INTERPRETATION_MODEL: 'not a model!' }).reason, 'interpreter_model_invalid');
    assert.deepEqual(resolveInterpreterModel({ MAX_INTERPRETATION_MODEL: ` ${MODEL} ` }), { configured: true, model: MODEL, source: 'MAX_INTERPRETATION_MODEL' });
  });

  it('the producer refuses to run without an explicit model', () => {
    const client = { messages: { create: async () => response(rawFrame()) } };
    assert.throws(() => new ModelFrameProducer({ client }), /explicit model/);
    assert.equal(new ModelFrameProducer({ client, model: MODEL }).model, MODEL);
  });

  it('an enabled observer with no configured model makes no call and writes no row, and says why', async () => {
    resetObserverHealth();
    const metered = require('../../../../utils/meteredAnthropic');
    const original = metered.forAgent;
    let constructed = 0;
    metered.forAgent = () => { constructed += 1; return class {}; };
    try {
      const rows = [];
      const observer = new ShadowInterpreter({ env: { MAX_INTERPRETATION_SHADOW: 'true', MAX_WORKSPACE_MODEL: 'claude-opus-5-5' }, persist: (r) => rows.push(r) });
      await observeOnce(observer, contextFor());
      assert.equal(constructed, 0);
      assert.equal(rows.length, 0);
      assert.equal(observer.counts.notConfigured, 1);
      assert.equal(observerHealthSnapshot().observers.semantic_shadow.lastFailureReason, 'interpreter_model_not_configured');
    } finally {
      metered.forAgent = original;
    }
  });

  it('a configured observer sends exactly the configured model through the metered client', async () => {
    const metered = require('../../../../utils/meteredAnthropic');
    const original = metered.forAgent;
    const sent = [];
    metered.forAgent = (agent) => class {
      constructor(_opts, telemetry) { this.messages = { create: async (params) => { sent.push({ agent, model: params.model, tenant: telemetry.tenantId }); return response(rawFrame()); } }; }
    };
    try {
      const rows = [];
      const observer = new ShadowInterpreter({ env: ON, persist: (r) => rows.push(r) });
      await observeOnce(observer, contextFor('77'));
      assert.deepEqual(sent, [{ agent: 'max-interpretation-shadow', model: MODEL, tenant: '77' }]);
      assert.equal(rows[0].model, MODEL);
      assert.equal(rows[0].servedModel, MODEL);
      assert.equal(rows[0].modelMismatch, false);
    } finally {
      metered.forAgent = original;
    }
  });
});

describe('pending-decision context: bound, sanitized, never authority', () => {
  it('reduces a pending decision to an opaque id and kind; the prompt never enters the context', () => {
    const observer = new ShadowInterpreter({ env: ON, producer: producerReturning(() => response(rawFrame())) });
    const ctx = contextFor();
    const m = mission();
    const before = JSON.stringify(m);
    observer.captureMission(ctx, m);
    assert.equal(ctx.pendingDecisionId, 'pd:m_pd1:plan_approval');
    assert.equal(ctx.pendingDecisionKind, 'plan_approval');
    assert.deepEqual(ctx.candidates.map((c) => [c.id, c.kind]), [['m_pd1', 'mission'], ['pd:m_pd1:plan_approval', 'pending_decision']]);
    assert.doesNotMatch(JSON.stringify(ctx), /confidential|Approve Edit/);
    assert.equal(JSON.stringify(m), before, 'mission untouched');
  });

  it('ignores a mission from another tenant or without a tenant, and unknown kinds stay generic', () => {
    assert.deepEqual(pendingDecisionCandidate(contextFor('10'), mission({ tenantId: '21' })), { rejected: 'pending_decision_tenant_mismatch' });
    assert.deepEqual(pendingDecisionCandidate(contextFor('10'), mission({ tenantId: undefined })), { rejected: 'pending_decision_tenant_mismatch' });
    assert.equal(pendingDecisionCandidate(contextFor('10'), mission({ pendingOperatorDecision: null })), null);
    assert.equal(pendingDecisionCandidate(contextFor('10'), mission({ pendingOperatorDecision: { kind: '<script>' } })).kind, 'unknown');
    const ctx = contextFor('10');
    new ShadowInterpreter({ env: ON }).captureMission(ctx, mission({ tenantId: '21' }));
    assert.equal(ctx.pendingDecisionId, null);
  });

  it('is a no-op when the observer is disabled', () => {
    const ctx = contextFor();
    new ShadowInterpreter({ env: {} }).captureMission(ctx, mission());
    assert.equal(ctx.pendingDecisionId, null);
    assert.equal(ctx.candidates.length, 0);
  });

  it('lets an authorization frame bind to the real decision (before: repaired to ambiguous) while grantsAuthority stays false', async () => {
    const approve = (ctx) => rawFrame({ intent: 'authorization', subjectKind: 'pending_decision', requestedOutcome: 'action',
      requestedAction: { verb: 'approve', targetCandidateId: ctx.pendingDecisionId, polarity: 'affirmative' } });
    // Without the captured decision (the previous hard-coded null).
    const unbound = [];
    const ctxA = contextFor();
    await observeOnce(new ShadowInterpreter({ env: ON, producer: producerReturning(() => response(approve(ctxA))), persist: (r) => unbound.push(r) }), ctxA);
    assert.equal(unbound[0].frame.intent, 'ambiguous');
    assert.ok(unbound[0].frame.repairs.includes('authorization_without_bound_pending_decision'));
    // With it.
    const bound = [];
    const observer = new ShadowInterpreter({ env: ON, producer: producerReturning(() => response(approve(ctxB))), persist: (r) => bound.push(r) });
    const ctxB = contextFor();
    observer.captureMission(ctxB, mission());
    await observeOnce(observer, ctxB);
    const row = bound[0];
    assert.equal(row.frame.intent, 'authorization');
    assert.equal(row.frame.requestedAction.targetCandidateId, 'pd:m_pd1:plan_approval');
    assert.equal(row.frame.authority.grantsAuthority, false);
    assert.equal(row.pendingDecisionBound, true);
    assert.equal(row.pendingDecisionKind, 'plan_approval');
    assert.doesNotMatch(JSON.stringify(row), /confidential|Go ahead with it/);
  });
});

describe('truthful reporting of model failures', () => {
  async function rowFor(respond) {
    resetObserverHealth();
    const rows = [];
    const observer = new ShadowInterpreter({ env: ON, producer: producerReturning(respond), persist: (r) => rows.push(r) });
    await observeOnce(observer, contextFor(), 'What is the status of the plan?', { route: 'inspection' });
    return { row: rows[0], health: observerHealthSnapshot().observers.semantic_shadow };
  }

  it('a degraded call is recorded as a fallback frame, never compared with the legacy route', async () => {
    const { row, health } = await rowFor(() => Promise.reject(Object.assign(new Error('overloaded'), { status: 529 })));
    assert.equal(row.frameSource, 'degraded_fallback');
    assert.equal(row.frame.producer, 'model_degraded_to_lexical');
    assert.equal(row.failure, 'http_529');
    assert.equal(row.disagreement, null);
    assert.equal(row.truncated, false);
    assert.equal(health.failed, 1);
    assert.equal(health.completed, 0);
    assert.equal(health.lastFailureReason, 'model_http_529');
  });

  it('a truncated response is flagged as truncated and degraded', async () => {
    const { row } = await rowFor(() => response(rawFrame(), { stop_reason: 'max_tokens' }));
    assert.equal(row.truncated, true);
    assert.equal(row.failure, 'stop_reason_max_tokens');
    assert.equal(row.stopReason, 'max_tokens');
    assert.equal(row.frameSource, 'degraded_fallback');
    assert.equal(row.disagreement, null);
  });

  it('records the served model and flags a substitution; a dated snapshot is the same model', async () => {
    const swapped = (await rowFor(() => response(rawFrame(), { model: 'claude-sonnet-5' }))).row;
    assert.equal(swapped.servedModel, 'claude-sonnet-5');
    assert.equal(swapped.modelMismatch, true);
    const dated = (await rowFor(() => response(rawFrame(), { model: `${MODEL}-20261001` }))).row;
    assert.equal(dated.modelMismatch, false);
    assert.equal(servedModelMatches(MODEL, 'claude-sonnet-5-55'), false);
  });

  it('a successful model frame is compared and priced from the served model', async () => {
    const { row, health } = await rowFor(() => response(rawFrame()));
    assert.equal(row.frameSource, 'model');
    assert.equal(row.failure, null);
    assert.equal(row.disagreement, false);
    assert.ok(row.estimatedUsd > 0);
    assert.equal(health.completed, 1);
  });
});

describe('real WorkspaceEngine at an approval gate', () => {
  const amo = require('../../../acquisition-mission');
  const A = require('../../workspace/AmoOperatorApproval');
  const { createHermeticWorkspaceEngine } = require('../../workspace/tests/workspaceTestHarness');
  const { createTestAmoRuntime } = require('../../workspace/tests/amoTestRuntime');
  const TENANT = '10';

  async function gate(kind, interpretationShadow) {
    const engine = amo.createAcquisitionMissionEngine();
    const m = engine.create({ tenantId: TENANT, objective: 'Acquire recurring commercial cleaning customers for Anchor Cleaning in Greater Manchester.', targetSegment: 'Facility buyers' });
    if (kind === 'discovery_approval') await A.advancePlanAfterApproval({ engine, mission: m, tenantId: TENANT, question: 'Approved.' });
    const get = () => engine.get(m.id, TENANT);
    assert.equal(get().pendingOperatorDecision.kind, kind);
    const ws = createHermeticWorkspaceEngine({ disableLlm: true, missionsEnabled: true, resolverEnabled: true, acquisitionMissionRuntime: createTestAmoRuntime({ engine }), interpretationShadow });
    const opened = await ws.open({ tenantId: TENANT, page: 'command-deck' });
    const session = ws._sessions.get(opened.sessionId);
    session.context.missionId = m.id;
    session.context.acquisitionMissionId = m.id;
    return { mission: m, get, ask: (question) => ws.ask({ sessionId: opened.sessionId, question }),
      fingerprint: () => JSON.stringify({ m: get(), e: engine.store.listEvents(m.id), c: engine.store.listContributions(m.id) }) };
  }

  for (const kind of ['plan_approval', 'discovery_approval']) {
    it(`${kind}: the observer sees the bound decision, nominates approval, and nothing changes in production`, async () => {
      const contexts = [];
      const rows = [];
      const nominating = {
        model: MODEL,
        async produce({ context }) {
          contexts.push(JSON.parse(JSON.stringify(context)));
          return producerReturning(() => response(rawFrame({ intent: 'authorization', subjectKind: 'pending_decision', requestedOutcome: 'action',
            requestedAction: { verb: 'approve', targetCandidateId: context.pendingDecisionId, polarity: 'affirmative' } })))
            .produce({ text: 'x', context });
        },
      };
      const observer = new ShadowInterpreter({ env: ON, producer: nominating, persist: (r) => rows.push(r) });
      const question = 'What would this step cover, in plain terms?';

      const withObserver = await gate(kind, observer);
      const before = withObserver.fingerprint();
      amo.clearExecutionRouterAudit();
      const answer = await withObserver.ask(question);
      await observer.drain();

      const baseline = await gate(kind, new ShadowInterpreter({ env: {} }));
      const baselineAnswer = await baseline.ask(question);

      assert.equal(contexts.length, 1);
      assert.equal(contexts[0].pendingDecisionId, `pd:${withObserver.mission.id}:${kind}`);
      assert.doesNotMatch(JSON.stringify(contexts[0]), /Proceed\?|Approve\nEdit/);
      assert.equal(rows[0].frame.intent, 'authorization');
      assert.equal(rows[0].frame.authority.grantsAuthority, false);
      assert.deepEqual(amo.listExecutionRouterAudit(), []);
      assert.equal(withObserver.get().pendingOperatorDecision.kind, kind, 'decision still pending');
      assert.equal(withObserver.fingerprint(), before, 'mission, events and contributions unchanged by the turn');
      assert.equal(answer.interpretationFrame, undefined);
      assert.equal(answer.prose, baselineAnswer.prose, 'production answer identical with the observer off');
    });
  }

  it('flag off: the JEV shadow is passed through and the observer is never asked for mission context', async () => {
    const observer = new ShadowInterpreter({ env: {} });
    let calls = 0;
    observer.captureMission = () => { calls += 1; };
    const g = await gate('plan_approval', observer);
    await g.ask('What would this step cover, in plain terms?');
    assert.equal(calls, 0);
  });
});
