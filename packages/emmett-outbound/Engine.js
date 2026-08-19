'use strict';

/**
 * SPEC-117 — Emmett Outbound Infrastructure Intelligence engine.
 * Emmett protects reputation. The operator approves. Max cannot override silently.
 */

const { PLAN_STATUS, clone, nowIso, newId, eoiError, asText } = require('./types');
const { createMemoryEoiStore } = require('./Store');
const { scoreInboxHealth } = require('./InboxHealth');
const { recommendCapacity } = require('./Capacity');
const { evaluateGovernor, evaluateSend, acknowledgeHalt, actorIsOperator } = require('./Governor');
const { buildTodayQueue } = require('./Queue');
const { buildRecommendations } = require('./Recommendations');
const { recordOutcome } = require('./Outcomes');
const { routeOutcome } = require('./Learning');
const { buildDashboard } = require('./Dashboard');

function localDateOf(now, timeZone = 'America/New_York') {
  return new Intl.DateTimeFormat('en-CA', { timeZone, year: 'numeric', month: '2-digit', day: '2-digit' }).format(now);
}

function createOutboundEngine(opts = {}) {
  const store = opts.store || createMemoryEoiStore();

  function assess(input = {}) {
    const tenantId = asText(input.tenantId || input.clientId);
    if (!tenantId) throw eoiError('eoi_tenant_required', 'tenantId is required.');
    const now = input.now instanceof Date ? input.now : new Date(input.now || Date.now());
    const timeZone = input.timeZone || input.snapshot?.timeZone || 'America/New_York';
    const snapshot = {
      ...clone(input.snapshot || {}),
      tenantId,
      clientId: input.clientId || Number(tenantId) || null,
      localDate: input.localDate || localDateOf(now, timeZone),
      timeZone,
    };
    if (input.sentToday != null) snapshot.sentToday = input.sentToday;
    const health = scoreInboxHealth(snapshot);
    const capacity = recommendCapacity(snapshot, health);
    const governor = evaluateGovernor(snapshot, health, capacity);
    const queue = buildTodayQueue({
      prospects: input.prospects || [],
      recommendedCapacity: governor.halt ? 0 : (governor.slowCap || capacity.recommended),
      capacity,
      now,
    });
    const recommendations = buildRecommendations(snapshot, health, capacity, now);
    const approvedPlan = store.getApprovedPlan(tenantId, snapshot.localDate);
    store.putSnapshot(snapshot);
    return {
      snapshot,
      health,
      capacity,
      governor,
      queue,
      recommendations,
      approvedPlan,
      dashboard: buildDashboard({
        health,
        capacity,
        governor,
        queue,
        recommendations,
        approvedPlan,
        sentToday: snapshot.sentToday || 0,
      }),
    };
  }

  function planDay(input = {}) {
    const assessed = assess(input);
    const { snapshot, health, capacity, governor, queue, recommendations, approvedPlan } = assessed;
    const now = input.now instanceof Date ? input.now : new Date(input.now || Date.now());
    const draft = store.putPlan({
      id: input.planId || newId('plan'),
      kind: 'send_plan',
      spec: 'SPEC-117',
      status: PLAN_STATUS.DRAFT,
      tenantId: snapshot.tenantId,
      clientId: snapshot.clientId,
      localDate: snapshot.localDate,
      recommendedCapacity: capacity.recommended,
      approvedCapacity: null,
      allowLegacySequences: false,
      queue,
      health,
      capacity,
      governor,
      recommendations,
      createdAt: nowIso(now),
      updatedAt: nowIso(now),
    });
    return {
      ...assessed,
      plan: draft,
    };
  }

  function approvePlan(planId, actor = {}, opts = {}) {
    const plan = store.requirePlan(planId);
    if (!actorIsOperator(actor)) {
      throw eoiError(
        'eoi_operator_acknowledgement_required',
        'Max cannot approve sends. Operator approval is required.'
      );
    }
    if (plan.governor?.halt && !opts.ack) {
      throw eoiError(
        'eoi_halt_blocks_approval',
        `${plan.governor.outcome}: ${plan.governor.reason} Operator acknowledgement is required.`
      );
    }
    for (const prior of store.listPlans(plan.tenantId)) {
      if (prior.id !== plan.id && prior.localDate === plan.localDate && prior.status === PLAN_STATUS.APPROVED) {
        prior.status = PLAN_STATUS.SUPERSEDED;
        store.putPlan(prior);
      }
    }
    plan.status = PLAN_STATUS.APPROVED;
    plan.approvedAt = nowIso(opts.now);
    plan.approvedBy = actor.id || actor.name || 'operator';
    plan.approvedCapacity = opts.approvedCapacity != null ? Number(opts.approvedCapacity) : plan.recommendedCapacity;
    plan.allowLegacySequences = opts.allowLegacySequences === true;
    plan.updatedAt = nowIso(opts.now);
    return store.putPlan(plan);
  }

  function acknowledge(planId, actor, note, now) {
    const plan = store.requirePlan(planId);
    const ack = acknowledgeHalt(plan.governor, actor, note, now);
    ack.tenantId = plan.tenantId;
    ack.planId = plan.id;
    store.addAck(ack);
    return ack;
  }

  function canSend(input = {}) {
    const tenantId = asText(input.tenantId || input.clientId);
    const localDate = input.localDate || localDateOf(input.now || new Date(), input.timeZone || 'America/New_York');
    const day = input.day || null;
    const health = input.health || (day && day.health) || scoreInboxHealth(input.snapshot || {});
    const capacity = input.capacity || (day && day.capacity) || recommendCapacity(input.snapshot || {}, health);
    const governor = input.governor || (day && day.governor) || evaluateGovernor(input.snapshot || {}, health, capacity);
    const approvedPlan = input.approvedPlan || store.getApprovedPlan(tenantId, localDate);
    return evaluateSend({
      governor,
      capacity,
      approvedPlan,
      candidate: input.candidate || {},
      sentToday: input.sentToday || 0,
      localDate,
      allowLegacySequences: input.allowLegacySequences,
    });
  }

  function ingestOutcome(input = {}, now) {
    const outcome = recordOutcome(input, now);
    if (!outcome) return { outcome: null, learning: [] };
    const learning = routeOutcome(store, outcome);
    return { outcome, learning };
  }

  return {
    store,
    assess,
    planDay,
    approvePlan,
    acknowledge,
    canSend,
    ingestOutcome,
    getApprovedPlan: (tenantId, localDate) => store.getApprovedPlan(tenantId, localDate),
    dashboardFor: (day, sentToday) => buildDashboard({ ...day, sentToday }),
  };
}

module.exports = {
  createOutboundEngine,
  localDateOf,
};
