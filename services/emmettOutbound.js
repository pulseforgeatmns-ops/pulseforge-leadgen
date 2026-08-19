'use strict';

/**
 * SPEC-117 — Emmett Outbound Infrastructure Intelligence service facade.
 */

const eoi = require('../packages/emmett-outbound');
const {
  persistPlan,
  persistAck,
  persistOutcome,
  persistLearning,
  loadTenantPlans,
  loadLearning,
} = require('./emmettOutboundPersistence');
const { buildInboxSnapshot, loadQueueProspects } = require('./emmettOutboundSnapshot');

let engine = null;

function getEngine(opts = {}) {
  if (opts.engine) return opts.engine;
  if (!engine) engine = eoi.createOutboundEngine({ store: opts.store });
  return engine;
}

function resetEngine() {
  engine = eoi.createOutboundEngine();
  return engine;
}

async function hydrateTenant(tenantId, opts = {}) {
  const instance = getEngine(opts);
  if (tenantId == null || tenantId === '' || opts.persist === false) return instance;
  try {
    const plans = await loadTenantPlans(tenantId, opts.pool);
    for (const plan of plans) instance.store.putPlan(plan);
    const learning = await loadLearning(tenantId, opts.pool);
    for (const row of learning) instance.store.addLearning(row);
  } catch (err) {
    if (!/relation .* does not exist/i.test(String(err.message))) {
      console.error('[eoi] hydrate:', err.message);
    }
  }
  return instance;
}

async function rememberPlan(plan, opts = {}) {
  if (!plan) return plan;
  getEngine(opts).store.putPlan(plan);
  if (opts.persist !== false) {
    try {
      await persistPlan(plan, opts.pool);
    } catch (err) {
      console.error('[eoi] persist plan:', err.message);
    }
  }
  return plan;
}

async function planDay(input = {}, opts = {}) {
  const instance = await hydrateTenant(input.tenantId || input.clientId, opts);
  const day = instance.planDay(input);
  await rememberPlan(day.plan, opts);
  return day;
}

async function assess(input = {}, opts = {}) {
  const instance = await hydrateTenant(input.tenantId || input.clientId, opts);
  return instance.assess(input);
}

async function approvePlan(planId, actor, approveOpts = {}, opts = {}) {
  if (opts.tenantId) await hydrateTenant(opts.tenantId, opts);
  const instance = getEngine(opts);
  const approved = instance.approvePlan(planId, actor, approveOpts);
  await rememberPlan(approved, opts);
  return approved;
}

async function acknowledgeHalt(planId, actor, note, opts = {}) {
  if (opts.tenantId) await hydrateTenant(opts.tenantId, opts);
  const instance = getEngine(opts);
  const ack = instance.acknowledge(planId, actor, note, opts.now);
  if (opts.persist !== false) {
    try {
      await persistAck(ack, opts.pool);
    } catch (err) {
      console.error('[eoi] persist ack:', err.message);
    }
  }
  return ack;
}

async function ingestOutcome(input = {}, opts = {}) {
  const instance = await hydrateTenant(input.tenantId || input.clientId, opts);
  const result = instance.ingestOutcome(input, opts.now);
  if (opts.persist !== false && result.outcome) {
    try {
      await persistOutcome(result.outcome, opts.pool);
      for (const row of result.learning) await persistLearning(row, opts.pool);
    } catch (err) {
      console.error('[eoi] persist outcome:', err.message);
    }
  }
  return result;
}

async function ingestBrevoResult(result = {}, opts = {}) {
  const clientId = result.client_id || result.clientId;
  if (!clientId) return null;
  return ingestOutcome({
    tenantId: String(clientId),
    clientId,
    prospectId: result.prospect_id || result.prospectId || null,
    eventType: result.event_type || result.eventType,
    subject: result.subject_line || result.subject || null,
    vertical: result.vertical || null,
    payload: { source: 'brevo', event_id: result.event_id || result.id },
  }, opts);
}

async function planFromClient(clientId, opts = {}) {
  const pool = opts.pool;
  if (!pool) {
    return planDay({
      tenantId: String(clientId),
      clientId,
      snapshot: opts.snapshot || {},
      prospects: opts.prospects || [],
    }, opts);
  }
  const snapshot = opts.snapshot || await buildInboxSnapshot(clientId, opts);
  const prospects = opts.prospects || await loadQueueProspects(clientId, pool);
  return planDay({
    tenantId: String(clientId),
    clientId,
    snapshot,
    prospects,
    now: opts.now,
  }, opts);
}

function canSend(input, opts = {}) {
  return getEngine(opts).canSend(input);
}

function getApprovedPlan(tenantId, localDate, opts = {}) {
  return getEngine(opts).getApprovedPlan(tenantId, localDate);
}

module.exports = {
  getEngine,
  resetEngine,
  hydrateTenant,
  planDay,
  assess,
  approvePlan,
  acknowledgeHalt,
  ingestOutcome,
  ingestBrevoResult,
  planFromClient,
  canSend,
  getApprovedPlan,
  buildInboxSnapshot,
  loadQueueProspects,
};
