'use strict';

/**
 * SPEC-116 — Operator Scorecard Intelligence service facade.
 */

const osi = require('../packages/operator-scorecard');
const {
  persistScorecard,
  persistLearning,
  loadScorecard,
  loadTenantScorecards,
  loadApprovedScorecard,
  loadLearning,
} = require('./operatorScorecardPersistence');

let engine = null;

function getEngine(opts = {}) {
  if (opts.engine) return opts.engine;
  if (!engine) engine = osi.createScorecardEngine({ store: opts.store });
  return engine;
}

function resetEngine() {
  engine = osi.createScorecardEngine();
  return engine;
}

async function hydrateTenant(tenantId, opts = {}) {
  const instance = getEngine(opts);
  if (tenantId == null || tenantId === '') return instance;
  if (opts.persist === false) return instance;
  try {
    const rows = await loadTenantScorecards(tenantId, opts.pool);
    for (const row of rows) instance.store.putScorecard(row);
    const learning = await loadLearning(tenantId, opts.pool);
    for (const row of learning) instance.store.addLearning(row);
  } catch (err) {
    if (!/relation .* does not exist/i.test(String(err.message))) {
      console.error('[osi] hydrate:', err.message);
    }
  }
  return instance;
}

async function rememberScorecard(scorecard, opts = {}) {
  if (!scorecard) return scorecard;
  getEngine(opts).store.putScorecard(scorecard);
  if (opts.persist !== false) {
    try {
      await persistScorecard(scorecard, opts.pool);
    } catch (err) {
      console.error('[osi] persist scorecard:', err.message);
    }
  }
  return scorecard;
}

async function rememberLearning(row, opts = {}) {
  if (!row) return row;
  getEngine(opts).store.addLearning(row);
  if (opts.persist !== false) {
    try {
      await persistLearning(row, opts.pool);
    } catch (err) {
      console.error('[osi] persist learning:', err.message);
    }
  }
  return row;
}

function draftInputsFromClientIntelligence(input = {}) {
  return {
    tenantId: input.tenantId || input.tenant_id || (input.clientId != null ? String(input.clientId) : null),
    clientId: input.clientId || input.client_id || null,
    businessName: input.businessName,
    businessGoal: input.businessGoal,
    objectives: input.objectives,
    objectiveRecords: input.objectiveRecords,
    blueprint: input.blueprint,
    normalizedFacts: input.normalizedFacts,
    aim: input.aim,
    outcomes: input.outcomes,
    operatorMetrics: input.operatorMetrics,
    profile: input.profile,
    businessStage: input.businessStage,
    businessModel: input.businessModel,
    revenueModel: input.revenueModel,
    learning: input.learning,
  };
}

async function generateDraft(input = {}, opts = {}) {
  const tenantId = input.tenantId || input.tenant_id || (input.clientId != null ? String(input.clientId) : null);
  const instance = await hydrateTenant(tenantId, opts);
  const draft = instance.generateDraft(draftInputsFromClientIntelligence({ ...input, tenantId }));
  await rememberScorecard(draft, opts);
  return draft;
}

async function getDraft(tenantId, opts = {}) {
  const instance = await hydrateTenant(tenantId, opts);
  return instance.getDraft(tenantId);
}

async function getOrCreateDraft(input = {}, opts = {}) {
  const tenantId = input.tenantId || input.tenant_id || (input.clientId != null ? String(input.clientId) : null);
  const existing = await getDraft(tenantId, opts);
  if (existing) return existing;
  try {
    return await generateDraft(input, opts);
  } catch (err) {
    if (err && err.code === 'osi_insufficient_understanding') return null;
    throw err;
  }
}

async function getApproved(tenantId, opts = {}) {
  const instance = await hydrateTenant(tenantId, opts);
  return instance.getApproved(tenantId) || (await loadApprovedScorecard(tenantId, opts.pool).catch(() => null));
}

async function getRuntime(tenantId, opts = {}) {
  const instance = await hydrateTenant(tenantId, opts);
  return instance.runtime(tenantId);
}

async function review(scorecardId, metricId, input, reviewOpts = {}, opts = {}) {
  const loaded = await loadScorecard(scorecardId, opts.pool).catch(() => null);
  if (loaded) getEngine(opts).store.putScorecard(loaded);
  const result = getEngine(opts).review(scorecardId, metricId, input, reviewOpts);
  await rememberScorecard(result.scorecard, opts);
  if (result.learning) await rememberLearning({
    ...result.learning,
    tenantId: result.scorecard.tenantId,
    clientId: result.scorecard.clientId,
    scorecardId: result.scorecard.id,
    id: result.learning.id || `learn-${Date.now().toString(36)}`,
  }, opts);
  return result;
}

async function addMetric(scorecardId, input, addOpts = {}, opts = {}) {
  const loaded = await loadScorecard(scorecardId, opts.pool).catch(() => null);
  if (loaded) getEngine(opts).store.putScorecard(loaded);
  const result = getEngine(opts).add(scorecardId, input, addOpts);
  await rememberScorecard(result.scorecard, opts);
  if (result.learning) await rememberLearning({
    ...result.learning,
    tenantId: result.scorecard.tenantId,
    clientId: result.scorecard.clientId,
    scorecardId: result.scorecard.id,
    id: result.learning.id || `learn-${Date.now().toString(36)}`,
  }, opts);
  return result;
}

async function reorder(scorecardId, orderedIds, reorderOpts = {}, opts = {}) {
  const loaded = await loadScorecard(scorecardId, opts.pool).catch(() => null);
  if (loaded) getEngine(opts).store.putScorecard(loaded);
  const result = getEngine(opts).reorder(scorecardId, orderedIds, reorderOpts);
  await rememberScorecard(result.scorecard, opts);
  return result;
}

async function provideRemovalReason(scorecardId, metricId, reason, reasonOpts = {}, opts = {}) {
  const loaded = await loadScorecard(scorecardId, opts.pool).catch(() => null);
  if (loaded) getEngine(opts).store.putScorecard(loaded);
  const result = getEngine(opts).provideRemovalReason(scorecardId, metricId, reason, reasonOpts);
  await rememberScorecard(result.scorecard, opts);
  return result;
}

async function approve(scorecardId, approveOpts = {}, opts = {}) {
  const loaded = await loadScorecard(scorecardId, opts.pool).catch(() => null);
  if (loaded) getEngine(opts).store.putScorecard(loaded);
  const approved = getEngine(opts).approve(scorecardId, approveOpts);
  await rememberScorecard(approved, opts);
  return approved;
}

async function evolve(tenantId, understanding = {}, opts = {}) {
  const instance = await hydrateTenant(tenantId, opts);
  return instance.evolve(tenantId, understanding);
}

function briefSectionsFor(scorecard) {
  return osi.buildBriefScorecardSections(scorecard);
}

function dailyBriefingSection(scorecard) {
  return osi.buildDailyBriefingScorecardSection(scorecard);
}

function digestCopy(runtime) {
  return osi.formatRuntimeForDigest(runtime);
}

function draftFromClientIntelligence(input = {}) {
  return osi.generateDraftScorecard(draftInputsFromClientIntelligence(input));
}

module.exports = {
  getEngine,
  resetEngine,
  generateDraft,
  getDraft,
  getOrCreateDraft,
  getApproved,
  getRuntime,
  review,
  addMetric,
  reorder,
  provideRemovalReason,
  approve,
  evolve,
  briefSectionsFor,
  dailyBriefingSection,
  digestCopy,
  draftFromClientIntelligence,
  draftInputsFromClientIntelligence,
  rememberScorecard,
  osi,
};
