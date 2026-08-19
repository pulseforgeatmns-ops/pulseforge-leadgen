'use strict';

/**
 * SPEC-116 — Operator Scorecard Intelligence engine.
 * Max recommends. Operators approve. Drafts never report.
 */

const { SCORECARD_STATUS, clone, osiError } = require('./types');
const { createMemoryOsiStore } = require('./Store');
const { generateDraftScorecard, getRuntimeScorecard } = require('./Reasoning');
const { reviewMetric, addMetric, reorderMetrics, removeMetric } = require('./Review');
const { approveScorecard, supersedeScorecard } = require('./Approval');
const { recordLearning, learningForTenant } = require('./Learning');
const { evaluateEvolution } = require('./Evolution');

function createScorecardEngine(opts = {}) {
  const store = opts.store || createMemoryOsiStore();

  function save(scorecard) {
    return store.putScorecard(scorecard);
  }

  function generateDraft(input = {}) {
    const tenantId = input.tenantId || input.tenant_id;
    const learning = input.learning || learningForTenant(store, tenantId);
    const draft = generateDraftScorecard({ ...input, learning });
    return save(draft);
  }

  function review(scorecardId, metricId, input, reviewOpts = {}) {
    const scorecard = store.requireScorecard(scorecardId);
    const result = reviewMetric(scorecard, metricId, input, reviewOpts);
    if (result.learning) {
      recordLearning(store, {
        ...result.learning,
        tenantId: scorecard.tenantId,
        clientId: scorecard.clientId,
        scorecardId: scorecard.id,
      });
      scorecard.learning = scorecard.learning || [];
      scorecard.learning.push(result.learning);
    }
    save(scorecard);
    return { scorecard: store.getScorecard(scorecardId), ...result };
  }

  function add(scorecardId, input, addOpts = {}) {
    const scorecard = store.requireScorecard(scorecardId);
    const result = addMetric(scorecard, input, addOpts);
    if (result.learning) {
      recordLearning(store, {
        ...result.learning,
        tenantId: scorecard.tenantId,
        clientId: scorecard.clientId,
        scorecardId: scorecard.id,
      });
      scorecard.learning = scorecard.learning || [];
      scorecard.learning.push(result.learning);
    }
    save(scorecard);
    return { scorecard: store.getScorecard(scorecardId), ...result };
  }

  function reorder(scorecardId, orderedIds, reorderOpts = {}) {
    const scorecard = store.requireScorecard(scorecardId);
    const result = reorderMetrics(scorecard, orderedIds, reorderOpts);
    save(scorecard);
    return { scorecard: store.getScorecard(scorecardId), ...result };
  }

  function provideRemovalReason(scorecardId, metricId, reason, opts = {}) {
    const scorecard = store.requireScorecard(scorecardId);
    const metric = (scorecard.metrics || []).find((m) => m.id === metricId || m.key === metricId);
    if (!metric) throw osiError('osi_metric_not_found', `Metric not found: ${metricId}`);
    metric.removedReason = reason;
    const learning = (scorecard.learning || []).find(
      (row) => row.metricKey === metric.key && row.action === 'remove'
    );
    if (learning) learning.reason = reason;
    recordLearning(store, {
      tenantId: scorecard.tenantId,
      clientId: scorecard.clientId,
      scorecardId: scorecard.id,
      metricKey: metric.key,
      metricName: metric.name,
      action: 'remove',
      reason,
      suppress: true,
    });
    save(scorecard);
    return { scorecard: store.getScorecard(scorecardId), metric };
  }

  function approve(scorecardId, approveOpts = {}) {
    const scorecard = store.requireScorecard(scorecardId);
    const current = store.getApproved(scorecard.tenantId);
    if (current && current.id !== scorecard.id) {
      supersedeScorecard(current);
      save(current);
    }
    const approved = approveScorecard(scorecard, approveOpts);
    return save(approved);
  }

  function runtime(tenantId) {
    return getRuntimeScorecard(store.getApproved(tenantId));
  }

  function evolve(tenantId, understanding = {}) {
    const approved = store.getApproved(tenantId);
    return evaluateEvolution(approved, understanding);
  }

  return {
    store,
    generateDraft,
    getScorecard: (id) => store.getScorecard(id),
    getDraft: (tenantId) => store.getDraft(tenantId),
    getApproved: (tenantId) => store.getApproved(tenantId),
    list: (tenantId) => store.listScorecards(tenantId),
    review,
    add,
    reorder,
    remove: (scorecardId, metricId, opts) => {
      const scorecard = store.requireScorecard(scorecardId);
      const result = removeMetric(scorecard, metricId, opts);
      if (result.learning) {
        recordLearning(store, {
          ...result.learning,
          tenantId: scorecard.tenantId,
          clientId: scorecard.clientId,
          scorecardId: scorecard.id,
        });
      }
      save(scorecard);
      return { scorecard: store.getScorecard(scorecardId), ...result };
    },
    provideRemovalReason,
    approve,
    runtime,
    evolve,
    learningFor: (tenantId) => learningForTenant(store, tenantId),
  };
}

module.exports = {
  createScorecardEngine,
  SCORECARD_STATUS,
  clone,
};
