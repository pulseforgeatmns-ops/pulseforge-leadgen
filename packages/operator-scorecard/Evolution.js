'use strict';

/**
 * SPEC-116 — scorecard evolution.
 * Max may recommend updates as the business matures.
 * Nothing changes automatically.
 */

const { BUSINESS_STAGES, asText, nowIso, newId } = require('./types');
const { getCatalogEntry } = require('./Catalog');

const STAGE_RANK = Object.freeze({
  [BUSINESS_STAGES.MARKET_VALIDATION]: 0,
  [BUSINESS_STAGES.REPEATABLE_ACQUISITION]: 1,
  [BUSINESS_STAGES.OPERATIONAL_SCALE]: 2,
  [BUSINESS_STAGES.MATURE_GROWTH]: 3,
});

function stageLabel(stage) {
  return asText(stage).replace(/_/g, ' ');
}

function evaluateEvolution(approved, understanding = {}) {
  if (!approved || approved.status !== 'approved') {
    return {
      needed: false,
      autoApplied: false,
      recommendations: [],
      message: 'No approved scorecard to evolve.',
    };
  }
  const from = approved.businessStage;
  const to = understanding.stage || understanding.businessStage;
  const recommendations = [];

  if (from && to && STAGE_RANK[to] > STAGE_RANK[from]) {
    const hasPain = (approved.metrics || []).some(
      (m) => m.key === 'pain_confirmation_rate' && m.status !== 'removed'
    );
    if (hasPain && (to === BUSINESS_STAGES.OPERATIONAL_SCALE || to === BUSINESS_STAGES.MATURE_GROWTH)) {
      const next = getCatalogEntry('student_completion_rate') || getCatalogEntry('client_retention');
      recommendations.push({
        id: newId('evo'),
        action: 'replace',
        removeKey: 'pain_confirmation_rate',
        removeName: 'Pain Confirmation Rate',
        addKey: next.key,
        addName: next.name,
        reason: `Your business has transitioned from ${stageLabel(from)} to ${stageLabel(
          to
        )}. I'd recommend replacing "Pain Confirmation Rate" with "${next.name}."`,
      });
    }
  }

  return {
    needed: recommendations.length > 0,
    autoApplied: false,
    fromStage: from,
    toStage: to,
    recommendations,
    message:
      recommendations.length > 0
        ? recommendations[0].reason
        : 'The approved scorecard still reflects the current business stage.',
    evaluatedAt: nowIso(),
  };
}

module.exports = {
  evaluateEvolution,
  STAGE_RANK,
};
