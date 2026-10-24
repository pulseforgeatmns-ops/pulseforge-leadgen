'use strict';

/**
 * Operator actions for Outcome Intelligence (SPEC-036 / ADR-023).
 */

const {
  RECOMMENDATION_STATUS,
  LEARNING_STATUS,
  buildMissionOutcomeEvent,
  buildMissionTimelineEntry,
  buildOutcomeSummary,
} = require('./types');
const {
  validateRecommendationAction,
  validateStrategyMutation,
  validateLearningPromotion,
} = require('./validate');

/**
 * @param {unknown} raw
 * @returns {object[]}
 */
function normalizeActions(raw) {
  const inputs = raw && typeof raw === 'object' ? raw : {};
  if (Array.isArray(inputs.outcomeActions)) return inputs.outcomeActions;
  if (Array.isArray(inputs.actions)) return inputs.actions;
  if (inputs.action && typeof inputs.action === 'object') return [inputs.action];
  return [];
}

/**
 * Apply operator actions onto an assembled outcome package.
 * @param {object} package_
 * @param {object[]} actions
 * @param {object} [opts]
 * @returns {{ package: object, errors: string[], changeSummary: string }}
 */
function applyOutcomeActions(package_, actions, opts = {}) {
  const operator = opts.operator || 'operator';
  const errors = [];
  const changes = [];
  let pkg = clonePackage(package_);

  for (const action of actions || []) {
    const type = String(action.type || action.action || '').toLowerCase();
    if (!type || type === 'capture_outcomes' || type === 'generate_learnings' || type === 'generate_recommendations') {
      // Assembly already performed these; no-op
      continue;
    }

    if (type === 'approve_recommendation') {
      const rec = findRec(pkg, action.recommendationId || action.id);
      const gate = validateRecommendationAction(rec, 'approve');
      if (!gate.ok) {
        errors.push(...gate.errors);
        continue;
      }
      rec.status = RECOMMENDATION_STATUS.APPROVED;
      rec.approvedAt = new Date().toISOString();
      rec.approvedBy = operator;
      // Promote linked learnings
      for (const lid of rec.learningIds || []) {
        const learning = pkg.learnings.find((l) => l.id === lid);
        if (learning && learning.status === LEARNING_STATUS.EVIDENCE_BACKED) {
          const promo = validateLearningPromotion(learning);
          if (promo.ok) learning.status = LEARNING_STATUS.PROMOTED;
        }
      }
      pkg.missionEvents.push(
        buildMissionOutcomeEvent({
          eventType: 'recommendation_approved',
          operator,
          recommendationId: rec.id,
          summary: `Approved: ${rec.summary}`,
        })
      );
      changes.push(`approved:${rec.id}`);
      continue;
    }

    if (type === 'reject_recommendation') {
      const rec = findRec(pkg, action.recommendationId || action.id);
      const gate = validateRecommendationAction(rec, 'reject');
      if (!gate.ok) {
        errors.push(...gate.errors);
        continue;
      }
      rec.status = RECOMMENDATION_STATUS.REJECTED;
      rec.rejectedAt = new Date().toISOString();
      rec.rejectedBy = operator;
      pkg.missionEvents.push(
        buildMissionOutcomeEvent({
          eventType: 'recommendation_rejected',
          operator,
          recommendationId: rec.id,
          summary: `Rejected: ${rec.summary}`,
        })
      );
      changes.push(`rejected:${rec.id}`);
      continue;
    }

    if (type === 'apply_recommendation') {
      const rec = findRec(pkg, action.recommendationId || action.id);
      const gate = validateRecommendationAction(rec, 'apply');
      if (!gate.ok) {
        errors.push(...gate.errors);
        continue;
      }
      const mutation = validateStrategyMutation(rec);
      if (!mutation.ok) {
        errors.push(...mutation.errors);
        continue;
      }
      rec.status = RECOMMENDATION_STATUS.APPLIED;
      rec.appliedAt = new Date().toISOString();
      pkg.missionEvents.push(
        buildMissionOutcomeEvent({
          eventType: 'recommendation_applied',
          operator,
          recommendationId: rec.id,
          summary: `Applied to ${rec.target}: ${rec.summary}`,
        })
      );
      changes.push(`applied:${rec.id}:${rec.target}`);
      continue;
    }

    if (type === 'conclude_mission') {
      const pending = pkg.recommendations.filter(
        (r) => r.status === RECOMMENDATION_STATUS.PENDING
      ).length;
      pkg.outcomeSummary = buildOutcomeSummary({
        ...pkg.outcomeSummary,
        objectiveAchieved:
          action.objectiveAchieved != null
            ? Boolean(action.objectiveAchieved)
            : pkg.outcomeSummary.objectiveAchieved,
        recommendationsPending: pending,
        concludedAt: new Date().toISOString(),
      });
      pkg.timeline.push(
        buildMissionTimelineEntry({
          stage: 'outcome_intelligence',
          status: 'mission_concluded',
          summary: `Mission concluded — ${pkg.outcomeSummary.lessonsLearned.length} lesson(s), ${pending} recommendation(s) still pending`,
          operator,
        })
      );
      pkg.missionEvents.push(
        buildMissionOutcomeEvent({
          eventType: 'mission_concluded',
          operator,
          summary: 'Outcome Summary attached to Mission Memory',
        })
      );
      changes.push('mission_concluded');
      continue;
    }

    errors.push(`unknown_action:${type}`);
  }

  // Refresh summary counts
  pkg.summary = {
    ...pkg.summary,
    recommendationCount: pkg.recommendations.length,
    pendingRecommendations: pkg.recommendations.filter(
      (r) => r.status === RECOMMENDATION_STATUS.PENDING
    ).length,
    updatedAt: new Date().toISOString(),
  };

  return {
    package: pkg,
    errors,
    changeSummary: changes.join('; '),
  };
}

/**
 * @param {object} pkg
 * @param {string} id
 * @returns {object|null}
 */
function findRec(pkg, id) {
  if (!id) return null;
  return (pkg.recommendations || []).find((r) => r.id === String(id)) || null;
}

/**
 * @param {object} pkg
 * @returns {object}
 */
function clonePackage(pkg) {
  return {
    ...pkg,
    outcomes: [...(pkg.outcomes || [])],
    learnings: (pkg.learnings || []).map((l) => ({ ...l })),
    recommendations: (pkg.recommendations || []).map((r) => ({ ...r })),
    rankingFeedback: [...(pkg.rankingFeedback || [])],
    historicalOutcomes: [...(pkg.historicalOutcomes || [])],
    personalizationFeedback: pkg.personalizationFeedback
      ? {
          ...pkg.personalizationFeedback,
          dimensions: { ...pkg.personalizationFeedback.dimensions },
        }
      : null,
    analytics: pkg.analytics ? { ...pkg.analytics } : null,
    outcomeSummary: pkg.outcomeSummary ? { ...pkg.outcomeSummary } : null,
    missionEvents: [...(pkg.missionEvents || [])],
    timeline: [...(pkg.timeline || [])],
    summary: pkg.summary ? { ...pkg.summary } : {},
  };
}

module.exports = {
  normalizeActions,
  applyOutcomeActions,
};
