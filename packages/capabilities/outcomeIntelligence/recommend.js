'use strict';

/**
 * Playbook / strategy recommendations (SPEC-036 / ADR-023).
 * Always start pending — never mutate strategy without approval.
 */

const {
  LEARNING_STATUS,
  RECOMMENDATION_STATUS,
  RECOMMENDATION_TARGETS,
  buildRecommendation,
} = require('./types');

function newId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Generate pending recommendations from evidence-backed learnings.
 * Candidate learnings do not produce strategy recommendations.
 *
 * @param {object[]} learnings
 * @param {object} [opts]
 * @returns {object[]}
 */
function generateRecommendations(learnings, opts = {}) {
  const list = Array.isArray(learnings) ? learnings : [];
  const backed = list.filter(
    (l) => l.status === LEARNING_STATUS.EVIDENCE_BACKED
  );
  const recs = [];

  for (const learning of backed) {
    const lift = learning.lift != null ? Number(learning.lift) : 0;
    const segment = learning.segment || 'segment';
    const dim = learning.dimension || 'vertical';

    if (dim === 'vertical' || dim === 'industry') {
      if (lift > 0) {
        recs.push(
          buildRecommendation({
            id: newId('rec'),
            summary: `Increase ${segment} targeting.`,
            action: 'increase_targeting',
            target: RECOMMENDATION_TARGETS.DISCOVERY_STRATEGY,
            learningIds: [learning.id],
            status: RECOMMENDATION_STATUS.PENDING,
            evidenceBacked: true,
          })
        );
      } else if (lift < 0) {
        recs.push(
          buildRecommendation({
            id: newId('rec'),
            summary: `Reduce ${segment} targeting.`,
            action: 'reduce_targeting',
            target: RECOMMENDATION_TARGETS.DISCOVERY_STRATEGY,
            learningIds: [learning.id],
            status: RECOMMENDATION_STATUS.PENDING,
            evidenceBacked: true,
          })
        );
      }
    } else if (dim === 'personalization' && segment === 'handwritten' && lift > 0) {
      recs.push(
        buildRecommendation({
          id: newId('rec'),
          summary: 'Increase handwritten personalization.',
          action: 'increase_handwritten',
          target: RECOMMENDATION_TARGETS.CAMPAIGN_TEMPLATES,
          learningIds: [learning.id],
          status: RECOMMENDATION_STATUS.PENDING,
          evidenceBacked: true,
        })
      );
    } else if (dim === 'personalization' && lift < 0) {
      recs.push(
        buildRecommendation({
          id: newId('rec'),
          summary: `Remove or revise ineffective ${segment} personalization.`,
          action: 'revise_personalization',
          target: RECOMMENDATION_TARGETS.CAMPAIGN_TEMPLATES,
          learningIds: [learning.id],
          status: RECOMMENDATION_STATUS.PENDING,
          evidenceBacked: true,
        })
      );
    } else if (dim === 'mail_day' && lift > 0) {
      recs.push(
        buildRecommendation({
          id: newId('rec'),
          summary: `Prefer ${segment} mailings.`,
          action: 'prefer_mail_day',
          target: RECOMMENDATION_TARGETS.CLIENT_PLAYBOOK,
          learningIds: [learning.id],
          status: RECOMMENDATION_STATUS.PENDING,
          evidenceBacked: true,
        })
      );
    } else if (dim === 'region') {
      recs.push(
        buildRecommendation({
          id: newId('rec'),
          summary:
            lift > 0
              ? `Increase targeting in ${segment}.`
              : `Reduce targeting in ${segment}.`,
          action: lift > 0 ? 'increase_region' : 'reduce_region',
          target: RECOMMENDATION_TARGETS.DISCOVERY_STRATEGY,
          learningIds: [learning.id],
          status: RECOMMENDATION_STATUS.PENDING,
          evidenceBacked: true,
        })
      );
    }

    // Always emit a ranking-weight suggestion for backed learnings
    recs.push(
      buildRecommendation({
        id: newId('rec'),
        summary:
          lift > 0
            ? `Increase ranking weight for ${segment} (${dim}).`
            : `Reduce ranking confidence for ${segment} (${dim}).`,
        action: lift > 0 ? 'increase_ranking_weight' : 'reduce_ranking_confidence',
        target: RECOMMENDATION_TARGETS.RANKING_WEIGHTS,
        learningIds: [learning.id],
        status: RECOMMENDATION_STATUS.PENDING,
        evidenceBacked: true,
      })
    );
  }

  // Deduplicate by summary
  const seen = new Set();
  const unique = [];
  for (const r of recs) {
    if (seen.has(r.summary)) continue;
    seen.add(r.summary);
    unique.push(r);
  }

  if (opts.maxRecommendations != null) {
    return unique.slice(0, Number(opts.maxRecommendations));
  }
  return unique;
}

module.exports = {
  generateRecommendations,
};
