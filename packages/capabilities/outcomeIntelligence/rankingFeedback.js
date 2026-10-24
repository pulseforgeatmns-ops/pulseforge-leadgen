'use strict';

/**
 * Ranking Feedback from campaign outcomes (SPEC-036 → SPEC-026).
 */

const {
  OUTCOME_POLARITY,
  SUCCESS_OUTCOMES,
  RESPONSE_OUTCOMES,
  buildRankingFeedback,
} = require('./types');

/**
 * Build structured ranking feedback from outcomes.
 * Successful characteristics increase future scores;
 * unsuccessful characteristics reduce future confidence.
 *
 * @param {object[]} outcomes
 * @returns {object[]}
 */
function buildRankingFeedbackFromOutcomes(outcomes) {
  const list = Array.isArray(outcomes) ? outcomes : [];
  if (!list.length) return [];

  /** @type {Map<string, { success: object[], fail: object[], all: object[] }>} */
  const byChar = new Map();

  for (const o of list) {
    const chars = characteristicsOf(o);
    for (const c of chars) {
      let bucket = byChar.get(c);
      if (!bucket) {
        bucket = { success: [], fail: [], all: [] };
        byChar.set(c, bucket);
      }
      bucket.all.push(o);
      if (SUCCESS_OUTCOMES.has(o.outcomeType) || o.successful === true) {
        bucket.success.push(o);
      } else if (
        o.polarity === OUTCOME_POLARITY.NEGATIVE ||
        (RESPONSE_OUTCOMES.has(o.outcomeType) === false &&
          o.outcomeType !== 'delivered' &&
          o.outcomeType !== 'no_response')
      ) {
        bucket.fail.push(o);
      } else if (o.polarity === OUTCOME_POLARITY.NEGATIVE) {
        bucket.fail.push(o);
      }
    }
  }

  // Also treat closed_lost / not_interested / returned as fail
  for (const o of list) {
    if (
      o.polarity === OUTCOME_POLARITY.NEGATIVE ||
      o.outcomeType === 'closed_lost' ||
      o.outcomeType === 'not_interested' ||
      o.outcomeType === 'returned_mail' ||
      o.outcomeType === 'wrong_contact' ||
      o.outcomeType === 'business_closed'
    ) {
      for (const c of characteristicsOf(o)) {
        const bucket = byChar.get(c);
        if (bucket && !bucket.fail.includes(o) && !bucket.success.includes(o)) {
          bucket.fail.push(o);
        }
      }
    }
  }

  const feedback = [];
  for (const [characteristic, bucket] of byChar) {
    const successN = bucket.success.length;
    const failN = bucket.fail.length;
    if (successN === 0 && failN === 0) continue;

    if (successN > failN) {
      feedback.push(
        buildRankingFeedback({
          characteristic,
          polarity: OUTCOME_POLARITY.POSITIVE,
          scoreDelta: Math.min(8, 2 + successN),
          confidenceDelta: Math.min(0.15, 0.03 * successN),
          sampleSize: bucket.all.length,
          outcomeIds: bucket.success.map((o) => o.id).filter(Boolean),
          successful: true,
        })
      );
    } else if (failN > 0) {
      feedback.push(
        buildRankingFeedback({
          characteristic,
          polarity: OUTCOME_POLARITY.NEGATIVE,
          scoreDelta: -Math.min(6, 1 + failN),
          confidenceDelta: -Math.min(0.2, 0.04 * failN),
          sampleSize: bucket.all.length,
          outcomeIds: bucket.fail.map((o) => o.id).filter(Boolean),
          successful: false,
        })
      );
    }
  }

  return feedback.sort(
    (a, b) => Math.abs(b.scoreDelta) - Math.abs(a.scoreDelta)
  );
}

/**
 * Convert ranking feedback into historicalOutcomes shape for SPEC-026.
 * @param {object[]} outcomes
 * @param {object[]} feedback
 * @returns {object[]}
 */
function toHistoricalOutcomes(outcomes, feedback) {
  const list = Array.isArray(outcomes) ? outcomes : [];
  const fb = Array.isArray(feedback) ? feedback : [];
  const positiveChars = new Set(
    fb.filter((f) => f.successful).map((f) => f.characteristic)
  );

  return list.map((o) => ({
    id: o.id,
    prospectId: o.prospectId,
    companyId: o.companyId,
    vertical: o.vertical || o.industry,
    industry: o.industry || o.vertical,
    region: o.region,
    outcome: o.successful || SUCCESS_OUTCOMES.has(o.outcomeType)
      ? 'successful'
      : o.polarity === OUTCOME_POLARITY.NEGATIVE
        ? 'unsuccessful'
        : 'neutral',
    successful: Boolean(o.successful || SUCCESS_OUTCOMES.has(o.outcomeType)),
    outcomeType: o.outcomeType,
    characteristics: characteristicsOf(o).filter((c) => positiveChars.has(c)),
  }));
}

/**
 * @param {object} o
 * @returns {string[]}
 */
function characteristicsOf(o) {
  const chars = [];
  if (o.vertical) chars.push(`vertical:${o.vertical}`);
  if (o.industry) chars.push(`industry:${o.industry}`);
  if (o.region) chars.push(`region:${o.region}`);
  if (o.attributes && o.attributes.handwritten) {
    chars.push('personalization:handwritten');
  }
  if (o.attributes && o.attributes.mailDay) {
    chars.push(`mail_day:${String(o.attributes.mailDay).toLowerCase()}`);
  }
  return chars;
}

module.exports = {
  buildRankingFeedbackFromOutcomes,
  toHistoricalOutcomes,
  characteristicsOf,
};
