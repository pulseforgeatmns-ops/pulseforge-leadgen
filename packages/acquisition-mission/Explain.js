'use strict';

/**
 * SPEC-118 — "Why is this mission here?" answered from evidence, never opinion.
 */

const { SPECIALISTS, round2 } = require('./types');

function collectEvidence(mission, contributions = [], extras = {}) {
  const reasons = [];
  const scout = [...contributions].reverse().find((row) => row.specialist === SPECIALISTS.SCOUT);
  const max = [...contributions].reverse().find((row) => row.specialist === SPECIALISTS.MAX);
  const emmett = [...contributions].reverse().find((row) => row.specialist === SPECIALISTS.EMMETT);

  const objectiveReason =
    (max && max.payload && (max.payload.objectiveReason || (max.payload.objectives && max.payload.objectives[0])))
    || extras.objectiveReason
    || null;
  if (objectiveReason) {
    reasons.push(typeof objectiveReason === 'string' ? objectiveReason : objectiveReason.text || objectiveReason.reason);
  } else if (mission.objective) {
    reasons.push(mission.objective);
  }

  const qualified = extras.qualifiedCount
    || (scout && scout.payload && (scout.payload.qualifiedCount || (scout.payload.prospects || scout.payload.companies || []).length))
    || 0;
  if (qualified) {
    reasons.push(`Scout identified ${qualified} qualified firms.`);
  }

  const capacity = extras.capacityAvailable
    || (emmett && emmett.payload && (emmett.payload.capacity && (emmett.payload.capacity.recommended || emmett.payload.capacity.available)));
  if (capacity) {
    reasons.push('Inbox capacity available.');
  }

  const previousReplyRate = extras.previousReplyRate;
  if (previousReplyRate != null) {
    reasons.push(`Previous campaign produced ${Math.round(Number(previousReplyRate) * 100)}% reply rate.`);
  }

  return reasons.filter(Boolean);
}

function explainWhy(mission, contributions = [], extras = {}) {
  const reasons = collectEvidence(mission, contributions, extras);
  return {
    spec: 'SPEC-118',
    missionId: mission.id,
    headline: 'Mission exists because',
    reasons,
    confidence: round2(mission.confidence),
    invented: false,
  };
}

function formatExplain(explain) {
  const lines = [explain.headline, '', ...explain.reasons.flatMap((reason) => [reason, '']), 'Confidence', '', String(explain.confidence)];
  return lines.join('\n').trim();
}

module.exports = {
  collectEvidence,
  explainWhy,
  formatExplain,
};
