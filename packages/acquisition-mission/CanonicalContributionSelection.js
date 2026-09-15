'use strict';

/**
 * Deterministic canonical contribution selection for AMO hydration.
 * Selects the latest valid committed, non-superseded contribution for a
 * specialist/kind pair — independent of DB insertion or array order.
 */

const { isSupersededContribution } = require('./ContributionSupersession');

function contributionAt(row) {
  const ts = row?.at ? new Date(row.at).getTime() : 0;
  return Number.isFinite(ts) ? ts : 0;
}

function isRolledBackContribution(row) {
  if (!row || typeof row !== 'object') return false;
  const body = row.payload && typeof row.payload === 'object' ? row.payload : {};
  const inner = body.payload && typeof body.payload === 'object' ? body.payload : {};
  return body.rolledBack === true
    || body.commitStatus === 'rolled_back'
    || inner.rolledBack === true
    || inner.commitStatus === 'rolled_back';
}

function matchesContribution(row, { missionId, specialist, kind }) {
  if (!row) return false;
  if (missionId != null && String(row.missionId) !== String(missionId)) return false;
  if (specialist != null && row.specialist !== specialist) return false;
  if (kind != null && row.kind !== kind) return false;
  return true;
}

const POINTER_KEYS = Object.freeze({
  scout: Object.freeze({ discovery: ['scoutContributionId'] }),
  max: Object.freeze({
    prioritization: ['maxContributionId'],
    acquisition_approach: ['approachContributionId'],
  }),
  paige: Object.freeze({ variants: ['paigeContributionId'] }),
  emmett: Object.freeze({ capacity: ['emmettContributionId'] }),
});

function contributionPointer(mission, specialist, kind) {
  if (!mission || typeof mission !== 'object') return null;
  const revisionState = mission.revisionState || {};
  const artifactBinding = mission.pendingOperatorDecision?.executionReview?.artifactBinding || {};
  const keys = POINTER_KEYS[specialist]?.[kind] || [];
  for (const key of keys) {
    if (revisionState[key]) return String(revisionState[key]);
    if (artifactBinding[key]) return String(artifactBinding[key]);
  }
  return null;
}

/**
 * @param {object[]} contributions
 * @param {object} criteria
 * @param {string} [criteria.missionId]
 * @param {string} criteria.specialist
 * @param {string} [criteria.kind]
 * @param {object} [criteria.mission]
 * @param {boolean} [criteria.preferPointer=true]
 * @returns {object|null}
 */
function selectCanonicalContribution(contributions = [], criteria = {}) {
  const {
    missionId,
    specialist,
    kind,
    mission,
    preferPointer = true,
  } = criteria;

  const active = contributions.filter(
    (row) =>
      matchesContribution(row, { missionId, specialist, kind })
      && !isSupersededContribution(row)
      && !isRolledBackContribution(row)
  );
  if (!active.length) return null;

  if (preferPointer && mission) {
    const pointer = contributionPointer(mission, specialist, kind);
    if (pointer) {
      const bound = active.find((row) => String(row.id) === pointer);
      if (bound) return bound;
    }
  }

  return [...active].sort((a, b) => {
    const byAt = contributionAt(b) - contributionAt(a);
    if (byAt !== 0) return byAt;
    return String(b.id || '').localeCompare(String(a.id || ''));
  })[0];
}

function listCanonicalContributions(contributions = [], criteria = {}) {
  const {
    missionId,
    specialist,
    kind,
  } = criteria;

  return contributions
    .filter(
      (row) =>
        matchesContribution(row, { missionId, specialist, kind })
        && !isSupersededContribution(row)
        && !isRolledBackContribution(row)
    )
    .sort((a, b) => {
      const byAt = contributionAt(a) - contributionAt(b);
      if (byAt !== 0) return byAt;
      return String(a.id || '').localeCompare(String(b.id || ''));
    });
}

module.exports = {
  contributionAt,
  isRolledBackContribution,
  matchesContribution,
  contributionPointer,
  selectCanonicalContribution,
  listCanonicalContributions,
};
