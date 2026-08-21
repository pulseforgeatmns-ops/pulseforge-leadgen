'use strict';

/**
 * SPEC-118 — explicit blockers. No silent failures.
 */

const {
  BLOCKER_KINDS,
  BLOCKER_LABELS,
  STAGES,
  asText,
  nowIso,
  newId,
  clone,
} = require('./types');
const { isStructuredMissionApproved } = require('./StructuredMission');

function createBlocker(input = {}) {
  const kind = asText(input.kind);
  return {
    id: asText(input.id) || newId('blk'),
    kind,
    label: asText(input.label) || BLOCKER_LABELS[kind] || kind,
    reason: asText(input.reason) || BLOCKER_LABELS[kind] || kind,
    specialist: asText(input.specialist) || null,
    at: nowIso(input.at || input.now),
  };
}

function inferBlockers(mission, ctx) {
  const blockers = [];
  if (ctx.deliverabilityPaused) {
    blockers.push(createBlocker({
      kind: BLOCKER_KINDS.PAUSED_DELIVERABILITY_RISK,
      specialist: 'emmett',
    }));
  }
  if (ctx.warmupRequired) {
    blockers.push(createBlocker({
      kind: BLOCKER_KINDS.WAITING_FOR_DOMAIN_WARMUP,
      specialist: 'emmett',
    }));
  }
  if (mission.stage === STAGES.DISCOVER && !isStructuredMissionApproved(mission)) {
    blockers.push(createBlocker({
      kind: BLOCKER_KINDS.WAITING_FOR_OPERATOR,
      specialist: 'operator',
      reason: 'Mission plan must be approved before discovery.',
    }));
  } else if ((mission.stage === STAGES.DISCOVER || mission.stage === STAGES.UNDERSTAND) && !ctx.scoutComplete) {
    if (ctx.prospectCount > 0 && ctx.prospectCount < (ctx.prospectThreshold || 15)) {
      blockers.push(createBlocker({ kind: BLOCKER_KINDS.WAITING_FOR_MORE_PROSPECTS, specialist: 'scout' }));
    } else {
      blockers.push(createBlocker({ kind: BLOCKER_KINDS.WAITING_FOR_SCOUT, specialist: 'scout' }));
    }
  }
  if (mission.stage === STAGES.PLAN && !ctx.maxComplete && !ctx.maxHasObjectives) {
    blockers.push(createBlocker({ kind: BLOCKER_KINDS.WAITING_FOR_MAX, specialist: 'max' }));
  }
  if (mission.stage === STAGES.PREPARE && !ctx.paigeComplete) {
    blockers.push(createBlocker({ kind: BLOCKER_KINDS.WAITING_FOR_PAIGE, specialist: 'paige' }));
  }
  if (mission.stage === STAGES.PREPARE && ctx.paigeComplete && !ctx.emmettComplete) {
    blockers.push(createBlocker({ kind: BLOCKER_KINDS.WAITING_FOR_EMMETT, specialist: 'emmett' }));
  }
  if ((mission.stage === STAGES.READY || (mission.stage === STAGES.PREPARE && ctx.paigeComplete && ctx.emmettComplete))
    && !ctx.operatorApproved) {
    blockers.push(createBlocker({ kind: BLOCKER_KINDS.WAITING_FOR_OPERATOR, specialist: 'operator' }));
  }
  return blockers;
}

function currentBlocker(blockers = []) {
  if (!blockers.length) return null;
  const paused = blockers.find((row) => row.kind === BLOCKER_KINDS.PAUSED_DELIVERABILITY_RISK);
  return clone(paused || blockers[0]);
}

module.exports = {
  createBlocker,
  inferBlockers,
  currentBlocker,
};
