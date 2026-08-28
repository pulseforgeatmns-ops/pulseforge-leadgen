'use strict';

/**
 * SPEC-153 / SPEC-155 — Operator intent contract helpers (leaf module, no workspace imports).
 */

/**
 * Whether an active mission may own this turn (SPEC-153 active mission contract).
 * SPEC-155 — conversation contract executionAllowed=false prohibits mission ownership.
 * SPEC-202 — unresolved pending operator decisions retain mission ownership.
 * @param {object|null|undefined} operatorIntent
 * @returns {boolean}
 */
function pendingDecisionRetainsTurn(operatorIntent) {
  const pending = operatorIntent && operatorIntent.pendingDecisionResolution;
  return Boolean(
    pending &&
      pending.pending === true &&
      pending.resolved !== true &&
      pending.outcome !== 'unrelated'
  );
}

function missionMayOwnTurn(operatorIntent) {
  if (!operatorIntent) return false;
  if (
    operatorIntent.conversationContract &&
    operatorIntent.conversationContract.executionAllowed === false
  ) {
    return false;
  }
  if (operatorIntent.conversationLocked) return false;
  if (pendingDecisionRetainsTurn(operatorIntent)) return true;
  return (
    operatorIntent.mutatesMission ||
    operatorIntent.executionRequested ||
    operatorIntent.planningRequested ||
    operatorIntent.missionContinuationRequested
  );
}

module.exports = {
  missionMayOwnTurn,
  pendingDecisionRetainsTurn,
};
