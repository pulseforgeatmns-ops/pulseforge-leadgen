'use strict';

/**
 * SPEC-153 / SPEC-155 — Operator intent contract helpers (leaf module, no workspace imports).
 */

/**
 * Whether an active mission may own this turn (SPEC-153 active mission contract).
 * SPEC-155 — conversation contract executionAllowed=false prohibits mission ownership.
 * @param {object|null|undefined} operatorIntent
 * @returns {boolean}
 */
function missionMayOwnTurn(operatorIntent) {
  if (!operatorIntent) return false;
  if (
    operatorIntent.conversationContract &&
    operatorIntent.conversationContract.executionAllowed === false
  ) {
    return false;
  }
  if (operatorIntent.conversationLocked) return false;
  return (
    operatorIntent.mutatesMission ||
    operatorIntent.executionRequested ||
    operatorIntent.planningRequested ||
    operatorIntent.missionContinuationRequested
  );
}

module.exports = {
  missionMayOwnTurn,
};
