'use strict';

/**
 * SPEC-153 — Operator intent contract helpers (leaf module, no workspace imports).
 */

/**
 * Whether an active mission may own this turn (SPEC-153 active mission contract).
 * @param {object|null|undefined} operatorIntent
 * @returns {boolean}
 */
function missionMayOwnTurn(operatorIntent) {
  if (!operatorIntent) return false;
  if (operatorIntent.conversationLocked) return false;
  return (
    operatorIntent.mutatesMission ||
    operatorIntent.executionRequested ||
    operatorIntent.planningRequested
  );
}

module.exports = {
  missionMayOwnTurn,
};
