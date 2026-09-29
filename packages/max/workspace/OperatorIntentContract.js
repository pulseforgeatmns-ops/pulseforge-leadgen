'use strict';

/**
 * SPEC-153 / SPEC-155 — Operator intent contract helpers (leaf module, no workspace imports).
 */

/**
 * SPEC-216 — Resolved pending decision with canonical mission action retains ownership.
 * Check if a resolved pending decision produced an execution intent that should
 * remain under active mission ownership (e.g., REVISE_PREPARED_OUTREACH).
 * @param {object|null|undefined} pending
 * @returns {boolean}
 */
function resolvedPendingHasCanonicalAction(pending) {
  if (!pending || !pending.resolved || !pending.resolvedFromPendingDecision) {
    return false;
  }
  // Canonical mission actions that must retain active-mission ownership
  return Boolean(pending.executionIntent);
}

/**
 * Whether an active mission may own this turn (SPEC-153 active mission contract).
 * SPEC-155 — conversation contract executionAllowed=false prohibits mission ownership.
 * SPEC-202 — unresolved pending operator decisions retain mission ownership.
 * SPEC-216 — resolved pending decisions with canonical actions also retain ownership.
 * @param {object|null|undefined} operatorIntent
 * @returns {boolean}
 */
function pendingDecisionRetainsTurn(operatorIntent) {
  const pending = operatorIntent && operatorIntent.pendingDecisionResolution;
  if (!pending || pending.outcome === 'unrelated') {
    return false;
  }
  // Unresolved pending decisions always retain turn ownership
  if (pending.pending === true && pending.resolved !== true) {
    return true;
  }
  // SPEC-216: Resolved pending decisions with canonical execution intents retain ownership
  if (resolvedPendingHasCanonicalAction(pending)) {
    return true;
  }
  return false;
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
  resolvedPendingHasCanonicalAction,
};
