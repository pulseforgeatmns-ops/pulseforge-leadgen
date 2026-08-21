'use strict';

/**
 * SPEC-130 — Detect Mission Planning Engine turns without pulling Scout/approval.
 * Kept as a leaf module so ActiveMissionGuard can require it without a cycle.
 */

const { OPERATOR_DECISION_KINDS } = require('../../acquisition-mission/types');

function missionFrom(missionOrSnapshot) {
  if (!missionOrSnapshot) return null;
  return missionOrSnapshot.mission || missionOrSnapshot;
}

function looksLikeInspectionQuestion(text) {
  const q = String(text || '').trim();
  if (!q) return false;
  if (/^(why|what|how|when|where|who|which)\b/i.test(q)) return true;
  if (/\?$/.test(q) && !/^(manchester|nh|uk|residential|commercial|mixed|str|short[- ]term)\b/i.test(q)) {
    return true;
  }
  return false;
}

/**
 * PLAN_CLARIFICATION / PLAN_EDIT consume answers, not inspection questions.
 * Unmatched short answers still belong to the planner so Max can re-ask.
 */
function isMissionPlanningTurn(missionOrSnapshot, question) {
  const mission = missionFrom(missionOrSnapshot);
  if (!mission || mission.planCancelled) return false;
  const pending = mission.pendingOperatorDecision;
  if (!pending) return false;
  const q = String(question || '');
  if (pending.kind === OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION) {
    if (looksLikeInspectionQuestion(q)) return false;
    return true;
  }
  if (pending.kind === OPERATOR_DECISION_KINDS.PLAN_EDIT) {
    if (looksLikeInspectionQuestion(q)) return false;
    return true;
  }
  if (pending.kind === OPERATOR_DECISION_KINDS.PLAN_APPROVAL) {
    return /\b(approv(e|al|ed)|edit|cancel|proceed)\b/i.test(q);
  }
  return false;
}

module.exports = {
  looksLikeInspectionQuestion,
  isMissionPlanningTurn,
};
