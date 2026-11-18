'use strict';

/**
 * P0 — gate-bound chat approvals (default-off: MAX_GATE_BOUND_APPROVAL=true).
 *
 * Today a chat approval ("Approved.") resolves against whatever decision the
 * mission has pending when the message is processed. If the mission moved on
 * without this conversation seeing it — another surface approved a gate, a
 * plan was revised, autonomous progression or a background stage advanced it,
 * or the server restarted — the approval silently applies to a decision the
 * operator never saw. The execution gate already binds to the reviewed
 * prepared-package revision (tme_execution_approval_*); this extends the same
 * rule to every approval gate, at the chat surface.
 *
 * Contract when enabled:
 *   - after every mission turn, the session records the identity of the
 *     pending decision the turn presented (a digest of the decision package:
 *     kind, stage, prompt, plan understanding, prepared revision ...);
 *   - a chat approval is submitted only when the mission's CURRENT pending
 *     decision has the identity this conversation last presented. Otherwise
 *     nothing is dispatched: Max says "Not executed", shows the current
 *     decision, and records it as presented, so the next explicit approval
 *     answers what the operator has now seen;
 *   - it only narrows. The router, TME and ExecutionRequest contract are
 *     unchanged; cancellation, revision and clarification answers are not
 *     bound (they are not approvals); approvals the operator names in the same
 *     message ("Approved, begin discovery") and the autonomous policy keep
 *     their existing, explicit rules.
 *
 * Known limit: a duplicated "Approved." (double send) still approves the next
 * gate, because the first approval's answer presents that gate. Defending
 * against duplicates needs a client message id / decision key echoed by the
 * UI, not presentation state.
 */

const crypto = require('node:crypto');
const { EXECUTION_INTENTS } = require('../../acquisition-mission/ExecutionRequest');

const FLAG = 'MAX_GATE_BOUND_APPROVAL';

const GATE_APPROVAL_INTENTS = new Set([
  EXECUTION_INTENTS.APPROVE_PLAN,
  EXECUTION_INTENTS.APPROVE_DISCOVERY,
  EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
  EXECUTION_INTENTS.APPROVE_PRIORITIZATION,
  EXECUTION_INTENTS.APPROVE_EXECUTION,
  EXECUTION_INTENTS.OPERATOR_APPROVED,
]);

const VOLATILE_KEY = /(?:At|Timestamp|evaluated|expires|computed|refreshed)$/i;

function isGateBoundApprovalEnabled(env = process.env) {
  return String((env && env[FLAG]) || '').toLowerCase() === 'true';
}

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      if (VOLATILE_KEY.test(key)) continue;
      out[key] = stable(value[key]);
    }
    return out;
  }
  return value;
}

/** Identity of the decision package a mission currently has pending, or null. */
function pendingDecisionKey(mission) {
  const pending = mission && mission.pendingOperatorDecision;
  if (!mission || !mission.id || !pending || !pending.kind) return null;
  const digest = crypto.createHash('sha256')
    .update(JSON.stringify({ missionId: mission.id, decision: stable(pending) }))
    .digest('hex')
    .slice(0, 32);
  return `${pending.kind}:${digest}`;
}

/**
 * Record which pending decision (if any) a turn presented for the session's
 * bound mission. Only mission turns count; a mission turn with nothing pending
 * clears the record. Other turns leave it unchanged.
 */
function recordPresentedDecision(session, result) {
  if (!session || !session.context || typeof session.context !== 'object') return;
  const boundMissionId = session.context.acquisitionMissionId || session.context.missionId || null;
  const metadata = (result && result.structured && result.structured.metadata) || {};
  const missionTurn = metadata.acquisitionMission === true || metadata.pendingDecisionTurnOwnership === true;
  if (!missionTurn) return;
  const mission = result && result.mission;
  const key = mission && boundMissionId && mission.id === boundMissionId ? pendingDecisionKey(mission) : null;
  session.context.presentedDecision = key
    ? { missionId: mission.id, kind: mission.pendingOperatorDecision.kind, key }
    : null;
}

/**
 * @returns {{ bound: true } | { bound: false, reason: 'not_presented'|'changed_since_presented', presentedKind: string|null }}
 */
function checkGateBinding({ session, mission, intent, env = process.env }) {
  if (!isGateBoundApprovalEnabled(env) || !GATE_APPROVAL_INTENTS.has(intent)) return { bound: true };
  const current = pendingDecisionKey(mission);
  const presented = session && session.context && session.context.presentedDecision;
  if (!presented || presented.missionId !== (mission && mission.id)) {
    return { bound: false, reason: 'not_presented', presentedKind: null };
  }
  if (!current || presented.key !== current) {
    return { bound: false, reason: 'changed_since_presented', presentedKind: presented.kind || null };
  }
  return { bound: true };
}

module.exports = {
  FLAG,
  GATE_APPROVAL_INTENTS,
  isGateBoundApprovalEnabled,
  pendingDecisionKey,
  recordPresentedDecision,
  checkGateBinding,
};
