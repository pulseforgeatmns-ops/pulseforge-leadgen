'use strict';

/**
 * Derive one ConversationLedger turn from a completed production turn
 * (P0-MAX-REASONING-001 PR 3). Reads only the turn's own structured result and
 * the session's own state; every record reference is tenant-checked against
 * the session tenant and dropped otherwise. No database access, no text kept.
 */

const { observedRoute } = require('../../decision-service/observedRoute');
const { classifyAuthorizationPolarity } = require('../workspace/NegatedDirective');
const { extractDiscourse, extractProhibitions } = require('./LedgerDiscourse');

const PROSPECT_REF = /^prospect:([A-Za-z0-9_.:-]+?):([A-Za-z0-9_.-]{1,120})$/;
const OUTCOMES = { affirm: 'operator_affirmed', reject: 'operator_rejected', ambiguous: 'operator_ambiguous' };
// Same action vocabulary observedRoute uses to classify an approval route.
const ACTION_KIND = { plan: 'plan_approval', discovery: 'discovery_approval', prioritization: 'prioritization_approval', execution: 'execution_approval' };

function decisionFromAction(action) {
  const a = String(action || '');
  let m = /^([a-z]+)_(approved|rejected)$/.exec(a) || /^(approve|reject)_([a-z]+)$/.exec(a);
  if (!m) return null;
  const [verb, subject] = m[2] === 'approved' || m[2] === 'rejected' ? [m[2], m[1]] : [m[1], m[2]];
  return { outcome: /^approve/.test(verb) ? 'operator_affirmed' : 'operator_rejected', decisionKind: ACTION_KIND[subject] || null };
}

function prospectSets(metadata, tenantId) {
  const items = Array.isArray(metadata.items) ? metadata.items : [];
  const members = [];
  let foreign = 0;
  for (const item of items) {
    const m = PROSPECT_REF.exec(String(item?.recordRef || ''));
    if (!m) continue;
    if (m[1] !== tenantId) { foreign += 1; continue; }
    members.push(m[2]);
  }
  return { members: [...new Set(members)], foreign };
}

function missionIds(session, tenantId) {
  const ids = [];
  const bound = session?.context?.acquisitionMissionId || session?.context?.missionId;
  if (bound) ids.push(String(bound));
  const focus = session?.operationalFocus;
  if (focus && String(focus.tenantId) === tenantId) for (const id of (focus.missionIds || []).slice(0, 4)) ids.push(String(id));
  return [...new Set(ids)];
}

function priorPendingKind(ledger, missionId) {
  const r = (ledger?.referents || []).find((x) => x.kind === 'pending_decision' && x.status === 'active' && x.missionId === missionId);
  return r ? r.decisionKind : null;
}

/**
 * @returns {object} ledger turn input for ConversationLedger.applyTurn
 */
function ledgerTurnFromResult({ session, tenantId, text, result, frame = null, ledger = null, now = Date.now() }) {
  const tenant = String(tenantId);
  const metadata = result?.structured?.metadata || {};
  const nowIso = new Date(now).toISOString();
  let route = {};
  try { route = observedRoute(result) || {}; } catch (_) { route = {}; }
  const introduced = [];
  const evidence = [];

  const sets = prospectSets(metadata, tenant);
  if (sets.members.length) {
    introduced.push({ kind: 'prospect_set', scope: 'existing', source: 'evidence', members: sets.members,
      label: `${sets.members.length} existing prospect records`, observedAt: nowIso });
    evidence.push({ kind: 'crm_pipeline', observedAt: nowIso, introducedIndex: introduced.length - 1 });
  } else if (metadata.launchedScout === true) {
    introduced.push({ kind: 'prospect_set', scope: 'new', source: 'evidence', members: [], label: 'new prospect discovery', observedAt: nowIso });
    evidence.push({ kind: 'scout_execution', observedAt: nowIso, introducedIndex: introduced.length - 1 });
  }

  const missions = missionIds(session, tenant);
  for (const id of missions) {
    introduced.push({ kind: 'mission', source: 'mission_runtime', missionId: id, label: 'acquisition mission', observedAt: nowIso });
  }
  const mission = result?.mission;
  const pending = mission?.pendingOperatorDecision;
  if (pending && mission?.id && String(mission.tenantId) === tenant) {
    introduced.push({ kind: 'pending_decision', source: 'mission_runtime', missionId: String(mission.id), decisionKind: pending.kind,
      label: `pending ${String(pending.kind || 'decision').replace(/_/g, ' ')}`, observedAt: nowIso });
    evidence.push({ kind: 'operator_decisions', observedAt: nowIso, introducedIndex: introduced.length - 1 });
  }
  const opEvidence = session?.operationalEvidence;
  if (opEvidence && String(opEvidence.tenantId) === tenant) {
    const observedAt = opEvidence.recordedAt || opEvidence.asOf;
    if (Number.isFinite(Date.parse(observedAt))) evidence.push({ kind: 'operational_inspection', observedAt });
  }

  // What production itself resolved about a pending decision, or a held reply.
  let decision = null;
  const resolution = metadata.pendingDecisionResolution || result?.operatorIntent?.pendingDecisionResolution;
  const boundMission = missions[0] || null;
  // The decision the operator answered is the one pending BEFORE this turn
  // (the ledger's), not whatever the mission advanced to afterwards.
  const priorKind = priorPendingKind(ledger, boundMission);
  const fromAction = route.route === 'approval' ? decisionFromAction(result?.resolution?.action || route.action) : null;
  if (resolution && OUTCOMES[resolution.outcome]) {
    decision = { outcome: OUTCOMES[resolution.outcome], decisionKind: resolution.decisionKind || priorKind || route.kind, missionId: boundMission };
  } else if (fromAction) {
    decision = { outcome: fromAction.outcome, decisionKind: priorKind || fromAction.decisionKind || route.kind, missionId: boundMission };
  } else if (boundMission && classifyAuthorizationPolarity(String(text || '')).negatedAuthorization) {
    decision = { outcome: 'operator_held', decisionKind: priorKind || route.kind, missionId: boundMission };
  }

  const retrieval = metadata.retrievalStatus;
  return {
    owner: route.owner || result?.workspaceOwnership?.owner || null,
    frame,
    discourse: extractDiscourse(text),
    introduced,
    prohibitions: extractProhibitions(text),
    decision,
    evidence,
    clarificationAsked: route.route === 'clarification' || metadata.clarificationRequired === true,
    unresolvedRead: retrieval === 'failed' || retrieval === 'unavailable',
    foreignTenant: metadata.operationalInspectionRefused === 'tenant_mismatch' || frame?.foreignTenantReference === true || sets.foreign > 0,
  };
}

module.exports = { ledgerTurnFromResult };
