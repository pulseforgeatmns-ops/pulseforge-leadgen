'use strict';

/**
 * SPEC-JEV-005 — Mission inspection snapshot, validation, and response prose.
 * Single canonical snapshot object drives mission status answers.
 */

const {
  STAGES,
  STAGE_LABELS,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  BLOCKER_KINDS,
  BLOCKER_LABELS,
} = require('./types');
const { currentBlocker } = require('./Blockers');
const { specialistContext } = require('./Lifecycle');
const {
  hasPendingDiscoveryApproval,
  hasPendingPrioritizationApproval,
  hasPendingPlanApproval,
} = require('./PendingOperatorDecision');
const { isDailyWrapperMission } = require('./resolveInspectionMission');

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function normalizeSpecialist(value) {
  const text = asText(value).toLowerCase();
  if (!text) return null;
  if (text === 'scout') return 'Scout';
  if (text === 'max') return 'Max';
  if (text === 'paige') return 'Paige';
  if (text === 'emmett') return 'Emmett';
  if (text === 'operator') return 'operator';
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function pendingDecisionType(pending) {
  if (!pending || !pending.kind) return null;
  const map = {
    [OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL]: 'approve_discovery',
    [OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL]: 'approve_prioritization',
    [OPERATOR_DECISION_KINDS.PLAN_APPROVAL]: 'approve_plan',
    [OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION]: 'clarify_plan',
    [OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL]: 'approve_execution',
    [OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION]: 'continue_investigation',
  };
  return map[pending.kind] || pending.kind;
}

function findLatestCompletedSpecialistAction(snapshot = {}) {
  const contributions = snapshot.contributions || [];
  const workspace = snapshot.workspace || {};
  const scoutState = workspace.scout && workspace.scout.state;

  const discovery = [...contributions]
    .reverse()
    .find(
      (row) =>
        row.specialist === SPECIALISTS.SCOUT && row.kind === CONTRIBUTION_KINDS.DISCOVERY
    );
  if (discovery) {
    return {
      specialist: 'Scout',
      action: 'discovery',
      status: 'completed',
      phase: 'discovery',
      at: discovery.at || null,
    };
  }

  const prioritization = [...contributions]
    .reverse()
    .find(
      (row) =>
        row.specialist === SPECIALISTS.MAX && row.kind === CONTRIBUTION_KINDS.PRIORITIZATION
    );
  if (prioritization) {
    return {
      specialist: 'Max',
      action: 'prioritization',
      status: 'completed',
      phase: 'prioritization',
      at: prioritization.at || null,
    };
  }

  if (scoutState === 'complete') {
    return {
      specialist: 'Scout',
      action: 'readiness',
      status: 'completed',
      phase: 'readiness',
      at: null,
    };
  }

  return null;
}

function resolveWaitingState(snapshot = {}) {
  const mission = snapshot.mission || {};
  const pending = mission.pendingOperatorDecision || null;
  const blocker = snapshot.blocker || currentBlocker(mission.blockers || []);
  const ctx = specialistContext(snapshot.contributions || [], {});

  if (pending && pending.prompt) {
    if (
      pending.kind === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL ||
      hasPendingDiscoveryApproval(snapshot)
    ) {
      return {
        waiting_on: 'operator',
        waiting_reason: 'Waiting for operator approval to begin Scout discovery',
      };
    }
    if (
      pending.kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL ||
      hasPendingPrioritizationApproval(snapshot)
    ) {
      return {
        waiting_on: 'operator',
        waiting_reason: 'Waiting for operator review of Scout discovery results',
      };
    }
    if (
      pending.kind === OPERATOR_DECISION_KINDS.PLAN_APPROVAL ||
      hasPendingPlanApproval(snapshot)
    ) {
      return {
        waiting_on: 'operator',
        waiting_reason: 'Waiting for operator approval of the mission plan',
      };
    }
    return {
      waiting_on: 'operator',
      waiting_reason: pending.prompt,
    };
  }

  if (blocker) {
    const specialist = normalizeSpecialist(blocker.specialist);
    if (blocker.kind === BLOCKER_KINDS.WAITING_FOR_OPERATOR) {
      return {
        waiting_on: 'operator',
        waiting_reason: blocker.reason || blocker.label || 'Waiting for Operator',
      };
    }
    if (blocker.kind === BLOCKER_KINDS.WAITING_FOR_SCOUT) {
      return {
        waiting_on: 'Scout',
        waiting_reason: blocker.reason || blocker.label || BLOCKER_LABELS[BLOCKER_KINDS.WAITING_FOR_SCOUT],
      };
    }
    return {
      waiting_on: specialist || 'unknown',
      waiting_reason: blocker.reason || blocker.label || null,
    };
  }

  if (!ctx.scoutComplete && (mission.stage === STAGES.DISCOVER || mission.stage === STAGES.UNDERSTAND)) {
    return {
      waiting_on: 'Scout',
      waiting_reason: BLOCKER_LABELS[BLOCKER_KINDS.WAITING_FOR_SCOUT],
    };
  }

  return {
    waiting_on: null,
    waiting_reason: null,
  };
}

function resolveNextAction(snapshot = {}, waiting = {}, pendingDecision = null) {
  if (pendingDecision && pendingDecision.type === 'approve_discovery') {
    return 'Approve discovery if you want Scout to begin prospect discovery';
  }
  if (pendingDecision && pendingDecision.type === 'approve_prioritization') {
    return 'Review discovered prospects and approve prioritization to continue';
  }
  if (waiting.waiting_on === 'operator' && pendingDecision && pendingDecision.prompt) {
    return pendingDecision.prompt.replace(/\?$/, '');
  }
  if (waiting.waiting_on === 'Scout') {
    return 'Wait for Scout to complete its current pass';
  }
  return 'Continue in mission workspace';
}

function resolveConfidence(snapshot = {}) {
  const mission = snapshot.mission || {};
  const contributions = snapshot.contributions || [];
  const scoutDiscovery = [...contributions]
    .reverse()
    .find(
      (row) =>
        row.specialist === SPECIALISTS.SCOUT && row.kind === CONTRIBUTION_KINDS.DISCOVERY
    );
  const payload = scoutDiscovery && scoutDiscovery.payload ? scoutDiscovery.payload : {};
  const breakdown = payload.confidenceBreakdown;

  if (breakdown && breakdown.overall != null && Number.isFinite(Number(breakdown.overall))) {
    return { value: Number(breakdown.overall), source: 'scout_discovery' };
  }
  if (payload.confidence != null && Number.isFinite(Number(payload.confidence))) {
    return { value: Number(payload.confidence), source: 'scout_discovery' };
  }
  if (mission.confidence != null && Number.isFinite(Number(mission.confidence))) {
    return { value: Number(mission.confidence), source: 'mission' };
  }
  return { value: null, source: null };
}

/**
 * Build a canonical mission inspection snapshot from engine.inspect() output.
 * @param {object} inspectResult
 * @param {object} resolution
 * @returns {object}
 */
function buildMissionInspectionSnapshot(inspectResult = {}, resolution = {}) {
  const mission = inspectResult.mission || {};
  const health = inspectResult.health || {};
  const waiting = resolveWaitingState(inspectResult);
  const pending = mission.pendingOperatorDecision || null;
  const pendingDecision = pending
    ? {
        type: pendingDecisionType(pending),
        kind: pending.kind,
        prompt: pending.prompt || null,
        stage: pending.stage || mission.stage || null,
      }
    : null;
  const lastCompleted = findLatestCompletedSpecialistAction(inspectResult);
  const confidence = resolveConfidence(inspectResult);

  const stateWarnings = Array.isArray(resolution.warnings) ? [...resolution.warnings] : [];

  return {
    mission_id: mission.id || null,
    tenant_id: resolution.tenant_id || mission.tenantId || null,
    objective: mission.objective || null,
    resolution_type: resolution.resolution_type || null,
    requested_mission_label: resolution.requested_mission_label || null,
    canonical_mission_id: resolution.canonical_mission_id || null,
    lifecycle_status: mission.status || null,
    stage: mission.stage || null,
    stage_label: STAGE_LABELS[mission.stage] || mission.status || null,
    confidence: confidence.value,
    confidence_source: confidence.source,
    confidence_available: confidence.value != null,
    health: health.label || health.status || null,
    waiting_on: waiting.waiting_on,
    waiting_reason: waiting.waiting_reason,
    pending_decision: pendingDecision,
    last_completed_specialist_action: lastCompleted,
    next_action: resolveNextAction(inspectResult, waiting, pendingDecision),
    evidence_timestamps: {
      last_contribution_at:
        [...(inspectResult.contributions || [])].reverse()[0]?.at || null,
      last_event_at:
        [...(inspectResult.timeline || [])].reverse()[0]?.at || null,
    },
    state_warnings: stateWarnings,
    is_daily_wrapper: isDailyWrapperMission(mission),
  };
}

function waitingReasonPointsToSpecialist(waitingReason, specialist) {
  const reason = asText(waitingReason).toLowerCase();
  const name = asText(specialist).toLowerCase();
  if (!reason || !name) return false;
  return new RegExp(`waiting for ${name}|waiting on ${name}`, 'i').test(reason);
}

/**
 * Validate snapshot for internal contradictions.
 * @param {object} snapshot
 * @returns {{ ok: boolean, warnings: object[] }}
 */
function validateMissionInspectionSnapshot(snapshot = {}) {
  const warnings = [];
  const last = snapshot.last_completed_specialist_action;
  const pending = snapshot.pending_decision;
  const waitingOn = normalizeSpecialist(snapshot.waiting_on);
  const waitingReason = snapshot.waiting_reason;

  if (
    last &&
    last.specialist === 'Scout' &&
    last.action === 'discovery' &&
    last.status === 'completed' &&
    waitingOn === 'Scout' &&
    (!last.phase || !snapshot.waiting_reason_phase || last.phase === snapshot.waiting_reason_phase)
  ) {
    warnings.push({
      code: 'specialist_finished_but_waiting_on_same_specialist',
      severity: 'error',
      message:
        'Snapshot says Scout completed the last action but mission is still waiting on Scout.',
    });
  }

  if (
    waitingOn === 'operator' &&
    waitingReasonPointsToSpecialist(waitingReason, 'Scout')
  ) {
    warnings.push({
      code: 'operator_waiting_reason_points_to_specialist',
      severity: 'error',
      message:
        'Snapshot says waiting on operator but waiting reason points to Scout.',
    });
  }

  if (
    pending &&
    pending.type === 'approve_discovery' &&
    last &&
    last.specialist === 'Scout' &&
    last.action === 'discovery' &&
    last.status === 'completed'
  ) {
    warnings.push({
      code: 'discovery_approval_with_completed_scout_discovery',
      severity: 'error',
      message:
        'Pending discovery approval conflicts with a completed Scout discovery action.',
    });
  }

  if (
    snapshot.requested_mission_label &&
    /anchor str/i.test(snapshot.requested_mission_label) &&
    snapshot.is_daily_wrapper
  ) {
    warnings.push({
      code: 'named_mission_resolved_to_daily_wrapper',
      severity: 'warning',
      message:
        'Named Anchor STR request resolved to a daily/watch mission instead of the canonical record.',
    });
  }

  const hasError = warnings.some((row) => row.severity === 'error');
  return { ok: !hasError, warnings };
}

function formatConfidenceLine(snapshot = {}) {
  if (snapshot.confidence_available && snapshot.confidence != null) {
    return `Confidence: ${Number(snapshot.confidence).toFixed(2)}`;
  }
  return 'Confidence: unavailable (no grounded value on this mission record)';
}

function formatMissionInspectionProse(snapshot = {}, validation = { ok: true, warnings: [] }) {
  const label = snapshot.requested_mission_label || 'This mission';
  const errors = (validation.warnings || []).filter((row) => row.severity === 'error');
  const notes = (validation.warnings || []).filter((row) => row.severity === 'warning');

  if (errors.length) {
    const verified = [];
    if (snapshot.objective) verified.push(`Objective: ${snapshot.objective}`);
    if (snapshot.mission_id) verified.push(`Mission id: ${snapshot.mission_id}`);
    if (snapshot.pending_decision && snapshot.pending_decision.prompt) {
      verified.push(`Pending decision: ${snapshot.pending_decision.prompt}`);
    }
    if (snapshot.waiting_on) {
      verified.push(`Waiting on: ${snapshot.waiting_on}`);
    }
    if (snapshot.waiting_reason) {
      verified.push(`Waiting reason: ${snapshot.waiting_reason}`);
    }

    const inconsistent = errors.map((row) => row.message);

    return [
      `I found conflicting mission state for ${label}.`,
      '',
      'What I can verify:',
      ...verified.map((row) => `- ${row}`),
      '',
      'What is inconsistent:',
      ...inconsistent.map((row) => `- ${row}`),
      '',
      'Safest next step:',
      '- Treat the mission as blocked pending state reconciliation before executing the next stage.',
    ].join('\n');
  }

  const lines = [];
  const missionName = snapshot.requested_mission_label || snapshot.objective || snapshot.mission_id;

  if (snapshot.pending_decision && snapshot.pending_decision.type === 'approve_discovery') {
    lines.push(
      `${missionName} is waiting on operator approval to begin Scout discovery.`,
      '',
      'Status: waiting for approval',
      `Stage: ${snapshot.stage_label || snapshot.stage || 'discovery not approved yet'}`,
      formatConfidenceLine(snapshot),
      'Pending decision: approve discovery',
      `Next action: ${snapshot.next_action || 'approve discovery if you want Scout to begin prospect discovery'}`,
    );
  } else if (
    snapshot.last_completed_specialist_action &&
    snapshot.last_completed_specialist_action.specialist === 'Scout' &&
    snapshot.last_completed_specialist_action.action === 'discovery' &&
    snapshot.waiting_on === 'operator'
  ) {
    lines.push(
      'Scout completed discovery. The mission is now waiting on operator review/approval of the discovered prospects.',
      '',
      'Status: waiting on operator review',
      `Stage: ${snapshot.stage_label || snapshot.stage || 'post-discovery review'}`,
      formatConfidenceLine(snapshot),
      'Last completed action: Scout discovery pass',
      `Next action: ${snapshot.next_action || 'review/approve prospects or proceed to outreach prep'}`,
    );
  } else {
    const statusParts = [];
    if (snapshot.waiting_on === 'operator') {
      statusParts.push(`waiting on you${snapshot.pending_decision?.prompt ? ` — ${snapshot.pending_decision.prompt}` : ''}`);
    } else if (snapshot.waiting_on) {
      statusParts.push(`waiting on ${snapshot.waiting_on}`);
    } else {
      statusParts.push(`in ${snapshot.stage_label || snapshot.stage || 'active'}`);
    }
    lines.push(`${missionName} is ${statusParts.join(', ')}.`);
    lines.push('');
    if (snapshot.stage_label || snapshot.stage) {
      lines.push(`Stage: ${snapshot.stage_label || snapshot.stage}`);
    }
    lines.push(formatConfidenceLine(snapshot));
    if (snapshot.pending_decision && snapshot.pending_decision.prompt) {
      lines.push(`Pending decision: ${snapshot.pending_decision.prompt}`);
    }
    if (snapshot.last_completed_specialist_action) {
      const last = snapshot.last_completed_specialist_action;
      lines.push(`Last completed action: ${last.specialist} ${last.action || last.status}`);
    }
    if (snapshot.waiting_reason) {
      lines.push(`Blocker: ${snapshot.waiting_reason}`);
    }
    if (snapshot.next_action) {
      lines.push(`Next action: ${snapshot.next_action}`);
    }
  }

  if (
    notes.some((row) => row.code === 'named_mission_resolved_to_daily_wrapper') ||
    snapshot.resolution_type === 'daily_wrapper'
  ) {
    lines.push('');
    lines.push(
      `Note: I found this through the daily watch mission rather than the original ${label.replace(/ mission$/i, '')} mission record.`
    );
  } else if (
    snapshot.resolution_type === 'not_found' ||
    notes.some((row) => row.code === 'named_mission_not_found')
  ) {
    lines.push('');
    lines.push(
      `Note: I could not find the original ${label} record${snapshot.mission_id ? `; reporting from ${snapshot.mission_id}` : ''}.`
    );
  }

  return lines.join('\n').trim();
}

function logMissionInspectionInconsistency(snapshot = {}, validation = {}, logger = console) {
  const errors = (validation.warnings || []).filter((row) => row.severity === 'error');
  if (!errors.length) return;
  const payload = {
    event: 'MISSION_INSPECTION_STATE_INCONSISTENT',
    spec: 'SPEC-JEV-005',
    tenant_id: snapshot.tenant_id,
    requested_mission_label: snapshot.requested_mission_label,
    resolved_mission_id: snapshot.mission_id,
    resolution_type: snapshot.resolution_type,
    warnings: errors.map((row) => ({ code: row.code, severity: row.severity })),
  };
  const line = `[MISSION_INSPECTION_STATE_INCONSISTENT] ${JSON.stringify(payload)}`;
  if (logger && typeof logger.warn === 'function') {
    logger.warn(line);
  } else if (logger && typeof logger.log === 'function') {
    logger.log(line);
  }
}

module.exports = {
  buildMissionInspectionSnapshot,
  validateMissionInspectionSnapshot,
  formatMissionInspectionProse,
  logMissionInspectionInconsistency,
  pendingDecisionType,
  findLatestCompletedSpecialistAction,
  resolveWaitingState,
};
