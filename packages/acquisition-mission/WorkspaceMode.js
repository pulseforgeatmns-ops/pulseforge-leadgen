'use strict';

/**
 * SPEC-153 / ADR-074 — Mission Workspace Modes.
 * Workspace mode describes the operator's current executable state, not merely
 * the next lifecycle stage. Modes are derived from progression execution state.
 */

const { STAGES } = require('./types');
const {
  deriveProgressionStage,
  PROGRESSION_STAGES,
  isDiscoveryRunning,
} = require('./MissionProgression');
const { hasPendingDiscoveryApproval } = require('./PendingOperatorDecision');

/** Operator-journey workspace modes (execution-state vocabulary). */
const WORKSPACE_MODES = Object.freeze({
  CREATE: 'create',
  MISSION_PLANNING: 'mission_planning',
  DISCOVERY_APPROVAL: 'discovery_approval',
  DISCOVERY_RUNNING: 'discovery_running',
  DISCOVERY_REVIEW: 'discovery_review',
  OUTREACH: 'outreach',
  EXECUTION: 'execution',
  COMPLETE: 'complete',
  /** @deprecated Use MISSION_PLANNING */
  UNDERSTANDING: 'mission_planning',
  /** @deprecated Use DISCOVERY_APPROVAL or DISCOVERY_RUNNING */
  DISCOVERY: 'discovery_approval',
});

/** Spec-level render groups for component contracts and backward compatibility. */
const RENDER_MODES = Object.freeze({
  CREATE: 'create',
  INSPECT: 'inspect',
  EXECUTION: 'execution',
  REVIEW: 'review',
});

const WORKSPACE_MODE_LABELS = Object.freeze({
  [WORKSPACE_MODES.CREATE]: 'Create',
  [WORKSPACE_MODES.MISSION_PLANNING]: 'Mission Planning',
  [WORKSPACE_MODES.DISCOVERY_APPROVAL]: 'Discovery Approval',
  [WORKSPACE_MODES.DISCOVERY_RUNNING]: 'Discovery Running',
  [WORKSPACE_MODES.DISCOVERY_REVIEW]: 'Discovery Review',
  [WORKSPACE_MODES.OUTREACH]: 'Outreach',
  [WORKSPACE_MODES.EXECUTION]: 'Execution',
  [WORKSPACE_MODES.COMPLETE]: 'Complete',
});

const INSPECT_LIKE_MODES = Object.freeze([
  WORKSPACE_MODES.MISSION_PLANNING,
  WORKSPACE_MODES.DISCOVERY_APPROVAL,
  WORKSPACE_MODES.DISCOVERY_RUNNING,
  WORKSPACE_MODES.DISCOVERY_REVIEW,
  WORKSPACE_MODES.OUTREACH,
  WORKSPACE_MODES.EXECUTION,
  WORKSPACE_MODES.COMPLETE,
]);

/** Top-level component contracts — each declares supportedModes. */
const COMPONENTS = Object.freeze({
  missionCreator: {
    id: 'missionCreator',
    supportedModes: [WORKSPACE_MODES.CREATE],
  },
  missionList: {
    id: 'missionList',
    supportedModes: INSPECT_LIKE_MODES.concat(WORKSPACE_MODES.CREATE),
  },
  missionUnderstanding: {
    id: 'missionUnderstanding',
    supportedModes: [
      WORKSPACE_MODES.MISSION_PLANNING,
      WORKSPACE_MODES.DISCOVERY_APPROVAL,
      WORKSPACE_MODES.DISCOVERY_RUNNING,
      WORKSPACE_MODES.DISCOVERY_REVIEW,
      WORKSPACE_MODES.OUTREACH,
      WORKSPACE_MODES.EXECUTION,
      WORKSPACE_MODES.COMPLETE,
    ],
  },
  missionTimeline: {
    id: 'missionTimeline',
    supportedModes: INSPECT_LIKE_MODES,
  },
  missionWorkspace: {
    id: 'missionWorkspace',
    supportedModes: INSPECT_LIKE_MODES,
  },
  missionHealth: {
    id: 'missionHealth',
    supportedModes: INSPECT_LIKE_MODES,
  },
  discovery: {
    id: 'discovery',
    supportedModes: [
      WORKSPACE_MODES.DISCOVERY_APPROVAL,
      WORKSPACE_MODES.DISCOVERY_RUNNING,
      WORKSPACE_MODES.DISCOVERY_REVIEW,
    ],
  },
  missionIntelligenceReport: {
    id: 'missionIntelligenceReport',
    supportedModes: [WORKSPACE_MODES.DISCOVERY_REVIEW],
  },
  blockers: {
    id: 'blockers',
    supportedModes: [
      WORKSPACE_MODES.MISSION_PLANNING,
      WORKSPACE_MODES.DISCOVERY_APPROVAL,
      WORKSPACE_MODES.DISCOVERY_RUNNING,
      WORKSPACE_MODES.DISCOVERY_REVIEW,
      WORKSPACE_MODES.OUTREACH,
      WORKSPACE_MODES.EXECUTION,
    ],
  },
  why: {
    id: 'why',
    supportedModes: INSPECT_LIKE_MODES,
  },
  operatorDecision: {
    id: 'operatorDecision',
    supportedModes: [
      WORKSPACE_MODES.MISSION_PLANNING,
      WORKSPACE_MODES.DISCOVERY_APPROVAL,
      WORKSPACE_MODES.DISCOVERY_REVIEW,
      WORKSPACE_MODES.EXECUTION,
    ],
  },
  executionWorkspace: {
    id: 'executionWorkspace',
    supportedModes: [WORKSPACE_MODES.EXECUTION],
  },
});

const PROGRESSION_TO_WORKSPACE = Object.freeze({
  [PROGRESSION_STAGES.MISSION_PLANNING]: WORKSPACE_MODES.MISSION_PLANNING,
  [PROGRESSION_STAGES.DISCOVERY_APPROVAL]: WORKSPACE_MODES.DISCOVERY_APPROVAL,
  [PROGRESSION_STAGES.DISCOVERY_RUNNING]: WORKSPACE_MODES.DISCOVERY_RUNNING,
  [PROGRESSION_STAGES.DISCOVERY_REVIEW]: WORKSPACE_MODES.DISCOVERY_REVIEW,
  [PROGRESSION_STAGES.OUTREACH_PLANNING]: WORKSPACE_MODES.OUTREACH,
  [PROGRESSION_STAGES.EXECUTION]: WORKSPACE_MODES.EXECUTION,
});

const TERMINAL_STAGES = Object.freeze([STAGES.OBSERVE, STAGES.LEARN, STAGES.IMPROVE]);

function deriveRenderMode(workspaceMode) {
  if (workspaceMode === WORKSPACE_MODES.CREATE) return RENDER_MODES.CREATE;
  if (workspaceMode === WORKSPACE_MODES.EXECUTION) return RENDER_MODES.EXECUTION;
  if (workspaceMode === WORKSPACE_MODES.DISCOVERY_REVIEW) return RENDER_MODES.REVIEW;
  return RENDER_MODES.INSPECT;
}

function deriveWorkspaceMode(input = {}) {
  const missionId = input.missionId || null;
  const snapshot = input.snapshot || null;

  if (!missionId && !snapshot) {
    return WORKSPACE_MODES.CREATE;
  }

  if (!snapshot) {
    return WORKSPACE_MODES.MISSION_PLANNING;
  }

  const mission = snapshot.mission || {};
  if (mission.planCancelled) {
    return WORKSPACE_MODES.COMPLETE;
  }
  if (/complete/i.test(String(mission.status || ''))) {
    return WORKSPACE_MODES.COMPLETE;
  }
  if (TERMINAL_STAGES.includes(mission.stage)) {
    return WORKSPACE_MODES.COMPLETE;
  }

  const progressionStage = (snapshot.progression && snapshot.progression.stage)
    || deriveProgressionStage(snapshot);

  return PROGRESSION_TO_WORKSPACE[progressionStage] || WORKSPACE_MODES.MISSION_PLANNING;
}

function isComponentVisible(componentId, workspaceMode) {
  const component = COMPONENTS[componentId];
  if (!component) return false;
  return component.supportedModes.includes(workspaceMode);
}

function visibleComponents(workspaceMode) {
  return Object.values(COMPONENTS)
    .filter((row) => row.supportedModes.includes(workspaceMode))
    .map((row) => row.id);
}

function buildWorkspaceContext(input = {}) {
  const workspaceMode = deriveWorkspaceMode(input);
  const renderMode = deriveRenderMode(workspaceMode);
  const snapshot = input.snapshot || null;
  return {
    spec: 'SPEC-153',
    adr: 'ADR-074',
    workspaceMode,
    renderMode,
    label: WORKSPACE_MODE_LABELS[workspaceMode] || workspaceMode,
    visibleComponents: visibleComponents(workspaceMode),
    missionId: input.missionId || (snapshot && snapshot.mission && snapshot.mission.id) || null,
    executionState: snapshot
      ? {
        pendingDiscoveryApproval: hasPendingDiscoveryApproval(snapshot),
        discoveryRunning: isDiscoveryRunning(snapshot),
        progressionStage: (snapshot.progression && snapshot.progression.stage)
          || deriveProgressionStage(snapshot),
      }
      : null,
  };
}

module.exports = {
  WORKSPACE_MODES,
  RENDER_MODES,
  WORKSPACE_MODE_LABELS,
  COMPONENTS,
  INSPECT_LIKE_MODES,
  deriveRenderMode,
  deriveWorkspaceMode,
  isComponentVisible,
  visibleComponents,
  buildWorkspaceContext,
};
