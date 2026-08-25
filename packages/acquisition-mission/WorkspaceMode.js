'use strict';

/**
 * SPEC-153 — Mission Workspace Modes.
 * A workspace has one active context. Rendering is driven by workspace mode,
 * not static HTML. Mission creation and mission inspection never render together.
 */

const { STAGES } = require('./types');
const { deriveProgressionStage, PROGRESSION_STAGES } = require('./MissionProgression');

/** Operator-journey workspace modes (expanded from base CREATE / INSPECT / EXECUTION / REVIEW). */
const WORKSPACE_MODES = Object.freeze({
  CREATE: 'create',
  UNDERSTANDING: 'understanding',
  DISCOVERY: 'discovery',
  DISCOVERY_REVIEW: 'discovery_review',
  OUTREACH: 'outreach',
  EXECUTION: 'execution',
  COMPLETE: 'complete',
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
  [WORKSPACE_MODES.UNDERSTANDING]: 'Understanding',
  [WORKSPACE_MODES.DISCOVERY]: 'Discovery',
  [WORKSPACE_MODES.DISCOVERY_REVIEW]: 'Discovery Review',
  [WORKSPACE_MODES.OUTREACH]: 'Outreach',
  [WORKSPACE_MODES.EXECUTION]: 'Execution',
  [WORKSPACE_MODES.COMPLETE]: 'Complete',
});

const INSPECT_LIKE_MODES = Object.freeze([
  WORKSPACE_MODES.UNDERSTANDING,
  WORKSPACE_MODES.DISCOVERY,
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
      WORKSPACE_MODES.UNDERSTANDING,
      WORKSPACE_MODES.DISCOVERY,
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
    supportedModes: [WORKSPACE_MODES.DISCOVERY, WORKSPACE_MODES.DISCOVERY_REVIEW],
  },
  missionIntelligenceReport: {
    id: 'missionIntelligenceReport',
    supportedModes: [WORKSPACE_MODES.DISCOVERY_REVIEW],
  },
  blockers: {
    id: 'blockers',
    supportedModes: [
      WORKSPACE_MODES.UNDERSTANDING,
      WORKSPACE_MODES.DISCOVERY,
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
      WORKSPACE_MODES.UNDERSTANDING,
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
  [PROGRESSION_STAGES.UNDERSTANDING]: WORKSPACE_MODES.UNDERSTANDING,
  [PROGRESSION_STAGES.DISCOVERY]: WORKSPACE_MODES.DISCOVERY,
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
    return WORKSPACE_MODES.UNDERSTANDING;
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

  return PROGRESSION_TO_WORKSPACE[progressionStage] || WORKSPACE_MODES.UNDERSTANDING;
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
  return {
    spec: 'SPEC-153',
    workspaceMode,
    renderMode,
    label: WORKSPACE_MODE_LABELS[workspaceMode] || workspaceMode,
    visibleComponents: visibleComponents(workspaceMode),
    missionId: input.missionId || (input.snapshot && input.snapshot.mission && input.snapshot.mission.id) || null,
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
