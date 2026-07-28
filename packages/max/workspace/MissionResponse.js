'use strict';

/**
 * Build a StructuredResponse for a Mission Engine outcome (SPEC-022 / SPEC-039).
 * Never invents Market Intelligence — surfaces mission progress only.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');

/**
 * @param {object} input
 * @param {object} input.mission
 * @param {string} input.question
 * @param {object} [input.card]
 */
function composeMissionResponse(input) {
  const mission = input.mission;
  const card = input.card || null;
  const title = mission.title || 'Mission';
  const stage =
    (mission.progress && mission.progress.currentStage) || mission.status;
  const percent =
    mission.progress && mission.progress.percent != null
      ? mission.progress.percent
      : 0;
  const counts = mission.progress && mission.progress.counts;

  const countLine = counts
    ? `${counts.completed} / ${counts.total}`
    : `${mission.progress?.completedSteps || 0} / ${mission.progress?.totalSteps || 0} steps`;

  const answer = [
    `Mission created: ${title}.`,
    mission.discoveryProfile
      ? mission.discoveryProfile.message ||
        `Using Discovery Profile: ${mission.discoveryProfile.name}.`
      : null,
    `Status: ${formatStatus(mission.status)}.`,
    `Current stage: ${stage} (${percent}%, ${countLine}).`,
    mission.status === 'review_required'
      ? 'Results are ready for your review. No outbound actions were taken.'
      : 'Progress will appear in Operations on the Command Deck.',
  ]
    .filter(Boolean)
    .join(' ');

  return buildMissionStructured({
    answer,
    mission,
    card,
    reasoning: [
      'Routed as a business objective through the Mission Engine (not Market Intelligence).',
      `Mission type: ${mission.type}.`,
      mission.discoveryProfile
        ? `Discovery Profile: ${mission.discoveryProfile.name} v${mission.discoveryProfile.version}.`
        : null,
      `Plan: ${((mission.plan && mission.plan.steps) || [])
        .map((s) => s.name || s.capabilityId)
        .join(' → ') || 'n/a'}.`,
    ].filter(Boolean),
  });
}

/**
 * SPEC-039 — response for resume / modify / diagnose (no new Mission).
 * @param {object} input
 * @param {object} input.resolution - ActiveMissionResolver.resolve result
 * @param {object} [input.card]
 * @param {string} input.question
 */
function composeActiveMissionResponse(input) {
  const resolution = input.resolution;
  const mission = resolution.mission;
  const card = input.card || null;
  const title = mission.title || 'Mission';
  const action = resolution.action;

  let answer;
  let reasoning;

  if (action === 'diagnosed' && resolution.diagnosis) {
    answer = resolution.diagnosis.summary;
    reasoning = [
      'Active Mission Resolver — diagnose (IntentRouter not used).',
      `Mission: ${title} (${mission.id}).`,
      `Classification: ${resolution.classification}.`,
      resolution.diagnosis.lastFail
        ? `Last step_fail: ${resolution.diagnosis.lastFail.capabilityId}.`
        : 'No step_fail events.',
    ];
  } else if (action === 'modified' && resolution.modification) {
    answer = [
      `Mission updated: ${title}.`,
      resolution.modification.summary,
      `Status: ${formatStatus(mission.status)}.`,
      mission.status === 'review_required'
        ? 'Results are ready for your review. No outbound actions were taken.'
        : null,
    ]
      .filter(Boolean)
      .join(' ');
    reasoning = [
      'Active Mission Resolver — modify (same Mission, stale capabilities rerun).',
      `Mission: ${title} (${mission.id}).`,
      `Classification: ${resolution.classification}.`,
    ];
  } else {
    const stage =
      (mission.progress && mission.progress.currentStage) || mission.status;
    answer = [
      `Resumed Mission: ${title}.`,
      `Status: ${formatStatus(mission.status)}.`,
      `Current stage: ${stage}.`,
      mission.status === 'review_required'
        ? 'Results are ready for your review. No outbound actions were taken.'
        : mission.status === 'waiting'
          ? 'Mission is paused — ask why it failed or run again.'
          : 'Continuing with the active Mission (no new Mission created).',
    ].join(' ');
    reasoning = [
      'Active Mission Resolver — resume (IntentRouter not used).',
      `Mission: ${title} (${mission.id}).`,
      `Classification: ${resolution.classification}.`,
      `Resolution path: ${resolution.resolutionPath}.`,
    ];
  }

  return buildMissionStructured({
    answer,
    mission,
    card,
    reasoning,
    metadataExtras: {
      activeMissionAction: action,
      classification: resolution.classification,
      resolutionPath: resolution.resolutionPath,
    },
  });
}

function buildMissionStructured(input) {
  const mission = input.mission;
  const card = input.card || null;

  const supportingEvidence = (
    (mission.deliverables && mission.deliverables.stepResults) ||
    []
  ).flatMap((step, i) =>
    (step.evidence || []).map((e, j) => ({
      id: `mission:${mission.id}:ev:${i}:${j}`,
      summary: e.summary || `${step.name} evidence`,
      sourceType: 'mission',
      kind: e.kind || 'mission',
    }))
  );

  if (!supportingEvidence.length) {
    supportingEvidence.push({
      id: `mission:${mission.id}:active`,
      summary: `Mission ${mission.id} tracked in Operations`,
      sourceType: 'mission',
      kind: 'mission',
    });
  }

  return buildStructuredResponse({
    answer: input.answer,
    reasoning: input.reasoning,
    supportingEvidence,
    contradictingEvidence: [],
    confidence: mission.confidence != null ? mission.confidence : 0.8,
    nextInvestigations: [
      'Open Mission Workspace for full evidence and review actions',
      'Check Operations on the Command Deck for live progress',
    ],
    recommendedActions: [
      {
        id: 'open_mission',
        type: 'open_mission',
        label: 'Open Mission Workspace',
        payload: { missionId: mission.id },
      },
      {
        id: 'review_mission',
        type: 'review_mission',
        label: 'Review results',
        payload: { missionId: mission.id },
      },
    ],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: false,
        memory: false,
        policy: false,
        knowledge: false,
      },
      evidenceCount: supportingEvidence.length,
      asOf: mission.updatedAt || mission.completedAt || mission.createdAt,
      unavailable: [],
      route: 'mission',
      missionId: mission.id,
      missionStatus: mission.status,
      missionCard: card,
      ...(input.metadataExtras || {}),
    },
  });
}

function formatStatus(status) {
  const map = {
    requested: 'Requested',
    planning: 'Planning',
    executing: 'Executing',
    waiting: 'Waiting',
    review_required: 'Review Required',
    completed: 'Completed',
    reviewed: 'Reviewed',
    archived: 'Archived',
    failed: 'Failed',
  };
  return map[status] || status;
}

module.exports = {
  composeMissionResponse,
  composeActiveMissionResponse,
};
