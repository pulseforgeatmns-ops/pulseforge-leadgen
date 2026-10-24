'use strict';

/**
 * Build a StructuredResponse for a Mission Engine outcome (SPEC-022 / SPEC-039).
 * Never invents Market Intelligence — surfaces mission progress only.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const { buildOpenMissionAction, MISSION_RUNTIMES, resolveMissionActionRuntime } = require('./MissionActions');
const {
  buildEngineMissionCommunication,
  formatMissionProse,
  applyMissionCommunication,
  buildReasoningEvidence,
} = require('./MissionCommunication');
const { formatDiscoveryOperatorResponse } = require('../../mission-engine/discoveryExecutionReport');

/**
 * @param {object} input
 * @param {object} input.mission
 * @param {string} input.question
 * @param {object} [input.card]
 */
function composeMissionResponse(input) {
  const mission = input.mission;
  const card = input.card || null;

  const missionComm = buildEngineMissionCommunication(mission, {
    created: true,
    reasoningLines: buildMissionReasoningLines(input, mission),
    includeReasoning: false,
  });
  missionComm.reasoningEvidence = buildReasoningEvidence({
    known: [`Mission type: ${mission.type || 'acquisition'}.`],
    inference: buildMissionReasoningLines(input, mission),
    unknown: [
      ...(Array.isArray(mission.blockingIssues) ? mission.blockingIssues : []),
      ...(mission.stageReview && Array.isArray(mission.stageReview.blockingIssues)
        ? mission.stageReview.blockingIssues
        : []),
    ].map(String),
    evidenceNeeded: [],
    confidence: mission.confidence != null ? Number(mission.confidence) : 0.8,
  });
  missionComm.includeReasoningMarker = true;
  const answer = formatMissionProse(missionComm);

  const recommendedExtras = [];
  if (
    mission.operatorProspectList &&
    mission.operatorProspectList.promptImport &&
    !mission.operatorProspectList.injected
  ) {
    recommendedExtras.push({
      id: 'import_prospect_list',
      type: 'import_prospect_list',
      label: 'Import detected Prospect List',
      payload: { missionId: mission.id },
    });
  }

  const base = buildMissionStructured({
    answer,
    mission,
    card,
    reasoning: [],
    recommendedExtras,
    metadataExtras: input.executionDomain
      ? { executionDomain: input.executionDomain, surface: 'mission_workspace' }
      : { surface: 'mission_workspace' },
  });

  return applyMissionCommunication(base, missionComm);
}

function buildMissionReasoningLines(input, mission) {
  return [
    input.executionDomain === 'mission_diagnostics'
      ? 'Routed as Mission Diagnostics through Intent Understanding → Mission Engine.'
      : 'Routed as a business objective through the Mission Engine (not Market Intelligence).',
    `Mission type: ${mission.type}.`,
    input.executionDomain ? `Execution domain: ${input.executionDomain}.` : null,
    mission.discoveryProfile
      ? `Discovery Profile: ${mission.discoveryProfile.name} v${mission.discoveryProfile.version}.`
      : null,
    mission.operatorProspectList && mission.operatorProspectList.injected
      ? `Operator Artifact Injection: ${mission.operatorProspectList.prospectCount} prospects via ${mission.operatorProspectList.source}.`
      : null,
    mission.plan && mission.plan.reasoning && mission.plan.reasoning.summary
      ? `Planner: ${mission.plan.reasoning.summary}.`
      : null,
    mission.plan && mission.plan.explanation && mission.plan.explanation.pipeline
      ? `Execution graph: ${mission.plan.explanation.pipeline}.`
      : `Plan: ${((mission.plan && mission.plan.steps) || [])
          .map((s) => s.name || s.capabilityId)
          .join(' → ') || 'n/a'}.`,
    mission.plan &&
    mission.plan.explanation &&
    mission.plan.explanation.answers &&
    mission.plan.explanation.answers.whyReviewRequired &&
    mission.plan.explanation.answers.whyReviewRequired.included
      ? `Review required: ${mission.plan.explanation.answers.whyReviewRequired.reason}.`
      : null,
  ].filter(Boolean);
}

function composeActiveMissionResponse(input) {
  const resolution = input.resolution;
  const mission = resolution.mission;
  const card = input.card || null;
  const title = mission.title || 'Mission';
  const action = resolution.action;

  let headline = 'Mission Updated';
  let nextStep = 'Continue in Mission Workspace.';
  let operatorDecision = null;
  let missionCommHealth = null;
  const reasoningLines = [
    `Active Mission Resolver — ${action}.`,
    `Mission: ${title} (${mission.id}).`,
    `Classification: ${resolution.classification}.`,
    `Resolution path: ${resolution.resolutionPath}.`,
  ];

  if (action === 'diagnosed' && resolution.diagnosis) {
    headline = 'Mission Blocked';
    nextStep = resolution.diagnosis.summary;
    if (resolution.diagnosis.lastFail) {
      reasoningLines.push(
        `Last step_fail: ${resolution.diagnosis.lastFail.capabilityId}.`
      );
    }
  } else if (action === 'executed' && resolution.stageExecution) {
    headline = 'Mission Updated';
    const exec = resolution.stageExecution;
    const stageName =
      (exec.stage && (exec.stage.stageName || exec.stage.stageId)) || 'Discovery';
    const discoveryReport =
      exec.result && exec.result.discoveryReport ? exec.result.discoveryReport : null;

    if (discoveryReport) {
      nextStep = formatDiscoveryOperatorResponse(discoveryReport);
      reasoningLines.push(
        `Stage executor: ${exec.executorId || 'none'}.`,
        `Selection: ${exec.selectionReason}.`,
        `Discovery strategy: ${discoveryReport.discoveryStrategy}.`,
        `Discovery outcome: ${discoveryReport.outcome}.`,
        discoveryReport.blockReason
          ? `Block reason: ${discoveryReport.blockReason}.`
          : null,
        `External discovery attempted: ${discoveryReport.externalDiscoveryAttempted ? 'yes' : 'no'}.`,
        `Prospects verified: ${discoveryReport.prospectCount}.`
      );
      if (discoveryReport.outcome === 'DISCOVERY_BLOCKED') {
        headline = 'Mission Updated';
        missionCommHealth = 'Blocked';
      }
    } else {
      nextStep = `Scout discovery executed for stage ${stageName}.`;
      if (mission.status === 'review_required') {
        operatorDecision = 'Awaiting Prioritization Approval';
      } else if (mission.status === 'executing') {
        nextStep = `Discovery in progress (${stageName}).`;
      }
      reasoningLines.push(
        `Stage executor: ${exec.executorId || 'none'}.`,
        `Selection: ${exec.selectionReason}.`,
        exec.result && exec.result.scoutPayload
          ? `Scout dispatch: mission ${exec.result.scoutPayload.missionId}.`
          : null
      );
    }
  } else if (action === 'stage_fallback' && resolution.stageExecution) {
    headline = 'Advisory Response';
    nextStep =
      (resolution.stageExecution.result &&
        resolution.stageExecution.result.summary) ||
      'No stage executor registered — advisory fallback.';
    reasoningLines.push('MISSION_EXECUTOR_FALLBACK — RecommendationEngine selected.');
  } else if (action === 'modified' && resolution.modification) {
    headline = 'Mission Updated';
    nextStep = resolution.modification.summary;
    if (mission.status === 'review_required') {
      operatorDecision = 'Review and approve results?';
    }
  } else if (action === 'blocked') {
    headline = 'Mission Blocked';
    nextStep = resolution.reason
      ? `${resolution.reason}. Mission remains active. No progress lost.`
      : 'Unable to continue. Mission remains active. No progress lost.';
    operatorDecision = 'Resolve blocker to continue?';
  } else if (action === 'clarified') {
    const base = buildMissionStructured({
      answer: [
        'Got it. I will not resume Direct Mail Execution from that correction.',
        'You are asking for a preparation/review-only canary, not an execution run.',
        'To continue, I need either an attached prospect list or permission to use the existing campaign prospects.',
        'Once I have that, I should return ready/blocked status, missing fields, packet contents, letters, notes, scorecard covers, follow-up notes, next actions, and tracking fields before anything is approved or mailed.',
      ].join(' '),
      mission,
      card,
      reasoning: [
        'The operator explicitly constrained this as preparation/review only.',
        'The active execution Mission was not resumed.',
      ],
      metadataExtras: {
        activeMissionAction: action,
        classification: resolution.classification,
        resolutionPath: resolution.resolutionPath,
        ...(input.executionDomain
          ? {
              executionDomain: input.executionDomain,
              surface: 'mission_workspace',
            }
          : { surface: 'mission_workspace' }),
      },
    });
    return base;
  } else {
    headline = 'Mission Updated';
    if (mission.status === 'review_required') {
      nextStep = 'Results are ready for your review. No outbound actions were taken.';
      operatorDecision = 'Review and approve results?';
    } else if (mission.status === 'waiting') {
      nextStep = 'Mission is paused — ask why it failed or run again.';
      operatorDecision = 'Retry, import a prospect list, or cancel?';
    } else {
      nextStep = 'Continuing with the active Mission (no new Mission created).';
    }
  }

  const missionComm = buildEngineMissionCommunication(mission, {
    created: false,
    reasoningLines,
    includeReasoning: false,
  });
  missionComm.headline = headline;
  missionComm.nextStep = nextStep;
  missionComm.operatorDecision = operatorDecision;
  if (
    missionCommHealth ||
    action === 'blocked' ||
    (Array.isArray(mission.blockingIssues) && mission.blockingIssues.length)
  ) {
    missionComm.health = missionCommHealth || 'Blocked';
    missionComm.waitingOn = 'Operator approval';
  }

  const answer = formatMissionProse(missionComm);
  const base = buildMissionStructured({
    answer,
    mission,
    card,
    reasoning: [],
    metadataExtras: {
      activeMissionAction: action,
      classification: resolution.classification,
      resolutionPath: resolution.resolutionPath,
      ...(input.executionDomain
        ? {
            executionDomain: input.executionDomain,
            surface: 'mission_workspace',
          }
        : { surface: 'mission_workspace' }),
    },
  });
  return applyMissionCommunication(base, missionComm);
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
      buildOpenMissionAction({
        missionId: mission.id,
        runtime: mission.runtime || MISSION_RUNTIMES.SPEC_022,
        label: 'Open Mission Workspace',
      }),
      {
        id: 'review_mission',
        type: 'review_mission',
        label: 'Review results',
        payload: {
          missionId: mission.id,
          runtime: resolveMissionActionRuntime(mission.runtime || MISSION_RUNTIMES.SPEC_022),
        },
      },
      ...((input.recommendedExtras) || []),
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
