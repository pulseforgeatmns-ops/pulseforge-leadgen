'use strict';

/**
 * SPEC-147 — Autonomous Mission Progression (ADR-066).
 * ADR-067 — Stage Contracts Are Authoritative.
 * Mission execution is progress. The runtime advances until genuine operator judgment is required.
 * Stage Contracts govern behavior; presentation objects explain but never determine execution.
 */

const {
  STAGES,
  STAGE_LABELS,
  SPECIALISTS,
  EVENT_KINDS,
  OPERATOR_DECISION_KINDS,
  asText,
  nowIso,
} = require('./types');
const { createEvent } = require('./Timeline');
const { isReadyForLock } = require('./StructuredMission');
const {
  hasPendingPlanClarification,
  hasPendingPlanApproval,
  hasPendingDiscoveryApproval,
  hasPendingPrioritizationApproval,
  hasDiscoveryArtifact,
} = require('./PendingOperatorDecision');
const {
  presentationFromDiscoveryPayload,
  findLatestDiscoveryContribution,
} = require('./DiscoveryPresentation');
const { currentBlocker } = require('./Blockers');
const { canEnter, specialistContext } = require('./Lifecycle');

/** Logical progression stages (SPEC-147 vocabulary). */
const PROGRESSION_STAGES = Object.freeze({
  UNDERSTANDING: 'understanding',
  DISCOVERY: 'discovery',
  DISCOVERY_REVIEW: 'discovery_review',
  OUTREACH_PLANNING: 'outreach_planning',
  EXECUTION: 'execution',
});

const PROGRESSION_STAGE_LABELS = Object.freeze({
  [PROGRESSION_STAGES.UNDERSTANDING]: 'Understanding',
  [PROGRESSION_STAGES.DISCOVERY]: 'Discovery',
  [PROGRESSION_STAGES.DISCOVERY_REVIEW]: 'Discovery Review',
  [PROGRESSION_STAGES.OUTREACH_PLANNING]: 'Outreach Planning',
  [PROGRESSION_STAGES.EXECUTION]: 'Execution',
});

const SCOUT_PIPELINE_STAGES = Object.freeze([
  'Market Understanding',
  'Investigation Planning',
  'Evidence Planning',
  'Provider Strategy',
  'Candidate Discovery',
  'Evidence Collection',
  'Evidence Conflict Resolution',
  'Qualification',
  'Opportunity Ranking',
  'Market Coverage',
]);

const MISSION_STAGE_CONTRACTS = Object.freeze({
  [PROGRESSION_STAGES.UNDERSTANDING]: {
    stage: PROGRESSION_STAGES.UNDERSTANDING,
    amoStage: STAGES.DISCOVER,
    executesAutomatically: true,
    requiresHumanDecision: false,
    completionCriteria: 'Mission plan ready for lock with no ambiguities.',
    nextStage: PROGRESSION_STAGES.DISCOVERY,
  },
  [PROGRESSION_STAGES.DISCOVERY]: {
    stage: PROGRESSION_STAGES.DISCOVERY,
    amoStage: STAGES.DISCOVER,
    executesAutomatically: true,
    requiresHumanDecision: false,
    completionCriteria: 'Scout Intelligence Pipeline complete; Mission Intelligence Report ready.',
    nextStage: PROGRESSION_STAGES.DISCOVERY_REVIEW,
  },
  [PROGRESSION_STAGES.DISCOVERY_REVIEW]: {
    stage: PROGRESSION_STAGES.DISCOVERY_REVIEW,
    amoStage: STAGES.DISCOVER,
    executesAutomatically: false,
    requiresHumanDecision: true,
    completionCriteria: 'Operator approves findings before outreach planning.',
    nextStage: PROGRESSION_STAGES.OUTREACH_PLANNING,
  },
  [PROGRESSION_STAGES.OUTREACH_PLANNING]: {
    stage: PROGRESSION_STAGES.OUTREACH_PLANNING,
    amoStage: STAGES.PLAN,
    executesAutomatically: true,
    requiresHumanDecision: false,
    completionCriteria: 'Outreach plan drafted.',
    nextStage: PROGRESSION_STAGES.EXECUTION,
  },
  [PROGRESSION_STAGES.EXECUTION]: {
    stage: PROGRESSION_STAGES.EXECUTION,
    amoStage: STAGES.EXECUTE,
    executesAutomatically: true,
    requiresHumanDecision: true,
    completionCriteria: 'Operator approval before send.',
    nextStage: null,
  },
});

const AUTONOMOUS_OPERATOR_ID = 'autonomous_runtime';
const AUTONOMOUS_COMMAND = 'Autonomous progression.';

function createStageTransition(input = {}) {
  return {
    from: asText(input.from) || null,
    to: asText(input.to) || null,
    trigger: asText(input.trigger) || null,
    timestamp: input.timestamp || nowIso(),
    automatic: input.automatic !== false,
  };
}

function createMissionPause(input = {}) {
  return {
    stage: asText(input.stage) || null,
    reason: asText(input.reason) || null,
    requiredDecision: asText(input.requiredDecision) || null,
    availableOptions: Array.isArray(input.availableOptions) ? input.availableOptions : [],
  };
}

function createExecutionBlock(input = {}) {
  return {
    stage: asText(input.stage) || null,
    unmetPrecondition: asText(input.unmetPrecondition) || null,
    blockingComponent: asText(input.blockingComponent) || null,
    recommendedAction: asText(input.recommendedAction) || null,
    pauseFallback: input.pauseFallback === true,
  };
}

function getStageContract(progressionStage) {
  if (!progressionStage) return null;
  return MISSION_STAGE_CONTRACTS[progressionStage] || null;
}

function validateStageTransition(fromStage, toStage) {
  const contract = getStageContract(fromStage);
  if (!contract) {
    return { ok: false, reason: 'Unknown progression stage.' };
  }
  if (contract.requiresHumanDecision) {
    return { ok: false, reason: 'Stage contract requires operator judgment before transition.' };
  }
  if (!contract.executesAutomatically) {
    return { ok: false, reason: 'Stage does not execute automatically.' };
  }
  if (contract.nextStage !== toStage) {
    return { ok: false, reason: `Transition ${fromStage} → ${toStage} is not permitted by stage contract.` };
  }
  return { ok: true };
}

function createPauseFallbackBlock(snapshot = {}, contract = null) {
  const stage = (contract && contract.stage) || deriveProgressionStage(snapshot);
  const mission = snapshot.mission || snapshot;
  const pending = mission && mission.pendingOperatorDecision;
  return createExecutionBlock({
    stage,
    unmetPrecondition: (pending && (pending.prompt || pending.clarificationPrompt))
      || (contract && contract.completionCriteria)
      || 'Operator judgment required before progression.',
    blockingComponent: 'Stage Contract',
    recommendedAction: 'Provide operator decision to continue mission progression.',
    pauseFallback: true,
  });
}

/**
 * ADR-067 — Stage Contract is authoritative for human-decision gates.
 * MissionPause explains the pause when available; ExecutionBlock is the required fallback.
 */
function resolveHumanDecisionGate(snapshot = {}) {
  const progressionStage = deriveProgressionStage(snapshot);
  const contract = getStageContract(progressionStage);
  if (!contract || !contract.requiresHumanDecision) {
    return {
      shouldPause: false,
      progressionStage,
      contract,
      pause: null,
      block: null,
    };
  }

  const pause = deriveMissionPause(snapshot);
  if (pause) {
    return { shouldPause: true, progressionStage, contract, pause, block: null };
  }

  return {
    shouldPause: true,
    progressionStage,
    contract,
    pause: null,
    block: createPauseFallbackBlock(snapshot, contract),
  };
}

function resolveProgressionState(snapshot = {}) {
  const progressionStage = deriveProgressionStage(snapshot);
  const contract = getStageContract(progressionStage);
  const gate = resolveHumanDecisionGate(snapshot);

  if (gate.shouldPause) {
    return {
      progressionStage,
      contract,
      outcome: 'paused',
      pause: gate.pause,
      block: gate.block,
    };
  }

  const block = deriveExecutionBlock(snapshot);
  if (block) {
    return { progressionStage, contract, outcome: 'blocked', pause: null, block };
  }

  const pause = deriveMissionPause(snapshot);
  if (pause) {
    return { progressionStage, contract, outcome: 'paused', pause, block: null };
  }

  return { progressionStage, contract, outcome: 'active', pause: null, block: null };
}

function deriveProgressionStage(snapshot = {}) {
  const mission = snapshot.mission || snapshot;
  const contributions = snapshot.contributions || [];
  if (!mission) return null;

  if (hasPendingPlanClarification(snapshot)) {
    return PROGRESSION_STAGES.UNDERSTANDING;
  }
  if (hasPendingPlanApproval(snapshot)) {
    return PROGRESSION_STAGES.UNDERSTANDING;
  }
  if (hasPendingDiscoveryApproval(snapshot)) {
    return PROGRESSION_STAGES.DISCOVERY;
  }
  if (hasPendingPrioritizationApproval(snapshot)) {
    return PROGRESSION_STAGES.DISCOVERY_REVIEW;
  }
  if (hasDiscoveryArtifact(snapshot) && mission.stage === STAGES.DISCOVER) {
    return PROGRESSION_STAGES.DISCOVERY_REVIEW;
  }
  if (mission.stage === STAGES.UNDERSTAND || mission.stage === STAGES.PLAN) {
    return PROGRESSION_STAGES.OUTREACH_PLANNING;
  }
  if (mission.stage === STAGES.EXECUTE || mission.stage === STAGES.READY) {
    return PROGRESSION_STAGES.EXECUTION;
  }
  if (mission.stage === STAGES.DISCOVER && !hasDiscoveryArtifact(snapshot)) {
    return hasPendingDiscoveryApproval(snapshot)
      ? PROGRESSION_STAGES.DISCOVERY
      : PROGRESSION_STAGES.UNDERSTANDING;
  }
  return PROGRESSION_STAGES.UNDERSTANDING;
}

function deriveMissionPause(snapshot = {}) {
  const mission = snapshot.mission || snapshot;
  if (!mission) return null;

  if (hasPendingPlanClarification(snapshot)) {
    const pending = mission.pendingOperatorDecision || {};
    return createMissionPause({
      stage: PROGRESSION_STAGES.UNDERSTANDING,
      reason: 'Mission understanding has unresolved ambiguities.',
      requiredDecision: pending.prompt || pending.clarificationPrompt || 'Clarify mission parameters.',
      availableOptions: pending.choices || [],
    });
  }

  if (hasPendingPlanApproval(snapshot)) {
    return createMissionPause({
      stage: PROGRESSION_STAGES.UNDERSTANDING,
      reason: 'Mission understanding complete. Operator approval required before Scout investigation.',
      requiredDecision: 'Approve mission plan?',
      availableOptions: ['Approve', 'Edit', 'Cancel'],
    });
  }

  if (hasPendingPrioritizationApproval(snapshot)) {
    return createMissionPause({
      stage: PROGRESSION_STAGES.DISCOVERY_REVIEW,
      reason: 'Scout investigation completed. Operator judgment required before outreach recommendations.',
      requiredDecision: 'Approve prioritization?',
      availableOptions: [
        'Approve findings',
        'Request additional investigation',
        'Adjust mission',
        'Cancel',
      ],
    });
  }

  const blocker = currentBlocker(mission.blockers);
  if (blocker) {
    return createMissionPause({
      stage: deriveProgressionStage(snapshot),
      reason: blocker.reason || blocker.label,
      requiredDecision: 'Resolve blocker',
      availableOptions: ['Retry', 'Cancel mission'],
    });
  }

  return null;
}

function deriveExecutionBlock(snapshot = {}, err = null) {
  const mission = snapshot.mission || snapshot;
  const progressionStage = deriveProgressionStage(snapshot);
  const message = err && err.message ? err.message : null;

  if (err && err.code === 'tme_plan_ambiguous') {
    return createExecutionBlock({
      stage: PROGRESSION_STAGES.UNDERSTANDING,
      unmetPrecondition: 'Mission plan has unresolved ambiguities.',
      blockingComponent: 'Mission Planning Engine',
      recommendedAction: 'Answer clarification questions before retrying.',
    });
  }

  if (message && /investigation planner|scout/i.test(message)) {
    return createExecutionBlock({
      stage: PROGRESSION_STAGES.DISCOVERY,
      unmetPrecondition: message,
      blockingComponent: 'Scout Investigation Runtime',
      recommendedAction: 'Retry planning.',
    });
  }

  const discovery = findLatestDiscoveryContribution(snapshot.contributions || []);
  if (discovery && discovery.payload && discovery.payload.blocked) {
    return createExecutionBlock({
      stage: PROGRESSION_STAGES.DISCOVERY,
      unmetPrecondition: discovery.payload.summary || 'Discovery blocked.',
      blockingComponent: 'Scout Intelligence Pipeline',
      recommendedAction: 'Resolve discovery blocker and retry.',
    });
  }

  const gate = canEnter(STAGES.UNDERSTAND, specialistContext(snapshot.contributions || []));
  if (progressionStage === PROGRESSION_STAGES.OUTREACH_PLANNING && !gate.ok) {
    return createExecutionBlock({
      stage: PROGRESSION_STAGES.OUTREACH_PLANNING,
      unmetPrecondition: gate.reason,
      blockingComponent: 'Mission Lifecycle',
      recommendedAction: 'Complete prior stage prerequisites.',
    });
  }

  if (err) {
    return createExecutionBlock({
      stage: progressionStage,
      unmetPrecondition: message || 'Execution failed.',
      blockingComponent: err.blockingComponent || 'Mission Runtime',
      recommendedAction: err.recommendedAction || 'Retry after resolving the blocker.',
    });
  }

  return null;
}

function recordStageTransition(engine, missionId, transition, opts = {}) {
  if (!engine || !transition) return transition;
  engine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.STAGE_TRANSITION,
    specialist: SPECIALISTS.MAX,
    at: transition.timestamp,
    label: `${transition.from} → ${transition.to}`,
    payload: {
      from: transition.from,
      to: transition.to,
      trigger: transition.trigger,
      automatic: transition.automatic,
      spec: 'SPEC-147',
    },
  }));
  return transition;
}

function canAutoAdvanceUnderstanding(snapshot) {
  const mission = snapshot.mission || snapshot;
  if (!mission || mission.planCancelled) return false;
  if (hasPendingPlanClarification(snapshot)) return false;
  if (!hasPendingPlanApproval(snapshot)) return false;
  const draft = mission.missionPlanDraft;
  return Boolean(draft && isReadyForLock(draft));
}

function canAutoAdvanceDiscovery(snapshot) {
  if (!hasPendingDiscoveryApproval(snapshot)) return false;
  const mission = snapshot.mission || snapshot;
  return Boolean(mission && mission.structuredMissionApproved);
}

function buildDiscoveryPipelineStatus(snapshot = {}) {
  const discovery = findLatestDiscoveryContribution(snapshot.contributions || []);
  if (!discovery) {
    return SCOUT_PIPELINE_STAGES.map((label) => ({ label, status: 'pending' }));
  }
  const payload = discovery.payload || {};
  const blocked = payload.blocked === true;
  const complete = !blocked && hasDiscoveryArtifact(snapshot);
  return SCOUT_PIPELINE_STAGES.map((label) => ({
    label,
    status: complete ? 'complete' : blocked ? 'blocked' : 'executing',
  }));
}

function formatMissionProgressPresentation(snapshot = {}, result = {}) {
  const mission = snapshot.mission || {};
  const progressionStage = result.progressionStage || deriveProgressionStage(snapshot);
  const pause = result.pause || deriveMissionPause(snapshot);
  const block = result.block || null;
  const transitions = result.transitions || [];
  const pipeline = buildDiscoveryPipelineStatus(snapshot);
  const lines = [];

  if (block && block.pauseFallback) {
    lines.push('Mission Paused', '', `Stage: ${PROGRESSION_STAGE_LABELS[block.stage] || block.stage}`, '');
    lines.push('Reason', '', block.unmetPrecondition || '', '');
    lines.push('Action', '', block.recommendedAction || '', '');
    return lines.join('\n').trim();
  }

  if (block) {
    lines.push('Mission Blocked', '', `Stage: ${PROGRESSION_STAGE_LABELS[block.stage] || block.stage}`, '');
    lines.push('Reason', '', block.unmetPrecondition || '', '');
    lines.push('Component', '', block.blockingComponent || '', '');
    lines.push('Action', '', block.recommendedAction || '', '');
    return lines.join('\n').trim();
  }

  if (pause && pause.stage === PROGRESSION_STAGES.DISCOVERY_REVIEW) {
    lines.push('Mission Intelligence Report Ready', '');
    for (const row of pipeline) {
      if (row.status === 'complete') lines.push(`✓ ${row.label}`);
    }
    lines.push('', 'Mission Paused', '', `Stage: ${PROGRESSION_STAGE_LABELS[pause.stage]}`, '');
    lines.push('Reason', '', pause.reason || '', '');
    lines.push('Decision Needed', '', pause.requiredDecision || '', '');
    if (pause.availableOptions.length) {
      lines.push('', 'Options', '', pause.availableOptions.join('\n'));
    }
    return lines.join('\n').trim();
  }

  if (pause) {
    lines.push('Mission Paused', '', `Stage: ${PROGRESSION_STAGE_LABELS[pause.stage] || pause.stage}`, '');
    lines.push('Reason', '', pause.reason || '', '');
    lines.push('Decision Needed', '', pause.requiredDecision || '', '');
    if (pause.availableOptions.length) {
      lines.push('', 'Options', '', pause.availableOptions.join('\n'));
    }
    return lines.join('\n').trim();
  }

  lines.push('Mission Progress', '');
  for (const t of transitions) {
    lines.push(`✓ ${PROGRESSION_STAGE_LABELS[t.from] || t.from} completed automatically.`);
  }
  if (progressionStage === PROGRESSION_STAGES.DISCOVERY) {
    lines.push('', 'Beginning Scout Investigation.', '', 'Current Stage', '', 'Discovery', '', 'Executing', '');
    for (const row of pipeline) {
      const mark = row.status === 'complete' ? '✓ ' : row.status === 'executing' ? '• ' : '  ';
      lines.push(`${mark}${row.label}`);
    }
  } else {
    lines.push('', 'Current Stage', '', PROGRESSION_STAGE_LABELS[progressionStage] || progressionStage);
  }
  return lines.join('\n').trim();
}

/**
 * Advance every stage whose preconditions are satisfied until human judgment is required.
 * @param {object} input
 * @param {object} input.engine
 * @param {string} input.missionId
 * @param {string} input.tenantId
 * @param {object} [input.deps] - inject advancePlanAfterApproval / advanceDiscoveryAfterApproval for tests
 */
async function runAutonomousProgression(input = {}) {
  const {
    engine,
    missionId,
    tenantId,
    operatorId = AUTONOMOUS_OPERATOR_ID,
    allowFixtureFallback = true,
    maxSteps = 8,
    deps = {},
  } = input;

  if (!engine || !missionId) {
    throw new Error('engine and missionId are required for autonomous progression.');
  }

  const advancePlan = deps.advancePlanAfterApproval
    || require('../max/workspace/AmoOperatorApproval').advancePlanAfterApproval;
  const advanceDiscovery = deps.advanceDiscoveryAfterApproval
    || require('../max/workspace/AmoOperatorApproval').advanceDiscoveryAfterApproval;

  const transitions = [];
  let steps = 0;
  let lastError = null;

  while (steps < maxSteps) {
    steps += 1;
    const snapshot = engine.inspect(missionId, { tenantId });
    const mission = snapshot.mission;
    if (!mission || mission.planCancelled) break;

    const gate = resolveHumanDecisionGate(snapshot);

    if (gate.shouldPause) {
      return {
        spec: 'SPEC-147',
        outcome: 'paused',
        progressionStage: gate.progressionStage,
        pause: gate.pause,
        block: gate.block,
        transitions,
        snapshot,
        presentation: formatMissionProgressPresentation(snapshot, {
          progressionStage: gate.progressionStage,
          pause: gate.pause,
          block: gate.block,
          transitions,
        }),
      };
    }

    if (canAutoAdvanceUnderstanding(snapshot)) {
      const transitionCheck = validateStageTransition(
        PROGRESSION_STAGES.UNDERSTANDING,
        PROGRESSION_STAGES.DISCOVERY
      );
      if (!transitionCheck.ok) break;
      try {
        await advancePlan({
          engine,
          mission,
          tenantId,
          question: `${AUTONOMOUS_COMMAND} Mission understanding complete. No ambiguity detected.`,
          operatorId,
          allowFixtureFallback,
          ...input,
        });
        transitions.push(createStageTransition({
          from: PROGRESSION_STAGES.UNDERSTANDING,
          to: PROGRESSION_STAGES.DISCOVERY,
          trigger: 'Mission understanding complete. No ambiguity detected.',
        }));
        recordStageTransition(engine, missionId, transitions[transitions.length - 1], { tenantId });
        continue;
      } catch (err) {
        lastError = err;
        break;
      }
    }

    if (canAutoAdvanceDiscovery(snapshot)) {
      const transitionCheck = validateStageTransition(
        PROGRESSION_STAGES.DISCOVERY,
        PROGRESSION_STAGES.DISCOVERY_REVIEW
      );
      if (!transitionCheck.ok) break;
      try {
        const result = await advanceDiscovery({
          engine,
          mission: engine.get(missionId, tenantId),
          tenantId,
          question: `${AUTONOMOUS_COMMAND} Begin Scout investigation.`,
          operatorId,
          allowFixtureFallback,
          ...input,
        });
        transitions.push(createStageTransition({
          from: PROGRESSION_STAGES.DISCOVERY,
          to: PROGRESSION_STAGES.DISCOVERY_REVIEW,
          trigger: result.executionOutcome === 'blocked'
            ? 'Scout investigation blocked.'
            : 'Scout investigation completed.',
        }));
        recordStageTransition(engine, missionId, transitions[transitions.length - 1], { tenantId });

        const after = engine.inspect(missionId, { tenantId });
        if (result.executionOutcome === 'blocked') {
          return {
            spec: 'SPEC-147',
            outcome: 'blocked',
            progressionStage: PROGRESSION_STAGES.DISCOVERY,
            pause: null,
            block: deriveExecutionBlock(after, { message: 'Discovery blocked.' }),
            transitions,
            snapshot: after,
            presentation: formatMissionProgressPresentation(after, {
              progressionStage: PROGRESSION_STAGES.DISCOVERY,
              block: deriveExecutionBlock(after),
              transitions,
            }),
          };
        }
        continue;
      } catch (err) {
        lastError = err;
        break;
      }
    }

    break;
  }

  const snapshot = engine.inspect(missionId, { tenantId });
  const gate = resolveHumanDecisionGate(snapshot);
  if (gate.shouldPause) {
    return {
      spec: 'SPEC-147',
      outcome: 'paused',
      progressionStage: gate.progressionStage,
      pause: gate.pause,
      block: gate.block,
      transitions,
      snapshot,
      presentation: formatMissionProgressPresentation(snapshot, {
        progressionStage: gate.progressionStage,
        pause: gate.pause,
        block: gate.block,
        transitions,
      }),
      error: lastError || null,
    };
  }

  const pause = deriveMissionPause(snapshot);
  const block = lastError ? deriveExecutionBlock(snapshot, lastError) : null;

  return {
    spec: 'SPEC-147',
    outcome: block ? 'blocked' : pause ? 'paused' : 'complete',
    progressionStage: deriveProgressionStage(snapshot),
    pause,
    block,
    transitions,
    snapshot,
    presentation: formatMissionProgressPresentation(snapshot, {
      progressionStage: deriveProgressionStage(snapshot),
      pause,
      block,
      transitions,
    }),
    error: lastError || null,
  };
}

function isAutonomousProgressionCommand(text) {
  const q = asText(text).toLowerCase();
  if (!q) return false;
  return (
    /\bexecute\b.*\bautonomous\b/i.test(q) ||
    /\bautonomous\b.*\bstage/i.test(q) ||
    /\bprogress\b.*\bautonom/i.test(q) ||
    /\bexecute\b.*\b(?:all|every)\b.*\bstage/i.test(q) ||
    /\brun\b.*\bend-to-end\b/i.test(q)
  );
}

module.exports = {
  PROGRESSION_STAGES,
  PROGRESSION_STAGE_LABELS,
  SCOUT_PIPELINE_STAGES,
  MISSION_STAGE_CONTRACTS,
  AUTONOMOUS_OPERATOR_ID,
  getStageContract,
  validateStageTransition,
  createPauseFallbackBlock,
  resolveHumanDecisionGate,
  resolveProgressionState,
  createStageTransition,
  createMissionPause,
  createExecutionBlock,
  deriveProgressionStage,
  deriveMissionPause,
  deriveExecutionBlock,
  recordStageTransition,
  canAutoAdvanceUnderstanding,
  canAutoAdvanceDiscovery,
  buildDiscoveryPipelineStatus,
  formatMissionProgressPresentation,
  runAutonomousProgression,
  isAutonomousProgressionCommand,
};
