'use strict';

/**
 * SPEC-147 — Autonomous Mission Progression (ADR-066).
 * Mission execution is progress. The runtime advances until genuine operator judgment is required.
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
const { isStructuredMissionApproved } = require('./StructuredMission');
const {
  presentationFromDiscoveryPayload,
  findLatestDiscoveryContribution,
} = require('./DiscoveryPresentation');
const { currentBlocker } = require('./Blockers');
const { canEnter, specialistContext } = require('./Lifecycle');

/** Logical progression stages (SPEC-147 + ADR-074 execution-state vocabulary). */
const PROGRESSION_STAGES = Object.freeze({
  MISSION_PLANNING: 'mission_planning',
  DISCOVERY_APPROVAL: 'discovery_approval',
  DISCOVERY_RUNNING: 'discovery_running',
  DISCOVERY_REVIEW: 'discovery_review',
  OUTREACH_PLANNING: 'outreach_planning',
  EXECUTION: 'execution',
  /** @deprecated Use MISSION_PLANNING */
  UNDERSTANDING: 'mission_planning',
  /** @deprecated Use DISCOVERY_APPROVAL or DISCOVERY_RUNNING */
  DISCOVERY: 'discovery_approval',
});

const PROGRESSION_STAGE_LABELS = Object.freeze({
  [PROGRESSION_STAGES.MISSION_PLANNING]: 'Mission Planning',
  [PROGRESSION_STAGES.DISCOVERY_APPROVAL]: 'Discovery Approval',
  [PROGRESSION_STAGES.DISCOVERY_RUNNING]: 'Discovery Running',
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
  [PROGRESSION_STAGES.MISSION_PLANNING]: {
    stage: PROGRESSION_STAGES.MISSION_PLANNING,
    amoStage: STAGES.DISCOVER,
    executesAutomatically: true,
    requiresHumanDecision: false,
    completionCriteria: 'Mission plan ready for lock with no ambiguities.',
    nextStage: PROGRESSION_STAGES.DISCOVERY_APPROVAL,
  },
  [PROGRESSION_STAGES.DISCOVERY_APPROVAL]: {
    stage: PROGRESSION_STAGES.DISCOVERY_APPROVAL,
    amoStage: STAGES.DISCOVER,
    executesAutomatically: true,
    requiresHumanDecision: false,
    completionCriteria: 'Operator or autonomous policy consumes discovery approval.',
    nextStage: PROGRESSION_STAGES.DISCOVERY_RUNNING,
  },
  [PROGRESSION_STAGES.DISCOVERY_RUNNING]: {
    stage: PROGRESSION_STAGES.DISCOVERY_RUNNING,
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
  };
}

function isDiscoveryRunning(snapshot = {}) {
  const mission = snapshot.mission || snapshot;
  if (!mission) return false;
  if (hasPendingPlanClarification(snapshot) || hasPendingPlanApproval(snapshot)) return false;
  if (hasPendingDiscoveryApproval(snapshot)) return false;
  if (hasPendingPrioritizationApproval(snapshot)) return false;
  if (hasDiscoveryArtifact(snapshot)) return false;
  if (!isStructuredMissionApproved(mission)) return false;
  if (mission.stage && mission.stage !== STAGES.DISCOVER) return false;
  return true;
}

function deriveProgressionStage(snapshot = {}) {
  const mission = snapshot.mission || snapshot;
  if (!mission) return null;

  if (hasPendingPlanClarification(snapshot)) {
    return PROGRESSION_STAGES.MISSION_PLANNING;
  }
  if (hasPendingPlanApproval(snapshot)) {
    return PROGRESSION_STAGES.MISSION_PLANNING;
  }
  if (hasPendingDiscoveryApproval(snapshot)) {
    return PROGRESSION_STAGES.DISCOVERY_APPROVAL;
  }
  if (hasPendingPrioritizationApproval(snapshot)) {
    return PROGRESSION_STAGES.DISCOVERY_REVIEW;
  }
  if (hasDiscoveryArtifact(snapshot) && mission.stage === STAGES.DISCOVER) {
    return PROGRESSION_STAGES.DISCOVERY_REVIEW;
  }
  if (isDiscoveryRunning(snapshot)) {
    return PROGRESSION_STAGES.DISCOVERY_RUNNING;
  }
  if (mission.stage === STAGES.UNDERSTAND || mission.stage === STAGES.PLAN) {
    return PROGRESSION_STAGES.OUTREACH_PLANNING;
  }
  if (mission.stage === STAGES.EXECUTE || mission.stage === STAGES.READY) {
    return PROGRESSION_STAGES.EXECUTION;
  }
  if (mission.stage === STAGES.DISCOVER && !hasDiscoveryArtifact(snapshot)) {
    return hasPendingDiscoveryApproval(snapshot)
      ? PROGRESSION_STAGES.DISCOVERY_APPROVAL
      : PROGRESSION_STAGES.MISSION_PLANNING;
  }
  return PROGRESSION_STAGES.MISSION_PLANNING;
}

function deriveMissionPause(snapshot = {}) {
  const mission = snapshot.mission || snapshot;
  if (!mission) return null;

  if (hasPendingPlanClarification(snapshot)) {
    const pending = mission.pendingOperatorDecision || {};
    return createMissionPause({
      stage: PROGRESSION_STAGES.MISSION_PLANNING,
      reason: 'Mission understanding has unresolved ambiguities.',
      requiredDecision: pending.prompt || pending.clarificationPrompt || 'Clarify mission parameters.',
      availableOptions: pending.choices || [],
    });
  }

  if (hasPendingPlanApproval(snapshot)) {
    return createMissionPause({
      stage: PROGRESSION_STAGES.MISSION_PLANNING,
      reason: 'Mission understanding complete. Operator approval required before Scout investigation.',
      requiredDecision: 'Approve mission plan?',
      availableOptions: ['Approve', 'Edit', 'Cancel'],
    });
  }

  if (hasPendingDiscoveryApproval(snapshot)) {
    return createMissionPause({
      stage: PROGRESSION_STAGES.DISCOVERY_APPROVAL,
      reason: 'Mission plan approved. Operator approval required before Scout investigation.',
      requiredDecision: 'Approve discovery?',
      availableOptions: ['Approve discovery', 'Cancel'],
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
      stage: PROGRESSION_STAGES.MISSION_PLANNING,
      unmetPrecondition: 'Mission plan has unresolved ambiguities.',
      blockingComponent: 'Mission Planning Engine',
      recommendedAction: 'Answer clarification questions before retrying.',
    });
  }

  if (message && /investigation planner|scout/i.test(message)) {
    return createExecutionBlock({
      stage: PROGRESSION_STAGES.DISCOVERY_RUNNING,
      unmetPrecondition: message,
      blockingComponent: 'Scout Investigation Runtime',
      recommendedAction: 'Retry planning.',
    });
  }

  const discovery = findLatestDiscoveryContribution(snapshot.contributions || []);
  if (discovery && discovery.payload && discovery.payload.blocked) {
    const summary = discovery.payload.summary || 'Discovery blocked.';
    const isCapabilityBlock =
      discovery.payload.blockerCode === 'external_discovery_capability_unavailable' ||
      /external discovery capability unavailable/i.test(summary);
    return createExecutionBlock({
      stage: PROGRESSION_STAGES.DISCOVERY_RUNNING,
      unmetPrecondition: summary,
      blockingComponent: isCapabilityBlock
        ? 'External Discovery Provider Registry'
        : 'Scout Intelligence Pipeline',
      recommendedAction: isCapabilityBlock
        ? 'Configure an external discovery provider before retrying.'
        : 'Resolve discovery blocker and retry.',
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

function canAutoAdvanceMaxPrioritization(snapshot) {
  const mission = snapshot.mission || snapshot;
  if (!mission || mission.planCancelled) return false;
  if (mission.stage !== STAGES.UNDERSTAND) return false;
  if (hasPendingPrioritizationApproval(snapshot)) return false;
  const ctx = specialistContext(snapshot.contributions || []);
  if (!ctx.scoutComplete || ctx.maxComplete) return false;
  const { findPrioritizationApproval } = require('../max/workspace/AmoOperatorApproval');
  return Boolean(findPrioritizationApproval(snapshot.contributions || []));
}

function canAutoAdvanceOutreachToPaige(snapshot) {
  const mission = snapshot.mission || snapshot;
  if (!mission || mission.planCancelled) return false;
  if (hasPendingPrioritizationApproval(snapshot)) return false;
  const ctx = specialistContext(snapshot.contributions || []);
  if (!ctx.maxComplete || ctx.paigeComplete) return false;
  return [STAGES.UNDERSTAND, STAGES.PLAN, STAGES.PREPARE].includes(mission.stage);
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
  if (progressionStage === PROGRESSION_STAGES.DISCOVERY_RUNNING) {
    lines.push('', 'Beginning Scout Investigation.', '', 'Current Stage', '', 'Discovery Running', '', 'Executing', '');
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
  const advanceMaxPrioritization = deps.advanceMaxPrioritization
    || require('../max/workspace/AmoOperatorApproval').advanceMaxPrioritization;
  const advancePaigeVariants = deps.advancePaigeVariants
    || require('../max/workspace/AmoOperatorApproval').advancePaigeVariants;

  const transitions = [];
  let steps = 0;
  let lastError = null;

  while (steps < maxSteps) {
    steps += 1;
    const snapshot = engine.inspect(missionId, { tenantId });
    const mission = snapshot.mission;
    if (!mission || mission.planCancelled) break;

    const pause = deriveMissionPause(snapshot);
    const contract = MISSION_STAGE_CONTRACTS[deriveProgressionStage(snapshot)];

    if (pause && contract && contract.requiresHumanDecision) {
      return {
        spec: 'SPEC-147',
        outcome: 'paused',
        progressionStage: deriveProgressionStage(snapshot),
        pause,
        block: null,
        transitions,
        snapshot,
        presentation: formatMissionProgressPresentation(snapshot, {
          progressionStage: deriveProgressionStage(snapshot),
          pause,
          transitions,
        }),
      };
    }

    if (canAutoAdvanceUnderstanding(snapshot)) {
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
          from: PROGRESSION_STAGES.MISSION_PLANNING,
          to: PROGRESSION_STAGES.DISCOVERY_RUNNING,
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
          from: PROGRESSION_STAGES.DISCOVERY_RUNNING,
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
            progressionStage: PROGRESSION_STAGES.DISCOVERY_RUNNING,
            pause: null,
            block: deriveExecutionBlock(after, { message: 'Discovery blocked.' }),
            transitions,
            snapshot: after,
            presentation: formatMissionProgressPresentation(after, {
              progressionStage: PROGRESSION_STAGES.DISCOVERY_RUNNING,
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

    if (canAutoAdvanceMaxPrioritization(snapshot)) {
      try {
        await advanceMaxPrioritization({
          engine,
          mission: engine.get(missionId, tenantId),
          tenantId,
          operatorId,
          allowFixtureFallback,
          ...input,
        });
        transitions.push(createStageTransition({
          from: PROGRESSION_STAGES.OUTREACH_PLANNING,
          to: PROGRESSION_STAGES.OUTREACH_PLANNING,
          trigger: 'Max prioritization committed.',
        }));
        recordStageTransition(engine, missionId, transitions[transitions.length - 1], { tenantId });
        continue;
      } catch (err) {
        lastError = err;
        break;
      }
    }

    if (canAutoAdvanceOutreachToPaige(snapshot)) {
      try {
        const beforeStage = snapshot.mission.stage;
        const result = await advancePaigeVariants({
          engine,
          mission: engine.get(missionId, tenantId),
          tenantId,
          operatorId,
          allowFixtureFallback,
          question: `${AUTONOMOUS_COMMAND} Generate outreach variants.`,
          ...input,
        });
        if (result.executionOutcome === 'blocked') {
          const after = engine.inspect(missionId, { tenantId });
          return {
            spec: 'SPEC-147',
            outcome: 'blocked',
            progressionStage: deriveProgressionStage(after),
            pause: null,
            block: deriveExecutionBlock(after, { message: 'Paige execution blocked.' }),
            transitions,
            snapshot: after,
            presentation: formatMissionProgressPresentation(after, {
              progressionStage: deriveProgressionStage(after),
              block: deriveExecutionBlock(after),
              transitions,
            }),
          };
        }
        const after = engine.inspect(missionId, { tenantId });
        transitions.push(createStageTransition({
          from: beforeStage === STAGES.PREPARE
            ? PROGRESSION_STAGES.OUTREACH_PLANNING
            : PROGRESSION_STAGES.OUTREACH_PLANNING,
          to: PROGRESSION_STAGES.OUTREACH_PLANNING,
          trigger: 'Paige variants committed.',
        }));
        recordStageTransition(engine, missionId, transitions[transitions.length - 1], { tenantId });
        if (after.mission.stage === STAGES.PREPARE) {
          const ctx = specialistContext(after.contributions || []);
          if (ctx.paigeComplete) {
            return {
              spec: 'SPEC-147',
              outcome: 'complete',
              progressionStage: deriveProgressionStage(after),
              pause: null,
              block: null,
              transitions,
              snapshot: after,
              presentation: formatMissionProgressPresentation(after, {
                progressionStage: deriveProgressionStage(after),
                transitions,
              }),
            };
          }
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
  createStageTransition,
  createMissionPause,
  createExecutionBlock,
  deriveProgressionStage,
  deriveMissionPause,
  deriveExecutionBlock,
  recordStageTransition,
  isDiscoveryRunning,
  canAutoAdvanceUnderstanding,
  canAutoAdvanceDiscovery,
  canAutoAdvanceMaxPrioritization,
  canAutoAdvanceOutreachToPaige,
  buildDiscoveryPipelineStatus,
  formatMissionProgressPresentation,
  runAutonomousProgression,
  isAutonomousProgressionCommand,
};
