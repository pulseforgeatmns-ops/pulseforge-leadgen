'use strict';

/**
 * SPEC-152 — Execution State (ADR-073).
 *
 * Event-sourced runtime record of planner execution progress.
 * The current ExecutionState is the latest projection of the immutable event log.
 * Execution State is the single source of truth for planner introspection.
 */

const { randomUUID } = require('crypto');
const { STEP_KINDS, STEP_OWNERS } = require('./MultiIntentTypes');

const EXECUTION_STATUSES = Object.freeze({
  PLANNING: 'planning',
  RUNNING: 'running',
  PAUSED: 'paused',
  BLOCKED: 'blocked',
  COMPLETED: 'completed',
  FAILED: 'failed',
});

const STEP_STATUSES = Object.freeze({
  PENDING: 'pending',
  RUNNING: 'running',
  COMPLETED: 'completed',
  FAILED: 'failed',
  BLOCKED: 'blocked',
});

const EXECUTION_EVENT_KINDS = Object.freeze({
  EXECUTION_PLANNED: 'execution_planned',
  EXECUTION_STARTED: 'execution_started',
  STEP_STARTED: 'step_started',
  STEP_COMPLETED: 'step_completed',
  STEP_FAILED: 'step_failed',
  EXECUTION_PAUSED: 'execution_paused',
  EXECUTION_BLOCKED: 'execution_blocked',
  EXECUTION_COMPLETED: 'execution_completed',
  EXECUTION_FAILED: 'execution_failed',
});

const STEP_LABELS = Object.freeze({
  [STEP_KINDS.APPLY_SESSION_CONFIGURATION]: 'Session Configuration',
  [STEP_KINDS.PERSIST_SESSION]: 'Persist Session',
  [STEP_KINDS.REFRESH_RUNTIME]: 'Runtime Refresh',
  [STEP_KINDS.SESSION_INSPECTION]: 'Session Inspection',
  [STEP_KINDS.MISSION_CREATION]: 'Mission Creation',
  [STEP_KINDS.MISSION_EXECUTION]: 'Mission Execution',
  [STEP_KINDS.BUSINESS_OPERATION]: 'Mission Update',
  [STEP_KINDS.BUSINESS_REASONING]: 'Business Reasoning',
  [STEP_KINDS.REFLECTION]: 'Reflection',
});

function nowIso() {
  return new Date().toISOString();
}

function stepIdFor(step, index) {
  return `step-${index + 1}-${step.kind}`;
}

function describeStepKind(kind) {
  return STEP_LABELS[kind] || String(kind || 'Execution Step').replace(/_/g, ' ');
}

/**
 * @param {object} step
 * @returns {string}
 */
function describeStep(step) {
  if (!step) return 'Unknown step';
  const label = describeStepKind(step.kind);
  if (step.segment && String(step.segment).trim()) {
    const segment = String(step.segment).trim();
    if (segment.length <= 80) return `${label}: ${segment}`;
  }
  return label;
}

/**
 * @param {object} event
 * @returns {object}
 */
function createExecutionEvent(event = {}) {
  return {
    id: event.id || randomUUID(),
    kind: event.kind,
    timestamp: event.timestamp || nowIso(),
    stepId: event.stepId || null,
    description: event.description || '',
    payload: event.payload && typeof event.payload === 'object' ? { ...event.payload } : {},
  };
}

/**
 * @param {object} step
 * @param {number} index
 * @returns {object}
 */
function createExecutionStepRecord(step, index) {
  return {
    id: stepIdFor(step, index),
    owner: step.owner || STEP_OWNERS.WORKSPACE_RUNTIME,
    description: describeStep(step),
    kind: step.kind,
    status: STEP_STATUSES.PENDING,
    startedAt: null,
    completedAt: null,
    result: null,
  };
}

/**
 * Initialize execution state from an execution plan.
 * @param {object} input
 * @returns {object}
 */
function createExecutionState(input = {}) {
  const plan = input.plan || null;
  const steps = Array.isArray(plan?.steps) ? plan.steps : [];
  const executionId = input.executionId || randomUUID();
  const startedAt = nowIso();

  const stepRecords = steps.map((step, index) => createExecutionStepRecord(step, index));

  const events = [
    createExecutionEvent({
      kind: EXECUTION_EVENT_KINDS.EXECUTION_PLANNED,
      description: `Execution plan created with ${steps.length} step(s).`,
      payload: { stepCount: steps.length },
    }),
    createExecutionEvent({
      kind: EXECUTION_EVENT_KINDS.EXECUTION_STARTED,
      description: 'Execution started.',
      payload: { executionId },
    }),
  ];

  return projectExecutionState({
    executionId,
    startedAt,
    plan,
    events,
    stepRecords,
  });
}

/**
 * Project current ExecutionState from the event log.
 * @param {object} input
 * @returns {object}
 */
function projectExecutionState(input = {}) {
  const events = Array.isArray(input.events) ? [...input.events] : [];
  const plan = input.plan || null;
  const steps = Array.isArray(input.stepRecords)
    ? input.stepRecords.map((row) => ({ ...row }))
    : Array.isArray(plan?.steps)
      ? plan.steps.map((step, index) => createExecutionStepRecord(step, index))
      : [];

  let status = EXECUTION_STATUSES.PLANNING;
  let currentStep = null;
  let blockedStep = null;
  let pauseReason = null;
  let nextStep = null;
  let blockingContract = null;
  let lastTransition = null;

  for (const event of events) {
    lastTransition = {
      kind: event.kind,
      timestamp: event.timestamp,
      description: event.description,
      stepId: event.stepId || null,
    };

    const payload = event.payload || {};
    const stepRecord = event.stepId
      ? steps.find((row) => row.id === event.stepId) || null
      : null;

    switch (event.kind) {
      case EXECUTION_EVENT_KINDS.EXECUTION_STARTED:
        status = EXECUTION_STATUSES.RUNNING;
        currentStep = steps.find((row) => row.status === STEP_STATUSES.PENDING) || null;
        break;
      case EXECUTION_EVENT_KINDS.STEP_STARTED:
        if (stepRecord) {
          stepRecord.status = STEP_STATUSES.RUNNING;
          stepRecord.startedAt = event.timestamp;
          currentStep = stepRecord;
        }
        status = EXECUTION_STATUSES.RUNNING;
        break;
      case EXECUTION_EVENT_KINDS.STEP_COMPLETED:
        if (stepRecord) {
          stepRecord.status = STEP_STATUSES.COMPLETED;
          stepRecord.completedAt = event.timestamp;
          stepRecord.result = payload.result || null;
        }
        currentStep = steps.find((row) => row.status === STEP_STATUSES.RUNNING) || null;
        if (!currentStep) {
          currentStep = steps.find((row) => row.status === STEP_STATUSES.PENDING) || null;
        }
        break;
      case EXECUTION_EVENT_KINDS.STEP_FAILED:
        if (stepRecord) {
          stepRecord.status = STEP_STATUSES.FAILED;
          stepRecord.completedAt = event.timestamp;
          stepRecord.result = payload.result || payload.error || null;
        }
        status = EXECUTION_STATUSES.FAILED;
        currentStep = stepRecord;
        break;
      case EXECUTION_EVENT_KINDS.EXECUTION_PAUSED:
        status = EXECUTION_STATUSES.PAUSED;
        pauseReason = payload.pauseReason || event.description || null;
        nextStep = payload.nextStep || null;
        blockingContract = payload.blockingContract || null;
        if (payload.blockedStepId) {
          blockedStep = steps.find((row) => row.id === payload.blockedStepId) || blockedStep;
        }
        break;
      case EXECUTION_EVENT_KINDS.EXECUTION_BLOCKED:
        status = EXECUTION_STATUSES.BLOCKED;
        pauseReason = payload.pauseReason || event.description || null;
        nextStep = payload.nextStep || null;
        blockingContract = payload.blockingContract || null;
        if (stepRecord) {
          stepRecord.status = STEP_STATUSES.BLOCKED;
          blockedStep = stepRecord;
          currentStep = stepRecord;
        } else if (payload.blockedStepId) {
          blockedStep = steps.find((row) => row.id === payload.blockedStepId) || blockedStep;
          currentStep = blockedStep;
        }
        break;
      case EXECUTION_EVENT_KINDS.EXECUTION_COMPLETED:
        status = EXECUTION_STATUSES.COMPLETED;
        currentStep = null;
        nextStep = payload.nextStep || null;
        break;
      case EXECUTION_EVENT_KINDS.EXECUTION_FAILED:
        status = EXECUTION_STATUSES.FAILED;
        pauseReason = payload.pauseReason || event.description || null;
        break;
      default:
        break;
    }
  }

  const completedSteps = steps.filter((row) => row.status === STEP_STATUSES.COMPLETED);
  const pendingSteps = steps.filter(
    (row) => row.status === STEP_STATUSES.PENDING || row.status === STEP_STATUSES.RUNNING
  );

  if (!nextStep && pendingSteps.length > 0) {
    nextStep = describeStep(pendingSteps[0]);
  }

  const updatedAt =
    lastTransition && lastTransition.timestamp ? lastTransition.timestamp : input.startedAt || nowIso();

  return {
    executionId: input.executionId || randomUUID(),
    status,
    currentStep: currentStep ? { ...currentStep } : null,
    completedSteps: completedSteps.map((row) => ({ ...row })),
    pendingSteps: pendingSteps.map((row) => ({ ...row })),
    blockedStep: blockedStep ? { ...blockedStep } : null,
    pauseReason,
    nextStep,
    blockingContract,
    lastTransition,
    startedAt: input.startedAt || updatedAt,
    updatedAt,
    events,
    stepRecords: steps,
    plan: plan || null,
  };
}

/**
 * Append an event and reproject state.
 * @param {object} state
 * @param {object} eventInput
 * @returns {object}
 */
function appendExecutionEvent(state, eventInput = {}) {
  const events = [...(state.events || []), createExecutionEvent(eventInput)];
  return projectExecutionState({
    executionId: state.executionId,
    startedAt: state.startedAt,
    plan: state.plan,
    stepRecords: state.stepRecords,
    events,
  });
}

/**
 * @param {object} state
 * @param {object} step
 * @param {number} index
 * @returns {object}
 */
function recordStepStarted(state, step, index) {
  const stepId = stepIdFor(step, index);
  return appendExecutionEvent(state, {
    kind: EXECUTION_EVENT_KINDS.STEP_STARTED,
    stepId,
    description: `${describeStepKind(step.kind)} started.`,
    payload: { kind: step.kind, owner: step.owner },
  });
}

/**
 * @param {object} state
 * @param {object} step
 * @param {number} index
 * @param {object} [result]
 * @returns {object}
 */
function recordStepCompleted(state, step, index, result = null) {
  const stepId = stepIdFor(step, index);
  return appendExecutionEvent(state, {
    kind: EXECUTION_EVENT_KINDS.STEP_COMPLETED,
    stepId,
    description: `${describeStepKind(step.kind)} completed.`,
    payload: { kind: step.kind, owner: step.owner, result },
  });
}

/**
 * @param {object} state
 * @param {object} input
 * @returns {object}
 */
function recordExecutionPaused(state, input = {}) {
  return appendExecutionEvent(state, {
    kind: EXECUTION_EVENT_KINDS.EXECUTION_PAUSED,
    description: input.pauseReason || 'Execution paused.',
    payload: {
      pauseReason: input.pauseReason || null,
      nextStep: input.nextStep || null,
      blockingContract: input.blockingContract || null,
      blockedStepId: input.blockedStepId || null,
    },
  });
}

/**
 * @param {object} state
 * @param {object} input
 * @returns {object}
 */
function recordExecutionBlocked(state, input = {}) {
  return appendExecutionEvent(state, {
    kind: EXECUTION_EVENT_KINDS.EXECUTION_BLOCKED,
    stepId: input.blockedStepId || null,
    description: input.pauseReason || 'Execution blocked.',
    payload: {
      pauseReason: input.pauseReason || null,
      nextStep: input.nextStep || null,
      blockingContract: input.blockingContract || null,
      blockedStepId: input.blockedStepId || null,
    },
  });
}

/**
 * @param {object} state
 * @param {object} [input]
 * @returns {object}
 */
function recordExecutionCompleted(state, input = {}) {
  return appendExecutionEvent(state, {
    kind: EXECUTION_EVENT_KINDS.EXECUTION_COMPLETED,
    description: input.message || 'Execution completed.',
    payload: {
      nextStep: input.nextStep || null,
    },
  });
}

/**
 * Infer pause after substantive step when autonomous progression did not continue.
 * @param {object} input
 * @returns {object|null}
 */
function inferPostStepPause(input = {}) {
  const { step, stepIndex, stepResult, sessionState, plan } = input;
  if (!step || !stepResult) return null;

  const response = stepResult.response || stepResult;
  const policy = sessionState && sessionState.executionPolicy;

  const missionKinds = new Set([
    STEP_KINDS.MISSION_CREATION,
    STEP_KINDS.MISSION_EXECUTION,
    STEP_KINDS.BUSINESS_OPERATION,
  ]);

  if (!missionKinds.has(step.kind)) return null;

  const autonomousPolicy = policy === 'autonomous';
  const action = response.resolution && response.resolution.action;
  const reason =
    response.reason ||
    (response.resolution && response.resolution.reason) ||
    (response.domainDecision && response.domainDecision.reason) ||
    null;
  const missionUpdated =
    action === 'acquisition_mission_created' ||
    action === 'acquisition_mission_updated' ||
    action === 'acquisition_mission_plan_ready' ||
    action === 'objective_persisted' ||
    reason === 'acquisition_mission_created' ||
    reason === 'acquisition_mission_resumed' ||
    reason === 'acquisition_mission_updated' ||
    reason === 'acquisition_mission_autonomous_progression' ||
    Boolean(response.mission);

  if (!missionUpdated) return null;

  const idx = stepIndex != null ? stepIndex : (plan?.steps || []).indexOf(step);
  const hasLaterSubstantive = (plan?.steps || []).some(
    (row, rowIdx) =>
      rowIdx > idx &&
      row.kind !== STEP_KINDS.PERSIST_SESSION &&
      row.kind !== STEP_KINDS.REFRESH_RUNTIME &&
      row.kind !== STEP_KINDS.APPLY_SESSION_CONFIGURATION
  );

  if (hasLaterSubstantive) return null;

  if (response.metadata && response.metadata.pause) {
    const pause = response.metadata.pause;
    return {
      pauseReason: pause.reason || 'Mission execution paused for operator decision.',
      blockingContract: pause.requiredDecision || pause.stage || 'SPEC-147 Operator Decision',
      nextStep: Array.isArray(pause.availableOptions) && pause.availableOptions.length
        ? pause.availableOptions[0]
        : 'Approve findings',
    };
  }

  if (autonomousPolicy && step.kind === STEP_KINDS.BUSINESS_OPERATION) {
    return {
      pauseReason:
        'Mission update completed. Autonomous progression has not continued because the business operation step finished without issuing an autonomous progression command.',
      blockingContract: 'SPEC-147 Autonomous Mission Progression',
      nextStep: 'Run autonomous progression or approve the next mission stage.',
    };
  }

  if (step.kind === STEP_KINDS.MISSION_EXECUTION && !response.metadata?.autonomousProgression) {
    return {
      pauseReason:
        'Mission execution step completed. Autonomous progression has not continued because operator approval or an explicit progression command is required.',
      blockingContract: 'SPEC-147 Discovery Review',
      nextStep: 'Approve findings or continue mission execution.',
    };
  }

  return null;
}

/**
 * Format event log for operator display.
 * @param {object[]} events
 * @returns {string[]}
 */
function formatExecutionEventLog(events = []) {
  return events.map((event) => {
    const time = event.timestamp ? event.timestamp.slice(11, 19) : '--:--:--';
    return `${time}  ${event.description}`;
  });
}

/**
 * Serialize execution state for API responses (omit internal stepRecords if needed).
 * @param {object} state
 * @returns {object}
 */
function serializeExecutionState(state) {
  if (!state) return null;
  return {
    executionId: state.executionId,
    status: state.status,
    currentStep: state.currentStep,
    completedSteps: state.completedSteps,
    pendingSteps: state.pendingSteps,
    blockedStep: state.blockedStep,
    pauseReason: state.pauseReason,
    nextStep: state.nextStep,
    blockingContract: state.blockingContract,
    lastTransition: state.lastTransition,
    startedAt: state.startedAt,
    updatedAt: state.updatedAt,
    eventLog: formatExecutionEventLog(state.events),
  };
}

function getExecutionState(session) {
  if (!session || !session.context) return null;
  return session.context.executionState || null;
}

function setExecutionState(session, state) {
  if (!session) return null;
  if (!session.context || typeof session.context !== 'object') {
    session.context = {};
  }
  session.context.executionState = state;
  return state;
}

module.exports = {
  EXECUTION_STATUSES,
  STEP_STATUSES,
  EXECUTION_EVENT_KINDS,
  STEP_LABELS,
  describeStepKind,
  describeStep,
  createExecutionEvent,
  createExecutionState,
  projectExecutionState,
  appendExecutionEvent,
  recordStepStarted,
  recordStepCompleted,
  recordExecutionPaused,
  recordExecutionBlocked,
  recordExecutionCompleted,
  inferPostStepPause,
  formatExecutionEventLog,
  serializeExecutionState,
  getExecutionState,
  setExecutionState,
  stepIdFor,
};
