'use strict';

/**
 * SPEC-152 — Execution Inspection Operator (ADR-073).
 *
 * Inspection reads stored Execution State. It never reconstructs planner progress
 * from memory or inference about the current prompt.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const { normalizeText } = require('./SessionState');
const {
  getExecutionState,
  serializeExecutionState,
  describeStepKind,
  EXECUTION_STATUSES,
} = require('./ExecutionState');

const EXECUTION_INSPECTION_RES = [
  /\bwhat are you doing\b/i,
  /\bwhat(?:'s| is) your current step\b/i,
  /\bwhat step are you on\b/i,
  /\bwhere are you in the plan\b/i,
  /\bshow me your execution state\b/i,
  /\bsummarize the execution plan\b/i,
  /\bwhat(?:'s| is) the execution status\b/i,
];

const EXECUTION_PAUSE_EXPLANATION_RES = [
  /\bwhy did you stop\b/i,
  /\bwhy are you paused\b/i,
  /\bwhy didn'?t you continue\b/i,
  /\bwhy is this blocked\b/i,
  /\bwhy didn'?t you continue autonomous execution\b/i,
  /\bwhat are you waiting for\b/i,
  /\bwhat(?:'s| is) blocking you\b/i,
];

const EXECUTION_NEXT_STEP_RES = [
  /\bwhat(?:'s| is) next\b/i,
  /\bwhat happens next\b/i,
  /\bwhat will you do next\b/i,
];

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

/**
 * @param {string} text
 * @returns {boolean}
 */
function isExecutionInspectionQuestion(text) {
  const q = normalizeText(text);
  if (!q) return false;
  return (
    matchesAny(q, EXECUTION_INSPECTION_RES) ||
    matchesAny(q, EXECUTION_PAUSE_EXPLANATION_RES) ||
    matchesAny(q, EXECUTION_NEXT_STEP_RES)
  );
}

/**
 * @param {string} text
 * @returns {boolean}
 */
function isExecutionExplanationQuestion(text) {
  const q = normalizeText(text);
  if (!q) return false;
  return matchesAny(q, EXECUTION_PAUSE_EXPLANATION_RES);
}

function detectInspectionMode(question) {
  const q = normalizeText(question);
  if (matchesAny(q, EXECUTION_PAUSE_EXPLANATION_RES)) return 'pause_explanation';
  if (matchesAny(q, EXECUTION_NEXT_STEP_RES)) return 'next_step';
  if (/\bshow me your execution state\b/i.test(q)) return 'full_state';
  if (/\bsummarize the execution plan\b/i.test(q)) return 'plan_summary';
  if (/\bwhere are you in the plan\b/i.test(q)) return 'plan_position';
  return 'current_activity';
}

function executionStateEvidence(state) {
  return {
    id: 'execution_state',
    summary:
      `status=${state.status}; ` +
      `currentStep=${state.currentStep ? state.currentStep.description : 'none'}; ` +
      `nextStep=${state.nextStep || 'none'}`,
    sourceType: 'execution_state',
  };
}

function formatCurrentActivity(state) {
  const current = state.currentStep
    ? state.currentStep.description
    : state.completedSteps.length
      ? state.completedSteps[state.completedSteps.length - 1].description
      : 'No active step';
  const parts = [
    `Execution Status: ${state.status}.`,
    `Current Step: ${current}.`,
  ];
  if (state.nextStep) {
    parts.push(`Next Step: ${state.nextStep}.`);
  }
  return parts.join(' ');
}

function formatPauseExplanation(state) {
  const parts = [];
  parts.push(`Execution Status: ${state.status}.`);

  if (state.pauseReason) {
    parts.push(`Pause Reason: ${state.pauseReason}`);
  } else if (state.status === EXECUTION_STATUSES.COMPLETED) {
    parts.push('Pause Reason: Execution completed.');
  } else if (state.status === EXECUTION_STATUSES.RUNNING) {
    parts.push('Pause Reason: Execution is still running.');
  } else {
    parts.push('Pause Reason: No pause reason recorded.');
  }

  if (state.blockingContract) {
    parts.push(`Blocking Contract: ${state.blockingContract}`);
  }

  if (state.nextStep) {
    parts.push(`Next Step: ${state.nextStep}`);
  }

  return parts.join('\n\n');
}

function formatNextStep(state) {
  if (state.nextStep) {
    return `Next Step: ${state.nextStep}`;
  }
  if (state.pendingSteps && state.pendingSteps.length) {
    return `Next Step: ${state.pendingSteps[0].description}`;
  }
  if (state.status === EXECUTION_STATUSES.COMPLETED) {
    return 'Next Step: None — execution completed.';
  }
  return 'Next Step: No next step recorded in Execution State.';
}

function formatFullExecutionState(state) {
  const serialized = serializeExecutionState(state);
  const lines = [
    'Execution State',
    '',
    `Status: ${serialized.status}`,
    `Execution ID: ${serialized.executionId}`,
    `Started: ${serialized.startedAt}`,
    `Updated: ${serialized.updatedAt}`,
    '',
  ];

  if (serialized.currentStep) {
    lines.push(`Current Step: ${serialized.currentStep.description}`);
  }

  if (serialized.completedSteps.length) {
    lines.push('', 'Completed Steps:');
    serialized.completedSteps.forEach((step) => {
      lines.push(`  ✓ ${step.description}`);
    });
  }

  if (serialized.pendingSteps.length) {
    lines.push('', 'Pending Steps:');
    serialized.pendingSteps.forEach((step) => {
      lines.push(`  ○ ${step.description}`);
    });
  }

  if (serialized.pauseReason) {
    lines.push('', `Pause Reason: ${serialized.pauseReason}`);
  }
  if (serialized.blockingContract) {
    lines.push(`Blocking Contract: ${serialized.blockingContract}`);
  }
  if (serialized.nextStep) {
    lines.push(`Next Step: ${serialized.nextStep}`);
  }

  if (serialized.eventLog && serialized.eventLog.length) {
    lines.push('', 'Event Log:');
    serialized.eventLog.forEach((row) => lines.push(`  ${row}`));
  }

  return lines.join('\n');
}

function formatPlanSummary(state) {
  const plan = state.plan;
  if (!plan || !Array.isArray(plan.steps) || !plan.steps.length) {
    return 'No execution plan recorded in Execution State.';
  }
  const lines = ['Execution Plan', ''];
  plan.steps.forEach((step, index) => {
    const record =
      (state.stepRecords || []).find((row) => row.kind === step.kind && row.id.includes(String(index + 1))) ||
      (state.completedSteps || []).find((row) => row.kind === step.kind) ||
      (state.pendingSteps || []).find((row) => row.kind === step.kind);
    const status = record ? record.status : 'pending';
    const marker =
      status === 'completed' ? '✓' : status === 'running' ? '→' : status === 'blocked' ? '✗' : '○';
    lines.push(`${marker} ${describeStepKind(step.kind)}`);
  });
  return lines.join('\n');
}

function formatPlanPosition(state) {
  const completed = state.completedSteps ? state.completedSteps.length : 0;
  const total = state.stepRecords ? state.stepRecords.length : 0;
  const current = state.currentStep
    ? state.currentStep.description
    : state.status === EXECUTION_STATUSES.COMPLETED
      ? 'Complete'
      : 'Unknown';
  return [
    `Completed ${completed} of ${total} step(s).`,
    `Current position: ${current}.`,
    state.nextStep ? `Next: ${state.nextStep}.` : '',
  ]
    .filter(Boolean)
    .join(' ');
}

/**
 * Read-only inspection of stored Execution State.
 * @param {object} input
 * @returns {{ handled: boolean, prose: string, structured: object, reason: string, executionState: object|null }}
 */
function inspectExecutionState(input = {}) {
  const state = input.executionState || getExecutionState(input.session);

  if (!state) {
    const prose =
      'No Execution State is recorded for this session. Execution State is created when the planner runs a multi-step execution plan.';
    return {
      handled: true,
      prose,
      structured: buildStructuredResponse({
        answer: prose,
        reasoning: [
          'SPEC-152 — EXECUTION_INSPECTION requires stored Execution State; no inference from the current prompt.',
        ],
        confidence: 1,
        recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
        confidenceContributors: ['spec_152', 'execution_state'],
        metadata: {
          executionInspection: true,
          executionStateRead: false,
        },
      }),
      reason: 'execution_state_missing',
      executionState: null,
    };
  }

  const mode = detectInspectionMode(input.question || '');
  let prose;
  let reason;

  switch (mode) {
    case 'pause_explanation':
      prose = formatPauseExplanation(state);
      reason = 'execution_pause_explained';
      break;
    case 'next_step':
      prose = formatNextStep(state);
      reason = 'execution_next_step';
      break;
    case 'full_state':
      prose = formatFullExecutionState(state);
      reason = 'execution_state_inspected';
      break;
    case 'plan_summary':
      prose = formatPlanSummary(state);
      reason = 'execution_plan_summarized';
      break;
    case 'plan_position':
      prose = formatPlanPosition(state);
      reason = 'execution_plan_position';
      break;
    default:
      prose = formatCurrentActivity(state);
      reason = 'execution_activity_inspected';
      break;
  }

  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: [
      mode === 'pause_explanation'
        ? 'SPEC-152 — Execution explanation reads stored pause reason and blocking contract; no inference.'
        : 'SPEC-152 — EXECUTION_INSPECTION reads stored Execution State; no business reasoning.',
    ],
    supportingEvidence: [executionStateEvidence(state)],
    confidence: 1,
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: ['spec_152', 'execution_state'],
    metadata: {
      executionInspection: true,
      executionStateRead: true,
      executionState: serializeExecutionState(state),
      inspectionMode: mode,
    },
  });

  return {
    handled: true,
    prose,
    structured,
    reason,
    executionState: state,
  };
}

module.exports = {
  EXECUTION_INSPECTION_RES,
  EXECUTION_PAUSE_EXPLANATION_RES,
  EXECUTION_NEXT_STEP_RES,
  isExecutionInspectionQuestion,
  isExecutionExplanationQuestion,
  detectInspectionMode,
  inspectExecutionState,
  formatCurrentActivity,
  formatPauseExplanation,
  formatNextStep,
  formatFullExecutionState,
};
