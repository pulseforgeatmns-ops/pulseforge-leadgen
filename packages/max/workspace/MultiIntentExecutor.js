'use strict';

/**
 * SPEC-151 — Multi-Intent Execution Plan executor (ADR-072).
 */

const askPathTrace = require('./audit/AskPathTrace');
const { attachRoutingTrace } = require('./SubjectRoutingTrace');
const { STEP_KINDS, STEP_OWNERS } = require('./MultiIntentTypes');
const { EXECUTION_POLICIES, REASONING_MODES } = require('./SessionState');
const { formatReasoningModeLabel } = require('./SessionStateManager');
const { buildStructuredResponse } = require('./WorkspaceTypes');
const { EXECUTION_DOMAINS } = require('./ExecutionDomain');
const { ROUTE_KINDS } = require('../../mission-engine');

const INTERNAL_STEP_KINDS = new Set([
  STEP_KINDS.APPLY_SESSION_CONFIGURATION,
  STEP_KINDS.PERSIST_SESSION,
  STEP_KINDS.REFRESH_RUNTIME,
]);

/**
 * @param {object} step
 * @param {object} sessionState
 * @returns {object}
 */
function buildBlockedStepResponse(step, sessionState, session, ctx = {}) {
  const policyLabel =
    sessionState.executionPolicy === EXECUTION_POLICIES.EXECUTION_DISABLED
      ? 'disabled'
      : 'read-only';
  const prose = [
    'Session settings were applied, but this step did not run.',
    `Execution is ${policyLabel} for this session, so I did not ${describeBlockedAction(step)}.`,
    'You can change the execution policy or approve the step explicitly when you are ready.',
  ].join(' ');

  const structured = buildStructuredResponse({
    headline: 'Execution blocked by session policy',
    prose,
    reasoning: [
      'SPEC-151 — MIEP respected session execution policy before mission runtime.',
      `Blocked step: ${step.kind}.`,
    ],
    recommendedActions: [
      {
        label: 'Enable execution',
        action: 'session_configuration',
        payload: { executionPolicy: EXECUTION_POLICIES.NORMAL },
      },
    ],
  });

  return {
    sessionId: session.id,
    prose,
    structured,
    metadata: {
      miep: true,
      miepBlocked: true,
      miepStep: step,
      sessionState,
      messageClassification: ctx.messageClassification || null,
    },
    suggestions: [],
    recommendedActions: structured.recommendedActions,
    contextSwitch: ctx.envelopeSwitch || null,
    domainSwitch: null,
    context: session.context,
    route: ROUTE_KINDS.INTELLIGENCE,
    mission: null,
    resolution: {
      action: 'miep_blocked',
      reason: `execution_policy_${policyLabel}`,
    },
    executionDomain: EXECUTION_DOMAINS.WORKSPACE,
    sessionState,
    workspaceOwnership: {
      owner: step.owner || STEP_OWNERS.MISSION_RUNTIME,
      reason: 'miep_execution_blocked',
      confidence: 0.95,
      specialist: null,
      fallback: false,
    },
  };
}

function describeBlockedAction(step) {
  switch (step.kind) {
    case STEP_KINDS.MISSION_CREATION:
      return 'create or execute the mission';
    case STEP_KINDS.MISSION_EXECUTION:
      return 'continue mission execution';
    case STEP_KINDS.BUSINESS_OPERATION:
      return 'start the business operation';
    default:
      return 'run the requested execution step';
  }
}

/**
 * @param {object|null} response
 * @param {object} meta
 * @returns {object|null}
 */
function enrichMultiIntentResponse(response, meta = {}) {
  if (!response || typeof response !== 'object') return response;
  const executionPlan = {
    steps: (meta.stepResults || []).map((row) => ({
      kind: row.step.kind,
      owner: row.step.owner,
      order: row.step.order,
      completed: row.completed,
      blocked: row.blocked === true,
    })),
    blocked: meta.blocked === true,
    pauseReason: meta.pauseReason || null,
  };

  return {
    ...response,
    metadata: {
      ...(response.metadata || {}),
      miep: true,
      executionPlan,
      sessionState: meta.sessionState || response.sessionState || null,
    },
    executionPlan,
    miep: {
      plan: meta.plan || null,
      stepResults: meta.stepResults || [],
      blocked: meta.blocked === true,
      pauseReason: meta.pauseReason || null,
    },
  };
}

/**
 * Execute a multi-intent plan sequentially.
 *
 * @param {import('./WorkspaceEngine').WorkspaceEngine} engine
 * @param {object} ctx
 * @returns {Promise<object>}
 */
async function executeMultiIntentPlan(engine, ctx) {
  askPathTrace.traceEnter('executeMultiIntentPlan');
  const {
    plan,
    session,
    sessionState,
    input,
    envelopeSwitch,
    messageClassification,
  } = ctx;

  /** @type {Array<{ step: object, completed: boolean, blocked: boolean, response?: object }>} */
  const stepResults = [];
  let lastResponse = null;
  let blocked = false;
  let pauseReason = null;

  const substantiveSteps = plan.steps.filter((step) => !INTERNAL_STEP_KINDS.has(step.kind));
  const lastSubstantive = substantiveSteps[substantiveSteps.length - 1] || null;

  for (const step of plan.steps) {
    if (INTERNAL_STEP_KINDS.has(step.kind)) {
      stepResults.push({ step, completed: true, blocked: false });
      continue;
    }

    if (step.blocking) {
      blocked = true;
      pauseReason = 'execution_disabled_by_session';
      const blockedResponse = buildBlockedStepResponse(step, sessionState, session, ctx);
      stepResults.push({
        step,
        completed: false,
        blocked: true,
        response: blockedResponse,
      });
      lastResponse = blockedResponse;
      askPathTrace.traceBranch('miep_blocked', {
        kind: step.kind,
        reason: pauseReason,
      });
      break;
    }

    askPathTrace.traceBranch('miep_execute_step', {
      kind: step.kind,
      owner: step.owner,
      segment: step.segment,
    });

    const segmentResult = await engine.ask({
      ...input,
      question: step.segment,
      sessionId: session.id,
      context: session.context,
      _miepInternal: true,
      _miepFinalStep: lastSubstantive && lastSubstantive.order === step.order,
    });

    stepResults.push({
      step,
      completed: true,
      blocked: false,
      response: segmentResult,
    });
    lastResponse = segmentResult;

    if (segmentResult && segmentResult.miep && segmentResult.miep.blocked) {
      blocked = true;
      pauseReason = segmentResult.miep.pauseReason || 'step_blocked';
      break;
    }
  }

  if (lastResponse && lastSubstantive && !lastResponse.metadata?.miepBlocked) {
    const sessionConfigAck =
      plan.steps.some((row) => row.kind === STEP_KINDS.APPLY_SESSION_CONFIGURATION) &&
      lastSubstantive.kind === STEP_KINDS.SESSION_INSPECTION;

    if (sessionConfigAck && sessionState.reasoningMode === REASONING_MODES.TEACHING) {
      const teachingLabel = formatReasoningModeLabel(REASONING_MODES.TEACHING);
      if (lastResponse.prose && !lastResponse.prose.includes(teachingLabel)) {
        lastResponse = {
          ...lastResponse,
          prose: `${lastResponse.prose} Reasoning mode is ${teachingLabel}.`,
        };
      }
    }
  }

  const enriched = enrichMultiIntentResponse(lastResponse, {
    plan,
    stepResults,
    sessionState,
    blocked,
    pauseReason,
  });

  askPathTrace.traceEarlyReturn('executeMultiIntentPlan', blocked ? 'blocked' : 'complete');
  return attachRoutingTrace(enriched, {
    pipeline: 'MultiIntentExecutionPlanner',
    claimedBy: 'intent_extractor',
    messageClassification,
    sessionState,
  });
}

module.exports = {
  executeMultiIntentPlan,
  enrichMultiIntentResponse,
  buildBlockedStepResponse,
  INTERNAL_STEP_KINDS,
};
