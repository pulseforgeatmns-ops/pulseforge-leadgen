'use strict';

/**
 * SPEC-151 — Multi-Intent Execution Planner (ADR-072).
 *
 * Builds a deterministic ordered execution plan from detected intents.
 * Dependencies are respected; segment order breaks ties.
 *
 * ADR-072 invariant: execution plans are additive, not competitive — intents
 * coexist unless mutually exclusive; the planner only forces a choice on conflict.
 */

const askPathTrace = require('./audit/AskPathTrace');
const {
  INTENT_TYPES,
  STEP_KINDS,
  INTENT_PRECEDENCE,
  INTENT_DEPENDENCIES,
  mapIntentToOwner,
} = require('./MultiIntentTypes');
const { sessionStateBlocksExecution } = require('./SessionState');

/**
 * Order intents by message segment order, moving producers before dependents when required.
 * @param {import('./MultiIntentTypes').DetectedIntent[]} intents
 * @returns {import('./MultiIntentTypes').DetectedIntent[]}
 */
function orderIntents(intents) {
  const sorted = [...intents].sort((a, b) => a.segmentIndex - b.segmentIndex);
  let changed = true;

  while (changed) {
    changed = false;
    const produced = new Set();

    for (let i = 0; i < sorted.length; i += 1) {
      const intent = sorted[i];
      const requires =
        intent.requires || INTENT_DEPENDENCIES[intent.type]?.requires || [];
      const unmet = requires.filter((key) => !produced.has(key));

      if (unmet.length) {
        const producerIdx = sorted.findIndex((row, idx) => {
          if (idx >= i) return false;
          const produces =
            row.produces || INTENT_DEPENDENCIES[row.type]?.produces || [];
          return produces.some((key) => unmet.includes(key));
        });

        if (producerIdx < 0) {
          const laterProducerIdx = sorted.findIndex((row, idx) => {
            if (idx <= i) return false;
            const produces =
              row.produces || INTENT_DEPENDENCIES[row.type]?.produces || [];
            return produces.some((key) => unmet.includes(key));
          });
          if (laterProducerIdx >= 0) {
            const [producer] = sorted.splice(laterProducerIdx, 1);
            sorted.splice(i, 0, producer);
            changed = true;
            break;
          }
        }
      }

      const produces =
        intent.produces || INTENT_DEPENDENCIES[intent.type]?.produces || [];
      produces.forEach((key) => produced.add(key));
    }
  }

  return sorted;
}

function mapIntentToStepKind(type) {
  switch (type) {
    case INTENT_TYPES.SESSION_CONFIGURATION:
      return STEP_KINDS.APPLY_SESSION_CONFIGURATION;
    case INTENT_TYPES.INSPECTION:
      return STEP_KINDS.SESSION_INSPECTION;
    case INTENT_TYPES.SYSTEM_CONFIGURATION:
      return STEP_KINDS.REFRESH_RUNTIME;
    case INTENT_TYPES.MISSION_CREATION:
      return STEP_KINDS.MISSION_CREATION;
    case INTENT_TYPES.MISSION_EXECUTION:
      return STEP_KINDS.MISSION_EXECUTION;
    case INTENT_TYPES.BUSINESS_OPERATION:
      return STEP_KINDS.BUSINESS_OPERATION;
    case INTENT_TYPES.BUSINESS_REASONING:
      return STEP_KINDS.BUSINESS_REASONING;
    case INTENT_TYPES.REFLECTION:
      return STEP_KINDS.REFLECTION;
    default:
      return STEP_KINDS.BUSINESS_REASONING;
  }
}

/**
 * Expand session configuration into apply → persist → refresh substeps.
 * @param {import('./MultiIntentTypes').ExecutionPlanStep[]} steps
 * @param {import('./MultiIntentTypes').DetectedIntent} intent
 * @param {number} baseOrder
 */
function expandSessionConfigurationSteps(steps, intent, baseOrder) {
  steps.push({
    kind: STEP_KINDS.APPLY_SESSION_CONFIGURATION,
    intentType: intent.type,
    owner: mapIntentToOwner(intent.type),
    segment: intent.segment,
    order: baseOrder,
    requires: [],
    produces: ['session_state'],
  });
  steps.push({
    kind: STEP_KINDS.PERSIST_SESSION,
    intentType: intent.type,
    owner: mapIntentToOwner(intent.type),
    segment: intent.segment,
    order: baseOrder + 0.1,
    requires: ['session_state'],
    produces: ['session_state_persisted'],
  });
  steps.push({
    kind: STEP_KINDS.REFRESH_RUNTIME,
    intentType: intent.type,
    owner: mapIntentToOwner(intent.type),
    segment: intent.segment,
    order: baseOrder + 0.2,
    requires: ['session_state_persisted'],
    produces: ['runtime_context'],
  });
}

/**
 * @param {object} input
 * @param {import('./MultiIntentTypes').DetectedIntent[]} input.intents
 * @param {object} [input.sessionState]
 * @returns {import('./MultiIntentTypes').ExecutionPlan}
 */
function buildExecutionPlan(input = {}) {
  askPathTrace.traceEnter('buildExecutionPlan');
  const intents = Array.isArray(input.intents) ? input.intents : [];
  const ordered = orderIntents(intents);
  /** @type {import('./MultiIntentTypes').ExecutionPlanStep[]} */
  const steps = [];
  let order = 1;

  for (const intent of ordered) {
    if (intent.type === INTENT_TYPES.SESSION_CONFIGURATION) {
      expandSessionConfigurationSteps(steps, intent, order);
      order += 1;
      continue;
    }

    const kind = mapIntentToStepKind(intent.type);
    const deps = INTENT_DEPENDENCIES[intent.type] || { requires: [], produces: [] };
    steps.push({
      kind,
      intentType: intent.type,
      owner: intent.owner || mapIntentToOwner(intent.type),
      segment: intent.segment,
      order,
      requires: [...deps.requires],
      produces: [...deps.produces],
      blocking:
        sessionStateBlocksExecution(input.sessionState) &&
        (kind === STEP_KINDS.MISSION_CREATION ||
          kind === STEP_KINDS.MISSION_EXECUTION ||
          kind === STEP_KINDS.BUSINESS_OPERATION),
    });
    order += 1;
  }

  const plan = {
    steps,
    intents: ordered,
    compound: intents.length > 1,
  };

  askPathTrace.traceBranch('execution_plan', {
    stepCount: steps.length,
    steps: steps.map((row) => ({
      kind: row.kind,
      owner: row.owner,
      order: row.order,
      blocking: row.blocking === true,
    })),
  });
  askPathTrace.traceEarlyReturn('buildExecutionPlan', steps.length);
  return plan;
}

module.exports = {
  orderIntents,
  buildExecutionPlan,
  mapIntentToStepKind,
};
