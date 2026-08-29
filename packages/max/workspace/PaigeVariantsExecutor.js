'use strict';

/**
 * Canonical Paige variant generation at STAGES.PREPARE.
 * Consumes Max prioritization and Scout intelligence — never recipient selection.
 */

const amo = require('../../acquisition-mission');
const {
  SPECIALISTS,
  buildExecutionInput,
  createExecutionResult,
  executeSpecialist,
  EXECUTION_STATUSES,
} = amo;
const {
  evaluatePaigePriorLearningInfluence,
  applyPaigePriorLearningAdjustments,
} = require('./PaigePriorLearningInfluence');

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function buildBasePaigeVariantsPayload(input = {}) {
  const max = input.max || {};
  const scout = input.scout || {};
  const plan = input.plan || {};
  const topTarget = max.rankedTargets?.[0]?.name
    || max.priorities?.[0]?.name
    || scout.companies?.[0]?.name
    || scout.rankedProspects?.[0]?.name
    || plan.market?.label
    || 'your office';
  const objective = max.objectives?.[0]?.text || plan.objective || null;
  const subject = `Commercial cleaning walkthrough for ${topTarget}`;
  const body = [
    `Hi — we help ${plan.market?.label || 'local offices'} maintain spotless workspaces.`,
    objective ? `Mission focus: ${objective}` : null,
    max.recommendations?.[0] ? `Why now: ${max.recommendations[0]}` : null,
  ].filter(Boolean).join('\n\n');

  return {
    variants: [{
      label: 'Primary',
      subject,
      body,
      cta: 'Reply to schedule a walkthrough',
    }],
    subjects: [subject],
    cta: 'Reply to schedule a walkthrough',
    hypotheses: [
      max.objectiveReason || 'Prioritized targets respond to timing-specific outreach.',
      scout.buyingSignals?.[0]
        ? `Signal: ${typeof scout.buyingSignals[0] === 'string'
          ? scout.buyingSignals[0]
          : scout.buyingSignals[0].label}`
        : 'Ops hiring signals indicate receptivity window.',
    ].filter(Boolean),
    experiments: [{
      name: 'subject_personalization',
      variant: 'company_name_in_subject',
      hypothesis: 'Company-specific subject lines increase open rates.',
    }],
    messaging: body,
  };
}

function extractPaigeUpstreamContext(executionInput = {}) {
  const max = executionInput.workspaceContext?.max
    || executionInput.specialistInput?.maxPrioritization
    || {};
  const scout = executionInput.workspaceContext?.scout
    || executionInput.specialistInput?.scoutDiscovery
    || {};
  const plan = executionInput.missionPlan
    || executionInput.specialistInput?.structuredMission
    || {};
  return { max, scout, plan };
}

function buildPaigeVariantsPayload(executionInput = {}) {
  const { max, scout, plan } = extractPaigeUpstreamContext(executionInput);
  const priorLearning = executionInput.memoryContext?.priorLearning || [];

  let payload = buildBasePaigeVariantsPayload({ max, scout, plan });

  const priorLearningEvaluation = evaluatePaigePriorLearningInfluence({
    priorLearning,
    max,
    scout,
    plan,
    channel: 'email',
  });

  payload = applyPaigePriorLearningAdjustments(payload, priorLearningEvaluation, plan);

  return {
    payload,
    learningInfluence: priorLearningEvaluation.learningInfluence || [],
  };
}

async function runPaigeVariants(executionInput = {}) {
  const transactionId = executionInput.transactionId;
  const { max, scout, plan } = extractPaigeUpstreamContext(executionInput);

  if (!max || !Object.keys(max).length) {
    return createExecutionResult({
      specialist: SPECIALISTS.PAIGE,
      transactionId,
      status: EXECUTION_STATUSES.BLOCKED,
      reason: 'Max prioritization is required before Paige variant generation.',
      requiredPrecondition: 'max_prioritization',
    });
  }

  const { payload, learningInfluence } = buildPaigeVariantsPayload(executionInput);
  const unknowns = [];
  if (executionInput.memoryContext?.priorLearningRetrievalWarning) {
    unknowns.push({
      unknown: 'Prior OutcomeLearning retrieval',
      reason: executionInput.memoryContext.priorLearningRetrievalWarning,
    });
  }

  return createExecutionResult({
    specialist: SPECIALISTS.PAIGE,
    transactionId,
    status: EXECUTION_STATUSES.SUCCESS,
    confidence: { overall: 0.75, evidence: 0.7, fit: 0.8, completeness: 0.75 },
    evidence: [{
      id: 'ev_paige_0',
      label: 'Max prioritization consumed for messaging',
      source: 'max_prioritization',
      timestamp: new Date().toISOString(),
      provenance: { kind: 'upstream_intelligence', source: 'max' },
    }],
    contributions: payload,
    recommendations: [{ tier: 'suggested', text: 'Review variants before operator approval.' }],
    unknowns,
    nextActions: [{ kind: 'operator_review', label: 'Operator review variants' }],
    learningInfluence,
  });
}

async function runPaigeForAmoMission(mission, opts = {}) {
  if (typeof opts.runPaige === 'function') {
    return opts.runPaige(mission, opts);
  }

  const contributions = opts.contributions
    || (opts.engine && opts.engine.inspect(mission.id, { tenantId: opts.tenantId }).contributions)
    || [];

  const executionInput = buildExecutionInput({
    mission,
    contributions,
    specialist: SPECIALISTS.PAIGE,
    transactionId: opts.transactionId,
    executionContext: opts.executionContext,
    store: opts.engine?.store,
  });

  const result = await executeSpecialist({
    specialist: SPECIALISTS.PAIGE,
    mission,
    contributions,
    transactionId: opts.transactionId,
    store: opts.engine?.store,
    run: () => runPaigeVariants({
      ...executionInput,
      mission,
    }),
    treatErrorsAsBlocked: opts.treatErrorsAsBlocked !== false,
  });

  if (result.status === EXECUTION_STATUSES.BLOCKED || result.status === EXECUTION_STATUSES.FAILED) {
    const reason =
      (result.blocked && result.blocked.reason)
      || result.reason
      || 'Paige variant generation did not complete.';
    const err = new Error(reason);
    err.code = 'tme_paige_blocked';
    throw err;
  }

  return result.contributions;
}

function fixturePaigeVariantsResult(mission, contributions = []) {
  const input = buildExecutionInput({
    mission,
    contributions,
    specialist: SPECIALISTS.PAIGE,
    transactionId: 'fixture_paige',
  });
  return buildPaigeVariantsPayload(input).payload;
}

module.exports = {
  buildBasePaigeVariantsPayload,
  buildPaigeVariantsPayload,
  runPaigeVariants,
  runPaigeForAmoMission,
  fixturePaigeVariantsResult,
  extractPaigeUpstreamContext,
};
