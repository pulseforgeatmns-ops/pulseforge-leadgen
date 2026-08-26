'use strict';

/**
 * Canonical Max prioritization at STAGES.UNDERSTAND.
 * Consumes mission-bound discovery intelligence — never CRM campaign state.
 */

const amo = require('../../acquisition-mission');
const {
  SPECIALISTS,
  CONTRIBUTION_KINDS,
} = amo;
const {
  buildExecutionInput,
  createExecutionResult,
  executeSpecialist,
  EXECUTION_STATUSES,
} = amo;
const { buildMissionExecutionContext } = require('../../acquisition-mission/MissionExecutionContext');
const { maxInput } = require('../../acquisition-mission/SpecialistInputs');

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function buildTimingFromPlan(plan, mission) {
  const campaign = asText(mission && mission.campaign) || asText(plan && plan.campaign);
  if (campaign) return campaign;
  return 'current quarter';
}

function buildPrioritiesFromDiscovery(discoveryPayload, plan) {
  const ranked = Array.isArray(discoveryPayload.rankedProspects)
    ? discoveryPayload.rankedProspects
    : [];
  const opportunities = Array.isArray(discoveryPayload.opportunities)
    ? discoveryPayload.opportunities
    : [];
  const source = ranked.length ? ranked : opportunities;
  const segment = asText(plan && plan.market && (plan.market.segment || plan.market.label))
    || 'target segment';

  return source.slice(0, 5).map((row, index) => ({
    rank: index + 1,
    segment,
    companyId: row.id || row.companyId || null,
    name: row.name || null,
    fit: row.fit != null ? Number(row.fit) : null,
    timing: row.timing != null ? Number(row.timing) : null,
    confidence: row.confidence != null ? Number(row.confidence) : null,
    rationale:
      row.rationale
      || (row.intelligenceBrief && row.intelligenceBrief.summary)
      || null,
  }));
}

function buildObjectivesFromPlan(plan, mission) {
  const success = (plan && (plan.successMetric || plan.success)) || {};
  const objectives = [];
  if (success.metric || success.type) {
    objectives.push({
      text: `Achieve ${success.target != null ? success.target : 1} ${success.metric || success.type}.`,
      metric: success.metric || success.type,
      target: success.target,
    });
  }
  const objective = asText((plan && plan.objective) || (mission && mission.objective));
  if (objective) objectives.push({ text: objective });
  return objectives.length ? objectives : [{ text: 'Advance qualified discovery prospects toward outreach.' }];
}

function buildRecommendationsFromDiscovery(discoveryPayload) {
  const recs = [];
  const mir = discoveryPayload.missionIntelligenceReport || null;
  if (mir && mir.recommendation && mir.recommendation.summary) {
    recs.push(String(mir.recommendation.summary));
  }
  const ranked = Array.isArray(discoveryPayload.rankedProspects)
    ? discoveryPayload.rankedProspects
    : [];
  if (ranked[0] && ranked[0].name) {
    recs.push(`Prioritize ${ranked[0].name} in the first outreach wave.`);
  }
  for (const signal of (discoveryPayload.buyingSignals || []).slice(0, 3)) {
    const label = typeof signal === 'string' ? signal : signal && signal.label;
    if (label) recs.push(`Act on buying signal: ${label}`);
  }
  return recs.length ? recs : ['Proceed with ranked discovery prospects.'];
}

function buildConstraintsFromPlan(plan, discoveryPayload) {
  const fromPlan = ((plan && plan.constraints) || []).map((row) =>
    (typeof row === 'string' ? row : row.label || row.text)
  );
  const fromDiscovery = ((discoveryPayload && discoveryPayload.constraints) || []).map((row) =>
    (typeof row === 'string' ? row : row.label || row.text)
  );
  const merged = [...fromPlan, ...fromDiscovery].filter(Boolean);
  return merged.length ? merged : ['Operator voice'];
}

function buildPrioritizationPayload(mission, discoveryPayload, plan) {
  const mir = discoveryPayload.missionIntelligenceReport || null;
  const priorities = buildPrioritiesFromDiscovery(discoveryPayload, plan);
  const objectives = buildObjectivesFromPlan(plan, mission);
  const recommendations = buildRecommendationsFromDiscovery(discoveryPayload);
  const objectiveReason =
    (mir && mir.recommendation && mir.recommendation.summary)
    || (objectives[0] && objectives[0].text)
    || 'Mission objective aligns with discovered commercial opportunities.';

  return {
    priorities,
    objectives,
    objectiveReason,
    timing: buildTimingFromPlan(plan, mission),
    recommendations,
    constraints: buildConstraintsFromPlan(plan, discoveryPayload),
    delegation: { paige: 'variants', emmett: 'capacity' },
    confidence: discoveryPayload.confidence != null ? discoveryPayload.confidence : null,
    evidence: discoveryPayload.evidence || [],
    buyingSignals: discoveryPayload.buyingSignals || [],
    missionIntelligenceReport: mir,
  };
}

function findLatestScoutDiscovery(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) => row.specialist === SPECIALISTS.SCOUT && row.kind === CONTRIBUTION_KINDS.DISCOVERY
    );
}

async function runMaxPrioritization(executionInput = {}) {
  const specialistInput = executionInput.specialistInput || {};
  const plan = specialistInput.structuredMission;
  const discovery = specialistInput.discovery;
  const transactionId = executionInput.transactionId;

  if (!plan) {
    return createExecutionResult({
      specialist: SPECIALISTS.MAX,
      transactionId,
      status: EXECUTION_STATUSES.BLOCKED,
      reason: 'Structured mission plan is required for Max prioritization.',
      requiredPrecondition: 'structured_mission',
    });
  }
  if (
    !discovery
    || (
      !(discovery.rankedProspects && discovery.rankedProspects.length)
      && !(discovery.companies && discovery.companies.length)
      && !(discovery.opportunities && discovery.opportunities.length)
    )
  ) {
    return createExecutionResult({
      specialist: SPECIALISTS.MAX,
      transactionId,
      status: EXECUTION_STATUSES.BLOCKED,
      reason: 'Scout discovery contribution is required before Max prioritization.',
      requiredPrecondition: 'discovery_contribution',
    });
  }

  const mission = executionInput.mission
    || (executionInput.executionContext && executionInput.executionContext.mission)
    || null;
  const prioritizationPayload = buildPrioritizationPayload(mission, discovery, plan);
  const evidence = (discovery.evidence || discovery.evidenceRefs || []).slice(0, 12);

  return createExecutionResult({
    specialist: SPECIALISTS.MAX,
    transactionId,
    status: EXECUTION_STATUSES.SUCCESS,
    confidence: discovery.confidence != null ? discovery.confidence : 0.72,
    evidence,
    contributions: prioritizationPayload,
    recommendations: prioritizationPayload.recommendations.map((text) => ({
      tier: 'required',
      text,
    })),
    unknowns: [],
    nextActions: [{ kind: 'advance_stage', label: 'Advance toward Plan and Prepare.' }],
  });
}

async function runMaxForAmoMission(mission, opts = {}) {
  if (typeof opts.runMax === 'function') {
    return opts.runMax(mission, opts);
  }

  const tenantId = opts.tenantId != null ? String(opts.tenantId) : String(mission.tenantId || '');
  const contributions = opts.contributions
    || (opts.engine && typeof opts.engine.inspect === 'function'
      ? (opts.engine.inspect(mission.id, { tenantId }).contributions || [])
      : []);

  const executionContext = buildMissionExecutionContext({
    engine: opts.engine,
    mission,
    tenantId,
    transactionId: opts.transactionId,
    pool: opts.pool,
  });
  if (opts.executionRequest) {
    executionContext.executionRequest = opts.executionRequest;
  }

  const discovery = findLatestScoutDiscovery(contributions);
  const input = buildExecutionInput({
    mission,
    specialist: SPECIALISTS.MAX,
    contributions,
    transactionId: opts.transactionId,
    executionContext,
    discovery: discovery && discovery.payload,
    operatorApproval: opts.operatorApproval || null,
    observations: opts.observations || [],
  });

  return executeSpecialist({
    specialist: SPECIALISTS.MAX,
    mission,
    contributions,
    transactionId: opts.transactionId,
    run: () => runMaxPrioritization({ ...input, mission }),
    treatErrorsAsBlocked: opts.treatErrorsAsBlocked !== false,
  });
}

function prioritizationPayloadFromMaxResult(maxResult = {}) {
  if (maxResult.contributions && Object.keys(maxResult.contributions).length) {
    return maxResult.contributions;
  }
  if (maxResult.payload && Object.keys(maxResult.payload).length) {
    return maxResult.payload;
  }
  throw new Error('Max prioritization result is missing contributions.');
}

module.exports = {
  buildPrioritizationPayload,
  runMaxForAmoMission,
  runMaxPrioritization,
  prioritizationPayloadFromMaxResult,
  findLatestScoutDiscovery,
};
