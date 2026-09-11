'use strict';

/**
 * SPEC-249 — Canonical Penny V1.
 * Mission-aware paid acquisition intelligence. Read, reason, recommend only.
 */

const amo = require('../../acquisition-mission');
const {
  ACQUISITION_APPROACHES,
  CONTRIBUTION_KINDS,
  SPECIALISTS,
  EXECUTION_STATUSES,
  createExecutionResult,
  buildExecutionInput,
  executeSpecialist,
} = amo;

const VIABILITY = Object.freeze({
  RECOMMEND_PAID: 'RECOMMEND_PAID',
  RECOMMEND_NO_PAID: 'RECOMMEND_NO_PAID',
  DEFER: 'DEFER',
  BLOCKED: 'BLOCKED',
});

const CHANNEL_FIT = Object.freeze({
  STRONG: 'strong',
  MODERATE: 'moderate',
  WEAK: 'weak',
  UNKNOWN: 'unknown',
});

const DEFAULT_PAID_CHANNELS = Object.freeze([
  'Google Search',
  'ChatGPT Ads',
  'Yelp',
  'Meta',
]);

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function clone(value) {
  return JSON.parse(JSON.stringify(value == null ? null : value));
}

function nowIso() {
  return new Date().toISOString();
}

function array(value) {
  return Array.isArray(value) ? value : [];
}

function lowerBag(...values) {
  return values.map((value) => {
    if (value == null) return '';
    if (typeof value === 'string') return value;
    return JSON.stringify(value);
  }).join(' ').toLowerCase();
}

function normalizedChannelName(value) {
  const text = asText(value.name || value.channel || value.id || value.label || value);
  if (!text) return '';
  if (/google|search/.test(text.toLowerCase())) return 'Google Search';
  if (/chatgpt|openai/.test(text.toLowerCase())) return 'ChatGPT Ads';
  if (/yelp/.test(text.toLowerCase())) return 'Yelp';
  if (/meta|facebook|instagram/.test(text.toLowerCase())) return 'Meta';
  return text;
}

function normalizeBudget(input = {}) {
  const source = input.availableBudget || input.budget || null;
  if (!source) return { known: false, amount: null, currency: null, source: null };
  if (typeof source === 'number') {
    return { known: Number.isFinite(source), amount: source, currency: 'USD', source: 'operator_budget' };
  }
  const amount = source.amount != null ? Number(source.amount)
    : source.max != null ? Number(source.max)
      : source.testBudget != null ? Number(source.testBudget)
        : null;
  return {
    known: Number.isFinite(amount),
    amount: Number.isFinite(amount) ? amount : null,
    currency: asText(source.currency) || 'USD',
    source: asText(source.source) || 'operator_budget',
  };
}

function normalizeReadiness(value, fallbackSource) {
  if (!value) {
    return { known: false, ready: null, source: fallbackSource, evidence: [] };
  }
  if (typeof value === 'boolean') {
    return { known: true, ready: value, source: fallbackSource, evidence: [] };
  }
  const status = asText(value.status || value.state || value.readiness).toLowerCase();
  let ready = null;
  if (value.ready === true || /ready|configured|live|complete/.test(status)) ready = true;
  if (value.ready === false || /missing|absent|broken|blocked|not_ready|inadequate/.test(status)) ready = false;
  return {
    known: ready != null || Boolean(status),
    ready,
    source: asText(value.source) || fallbackSource,
    evidence: array(value.evidence || value.evidenceRefs).slice(),
    details: clone(value),
  };
}

function evidenceItem(id, label, source, confidence = 0.6, kind = 'observed') {
  return {
    id,
    label,
    source,
    confidence,
    timestamp: nowIso(),
    provenance: { kind, source },
  };
}

function collectBaseEvidence(input = {}, budget, conversion, measurement) {
  const evidence = [];
  const specialistInput = input.specialistInput || {};
  if (specialistInput.objective) {
    evidence.push(evidenceItem(
      'ev_penny_objective',
      'Mission objective supplied to Penny',
      'structured_mission',
      0.9,
      'fact'
    ));
  }
  if (specialistInput.acquisitionApproach?.selectedApproach) {
    evidence.push(evidenceItem(
      'ev_penny_approach',
      `Max selected ${specialistInput.acquisitionApproach.selectedApproach} acquisition approach`,
      'max_acquisition_approach',
      0.85,
      'upstream_decision'
    ));
  }
  if (budget.known) {
    evidence.push(evidenceItem(
      'ev_penny_budget',
      `Operator supplied paid test budget (${budget.currency} ${budget.amount})`,
      budget.source,
      0.8,
      'operator_constraint'
    ));
  }
  if (conversion.known) {
    evidence.push(evidenceItem(
      'ev_penny_conversion_readiness',
      conversion.ready ? 'Conversion path is reported ready' : 'Conversion path is reported inadequate',
      conversion.source,
      conversion.ready ? 0.75 : 0.85,
      'readiness_evidence'
    ));
  }
  if (measurement.known) {
    evidence.push(evidenceItem(
      'ev_penny_measurement_readiness',
      measurement.ready ? 'Measurement path is reported ready' : 'Measurement path is reported inadequate',
      measurement.source,
      measurement.ready ? 0.75 : 0.85,
      'readiness_evidence'
    ));
  }
  for (const [index, row] of array(specialistInput.evidence).entries()) {
    evidence.push(evidenceItem(
      `ev_penny_upstream_${index}`,
      asText(row.label || row.text || row.reason || row.source) || 'Upstream acquisition evidence',
      asText(row.source || row.sourceKind) || 'upstream_acquisition_evidence',
      row.confidence != null ? Number(row.confidence) : 0.6,
      asText(row.kind || row.sourceKind) || 'upstream_evidence'
    ));
  }
  return evidence;
}

function inferCandidateChannels(input = {}) {
  const specialistInput = input.specialistInput || {};
  const explicit = array(specialistInput.candidatePaidChannels)
    .map((row) => (typeof row === 'string' ? { name: row } : row))
    .filter(Boolean);
  const byName = new Map();
  for (const row of explicit) {
    const name = normalizedChannelName(row);
    if (name) byName.set(name, { ...row, name });
  }
  for (const row of array(specialistInput.platformEvidence)) {
    const name = normalizedChannelName(row);
    if (name && !byName.has(name)) byName.set(name, { name, platformEvidence: [row] });
    else if (name) byName.get(name).platformEvidence = [...array(byName.get(name).platformEvidence), row];
  }
  if (!byName.size) {
    DEFAULT_PAID_CHANNELS.forEach((name) => byName.set(name, { name }));
  }
  return [...byName.values()];
}

function scoreChannel(channel = {}, context = {}) {
  const name = normalizedChannelName(channel);
  const bag = lowerBag(name, channel, context.objective, context.market, context.buyer);
  let score = 0.35;
  const reasons = [];
  const risks = [];
  const unknowns = [];

  if (/search|google|yelp/.test(bag)) {
    score += 0.25;
    reasons.push('Captures declared buyer intent instead of relying only on interruption.');
  }
  if (/commercial|property|facility|office|law|clean/.test(bag) && /search|google|yelp/.test(bag)) {
    score += 0.15;
    reasons.push('Local service replacement intent can be expressed through search/review discovery.');
  }
  if (/meta|facebook|instagram/.test(bag)) {
    score -= 0.05;
    risks.push('Interruptive social traffic may be weaker for urgent replacement-service intent.');
  }
  if (/chatgpt|openai/.test(bag)) {
    score += 0.05;
    unknowns.push('Channel inventory, targeting, and conversion mechanics are not evidenced here.');
  }
  if (channel.fit) {
    const fit = asText(channel.fit).toLowerCase();
    if (/strong|high/.test(fit)) score += 0.2;
    if (/weak|poor|low/.test(fit)) score -= 0.2;
  }
  if (channel.ready === false || /blocked|missing|not_ready/.test(asText(channel.readiness || channel.status).toLowerCase())) {
    score -= 0.25;
    risks.push('Channel readiness is not established.');
  }
  if (!array(channel.evidence).length && !array(channel.platformEvidence).length) {
    unknowns.push('No channel-specific performance or access evidence supplied.');
  }

  const bounded = Math.max(0, Math.min(1, Math.round(score * 100) / 100));
  const fit = bounded >= 0.7 ? CHANNEL_FIT.STRONG
    : bounded >= 0.5 ? CHANNEL_FIT.MODERATE
      : bounded >= 0.3 ? CHANNEL_FIT.WEAK
        : CHANNEL_FIT.UNKNOWN;
  return {
    channel: name,
    fit,
    confidence: bounded,
    rationale: reasons.length ? reasons.join(' ') : 'Channel can be reasoned about, but supplied evidence is limited.',
    evidence: [
      ...array(channel.evidence),
      ...array(channel.platformEvidence),
    ],
    buyerIntentHypothesis: /search|google|yelp|chatgpt/.test(bag)
      ? 'High-intent buyers may reveal replacement need through explicit search or answer-seeking behavior.'
      : 'Audience targeting would need to find likely buyers before active intent is visible.',
    economicConsiderations: channel.economics || 'Paid viability depends on qualified opportunity progression, CAC/payback, and recurring client value.',
    readiness: channel.ready === false ? 'blocked'
      : channel.ready === true ? 'ready'
        : asText(channel.readiness || channel.status) || 'unknown',
    risks,
    blockers: array(channel.blockers),
    unknowns,
  };
}

function chooseViability({ approach, budget, conversion, measurement, channels }) {
  const blockers = [];
  const unknowns = [];
  const selected = asText(approach?.selectedApproach || approach?.approach).toLowerCase();

  if (!approach || !selected) {
    blockers.push({
      kind: 'missing_acquisition_approach',
      reason: 'Max acquisition approach decision is required before Penny performs paid planning.',
    });
  }
  if (selected && ![ACQUISITION_APPROACHES.PAID, ACQUISITION_APPROACHES.BOTH].includes(selected)) {
    return {
      viability: VIABILITY.RECOMMEND_NO_PAID,
      blockers,
      unknowns,
      rationale: `Max selected ${selected}; Penny does not override the mission-level acquisition approach.`,
    };
  }
  if (conversion.known && conversion.ready === false) {
    blockers.push({
      kind: 'conversion_infrastructure_inadequate',
      reason: 'Paid traffic does not have a credible conversion path.',
    });
  }
  if (measurement.known && measurement.ready === false) {
    blockers.push({
      kind: 'measurement_infrastructure_inadequate',
      reason: 'Paid test outcomes cannot be attributed to qualified acquisition progression.',
    });
  }
  if (!budget.known) {
    unknowns.push({
      unknown: 'Available paid test budget',
      reason: 'Penny must not invent budget or economics.',
    });
  }
  if (!conversion.known) {
    unknowns.push({
      unknown: 'Conversion path readiness',
      reason: 'Landing page, CTA, lead capture, and contact path evidence was not supplied.',
    });
  }
  if (!measurement.known) {
    unknowns.push({
      unknown: 'Measurement readiness',
      reason: 'Conversion tracking, attribution, and downstream outcome capture evidence was not supplied.',
    });
  }
  if (blockers.length) {
    return {
      viability: VIABILITY.BLOCKED,
      blockers,
      unknowns,
      rationale: 'Paid acquisition should not start until conversion and measurement blockers are resolved.',
    };
  }
  if (unknowns.length) {
    return {
      viability: VIABILITY.DEFER,
      blockers,
      unknowns,
      rationale: 'Paid acquisition may be viable, but required budget or funnel evidence is missing.',
    };
  }
  const best = channels[0];
  if (!best || best.confidence < 0.45) {
    return {
      viability: VIABILITY.RECOMMEND_NO_PAID,
      blockers,
      unknowns,
      rationale: 'No paid channel has enough fit or evidence to justify scarce test capital.',
    };
  }
  return {
    viability: VIABILITY.RECOMMEND_PAID,
    blockers,
    unknowns,
    rationale: 'Paid acquisition is viable as a constrained learning test with measurement and spend limits.',
  };
}

function buildRecommendedTest({ viability, channels, budget, context }) {
  if (viability !== VIABILITY.RECOMMEND_PAID) {
    return null;
  }
  const best = channels[0];
  return {
    preferredChannel: best.channel,
    objective: context.successMetric?.metric || context.objective || 'Validate paid acquisition can create qualified opportunities.',
    hypothesis: best.buyerIntentHypothesis,
    targetIntentOrAudience: /google|search/i.test(best.channel)
      ? 'High-intent local service searches from the mission geography and target segment.'
      : `Mission-bound paid audience or intent concept for ${best.channel}.`,
    budgetEnvelope: budget.known
      ? { amount: budget.amount, currency: budget.currency, source: budget.source, invented: false }
      : { amount: null, currency: null, source: null, invented: false, required: true },
    measurementRequirements: [
      'Track traffic source through lead capture.',
      'Capture qualified lead and sales-stage progression, not only clicks.',
      'Tie paid lead source to walkthrough or equivalent sales event.',
      'Record proposal and recurring-client outcome when available.',
    ],
    conversionRequirement: 'Dedicated conversion path with clear CTA, phone/contact path, and lead qualification capture.',
    durationOrSampleLogic: 'Run only long enough to learn whether qualified opportunities emerge under the approved budget cap.',
    successCriteria: [
      'Relevant traffic produces attributable qualified conversations.',
      'At least one downstream sales event is traceable where sample size permits.',
      'Learning value justifies continuing scarce paid spend.',
    ],
    stopConditions: [
      'Spend occurs without attributable qualified conversations.',
      'Traffic quality is irrelevant to the target customer.',
      'Conversion or attribution path breaks.',
    ],
    continueConditions: [
      'Qualified conversations appear with traceable paid source.',
      'Search/audience terms match the intended buyer and use case.',
      'Cost per qualified progression remains plausible against recurring client value.',
    ],
    scaleConditions: [
      'Paid source contributes qualified opportunities that progress toward recurring revenue.',
      'Measurement confirms business outcomes, not just platform engagement.',
    ],
    materialAssumptions: [
      'Recurring client value can justify a bounded test if qualified opportunity progression is measured.',
      'The preferred paid channel can reach the mission target in the selected geography.',
    ],
    nextAction: 'Operator reviews Penny recommendation before any paid setup or spend.',
  };
}

function buildPaidAcquisitionRecommendationPayload(executionInput = {}) {
  const si = executionInput.specialistInput || {};
  const context = {
    objective: si.objective,
    successMetric: si.successMetric,
    market: si.market,
    buyer: si.buyer,
  };
  const budget = normalizeBudget(si);
  const conversion = normalizeReadiness(si.conversionReadiness, 'conversion_readiness');
  const measurement = normalizeReadiness(si.measurementReadiness, 'measurement_readiness');
  const channels = inferCandidateChannels(executionInput)
    .map((channel) => scoreChannel(channel, context))
    .sort((a, b) => b.confidence - a.confidence);
  const viabilityResult = chooseViability({
    approach: si.acquisitionApproach,
    budget,
    conversion,
    measurement,
    channels,
  });
  const recommendedTest = buildRecommendedTest({
    viability: viabilityResult.viability,
    channels,
    budget,
    context,
  });
  const measurementRequirements = recommendedTest?.measurementRequirements || [
    'Confirm conversion path before spending.',
    'Confirm attribution from paid source to qualified opportunity.',
    'Capture downstream business outcomes beyond platform metrics.',
  ];
  const confidenceOverall = viabilityResult.viability === VIABILITY.RECOMMEND_PAID ? 0.72
    : viabilityResult.viability === VIABILITY.RECOMMEND_NO_PAID ? 0.66
      : viabilityResult.viability === VIABILITY.BLOCKED ? 0.62
        : 0.46;
  const payload = {
    paidAcquisitionRecommendation: {
      spec: 'SPEC-249',
      version: 1,
      viability: viabilityResult.viability,
      rationale: viabilityResult.rationale,
      preferredChannel: recommendedTest?.preferredChannel || null,
      businessOutcomeChain: [
        'spend',
        'relevant_traffic',
        'lead_or_conversation',
        'qualified_opportunity',
        'walkthrough_or_sales_event',
        'proposal',
        'recurring_client',
        'revenue_margin_retention',
      ],
      channelAssessments: channels,
      recommendedTest,
      measurementRequirements,
      budgetConstraints: {
        known: budget.known,
        amount: budget.amount,
        currency: budget.currency,
        invented: false,
        instruction: budget.known
          ? 'Do not exceed supplied test budget without explicit operator approval.'
          : 'Request budget before recommending spend.',
      },
      conversionReadiness: conversion,
      measurementReadiness: measurement,
      evidence: collectBaseEvidence(executionInput, budget, conversion, measurement),
      assumptions: recommendedTest?.materialAssumptions || [
        'Paid channel fit cannot be validated without conversion and measurement readiness.',
      ],
      unknowns: viabilityResult.unknowns,
      blockers: viabilityResult.blockers,
      stopConditions: recommendedTest?.stopConditions || [],
      continueConditions: recommendedTest?.continueConditions || [],
      scaleConditions: recommendedTest?.scaleConditions || [],
      nextAction: recommendedTest?.nextAction || (
        viabilityResult.viability === VIABILITY.BLOCKED
          ? 'Resolve conversion or measurement blockers before paid spend.'
          : 'Provide missing budget, conversion, and measurement evidence before paid spend.'
      ),
      noExternalMutation: true,
      platformMetricsAreEvidenceOnly: true,
    },
    viability: viabilityResult.viability,
    channelAssessments: channels,
    recommendedTest,
    measurementRequirements,
    budgetConstraints: {
      known: budget.known,
      amount: budget.amount,
      currency: budget.currency,
      invented: false,
    },
    stopConditions: recommendedTest?.stopConditions || [],
    continueConditions: recommendedTest?.continueConditions || [],
    scaleConditions: recommendedTest?.scaleConditions || [],
    evidence: collectBaseEvidence(executionInput, budget, conversion, measurement),
    confidence: {
      overall: confidenceOverall,
      evidence: confidenceOverall - 0.06,
      fit: channels[0]?.confidence || 0.4,
      completeness: viabilityResult.unknowns.length ? 0.45 : 0.75,
    },
    unknowns: viabilityResult.unknowns,
    blockers: viabilityResult.blockers,
    recommendations: [viabilityResult.rationale],
  };
  return payload;
}

async function runPennyPaidAcquisition(executionInput = {}) {
  const transactionId = executionInput.transactionId;
  const payload = buildPaidAcquisitionRecommendationPayload(executionInput);
  const recommendation = payload.paidAcquisitionRecommendation;
  const status = recommendation.viability === VIABILITY.BLOCKED
    ? EXECUTION_STATUSES.BLOCKED
    : EXECUTION_STATUSES.SUCCESS;

  return createExecutionResult({
    specialist: SPECIALISTS.PENNY,
    transactionId,
    status,
    confidence: payload.confidence,
    evidence: payload.evidence,
    contributions: payload,
    recommendations: [{
      tier: recommendation.viability === VIABILITY.RECOMMEND_PAID ? 'required' : 'suggested',
      text: recommendation.nextAction,
      reason: recommendation.rationale,
    }],
    unknowns: payload.unknowns,
    nextActions: [{ kind: 'operator_review', label: recommendation.nextAction }],
    reason: status === EXECUTION_STATUSES.BLOCKED ? recommendation.rationale : null,
    requiredPrecondition: status === EXECUTION_STATUSES.BLOCKED ? 'paid_conversion_measurement_readiness' : null,
  });
}

async function runPennyForAmoMission(mission, opts = {}) {
  const contributions = opts.contributions
    || (opts.engine && opts.engine.inspect(mission.id, { tenantId: opts.tenantId }).contributions)
    || [];
  const executionInput = buildExecutionInput({
    mission,
    contributions,
    specialist: SPECIALISTS.PENNY,
    transactionId: opts.transactionId,
    executionContext: opts.executionContext,
    store: opts.engine?.store,
    acquisitionEvidence: opts.acquisitionEvidence,
    knownAcquisitionHistory: opts.knownAcquisitionHistory,
    conversionReadiness: opts.conversionReadiness,
    measurementReadiness: opts.measurementReadiness,
    candidatePaidChannels: opts.candidatePaidChannels,
    platformEvidence: opts.platformEvidence,
    availableBudget: opts.availableBudget,
    operatorPreferences: opts.operatorPreferences,
  });
  return executeSpecialist({
    specialist: SPECIALISTS.PENNY,
    mission,
    contributions,
    transactionId: opts.transactionId,
    store: opts.engine?.store,
    run: async () => {
      if (typeof opts.runPenny === 'function') {
        const custom = await opts.runPenny(mission, {
          ...opts,
          executionInput,
          contributions,
        });
        if (custom && custom.spec === 'SPEC-132') return custom;
        return createExecutionResult({
          specialist: SPECIALISTS.PENNY,
          transactionId: opts.transactionId,
          status: EXECUTION_STATUSES.SUCCESS,
          confidence: custom?.confidence,
          evidence: custom?.evidence,
          contributions: custom,
          recommendations: custom?.recommendations,
          unknowns: custom?.unknowns,
        });
      }
      return runPennyPaidAcquisition({
        ...executionInput,
        mission,
      });
    },
    treatErrorsAsBlocked: opts.treatErrorsAsBlocked !== false,
  });
}

module.exports = {
  VIABILITY,
  CHANNEL_FIT,
  DEFAULT_PAID_CHANNELS,
  buildPaidAcquisitionRecommendationPayload,
  runPennyPaidAcquisition,
  runPennyForAmoMission,
};
