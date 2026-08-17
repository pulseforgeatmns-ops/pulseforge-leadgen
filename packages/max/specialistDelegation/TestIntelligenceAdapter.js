'use strict';

/**
 * SPEC-098 — deterministic test_intelligence fixture.
 * No external action. Exercises the full Max ↔ specialist contract.
 */

const { nowIso, normalizeEvidenceRefs, normalizeStringRecords } = require('./Types');

const CONTRACT_OBJECTIVE =
  'Assess whether Acquisition currently has meaningful opportunity.';

const DEFAULT_EVIDENCE = Object.freeze([
  {
    id: 'ev-test-opp-1',
    kind: 'company',
    sourceKind: 'observed_fact',
    label: 'Property manager A — recent portfolio expansion',
  },
  {
    id: 'ev-test-opp-2',
    kind: 'company',
    sourceKind: 'observed_fact',
    label: 'Property manager B — new commercial listings',
  },
  {
    id: 'ev-test-opp-3',
    kind: 'market',
    sourceKind: 'observed_fact',
    label: 'Property manager C — timing evidence incomplete',
  },
]);

/**
 * @param {object} delegation
 * @param {object} [opts]
 * @returns {object} SpecialistResult fields (id/timestamps filled by orchestrator)
 */
function runTestIntelligence(delegation, opts = {}) {
  const mode = opts.mode || (delegation && delegation._fixtureMode) || 'completed';
  const startedAt = nowIso();

  if (mode === 'blocked') {
    return withConsumed({
      status: 'blocked',
      summary: 'Cannot assess Acquisition — no configured intelligence source.',
      observations: [],
      actionsTaken: [
        { text: 'Checked for a configured acquisition intelligence source.' },
      ],
      evidenceRefs: [],
      artifactRefs: [],
      confidence: null,
      uncertainties: ['No LinkedIn or market intelligence source is configured.'],
      recommendedNextAction: {
        type: 'ask_operator',
        text: 'Configure an acquisition intelligence source, then retry.',
      },
      policyEvents: [],
      errors: [
        {
          code: 'source_unavailable',
          message: 'No configured LinkedIn intelligence source.',
        },
      ],
      startedAt,
      completedAt: nowIso(),
    }, delegation);
  }

  const gathered = normalizeEvidenceRefs(opts.evidenceRefs || DEFAULT_EVIDENCE);

  if (mode === 'partial') {
    return withConsumed({
      status: 'partial',
      summary: 'Two relevant opportunities detected before enrichment failed.',
      observations: [
        {
          text: 'Two property-management companies match the current target profile.',
        },
      ],
      actionsTaken: [
        { text: 'Retrieved and analyzed recent company evidence.' },
        { text: 'Attempted enrichment; provider unavailable.' },
      ],
      evidenceRefs: gathered.slice(0, 2),
      artifactRefs: [],
      confidence: 0.61,
      uncertainties: [
        'Enrichment provider unavailable — third opportunity not confirmed.',
      ],
      recommendedNextAction: {
        type: 'retry',
        specialist: 'test_intelligence',
        capability: 'acquisition_assessment',
        text: 'Retry enrichment once the provider recovers.',
      },
      policyEvents: [],
      errors: [
        {
          code: 'enrichment_unavailable',
          message: 'Enrichment provider unavailable.',
        },
      ],
      startedAt,
      completedAt: nowIso(),
    }, delegation);
  }

  if (mode === 'failed') {
    return withConsumed({
      status: 'failed',
      summary: 'Assessment failed after partial evidence was gathered.',
      observations: [
        { text: 'Initial company scan completed before the provider error.' },
      ],
      actionsTaken: [{ text: 'Retrieved initial company evidence.' }],
      evidenceRefs: gathered.slice(0, 1),
      artifactRefs: [],
      confidence: null,
      uncertainties: ['Provider failure interrupted the assessment.'],
      recommendedNextAction: {
        type: 'retry',
        text: 'Retry the assessment.',
      },
      policyEvents: [],
      errors: [
        { code: 'provider_error', message: 'Downstream provider failed.' },
      ],
      startedAt,
      completedAt: nowIso(),
    }, delegation);
  }

  return withConsumed({
    status: 'completed',
    summary: 'Three relevant opportunities detected.',
    observations: [
      {
        text: 'Three property-management companies match the current Acquisition target profile.',
      },
      {
        text: 'Two of the three show recent expansion or listing activity.',
      },
    ],
    actionsTaken: [
      { text: 'Retrieved and analyzed recent company evidence.' },
      { text: 'Compared opportunities against the current target profile.' },
    ],
    evidenceRefs: gathered,
    artifactRefs: [],
    confidence: 0.84,
    uncertainties: [
      'One opportunity lacks recent timing evidence.',
    ],
    recommendedNextAction: {
      type: 'review',
      text: 'Review the three opportunities before deciding whether to elevate Acquisition.',
    },
    policyEvents: [],
    errors: [],
    startedAt,
    completedAt: nowIso(),
    _contractObjective: CONTRACT_OBJECTIVE,
  }, delegation);
}

function consumedContextFromDelegation(delegation) {
  const target = (delegation && delegation.targetContext) || {};
  const business = (delegation && delegation.businessContext) || {};
  const geography = target.geography || business.serviceGeography || null;
  return {
    geography,
    geographyResolved: Boolean(geography),
    segments: Array.isArray(target.segments) ? target.segments.slice() : [],
    businessNeed: target.businessType || business.commercialCapability || null,
    valid: true,
    invalidReason: null,
  };
}

function withConsumed(result, delegation) {
  return {
    ...result,
    payload: {
      ...(result.payload || {}),
      consumedContext: consumedContextFromDelegation(delegation),
    },
  };
}

function isTestIntelligence(specialist, capability) {
  return (
    specialist === 'test_intelligence' &&
    (capability === 'acquisition_assessment' || capability == null)
  );
}

module.exports = {
  CONTRACT_OBJECTIVE,
  DEFAULT_EVIDENCE,
  runTestIntelligence,
  isTestIntelligence,
  normalizeStringRecords,
};
