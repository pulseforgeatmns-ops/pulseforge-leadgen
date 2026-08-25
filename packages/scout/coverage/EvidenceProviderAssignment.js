'use strict';

/**
 * SPEC-177 — Evidence Provider Assignment.
 * Providers satisfy evidence — not questions.
 * The planner never says "Search Google Places"; it says "Need business identities"
 * and assigns all providers capable of producing that evidence.
 */

const { INVESTIGATIVE_EVIDENCE } = require('./EvidenceRequirements');
const {
  createDefaultProviderRegistry,
  EVIDENCE_CAPABILITIES,
} = require('../intelligence/ProviderCapabilityRegistry');

/** Evidence type → provider IDs (ordered by preference). */
const EVIDENCE_TO_PROVIDERS = Object.freeze({
  [INVESTIGATIVE_EVIDENCE.IDENTITY]: ['google_maps', 'county_records', 'existing_pf'],
  [INVESTIGATIVE_EVIDENCE.DECISION_MAKERS]: ['linkedin', 'website', 'prospeo'],
  [INVESTIGATIVE_EVIDENCE.REVIEWS]: ['google_maps'],
  [INVESTIGATIVE_EVIDENCE.PORTFOLIO]: ['website', 'county_records'],
  [INVESTIGATIVE_EVIDENCE.GROWTH]: ['linkedin', 'news'],
  [INVESTIGATIVE_EVIDENCE.CLEANING]: ['website', 'google_maps'],
  [INVESTIGATIVE_EVIDENCE.LICENSING]: ['county_records'],
  [INVESTIGATIVE_EVIDENCE.SOCIAL]: ['facebook', 'instagram'],
  [INVESTIGATIVE_EVIDENCE.CONTACT]: ['prospeo', 'hunter', 'website'],
  [INVESTIGATIVE_EVIDENCE.BUYING]: ['news', 'linkedin'],
});

/** Fallback providers when primary is unavailable (Scenario 4). */
const EVIDENCE_FALLBACK_PROVIDERS = Object.freeze({
  [INVESTIGATIVE_EVIDENCE.IDENTITY]: ['county_records', 'existing_pf'],
  [INVESTIGATIVE_EVIDENCE.DECISION_MAKERS]: ['website', 'prospeo'],
  [INVESTIGATIVE_EVIDENCE.REVIEWS]: ['website'],
  [INVESTIGATIVE_EVIDENCE.PORTFOLIO]: ['county_records'],
  [INVESTIGATIVE_EVIDENCE.CLEANING]: ['google_maps'],
});

/** Evidence type → capability for registry lookup. */
const EVIDENCE_TO_CAPABILITY = Object.freeze({
  [INVESTIGATIVE_EVIDENCE.IDENTITY]: EVIDENCE_CAPABILITIES.BUSINESSES,
  [INVESTIGATIVE_EVIDENCE.DECISION_MAKERS]: EVIDENCE_CAPABILITIES.PEOPLE,
  [INVESTIGATIVE_EVIDENCE.REVIEWS]: EVIDENCE_CAPABILITIES.REVIEWS,
  [INVESTIGATIVE_EVIDENCE.PORTFOLIO]: EVIDENCE_CAPABILITIES.PROPERTY_COUNT,
  [INVESTIGATIVE_EVIDENCE.GROWTH]: EVIDENCE_CAPABILITIES.GROWTH,
  [INVESTIGATIVE_EVIDENCE.CLEANING]: EVIDENCE_CAPABILITIES.WEBSITE,
  [INVESTIGATIVE_EVIDENCE.LICENSING]: EVIDENCE_CAPABILITIES.COUNTY_RECORDS,
  [INVESTIGATIVE_EVIDENCE.SOCIAL]: EVIDENCE_CAPABILITIES.BUSINESSES,
  [INVESTIGATIVE_EVIDENCE.CONTACT]: EVIDENCE_CAPABILITIES.CONTACTS,
  [INVESTIGATIVE_EVIDENCE.BUYING]: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
});

/** Human-readable evidence labels for operator explainability. */
const EVIDENCE_LABELS = Object.freeze({
  [INVESTIGATIVE_EVIDENCE.IDENTITY]: 'business identities',
  [INVESTIGATIVE_EVIDENCE.DECISION_MAKERS]: 'organizational roles and decision makers',
  [INVESTIGATIVE_EVIDENCE.REVIEWS]: 'customer reviews and service feedback',
  [INVESTIGATIVE_EVIDENCE.PORTFOLIO]: 'property portfolio evidence',
  [INVESTIGATIVE_EVIDENCE.GROWTH]: 'growth signals',
  [INVESTIGATIVE_EVIDENCE.CLEANING]: 'cleaning responsibility signals',
  [INVESTIGATIVE_EVIDENCE.LICENSING]: 'licensing and registry records',
  [INVESTIGATIVE_EVIDENCE.SOCIAL]: 'social presence',
  [INVESTIGATIVE_EVIDENCE.CONTACT]: 'contact paths',
  [INVESTIGATIVE_EVIDENCE.BUYING]: 'buying signals',
});

function buildProviderAssignment(partial = {}) {
  return {
    providerId: partial.providerId || partial.provider || '',
    providerLabel: partial.providerLabel || partial.providerId || '',
    evidenceType: partial.evidenceType || '',
    task: partial.task || '',
    rationale: partial.rationale || '',
    confidence: partial.confidence != null ? Number(partial.confidence) : null,
    coverage: partial.coverage != null ? Number(partial.coverage) : null,
    limitations: Array.isArray(partial.limitations) ? partial.limitations : [],
    status: partial.status || 'pending',
    order: partial.order != null ? partial.order : 0,
    isFallback: partial.isFallback === true,
  };
}

function isProviderAvailable(providerId, registry) {
  const meta = registry.get(providerId);
  if (!meta) return false;
  if (typeof meta.available === 'function') return meta.available();
  return true;
}

/**
 * Assign providers for a single evidence type.
 * @param {string} evidenceType
 * @param {object} [opts]
 * @returns {object[]}
 */
function assignProvidersForEvidence(evidenceType, opts = {}) {
  const registry = opts.registry || createDefaultProviderRegistry();
  const unavailable = new Set((opts.unavailableProviders || []).map((p) => String(p).toLowerCase()));
  const preferred = EVIDENCE_TO_PROVIDERS[evidenceType] || [];
  const fallbacks = EVIDENCE_FALLBACK_PROVIDERS[evidenceType] || [];
  const label = EVIDENCE_LABELS[evidenceType] || evidenceType;
  const task = `Collect ${label}`;

  const assignments = [];
  const seen = new Set();
  let order = 0;

  function tryAssign(providerId, isFallback = false) {
    const key = String(providerId).toLowerCase();
    if (seen.has(key)) return;
    if (unavailable.has(key)) return;

    const meta = registry.get(providerId);
    const available = isProviderAvailable(providerId, registry);

    if (!available && opts.includeUnavailable === false) return;

    seen.add(key);
    order += 1;
    assignments.push(
      buildProviderAssignment({
        providerId,
        providerLabel: meta?.label || providerId,
        evidenceType,
        task,
        rationale: explainProviderSelection(evidenceType, providerId, { isFallback }),
        confidence: meta?.reliability || (available ? 0.7 : 0),
        coverage: meta?.coverage || 0.5,
        limitations: available ? [] : [`${providerId} unavailable`],
        status: available ? 'pending' : 'unavailable',
        order,
        isFallback,
      })
    );
  }

  for (const providerId of preferred) {
    tryAssign(providerId, false);
  }

  // When all preferred are unavailable, use fallbacks (Scenario 4).
  const allPreferredUnavailable = preferred.length > 0 && preferred.every(
    (pid) => unavailable.has(String(pid).toLowerCase()) || !isProviderAvailable(pid, registry)
  );
  if (allPreferredUnavailable) {
    for (const providerId of fallbacks) {
      tryAssign(providerId, true);
    }
  }

  return assignments;
}

/**
 * Assign providers for all evidence requirements, avoiding duplicates.
 * @param {object[]} evidenceRequirements
 * @param {object} [opts]
 * @returns {object[]}
 */
function assignProvidersForRequirements(evidenceRequirements = [], opts = {}) {
  const all = [];
  const seen = new Set();

  for (const req of evidenceRequirements) {
    const assignments = assignProvidersForEvidence(req.evidenceType, opts);
    for (const row of assignments) {
      const key = `${row.providerId}:${row.evidenceType}`;
      if (seen.has(key)) continue;
      seen.add(key);
      all.push(row);
    }
  }

  return all.sort((a, b) => a.order - b.order);
}

/**
 * Operator explainability — why was this provider selected?
 * @param {string} evidenceType
 * @param {string} providerId
 * @param {object} [context]
 * @returns {string}
 */
function explainProviderSelection(evidenceType, providerId, context = {}) {
  const evidenceLabel = EVIDENCE_LABELS[evidenceType] || evidenceType;
  const registry = context.registry || createDefaultProviderRegistry();
  const meta = registry.get(providerId);
  const providerLabel = meta?.label || providerId;

  if (context.isFallback) {
    return `Primary identity sources unavailable; ${providerLabel} assigned as fallback to collect ${evidenceLabel}.`;
  }

  const reasons = {
    google_maps: `Google Maps is our highest-confidence source for ${evidenceLabel} in local markets.`,
    county_records: `${providerLabel} provides registry and licensing evidence for ${evidenceLabel}.`,
    linkedin: `Because the current hypothesis requires identifying decision makers, and ${providerLabel} is our highest-confidence source for organizational roles.`,
    website: `${providerLabel} can verify ${evidenceLabel} from public business web presence.`,
    prospeo: `${providerLabel} enriches contact and title data for ${evidenceLabel}.`,
    existing_pf: `Existing PulseForge intelligence may already hold ${evidenceLabel}.`,
    facebook: `${providerLabel} provides social presence signals for ${evidenceLabel}.`,
    instagram: `${providerLabel} provides social presence signals for ${evidenceLabel}.`,
    news: `${providerLabel} surfaces growth and buying signals for ${evidenceLabel}.`,
    hunter: `${providerLabel} verifies email contact paths for ${evidenceLabel}.`,
  };

  return (
    reasons[providerId] ||
    `${providerLabel} assigned to collect ${evidenceLabel} because it satisfies the evidence requirement.`
  );
}

/**
 * Answer "Why are we searching LinkedIn?" style questions for Max.
 * @param {object} assignment
 * @param {object} [plan]
 * @returns {string}
 */
function explainProviderForOperator(assignment, plan = {}) {
  const hypothesis =
    (plan.hypotheses || []).find((h) => h.id === assignment.hypothesisId) ||
    (plan.hypotheses || [])[0];
  const hypText = hypothesis?.text || 'the current hypothesis';
  const evidenceLabel = EVIDENCE_LABELS[assignment.evidenceType] || assignment.evidenceType;

  if (assignment.providerId === 'linkedin') {
    return `Because ${hypText} requires identifying decision makers, and LinkedIn is our highest-confidence source for organizational roles.`;
  }

  return `Because ${hypText} requires ${evidenceLabel}, and ${assignment.providerLabel || assignment.providerId} was selected: ${assignment.rationale || explainProviderSelection(assignment.evidenceType, assignment.providerId)}`;
}

module.exports = {
  EVIDENCE_TO_PROVIDERS,
  EVIDENCE_FALLBACK_PROVIDERS,
  EVIDENCE_TO_CAPABILITY,
  EVIDENCE_LABELS,
  buildProviderAssignment,
  assignProvidersForEvidence,
  assignProvidersForRequirements,
  explainProviderSelection,
  explainProviderForOperator,
  isProviderAvailable,
};
