'use strict';

/**
 * SPEC-177 / SPEC-182 — Evidence Provider Assignment.
 * Providers satisfy evidence — not questions.
 * The planner asks "who can answer this question?" via dynamic capability matching.
 */

const { INVESTIGATIVE_EVIDENCE } = require('./EvidenceRequirements');
const {
  createDefaultUnifiedRegistry,
  EVIDENCE_TO_CAPABILITY,
} = require('./ProviderCapabilityRegistry');

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

/**
 * Derive provider preference order from registry capability matching.
 * Used for backward-compatible test assertions.
 * @param {object} [registry]
 * @returns {object}
 */
function buildEvidenceToProvidersMap(registry = createDefaultUnifiedRegistry()) {
  const map = {};
  for (const evidenceType of Object.values(INVESTIGATIVE_EVIDENCE)) {
    map[evidenceType] = registry
      .selectForEvidenceType(evidenceType, { includeUnavailable: true })
      .map((p) => p.id);
  }
  return Object.freeze(map);
}

/** @deprecated Dynamic — derived from registry at runtime. Kept for test compat. */
const EVIDENCE_TO_PROVIDERS = buildEvidenceToProvidersMap();

/** @deprecated Fallbacks are now automatic via registry ranking. */
const EVIDENCE_FALLBACK_PROVIDERS = Object.freeze({});

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

function isProviderAvailable(providerId, registry, opts = {}) {
  const unified = registry._unified || registry;
  const meta = unified.get(providerId);
  if (!meta) return false;
  return unified.isAvailable(providerId, opts);
}

/**
 * Assign providers for a single evidence type via dynamic capability matching.
 * @param {string} evidenceType
 * @param {object} [opts]
 * @returns {object[]}
 */
function assignProvidersForEvidence(evidenceType, opts = {}) {
  const registry = opts.registry?._unified || opts.registry || createDefaultUnifiedRegistry();
  const unavailable = new Set((opts.unavailableProviders || []).map((p) => String(p).toLowerCase()));
  const label = EVIDENCE_LABELS[evidenceType] || evidenceType;
  const task = `Collect ${label}`;

  const capable = registry.selectForEvidenceType(evidenceType, {
    includeUnavailable: opts.includeUnavailable !== false,
    ...opts,
  });

  const assignments = [];
  let order = 0;

  function tryAssign(provider, isFallback = false) {
    const key = String(provider.id).toLowerCase();
    if (unavailable.has(key)) return;
    if (assignments.some((a) => a.providerId === provider.id)) return;

    const available = isProviderAvailable(provider.id, registry, opts);
    if (!available && opts.includeUnavailable === false) return;

    order += 1;
    assignments.push(
      buildProviderAssignment({
        providerId: provider.id,
        providerLabel: provider.label || provider.id,
        evidenceType,
        task,
        rationale: explainProviderSelection(evidenceType, provider.id, {
          registry,
          isFallback,
        }),
        confidence: provider.reliability || (available ? 0.7 : 0),
        coverage: provider.coverage || 0.5,
        limitations: available ? provider.limitations || [] : [`${provider.id} unavailable`],
        status: available ? 'pending' : 'unavailable',
        order,
        isFallback,
      })
    );
  }

  for (const provider of capable) {
    tryAssign(provider, false);
  }

  const allCapableUnavailable =
    capable.length > 0 &&
    capable.every(
      (p) =>
        unavailable.has(String(p.id).toLowerCase()) || !isProviderAvailable(p.id, registry, opts)
    );

  if (allCapableUnavailable) {
    for (const provider of capable) {
      tryAssign(provider, true);
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
  const registry = context.registry || createDefaultUnifiedRegistry();
  const meta = registry.get(providerId);
  const providerLabel = meta?.label || providerId;
  const capability = EVIDENCE_TO_CAPABILITY[evidenceType];

  if (context.isFallback) {
    return `Primary sources unavailable; ${providerLabel} assigned as fallback to collect ${evidenceLabel}.`;
  }

  const confidencePct = meta?.reliability ? Math.round(meta.reliability * 100) : null;
  const costTier = meta?.costTier || 'paid';
  const parts = [
    `${providerLabel} advertises capability for ${evidenceLabel}`,
  ];
  if (capability) parts.push(`(${capability})`);
  if (confidencePct) parts.push(`with ${confidencePct}% reliability`);
  if (costTier === 'free' || costTier === 'cached') {
    parts.push(`— preferred ${costTier} tier`);
  }
  return `${parts.join(' ')}.`;
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
  buildEvidenceToProvidersMap,
  buildProviderAssignment,
  assignProvidersForEvidence,
  assignProvidersForRequirements,
  explainProviderSelection,
  explainProviderForOperator,
  isProviderAvailable,
};
