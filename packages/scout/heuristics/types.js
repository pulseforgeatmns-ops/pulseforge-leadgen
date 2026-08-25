'use strict';

/**
 * SPEC-162 — Business Heuristics types.
 * ADR-082 — Business judgment through reusable heuristics.
 *
 * Heuristics are reusable patterns that transform understanding into judgment.
 */

function nowIso() {
  return new Date().toISOString();
}

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

const HEURISTIC_CATEGORIES = Object.freeze({
  MARKET_GROWTH: 'market_growth',
  VENDOR_REPLACEMENT: 'vendor_replacement',
  VENDOR_STABILITY: 'vendor_stability',
  BUYING_READINESS: 'buying_readiness',
  OPERATIONAL_MATURITY: 'operational_maturity',
  RELATIONSHIP_LEVERAGE: 'relationship_leverage',
});

const OUTCOME_KINDS = Object.freeze({
  WON: 'won',
  LOST: 'lost',
});

/**
 * Reusable business heuristic definition.
 * @param {object} partial
 * @returns {object}
 */
function buildBusinessHeuristic(partial = {}) {
  const triggerConditions = partial.triggerConditions || {};
  return {
    id: partial.id || `heur-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    name: asText(partial.name),
    category: partial.category || HEURISTIC_CATEGORIES.MARKET_GROWTH,
    description: asText(partial.description),
    triggerConditions: {
      patterns: (triggerConditions.patterns || []).slice(),
      minMatches: triggerConditions.minMatches != null ? Number(triggerConditions.minMatches) : 1,
      understandingKinds: (triggerConditions.understandingKinds || []).slice(),
      assertionPatterns: (triggerConditions.assertionPatterns || []).slice(),
    },
    implications: Array.isArray(partial.implications)
      ? partial.implications.map(asText).filter(Boolean)
      : partial.implication
        ? [asText(partial.implication)]
        : [],
    confidenceModifier:
      partial.confidenceModifier != null ? Number(partial.confidenceModifier) : 0,
    evidenceRequirements: {
      minSignals:
        partial.evidenceRequirements?.minSignals != null
          ? Number(partial.evidenceRequirements.minSignals)
          : 1,
    },
    applicability: {
      verticals: partial.applicability?.verticals || ['*'],
      geographies: partial.applicability?.geographies || ['*'],
    },
    learnedFrom: partial.learnedFrom || null,
    strength: partial.strength != null ? Number(partial.strength) : 1,
    contradicts: Array.isArray(partial.contradicts) ? partial.contradicts.slice() : [],
    supportingEvidence: Array.isArray(partial.supportingEvidence) ? partial.supportingEvidence.slice() : [],
    contradictoryEvidence: Array.isArray(partial.contradictoryEvidence)
      ? partial.contradictoryEvidence.slice()
      : [],
    createdAt: partial.createdAt || nowIso(),
    updatedAt: partial.updatedAt || nowIso(),
  };
}

/**
 * Activated heuristic instance with scored judgment for one entity or market.
 * @param {object} partial
 * @returns {object}
 */
function buildActivatedHeuristic(partial = {}) {
  return {
    heuristicId: partial.heuristicId,
    name: asText(partial.name),
    category: partial.category,
    description: asText(partial.description),
    score: partial.score != null ? Number(partial.score) : 0,
    confidence: partial.confidence != null ? Number(partial.confidence) : 0,
    implications: Array.isArray(partial.implications) ? partial.implications.slice() : [],
    triggeringEvidence: Array.isArray(partial.triggeringEvidence) ? partial.triggeringEvidence.slice() : [],
    contradictoryEvidence: Array.isArray(partial.contradictoryEvidence)
      ? partial.contradictoryEvidence.slice()
      : [],
    entity: partial.entity || null,
    entityId: partial.entityId || null,
    strength: partial.strength != null ? Number(partial.strength) : 1,
    activatedAt: partial.activatedAt || nowIso(),
  };
}

/**
 * Pair of opposing activated heuristics.
 * @param {object} partial
 * @returns {object}
 */
function buildHeuristicContradiction(partial = {}) {
  return {
    heuristicA: partial.heuristicA,
    heuristicB: partial.heuristicB,
    nameA: partial.nameA,
    nameB: partial.nameB,
    scoreA: partial.scoreA,
    scoreB: partial.scoreB,
    tension: asText(partial.tension),
    confidencePenalty: partial.confidencePenalty != null ? Number(partial.confidencePenalty) : 0.1,
  };
}

module.exports = {
  HEURISTIC_CATEGORIES,
  OUTCOME_KINDS,
  buildBusinessHeuristic,
  buildActivatedHeuristic,
  buildHeuristicContradiction,
  nowIso,
  asText,
};
