'use strict';

/**
 * Market Strategy Pack types (SPEC-016).
 * JSDoc-only — no runtime type enforcement.
 */

/** @typedef {'price_tick'|'volume_update'|'news_event'|'economic_release'|'volatility_observation'|'market_session'|'market_snapshot'} ObservationType */

/**
 * @typedef {object} PriceTick
 * @property {'price_tick'} type
 * @property {string} asset
 * @property {number} price
 * @property {string} timestamp
 * @property {string} [venue]
 */

/**
 * @typedef {object} VolumeUpdate
 * @property {'volume_update'} type
 * @property {string} asset
 * @property {number} volume
 * @property {string} window
 * @property {string} timestamp
 */

/**
 * @typedef {object} NewsEvent
 * @property {'news_event'} type
 * @property {string} headline
 * @property {string[]} [symbols]
 * @property {number|null} [sentiment] - -1 to 1
 * @property {string} timestamp
 */

/**
 * @typedef {object} EconomicRelease
 * @property {'economic_release'} type
 * @property {string} series
 * @property {number|null} actual
 * @property {number|null} forecast
 * @property {number|null} prior
 * @property {string} timestamp
 */

/**
 * @typedef {object} VolatilityObservation
 * @property {'volatility_observation'} type
 * @property {string} asset
 * @property {number} value
 * @property {string} measure
 * @property {string} timestamp
 */

/**
 * @typedef {object} MarketSession
 * @property {'market_session'} type
 * @property {string} session
 * @property {string} status
 * @property {string} timestamp
 */

/**
 * @typedef {object} MarketSnapshot
 * @property {'market_snapshot'} type
 * @property {string} asset
 * @property {number} price
 * @property {number} [volume24h]
 * @property {number} [changePct]
 * @property {string} timestamp
 */

/**
 * @typedef {PriceTick|VolumeUpdate|NewsEvent|EconomicRelease|VolatilityObservation|MarketSession|MarketSnapshot} RawMarketObservation
 */

/**
 * @typedef {object} NormalizedObservation
 * @property {string} id
 * @property {ObservationType} observationType
 * @property {string} asset
 * @property {string} timestamp
 * @property {object} payload
 */

/**
 * @typedef {object} MarketEvidenceRef
 * @property {string} id
 * @property {string} kind
 * @property {string} summary
 * @property {string|null} [sourceId]
 * @property {string|null} [sourceType]
 * @property {number|null} [confidence]
 */

/**
 * @typedef {object} MarketClaim
 * @property {string} id
 * @property {string} statement
 * @property {string} claimType
 * @property {number|null} [confidence]
 * @property {string} [status]
 */

/**
 * @typedef {object} MarketContext
 * @property {string} subjectId
 * @property {{ id: string, symbol: string, name: string|null }} asset
 * @property {NormalizedObservation[]} observations
 * @property {MarketEvidenceRef[]} evidence
 * @property {MarketClaim[]} claims
 * @property {object} metrics
 * @property {object|null} session
 * @property {string} builtAt
 * @property {string} repositoryType
 */

/**
 * @typedef {object} MarketStrategyResult
 * @property {string} strategy
 * @property {number} scoreDelta
 * @property {number} confidence
 * @property {MarketEvidenceRef[]} supportingEvidence
 * @property {MarketEvidenceRef[]} contradictingEvidence
 * @property {string[]} claims
 * @property {string} summary
 */

/**
 * @typedef {object} HistoricalAnalog
 * @property {string} id
 * @property {number} similarityScore
 * @property {string} timestamp
 * @property {string[]} supportingClaims
 */

/**
 * @typedef {object} ResearchRecommendation
 * @property {string} id
 * @property {{ id: string, name: string|null, type: string }} subject
 * @property {string} type
 * @property {string} priority
 * @property {number} score
 * @property {number} confidence
 * @property {string} recommendedAction
 * @property {MarketEvidenceRef[]} supportingSignals
 * @property {MarketEvidenceRef[]} opposingSignals
 * @property {string[]} claims
 * @property {string[]} evidence
 * @property {object} reasoningSummary
 */

const MARKET_CLAIM_TYPES = Object.freeze({
  MOMENTUM_CONTINUATION: 'momentum_continuation',
  MOMENTUM_EXHAUSTION: 'momentum_exhaustion',
  ELEVATED_VOLATILITY: 'elevated_volatility',
  MEAN_REVERSION: 'mean_reversion',
  REGIME_TRANSITION: 'regime_transition',
  NEWS_DRIVEN_EXPANSION: 'news_driven_expansion',
  LIQUIDITY_CONTRACTION: 'liquidity_contraction',
});

const RESEARCH_ACTIONS = Object.freeze({
  OBSERVE: 'observe',
  GATHER_MORE_EVIDENCE: 'gather_more_evidence',
  HISTORICAL_ANALOG_FOUND: 'historical_analog_found',
  REGIME_TRANSITION: 'regime_transition',
  EVIDENCE_SHIFT: 'evidence_shift',
  HYPOTHESIS_STRENGTHENING: 'hypothesis_strengthening',
  HYPOTHESIS_WEAKENING: 'hypothesis_weakening',
  REPLAY_SUGGESTED: 'replay_suggested',
});

/** Explicitly prohibited execution vocabulary (SPEC-016). */
const FORBIDDEN_ACTIONS = Object.freeze([
  'buy',
  'sell',
  'long',
  'short',
  'enter',
  'exit',
]);

const DEFAULT_MARKET_WEIGHTS = Object.freeze({
  [MARKET_CLAIM_TYPES.MOMENTUM_CONTINUATION]: 0.2,
  [MARKET_CLAIM_TYPES.MOMENTUM_EXHAUSTION]: 0.1,
  [MARKET_CLAIM_TYPES.ELEVATED_VOLATILITY]: 0.15,
  [MARKET_CLAIM_TYPES.MEAN_REVERSION]: 0.15,
  [MARKET_CLAIM_TYPES.REGIME_TRANSITION]: 0.15,
  [MARKET_CLAIM_TYPES.NEWS_DRIVEN_EXPANSION]: 0.15,
  [MARKET_CLAIM_TYPES.LIQUIDITY_CONTRACTION]: 0.1,
});

/**
 * @param {number} n
 * @param {number} min
 * @param {number} max
 */
function clamp(n, min, max) {
  const v = Number(n);
  if (!Number.isFinite(v)) return min;
  if (v < min) return min;
  if (v > max) return max;
  return v;
}

/**
 * @param {number} n
 * @param {number} [digits=2]
 */
function round(n, digits = 2) {
  const f = 10 ** digits;
  return Math.round(Number(n) * f) / f;
}

/**
 * @param {Partial<MarketEvidenceRef> & { id: string, kind: string, summary: string }} input
 * @returns {MarketEvidenceRef}
 */
function evidenceRef(input) {
  return {
    id: String(input.id),
    kind: String(input.kind),
    summary: String(input.summary),
    sourceId: input.sourceId != null ? String(input.sourceId) : null,
    sourceType: input.sourceType != null ? String(input.sourceType) : null,
    confidence:
      input.confidence == null || !Number.isFinite(Number(input.confidence))
        ? null
        : Number(input.confidence),
  };
}

/**
 * @param {object} input
 * @returns {MarketStrategyResult}
 */
function strategyResult(input) {
  return {
    strategy: String(input.strategy),
    scoreDelta: round(clamp(input.scoreDelta == null ? 0 : input.scoreDelta, -100, 100)),
    confidence: round(clamp(input.confidence == null ? 0 : input.confidence, 0, 100)),
    supportingEvidence: [...(input.supportingEvidence || [])].sort((a, b) =>
      String(a.id).localeCompare(String(b.id))
    ),
    contradictingEvidence: [...(input.contradictingEvidence || [])].sort((a, b) =>
      String(a.id).localeCompare(String(b.id))
    ),
    claims: [...new Set(input.claims || [])].map(String).sort(),
    summary: String(input.summary || ''),
  };
}

module.exports = {
  MARKET_CLAIM_TYPES,
  RESEARCH_ACTIONS,
  FORBIDDEN_ACTIONS,
  DEFAULT_MARKET_WEIGHTS,
  clamp,
  round,
  evidenceRef,
  strategyResult,
};
