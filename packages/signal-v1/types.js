'use strict';

/** @typedef {'solana'} SignalChain */

/**
 * @typedef {(
 *   | 'CALL'
 *   | 'TOKEN_MENTION'
 *   | 'WALLET_BUY'
 *   | 'WALLET_SELL'
 *   | 'DEV_BUY'
 *   | 'DEV_SELL'
 *   | 'WHALE_BUY'
 *   | 'WHALE_SELL'
 *   | 'LIQUIDITY_CHANGE'
 *   | 'MARKET_SNAPSHOT'
 *   | 'HOLDER_SNAPSHOT'
 *   | 'DEX_RANK_CHANGE'
 *   | 'FOMO_RANK_CHANGE'
 *   | 'SOCIAL_ACCELERATION'
 *   | 'AMPLIFIER_ENTRY'
 *   | 'DISTRIBUTION_SIGNAL'
 *   | 'RUG_SIGNAL'
 *   | string
 * )} SignalEventType
 */

/**
 * @typedef {(
 *   | 'telegram'
 *   | 'x'
 *   | 'fomo'
 *   | 'wallet'
 *   | 'dex'
 *   | 'market'
 *   | 'manual'
 *   | 'other'
 * )} SignalSourceType
 */

/**
 * @typedef {(
 *   | 'REJECT'
 *   | 'WATCH'
 *   | 'ENTRY'
 *   | 'DE_RISK'
 *   | 'EXIT'
 * )} SignalState
 */

/**
 * @typedef {(
 *   | 'OPEN'
 *   | 'DE_RISKED'
 *   | 'CLOSED'
 * )} PaperPositionStatus
 */

/**
 * @typedef {(
 *   | 'PASS'
 *   | 'FAIL'
 *   | 'UNRESOLVED'
 * )} OutcomeLabel
 */

/**
 * @typedef {'promotional_network' | 'shared_operator' | 'copy_network' | 'unknown'} ClusterType
 */

/**
 * @typedef {'verified' | 'source-claimed' | 'inferred' | 'unknown'} AddressProvenance
 */

const FEATURE_VERSION = 'signal-features-v1';
const STRATEGY_VERSION = 'signal-strategy-v1';
const SOURCE_PERFORMANCE_VERSION = 'source-performance-v1';
const RESEARCH_DEFINITION_VERSION = 'signal-research-v1';

/** @typedef {(
 *   | 'FIRST_CALLER'
 *   | 'INDEPENDENT_CONVERGENCE'
 *   | 'QUALITY_CONVERGENCE'
 *   | 'WALLET_CONFIRMATION'
 *   | 'STRUCTURE_GATE'
 *   | 'AMPLIFIER_ARRIVAL'
 *   | 'SIGNAL_ENTRY'
 * )} ResearchObservationType */

/** @typedef {(
 *   | 'AVAILABLE'
 *   | 'PARTIAL'
 *   | 'UNAVAILABLE'
 *   | 'INSUFFICIENT_MARKET_DATA'
 * )} ResearchDataAvailability */

const RESEARCH_OBSERVATION_TYPES = Object.freeze([
  'FIRST_CALLER',
  'INDEPENDENT_CONVERGENCE',
  'QUALITY_CONVERGENCE',
  'WALLET_CONFIRMATION',
  'STRUCTURE_GATE',
  'AMPLIFIER_ARRIVAL',
  'SIGNAL_ENTRY',
]);

const SIGNAL_EVENT_TYPES = Object.freeze([
  'CALL',
  'TOKEN_MENTION',
  'WALLET_BUY',
  'WALLET_SELL',
  'DEV_BUY',
  'DEV_SELL',
  'WHALE_BUY',
  'WHALE_SELL',
  'LIQUIDITY_CHANGE',
  'MARKET_SNAPSHOT',
  'HOLDER_SNAPSHOT',
  'DEX_RANK_CHANGE',
  'FOMO_RANK_CHANGE',
  'SOCIAL_ACCELERATION',
  'AMPLIFIER_ENTRY',
  'DISTRIBUTION_SIGNAL',
  'RUG_SIGNAL',
]);

const SIGNAL_STATES = Object.freeze(['REJECT', 'WATCH', 'ENTRY', 'DE_RISK', 'EXIT']);

module.exports = {
  FEATURE_VERSION,
  STRATEGY_VERSION,
  SOURCE_PERFORMANCE_VERSION,
  RESEARCH_DEFINITION_VERSION,
  RESEARCH_OBSERVATION_TYPES,
  SIGNAL_EVENT_TYPES,
  SIGNAL_STATES,
};
