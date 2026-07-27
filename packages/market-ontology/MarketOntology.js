'use strict';

const { createDomainOntology } = require('@pulseforge/knowledge/ontology');

const MARKET_ENTITY_TYPES = Object.freeze({
  ASSET: 'asset',
  EXCHANGE: 'exchange',
  CONTRACT: 'contract',
  MARKET: 'market',
  MARKET_SESSION: 'market_session',
  ECONOMIC_CALENDAR: 'economic_calendar',
  NEWS_SOURCE: 'news_source',
  INDICATOR: 'indicator',
});

const MARKET_SUBJECT_TYPES = Object.freeze({
  ASSET: 'asset',
  CONTRACT: 'contract',
});

const MARKET_OBSERVATION_TYPES = Object.freeze({
  PRICE_TICK: 'price_tick',
  VOLUME_UPDATE: 'volume_update',
  VOLATILITY_OBSERVATION: 'volatility_observation',
  FUNDING_UPDATE: 'funding_update',
  LIQUIDATION: 'liquidation',
  NEWS_EVENT: 'news_event',
  ECONOMIC_RELEASE: 'economic_release',
  SESSION_TRANSITION: 'session_transition',
  INDICATOR_SNAPSHOT: 'indicator_snapshot',
});

const MARKET_CLAIM_VOCABULARY = Object.freeze([
  { id: 'momentum_continuation', label: 'Momentum Continuation' },
  { id: 'momentum_exhaustion', label: 'Momentum Exhaustion' },
  { id: 'elevated_volatility', label: 'Elevated Volatility' },
  { id: 'regime_transition', label: 'Regime Transition' },
  { id: 'mean_reversion', label: 'Mean Reversion' },
  { id: 'liquidity_contraction', label: 'Liquidity Contraction' },
  { id: 'news_driven_expansion', label: 'News Expansion' },
]);

const MARKET_OUTCOME_VOCABULARY = Object.freeze([
  { id: 'trend_continued', label: 'Trend Continued' },
  { id: 'trend_failed', label: 'Trend Failed' },
  { id: 'volatility_expanded', label: 'Volatility Expanded' },
  { id: 'volatility_contracted', label: 'Volatility Contracted' },
  { id: 'range_held', label: 'Range Held' },
  { id: 'breakout_confirmed', label: 'Breakout Confirmed' },
  { id: 'breakout_failed', label: 'Breakout Failed' },
]);

const MARKET_RELATIONSHIP_TYPES = Object.freeze({
  TRADES_ON: 'TRADES_ON',
});

function createMarketOntology() {
  return createDomainOntology({
    id: 'market',
    label: 'Market',
    entityTypes: Object.values(MARKET_ENTITY_TYPES),
    subjectTypes: Object.values(MARKET_SUBJECT_TYPES),
    relationshipTypes: Object.values(MARKET_RELATIONSHIP_TYPES),
    observationTypes: Object.values(MARKET_OBSERVATION_TYPES),
    claimVocabulary: [...MARKET_CLAIM_VOCABULARY],
    outcomeVocabulary: [...MARKET_OUTCOME_VOCABULARY],
  });
}

module.exports = {
  MARKET_ENTITY_TYPES,
  MARKET_SUBJECT_TYPES,
  MARKET_OBSERVATION_TYPES,
  MARKET_CLAIM_VOCABULARY,
  MARKET_OUTCOME_VOCABULARY,
  MARKET_RELATIONSHIP_TYPES,
  createMarketOntology,
};
