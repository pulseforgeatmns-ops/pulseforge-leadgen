'use strict';

const {
  CommandDeckComposer,
  createCommandDeckComposer,
} = require('./CommandDeckComposer');
const {
  CARD_TYPES,
  ACTION_TYPES,
  TREND_DIRECTIONS,
  COMMAND_DECK_PERFORMANCE_TARGET_MS,
  DEFAULT_WATCH_ALERT_LIMIT,
  DEFAULT_MARKET_TREND_LIMIT,
  DEFAULT_PRIORITY_QUEUE_LIMIT,
  WATCH_SEVERITY_RANK,
} = require('./CommandDeckTypes');
const {
  buildIntelligenceCard,
  normalizeAction,
  recommendationActions,
  buildBriefingId,
} = require('./cards/IntelligenceCard');
const {
  EMPTY_CATALOG,
  buildEmptyStateCard,
  buildEmptyStates,
} = require('./empty/EmptyStates');
const { buildMorningBrief } = require('./sections/MorningBrief');
const {
  buildHighestLeverageAction,
} = require('./sections/HighestLeverageAction');
const {
  composeWatchAlerts,
  dedupeAlerts,
  deriveSeverity,
} = require('./sections/WatchAlerts');
const {
  composeMarketTrends,
  deriveDirection,
} = require('./sections/MarketTrends');
const {
  composePriorityQueue,
  formatMovement,
} = require('./sections/PriorityQueue');
const { composeOperations } = require('./sections/Operations');

module.exports = {
  CommandDeckComposer,
  createCommandDeckComposer,
  CARD_TYPES,
  ACTION_TYPES,
  TREND_DIRECTIONS,
  COMMAND_DECK_PERFORMANCE_TARGET_MS,
  DEFAULT_WATCH_ALERT_LIMIT,
  DEFAULT_MARKET_TREND_LIMIT,
  DEFAULT_PRIORITY_QUEUE_LIMIT,
  WATCH_SEVERITY_RANK,
  buildIntelligenceCard,
  normalizeAction,
  recommendationActions,
  buildBriefingId,
  EMPTY_CATALOG,
  buildEmptyStateCard,
  buildEmptyStates,
  buildMorningBrief,
  buildHighestLeverageAction,
  composeWatchAlerts,
  dedupeAlerts,
  deriveSeverity,
  composeMarketTrends,
  deriveDirection,
  composePriorityQueue,
  formatMovement,
  composeOperations,
};
