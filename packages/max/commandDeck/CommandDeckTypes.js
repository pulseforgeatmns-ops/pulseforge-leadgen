'use strict';

/**
 * Command Deck Composition types (SPEC-007 / v0.9.2).
 * Composer never reasons — it assembles Briefing + Policy into one view model.
 */

const CARD_TYPES = Object.freeze({
  MORNING_BRIEF: 'morning_brief',
  HIGHEST_LEVERAGE: 'highest_leverage',
  WATCH_ALERT: 'watch_alert',
  MARKET_TREND: 'market_trend',
  PRIORITY_ITEM: 'priority_item',
  EMPTY: 'empty',
});

const ACTION_TYPES = Object.freeze({
  REVIEW_RECOMMENDATION: 'review_recommendation',
  ASK_MAX: 'ask_max',
  OPEN_COMPANY: 'open_company',
  OPEN_MISSION: 'open_mission',
  DISMISS: 'dismiss',
  SNOOZE: 'snooze',
});

const TREND_DIRECTIONS = Object.freeze({
  UP: 'up',
  DOWN: 'down',
  FLAT: 'flat',
  INSUFFICIENT: 'insufficient',
});

/** Target latency for a full Command Deck compose (ms). */
const COMMAND_DECK_PERFORMANCE_TARGET_MS = 2500;

/** Default max watch alerts surfaced. */
const DEFAULT_WATCH_ALERT_LIMIT = 25;

/** Default max market trends surfaced. */
const DEFAULT_MARKET_TREND_LIMIT = 10;

/** Default max priority queue items. */
const DEFAULT_PRIORITY_QUEUE_LIMIT = 25;

/** Severity rank for watch alert ordering (higher first). */
const WATCH_SEVERITY_RANK = Object.freeze({
  critical: 4,
  high: 3,
  medium: 2,
  low: 1,
  none: 0,
});

module.exports = {
  CARD_TYPES,
  ACTION_TYPES,
  TREND_DIRECTIONS,
  COMMAND_DECK_PERFORMANCE_TARGET_MS,
  DEFAULT_WATCH_ALERT_LIMIT,
  DEFAULT_MARKET_TREND_LIMIT,
  DEFAULT_PRIORITY_QUEUE_LIMIT,
  WATCH_SEVERITY_RANK,
};
