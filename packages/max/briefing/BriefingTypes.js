'use strict';

/**
 * Briefing Engine types (SPEC-004 / v0.9.0).
 * Briefing never computes — it assembles Knowledge + Reasoning + Memory.
 */

const BRIEFING_PERIODS = Object.freeze({
  DAILY: 'daily',
  WEEKLY: 'weekly',
  MONTHLY: 'monthly',
});

const BRIEFING_SECTIONS = Object.freeze([
  'summary',
  'priorities',
  'changes',
  'watchAlerts',
  'risks',
  'recommendations',
  'metrics',
]);

/** Target latency for a full tenant briefing (ms). */
const BRIEFING_PERFORMANCE_TARGET_MS = 2000;

/** Default priority-queue size. */
const DEFAULT_PRIORITY_LIMIT = 25;

/** Default recommendations list size. */
const DEFAULT_RECOMMENDATION_LIMIT = 25;

/** Default risks list size. */
const DEFAULT_RISK_LIMIT = 15;

/** Default change-summary size. */
const DEFAULT_CHANGE_LIMIT = 50;

/**
 * Deterministic prioritization weights (must sum conceptually; used as absolute).
 * Priority considers: score, confidence, trend, urgency, contradiction severity.
 */
const PRIORITY_WEIGHTS = Object.freeze({
  score: 0.4,
  confidence: 0.2,
  trend: 0.15,
  urgency: 0.15,
  contradictionSeverity: 0.1,
});

const TREND_SCORE = Object.freeze({
  up: 100,
  flat: 50,
  insufficient: 40,
  down: 10,
});

const RISK_CHANGE_TYPES = Object.freeze([
  'score_decreased',
  'confidence_decreased',
  'strategy_score_down',
  'new_contradiction',
  'removed_claim',
  'removed_evidence',
  'priority_changed',
  'type_changed',
  'action_changed',
]);

module.exports = {
  BRIEFING_PERIODS,
  BRIEFING_SECTIONS,
  BRIEFING_PERFORMANCE_TARGET_MS,
  DEFAULT_PRIORITY_LIMIT,
  DEFAULT_RECOMMENDATION_LIMIT,
  DEFAULT_RISK_LIMIT,
  DEFAULT_CHANGE_LIMIT,
  PRIORITY_WEIGHTS,
  TREND_SCORE,
  RISK_CHANGE_TYPES,
};
