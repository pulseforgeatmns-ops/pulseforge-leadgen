'use strict';

const {
  BriefingEngine,
  createBriefingEngine,
  BriefingBuilder,
  DigestBuilder,
  Prioritizer,
} = require('./BriefingEngine');
const {
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
} = require('./BriefingTypes');
const { resolvePeriodWindow } = require('./digest/PeriodWindow');
const {
  deriveUrgency,
  deriveContradictionSeverity,
} = require('./priorities/Prioritizer');
const {
  PresentationAdapter,
  StructuredPresentationAdapter,
  MarkdownPresentationAdapter,
  createPresentationAdapter,
} = require('./presentation/PresentationAdapter');
const { applyBriefingTemplate } = require('./templates/BriefingTemplate');
const { RISK_KIND } = require('./sections/RisksSection');

module.exports = {
  BriefingEngine,
  createBriefingEngine,
  BriefingBuilder,
  DigestBuilder,
  Prioritizer,
  deriveUrgency,
  deriveContradictionSeverity,
  resolvePeriodWindow,
  PresentationAdapter,
  StructuredPresentationAdapter,
  MarkdownPresentationAdapter,
  createPresentationAdapter,
  applyBriefingTemplate,
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
  RISK_KIND,
};
