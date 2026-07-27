'use strict';

const { ACTION_TYPES } = require('../CommandDeckTypes');
const { buildIntelligenceCard, CARD_TYPES } = require('../cards/IntelligenceCard');

/**
 * Morning Brief — summarize briefing.summary into a landing headline block.
 * Never invents counts; only surfaces assembled briefing fields.
 *
 * @param {object} input
 * @param {object} input.briefing
 * @param {string} input.briefingId
 * @param {string} input.generatedAt
 */
function buildMorningBrief(input) {
  const summary = (input.briefing && input.briefing.summary) || {};
  const priorities = (input.briefing && input.briefing.priorities) || [];
  const watchAlerts =
    (input.briefing && input.briefing.watchAlerts && input.briefing.watchAlerts.items) ||
    [];
  const changes = (input.briefing && input.briefing.changes) || {};

  const marketChanges = Number(changes.total) || 0;
  const watchAlertCount = watchAlerts.length;
  const priorityCount = priorities.length;
  const companiesMonitored = Number(summary.companiesMonitored) || 0;
  const companiesWithMemory = Number(summary.companiesWithMemory) || 0;
  const generatedAt = input.generatedAt || summary.asOf || null;

  const headline = deriveHeadline({
    marketChanges,
    watchAlertCount,
    priorityCount,
    priorityOpportunities: Number(summary.priorityOpportunities) || 0,
    newDecisionMakers: Number(summary.newDecisionMakers) || 0,
    companiesMonitored,
    companiesWithMemory,
  });

  const summaryText = deriveSummaryText({
    companiesMonitored,
    companiesWithMemory,
    marketChanges,
    watchAlertCount,
    priorityCount,
    priorityOpportunities: Number(summary.priorityOpportunities) || 0,
    newDecisionMakers: Number(summary.newDecisionMakers) || 0,
    newHiringSignals: Number(summary.newHiringSignals) || 0,
  });

  const morningBrief = {
    headline,
    summary: summaryText,
    marketChanges,
    watchAlertCount,
    priorityCount,
    generatedAt,
    marketContext: marketContextStatus({ companiesMonitored, companiesWithMemory }),
  };

  const card = buildIntelligenceCard({
    id: 'card:morning_brief',
    type: CARD_TYPES.MORNING_BRIEF,
    priority: 1000,
    title: headline,
    summary: summaryText,
    confidence: null,
    updatedAt: generatedAt,
    actions: [
      {
        id: 'ask_max',
        type: ACTION_TYPES.ASK_MAX,
        label: 'Ask Max',
        payload: { context: 'morning_brief' },
      },
    ],
    sources: [
      { kind: 'briefing', id: input.briefingId, field: 'summary' },
      { kind: 'briefing', id: input.briefingId, field: 'changes' },
      { kind: 'briefing', id: input.briefingId, field: 'watchAlerts' },
      { kind: 'briefing', id: input.briefingId, field: 'priorities' },
    ],
    reasoningId: null,
    policyId: null,
    briefingId: input.briefingId,
    payload: morningBrief,
  });

  return { morningBrief, card };
}

function deriveHeadline(stats) {
  if (stats.marketChanges > 0) {
    return 'Your market shifted overnight.';
  }
  if (stats.watchAlertCount > 0) {
    return 'Watch alerts require attention.';
  }
  if (stats.priorityOpportunities > 0 || stats.priorityCount > 0) {
    return 'Priority opportunities are ready.';
  }
  if (stats.newDecisionMakers > 0) {
    return 'New decision-makers entered the window.';
  }
  if (stats.companiesMonitored === 0) {
    return 'Market intelligence is not available yet.';
  }
  if (stats.companiesWithMemory === 0) {
    return 'Market context is still building.';
  }
  return 'No material market movement was recorded this morning.';
}

function deriveSummaryText(stats) {
  const parts = [];
  if (stats.companiesMonitored > 0) {
    parts.push(`${stats.companiesMonitored} companies monitored.`);
  }
  if (stats.companiesMonitored > 0 && stats.companiesWithMemory === 0) {
    parts.push('No historical snapshots are available yet.');
  }
  if (stats.marketChanges > 0) {
    parts.push(`${stats.marketChanges} market changes in period.`);
  }
  if (stats.priorityOpportunities > 0) {
    parts.push(`${stats.priorityOpportunities} priority opportunities.`);
  } else if (stats.priorityCount > 0) {
    parts.push(`${stats.priorityCount} ranked priorities.`);
  }
  if (stats.watchAlertCount > 0) {
    parts.push(`${stats.watchAlertCount} watch alerts.`);
  }
  if (stats.newDecisionMakers > 0) {
    parts.push(`${stats.newDecisionMakers} new decision-makers.`);
  }
  if (stats.newHiringSignals > 0) {
    parts.push(`${stats.newHiringSignals} hiring signals.`);
  }
  if (parts.length === 0) {
    return 'No monitored companies or historical market snapshots are available in this window.';
  }
  return parts.join(' ');
}

function marketContextStatus({ companiesMonitored, companiesWithMemory }) {
  if (companiesMonitored === 0) return 'unavailable';
  if (companiesWithMemory === 0) return 'building';
  return 'ready';
}

module.exports = {
  buildMorningBrief,
};
