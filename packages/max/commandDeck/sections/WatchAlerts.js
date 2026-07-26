'use strict';

const {
  ACTION_TYPES,
  CARD_TYPES,
  DEFAULT_WATCH_ALERT_LIMIT,
  WATCH_SEVERITY_RANK,
} = require('../CommandDeckTypes');
const { buildIntelligenceCard } = require('../cards/IntelligenceCard');

/**
 * Watch Alerts — ordered, deduplicated, severity-ranked, newest first.
 *
 * @param {object} input
 * @param {object} input.briefing
 * @param {string} input.briefingId
 * @param {string} input.generatedAt
 * @param {number} [input.limit]
 */
function composeWatchAlerts(input) {
  const raw =
    (input.briefing &&
      input.briefing.watchAlerts &&
      input.briefing.watchAlerts.items) ||
    [];
  const limit =
    input.limit != null ? Number(input.limit) : DEFAULT_WATCH_ALERT_LIMIT;

  const deduped = dedupeAlerts(raw);
  const ranked = deduped
    .map((alert) => enrichAlert(alert))
    .sort(compareWatchAlerts)
    .slice(0, limit);

  const cards = ranked.map((alert, index) => {
    const cardId = `card:watch:${alert.dedupeKey}`;
    return buildIntelligenceCard({
      id: cardId,
      type: CARD_TYPES.WATCH_ALERT,
      priority: 700 - index,
      title: alert.title,
      summary: alert.message || alert.title,
      confidence: null,
      updatedAt: alert.at || input.generatedAt,
      actions: [
        {
          id: 'ask_max',
          type: ACTION_TYPES.ASK_MAX,
          label: 'Ask Max',
          payload: {
            context: 'watch_alert',
            watchId: alert.watchId,
            companyId: alert.companyId,
          },
        },
        alert.companyId
          ? {
              id: 'open_company',
              type: ACTION_TYPES.OPEN_COMPANY,
              label: 'Open Company',
              payload: { companyId: alert.companyId },
            }
          : null,
        {
          id: 'dismiss',
          type: ACTION_TYPES.DISMISS,
          label: 'Dismiss',
          payload: { cardId, watchId: alert.watchId },
        },
        {
          id: 'snooze',
          type: ACTION_TYPES.SNOOZE,
          label: 'Snooze',
          payload: { cardId, watchId: alert.watchId },
        },
      ].filter(Boolean),
      sources: [
        { kind: 'briefing', id: input.briefingId, field: 'watchAlerts' },
        { kind: 'watch', id: String(alert.watchId) },
        alert.snapshotId
          ? { kind: 'snapshot', id: String(alert.snapshotId) }
          : null,
      ].filter(Boolean),
      reasoningId: null,
      policyId: null,
      briefingId: input.briefingId,
      payload: {
        watchId: alert.watchId,
        companyId: alert.companyId,
        companyName: alert.companyName,
        severity: alert.severity,
        at: alert.at,
        scoreDelta: alert.scoreDelta,
        confidenceDelta: alert.confidenceDelta,
        condition: alert.condition,
      },
    });
  });

  return { watchAlerts: cards, items: ranked };
}

function dedupeAlerts(alerts) {
  const seen = new Map();
  for (const alert of alerts || []) {
    const key = [
      alert.watchId != null ? String(alert.watchId) : '',
      alert.companyId != null ? String(alert.companyId) : '',
      alert.targetId != null ? String(alert.targetId) : '',
      alert.message != null ? String(alert.message) : '',
    ].join('|');
    if (!seen.has(key)) {
      seen.set(key, { ...alert, dedupeKey: key });
    }
  }
  return [...seen.values()];
}

function enrichAlert(alert) {
  const severity = deriveSeverity(alert);
  const companyLabel = alert.companyName || alert.companyId || 'company';
  const title =
    alert.message ||
    `Watch ${alert.watchId} · ${companyLabel}`;
  return {
    ...alert,
    severity,
    title,
  };
}

function deriveSeverity(alert) {
  const scoreDelta = Math.abs(Number(alert.scoreDelta) || 0);
  const confDelta = Math.abs(Number(alert.confidenceDelta) || 0);
  const magnitude = Math.max(scoreDelta, confDelta);
  if (magnitude >= 25) return 'critical';
  if (magnitude >= 15) return 'high';
  if (magnitude >= 8) return 'medium';
  if (magnitude > 0) return 'low';
  return 'none';
}

function compareWatchAlerts(a, b) {
  const sevA = WATCH_SEVERITY_RANK[a.severity] || 0;
  const sevB = WATCH_SEVERITY_RANK[b.severity] || 0;
  if (sevB !== sevA) return sevB - sevA;
  const atA = Date.parse(a.at || '') || 0;
  const atB = Date.parse(b.at || '') || 0;
  if (atB !== atA) return atB - atA;
  const w = String(a.watchId || '').localeCompare(String(b.watchId || ''));
  if (w !== 0) return w;
  return String(a.companyId || '').localeCompare(String(b.companyId || ''));
}

module.exports = {
  composeWatchAlerts,
  dedupeAlerts,
  deriveSeverity,
  compareWatchAlerts,
};
