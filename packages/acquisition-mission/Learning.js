'use strict';

/**
 * SPEC-118 — mission outcomes become reusable intelligence.
 * Never auto-applied to a live mission.
 */

const { asText, nowIso, newId, round2, pct, clone } = require('./types');

function recordSegmentOutcome(input = {}) {
  const sends = Number(input.sends || 0);
  const replies = Number(input.replies || 0);
  const replyRate = input.replyRate != null ? pct(input.replyRate) : (sends ? replies / sends : 0);
  return {
    id: asText(input.id) || newId('learn'),
    tenantId: asText(input.tenantId),
    missionId: asText(input.missionId) || null,
    segment: asText(input.segment || input.vertical),
    sends,
    replies,
    replyRate: round2(replyRate),
    statement: asText(input.statement) || null,
    autoApplied: false,
    at: nowIso(input.at || input.now),
  };
}

function summarizeLearning(rows = []) {
  const bySegment = new Map();
  for (const row of rows) {
    const key = asText(row.segment) || 'unknown';
    const current = bySegment.get(key) || { segment: key, sends: 0, replies: 0 };
    current.sends += Number(row.sends || 0);
    current.replies += Number(row.replies || 0);
    bySegment.set(key, current);
  }
  const segments = [...bySegment.values()]
    .map((row) => ({
      ...row,
      replyRate: row.sends ? round2(row.replies / row.sends) : 0,
    }))
    .sort((a, b) => b.replyRate - a.replyRate);

  const best = segments[0] || null;
  const recommendation = best && segments.length > 1
    ? `Increase ${best.segment} allocation.`
    : (best ? `Continue ${best.segment} allocation.` : null);

  const operational = rows.find((row) => /operational/i.test(row.statement || ''));
  const learningSummary = operational
    ? operational.statement
    : (best && segments[1] && best.replyRate > (segments[1].replyRate || 0)
      ? `${capitalize(best.segment)} responding better.`
      : null);

  return {
    segments,
    recommendation,
    learningSummary,
    autoApplied: false,
  };
}

function formatLearning(summary) {
  const lines = ['Mission Learning', ''];
  for (const row of summary.segments || []) {
    lines.push(capitalize(row.segment), 'Reply Rate', `${Math.round(row.replyRate * 100)}%`, '');
  }
  if (summary.recommendation) {
    lines.push('Recommendation', '', summary.recommendation);
  }
  return lines.join('\n').trim();
}

function capitalize(value) {
  const text = String(value || '');
  return text ? text.charAt(0).toUpperCase() + text.slice(1).replace(/_/g, ' ') : text;
}

module.exports = {
  recordSegmentOutcome,
  summarizeLearning,
  formatLearning,
  clone,
};
