'use strict';

const { DEFAULT_CHANGE_LIMIT } = require('../BriefingTypes');
const { CHANGE_TYPES } = require('../../memory/snapshots/MemoryTypes');

/**
 * What's Changed — powered entirely by Memory ChangeEvents.
 */
function buildChangesSection(contexts, options = {}) {
  const limit =
    options.limit != null ? Number(options.limit) : DEFAULT_CHANGE_LIMIT;

  /** @type {object[]} */
  const entries = [];
  for (const ctx of contexts || []) {
    if (!ctx.changes || ctx.changes.length === 0) continue;
    for (const change of ctx.changes) {
      entries.push({
        companyId: ctx.companyId,
        companyName: ctx.companyName,
        type: change.type,
        magnitude: change.magnitude != null ? change.magnitude : null,
        details: change.details || {},
        fromSnapshotId: ctx.baseline ? ctx.baseline.id : null,
        toSnapshotId: ctx.latest ? ctx.latest.id : null,
        scoreBefore: ctx.diff ? ctx.diff.scoreBefore : null,
        scoreAfter: ctx.diff ? ctx.diff.scoreAfter : null,
        confidenceBefore: ctx.diff ? ctx.diff.confidenceBefore : null,
        confidenceAfter: ctx.diff ? ctx.diff.confidenceAfter : null,
        summary: summarizeChange(change),
      });
    }
  }

  entries.sort(compareChanges);

  const byType = {};
  for (const e of entries) {
    byType[e.type] = (byType[e.type] || 0) + 1;
  }

  return {
    total: entries.length,
    byType: sortObjectKeys(byType),
    highlights: buildHighlights(entries),
    items: entries.slice(0, limit),
  };
}

function summarizeChange(change) {
  const mag = change.magnitude != null ? `:magnitude=${change.magnitude}` : '';
  const detailKeys = Object.keys(change.details || {})
    .sort()
    .filter((k) => k !== 'magnitude' && k !== 'message');
  const extras = detailKeys
    .map((k) => `${k}=${change.details[k]}`)
    .join(',');
  return extras ? `${change.type}${mag}:${extras}` : `${change.type}${mag}`;
}

function buildHighlights(entries) {
  const interesting = new Set([
    CHANGE_TYPES.NEW_HIRING_SIGNAL,
    CHANGE_TYPES.NEW_DECISION_MAKER,
    CHANGE_TYPES.NEW_OPPORTUNITY_SIGNAL,
    CHANGE_TYPES.NEW_CONTRADICTION,
    CHANGE_TYPES.SCORE_INCREASED,
    CHANGE_TYPES.SCORE_DECREASED,
    CHANGE_TYPES.CONFIDENCE_DECREASED,
    CHANGE_TYPES.PRIORITY_CHANGED,
  ]);
  return entries
    .filter((e) => interesting.has(e.type))
    .slice(0, 20)
    .map((e) => ({
      companyId: e.companyId,
      companyName: e.companyName,
      type: e.type,
      summary: e.summary,
      magnitude: e.magnitude,
    }));
}

function compareChanges(a, b) {
  const magA = Number(a.magnitude) || 0;
  const magB = Number(b.magnitude) || 0;
  if (magB !== magA) return magB - magA;
  const t = String(a.type).localeCompare(String(b.type));
  if (t !== 0) return t;
  return String(a.companyId).localeCompare(String(b.companyId));
}

function sortObjectKeys(obj) {
  const out = {};
  for (const key of Object.keys(obj).sort()) {
    out[key] = obj[key];
  }
  return out;
}

module.exports = {
  buildChangesSection,
};
