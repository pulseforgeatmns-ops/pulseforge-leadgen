'use strict';

/**
 * Learning Engine — evidence-backed learnings only (SPEC-036 / ADR-023).
 */

const {
  RESPONSE_OUTCOMES,
  LEARNING_STATUS,
  MIN_EVIDENCE_SAMPLES,
  MIN_EVIDENCE_LIFT,
  buildLearning,
} = require('./types');

function newId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Generate structured learnings from outcome records.
 * Only segments with enough samples AND measurable lift become evidence_backed.
 *
 * @param {object[]} outcomes
 * @param {object} [opts]
 * @returns {object[]}
 */
function generateLearnings(outcomes, opts = {}) {
  const list = Array.isArray(outcomes) ? outcomes : [];
  if (!list.length) return [];

  const baseline = responseRate(list);
  const learnings = [];

  const dimensions = [
    { key: 'vertical', label: 'vertical', attr: (o) => o.vertical || o.industry },
    { key: 'industry', label: 'industry', attr: (o) => o.industry || o.vertical },
    { key: 'region', label: 'region', attr: (o) => o.region },
    {
      key: 'personalization',
      label: 'personalization',
      attr: (o) =>
        o.attributes && o.attributes.handwritten
          ? 'handwritten'
          : o.attributes && o.attributes.personalization
            ? String(o.attributes.personalization)
            : null,
    },
    {
      key: 'mail_day',
      label: 'mail_day',
      attr: (o) =>
        o.attributes && o.attributes.mailDay
          ? String(o.attributes.mailDay).toLowerCase()
          : null,
    },
  ];

  for (const dim of dimensions) {
    const groups = groupBy(list, dim.attr);
    for (const [segment, rows] of groups) {
      if (!segment || rows.length < 1) continue;
      const rate = responseRate(rows);
      const lift =
        baseline > 0 ? Math.round((rate / baseline - 1) * 1000) / 1000 : null;
      const sampleSize = rows.length;
      const evidenceOk =
        sampleSize >= (opts.minSamples || MIN_EVIDENCE_SAMPLES) &&
        lift != null &&
        Math.abs(lift) >= (opts.minLift || MIN_EVIDENCE_LIFT);

      const direction =
        lift != null && lift >= 0 ? 'outperform' : 'underperform';
      const liftText =
        lift != null && baseline > 0
          ? `${(Math.abs(rate / baseline)).toFixed(1)}× baseline`
          : `${Math.round(rate * 100)}% response`;

      const statement =
        dim.key === 'personalization' && segment === 'handwritten'
          ? evidenceOk && lift > 0
            ? `Handwritten notes increase response rate (${liftText}).`
            : `Handwritten personalization observed (${sampleSize} samples) — insufficient evidence to promote.`
          : evidenceOk
            ? `${titleCase(segment)} ${direction}s (${liftText}) on ${dim.label}.`
            : `${titleCase(segment)} ${dim.label} segment has ${sampleSize} sample(s) — candidate only.`;

      learnings.push(
        buildLearning({
          id: newId('lrn'),
          statement,
          dimension: dim.key,
          segment,
          baselineRate: baseline,
          segmentRate: rate,
          lift,
          sampleSize,
          evidenceIds: rows.map((r) => r.id).filter(Boolean),
          status: evidenceOk
            ? LEARNING_STATUS.EVIDENCE_BACKED
            : LEARNING_STATUS.CANDIDATE,
          confidence: evidenceOk
            ? Math.min(0.95, 0.5 + sampleSize * 0.05 + Math.abs(lift || 0) * 0.2)
            : Math.min(0.4, sampleSize * 0.08),
        })
      );
    }
  }

  // Prefer evidence_backed, then higher |lift|, then sample size
  return learnings.sort((a, b) => {
    const rank = (s) =>
      s === LEARNING_STATUS.EVIDENCE_BACKED
        ? 2
        : s === LEARNING_STATUS.CANDIDATE
          ? 1
          : 0;
    const rd = rank(b.status) - rank(a.status);
    if (rd) return rd;
    const ld = Math.abs(b.lift || 0) - Math.abs(a.lift || 0);
    if (ld) return ld;
    return b.sampleSize - a.sampleSize;
  });
}

/**
 * @param {object[]} outcomes
 * @returns {number}
 */
function responseRate(outcomes) {
  const n = outcomes.length;
  if (!n) return 0;
  const hits = outcomes.filter((o) => RESPONSE_OUTCOMES.has(o.outcomeType)).length;
  return Math.round((hits / n) * 1000) / 1000;
}

/**
 * @param {object[]} rows
 * @param {(row: object) => string|null} attrFn
 * @returns {Map<string, object[]>}
 */
function groupBy(rows, attrFn) {
  /** @type {Map<string, object[]>} */
  const map = new Map();
  for (const row of rows) {
    const key = attrFn(row);
    if (key == null || key === '') continue;
    const k = String(key).toLowerCase();
    const list = map.get(k) || [];
    list.push(row);
    map.set(k, list);
  }
  return map;
}

/**
 * @param {string} s
 * @returns {string}
 */
function titleCase(s) {
  return String(s || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

module.exports = {
  generateLearnings,
  responseRate,
  MIN_EVIDENCE_SAMPLES,
  MIN_EVIDENCE_LIFT,
};
