'use strict';

/**
 * Deterministic aggregates for Content Outcome Intelligence.
 * No recommendations — SPEC-092 records reality only.
 */

/**
 * @param {number[]} values
 * @returns {number|null}
 */
function median(values) {
  const nums = values.filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
  if (!nums.length) return null;
  const mid = Math.floor(nums.length / 2);
  if (nums.length % 2 === 0) return (nums[mid - 1] + nums[mid]) / 2;
  return nums[mid];
}

/**
 * @param {number[]} values
 * @returns {number|null}
 */
function average(values) {
  const nums = values.filter((v) => Number.isFinite(v));
  if (!nums.length) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

/**
 * Latest snapshot per publication (by observed_at), then aggregate.
 * @param {object[]} publications
 * @param {object[]} snapshots
 * @param {object[]} businessOutcomes
 * @returns {object}
 */
function buildComparisonSummary(publications, snapshots, businessOutcomes) {
  const pubs = Array.isArray(publications) ? publications : [];
  const snaps = Array.isArray(snapshots) ? snapshots : [];
  const outcomes = Array.isArray(businessOutcomes) ? businessOutcomes : [];

  const latestByPub = new Map();
  for (const snap of snaps) {
    const key = String(snap.publication_id || snap.publicationId);
    const prev = latestByPub.get(key);
    const at = new Date(snap.observed_at || snap.observedAt).getTime();
    if (!prev || at >= prev._at) {
      latestByPub.set(key, { ...snap, _at: at });
    }
  }

  const impressions = [];
  const comments = [];
  for (const snap of latestByPub.values()) {
    if (snap.impressions != null) impressions.push(Number(snap.impressions));
    if (snap.comments != null) comments.push(Number(snap.comments));
  }

  const countType = (type) =>
    outcomes.filter((o) => String(o.outcome_type || o.outcomeType) === type).length;

  return {
    total_publications: pubs.length,
    median_impressions: median(impressions),
    average_comments: average(comments),
    total_qualified_conversations: countType('qualified_dm') + countType('prospect_conversation'),
    total_partner_conversations: countType('partner_conversation'),
    total_meetings: countType('meeting_booked'),
    total_business_outcomes: outcomes.length,
  };
}

/**
 * Group publications by a known dimension.
 * @param {object[]} publications
 * @param {'objective'|'topic'|'format'|'intended_audience'} dimension
 * @param {object[]} snapshots
 * @param {object[]} businessOutcomes
 */
function groupByDimension(publications, dimension, snapshots, businessOutcomes) {
  const groups = new Map();

  for (const pub of publications || []) {
    let keys;
    if (dimension === 'intended_audience') {
      const audience = pub.intended_audience || pub.intendedAudience || [];
      keys = audience.length ? audience.map(String) : ['(none)'];
    } else {
      const raw = pub[dimension] ?? pub[toCamel(dimension)];
      keys = [raw != null && String(raw).trim() ? String(raw) : '(none)'];
    }
    for (const key of keys) {
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(pub);
    }
  }

  const result = {};
  for (const [key, pubs] of groups.entries()) {
    const ids = new Set(pubs.map((p) => String(p.id)));
    const snaps = (snapshots || []).filter((s) =>
      ids.has(String(s.publication_id || s.publicationId))
    );
    const outs = (businessOutcomes || []).filter((o) =>
      ids.has(String(o.publication_id || o.publicationId))
    );
    result[key] = buildComparisonSummary(pubs, snaps, outs);
  }
  return result;
}

function toCamel(snake) {
  return String(snake).replace(/_([a-z])/g, (_, c) => c.toUpperCase());
}

module.exports = {
  median,
  average,
  buildComparisonSummary,
  groupByDimension,
};
