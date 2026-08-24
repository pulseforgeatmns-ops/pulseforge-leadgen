'use strict';

/**
 * SPEC-146 — Evidence Conflict Resolution Engine.
 * Applies freshness, authority, majority, context, and operator escalation strategies.
 */

const { evidenceWeight, evidenceSourceLabel } = require('../credibility/EvidenceWeights');
const { classifyFreshness, evidenceAgeDays } = require('../credibility/EvidenceFreshness');
const {
  CONFLICT_CATEGORIES,
  CONFLICT_SEVERITY,
  RESOLUTION_STRATEGIES,
  buildEvidenceConflict,
} = require('./types');
const { subjectLabel } = require('./ConflictDetection');

function round2(n) {
  return Number(Number(n).toFixed(2));
}

function normalizeSource(source) {
  return String(source || 'unknown')
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
}

function scoreClaim(claim, now = new Date()) {
  const source = normalizeSource(claim.source);
  const authority = evidenceWeight(source);
  const freshness = classifyFreshness(claim.observedAt, now);
  return {
    source,
    authority,
    freshnessScore: freshness.multiplier,
    ageDays: freshness.ageDays,
    effectiveScore: authority * freshness.multiplier,
    claim,
  };
}

function numericValues(claims = []) {
  return claims.map((c) => c.value).filter((v) => typeof v === 'number');
}

function buildWorkingEstimate(subject, values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  if (sorted.length === 1) return String(sorted[0]);
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  if (min === max) return String(min);
  return `≈${min}–${max}`;
}

function tryFreshnessResolution(conflict, now = new Date()) {
  const claims = conflict.conflictingClaims || [];
  if (claims.length < 2) return null;

  const scored = claims.map((c) => scoreClaim(c, now));
  const withDates = scored.filter((s) => s.ageDays != null);
  if (withDates.length < 2) return null;

  const sorted = [...withDates].sort((a, b) => (a.ageDays ?? 9999) - (b.ageDays ?? 9999));
  const freshest = sorted[0];
  const stalest = sorted[sorted.length - 1];
  const ageGap = (stalest.ageDays ?? 0) - (freshest.ageDays ?? 0);

  if (ageGap < 30) return null;

  const numeric = numericValues(claims);
  const workingEstimate =
    numeric.length >= 2
      ? buildWorkingEstimate(conflict.subject, numeric.filter((v) => {
          const claim = claims.find((c) => c.value === v);
          const s = scoreClaim(claim || {}, now);
          return s.ageDays != null && s.ageDays <= (freshest.ageDays ?? 0) + 30;
        }))
      : asText(freshest.claim.value);

  const freshSources = withDates
    .filter((s) => s.ageDays != null && s.ageDays <= (freshest.ageDays ?? 0) + 30)
    .map((s) => evidenceSourceLabel(s.source));

  const staleSources = withDates
    .filter((s) => s.ageDays != null && s.ageDays > (freshest.ageDays ?? 0) + 30)
    .map((s) => evidenceSourceLabel(s.source));

  if (!freshSources.length) return null;

  return {
    strategy: RESOLUTION_STRATEGIES.FRESHNESS,
    workingEstimate,
    reason: [
      freshSources.length > 1
        ? `${freshSources.join(' and ')} were both updated within 30 days.`
        : `${freshSources[0]} was updated ${freshest.ageDays} days ago.`,
      staleSources.length
        ? `${staleSources.join(' and ')} last updated ${stalest.ageDays} days ago.`
        : null,
      workingEstimate ? `Working estimate: ${workingEstimate}.` : null,
    ]
      .filter(Boolean)
      .join(' '),
    resolved: true,
    confidence: round2(Math.min(0.95, 0.7 + freshest.effectiveScore * 0.2)),
    category: CONFLICT_CATEGORIES.TEMPORAL,
    confidencePenalty: 0.05,
  };
}

function tryAuthorityResolution(conflict, now = new Date()) {
  const claims = conflict.conflictingClaims || [];
  if (claims.length < 2) return null;

  const scored = claims.map((c) => scoreClaim(c, now));
  const sorted = [...scored].sort((a, b) => b.effectiveScore - a.effectiveScore);
  const best = sorted[0];
  const second = sorted[1];

  if (!best || !second || best.effectiveScore - second.effectiveScore < 0.08) return null;

  const numeric = numericValues(claims);
  const bestValue = best.claim.value;
  const workingEstimate =
    typeof bestValue === 'number'
      ? String(bestValue)
      : numeric.length >= 2
        ? buildWorkingEstimate(conflict.subject, [bestValue, ...numeric.slice(0, 2)].filter((v) => typeof v === 'number'))
        : asText(bestValue);

  const agreeing = scored.filter(
    (s) => s.effectiveScore >= best.effectiveScore - 0.05 && s.claim.value === best.claim.value
  );

  return {
    strategy: RESOLUTION_STRATEGIES.AUTHORITY,
    workingEstimate,
    reason: `Higher-authority source (${evidenceSourceLabel(best.source)}) preferred. ${evidenceSourceLabel(best.source)} reports ${best.claim.value}.`,
    resolved: agreeing.length >= 2 || best.effectiveScore >= 0.85,
    confidence: round2(Math.min(0.95, 0.65 + best.effectiveScore * 0.25)),
    category: CONFLICT_CATEGORIES.SOURCE_AUTHORITY,
    confidencePenalty: agreeing.length >= 2 ? 0.05 : 0.08,
  };
}

function tryMajorityResolution(conflict) {
  const claims = conflict.conflictingClaims || [];
  if (claims.length < 3) return null;

  const byValue = {};
  for (const claim of claims) {
    const key = String(claim.value);
    if (!byValue[key]) byValue[key] = [];
    byValue[key].push(claim);
  }

  const groups = Object.entries(byValue).sort((a, b) => b[1].length - a[1].length);
  const [topValue, topClaims] = groups[0];
  const [secondValue, secondClaims] = groups[1] || [null, []];

  if (topClaims.length < 2 || topClaims.length <= secondClaims.length) return null;

  const sources = topClaims.map((c) => evidenceSourceLabel(c.source));
  const numeric = numericValues(topClaims);
  const workingEstimate =
    numeric.length > 0 ? String(numeric[0]) : topValue;

  return {
    strategy: RESOLUTION_STRATEGIES.MAJORITY,
    workingEstimate,
    reason: `${sources.join(', ')} agree on ${workingEstimate}.`,
    resolved: true,
    confidence: round2(Math.min(0.92, 0.6 + topClaims.length * 0.1)),
    category: CONFLICT_CATEGORIES.SOURCE_AUTHORITY,
    confidencePenalty: 0.06,
  };
}

function tryContextResolution(conflict) {
  const claims = conflict.conflictingClaims || [];
  const labels = claims.map((c) => asText(c.label).toLowerCase());

  const hiringSignal = labels.some((l) => l.includes('hiring') || l.includes('job'));
  const smallTeamSignal = labels.some((l) => l.includes('small team') || l.includes('boutique'));

  if (hiringSignal && smallTeamSignal) {
    return {
      strategy: RESOLUTION_STRATEGIES.CONTEXT,
      workingEstimate: 'Growth likely underway',
      reason: 'LinkedIn hiring signals and small-team website copy are not necessarily contradictory — growth may be underway.',
      resolved: true,
      confidence: 0.75,
      category: CONFLICT_CATEGORIES.OBSERVATION,
      confidencePenalty: 0.04,
    };
  }

  const temporalHints = labels.filter(
    (l) => l.includes('since') || l.includes('changed') || l.includes('updated')
  );
  if (temporalHints.length >= 2) {
    return {
      strategy: RESOLUTION_STRATEGIES.CONTEXT,
      workingEstimate: null,
      reason: 'Claims may refer to different time periods — not a direct contradiction.',
      resolved: true,
      confidence: 0.7,
      category: CONFLICT_CATEGORIES.TEMPORAL,
      confidencePenalty: 0.05,
    };
  }

  return null;
}

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function buildOperatorEscalation(conflict) {
  const label = subjectLabel(conflict.subject);
  const sources = (conflict.conflictingClaims || [])
    .map((c) => `${c.sourceLabel}: ${c.value}`)
    .join('; ');

  return {
    strategy: RESOLUTION_STRATEGIES.OPERATOR_ESCALATION,
    workingEstimate: null,
    reason: null,
    resolved: false,
    confidence: round2(Math.max(0.35, 0.65 - (conflict.confidencePenalty || 0.12))),
    category: CONFLICT_CATEGORIES.GENUINE_UNKNOWN,
    confidencePenalty: conflict.confidencePenalty || 0.12,
    unresolvedReason: `Unable to resolve ${label} conflict (${sources}). Operator verification recommended.`,
  };
}

function recommendedProvidersForSubject(subject) {
  const map = {
    ownership: ['county_records', 'secretary_of_state', 'website'],
    employee_count: ['linkedin', 'website', 'google_maps'],
    property_count: ['county_records', 'website', 'google_maps'],
    listing_count: ['website', 'google_maps', 'airbnb'],
    decision_maker: ['linkedin', 'prospeo', 'website'],
    operating_status: ['website', 'news', 'linkedin'],
  };
  return map[subject] || ['county_records', 'website', 'linkedin'];
}

/**
 * Resolve a single EvidenceConflict.
 * @param {object} conflict
 * @param {object} [opts]
 * @returns {object}
 */
function resolveEvidenceConflict(conflict, opts = {}) {
  const now = opts.now || new Date();

  const strategies = [
    tryContextResolution,
    (c) => tryFreshnessResolution(c, now),
    (c) => tryAuthorityResolution(c, now),
    tryMajorityResolution,
  ];

  let resolution = null;
  for (const strategy of strategies) {
    resolution = strategy(conflict);
    if (resolution) break;
  }

  if (!resolution) {
    resolution = buildOperatorEscalation(conflict);
  }

  const resolved = buildEvidenceConflict({
    ...conflict,
    category: resolution.category || conflict.category,
    confidence: resolution.confidence,
    resolution: {
      strategy: resolution.strategy,
      workingEstimate: resolution.workingEstimate,
      reason: resolution.reason,
      resolved: resolution.resolved,
    },
    unresolvedReason: resolution.unresolvedReason || null,
    confidencePenalty: resolution.confidencePenalty,
  });

  if (!resolution.resolved) {
    resolved.recommendedProviders = recommendedProvidersForSubject(conflict.subject);
    resolved.investigationTask = {
      subject: conflict.subject,
      label: subjectLabel(conflict.subject),
      reason: resolved.unresolvedReason,
      recommendedProviders: resolved.recommendedProviders,
    };
  }

  return resolved;
}

/**
 * Resolve all conflicts for a candidate or evidence bundle.
 * @param {object[]} conflicts
 * @param {object} [opts]
 * @returns {object[]}
 */
function resolveAllConflicts(conflicts = [], opts = {}) {
  return conflicts.map((c) => resolveEvidenceConflict(c, opts));
}

/**
 * Compute confidence adjustment from conflict resolution results.
 * @param {number} baseConfidence
 * @param {object[]} resolvedConflicts
 * @returns {number}
 */
function applyConflictConfidenceAdjustment(baseConfidence, resolvedConflicts = []) {
  let adjusted = baseConfidence;
  for (const conflict of resolvedConflicts) {
    adjusted -= conflict.confidencePenalty || 0;
  }
  return round2(Math.max(0, Math.min(0.98, adjusted)));
}

module.exports = {
  resolveEvidenceConflict,
  resolveAllConflicts,
  applyConflictConfidenceAdjustment,
  recommendedProvidersForSubject,
  scoreClaim,
};
