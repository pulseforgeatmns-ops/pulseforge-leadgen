'use strict';

/**
 * SPEC-099A — Scout investigation provenance and coverage intelligence.
 * A specialist conclusion and the quality of the investigation are separate.
 * Reuses SpecialistResult.payload — not a second evidence architecture.
 */

const {
  asText,
  clone,
  isPlainObject,
  nowIso,
  ageMs,
  isTimely,
  TIMELY_SIGNAL_MS,
  SOURCE_TYPES,
  CORE_SOURCE_TYPES,
  SOCIAL_SOURCE_TYPES,
  PERCEPTION_CHANNELS,
  REJECTION_REASONS,
  COVERAGE_BANDS,
} = require('./Types');

const TIMING_SIGNAL_TYPES = Object.freeze([
  'expansion',
  'new_location',
  'portfolio_growth',
  'hiring',
  'leadership_change',
  'operational_change',
  'vendor_dissatisfaction',
  'contract_timing',
  'facility_growth',
  'service_gap',
]);

const SYSTEM_PROVENANCE_IDS = Object.freeze([
  'scout_acquisition',
  'spec_100',
  'spec_100a',
  'spec_099',
  'spec_099a',
  'spec_098',
  'specialist_delegation',
]);

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

function coverageBand(score) {
  const n = clamp01(score);
  if (n >= 0.7) return COVERAGE_BANDS.STRONG;
  if (n >= 0.45) return COVERAGE_BANDS.MODERATE;
  return COVERAGE_BANDS.WEAK;
}

function parseGeographyList(value) {
  if (Array.isArray(value)) return value.map(asText).filter(Boolean);
  const text = asText(value);
  if (!text) return [];
  return text
    .split(/\s+and\s+|;\s*|\/\s*/i)
    .map((part) => part.trim())
    .filter(Boolean);
}

function normalizeLocationLabel(value) {
  const text = asText(value);
  if (!text) return null;
  return text.replace(/\s+/g, ' ').trim();
}

function locationKey(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function geographyCovered(requestedList, investigatedList) {
  const requested = (requestedList || []).map(locationKey).filter(Boolean);
  const investigated = (investigatedList || []).map(locationKey).filter(Boolean);
  if (!requested.length) return investigated.length ? 1 : 0;
  let hit = 0;
  for (const req of requested) {
    const tokens = req.split(/\s+/).filter((t) => t.length > 1 && !['nh', 'tn', 'wv'].includes(t));
    const matched = investigated.some((inv) => {
      if (inv === req || inv.includes(req) || req.includes(inv)) return true;
      return tokens.some((t) => inv.includes(t));
    });
    if (matched) hit += 1;
  }
  return hit / requested.length;
}

function uniqueLocations(companies) {
  const seen = new Set();
  const out = [];
  for (const company of companies || []) {
    const label = normalizeLocationLabel(company.location || company.geography);
    if (!label) continue;
    const key = locationKey(label);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(label);
  }
  return out;
}

function uniqueSegments(companies, fallback = []) {
  const seen = new Set();
  const out = [];
  for (const company of companies || []) {
    const label = asText(company.industry || company.vertical || company.segment);
    if (!label) continue;
    const key = label.toLowerCase().replace(/[\s-]+/g, '_');
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(label);
  }
  if (!out.length) return (fallback || []).slice();
  return out;
}

function monthsOld(iso, now = Date.now()) {
  const ms = ageMs(iso, now);
  if (!Number.isFinite(ms) || ms === Number.POSITIVE_INFINITY) return null;
  return Math.max(1, Math.round(ms / (30 * 24 * 60 * 60 * 1000)));
}

function timingSignals(signals) {
  return (signals || []).filter((s) => TIMING_SIGNAL_TYPES.includes(s && s.type));
}

function qualifyCandidate(classified, company, now = Date.now()) {
  const signals = Array.isArray(classified.signals) ? classified.signals : [];
  const timing = timingSignals(signals);
  const timely = timing.filter((s) => isTimely(s.observedAt, now));
  const icp = company && company.icpScore != null ? Number(company.icpScore) : null;
  const fit = Number(classified.fit || 0);
  const fitBasic = fit >= 0.5 || (icp != null && icp >= 55);
  const fitStrong = fit >= 0.7 || (icp != null && icp >= 70);
  const hasEvidence =
    (classified.evidenceRefs && classified.evidenceRefs.length > 0) ||
    (classified.observations && classified.observations.length > 0);
  const primarySignal = timing[0] || signals.find((s) => s.type && s.type !== 'decision_maker') || null;

  if (!fitBasic) {
    return {
      supported: false,
      basicFit: false,
      signalBearing: timing.length > 0,
      reason: REJECTION_REASONS.INSUFFICIENT_BUSINESS_FIT,
      nearThreshold: false,
      rejectedBecause: `${classified.name || company.name || company.id} did not meet the current commercial-fit bar.`,
    };
  }

  if (!timing.length && !hasEvidence) {
    return {
      supported: false,
      basicFit: true,
      signalBearing: false,
      reason: REJECTION_REASONS.INSUFFICIENT_SOURCE_SUPPORT,
      nearThreshold: fitStrong,
      rejectedBecause: `${classified.name || company.name} had basic fit but no source-backed timing or company evidence.`,
    };
  }

  if (timing.length && !timely.length) {
    const oldest = timing
      .map((s) => s.observedAt)
      .filter(Boolean)
      .sort()[0];
    const months = monthsOld(oldest, now);
    return {
      supported: false,
      basicFit: true,
      signalBearing: true,
      reason: REJECTION_REASONS.STALE_EVIDENCE,
      nearThreshold: fitStrong || fit >= 0.6,
      rejectedBecause:
        months != null
          ? `${primarySignal && primarySignal.type ? primarySignal.type.replace(/_/g, ' ') : 'Expansion'} evidence is ${months} month${months === 1 ? '' : 's'} old and no recent operational signal was found.`
          : 'Timing evidence exists but is outside the current evidence window.',
      signal: primarySignal && primarySignal.type,
    };
  }

  if (!timely.length) {
    return {
      supported: false,
      basicFit: true,
      signalBearing: false,
      reason: REJECTION_REASONS.NO_TIMING_SIGNAL,
      nearThreshold: fitStrong,
      rejectedBecause: `${classified.name || company.name} had reasonable business fit but no current timing signal.`,
    };
  }

  if (!hasEvidence) {
    return {
      supported: false,
      basicFit: true,
      signalBearing: true,
      reason: REJECTION_REASONS.INSUFFICIENT_SOURCE_SUPPORT,
      nearThreshold: true,
      rejectedBecause: `A timing signal was present for ${classified.name || company.name} but lacked source-backed evidence.`,
      signal: primarySignal && primarySignal.type,
    };
  }

  return {
    supported: true,
    basicFit: true,
    signalBearing: true,
    reason: null,
    nearThreshold: false,
    signal: primarySignal && primarySignal.type,
  };
}

function incrementReason(map, reason) {
  const key = reason || REJECTION_REASONS.UNRESOLVED;
  map[key] = (map[key] || 0) + 1;
}

function rejectionSummaryFromMap(map) {
  return Object.keys(map)
    .sort()
    .map((reason) => ({ reason, count: map[reason] }));
}

function defaultPerception(overrides = {}) {
  const perception = {};
  for (const channel of PERCEPTION_CHANNELS) {
    const raw = overrides[channel];
    perception[channel] =
      raw === 'available' || raw === true ? 'available' : 'unavailable';
  }
  return perception;
}

function socialSourceForChannel(channel) {
  if (channel === 'linkedin') return SOURCE_TYPES.LINKEDIN;
  if (channel === 'facebook') return SOURCE_TYPES.FACEBOOK;
  if (channel === 'instagram') return SOURCE_TYPES.INSTAGRAM;
  return null;
}

/**
 * Record only sources that were actually used or that failed when attempted.
 * Social perception slots are always represented for future Faye/Link/Ivy.
 */
function resolveSourceCoverage(input = {}) {
  const checked = new Set();
  const unavailable = new Set();
  const attempted = Array.isArray(input.sourceTypesChecked) ? input.sourceTypesChecked : [];
  const failed = Array.isArray(input.sourceTypesUnavailable) ? input.sourceTypesUnavailable : [];
  for (const src of attempted) {
    if (src) checked.add(src);
  }
  for (const src of failed) {
    if (src) unavailable.add(src);
  }

  const perception = defaultPerception(input.perception);
  for (const channel of PERCEPTION_CHANNELS) {
    const source = socialSourceForChannel(channel);
    if (!source) continue;
    if (perception[channel] === 'available') {
      checked.add(source);
      unavailable.delete(source);
    } else if (!checked.has(source)) {
      unavailable.add(source);
    }
  }

  return {
    sourceTypesChecked: [...checked],
    sourceTypesUnavailable: [...unavailable],
    perception,
  };
}

function scoreCoverage(input = {}) {
  const requestedGeos = parseGeographyList(input.requestedGeography);
  const investigatedGeos = parseGeographyList(input.investigatedGeography);
  const geoCoverage = geographyCovered(requestedGeos, investigatedGeos);

  const requestedSegs = (input.requestedSegments || []).map((s) =>
    String(s).toLowerCase().replace(/[\s-]+/g, '_')
  );
  const investigatedSegs = (input.investigatedSegments || []).map((s) =>
    String(s).toLowerCase().replace(/[\s-]+/g, '_')
  );
  const segmentCoverage = requestedSegs.length
    ? requestedSegs.filter((s) => investigatedSegs.includes(s)).length / requestedSegs.length
    : investigatedSegs.length
      ? 1
      : 0;

  const checked = input.sourceTypesChecked || [];
  const unavailable = input.sourceTypesUnavailable || [];
  const coreChecked = checked.filter((s) => CORE_SOURCE_TYPES.includes(s)).length;
  const coreRelevant = new Set([
    ...checked.filter((s) => CORE_SOURCE_TYPES.includes(s)),
    ...unavailable.filter((s) => CORE_SOURCE_TYPES.includes(s)),
  ]).size;
  const sourceCoverage = coreRelevant ? coreChecked / coreRelevant : checked.length ? 1 : 0.35;

  const evaluated = Number(input.candidatesEvaluated || 0);
  const volumeScore = Math.min(1, evaluated / 24);

  const enrichmentAttempted = input.enrichmentAttempted === true;
  const enrichmentFailureRate = clamp01(input.enrichmentFailureRate || 0);
  const enrichmentScore = enrichmentAttempted ? 1 - enrichmentFailureRate : 0.72;

  const freshnessScore =
    evaluated > 0 ? clamp01((input.timelyEvidenceCount || 0) / evaluated) : 0.35;

  const discovered = Number(input.candidatesDiscovered || 0);
  const unresolved = Number(input.unresolvedCount || 0);
  const completionScore = input.providerFailed
    ? discovered
      ? clamp01(1 - unresolved / Math.max(discovered, 1)) * 0.6
      : 0.15
    : discovered
      ? clamp01(1 - unresolved / Math.max(discovered, 1))
      : evaluated
        ? 0.7
        : 0.4;

  const socialUnavailable = (unavailable || []).filter((s) =>
    SOCIAL_SOURCE_TYPES.includes(s)
  ).length;
  const socialPenalty = socialUnavailable ? Math.min(0.06, socialUnavailable * 0.02) : 0;

  let raw =
    0.18 * geoCoverage +
    0.12 * segmentCoverage +
    0.22 * sourceCoverage +
    0.2 * volumeScore +
    0.1 * enrichmentScore +
    0.08 * freshnessScore +
    0.1 * completionScore -
    socialPenalty;

  // Narrow candidate volume cannot become "moderate/strong" coverage.
  if (evaluated < 6) raw = Math.min(raw, 0.42);
  else if (evaluated < 10 && input.enrichmentAttempted && (input.enrichmentFailureRate || 0) >= 0.5) {
    raw = Math.min(raw, 0.44);
  }

  return Number(clamp01(raw).toFixed(2));
}

function buildLimitations(input = {}) {
  const limitations = [];
  const perception = input.perception || defaultPerception();
  if (perception.linkedin === 'unavailable') {
    limitations.push('LinkedIn social intelligence was unavailable.');
  }
  if (perception.facebook === 'unavailable') {
    limitations.push('Facebook social intelligence was unavailable.');
  }
  if (perception.instagram === 'unavailable') {
    limitations.push('Instagram social intelligence was unavailable.');
  }
  if (input.enrichmentAttempted && input.enrichmentFailureRate > 0) {
    const pct = Math.round(input.enrichmentFailureRate * 100);
    limitations.push(`Decision-maker enrichment failed for ${pct}% of candidates.`);
  }
  if (input.basicFitCount > 0 && input.supportedOpportunityCount === 0) {
    limitations.push(
      'Evidence coverage was stronger for company fit than for current vendor timing.'
    );
  }
  if (
    (input.sourceTypesChecked || []).includes(SOURCE_TYPES.EXISTING_PF) &&
    !(input.sourceTypesChecked || []).includes(SOURCE_TYPES.PUBLIC_BUSINESS_DATA) &&
    (input.sourceTypesUnavailable || []).includes(SOURCE_TYPES.PUBLIC_BUSINESS_DATA)
  ) {
    limitations.push(
      'Only existing PF intelligence was available; no fresh external discovery source succeeded.'
    );
  } else if (
    (input.sourceTypesChecked || []).length === 1 &&
    (input.sourceTypesChecked || [])[0] === SOURCE_TYPES.EXISTING_PF
  ) {
    limitations.push(
      'Only existing PF intelligence was available; no fresh external discovery source succeeded.'
    );
  }
  if (input.providerFailed) {
    limitations.push('A discovery or repository provider failed before the search space was fully covered.');
  }
  if (input.unresolvedCount > 0) {
    limitations.push(
      `${input.unresolvedCount} candidate${input.unresolvedCount === 1 ? '' : 's'} remained unresolved.`
    );
  }
  if (Array.isArray(input.extraLimitations)) {
    for (const line of input.extraLimitations) {
      const text = asText(line);
      if (text && !limitations.includes(text)) limitations.push(text);
    }
  }
  return limitations;
}

function buildEvidenceWindow(input = {}) {
  const dates = (input.observedAtValues || []).filter(Boolean).sort();
  const startedAt = asText(input.startedAt) || nowIso();
  const completedAt = asText(input.completedAt) || nowIso();
  return {
    startedAt,
    completedAt,
    evidenceWindow: {
      from: dates[0] || startedAt,
      to: dates[dates.length - 1] || completedAt,
      timelyWindowDays: Math.round(TIMELY_SIGNAL_MS / (24 * 60 * 60 * 1000)),
    },
  };
}

function toNearThresholdRecord(entry) {
  if (!entry || !entry.company) return null;
  const company = entry.company;
  return {
    company: asText(company.name) || asText(company.id),
    companyId: asText(company.id),
    fit: entry.fitStrong ? 'strong' : entry.basicFit ? 'moderate' : 'weak',
    signal: asText(entry.signal) || null,
    rejectedBecause: asText(entry.rejectedBecause) || asText(entry.reason),
  };
}

/**
 * Build the durable investigation object attached to Scout's SpecialistResult.
 *
 * @param {object} input
 * @returns {object}
 */
function buildInvestigation(input = {}) {
  const requestedGeography = asText(input.requestedGeography) || null;
  const requestedSegments = Array.isArray(input.requestedSegments)
    ? input.requestedSegments.map(asText).filter(Boolean)
    : [];
  const evaluatedCompanies = Array.isArray(input.evaluatedCompanies)
    ? input.evaluatedCompanies
    : [];
  const investigatedGeographyList =
    input.investigatedGeographyList || uniqueLocations(evaluatedCompanies);
  const investigatedSegments =
    input.investigatedSegments || uniqueSegments(evaluatedCompanies, requestedSegments);

  const sources = resolveSourceCoverage(input);
  const freshness = buildEvidenceWindow(input);
  const coverage = {
    candidatesDiscovered: Number(input.candidatesDiscovered || 0),
    candidatesResolved: Number(
      input.candidatesResolved != null
        ? input.candidatesResolved
        : input.candidatesEvaluated || evaluatedCompanies.length || 0
    ),
    candidatesEvaluated: Number(input.candidatesEvaluated || evaluatedCompanies.length || 0),
    basicFitCount: Number(input.basicFitCount || 0),
    signalBearingCount: Number(input.signalBearingCount || 0),
    supportedOpportunityCount: Number(input.supportedOpportunityCount || 0),
    unresolvedCount: Number(input.unresolvedCount || 0),
  };

  const coverageConfidence = scoreCoverage({
    requestedGeography,
    investigatedGeography: investigatedGeographyList,
    requestedSegments,
    investigatedSegments,
    sourceTypesChecked: sources.sourceTypesChecked,
    sourceTypesUnavailable: sources.sourceTypesUnavailable,
    candidatesEvaluated: coverage.candidatesEvaluated,
    candidatesDiscovered: coverage.candidatesDiscovered,
    enrichmentAttempted: input.enrichmentAttempted === true,
    enrichmentFailureRate: input.enrichmentFailureRate || 0,
    timelyEvidenceCount: input.timelyEvidenceCount || 0,
    unresolvedCount: coverage.unresolvedCount,
    providerFailed: input.providerFailed === true,
  });

  const limitations = buildLimitations({
    ...input,
    ...coverage,
    perception: sources.perception,
    sourceTypesChecked: sources.sourceTypesChecked,
    sourceTypesUnavailable: sources.sourceTypesUnavailable,
  });

  const nearThreshold = (input.nearThreshold || [])
    .map(toNearThresholdRecord)
    .filter(Boolean)
    .slice(0, 8);

  return {
    scope: {
      geography: investigatedGeographyList.length === 1
        ? investigatedGeographyList[0]
        : investigatedGeographyList.length
          ? investigatedGeographyList.join(' and ')
          : null,
      requestedGeography,
      investigatedGeography: investigatedGeographyList,
      segments: investigatedSegments,
      requestedSegments,
      targetCriteria: isPlainObject(input.targetCriteria) ? clone(input.targetCriteria) : {
        geography: requestedGeography,
        segments: requestedSegments,
      },
      desiredSignals: Array.isArray(input.desiredSignals)
        ? input.desiredSignals.map(asText).filter(Boolean)
        : [],
    },
    coverage,
    sources: {
      sourceTypesChecked: sources.sourceTypesChecked,
      sourceTypesUnavailable: sources.sourceTypesUnavailable,
      perception: sources.perception,
    },
    rejectionSummary: Array.isArray(input.rejectionSummary)
      ? input.rejectionSummary.slice()
      : rejectionSummaryFromMap(input.rejectionReasonCounts || {}),
    nearThreshold,
    freshness,
    limitations,
    coverageConfidence,
    coverageBand: coverageBand(coverageConfidence),
    contributors: {
      requestedVsInvestigatedGeography: geographyCovered(
        parseGeographyList(requestedGeography),
        investigatedGeographyList
      ),
      candidateVolume: coverage.candidatesEvaluated,
      providerFailed: input.providerFailed === true,
      enrichmentFailureRate: Number(input.enrichmentFailureRate || 0),
    },
  };
}

function investigationFromResult(result) {
  if (!result) return null;
  if (result.payload && isPlainObject(result.payload.investigation)) {
    return result.payload.investigation;
  }
  if (isPlainObject(result.investigation)) return result.investigation;
  return null;
}

function isSystemProvenanceId(value) {
  const id = String(value || '').trim().toLowerCase();
  if (!id) return false;
  if (SYSTEM_PROVENANCE_IDS.includes(id)) return true;
  if (/^spec[_-]?\d+[a-z]?$/.test(id)) return true;
  if (/^(scout_|specialist_|capability_|delegation_|evaluation_|result_)/.test(id)) {
    return true;
  }
  return false;
}

function isBusinessEvidenceRef(ref) {
  if (!ref || typeof ref !== 'object') return false;
  if (isSystemProvenanceId(ref.id)) return false;
  const sourceKind = String(ref.sourceKind || ref.sourceType || '').toLowerCase();
  if (sourceKind === 'system' || sourceKind === 'provenance' || sourceKind === 'capability') {
    return false;
  }
  const label = String(ref.label || ref.summary || ref.statement || '');
  if (isSystemProvenanceId(label)) return false;
  return Boolean(ref.id || label);
}

function toBusinessEvidenceRefs(refs) {
  return (Array.isArray(refs) ? refs : [])
    .filter(isBusinessEvidenceRef)
    .map((ref) => ({
      id: String(ref.id || 'unknown'),
      summary: String(ref.summary || ref.label || ref.statement || ref.title || ''),
      sourceType: ref.sourceType || ref.snapshot && ref.snapshot.source || null,
      sourceKind: ref.sourceKind || 'observed_fact',
      kind: ref.kind || 'company',
      label: ref.label || ref.summary || null,
    }));
}

function buildSystemProvenance(input = {}) {
  const items = [
    { id: 'scout_acquisition', kind: 'capability', label: 'Scout acquisition intelligence' },
    { id: 'spec_100', kind: 'spec', label: 'SPEC-100 Max ↔ Scout acquisition loop' },
    { id: 'spec_100a', kind: 'spec', label: 'SPEC-100A Scout acquisition discovery foundation' },
    { id: 'spec_099a', kind: 'spec', label: 'SPEC-099A investigation provenance' },
  ];
  if (input.delegationId) {
    items.push({
      id: String(input.delegationId),
      kind: 'delegation',
      label: 'Scout delegation',
    });
  }
  if (input.resultId) {
    items.push({
      id: String(input.resultId),
      kind: 'result',
      label: 'Scout specialist result',
    });
  }
  if (input.evaluationId) {
    items.push({
      id: String(input.evaluationId),
      kind: 'evaluation',
      label: 'Max evaluation',
    });
  }
  return items;
}

function classifyInspectionPresentation(structured = {}) {
  const supporting = toBusinessEvidenceRefs(structured.supportingEvidence);
  const contradicting = toBusinessEvidenceRefs(structured.contradictingEvidence);
  const investigation = structured.investigation || null;
  const provenance = Array.isArray(structured.provenance)
    ? structured.provenance.slice()
    : [];
  const contributors = Array.isArray(structured.confidenceContributors)
    ? structured.confidenceContributors
    : [];
  for (const item of contributors) {
    if (isSystemProvenanceId(item) && !provenance.some((p) => p.id === item || p === item)) {
      provenance.push({ id: String(item), kind: 'system', label: String(item) });
    }
  }
  const evidenceCount = supporting.length + contradicting.length;
  const parts = [];
  if (evidenceCount) parts.push(`Evidence · ${evidenceCount}`);
  if (investigation) parts.push('Investigation');
  if (provenance.length) parts.push('Provenance');
  return {
    evidence: supporting,
    contradicting,
    investigation,
    provenance,
    evidenceCount,
    summary: parts.join(' · ') || 'Investigation',
    mislabeledSystemAsEvidence: false,
  };
}

module.exports = {
  TIMING_SIGNAL_TYPES,
  SYSTEM_PROVENANCE_IDS,
  clamp01,
  coverageBand,
  parseGeographyList,
  uniqueLocations,
  uniqueSegments,
  qualifyCandidate,
  incrementReason,
  rejectionSummaryFromMap,
  resolveSourceCoverage,
  scoreCoverage,
  buildLimitations,
  buildEvidenceWindow,
  buildInvestigation,
  investigationFromResult,
  isSystemProvenanceId,
  isBusinessEvidenceRef,
  toBusinessEvidenceRefs,
  buildSystemProvenance,
  classifyInspectionPresentation,
  defaultPerception,
};
