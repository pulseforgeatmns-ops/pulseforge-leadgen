'use strict';

/**
 * SPEC-201 — Mission resume material compatibility.
 *
 * Resume requires semantic contract compatibility, not topical token overlap.
 * A narrower exclusive-segment mission must not resume when the operator
 * objective materially broadens market / buyer scope.
 */

const { asText } = require('../../acquisition-mission/types');
const {
  inferTargetSegmentFromObjective,
  segmentToSearchKey,
  extractGeography,
} = require('../../acquisition-mission/MissionNaming');
const {
  expandGeography,
  inferSegmentKey,
  segmentMeta,
} = require('../../acquisition-mission/MissionPlanner');
const { MISSION_TYPES } = require('../../acquisition-mission/StructuredMission');
const { objectivesSimilar } = require('../scoutAcquisition/NeedAssessment');

const SEGMENT_PATTERNS = Object.freeze([
  { key: 'short_term_rental', re: /\bshort[- ]term rental|\bstr operators?\b|\bairbnb|\bvrbo|\bvacation rental/i },
  { key: 'property_management', re: /\bproperty[\s-]?manag/i },
  { key: 'law_firm', re: /\blaw firms?\b|\blegal (?:office|practice|firm)/i },
  { key: 'accounting', re: /\baccounting (?:firm|practice)|\bcpa firm/i },
  { key: 'facility_management', re: /\bfacility manag/i },
]);

const COMMERCIAL_BUYER_RE =
  /\bcommercial(?:\s+\w+){0,2}\s+(?:client|customer|account|opportunit)/i;

function normalizeRegion(region) {
  return String(region || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function normalizeSegmentKey(key) {
  const normalized = asText(key).toLowerCase();
  if (!normalized) return null;
  if (
    normalized === 'commercial_cleaning_client' ||
    normalized === 'commercial_cleaning' ||
    normalized === 'commercial_clients'
  ) {
    return 'commercial';
  }
  return normalized;
}

function regionsCompatible(left, right) {
  const a = normalizeRegion(left);
  const b = normalizeRegion(right);
  if (!a || !b) return true;
  if (a === b) return true;
  if (a.includes(b) || b.includes(a)) return true;
  return false;
}

function detectMentionedSegments(text) {
  const hay = asText(text);
  const keys = [];
  for (const { key, re } of SEGMENT_PATTERNS) {
    if (re.test(hay)) keys.push(key);
  }
  const mentionsCommercialCleaning = /\bcommercial cleaning\b/i.test(hay);
  if (
    !mentionsCommercialCleaning &&
    COMMERCIAL_BUYER_RE.test(hay) &&
    !keys.includes('commercial')
  ) {
    keys.push('commercial');
  }
  if (
    /\bcommercial\s+(?:and|&)\s+property/i.test(hay) &&
    !keys.includes('commercial')
  ) {
    keys.push('commercial');
  }
  if (
    /\bhigh-fit commercial\b/i.test(hay) &&
    !keys.includes('commercial')
  ) {
    keys.push('commercial');
  }
  return [...new Set(keys)];
}

function isExclusiveSegmentObjective(text) {
  const hay = asText(text);
  if (/\bfrom\s+(?:a|an|one\s+)?(?:short[- ]term rental|property manag|law firm|accounting)/i.test(hay)) {
    return true;
  }
  const segments = detectMentionedSegments(hay);
  if (segments.length === 1) {
    return /\bfrom\s+(?:a|an|one\s+)/i.test(hay) || /\btargeting\b/i.test(hay);
  }
  return false;
}

function isMultiSegmentObjective(text) {
  const hay = asText(text);
  const segments = detectMentionedSegments(hay);
  if (segments.length >= 2) return true;
  if (/\bwhere appropriate\b/i.test(hay)) return true;
  if (/\bincluding\b/i.test(hay)) return true;
  if (/\b(?:commercial|property)[\s-]?(?:management|manager)?\s+(?:and|&|plus)\b/i.test(hay)) {
    return true;
  }
  if (/\bprioritize\b[\s\S]{0,80}\b(?:and|,)\b/i.test(hay)) return true;
  if (/\bhigh-fit\b/i.test(hay) && segments.length >= 1) return true;
  return false;
}

function extractSegmentScopeFromText(text) {
  const hay = asText(text);
  const mentioned = detectMentionedSegments(hay);
  const primaryLabel = inferTargetSegmentFromObjective(hay);
  const primarySegment = primaryLabel ? segmentToSearchKey(primaryLabel) : inferSegmentKey(hay, primaryLabel);

  if (isMultiSegmentObjective(hay)) {
    const eligible = mentioned.length ? mentioned : (primarySegment ? [primarySegment, 'commercial'] : ['commercial']);
    return {
      mode: 'multi',
      primarySegment: null,
      eligibleSegments: [...new Set(eligible)],
      buyer: null,
    };
  }

  if (isExclusiveSegmentObjective(hay) || mentioned.length === 1) {
    const segment = mentioned[0] || primarySegment;
    const meta = segment ? segmentMeta(segment) : {};
    return {
      mode: 'exclusive',
      primarySegment: segment || null,
      eligibleSegments: segment ? [segment] : [],
      buyer: meta.buyer || null,
    };
  }

  return {
    mode: 'general',
    primarySegment: primarySegment || null,
    eligibleSegments: mentioned,
    buyer: null,
  };
}

function extractSegmentScope(text, structuredMission) {
  const fromObjective = extractSegmentScopeFromText(text);
  if (structuredMission && structuredMission.market && structuredMission.market.segment) {
    const segmentKey = asText(structuredMission.market.segment);
    const buyer = asText(structuredMission.market.buyer) || null;
    const fromDraft = {
      mode: 'exclusive',
      primarySegment: segmentKey,
      eligibleSegments: segmentKey ? [segmentKey] : [],
      buyer,
    };
    const draftKey = normalizeSegmentKey(fromDraft.primarySegment);
    const objectiveKey = normalizeSegmentKey(fromObjective.primarySegment);
    if (
      objectiveKey &&
      draftKey &&
      objectiveKey !== draftKey &&
      (fromObjective.mode === 'exclusive' || fromObjective.mode === 'multi')
    ) {
      return fromObjective;
    }
    return fromDraft;
  }
  return fromObjective;
}

function extractSuccessMetric(text, structuredMission) {
  if (structuredMission && structuredMission.successMetric) {
    return {
      type: asText(structuredMission.successMetric.type || structuredMission.successMetric.metric),
      target: Number(structuredMission.successMetric.target) || 1,
    };
  }
  const hay = asText(text).toLowerCase();
  const targetMatch = hay.match(/\b(?:acquire|get|win|land|sign|close|add)\s+(?:one|a|an|(\d+))\b/i);
  let target = 1;
  if (targetMatch && targetMatch[1]) target = Number(targetMatch[1]) || 1;
  const type = /\brecurring\b/.test(hay) ? 'recurring_clients' : 'customers';
  return { type, target };
}

function extractGeographyContract(text, structuredMission) {
  if (structuredMission && structuredMission.geography) {
    return structuredMission.geography;
  }
  const mention = extractGeography(text);
  return expandGeography(mention || '', text);
}

function buildResumeContract(objective, opts = {}) {
  const text = asText(objective);
  const structuredMission = opts.structuredMission || null;
  const resolvedObjective = opts.resolvedObjective || null;

  const missionType = structuredMission
    ? asText(structuredMission.missionType)
    : (resolvedObjective && resolvedObjective.missionType) || MISSION_TYPES.ACQUISITION;

  return {
    missionType,
    objective: text,
    successMetric: extractSuccessMetric(text, structuredMission),
    geography: extractGeographyContract(text, structuredMission),
    segmentScope: extractSegmentScope(text, structuredMission),
    constraints: structuredMission
      ? (structuredMission.constraints || [])
      : (resolvedObjective && resolvedObjective.constraints) || [],
  };
}

function segmentScopesCompatible(existingScope, newScope) {
  if (!existingScope || !newScope) return true;

  const existingPrimary = normalizeSegmentKey(existingScope.primarySegment);
  const newPrimary = normalizeSegmentKey(newScope.primarySegment);
  const existingExclusive = existingScope.mode === 'exclusive';
  const newExclusive = newScope.mode === 'exclusive';
  const newMulti = newScope.mode === 'multi';
  const newGeneral = newScope.mode === 'general';

  if (existingExclusive && newMulti) {
    const existingKey = normalizeSegmentKey(existingScope.primarySegment);
    const eligible = (newScope.eligibleSegments || []).map(normalizeSegmentKey);
    if (existingKey && eligible.includes(existingKey) && eligible.length > 1) {
      return false;
    }
  }

  if (existingScope.mode === 'multi' && newExclusive) {
    const eligible = (existingScope.eligibleSegments || []).map(normalizeSegmentKey);
    if (newPrimary && eligible.length && !eligible.includes(newPrimary)) {
      return false;
    }
  }

  if (existingExclusive && newExclusive) {
    if (existingPrimary && newPrimary && existingPrimary !== newPrimary) {
      return false;
    }
  }

  if (existingExclusive && newGeneral) {
    if (existingPrimary && newPrimary && existingPrimary !== newPrimary) {
      return false;
    }
  }

  if (existingExclusive && newExclusive && existingScope.buyer && newScope.buyer) {
    if (existingScope.buyer !== newScope.buyer) return false;
  }

  return true;
}

function successMetricsCompatible(left, right) {
  if (!left || !right) return true;
  if (left.type !== right.type) return false;
  if (left.target !== right.target) return false;
  return true;
}

/**
 * Material mission contract compatibility for resume/create decision.
 * Geography overlap or shared success metric alone is insufficient.
 *
 * @param {object} existingMission
 * @param {string} newObjective
 * @param {object} [opts]
 * @returns {{ compatible: boolean, reason: string|null, existingContract: object, newContract: object }}
 */
function assessMissionResumeCompatibility(existingMission, newObjective, opts = {}) {
  const existingContract = buildResumeContract(
    existingMission.objective,
    {
      structuredMission: existingMission.structuredMission || existingMission.missionPlanDraft,
      resolvedObjective: existingMission.resolvedObjective,
    }
  );
  const canonicalNewObjective =
    (opts.resolvedObjective && opts.resolvedObjective.objective) ||
    newObjective;
  const newContract = buildResumeContract(canonicalNewObjective, {
    structuredMission: null,
    resolvedObjective: opts.resolvedObjective || null,
  });

  if (existingContract.missionType !== newContract.missionType) {
    return {
      compatible: false,
      reason: 'mission_type_mismatch',
      existingContract,
      newContract,
    };
  }

  if (
    existingContract.geography.region &&
    newContract.geography.region &&
    !regionsCompatible(existingContract.geography.region, newContract.geography.region)
  ) {
    return {
      compatible: false,
      reason: 'geography_mismatch',
      existingContract,
      newContract,
    };
  }

  if (!successMetricsCompatible(existingContract.successMetric, newContract.successMetric)) {
    return {
      compatible: false,
      reason: 'success_metric_mismatch',
      existingContract,
      newContract,
    };
  }

  if (!segmentScopesCompatible(existingContract.segmentScope, newContract.segmentScope)) {
    return {
      compatible: false,
      reason: 'segment_scope_incompatible',
      existingContract,
      newContract,
    };
  }

  const textSimilar =
    objectivesSimilar(existingMission.objective, canonicalNewObjective) ||
    normalizeRegion(existingContract.objective) === normalizeRegion(newContract.objective);

  if (!textSimilar) {
    return {
      compatible: false,
      reason: 'objective_not_similar',
      existingContract,
      newContract,
    };
  }

  return {
    compatible: true,
    reason: null,
    existingContract,
    newContract,
  };
}

function missionsMateriallyCompatible(existingMission, newObjective, opts = {}) {
  return assessMissionResumeCompatibility(existingMission, newObjective, opts).compatible;
}

module.exports = {
  buildResumeContract,
  assessMissionResumeCompatibility,
  missionsMateriallyCompatible,
  extractSegmentScope,
  detectMentionedSegments,
  isMultiSegmentObjective,
  isExclusiveSegmentObjective,
  segmentScopesCompatible,
};
