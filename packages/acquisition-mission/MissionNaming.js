'use strict';

/**
 * SPEC-118 — Mission title and segment derivation from operator objective only.
 * Blueprint ICP must never contaminate mission identity.
 *
 * Market scope resolution: explicit operator objective outranks incidental STR
 * mentions (e.g. "including short-term rental operators where appropriate").
 */

const { asText, titleCaseSegment } = require('./types');

const BEACHHEAD_PATTERNS = Object.freeze([
  { re: /\bshort[- ]term rental|\bstr operator|\bairbnb|\bvrbo|\bvacation rental/i, label: 'Short-Term Rental Operators', key: 'short_term_rental' },
  { re: /\bproperty manager|\bproperty[- ]management\b/i, label: 'Property Managers', key: 'property_management' },
  { re: /\bfacility manager/i, label: 'Facility Managers', key: 'facility_management' },
  { re: /\blaw firm|\blegal (?:office|practice|firm)/i, label: 'Law Firms', key: 'law_firm' },
  { re: /\baccounting (?:firm|practice)|\bcpa firm/i, label: 'Accounting Firms', key: 'accounting' },
  { re: /\bmedical (?:office|practice)|\bdental (?:office|practice)/i, label: 'Medical Practices', key: 'medical_practice' },
  { re: /\brestaurant|\bfood service/i, label: 'Restaurants', key: 'restaurant' },
  { re: /\bsalon|\bspa\b/i, label: 'Salons', key: 'salon' },
  { re: /\bfitness|\bgym\b/i, label: 'Fitness Centers', key: 'fitness' },
]);

/** Default eligible subsegments for broad Anchor commercial cleaning missions. */
const BROAD_COMMERCIAL_SUBSEGMENTS = Object.freeze([
  'property_management',
  'short_term_rental',
  'law_firm',
  'accounting',
]);

const STR_PATTERN = /\bshort[- ]term rental|\bstr operator|\bairbnb|\bvrbo|\bvacation rental/i;

function extractGeography(objective) {
  const text = asText(objective);
  const geoMatch = text.match(
    /\bin\s+((?:greater\s+)?[A-Za-z][A-Za-z\s,]*?)(?:\s+for\b|\s+from\b|\.|$)/i
  );
  return geoMatch ? geoMatch[1].trim() : null;
}

/**
 * STR is mentioned as an eligible subsegment, not the sole primary target.
 * @param {string} text
 * @returns {boolean}
 */
function isStrOptionalSubsegment(text) {
  const hay = asText(text);
  if (!hay) return false;
  if (/\b(?:including|such as|like)\b[^.]{0,160}\b(?:short[- ]term rental|\bstr\b)[^.]{0,120}\bwhere appropriate\b/i.test(hay)) {
    return true;
  }
  if (/\bshort[- ]term rental operators?\s+where appropriate\b/i.test(hay)) {
    return true;
  }
  if (/\bwhere appropriate\b/i.test(hay) && STR_PATTERN.test(hay) && isBroadCommercialPropertyObjective(hay)) {
    return true;
  }
  return false;
}

/**
 * Operator explicitly targets STR as the primary beachhead.
 * @param {string} text
 * @returns {boolean}
 */
function isExplicitStrPrimaryTarget(text) {
  const hay = asText(text);
  if (!hay) return false;
  if (isStrOptionalSubsegment(hay)) return false;
  if (/\bfrom\s+(?:a|an|one\s+)?(?:short[- ]term rental|str\b)/i.test(hay)) return true;
  if (/\btarget(?:ing)?\s+(?:short[- ]term rental|str\b)/i.test(hay)) return true;
  if (/\bfind\s+(?:short[- ]term rental|str\b)/i.test(hay)) return true;
  if (/\b(?:short[- ]term rental|str)\s+(?:operator|operators)\b/i.test(hay) && !isBroadCommercialPropertyObjective(hay)) {
    return true;
  }
  return false;
}

/**
 * Broad commercial / property-management acquisition objective (Anchor-style).
 * @param {string} text
 * @returns {boolean}
 */
function isBroadCommercialPropertyObjective(text) {
  const hay = asText(text).toLowerCase();
  const hasPropertyMgmt = /\bproperty[- ]management\b|\bproperty managers?\b/i.test(hay);
  const hasCommercial = /\bcommercial\b/i.test(hay);
  const hasCleaningClient =
    /\b(?:recurring\s+)?cleaning client\b|\bcommercial cleaning\b|\brecurring cleaning\b/i.test(hay);
  return (hasCommercial && hasPropertyMgmt) || (hasCleaningClient && hasPropertyMgmt) || (
    hasCleaningClient && hasCommercial && /\bopportunities\b/i.test(hay)
  );
}

/**
 * Resolve primary segment + eligible subsegments from operator objective.
 * Current operator objective outranks historical / incidental STR mentions.
 *
 * @param {string} objective
 * @returns {{ primarySegment: string|null, eligibleSubsegments: string[], segmentLabel: string|null, source: string }}
 */
function resolveMarketScopeFromObjective(objective) {
  const text = asText(objective);
  if (!text) {
    return { primarySegment: null, eligibleSubsegments: [], segmentLabel: null, source: 'none' };
  }

  if (isBroadCommercialPropertyObjective(text)) {
    const subsegments = [...BROAD_COMMERCIAL_SUBSEGMENTS];
    return {
      primarySegment: 'property_management',
      eligibleSubsegments: subsegments,
      segmentLabel: 'Commercial property management',
      source: 'operator_objective',
    };
  }

  if (isExplicitStrPrimaryTarget(text)) {
    return {
      primarySegment: 'short_term_rental',
      eligibleSubsegments: ['short_term_rental'],
      segmentLabel: 'Short-term rental operators',
      source: 'operator_objective',
    };
  }

  for (const { re, label, key } of BEACHHEAD_PATTERNS) {
    if (!re.test(text)) continue;
    if (key === 'short_term_rental' && isStrOptionalSubsegment(text)) continue;
    return {
      primarySegment: key || segmentToSearchKey(label),
      eligibleSubsegments: [key || segmentToSearchKey(label)],
      segmentLabel: label,
      source: 'operator_objective',
    };
  }

  const fromMatch = text.match(/\bfrom\s+(?:a|an|one\s+)?(.+?)(?:\.|$)/i);
  if (fromMatch) {
    const segment = fromMatch[1].trim();
    if (segment.length <= 80) {
      const label = titleCaseSegment(segment);
      const key = segmentToSearchKey(label);
      return {
        primarySegment: key,
        eligibleSubsegments: [key],
        segmentLabel: label,
        source: 'operator_objective',
      };
    }
  }

  const commercialMatch = text.match(
    /\b(commercial(?:\s+\w+){0,3}\s+(?:client|customer|account)s?)\b/i
  );
  if (commercialMatch) {
    const label = titleCaseSegment(commercialMatch[1]);
    return {
      primarySegment: 'property_management',
      eligibleSubsegments: [...BROAD_COMMERCIAL_SUBSEGMENTS],
      segmentLabel: label,
      source: 'operator_objective',
    };
  }

  return { primarySegment: null, eligibleSubsegments: [], segmentLabel: null, source: 'none' };
}

/**
 * Infer target segment label from mission objective text — never from Blueprint.
 * @param {string} objective
 * @returns {string|null}
 */
function inferTargetSegmentFromObjective(objective) {
  const scope = resolveMarketScopeFromObjective(objective);
  if (scope.segmentLabel) return scope.segmentLabel;
  return null;
}

/**
 * Infer canonical segment key from objective (primary segment only).
 * @param {string} objective
 * @returns {string|null}
 */
function inferSegmentKeyFromObjective(objective) {
  const scope = resolveMarketScopeFromObjective(objective);
  return scope.primarySegment || null;
}

/**
 * Whether two objectives target incompatible primary market scopes.
 * Used to prevent resuming a prior STR mission for a broader commercial objective.
 * @param {string} a
 * @param {string} b
 * @returns {boolean}
 */
function marketScopesCompatible(a, b) {
  const left = resolveMarketScopeFromObjective(a);
  const right = resolveMarketScopeFromObjective(b);
  if (!left.primarySegment || !right.primarySegment) return true;
  if (left.primarySegment === right.primarySegment) return true;
  const strInvolved =
    left.primarySegment === 'short_term_rental' || right.primarySegment === 'short_term_rental';
  if (strInvolved) return false;
  const leftSubs = new Set(left.eligibleSubsegments || []);
  const rightSubs = new Set(right.eligibleSubsegments || []);
  for (const seg of leftSubs) {
    if (rightSubs.has(seg)) return true;
  }
  return false;
}

/**
 * Derive a concise mission title: one beachhead, optional geography.
 * @param {string} objective
 * @param {string|null} [targetSegment]
 * @returns {string}
 */
function deriveMissionTitle(objective, targetSegment) {
  const text = asText(objective);
  const scope = resolveMarketScopeFromObjective(text);
  const segment = asText(targetSegment) || scope.segmentLabel || inferTargetSegmentFromObjective(text);
  const geography = extractGeography(text);

  if (segment && geography) return `${titleCaseSegment(segment)} — ${geography}`;
  if (segment) return titleCaseSegment(segment);
  if (text.length <= 72) return titleCaseSegment(text);
  return titleCaseSegment(`${text.slice(0, 69)}…`);
}

/**
 * Map human-readable segment to search key for Scout delegation.
 * @param {string} segment
 * @returns {string}
 */
function segmentToSearchKey(segment) {
  const text = asText(segment).toLowerCase();
  if (/short[- ]term rental|str operator|airbnb|vrbo|vacation rental/.test(text)) {
    return 'short_term_rental';
  }
  if (/property manager|property management|commercial property/.test(text)) {
    return 'property_management';
  }
  if (/facility manager/.test(text)) return 'facility_management';
  if (/law firm|legal/.test(text)) return 'law_firm';
  if (/accounting|cpa/.test(text)) return 'accounting';
  if (/commercial cleaning|commercial prospect/.test(text)) return 'property_management';
  return text.replace(/[\s-]+/g, '_').replace(/[^a-z0-9_]/g, '');
}

module.exports = {
  extractGeography,
  inferTargetSegmentFromObjective,
  inferSegmentKeyFromObjective,
  resolveMarketScopeFromObjective,
  isStrOptionalSubsegment,
  isExplicitStrPrimaryTarget,
  isBroadCommercialPropertyObjective,
  marketScopesCompatible,
  deriveMissionTitle,
  segmentToSearchKey,
  BEACHHEAD_PATTERNS,
  BROAD_COMMERCIAL_SUBSEGMENTS,
};
