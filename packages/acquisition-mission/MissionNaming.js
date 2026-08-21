'use strict';

/**
 * SPEC-118 — Mission title and segment derivation from operator objective only.
 * Blueprint ICP must never contaminate mission identity.
 */

const { asText, titleCaseSegment } = require('./types');

const BEACHHEAD_PATTERNS = Object.freeze([
  { re: /\bshort[- ]term rental|\bstr operator|\bairbnb|\bvrbo|\bvacation rental/i, label: 'Short-Term Rental Operators' },
  { re: /\bproperty manager/i, label: 'Property Managers' },
  { re: /\bfacility manager/i, label: 'Facility Managers' },
  { re: /\blaw firm|\blegal (?:office|practice|firm)/i, label: 'Law Firms' },
  { re: /\baccounting (?:firm|practice)|\bcpa firm/i, label: 'Accounting Firms' },
  { re: /\bmedical (?:office|practice)|\bdental (?:office|practice)/i, label: 'Medical Practices' },
  { re: /\brestaurant|\bfood service/i, label: 'Restaurants' },
  { re: /\bsalon|\bspa\b/i, label: 'Salons' },
  { re: /\bfitness|\bgym\b/i, label: 'Fitness Centers' },
]);

function extractGeography(objective) {
  const text = asText(objective);
  const geoMatch = text.match(
    /\bin\s+((?:greater\s+)?[A-Za-z][A-Za-z\s,]*?)(?:\s+for\b|\s+from\b|\.|$)/i
  );
  return geoMatch ? geoMatch[1].trim() : null;
}

/**
 * Infer target segment from mission objective text — never from Blueprint.
 * @param {string} objective
 * @returns {string|null}
 */
function inferTargetSegmentFromObjective(objective) {
  const text = asText(objective);
  if (!text) return null;

  for (const { re, label } of BEACHHEAD_PATTERNS) {
    if (re.test(text)) return label;
  }

  const fromMatch = text.match(/\bfrom\s+(?:a|an|one\s+)?(.+?)(?:\.|$)/i);
  if (fromMatch) {
    const segment = fromMatch[1].trim();
    if (segment.length <= 80) return titleCaseSegment(segment);
  }

  const commercialMatch = text.match(
    /\b(commercial(?:\s+\w+){0,3}\s+(?:client|customer|account)s?)\b/i
  );
  return commercialMatch ? titleCaseSegment(commercialMatch[1]) : null;
}

/**
 * Derive a concise mission title: one beachhead, optional geography.
 * @param {string} objective
 * @param {string|null} [targetSegment]
 * @returns {string}
 */
function deriveMissionTitle(objective, targetSegment) {
  const text = asText(objective);
  const segment = asText(targetSegment) || inferTargetSegmentFromObjective(text);
  const geography = extractGeography(text);

  if (segment && geography) return `${segment} — ${geography}`;
  if (segment) return segment;
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
  if (/property manager/.test(text)) return 'property_management';
  if (/facility manager/.test(text)) return 'facility_management';
  if (/law firm|legal/.test(text)) return 'law_firm';
  if (/accounting|cpa/.test(text)) return 'accounting';
  return text.replace(/[\s-]+/g, '_').replace(/[^a-z0-9_]/g, '');
}

module.exports = {
  extractGeography,
  inferTargetSegmentFromObjective,
  deriveMissionTitle,
  segmentToSearchKey,
  BEACHHEAD_PATTERNS,
};
