'use strict';

/**
 * SPEC-144 — Evidence source weights.
 * Not all evidence is equal; quality is explicit in credibility output.
 */

function normalizeSourceKey(source) {
  return String(source || '')
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
}

const EVIDENCE_WEIGHTS = Object.freeze({
  county_records: 1.0,
  official_county_records: 1.0,
  secretary_of_state: 0.95,
  sos: 0.95,
  website: 0.95,
  company_website: 0.95,
  google_business: 0.8,
  google_places: 0.8,
  google_maps: 0.8,
  public_business_places: 0.8,
  linkedin: 0.75,
  facebook: 0.55,
  forum: 0.2,
  forum_post: 0.2,
  news: 0.7,
  hunter: 0.78,
  prospeo: 0.78,
  existing_pf: 0.85,
  apollo: 0.72,
  job_board: 0.65,
  fixture: 0.5,
  unknown: 0.55,
});

const EVIDENCE_LABELS = Object.freeze({
  county_records: 'County records',
  official_county_records: 'County records',
  secretary_of_state: 'Secretary of State',
  sos: 'Secretary of State',
  website: 'Website',
  company_website: 'Website',
  google_business: 'Google Business',
  google_places: 'Google Business',
  google_maps: 'Google Business',
  public_business_places: 'Google Business',
  linkedin: 'LinkedIn',
  facebook: 'Facebook',
  forum: 'Forum post',
  forum_post: 'Forum post',
  news: 'News',
  hunter: 'Hunter',
  prospeo: 'Prospeo',
  existing_pf: 'Company repository',
  apollo: 'Apollo',
  job_board: 'Job board',
  fixture: 'Test fixture',
  unknown: 'Unknown source',
});

const CHECKLIST_SOURCES = Object.freeze([
  'website',
  'google_business',
  'linkedin',
  'county_records',
  'secretary_of_state',
  'decision_maker',
  'current_vendor',
]);

/**
 * @param {string} source
 * @returns {number}
 */
function evidenceWeight(source) {
  const key = normalizeSourceKey(source);
  return EVIDENCE_WEIGHTS[key] != null ? EVIDENCE_WEIGHTS[key] : 0.65;
}

/**
 * @param {string} source
 * @returns {string}
 */
function evidenceSourceLabel(source) {
  const key = normalizeSourceKey(source);
  return EVIDENCE_LABELS[key] || (source ? String(source) : 'Unknown source');
}

/**
 * Rank evidence items by weight descending.
 * @param {object[]} evidence
 * @returns {object[]}
 */
function rankEvidenceByWeight(evidence = []) {
  return [...evidence]
    .map((item) => {
      const source = typeof item === 'string' ? item : item.source || item.kind || 'unknown';
      const weight = typeof item === 'object' && item.weight != null ? item.weight : evidenceWeight(source);
      return {
        ...(typeof item === 'object' ? item : { label: item }),
        source,
        weight,
        sourceLabel: evidenceSourceLabel(source),
      };
    })
    .sort((a, b) => b.weight - a.weight);
}

module.exports = {
  EVIDENCE_WEIGHTS,
  EVIDENCE_LABELS,
  CHECKLIST_SOURCES,
  normalizeSourceKey,
  evidenceWeight,
  evidenceSourceLabel,
  rankEvidenceByWeight,
};
