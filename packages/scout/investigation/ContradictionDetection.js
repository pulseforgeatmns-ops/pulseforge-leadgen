'use strict';

/**
 * SPEC-142 — Contradiction Detection.
 * Conflicting evidence lowers confidence until resolved.
 */

function asText(value) {
  if (value == null) return '';
  return String(value).trim().toLowerCase();
}

const CONFLICT_RULES = Object.freeze([
  {
    id: 'ownership_vs_size',
    fieldA: 'ownership',
    fieldB: 'company_size',
    detect: (a, b) =>
      (asText(a).includes('family') || asText(a).includes('owned')) &&
      (/\d{2,}/.test(asText(b)) || asText(b).includes('employees')),
    description: 'Family-owned claim conflicts with large employee count.',
  },
  {
    id: 'local_vs_national',
    fieldA: 'location',
    fieldB: 'company_size',
    detect: (a, b) =>
      asText(a).includes('local') &&
      (asText(b).includes('national') || asText(b).includes('350') || asText(b).includes('500')),
    description: 'Local business claim conflicts with national scale signals.',
  },
  {
    id: 'single_tenant_vs_multi',
    fieldA: 'address',
    fieldB: 'address',
    detect: (a, b) =>
      asText(a).includes('suite') &&
      asText(b).includes('floor') &&
      a !== b,
    description: 'Address signals suggest multi-tenant vs single-tenant conflict.',
  },
]);

/**
 * Extract structured facts from candidate and evidence for conflict checking.
 * @param {object} candidate
 * @param {object[]} evidence
 * @returns {object}
 */
function extractFactSnapshot(candidate, evidence = []) {
  const facts = {
    ownership: null,
    company_size: null,
    location: candidate.location || null,
    address: candidate.address || candidate.location || null,
  };

  for (const item of evidence) {
    const label = asText(item.label || item.text || '');
    const source = asText(item.source);

    if (label.includes('family') || label.includes('owned')) facts.ownership = item.label || label;
    if (label.includes('employee') || /\d+\s*employee/.test(label)) facts.company_size = item.label || label;
    if (source === 'linkedin' && !facts.company_size && label) facts.company_size = item.label;
    if (source === 'website' && (label.includes('family') || label.includes('owned')))
      facts.ownership = item.label || label;
  }

  if (candidate.metadata) {
    if (candidate.metadata.ownership) facts.ownership = candidate.metadata.ownership;
    if (candidate.metadata.employeeCount) facts.company_size = String(candidate.metadata.employeeCount);
  }

  return facts;
}

/**
 * Detect contradictions in evidence for a candidate.
 * @param {object} candidate
 * @param {object[]} evidence
 * @returns {object[]}
 */
function detectContradictions(candidate, evidence = []) {
  const facts = extractFactSnapshot(candidate, evidence);
  const conflicts = [];

  for (const rule of CONFLICT_RULES) {
    const valA = facts[rule.fieldA];
    const valB = facts[rule.fieldB];
    if (valA && valB && rule.detect(valA, valB)) {
      conflicts.push({
        id: `conflict:${candidate.id}:${rule.id}`,
        entityId: candidate.id,
        ruleId: rule.id,
        description: rule.description,
        fieldA: rule.fieldA,
        valueA: valA,
        fieldB: rule.fieldB,
        valueB: valB,
        resolved: false,
        confidencePenalty: 0.15,
      });
    }
  }

  for (const meta of candidate.conflictHints || []) {
    conflicts.push({
      id: `conflict:${candidate.id}:hint:${conflicts.length}`,
      entityId: candidate.id,
      ruleId: 'explicit_hint',
      description: meta.description || 'Explicit conflict hint on candidate.',
      fieldA: meta.fieldA,
      valueA: meta.valueA,
      fieldB: meta.fieldB,
      valueB: meta.valueB,
      resolved: meta.resolved === true,
      confidencePenalty: meta.resolved ? 0.05 : 0.15,
    });
  }

  return conflicts;
}

/**
 * Apply contradiction penalty to claim confidence.
 * @param {number} confidence
 * @param {object[]} contradictions
 * @returns {number}
 */
function applyContradictionPenalty(confidence, contradictions = []) {
  let adjusted = confidence;
  for (const c of contradictions) {
    adjusted -= c.resolved ? 0.05 : c.confidencePenalty || 0.15;
  }
  return Number(Math.max(0, Math.min(0.98, adjusted)).toFixed(2));
}

module.exports = {
  CONFLICT_RULES,
  extractFactSnapshot,
  detectContradictions,
  applyContradictionPenalty,
};
