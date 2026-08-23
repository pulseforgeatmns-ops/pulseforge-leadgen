'use strict';

/**
 * SPEC-144 — Contradiction analysis beyond SPEC-142 rule set.
 * Detects numeric and cross-source conflicts (e.g. website 15 vs county 42 properties).
 */

const { evidenceSourceLabel } = require('./EvidenceWeights');

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function extractNumericClaims(label) {
  const text = asText(label);
  const matches = [];
  const propertyMatch = text.match(/(\d+)\s*(?:managed\s*)?propert(?:y|ies)/i);
  if (propertyMatch) {
    matches.push({ field: 'property_count', value: Number(propertyMatch[1]), raw: text });
  }
  const employeeMatch = text.match(/(\d+)\s*employees?/i);
  if (employeeMatch) {
    matches.push({ field: 'employee_count', value: Number(employeeMatch[1]), raw: text });
  }
  return matches;
}

/**
 * Detect numeric contradictions across evidence items.
 * @param {object[]} evidence
 * @returns {object[]}
 */
function detectNumericContradictions(evidence = []) {
  const byField = {};

  for (const item of evidence) {
    const label = asText(item.label || item.text || item);
    const source = item.source || 'unknown';
    for (const claim of extractNumericClaims(label)) {
      if (!byField[claim.field]) byField[claim.field] = [];
      byField[claim.field].push({
        source,
        sourceLabel: evidenceSourceLabel(source),
        value: claim.value,
        label,
      });
    }
  }

  const contradictions = [];
  for (const [field, entries] of Object.entries(byField)) {
    if (entries.length < 2) continue;
    const values = [...new Set(entries.map((e) => e.value))];
    if (values.length <= 1) continue;

    const sorted = [...entries].sort((a, b) => b.value - a.value);
    contradictions.push({
      id: `numeric:${field}:${sorted[0].source}:${sorted[1].source}`,
      type: 'numeric_mismatch',
      field,
      description: `${sorted[0].sourceLabel} reports ${sorted[0].value}; ${sorted[1].sourceLabel} reports ${sorted[1].value}.`,
      sources: sorted.map((e) => ({ source: e.source, sourceLabel: e.sourceLabel, value: e.value, label: e.label })),
      confidencePenalty: 0.12,
      resolved: false,
      recommendation: 'Verify the authoritative count before acting on portfolio-size assumptions.',
    });
  }

  return contradictions;
}

/**
 * Merge SPEC-142 conflicts with numeric contradictions, deduping by description.
 * @param {object[]} existingConflicts
 * @param {object[]} evidence
 * @returns {object[]}
 */
function mergeContradictions(existingConflicts = [], evidence = []) {
  const numeric = detectNumericContradictions(evidence);
  const seen = new Set();
  const merged = [];

  for (const conflict of [...existingConflicts, ...numeric]) {
    const key = conflict.id || conflict.description;
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(conflict);
  }

  return merged;
}

module.exports = {
  extractNumericClaims,
  detectNumericContradictions,
  mergeContradictions,
};
