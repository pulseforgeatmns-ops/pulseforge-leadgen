'use strict';

/**
 * SPEC-144 — Contradiction analysis beyond SPEC-142 rule set.
 * Delegates numeric detection to SPEC-146 ECRE; retains backward-compatible exports.
 */

const {
  detectEvidenceConflicts,
  resolveAllConflicts,
  conflictsToLegacyFormat,
} = require('../conflict');
const { evidenceSourceLabel } = require('./EvidenceWeights');

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

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
 * Uses SPEC-146 ECRE detection; returns legacy format for backward compatibility.
 * @param {object[]} evidence
 * @returns {object[]}
 */
function detectNumericContradictions(evidence = []) {
  const conflicts = detectEvidenceConflicts({ id: 'numeric-scan' }, evidence);
  return conflictsToLegacyFormat(conflicts);
}

/**
 * Merge SPEC-142 conflicts with ECRE-resolved conflicts, deduping by description.
 * @param {object[]} existingConflicts
 * @param {object[]} evidence
 * @returns {object[]}
 */
function mergeContradictions(existingConflicts = [], evidence = []) {
  const detected = detectEvidenceConflicts({ id: 'merge-scan' }, evidence);
  const resolved = resolveAllConflicts(detected);
  const ecreLegacy = conflictsToLegacyFormat(resolved);
  const seen = new Set();
  const merged = [];

  for (const conflict of [...existingConflicts, ...ecreLegacy]) {
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
