'use strict';

/**
 * SPEC-143 — Contradiction handling across investigation memory.
 * Scout notices when new evidence conflicts with stored knowledge.
 */

const { MEMORY_STATUS } = require('./types');
const { computeEffectiveConfidence } = require('./MemoryConfidence');

function normalizeClaimText(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function extractNumeric(value) {
  const match = String(value || '').match(/(\d+)/);
  return match ? Number(match[1]) : null;
}

const CONTRADICTION_DETECTORS = [
  {
    id: 'employee_count',
    field: 'company_size',
    detect: (prev, next) => {
      const a = extractNumeric(prev);
      const b = extractNumeric(next);
      if (a == null || b == null) return false;
      const ratio = Math.max(a, b) / Math.min(a, b);
      return ratio >= 3;
    },
    describe: (prev, next) => `Employee count changed from ${prev} to ${next}.`,
  },
  {
    id: 'ownership',
    field: 'ownership',
    detect: (prev, next) => {
      const p = String(prev || '').toLowerCase();
      const n = String(next || '').toLowerCase();
      if (!p || !n || p === n) return false;
      const familyOwned = p.includes('family') || p.includes('owned');
      const corporate = n.includes('national') || n.includes('corporate') || n.includes('public');
      return familyOwned && corporate;
    },
    describe: (prev, next) => `Ownership changed from "${prev}" to "${next}".`,
  },
];

function detectClaimContradiction(existingClaim, newClaim) {
  const prevText = normalizeClaimText(existingClaim.text);
  const nextText = normalizeClaimText(newClaim.text);
  if (prevText === nextText) return null;

  for (const detector of CONTRADICTION_DETECTORS) {
    if (
      prevText.includes(detector.field.replace('_', ' ')) ||
      nextText.includes(detector.field.replace('_', ' ')) ||
      prevText.includes('employee') ||
      nextText.includes('employee')
    ) {
      if (detector.detect(existingClaim.text, newClaim.text)) {
        return {
          id: `conflict:${existingClaim.entityKey}:${detector.id}`,
          detectorId: detector.id,
          previous: existingClaim.text,
          current: newClaim.text,
          description: detector.describe(existingClaim.text, newClaim.text),
          previousConfidence: existingClaim.confidence,
          currentConfidence: newClaim.confidence,
          action: 'reinvestigate',
        };
      }
    }
  }

  if (
    existingClaim.entityId === newClaim.entityId &&
    existingClaim.text &&
    newClaim.text &&
    prevText !== nextText &&
    Math.abs((existingClaim.confidence || 0) - (newClaim.confidence || 0)) > 0.3
  ) {
    return {
      id: `conflict:${existingClaim.entityKey}:semantic`,
      detectorId: 'semantic_drift',
      previous: existingClaim.text,
      current: newClaim.text,
      description: 'Stored claim differs materially from new investigation claim.',
      previousConfidence: existingClaim.confidence,
      currentConfidence: newClaim.confidence,
      action: 'reinvestigate',
    };
  }

  return null;
}

/**
 * Reconcile incoming claim memory with stored claim.
 * @returns {{ memory: object, conflict: object|null }}
 */
function reconcileClaimMemory(existing, incoming, opts = {}) {
  if (!existing) {
    return { memory: incoming, conflict: null };
  }

  const conflict = detectClaimContradiction(existing, incoming);
  if (conflict) {
    const existingEffective = computeEffectiveConfidence(existing, opts);
    const incomingConf = incoming.confidence != null ? Number(incoming.confidence) : 0;

    if (incomingConf > existingEffective + 0.1) {
      return {
        memory: {
          ...incoming,
          status: MEMORY_STATUS.ACTIVE,
          contradictions: [...(existing.contradictions || []), conflict],
          previousValue: existing.text,
        },
        conflict,
      };
    }

    return {
      memory: {
        ...existing,
        status: MEMORY_STATUS.CONFLICT,
        contradictions: [...(existing.contradictions || []), conflict],
      },
      conflict,
    };
  }

  return {
    memory: {
      ...existing,
      ...incoming,
      status: MEMORY_STATUS.ACTIVE,
      confidence: Math.max(existing.confidence || 0, incoming.confidence || 0),
    },
    conflict: null,
  };
}

function detectMemoryContradictions(existingClaims = [], incomingClaims = []) {
  const conflicts = [];
  const byKey = new Map(existingClaims.map((c) => [c.entityKey, c]));

  for (const incoming of incomingClaims) {
    const existing = byKey.get(incoming.entityKey);
    if (!existing) continue;
    const conflict = detectClaimContradiction(existing, incoming);
    if (conflict) conflicts.push(conflict);
  }

  return conflicts;
}

module.exports = {
  detectClaimContradiction,
  reconcileClaimMemory,
  detectMemoryContradictions,
  CONTRADICTION_DETECTORS,
};
