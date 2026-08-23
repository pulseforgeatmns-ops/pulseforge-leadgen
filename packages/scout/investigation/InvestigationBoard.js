'use strict';

/**
 * SPEC-145 — Live investigation board.
 * Maintains Known / Unknown / Persistent unknowns with value-of-information scores.
 */

const { UNKNOWN_VERIFICATION } = require('../credibility/CredibilityFramework');

const UNKNOWN_STATUS = Object.freeze({
  OPEN: 'open',
  IN_PROGRESS: 'in_progress',
  RESOLVED: 'resolved',
  PERSISTENT: 'persistent',
});

const PERSISTENT_RESOLUTION = Object.freeze({
  REQUIRES_HUMAN: 'requires_human_conversation',
  SOURCE_UNAVAILABLE: 'source_unavailable',
  NOT_PUBLICLY_DISCOVERABLE: 'not_publicly_discoverable',
});

/** Default impact (0–1) and difficulty (0–1) per investigation gap. */
const GAP_VALUE_PROFILE = Object.freeze({
  decision_maker: { impact: 0.95, difficulty: 0.3, label: 'Decision maker' },
  portfolio_size: { impact: 0.9, difficulty: 0.45, label: 'Portfolio size' },
  cleaning_responsibility: { impact: 0.75, difficulty: 0.5, label: 'Current cleaner' },
  current_vendor: { impact: 0.9, difficulty: 0.6, label: 'Current cleaning vendor' },
  contact_path: { impact: 0.85, difficulty: 0.25, label: 'Contact path' },
  buying_signals: { impact: 0.8, difficulty: 0.4, label: 'Buying signals' },
  business_fit: { impact: 0.7, difficulty: 0.2, label: 'Business fit' },
  geographic_fit: { impact: 0.6, difficulty: 0.15, label: 'Geographic fit' },
  vendor_relationship: { impact: 0.85, difficulty: 0.55, label: 'Vendor relationship' },
  company_size: { impact: 0.5, difficulty: 0.25, label: 'Company size' },
  ownership: { impact: 0.75, difficulty: 0.35, label: 'Ownership' },
  office_hours: { impact: 0.1, difficulty: 0.05, label: 'Office hours' },
  revenue_estimate: { impact: 0.65, difficulty: 0.55, label: 'Revenue estimate' },
  contract_timing: { impact: 0.85, difficulty: 0.65, label: 'Contract timing' },
  expansion_plans: { impact: 0.7, difficulty: 0.5, label: 'Expansion plans' },
});

const DEFAULT_COVERAGE_THRESHOLD = 0.91;
const DEFAULT_DEAD_END_ATTEMPTS = 3;

function impactFromCredibility(gap) {
  const entry = UNKNOWN_VERIFICATION[gap];
  if (!entry) return null;
  if (entry.impact === 'high') return 0.9;
  if (entry.impact === 'medium') return 0.6;
  if (entry.impact === 'low') return 0.3;
  return null;
}

function getGapProfile(gap) {
  const key = String(gap || '').toLowerCase();
  const base = GAP_VALUE_PROFILE[key] || {
    impact: impactFromCredibility(key) || 0.5,
    difficulty: 0.4,
    label: key.replace(/_/g, ' '),
  };
  return { gap: key, ...base };
}

function computeExpectedValue(impact, difficulty) {
  return Number((impact * (1 - difficulty)).toFixed(3));
}

function buildKnownEntry(partial = {}) {
  return {
    gap: partial.gap,
    label: partial.label || getGapProfile(partial.gap).label,
    confidence: partial.confidence != null ? Number(partial.confidence) : 1,
    evidence: partial.evidence || partial.supportedBy || [],
    resolvedAt: partial.resolvedAt || new Date().toISOString(),
    source: partial.source || null,
  };
}

function buildUnknownEntry(partial = {}) {
  const profile = getGapProfile(partial.gap);
  const impact = partial.impact != null ? partial.impact : profile.impact;
  const difficulty = partial.difficulty != null ? partial.difficulty : profile.difficulty;
  return {
    gap: profile.gap,
    label: profile.label,
    impact,
    difficulty,
    expectedValue: computeExpectedValue(impact, difficulty),
    status: partial.status || UNKNOWN_STATUS.OPEN,
    failedProviders: Array.isArray(partial.failedProviders) ? partial.failedProviders : [],
    attemptCount: partial.attemptCount != null ? partial.attemptCount : 0,
    howToVerify: partial.howToVerify || (UNKNOWN_VERIFICATION[profile.gap] || {}).howToVerify || null,
  };
}

function buildPersistentEntry(partial = {}) {
  return {
    gap: partial.gap,
    label: partial.label || getGapProfile(partial.gap).label,
    resolution: partial.resolution || PERSISTENT_RESOLUTION.REQUIRES_HUMAN,
    failedProviders: partial.failedProviders || [],
    reason: partial.reason || `Three providers failed to resolve ${partial.gap}`,
    markedAt: partial.markedAt || new Date().toISOString(),
  };
}

/**
 * Create an investigation board from starting evidence and open gaps.
 * @param {object} input
 * @returns {object}
 */
function createInvestigationBoard(input = {}) {
  const known = (input.known || []).map(buildKnownEntry);
  const unknown = (input.unknown || input.missing || []).map((gap) =>
    typeof gap === 'string' ? buildUnknownEntry({ gap }) : buildUnknownEntry(gap)
  );
  const persistent = (input.persistent || []).map(buildPersistentEntry);

  return {
    known,
    unknown,
    persistent,
    coverageThreshold: input.coverageThreshold != null ? input.coverageThreshold : DEFAULT_COVERAGE_THRESHOLD,
    deadEndAttempts: input.deadEndAttempts != null ? input.deadEndAttempts : DEFAULT_DEAD_END_ATTEMPTS,
  };
}

function sortUnknownsByValue(unknowns = []) {
  return [...unknowns]
    .filter((u) => u.status === UNKNOWN_STATUS.OPEN || u.status === UNKNOWN_STATUS.IN_PROGRESS)
    .sort((a, b) => b.expectedValue - a.expectedValue);
}

/**
 * Highest-value open unknown.
 * @param {object} board
 * @returns {object|null}
 */
function getTopPriorityUnknown(board) {
  const sorted = sortUnknownsByValue(board.unknown || []);
  return sorted[0] || null;
}

function computeCoverage(board, keyGaps = null) {
  const keys =
    keyGaps ||
    [
      'decision_maker',
      'buying_signals',
      'portfolio_size',
      'contact_path',
      'cleaning_responsibility',
      'current_vendor',
    ];
  const knownGaps = new Set((board.known || []).map((k) => k.gap));
  const persistentGaps = new Set((board.persistent || []).map((p) => p.gap));
  let satisfied = 0;
  for (const gap of keys) {
    if (knownGaps.has(gap) || persistentGaps.has(gap)) satisfied += 1;
  }
  return keys.length > 0 ? Number((satisfied / keys.length).toFixed(3)) : 0;
}

/**
 * Update board after a step outcome.
 * @param {object} board
 * @param {object} update
 * @returns {object}
 */
function updateBoardAfterStep(board, update = {}) {
  const next = {
    ...board,
    known: [...(board.known || [])],
    unknown: [...(board.unknown || [])],
    persistent: [...(board.persistent || [])],
  };

  const gap = update.gap;
  if (!gap) return next;

  const idx = next.unknown.findIndex((u) => u.gap === gap);
  const resolved = (update.resolvedGaps || []).includes(gap) || update.resolved === true;
  const failed = update.failed === true || (update.collected || []).length === 0;

  if (resolved) {
    if (idx >= 0) next.unknown.splice(idx, 1);
    next.known.push(
      buildKnownEntry({
        gap,
        confidence: update.confidence,
        evidence: update.collected,
        source: update.providerId,
      })
    );
    return next;
  }

  if (idx < 0) return next;

  const entry = { ...next.unknown[idx] };
  if (failed && update.providerId) {
    entry.failedProviders = [...new Set([...(entry.failedProviders || []), update.providerId])];
    entry.attemptCount = (entry.attemptCount || 0) + 1;
  }

  if (entry.failedProviders.length >= (board.deadEndAttempts || DEFAULT_DEAD_END_ATTEMPTS)) {
    next.unknown.splice(idx, 1);
    next.persistent.push(
      buildPersistentEntry({
        gap,
        label: entry.label,
        failedProviders: entry.failedProviders,
        resolution: PERSISTENT_RESOLUTION.REQUIRES_HUMAN,
        reason: `${entry.failedProviders.length} providers failed to resolve ${entry.label}`,
      })
    );
  } else {
    entry.status = UNKNOWN_STATUS.OPEN;
    next.unknown[idx] = entry;
  }

  return next;
}

/**
 * Build board snapshot for reporting.
 * @param {object} board
 * @returns {object}
 */
function summarizeBoard(board) {
  const openUnknowns = sortUnknownsByValue(board.unknown || []);
  const top = openUnknowns[0] || null;
  const coverage = computeCoverage(board);

  return {
    knownCount: (board.known || []).length,
    unknownCount: openUnknowns.length,
    persistentCount: (board.persistent || []).length,
    coverage,
    coveragePct: Math.round(coverage * 100),
    topPriorityUnknown: top
      ? {
          gap: top.gap,
          label: top.label,
          impact: top.impact,
          difficulty: top.difficulty,
          expectedValue: top.expectedValue,
          whyHighestPriority: `Impact ${top.impact} vs difficulty ${top.difficulty} → expected value ${top.expectedValue}`,
        }
      : null,
    known: (board.known || []).map((k) => ({ gap: k.gap, label: k.label, confidence: k.confidence })),
    unknown: openUnknowns.map((u) => ({
      gap: u.gap,
      label: u.label,
      impact: u.impact,
      difficulty: u.difficulty,
      expectedValue: u.expectedValue,
      attemptCount: u.attemptCount,
    })),
    persistent: (board.persistent || []).map((p) => ({
      gap: p.gap,
      label: p.label,
      resolution: p.resolution,
      reason: p.reason,
    })),
  };
}

module.exports = {
  UNKNOWN_STATUS,
  PERSISTENT_RESOLUTION,
  GAP_VALUE_PROFILE,
  DEFAULT_COVERAGE_THRESHOLD,
  DEFAULT_DEAD_END_ATTEMPTS,
  getGapProfile,
  computeExpectedValue,
  buildKnownEntry,
  buildUnknownEntry,
  buildPersistentEntry,
  createInvestigationBoard,
  sortUnknownsByValue,
  getTopPriorityUnknown,
  computeCoverage,
  updateBoardAfterStep,
  summarizeBoard,
};
