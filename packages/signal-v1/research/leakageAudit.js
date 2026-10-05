'use strict';

const fs = require('fs');
const path = require('path');

const BACKFILL_DIR = path.join(__dirname, '../acquisition/backfill');

/** Tokens that must not appear as live reads in backfill generation (comments OK). */
const FORBIDDEN_IN_BACKFILL_SOURCE = Object.freeze([
  'selectionCategory',
  'PASS',
  'FAIL',
  'futurePrice',
  'future price',
  'labelMarketOutcome',
  'athTimestamp',
  'ATH',
]);

/**
 * Static scan: backfill modules must not branch on cohort balancing / outcome labels.
 */
function scanBackfillModulesForLeakage() {
  const files = fs.readdirSync(BACKFILL_DIR).filter(f => f.endsWith('.js'));
  const violations = [];

  for (const file of files) {
    const abs = path.join(BACKFILL_DIR, file);
    const lines = fs.readFileSync(abs, 'utf8').split('\n');
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      const trimmed = line.trim();
      if (trimmed.startsWith('//') || trimmed.startsWith('*')) continue;
      for (const token of FORBIDDEN_IN_BACKFILL_SOURCE) {
        if (line.includes(token)) {
          violations.push({ file, line: i + 1, token, snippet: trimmed.slice(0, 120) });
        }
      }
    }
  }

  return {
    clean: violations.length === 0,
    violations,
  };
}

/**
 * Documented coupling in v1 procedural catalog (cohort 001 artifact source).
 */
function documentKnownCohort001Coupling() {
  return {
    proceduralCatalogV1: {
      file: 'acquisition/providers/proceduralCandidateProvider.js',
      issue: 'acquisitionPayload.pattern derived from selectionCategory (stronger→dual_cluster_run)',
      affects: ['caller count', 'cluster identity', 'convergence events', 'fixture price paths via pattern'],
    },
    marketBackfillHistorical: {
      note: 'marketBackfill no longer reads selectionCategory after SIGNAL-V1-004 hardening',
    },
  };
}

module.exports = {
  scanBackfillModulesForLeakage,
  documentKnownCohort001Coupling,
  FORBIDDEN_IN_BACKFILL_SOURCE,
};
