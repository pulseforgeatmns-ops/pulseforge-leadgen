'use strict';
// Diagnostic tests for the first production run of
// scripts/investigationNarrativeAudit.js (PR #1120). They pin the root causes
// found in that run without changing behaviour; the proposed fixes await review.
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { extractDealClaims } = require('../ProspectDealClaims');

// The first root cause (the guard rejecting the canonical projection's own
// BEGIN READ ONLY / ROLLBACK, producing CANONICAL_PROJECTION_FAILURE before any
// read) is fixed by savepoint-based nesting; see p0NarrativeAuditTransactions.

test('diagnostic: "in rehab funds" is currently misread as a deal stage (false positive found while probing amount phrasing)', () => {
  const d = extractDealClaims('Fit evidence: $35K in rehab funds needed.');
  // Current behaviour (the defect): stage "in rehab" is extracted from a
  // funding phrase; no amount is extracted because no request verb precedes it.
  assert.equal(d.values.stage, 'in rehab');
  assert.equal(d.values.amount, undefined);
});
