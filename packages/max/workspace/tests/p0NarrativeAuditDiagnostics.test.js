'use strict';
// Diagnostic tests for the production runs of
// scripts/investigationNarrativeAudit.js (PR #1120).
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { extractDealClaims } = require('../ProspectDealClaims');

// The first root cause (the guard rejecting the canonical projection's own
// BEGIN READ ONLY / ROLLBACK, producing CANONICAL_PROJECTION_FAILURE before any
// read) is fixed by savepoint-based nesting; see p0NarrativeAuditTransactions.

test('diagnostic (fixed in fit-narrative-claims-v2): "in rehab funds" is no longer misread as a deal stage', () => {
  const d = extractDealClaims('Fit evidence: $35K in rehab funds needed.');
  // Previously stage "in rehab" was extracted from this funding phrase. The
  // approved correction removes the false positive only; this wording is not
  // added as a financing-request form.
  assert.equal(d.values.stage, undefined);
  assert.equal(d.values.amount, undefined);
  assert.deepEqual(d.unparsed, []);
});
