'use strict';
// Diagnostic tests for the first production run of
// scripts/investigationNarrativeAudit.js (PR #1120). They pin the root causes
// found in that run without changing behaviour; the proposed fixes await review.
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const audit = require('../../../../scripts/investigationNarrativeAudit');
const { deriveBlueprintCompatibility } = require('../../../../lib/canonicalProjection');
const { extractDealClaims } = require('../ProspectDealClaims');

test('diagnostic: the audit guard rejects the canonical projection\'s own BEGIN READ ONLY, so CANONICAL_PROJECTION_FAILURE is produced before any database read', async () => {
  const reachedDb = [];
  const boundary = { reads: 0, writesBlocked: 0 };
  const conn = audit.guardedConnection({ query: async sql => { reachedDb.push(sql); return { rows: [] }; } }, boundary);
  const facts = await deriveBlueprintCompatibility({ tenant_id: 't', snapshot_id: 's', pool: conn });
  // Current behaviour (the defect): the projection reports UNAVAILABLE because
  // the guard threw on BEGIN; getApprovedClientBlueprint turns that into
  // CANONICAL_PROJECTION_FAILURE. No canonical data was ever queried, so the
  // audit's Blueprint result says nothing about the production Blueprint.
  assert.equal(facts._projection_metadata.completeness, 'UNAVAILABLE');
  assert.equal(facts._projection_metadata.error, 'operational_inspection_read_only_violation');
  assert.deepEqual(reachedDb, []);
  assert.equal(boundary.writesBlocked, 2); // its BEGIN READ ONLY, then its cleanup ROLLBACK
});

test('diagnostic: "in rehab funds" is currently misread as a deal stage (false positive found while probing amount phrasing)', () => {
  const d = extractDealClaims('Fit evidence: $35K in rehab funds needed.');
  // Current behaviour (the defect): stage "in rehab" is extracted from a
  // funding phrase; no amount is extracted because no request verb precedes it.
  assert.equal(d.values.stage, 'in rehab');
  assert.equal(d.values.amount, undefined);
});
