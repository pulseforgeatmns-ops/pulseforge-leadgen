'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  SCOUT_HANDOFF_STATUSES,
  SCOUT_HANDOFF_UI_STATUS,
  SCOUT_SOURCING_NOT_WIRED_MESSAGE,
  buildScoutHandoff,
  handBriefToScout,
  isScoutSourcingExecutionWired,
  filterValidScoutCandidates,
  approveScoutResults,
} = require('../services/scoutHandoff');

describe('scoutHandoff lifecycle', () => {
  it('builds a draft handoff object with required schema fields', () => {
    const handoff = buildScoutHandoff({
      campaignObjective: 'Validate walkthrough demand',
      targetSegment: 'Property managers',
      targetSubtype: 'multi-family',
      marketBounds: 'Greater Manchester NH',
      inclusionCriteria: ['Local portfolios'],
      exclusionCriteria: ['National chains'],
    });

    assert.equal(handoff.kind, 'scout_handoff');
    assert.ok(handoff.handoffId);
    assert.equal(handoff.source, 'max');
    assert.equal(handoff.target, 'scout');
    assert.equal(handoff.status, SCOUT_HANDOFF_STATUSES.DRAFT);
    assert.equal(handoff.uiStatus, SCOUT_HANDOFF_UI_STATUS.BRIEF_CREATED);
    assert.equal(handoff.campaignObjective, 'Validate walkthrough demand');
    assert.ok(Array.isArray(handoff.requiredFields));
    assert.ok(Array.isArray(handoff.sourceTypes));
    assert.ok(Array.isArray(handoff.evidenceRequirements));
    assert.ok(Array.isArray(handoff.confidenceRules));
    assert.ok(Array.isArray(handoff.guardrails));
    assert.ok(handoff.createdAt);
    assert.ok(handoff.updatedAt);
    assert.equal(handoff.scoutRan, false);
    assert.equal(handoff.crmWritesMade, false);
    assert.equal(handoff.outreachCopyGenerated, false);
  });

  it('defaults Scout sourcing as not wired without Places or injects', () => {
    assert.equal(isScoutSourcingExecutionWired({ scoutSourcingSupported: false }), false);
    assert.equal(
      isScoutSourcingExecutionWired({
        scoutSourcingSupported: false,
        scoutPublicSourcingSupported: false,
      }),
      false
    );
    assert.equal(isScoutSourcingExecutionWired({ scoutSourcingSupported: true }), true);
    assert.equal(
      isScoutSourcingExecutionWired({ scoutSourcingFn: () => [] }),
      true
    );
    assert.equal(
      isScoutSourcingExecutionWired({
        publicSearchFn: async () => [],
      }),
      true
    );
  });

  it('Hand this brief to Scout creates work request and returns not-wired boundary', () => {
    const draft = buildScoutHandoff({
      campaignObjective: 'Validate walkthrough demand',
      targetSegment: 'Property managers',
      marketBounds: 'Manchester NH',
    });
    const result = handBriefToScout(draft, {
      scoutSourcingSupported: false,
      scoutPublicSourcingSupported: false,
    });

    assert.equal(result.ok, true);
    assert.equal(result.scoutRan, false);
    assert.equal(result.sourcingUnavailable, true);
    assert.equal(result.executionWired, false);
    assert.equal(result.shouldExecuteScoutSourcing, false);
    assert.equal(result.intent, 'scout_sourcing_not_wired');
    assert.match(result.message, new RegExp(SCOUT_SOURCING_NOT_WIRED_MESSAGE));
    assert.doesNotMatch(result.message, /Scout inspected/i);
    assert.doesNotMatch(result.message, /placeholder/i);
    assert.ok(result.workRequest);
    assert.ok(result.workRequest.workRequestId);
    assert.equal(result.workRequest.handoffId, draft.handoffId);
    assert.equal(result.workRequest.crmWritesAllowed, false);
    assert.equal(result.workRequest.outreachAllowed, false);
    assert.equal(
      result.handoff.uiStatus,
      SCOUT_HANDOFF_UI_STATUS.SCOUT_UNAVAILABLE
    );
    assert.equal(result.candidateBatch, null);
  });

  it('when Scout sourcing is wired, returns review-only candidates with source URLs', () => {
    const draft = buildScoutHandoff({
      campaignObjective: 'Validate walkthrough demand',
      targetSegment: 'Property managers',
      marketBounds: 'Manchester NH',
    });
    const result = handBriefToScout(draft, {
      scoutSourcingFn: () => [
        {
          companyName: 'Granite Property Mgmt',
          sourceUrl: 'https://example.com/granite',
          location: 'Manchester NH',
          fitRationale: 'Local PM in market bounds',
          risks: 'Thin contact page',
          suggestedContactRole: 'Owner / property manager',
          confidence: 'medium',
        },
        {
          companyName: 'Missing URL Co',
          location: 'Bedford NH',
          fitRationale: 'Should be dropped',
        },
        {
          companyName: 'Hooksett Facilities LLC',
          website: 'https://example.com/hooksett',
          location: 'Hooksett NH',
          fitReason: 'Facility contact listing',
          contactRole: 'Office manager',
          confidence: 'high',
        },
      ],
    });

    assert.equal(result.ok, true);
    assert.equal(result.scoutRan, true);
    assert.equal(result.intent, 'scout_handoff_completed');
    assert.equal(result.handoff.status, SCOUT_HANDOFF_STATUSES.COMPLETED);
    assert.equal(
      result.handoff.uiStatus,
      SCOUT_HANDOFF_UI_STATUS.SCOUT_RESULTS_READY
    );
    assert.ok(result.candidateBatch);
    assert.equal(result.candidateBatch.reviewOnly, true);
    assert.equal(result.candidateBatch.resultsApproved, false);
    assert.equal(result.candidateBatch.crmWritesMade, false);
    assert.equal(result.candidateBatch.outreachCopyGenerated, false);
    assert.equal(result.candidateBatch.candidates.length, 2);
    for (const row of result.candidateBatch.candidates) {
      assert.ok(row.sourceUrl);
      assert.equal(row.placeholder, false);
    }
    assert.doesNotMatch(result.message, /placeholder/i);
  });

  it('drops candidates without source URLs', () => {
    const rows = filterValidScoutCandidates([
      { companyName: 'A', sourceUrl: 'https://a.example' },
      { companyName: 'B' },
      { companyName: 'C', website: 'https://c.example' },
    ]);
    assert.equal(rows.length, 2);
    assert.ok(rows.every((r) => r.sourceUrl));
  });

  it('approveScoutResults marks review gate without CRM/outreach', () => {
    const draft = buildScoutHandoff({ campaignObjective: 'x' });
    const ran = handBriefToScout(draft, {
      scoutSourcingFn: () => [
        {
          companyName: 'A',
          sourceUrl: 'https://a.example',
          location: 'Manchester',
          fitRationale: 'fit',
        },
      ],
    });
    const approved = approveScoutResults(ran.handoff);
    assert.equal(approved.ok, true);
    assert.equal(approved.handoff.resultsApproved, true);
    assert.equal(approved.handoff.crmWritesMade, false);
    assert.equal(approved.handoff.outreachCopyGenerated, false);
    assert.equal(approved.handoff.accountChangesMade, false);
  });
});
