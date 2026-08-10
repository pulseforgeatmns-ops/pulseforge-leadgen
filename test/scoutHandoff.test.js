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
      marketBounds: 'Bedford NH, Hooksett NH',
    });
    const result = handBriefToScout(draft, {
      scoutSourcingFn: () => [
        {
          companyName: 'Granite Property Management',
          sourceUrl: 'https://example.com/granite',
          location: 'Bedford NH',
          address: '12 Main St, Bedford, NH',
          placeTypes: ['real_estate_agency'],
          industry: 'property management',
          phone: '603-555-0100',
          fitRationale:
            'Granite Property Management sourced from public listing — address/location on source: 12 Main St, Bedford, NH — source URL: https://example.com/granite',
          risks: 'Thin contact page',
          suggestedContactRole: 'Owner / property manager',
          confidence: 'high',
        },
        {
          companyName: 'Missing URL Co',
          location: 'Bedford NH',
          fitRationale: 'Should be dropped',
        },
        {
          companyName: 'Salford UK Cleaning Co',
          sourceUrl: 'https://salford.example.co.uk',
          location: 'Salford, Greater Manchester, UK',
          placeTypes: ['cleaning_service'],
          confidence: 'high',
        },
        {
          companyName: 'Hooksett Property Management LLC',
          website: 'https://example.com/hooksett',
          location: 'Hooksett NH',
          address: 'Hooksett NH',
          placeTypes: ['real_estate_agency'],
          industry: 'property management',
          phone: '603-555-0101',
          fitReason:
            'Hooksett Property Management LLC sourced from public listing — address/location on source: Hooksett NH — source URL: https://example.com/hooksett',
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
    assert.ok(result.candidateBatch.candidates.length >= 1);
    assert.ok(
      result.candidateBatch.candidates.every((row) => row.status !== 'rejected')
    );
    assert.ok(
      (result.candidateBatch.rejected || []).some((row) =>
        /Salford|UK|cleaning/i.test(
          `${row.companyName} ${row.rejectionReason || row.statusReason || ''}`
        )
      )
    );
    for (const row of result.candidateBatch.candidates) {
      assert.ok(row.sourceUrl);
      assert.equal(row.placeholder, false);
      assert.match(String(row.suggestedContactRole || ''), /Suggested contact role:/i);
      assert.doesNotMatch(
        String(row.suggestedContactRole || ''),
        /Owner \/ decision-maker/i
      );
    }
    assert.doesNotMatch(result.message, /placeholder/i);
    assert.doesNotMatch(result.message, /Creating this brief does not hand/i);
    assert.doesNotMatch(result.message, /when sourcing execution is wired/i);
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
    const draft = buildScoutHandoff({
      campaignObjective: 'x',
      targetSegment: 'Property managers',
      marketBounds: 'Bedford NH',
    });
    const ran = handBriefToScout(draft, {
      scoutSourcingFn: () => [
        {
          companyName: 'Bedford Property Management',
          sourceUrl: 'https://a.example',
          location: 'Bedford NH',
          address: 'Bedford, NH',
          placeTypes: ['real_estate_agency'],
          industry: 'property management',
          phone: '603-555-0100',
          fitRationale:
            'Bedford Property Management sourced from public listing — address/location on source: Bedford, NH — source URL: https://a.example',
        },
      ],
    });
    assert.equal(ran.ok, true);
    const approved = approveScoutResults(ran.handoff);
    assert.equal(approved.ok, true);
    assert.equal(approved.handoff.resultsApproved, true);
    assert.equal(approved.handoff.crmWritesMade, false);
    assert.equal(approved.handoff.outreachCopyGenerated, false);
    assert.equal(approved.handoff.accountChangesMade, false);
  });
});
