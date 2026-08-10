'use strict';

/**
 * Scout sourcing quality gates — NH/US geo, cleaning competitors,
 * institutional firms, confidence, and source-specific fit rationale.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  CANDIDATE_STATUS,
  CONFIDENCE,
  PRIORITY_TOWNS_NH,
  REJECTION_REASON,
  interpretAnchorMarket,
  buildNhScopedSearchQueries,
  evaluateScoutCandidate,
  isGenericCriteriaCopy,
  formatSuggestedContactRole,
  batchMeetsQualityThreshold,
  gateScoutCandidateRows,
} = require('../services/scoutQualityGate');
const {
  buildSearchQueries,
  mapPublicHitToScoutCandidate,
  sourceScoutCandidatesFromPublicSources,
} = require('../services/scoutPublicSourcing');
const {
  COMPLETED_RESULT_GUARDRAILS,
  SCOUT_HANDOFF_STATUSES,
  buildScoutHandoff,
  handBriefToScout,
  handBriefToScoutAsync,
} = require('../services/scoutHandoff');
const {
  applyScoutExecutionResult,
} = require('../services/clientIntelligenceCampaignPlanning');
const {
  createMemoryScoutWorkRequestStore,
} = require('../services/scoutWorkRequestStore');

const PM_WORK_REQUEST = Object.freeze({
  targetSegment: 'Property managers',
  targetSubtype: 'multi-family',
  marketBounds:
    'Start with Bedford, Hooksett, Londonderry, Auburn, and Goffstown. Keep Greater Manchester in scope.',
  inclusionCriteria: [
    'Manage offices, mixed-use buildings, small commercial properties, or multi-tenant spaces',
    'Are located in Bedford, Hooksett, Londonderry, Auburn, Goffstown, or nearby Greater Manchester markets',
  ],
  exclusionCriteria: [
    'Large institutional property managers',
    'Cleaning companies, maid services, housekeeping, janitorial, carpet cleaning, and cleaning competitors',
  ],
  clientId: 10,
});

describe('scoutQualityGate — market interpretation', () => {
  it('interprets Anchor / Greater Manchester as New Hampshire, USA', () => {
    const market = interpretAnchorMarket('Greater Manchester', {
      clientId: 10,
    });
    assert.equal(market.marketLabel, 'New Hampshire, USA');
    assert.equal(market.state, 'NH');
    assert.equal(market.interpretedAsNewHampshire, true);
    assert.deepEqual(market.priorityTowns, [...PRIORITY_TOWNS_NH]);
    assert.ok(market.nearbyFillTowns.includes('Manchester'));
  });

  it('builds every search query with town + NH or New Hampshire', () => {
    const { queries } = buildNhScopedSearchQueries(PM_WORK_REQUEST);
    assert.ok(queries.length >= 5);
    for (const q of queries) {
      assert.match(q, /\bNH\b|New Hampshire/);
      assert.doesNotMatch(q, /Greater Manchester(?!\s+NH)/);
    }
    assert.ok(queries.some((q) => /Bedford NH/.test(q)));
    assert.ok(queries.some((q) => /Hooksett NH/.test(q)));
    assert.ok(queries.some((q) => /Londonderry NH/.test(q)));
    assert.ok(queries.some((q) => /Auburn NH/.test(q)));
    assert.ok(queries.some((q) => /Goffstown NH/.test(q)));
  });

  it('buildSearchQueries prioritizes NH towns over ambiguous Greater Manchester', () => {
    const queries = buildSearchQueries(PM_WORK_REQUEST);
    assert.ok(queries.every((q) => /\bNH\b|New Hampshire/.test(q)));
    assert.ok(queries.some((q) => /Bedford NH/.test(q)));
  });
});

describe('scoutQualityGate — hard rejects', () => {
  it('rejects UK candidates with rejectionReason outside_market_country', () => {
    const gate = evaluateScoutCandidate(
      {
        companyName: 'Salford Estates Management',
        address: 'Salford, Greater Manchester M3 5EQ, UK',
        location: 'Salford, Greater Manchester, England',
        website: 'https://salford-estates.co.uk',
        placeTypes: ['real_estate_agency'],
      },
      PM_WORK_REQUEST
    );
    assert.equal(gate.status, CANDIDATE_STATUS.REJECTED);
    assert.equal(gate.rejectionReason, REJECTION_REASON.OUTSIDE_MARKET_COUNTRY);
    assert.match(gate.statusReason, /outside_market_country|UK|non-US/i);

    const bareManchester = evaluateScoutCandidate(
      {
        companyName: 'Deansgate Property Management',
        address: '1 Deansgate, Manchester',
        location: 'Manchester',
        website: 'https://deansgate-pm.example',
        placeTypes: ['real_estate_agency'],
      },
      PM_WORK_REQUEST
    );
    assert.equal(bareManchester.status, CANDIDATE_STATUS.REJECTED);
    assert.equal(
      bareManchester.rejectionReason,
      REJECTION_REASON.OUTSIDE_MARKET_COUNTRY
    );

    const mapped = mapPublicHitToScoutCandidate(
      {
        companyName: 'Stockport Property Group',
        address: 'Stockport, Greater Manchester, United Kingdom',
        location: 'Stockport, UK',
        website: 'https://stockport-pm.example.co.uk',
        placeTypes: ['real_estate_agency'],
      },
      PM_WORK_REQUEST,
      0
    );
    assert.ok(mapped);
    assert.equal(mapped.status, CANDIDATE_STATUS.REJECTED);
    assert.equal(mapped.rejectionReason, REJECTION_REASON.OUTSIDE_MARKET_COUNTRY);
  });

  it('rejects cleaning companies with rejectionReason wrong_segment_cleaning_competitor', () => {
    const cases = [
      {
        companyName: 'Sparkle Cleaning Company',
        address: 'Bedford NH',
        placeTypes: ['cleaning_service'],
      },
      {
        companyName: 'Bedford Maid Service',
        address: 'Bedford, NH 03110',
        industry: 'maid service',
      },
      {
        companyName: 'Hooksett Housekeeping LLC',
        address: 'Hooksett NH',
        placeTypes: ['home_goods_store'],
      },
      {
        companyName: 'Granite Janitorial Services',
        address: 'Londonderry NH',
        industry: 'janitorial',
      },
      {
        companyName: 'Auburn Carpet Cleaning',
        address: 'Auburn NH',
      },
    ];
    for (const hit of cases) {
      const gate = evaluateScoutCandidate(
        { ...hit, website: 'https://example.com/clean' },
        PM_WORK_REQUEST
      );
      assert.equal(
        gate.status,
        CANDIDATE_STATUS.REJECTED,
        `expected reject for ${hit.companyName}`
      );
      assert.equal(
        gate.rejectionReason,
        REJECTION_REASON.WRONG_SEGMENT_CLEANING_COMPETITOR,
        `expected cleaning code for ${hit.companyName}`
      );
      assert.match(gate.statusReason, /wrong_segment_cleaning_competitor|cleaning|maid|housekeeping|janitorial|carpet/i);
    }
  });

  it('rejects large institutional / national firms unless operator approves', () => {
    const hit = {
      companyName: 'CBRE National Property Management',
      address: 'Bedford NH',
      location: 'Bedford, NH',
      website: 'https://www.cbre.com/bedford',
      placeTypes: ['real_estate_agency'],
      phone: '603-555-0100',
    };
    const rejected = evaluateScoutCandidate(hit, PM_WORK_REQUEST);
    assert.equal(rejected.status, CANDIDATE_STATUS.REJECTED);
    assert.match(rejected.statusReason, /institutional|national/i);

    const approved = evaluateScoutCandidate(hit, {
      ...PM_WORK_REQUEST,
      allowInstitutional: true,
    });
    assert.notEqual(approved.status, CANDIDATE_STATUS.REJECTED);
    assert.equal(approved.exclusionRisk, true);
    assert.notEqual(approved.confidence, CONFIDENCE.HIGH);
  });
});

describe('scoutQualityGate — confidence and rationale', () => {
  it('blocks high confidence when exclusion risk exists', () => {
    const gate = evaluateScoutCandidate(
      {
        companyName: 'Greystar Multifamily NH',
        address: 'Bedford, NH 03110',
        location: 'Bedford NH',
        website: 'https://greystar.example/bedford',
        placeTypes: ['real_estate_agency', 'property_management'],
        phone: '603-555-0199',
        industry: 'property management',
      },
      {
        ...PM_WORK_REQUEST,
        allowInstitutional: true,
      }
    );
    assert.equal(gate.exclusionRisk, true);
    assert.notEqual(gate.confidence, CONFIDENCE.HIGH);
  });

  it('allows high confidence only with source URL, NH location, PM fit, and contact signal', () => {
    const gate = evaluateScoutCandidate(
      {
        companyName: 'Bedford Property Management LLC',
        address: '12 Main St, Bedford, NH 03110',
        location: 'Bedford NH',
        website: 'https://bedfordpm.example',
        sourceUrl: 'https://bedfordpm.example',
        placeTypes: ['real_estate_agency'],
        industry: 'property management',
        phone: '603-555-0111',
      },
      PM_WORK_REQUEST
    );
    assert.equal(gate.status, CANDIDATE_STATUS.ACCEPTED);
    assert.equal(gate.confidence, CONFIDENCE.HIGH);
    assert.equal(gate.signals.sourceUrl, true);
    assert.equal(gate.signals.nhLocation, true);
    assert.equal(gate.signals.propertyManagementFit, true);
    assert.equal(gate.signals.reachableContactSignal, true);
  });

  it('requires source-specific fit rationale — not copied criteria', () => {
    const criteriaLine = PM_WORK_REQUEST.inclusionCriteria[0];
    assert.equal(isGenericCriteriaCopy(criteriaLine, PM_WORK_REQUEST), true);
    assert.equal(
      isGenericCriteriaCopy(
        'Public listing matches property managers — Market bounds: Greater Manchester',
        PM_WORK_REQUEST
      ),
      true
    );

    const gate = evaluateScoutCandidate(
      {
        companyName: 'Hooksett Facilities Partners',
        address: '88 Alice Ave, Hooksett, NH',
        location: 'Hooksett NH',
        website: 'https://hooksettfp.example',
        placeTypes: ['real_estate_agency'],
        industry: 'property management',
        phone: '603-555-0144',
        fitRationale: criteriaLine,
      },
      PM_WORK_REQUEST
    );
    assert.ok(gate.fitRationale);
    assert.notEqual(gate.fitRationale, criteriaLine);
    assert.doesNotMatch(gate.fitRationale, /Manage offices, mixed-use/i);
    assert.match(
      gate.fitRationale,
      /Hooksett Facilities Partners|address\/location on source|listing category\/type|website\/source signal|property-management relevance/i
    );
    assert.equal(isGenericCriteriaCopy(gate.fitRationale, PM_WORK_REQUEST), false);
  });

  it('labels suggested contact role unless verified named+title contact exists', () => {
    const suggested = formatSuggestedContactRole(PM_WORK_REQUEST, {
      companyName: 'x',
    });
    assert.match(suggested, /^Suggested contact role:/i);

    const verified = formatSuggestedContactRole(PM_WORK_REQUEST, {
      contactName: 'Jamie Lee',
      contactTitle: 'Property Manager',
    });
    assert.equal(verified, 'Jamie Lee — Property Manager');
    assert.doesNotMatch(verified, /^Suggested contact role:/i);
  });

  it('marks Manchester/Derry/Concord NH as review_required with outside_primary_town_cluster', () => {
    for (const town of ['Manchester', 'Derry', 'Concord']) {
      const gate = evaluateScoutCandidate(
        {
          companyName: `${town} NH Property Co`,
          address: `100 Main St, ${town}, NH`,
          location: `${town} NH`,
          website: `https://${town.toLowerCase()}pm.example`,
          placeTypes: ['real_estate_agency'],
          industry: 'property management',
          phone: '603-555-0188',
        },
        PM_WORK_REQUEST
      );
      assert.equal(gate.status, CANDIDATE_STATUS.REVIEW_REQUIRED);
      assert.notEqual(gate.status, CANDIDATE_STATUS.ACCEPTED);
      assert.notEqual(gate.confidence, CONFIDENCE.HIGH);
      assert.match(
        `${gate.statusReason} ${gate.risks} ${gate.reasonCode || ''}`,
        /outside_primary_town_cluster/
      );
    }
  });

  it('accepts only primary NH town cluster with property-management evidence', () => {
    const accepted = evaluateScoutCandidate(
      {
        companyName: 'Bedford Property Management LLC',
        address: '12 Main St, Bedford, NH 03110',
        location: 'Bedford NH',
        website: 'https://bedfordpm.example',
        sourceUrl: 'https://bedfordpm.example',
        placeTypes: ['real_estate_agency'],
        industry: 'property management',
        phone: '603-555-0111',
      },
      PM_WORK_REQUEST
    );
    assert.equal(accepted.status, CANDIDATE_STATUS.ACCEPTED);
    assert.equal(accepted.geo.tier, 'priority');
    assert.equal(accepted.signals.propertyManagementFit, true);

    const noPm = evaluateScoutCandidate(
      {
        companyName: 'Bedford Widget Shop',
        address: '12 Main St, Bedford, NH 03110',
        location: 'Bedford NH',
        website: 'https://bedfordwidgets.example',
        placeTypes: ['store'],
        phone: '603-555-0111',
      },
      PM_WORK_REQUEST
    );
    assert.notEqual(noPm.status, CANDIDATE_STATUS.ACCEPTED);
  });
});

describe('scoutQualityGate — sourcing integration', () => {
  it('filters UK and cleaning hits out of the usable batch', async () => {
    const result = await sourceScoutCandidatesFromPublicSources({
      workRequest: {
        ...PM_WORK_REQUEST,
        targetCountMin: 2,
        targetCountMax: 10,
      },
      opts: {
        publicSearchFn: async () => [
          {
            companyName: 'Salford PM UK',
            website: 'https://salford.example.co.uk',
            address: 'Salford, Greater Manchester, UK',
            location: 'Salford, UK',
            placeTypes: ['real_estate_agency'],
          },
          {
            companyName: 'Sparkle Maid Service NH',
            website: 'https://sparklemaid.example',
            address: 'Bedford NH',
            location: 'Bedford NH',
            placeTypes: ['cleaning_service'],
          },
          {
            companyName: 'Bedford Property Management LLC',
            website: 'https://bedfordpm.example',
            address: 'Bedford, NH 03110',
            location: 'Bedford NH',
            placeTypes: ['real_estate_agency'],
            industry: 'property management',
            phone: '603-555-0101',
          },
          {
            companyName: 'Hooksett Asset Managers',
            website: 'https://hooksettam.example',
            address: 'Hooksett NH',
            location: 'Hooksett NH',
            placeTypes: ['real_estate_agency'],
            industry: 'property management',
            phone: '603-555-0102',
          },
        ],
      },
    });
    assert.equal(result.ok, true);
    assert.ok(result.rejected.length >= 2);
    assert.ok(
      result.rejected.some(
        (r) => r.rejectionReason === REJECTION_REASON.OUTSIDE_MARKET_COUNTRY
      )
    );
    assert.ok(
      result.rejected.some(
        (r) =>
          r.rejectionReason === REJECTION_REASON.WRONG_SEGMENT_CLEANING_COMPETITOR
      )
    );
    assert.ok(result.candidates.every((c) => c.status !== CANDIDATE_STATUS.REJECTED));
    assert.ok(
      result.candidates.every((c) => /NH|New Hampshire/i.test(String(c.location || '')))
    );
    for (const c of result.candidates) {
      assert.ok(c.status === 'accepted' || c.status === 'review_required');
      assert.ok(c.statusReason);
      assert.match(c.suggestedContactRole, /Suggested contact role:|^.+\s—\s.+/);
    }
    assert.ok(result.groups);
    assert.ok(Array.isArray(result.groups.rejected));
    assert.ok(
      !result.groups.review_required.some((r) =>
        /Salford|Maid|cleaning/i.test(r.companyName)
      )
    );
  });

  it('fails quality gate when accepted/reviewable count is below threshold', () => {
    const gated = gateScoutCandidateRows(
      [
        {
          companyName: 'Bedford Property Management LLC',
          sourceUrl: 'https://bedfordpm.example',
          location: 'Bedford NH',
          address: 'Bedford, NH',
          placeTypes: ['real_estate_agency'],
          industry: 'property management',
          phone: '603-555-0111',
        },
        {
          companyName: 'UK Manchester Cleaners',
          sourceUrl: 'https://cleaners.example.co.uk',
          location: 'Manchester',
        },
      ],
      { ...PM_WORK_REQUEST, targetCountMin: 5, targetCountMax: 10 }
    );
    assert.equal(gated.meetsQualityThreshold, false);
    assert.ok(gated.usableCount < 5);
    const threshold = batchMeetsQualityThreshold(gated.candidates, {
      ...PM_WORK_REQUEST,
      targetCountMin: 5,
    });
    assert.equal(threshold.ok, false);

    const draft = buildScoutHandoff({
      campaignObjective: 'Validate walkthrough demand',
      targetSegment: 'Property managers',
      marketBounds: 'Bedford NH',
    });
    const result = handBriefToScout(draft, {
      targetCountMin: 5,
      scoutSourcingFn: () => [
        {
          companyName: 'Bedford Property Management LLC',
          sourceUrl: 'https://bedfordpm.example',
          location: 'Bedford NH',
          address: 'Bedford, NH',
          placeTypes: ['real_estate_agency'],
          industry: 'property management',
          phone: '603-555-0111',
        },
        {
          companyName: 'Salford Estates UK',
          sourceUrl: 'https://salford.example.co.uk',
          location: 'Salford, UK',
        },
        {
          companyName: 'Sparkle Cleaning Company',
          sourceUrl: 'https://sparkle.example',
          location: 'Bedford NH',
          placeTypes: ['cleaning_service'],
        },
      ],
    });
    assert.equal(result.ok, false);
    assert.equal(result.handoff.status, SCOUT_HANDOFF_STATUSES.FAILED_QUALITY_GATE);
    assert.equal(result.workRequest.status, SCOUT_HANDOFF_STATUSES.FAILED_QUALITY_GATE);
    assert.ok(result.candidateBatch);
    assert.ok(
      (result.candidateBatch.groups.rejected || []).some(
        (r) => r.rejectionReason === REJECTION_REASON.OUTSIDE_MARKET_COUNTRY
      )
    );
    assert.ok(
      (result.candidateBatch.groups.rejected || []).some(
        (r) =>
          r.rejectionReason === REJECTION_REASON.WRONG_SEGMENT_CLEANING_COMPETITOR
      )
    );
    assert.ok(
      !(result.candidateBatch.groups.review_required || []).some((r) =>
        /Salford|Sparkle|UK|Cleaning/i.test(r.companyName)
      )
    );
    assert.match(result.message, /Accepted candidates|Review required|Rejected candidates/i);
    assert.doesNotMatch(result.message, /Creating this brief does not hand/i);
  });

  it('executed work request output does not include draft handoff instructions', async () => {
    const store = createMemoryScoutWorkRequestStore();
    const draft = buildScoutHandoff({
      campaignObjective: 'Validate walkthrough demand',
      targetSegment: 'Property managers',
      marketBounds: 'Bedford NH, Hooksett NH',
      guardrails: [
        'Max does not build or fabricate the prospect list in this step',
        'Creating this brief does not hand it to Scout — say “Hand this brief to Scout” to approve and queue',
        'Scout inspects public sources only when sourcing execution is wired',
      ],
    });
    const result = await handBriefToScoutAsync(draft, {
      workRequestStore: store,
      targetCountMin: 1,
      publicSearchFn: async () => [
        {
          companyName: 'Bedford Property Management LLC',
          website: 'https://bedfordpm.example',
          address: 'Bedford, NH',
          location: 'Bedford NH',
          placeTypes: ['real_estate_agency'],
          industry: 'property management',
          phone: '603-555-0101',
        },
      ],
    });
    // One evidenced PM with min=1 completes; draft text must still be gone.
    assert.equal(result.ok, true);
    assert.deepEqual(result.handoff.guardrails, [...COMPLETED_RESULT_GUARDRAILS]);
    assert.doesNotMatch(
      JSON.stringify(result.handoff.guardrails),
      /creating this brief|when sourcing execution is wired|in this step/i
    );
    assert.doesNotMatch(result.message, /Creating this brief does not hand/i);
    for (const g of COMPLETED_RESULT_GUARDRAILS) {
      assert.ok(result.candidateBatch.guardrails.includes(g));
    }

    const applied = applyScoutExecutionResult(
      {
        message: 'Approving the Scout Handoff Brief…',
        scoutHandoffBrief: {
          guardrails: [
            'Creating this brief does not hand it to Scout — say “Hand this brief to Scout” to approve and queue',
            'Scout inspects public sources only when sourcing execution is wired',
          ],
          reviewGate:
            'Creating this brief does not hand it to Scout.',
          disclaimer:
            'Scout Handoff Brief only. Creating this brief does not hand it to Scout.',
          recommendedNextStep: 'Say Hand this brief to Scout',
        },
      },
      result
    );
    assert.deepEqual(applied.scoutHandoffBrief.guardrails, [
      ...COMPLETED_RESULT_GUARDRAILS,
    ]);
    assert.doesNotMatch(
      JSON.stringify(applied.scoutHandoffBrief),
      /Creating this brief does not hand|when sourcing execution is wired|in this step/i
    );

    // Failed quality-gate path also strips draft section-11 language.
    const failed = handBriefToScout(draft, {
      targetCountMin: 10,
      scoutSourcingFn: () => [
        {
          companyName: 'Salford UK PM',
          sourceUrl: 'https://salford.example.co.uk',
          location: 'Salford, UK',
        },
      ],
    });
    assert.equal(failed.handoff.status, SCOUT_HANDOFF_STATUSES.FAILED_QUALITY_GATE);
    assert.deepEqual(failed.handoff.guardrails, [...COMPLETED_RESULT_GUARDRAILS]);
    const appliedFailed = applyScoutExecutionResult(
      {
        scoutHandoffBrief: {
          guardrails: [
            'Creating this brief does not hand it to Scout — say “Hand this brief to Scout” to approve and queue',
          ],
        },
      },
      failed
    );
    assert.doesNotMatch(
      JSON.stringify(appliedFailed.scoutHandoffBrief.guardrails),
      /Creating this brief does not hand/i
    );
  });

  it('injected scoutSourcingFn path cannot bypass quality gates', () => {
    const draft = buildScoutHandoff({
      campaignObjective: 'Validate walkthrough demand',
      targetSegment: 'Property managers',
      marketBounds: 'Bedford NH',
      inclusionCriteria: PM_WORK_REQUEST.inclusionCriteria,
      exclusionCriteria: PM_WORK_REQUEST.exclusionCriteria,
    });
    const result = handBriefToScout(draft, {
      targetCountMin: 1,
      scoutSourcingFn: () => [
        {
          companyName: 'UK Manchester Cleaners',
          sourceUrl: 'https://cleaners.example.co.uk',
          location: 'Manchester',
          fitRationale: PM_WORK_REQUEST.inclusionCriteria[0],
          suggestedContactRole: 'Owner / decision-maker',
          confidence: 'high',
        },
        {
          companyName: 'Bedford Property Management LLC',
          sourceUrl: 'https://bedfordpm.example',
          location: 'Bedford NH',
          address: 'Bedford, NH',
          placeTypes: ['real_estate_agency'],
          industry: 'property management',
          phone: '603-555-0111',
          fitRationale: PM_WORK_REQUEST.inclusionCriteria[0],
          suggestedContactRole: 'Owner / decision-maker',
          confidence: 'high',
        },
      ],
    });
    assert.equal(result.ok, true);
    assert.ok(
      (result.candidateBatch.rejected || []).some(
        (r) =>
          r.rejectionReason === REJECTION_REASON.OUTSIDE_MARKET_COUNTRY ||
          r.rejectionReason === REJECTION_REASON.WRONG_SEGMENT_CLEANING_COMPETITOR
      )
    );
    const kept = result.candidateBatch.candidates;
    assert.ok(kept.length >= 1);
    assert.ok(kept.every((r) => r.status !== 'rejected'));
    for (const row of kept) {
      assert.doesNotMatch(row.fitRationale || '', /Manage offices, mixed-use/i);
      assert.match(
        row.fitRationale || '',
        /sourced from public listing|address\/location on source|listing category\/type|website\/source signal|property-management relevance/i
      );
      assert.match(row.suggestedContactRole || '', /Suggested contact role:/i);
      assert.doesNotMatch(row.suggestedContactRole || '', /decision-maker/i);
    }
    assert.ok(result.candidateBatch.groups);
    assert.ok(Array.isArray(result.candidateBatch.groups.rejected));
  });
});
