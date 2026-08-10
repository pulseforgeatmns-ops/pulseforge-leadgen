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
  interpretAnchorMarket,
  buildNhScopedSearchQueries,
  evaluateScoutCandidate,
  isGenericCriteriaCopy,
  formatSuggestedContactRole,
} = require('../services/scoutQualityGate');
const {
  buildSearchQueries,
  mapPublicHitToScoutCandidate,
  sourceScoutCandidatesFromPublicSources,
} = require('../services/scoutPublicSourcing');
const {
  COMPLETED_RESULT_GUARDRAILS,
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
  it('rejects UK Greater Manchester / Salford / Stockport results', () => {
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
    assert.ok(gate.rejectionReason);
    assert.match(gate.statusReason, /UK|non-US|New Hampshire/i);

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
    assert.match(bareManchester.rejectionReason || bareManchester.statusReason, /UK|Manchester|NH/i);

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
    assert.match(mapped.statusReason, /UK|non-US|New Hampshire/i);
  });

  it('rejects cleaning companies, maid services, and housekeeping competitors', () => {
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
      assert.match(gate.statusReason, /cleaning|maid|housekeeping|janitorial|carpet/i);
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
    assert.match(gate.fitRationale, /Hooksett Facilities Partners|Hooksett|listing types|source URL/i);
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

  it('marks Manchester NH as review_required (nearby/fill)', () => {
    const gate = evaluateScoutCandidate(
      {
        companyName: 'Manchester NH Property Co',
        address: '1000 Elm St, Manchester, NH',
        location: 'Manchester NH',
        website: 'https://mancpm.example',
        placeTypes: ['real_estate_agency'],
        industry: 'property management',
        phone: '603-555-0188',
      },
      PM_WORK_REQUEST
    );
    assert.equal(gate.status, CANDIDATE_STATUS.REVIEW_REQUIRED);
    assert.match(gate.statusReason, /Manchester|nearby|fill|review/i);
  });

  it('downgrades Concord/Derry NH unless explicitly allowed', () => {
    const concord = evaluateScoutCandidate(
      {
        companyName: 'Concord Property Management',
        address: '33 N Main St, Concord, NH',
        location: 'Concord NH',
        website: 'https://concordpm.example',
        placeTypes: ['real_estate_agency'],
        industry: 'property management',
        phone: '603-555-0200',
      },
      PM_WORK_REQUEST
    );
    assert.equal(concord.status, CANDIDATE_STATUS.REVIEW_REQUIRED);
    assert.notEqual(concord.status, CANDIDATE_STATUS.ACCEPTED);
    assert.match(concord.statusReason, /Concord|cluster|review/i);
    assert.notEqual(concord.confidence, CONFIDENCE.HIGH);

    const derry = evaluateScoutCandidate(
      {
        companyName: 'Derry Real Estate Management',
        address: '10 Birch St, Derry, NH',
        location: 'Derry NH',
        website: 'https://derrypm.example',
        placeTypes: ['real_estate_agency'],
        industry: 'property management',
        phone: '603-555-0201',
      },
      PM_WORK_REQUEST
    );
    assert.equal(derry.status, CANDIDATE_STATUS.REVIEW_REQUIRED);
    assert.notEqual(derry.confidence, CONFIDENCE.HIGH);

    const manchester = evaluateScoutCandidate(
      {
        companyName: 'Manchester NH Property Co',
        address: '1000 Elm St, Manchester, NH',
        location: 'Manchester NH',
        website: 'https://mancpm.example',
        placeTypes: ['real_estate_agency'],
        industry: 'property management',
        phone: '603-555-0188',
      },
      PM_WORK_REQUEST
    );
    assert.equal(manchester.status, CANDIDATE_STATUS.REVIEW_REQUIRED);
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
      result.rejected.some((r) => /Salford|UK/i.test(r.companyName + r.statusReason))
    );
    assert.ok(
      result.rejected.some((r) => /Maid|cleaning/i.test(r.companyName + r.statusReason))
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
  });

  it('completed handoff drops draft-style guardrail text', async () => {
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
      (result.candidateBatch.rejected || []).some((r) =>
        /UK|Manchester|cleaning|cleaner/i.test(
          `${r.companyName} ${r.rejectionReason || ''}`
        )
      )
    );
    const kept = result.candidateBatch.candidates;
    assert.ok(kept.length >= 1);
    assert.ok(kept.every((r) => r.status !== 'rejected'));
    for (const row of kept) {
      assert.doesNotMatch(row.fitRationale || '', /Manage offices, mixed-use/i);
      assert.match(row.fitRationale || '', /sourced from public listing|address\/location on source|source URL/i);
      assert.match(row.suggestedContactRole || '', /Suggested contact role:/i);
      assert.doesNotMatch(row.suggestedContactRole || '', /decision-maker/i);
    }
    assert.ok(result.candidateBatch.groups);
    assert.ok(Array.isArray(result.candidateBatch.groups.rejected));
  });
});
