'use strict';

/**
 * SPEC-255 — First-party paid attribution retrieval for canonical Penny.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const amo = require('../../acquisition-mission');
const {
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  ACQUISITION_APPROACHES,
  buildExecutionInput,
  createAcquisitionMissionEngine,
} = amo;
const {
  loadFirstPartyAttributionEvidence,
  mergeAcquisitionEvidence,
  mapAgentActionToEvidence,
  unavailableFirstPartyAttributionEvidence,
  ACTION_TYPE,
  AVAILABILITY,
  PLATFORM,
} = require('../index');
const {
  resolvePlatformEvidenceForPenny,
  loadFirstPartyAttributionEvidenceForPenny,
  runPennyForAmoMission,
  runPennyPaidAcquisition,
} = require('../../max/workspace/PennyPaidAcquisitionExecutor');
const { SOURCE_KIND } = require('../../../lib/walkthroughAttribution');

const CLIENT_10 = 10;
const CLIENT_1 = 1;

function productionFixture(overrides = {}) {
  const createdAt = overrides.created_at || new Date('2026-09-10T12:00:00.000Z');
  return {
    id: overrides.id ?? 9001,
    created_at: createdAt,
    payload: {
      source: 'website_walkthrough',
      attribution: {
        raw: {
          campaign_id: 'test_campaign',
          ad_group_id: 'test_group',
          ad_id: 'test_ad',
          opref: 'test_opref',
          oppref: 'test_oppref',
          utm_source: 'openai',
        },
        normalized: {
          lead_source: 'chatgpt_ads',
          attribution_status: 'deterministic',
          captured_at: createdAt.toISOString(),
        },
        provenance: {
          sourceKind: SOURCE_KIND,
          clientSubmitted: true,
          readOnly: true,
          observedAt: createdAt.toISOString(),
        },
      },
      ...(overrides.payloadExtra || {}),
    },
  };
}

function mockQueryRows(rowsByClient = {}) {
  return async ({ clientId, startAt, endAt, limit, actionType }) => {
    const rows = (rowsByClient[clientId] || [])
      .filter((row) => {
        const ts = new Date(row.created_at);
        return ts >= startAt && ts < endAt;
      })
      .slice(0, limit);
    return rows;
  };
}

describe('SPEC-255 — First-party attribution evidence loader', () => {
  const window = {
    start: '2026-09-01',
    end: '2026-09-14',
    days: 14,
    label: 'LAST_14_DAYS',
  };

  it('queries walkthrough_request agent_actions scoped by client', async () => {
    let capturedSql = '';
    const pool = {
      query: async (sql, params) => {
        capturedSql = sql;
        assert.equal(params[0], CLIENT_10);
        assert.equal(params[1], ACTION_TYPE);
        return { rows: [productionFixture()] };
      },
    };

    const result = await loadFirstPartyAttributionEvidence({
      clientId: CLIENT_10,
      pool,
      window,
    });

    assert.match(capturedSql, /FROM agent_actions/i);
    assert.match(capturedSql, /action_type = \$2/i);
    assert.match(capturedSql, /payload->'attribution' IS NOT NULL/i);
    assert.match(capturedSql, /client_id = \$1/i);
    assert.equal(result.availability, AVAILABILITY.AVAILABLE);
    assert.equal(result.observedCount, 1);
  });

  it('excludes attribution-null rows and enforces observation window', async () => {
    const rows = [
      productionFixture({ id: 1, created_at: new Date('2026-09-05T10:00:00.000Z') }),
      {
        id: 2,
        created_at: new Date('2026-08-20T10:00:00.000Z'),
        payload: { attribution: { raw: { campaign_id: 'old' }, normalized: {}, provenance: {} } },
      },
      {
        id: 3,
        created_at: new Date('2026-09-06T10:00:00.000Z'),
        payload: { source: 'website_walkthrough' },
      },
    ];

    const result = await loadFirstPartyAttributionEvidence({
      clientId: CLIENT_10,
      window,
      queryRows: mockQueryRows({ [CLIENT_10]: rows }),
    });

    assert.equal(result.observedCount, 1);
    assert.equal(result.evidence[0].evidenceId, 1);
  });

  it('maps agent_actions.id to stable evidence identity and preserves attribution fields', async () => {
    const row = productionFixture({ id: 4242 });
    const evidence = mapAgentActionToEvidence(row);

    assert.equal(evidence.evidenceId, 4242);
    assert.equal(evidence.kind, 'first_party_attributed_lead');
    assert.equal(evidence.sourceKind, SOURCE_KIND);
    assert.equal(evidence.campaignId, 'test_campaign');
    assert.equal(evidence.adGroupId, 'test_group');
    assert.equal(evidence.adId, 'test_ad');
    assert.equal(evidence.opref, 'test_opref');
    assert.equal(evidence.oppref, 'test_oppref');
    assert.equal(evidence.leadSource, 'chatgpt_ads');
    assert.equal(evidence.attributionStatus, 'deterministic');
    assert.equal(evidence.attribution.provenance.sourceKind, SOURCE_KIND);
    assert.equal(evidence.provenance.sourceKind, SOURCE_KIND);
    assert.notEqual(evidence.sourceKind, 'PLATFORM_API');
    assert.notEqual(evidence.provenance.sourceKind, 'PLATFORM_API');
  });

  it('returns valid empty observation when no rows match', async () => {
    const result = await loadFirstPartyAttributionEvidence({
      clientId: CLIENT_10,
      window,
      queryRows: mockQueryRows({ [CLIENT_10]: [] }),
    });

    assert.equal(result.availability, AVAILABILITY.AVAILABLE);
    assert.equal(result.observedCount, 0);
    assert.deepEqual(result.evidence, []);
  });

  it('does not treat query failure as zero leads', async () => {
    const pool = {
      query: async () => {
        throw new Error('connection refused');
      },
    };

    const result = await loadFirstPartyAttributionEvidence({
      clientId: CLIENT_10,
      pool,
      window,
    });

    assert.notEqual(result.availability, AVAILABILITY.AVAILABLE);
    assert.equal(result.observedCount, 0);
    assert.ok(result.error);
    assert.ok(result.reason);
  });

  it('returns no evidence for wrong client', async () => {
    const result = await loadFirstPartyAttributionEvidence({
      clientId: CLIENT_1,
      window,
      queryRows: mockQueryRows({ [CLIENT_10]: [productionFixture()] }),
    });

    assert.equal(result.observedCount, 0);
  });

  it('does not query prospects table', () => {
    const source = fs.readFileSync(
      path.join(__dirname, '../FirstPartyAttributionEvidence.js'),
      'utf8'
    );
    assert.doesNotMatch(source, /FROM prospects/i);
    assert.doesNotMatch(source, /acquisition_metadata/i);
  });

  it('uses read-only SELECT SQL only', () => {
    const source = fs.readFileSync(
      path.join(__dirname, '../FirstPartyAttributionEvidence.js'),
      'utf8'
    );
    assert.match(source, /SELECT/i);
    assert.doesNotMatch(source, /\bINSERT\b/i);
    assert.doesNotMatch(source, /\bUPDATE\b/i);
    assert.doesNotMatch(source, /\bDELETE\b/i);
  });

  it('produces one acquisition evidence item per agent_action row', async () => {
    const rows = [
      productionFixture({ id: 11 }),
      productionFixture({ id: 12, created_at: new Date('2026-09-11T12:00:00.000Z') }),
    ];
    const result = await loadFirstPartyAttributionEvidence({
      clientId: CLIENT_10,
      window,
      queryRows: mockQueryRows({ [CLIENT_10]: rows }),
    });

    assert.equal(result.evidence.length, 2);
    assert.deepEqual(result.evidence.map((row) => row.evidenceId), [11, 12]);
  });
});

describe('SPEC-255 — acquisitionEvidence merge', () => {
  it('preserves caller-supplied evidence and deduplicates by evidenceId', () => {
    const observed = [mapAgentActionToEvidence(productionFixture({ id: 55 }))];
    const supplied = [
      {
        id: 55,
        label: 'Operator duplicate should not double-add',
        source: 'operator_manual',
      },
      {
        label: 'Operator supplemental note',
        source: 'operator_manual',
      },
    ];

    const merged = mergeAcquisitionEvidence(supplied, observed);
    assert.equal(merged.length, 2);
    assert.ok(merged.some((row) => row.kind === 'first_party_attributed_lead'));
    assert.ok(merged.some((row) => row.label === 'Operator supplemental note'));
    assert.ok(merged.some((row) => row.provenance?.sourceKind === SOURCE_KIND));
    assert.ok(merged.some((row) => row.provenance?.sourceKind === 'OPERATOR_SUPPLIED'));
  });
});

describe('SPEC-255 — Canonical Penny integration', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: 'Acquire recurring commercial cleaning customers in Manchester NH.',
      targetSegment: 'Law Firms',
    });
  });

  it('auto-loads first-party attribution before Penny execution', async () => {
    engine.contribute(mission.id, {
      specialist: SPECIALISTS.MAX,
      kind: CONTRIBUTION_KINDS.ACQUISITION_APPROACH,
      payload: {
        acquisitionApproach: {
          selectedApproach: ACQUISITION_APPROACHES.PAID,
          rationale: 'Test paid path.',
        },
      },
    }, { tenantId: '10' });

    const fixture = productionFixture();
    const result = await runPennyForAmoMission(engine.get(mission.id, '10'), {
      engine,
      tenantId: '10',
      contributions: engine.inspect(mission.id, { tenantId: '10' }).contributions,
      transactionId: 'txn-fp-1',
      skipPlatformEvidenceCollection: true,
      queryWalkthroughAttribution: mockQueryRows({ [CLIENT_10]: [fixture] }),
      availableBudget: { amount: 500, currency: 'USD' },
      conversionReadiness: { ready: true },
      measurementReadiness: { ready: true },
    });

    assert.equal(result.spec, 'SPEC-132');
    const recommendation = result.contributions.paidAcquisitionRecommendation;
    assert.ok(recommendation);
  });

  it('feeds first-party rows into pennyInput specialistInput.evidence', async () => {
    const current = engine.get(mission.id, '10');
    const acquisitionEvidence = await loadFirstPartyAttributionEvidenceForPenny(current, {
      tenantId: '10',
      queryWalkthroughAttribution: mockQueryRows({ [CLIENT_10]: [productionFixture({ id: 777 })] }),
      window: { start: '2026-09-01', end: '2026-09-14', days: 14 },
    });

    const input = buildExecutionInput({
      mission: current,
      specialist: SPECIALISTS.PENNY,
      contributions: [],
      acquisitionEvidence,
      acquisitionApproach: { selectedApproach: ACQUISITION_APPROACHES.PAID },
    });

    const firstParty = input.specialistInput.evidence.find(
      (row) => row.kind === 'first_party_attributed_lead'
    );
    assert.ok(firstParty);
    assert.equal(firstParty.evidenceId, 777);
    assert.equal(firstParty.sourceKind, SOURCE_KIND);
  });

  it('preserves caller-supplied acquisitionEvidence through Penny execution path', async () => {
    const supplied = [{
      label: 'Prior scout evidence retained',
      source: 'scout_max_evidence',
      confidence: 0.7,
    }];

    const merged = await loadFirstPartyAttributionEvidenceForPenny(engine.get(mission.id, '10'), {
      tenantId: '10',
      acquisitionEvidence: supplied,
      queryWalkthroughAttribution: mockQueryRows({ [CLIENT_10]: [productionFixture({ id: 888 })] }),
      window: { start: '2026-09-01', end: '2026-09-14', days: 14 },
    });

    assert.ok(merged.some((row) => row.label === 'Prior scout evidence retained'));
    assert.ok(merged.some((row) => row.evidenceId === 888));
  });

  it('keeps platformEvidence unchanged and does not increment platformConversions', async () => {
    const platformEvidence = [{
      spec: 'SPEC-253',
      platform: PLATFORM.CHATGPT_ADS,
      channel: 'ChatGPT Ads',
      availability: AVAILABILITY.AVAILABLE,
      campaigns: [{
        externalCampaignId: 'test_campaign',
        platformConversions: 0,
        spend: 50,
        impressions: 1000,
        clicks: 20,
      }],
      aggregates: {
        platformConversions: 0,
        spend: 50,
        impressions: 1000,
        clicks: 20,
      },
      provenance: { sourceKind: 'PLATFORM_API', readOnly: true },
    }];

    const acquisitionEvidence = [
      mapAgentActionToEvidence(productionFixture({ id: 999 })),
    ];

    const input = buildExecutionInput({
      mission: engine.get(mission.id, '10'),
      specialist: SPECIALISTS.PENNY,
      contributions: [],
      platformEvidence,
      acquisitionEvidence,
      acquisitionApproach: { selectedApproach: ACQUISITION_APPROACHES.PAID },
      availableBudget: { amount: 600, currency: 'USD' },
      conversionReadiness: { ready: true },
      measurementReadiness: { ready: true },
    });

    const pennyResult = await runPennyPaidAcquisition(input);
    const recommendation = pennyResult.contributions.paidAcquisitionRecommendation;

    assert.equal(recommendation.platformEvidence[0].aggregates.platformConversions, 0);
    assert.ok(input.specialistInput.evidence.some((row) => row.kind === 'first_party_attributed_lead'));
    assert.ok(!recommendation.platformEvidence.some((row) => row.aggregates?.platformConversions > 0));
  });

  it('surfaces retrieval-unavailable blocker instead of silent zero on pool failure', async () => {
    const merged = await loadFirstPartyAttributionEvidenceForPenny(engine.get(mission.id, '10'), {
      tenantId: '10',
      pool: {
        query: async () => {
          throw new Error('pool down');
        },
      },
    });

    assert.ok(merged.some((row) => row.kind === 'first_party_attribution_retrieval_unavailable'));
    assert.ok(merged.every((row) => row.sourceKind !== 'PLATFORM_API'));
  });

  it('resolvePlatformEvidenceForPenny remains independent of first-party retrieval', async () => {
    const platformOnly = await resolvePlatformEvidenceForPenny(engine.get(mission.id, '10'), {
      tenantId: '10',
      skipPlatformEvidenceCollection: true,
      platformEvidence: [{
        channel: 'ChatGPT Ads',
        availability: AVAILABILITY.AVAILABLE,
        aggregates: { platformConversions: 0 },
        provenance: { sourceKind: 'PLATFORM_API' },
      }],
    });

    assert.equal(platformOnly.length, 1);
    assert.equal(platformOnly[0].provenance.sourceKind, 'PLATFORM_API');
  });
});
