'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createBuiltinRegistry,
  createCapabilityRunner,
  createCapabilityRegistry,
  BUILTIN_IDS,
  CAPABILITY_RESULT_STATUS,
  PROGRESS_KINDS,
  buildCapabilityContext,
  discovery,
  ranking,
} = require('..');

function testRegistry() {
  return createBuiltinRegistry({ discovery: { useFixture: true } });
}

describe('SPEC-023 CapabilityRegistry', () => {
  it('registers and lists built-ins', () => {
    const registry = testRegistry();
    const list = registry.list();
    assert.equal(list.length, 5);
    assert.ok(registry.get(BUILTIN_IDS.PROSPECT_DISCOVERY));
    assert.ok(registry.get(BUILTIN_IDS.CAMPAIGN_BUILDER));
  });

  it('discovers by outcome tags', () => {
    const registry = testRegistry();
    const found = registry.discover(['prospects_discovered']);
    assert.equal(found.length, 1);
    assert.equal(found[0].id, BUILTIN_IDS.PROSPECT_DISCOVERY);
  });

  it('rejects duplicate registration', () => {
    const registry = testRegistry();
    assert.throws(() => {
      registry.register(registry.get(BUILTIN_IDS.PROSPECT_DISCOVERY));
    }, /already registered/);
  });
});

describe('SPEC-023 CapabilityRunner', () => {
  it('executes via registry and emits progress', async () => {
    const registry = testRegistry();
    const events = [];
    const runner = createCapabilityRunner({
      registry,
      onProgress: (e) => events.push(e),
    });

    const out = await runner.run({
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      context: buildCapabilityContext({
        missionId: 'm1',
        tenantId: '10',
        clientId: 10,
        objective: 'Find prospects',
        constraints: { targetCount: 50 },
      }),
    });

    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    assert.ok(out.result.outputs.prospectCount >= 1);
    assert.ok(events.some((e) => e.kind === PROGRESS_KINDS.QUEUED));
    assert.ok(events.some((e) => e.kind === PROGRESS_KINDS.RUNNING));
    assert.ok(events.some((e) => e.kind === PROGRESS_KINDS.PROGRESS));
    assert.ok(events.some((e) => e.kind === PROGRESS_KINDS.COMPLETED));
    assert.equal(out.name, 'Discovering Prospects');
  });

  it('fails unknown capability without agent branching', async () => {
    const runner = createCapabilityRunner({
      registry: createCapabilityRegistry(),
    });
    await assert.rejects(
      () => runner.run({ capabilityId: 'scout', context: {} }),
      /Unknown capability/
    );
  });
});

describe('SPEC-023 stub composition', () => {
  it('campaign builder produces review-gated draft', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const discoveryResult = await runner.run({
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      context: {
        missionId: 'm1',
        tenantId: '10',
        clientId: 10,
        objective: 'Build Campaign 001',
        constraints: { targetCount: 50 },
      },
    });
    const campaign = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_BUILDER,
      context: {
        missionId: 'm1',
        tenantId: '10',
        objective: 'Build Campaign 001 for Anchor Cleaning',
        inputs: { prospects: discoveryResult.result.outputs.prospects },
      },
    });
    assert.equal(campaign.result.outputs.campaign.name, 'Campaign 001');
    assert.equal(campaign.result.outputs.campaign.status, 'review_required');
    assert.equal(campaign.result.outputs.outboundBlocked, true);
  });
});

describe('SPEC-024 Discovery Profiles', () => {
  it('seeds Commercial Cleaning - Manchester profile', () => {
    const store = discovery.createDiscoveryProfileStore();
    const profile = store.get('dp_commercial_cleaning_manchester');
    assert.ok(profile);
    assert.equal(profile.name, 'Commercial Cleaning - Manchester');
    assert.equal(profile.version, '1.0');
    assert.ok(profile.industryTargets.includes('Law Firms'));
    assert.equal(profile.minimumConfidence, 0.75);
  });

  it('versions immutably and requires approval', () => {
    const store = discovery.createDiscoveryProfileStore();
    const v11 = store.createVersion(
      'dp_commercial_cleaning_manchester',
      { minimumConfidence: 0.8 },
      { autoActivate: false }
    );
    assert.equal(v11.version, '1.1');
    assert.equal(v11.status, 'pending_review');
    const active = store.get('dp_commercial_cleaning_manchester');
    assert.equal(active.version, '1.0');
    assert.equal(active.minimumConfidence, 0.75);
    const approved = store.approveVersion(
      'dp_commercial_cleaning_manchester',
      '1.1'
    );
    assert.equal(approved.status, 'active');
    assert.equal(approved.minimumConfidence, 0.8);
    const after = store.get('dp_commercial_cleaning_manchester');
    assert.equal(after.version, '1.1');
    const historical = store.get('dp_commercial_cleaning_manchester', '1.0');
    assert.equal(historical.minimumConfidence, 0.75);
    assert.equal(historical.status, 'superseded');
  });

  it('selector picks Manchester profile for Anchor Cleaning campaign', () => {
    const selector = discovery.createProfileSelector();
    const result = selector.select({
      objective: 'Build Campaign 001 for Anchor Cleaning',
      clientId: 10,
      tenantId: '10',
    });
    assert.equal(result.profile.name, 'Commercial Cleaning - Manchester');
    assert.match(result.message, /Commercial Cleaning/);
  });
});

describe('SPEC-024 Prospect Discovery', () => {
  it('returns verified prospects with ranking signals and evidence', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      context: buildCapabilityContext({
        missionId: 'm_disc',
        tenantId: '10',
        clientId: 10,
        objective: 'Build Campaign 001 for Anchor Cleaning',
        constraints: { targetCount: 50 },
      }),
    });

    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    const { prospects, discoveryProfile, reviewPackage, summary } =
      out.result.outputs;
    assert.ok(prospects.length >= 1);
    assert.equal(discoveryProfile.name, 'Commercial Cleaning - Manchester');
    assert.ok(summary.verified >= 1);
    assert.ok(summary.rejected >= 1); // residential + closed fixtures

    const top = prospects[0];
    assert.ok(top.companyName);
    assert.ok(top.website);
    assert.ok(Array.isArray(top.rankingSignals));
    assert.ok(top.rankingSignals.some((s) => s.profileName));
    assert.ok(top.evidence.length >= 1);
    assert.ok(top.confidence >= 0.5);
    assert.ok(reviewPackage.rankedList.length === prospects.length);
    assert.ok(reviewPackage.discoveryNotes.length >= 1);
    assert.ok(reviewPackage.operatorActions.includes('approve'));
  });

  it('dedupes by website and excludes residential', async () => {
    const provider = discovery.createFixtureProvider([
      {
        companyName: 'Alpha Law',
        website: 'alphalaw.com',
        industry: 'Law Firms',
        address: '1 Elm St, Manchester, NH',
        phone: '603-555-0001',
        source: 'fixture',
        snippet: 'law firm attorney',
      },
      {
        companyName: 'Alpha Law LLC',
        website: 'www.alphalaw.com',
        industry: 'Law Firms',
        address: '1 Elm St, Manchester, NH',
        phone: '603-555-0001',
        source: 'fixture',
        snippet: 'law firm',
      },
      {
        companyName: 'Home Maid Cleaners',
        website: 'homemaid.com',
        industry: 'Residential',
        address: '2 Oak St, Manchester, NH',
        snippet: 'residential house cleaning maid service',
        source: 'fixture',
      },
    ]);
    const cap = discovery.createProspectDiscoveryCapability({
      searchProvider: provider,
    });
    const registry = createCapabilityRegistry();
    registry.register(cap);
    const runner = createCapabilityRunner({ registry });
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      context: {
        missionId: 'm2',
        tenantId: '10',
        clientId: 10,
        objective: 'Discover law firm prospects in Manchester',
        constraints: {
          discoveryProfileId: 'dp_commercial_cleaning_manchester',
          targetCount: 10,
        },
      },
    });
    const names = out.result.outputs.prospects.map((p) => p.companyName);
    assert.equal(names.filter((n) => /Alpha Law/i.test(n)).length, 1);
    assert.ok(!names.some((n) => /Maid/i.test(n)));
  });

  it('emits Searching → Filtering → Verifying → Ranking → Completed', async () => {
    const registry = testRegistry();
    const stages = [];
    const runner = createCapabilityRunner({
      registry,
      onProgress: (e) => {
        if (e.kind === PROGRESS_KINDS.PROGRESS && e.payload?.stage) {
          stages.push(e.payload.stage);
        }
      },
    });
    await runner.run({
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      context: {
        missionId: 'm3',
        tenantId: '10',
        clientId: 10,
        objective: 'Build Campaign 001 for Anchor Cleaning',
        constraints: { targetCount: 10 },
      },
    });
    assert.deepEqual(stages, [
      discovery.DISCOVERY_PROGRESS_STAGES.SEARCHING,
      discovery.DISCOVERY_PROGRESS_STAGES.FILTERING,
      discovery.DISCOVERY_PROGRESS_STAGES.VERIFYING,
      discovery.DISCOVERY_PROGRESS_STAGES.RANKING,
      discovery.DISCOVERY_PROGRESS_STAGES.COMPLETED,
    ]);
  });
});

describe('SPEC-026 Opportunity Ranking', () => {
  it('scores each prospect with explainable factors and a brief', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });

    const discoveryOut = await runner.run({
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      context: buildCapabilityContext({
        missionId: 'm_rank',
        tenantId: '10',
        clientId: 10,
        objective: 'Build Campaign 001 for Anchor Cleaning',
        constraints: { targetCount: 10 },
      }),
    });

    const enrichOut = await runner.run({
      capabilityId: BUILTIN_IDS.COMPANY_ENRICHMENT,
      context: {
        missionId: 'm_rank',
        tenantId: '10',
        clientId: 10,
        objective: 'Build Campaign 001 for Anchor Cleaning',
        inputs: { prospects: discoveryOut.result.outputs.prospects },
      },
    });

    const rankOut = await runner.run({
      capabilityId: BUILTIN_IDS.OPPORTUNITY_RANKING,
      context: {
        missionId: 'm_rank',
        tenantId: '10',
        clientId: 10,
        objective: 'Build Campaign 001 for Anchor Cleaning',
        inputs: {
          prospects: enrichOut.result.outputs.prospects,
          discoveryProfile: discoveryOut.result.outputs.discoveryProfile,
        },
      },
    });

    assert.equal(rankOut.result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    assert.equal(rankOut.name, 'Ranking Opportunities');
    const { prospects, reviewPackage, summary, feedCampaignBuilder } =
      rankOut.result.outputs;
    assert.ok(prospects.length >= 1);
    assert.equal(feedCampaignBuilder, true);
    assert.ok(reviewPackage.operatorActions.includes('continue_to_campaign_builder'));
    assert.ok(reviewPackage.operatorActions.includes('approve'));
    assert.ok(summary.high + summary.medium + summary.low === prospects.length);

    const top = prospects[0];
    assert.ok(top.rank === 1);
    assert.ok(top.overallScore >= 0 && top.overallScore <= 100);
    assert.ok(['high', 'medium', 'low'].includes(top.priority));
    assert.ok(Array.isArray(top.factorScores));
    assert.equal(top.factorScores.length, 8);
    for (const f of top.factorScores) {
      assert.ok(f.factor);
      assert.ok(typeof f.detail === 'string' && f.detail.length > 0);
      assert.ok(f.score >= 0 && f.score <= f.max);
    }
    assert.ok(top.opportunityBrief.whyFit);
    assert.ok(top.opportunityBrief.bestOutreachAngle);
    assert.equal(top.opportunityBrief.talkingPoints.length, 3);
    assert.ok(top.opportunityBrief.potentialObjections.length >= 1);
    assert.ok(top.recommendedNextAction);
    assert.ok(Array.isArray(top.topReasons));
    assert.ok(Array.isArray(top.risks));
  });

  it('does not invent buying signals without evidence', () => {
    const scored = ranking.scoreOpportunity(
      {
        id: 'thin_1',
        companyName: 'Thin Co',
        confidence: 0.8,
        rankingSignals: [],
      },
      { profile: null, knowledge: {}, historicalOutcomes: [] }
    );
    const buying = scored.factorScores.find((f) => f.factor === 'buying_signals');
    assert.equal(buying.score, 0);
    assert.match(buying.detail, /scored 0/i);
    assert.ok(scored.risks.some((r) => /buying signals/i.test(r)));
  });

  it('raises score when enrichment evidence is present', () => {
    const thin = ranking.scoreOpportunity({
      id: 'a',
      companyName: 'Alpha Law',
      industry: 'Law Firms',
      address: '1 Elm St, Manchester, NH',
      website: 'https://alphalaw.com',
      confidence: 0.85,
      rankingSignals: [
        {
          signal: 'target_industry',
          weight: 0.9,
          matched: true,
          detail: 'Matched Profile Signal: Commercial Cleaning - Manchester — Law Firms',
        },
      ],
    });
    const rich = ranking.scoreOpportunity(
      {
        id: 'a',
        companyName: 'Alpha Law',
        industry: 'Law Firms',
        address: '1 Elm St, Manchester, NH',
        website: 'https://alphalaw.com',
        email: 'owner@alphalaw.com',
        phone: '603-555-0100',
        jobTitle: 'Managing Partner',
        employeeCount: 12,
        hiringActivity: true,
        enriched: true,
        confidence: 0.85,
        rankingSignals: [
          {
            signal: 'target_industry',
            weight: 0.9,
            matched: true,
            detail: 'Matched Profile Signal: Commercial Cleaning - Manchester — Law Firms',
          },
        ],
      },
      {
        profile: {
          name: 'Commercial Cleaning - Manchester',
          geography: {
            label: 'Manchester NH',
            cities: ['Manchester', 'Bedford'],
            state: 'NH',
          },
          industryTargets: ['Law Firms'],
        },
        historicalOutcomes: [
          { successful: true, vertical: 'law firms', id: 'out_1' },
        ],
      }
    );
    assert.ok(rich.overallScore > thin.overallScore);
    assert.ok(rich.overallScore >= 70);
    assert.equal(ranking.priorityFromScore(rich.overallScore), 'high');
  });

  it('emits Scoring → Briefing → Prioritizing → Completed', async () => {
    const registry = testRegistry();
    const stages = [];
    const runner = createCapabilityRunner({
      registry,
      onProgress: (e) => {
        if (e.kind === PROGRESS_KINDS.PROGRESS && e.payload?.stage) {
          stages.push(e.payload.stage);
        }
      },
    });
    await runner.run({
      capabilityId: BUILTIN_IDS.OPPORTUNITY_RANKING,
      context: {
        missionId: 'm_rank_prog',
        tenantId: '10',
        inputs: {
          prospects: [
            {
              id: 'p1',
              companyName: 'Test Law',
              industry: 'Law Firms',
              address: 'Manchester, NH',
              website: 'https://testlaw.com',
              email: 'a@testlaw.com',
              confidence: 0.8,
              enriched: true,
            },
          ],
          discoveryProfile: {
            name: 'Commercial Cleaning - Manchester',
            geography: { cities: ['Manchester'], state: 'NH', label: 'Manchester NH' },
            industryTargets: ['Law Firms'],
          },
        },
      },
    });
    assert.deepEqual(stages, [
      ranking.RANKING_PROGRESS_STAGES.SCORING,
      ranking.RANKING_PROGRESS_STAGES.BRIEFING,
      ranking.RANKING_PROGRESS_STAGES.PRIORITIZING,
      ranking.RANKING_PROGRESS_STAGES.COMPLETED,
    ]);
  });

  it('feeds Campaign Builder with ranked prospects', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const ranked = await runner.run({
      capabilityId: BUILTIN_IDS.OPPORTUNITY_RANKING,
      context: {
        missionId: 'm_cb',
        tenantId: '10',
        objective: 'Build Campaign 001 for Anchor Cleaning',
        inputs: {
          prospects: [
            {
              id: 'p1',
              companyName: 'Bedford Dental',
              industry: 'Dental Practices',
              address: 'Bedford, NH',
              email: 'office@bedforddental.com',
              confidence: 0.9,
              enriched: true,
            },
          ],
        },
      },
    });
    const campaign = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_BUILDER,
      context: {
        missionId: 'm_cb',
        tenantId: '10',
        objective: 'Build Campaign 001 for Anchor Cleaning',
        inputs: { prospects: ranked.result.outputs.prospects },
      },
    });
    assert.equal(campaign.result.outputs.campaign.status, 'review_required');
    assert.equal(campaign.result.outputs.campaign.prospectCount, 1);
    assert.ok(campaign.result.outputs.campaign.prospects[0].overallScore != null);
  });
});
