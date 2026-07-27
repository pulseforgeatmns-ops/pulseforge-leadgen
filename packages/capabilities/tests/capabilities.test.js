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
  playbook,
} = require('..');

function testRegistry() {
  return createBuiltinRegistry({ discovery: { useFixture: true } });
}

describe('SPEC-023 CapabilityRegistry', () => {
  it('registers and lists built-ins', () => {
    const registry = testRegistry();
    const list = registry.list();
    assert.equal(list.length, 7);
    assert.ok(registry.get(BUILTIN_IDS.PROSPECT_DISCOVERY));
    assert.ok(registry.get(BUILTIN_IDS.CAMPAIGN_BUILDER));
    assert.ok(registry.get(BUILTIN_IDS.PROPOSAL_GENERATOR));
    assert.ok(registry.get(BUILTIN_IDS.MAIL_PACKAGE_GENERATOR));
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

describe('SPEC-027B Proposal Generator', () => {
  const asCleaningSummary = {
    companyName: 'AS Cleaning Co.',
    contactName: 'Alex',
    industry: 'Commercial Cleaning',
    geography: 'Greater Toronto Area',
    companyStage: 'Four months in business with a first commercial client',
    currentClients: ['First commercial client'],
    currentProcess: 'Manual follow-up today',
    icp: ['Medical Offices', 'Dental Practices', 'Property Managers'],
    challenges: ['No predictable pipeline', 'Manual follow-up'],
    goals: ['Commercial growth focus', 'Desire to hire subcontractors'],
    growthVision: 'Hire subcontractors against a growing commercial book',
  };

  it('generates a complete personalized proposal from discovery', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const stages = [];
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.PROPOSAL_GENERATOR,
      context: {
        missionId: 'm_prop',
        tenantId: '1',
        objective: 'Generate proposal for AS Cleaning Co.',
        inputs: {
          discoverySummary: asCleaningSummary,
          discoveryProfile: {
            id: 'dp_gta',
            name: 'Commercial Cleaning — Greater Toronto Area',
            industryTargets: [
              'Medical Offices',
              'Dental Practices',
              'Property Managers',
            ],
            geography: { label: 'Greater Toronto Area' },
          },
          pricingPackageId: 'founding_partner',
        },
      },
      onProgress: (e) => {
        if (e.stage) stages.push(e.stage);
      },
    });

    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    assert.equal(out.result.outputs.document.sections.length, 11);
    assert.equal(out.result.outputs.document.preparedFor, 'AS Cleaning Co.');
    assert.ok(out.result.outputs.html.includes('AS Cleaning Co.'));
    assert.ok(!/\[insert|TODO|TBD|lorem ipsum/i.test(out.result.outputs.html));
    assert.ok(out.result.outputs.reviewPackage.operatorActions.includes('approve'));
    assert.ok(out.result.outputs.proposal.version >= 1);
    assert.equal(out.result.outputs.document.pricing.id, 'founding_partner');
    assert.ok(out.result.outputs.reviewPackage.personalization.ok);
    assert.ok(
      out.result.outputs.document.sections.every(
        (s) => Array.isArray(s.evidenceRefs) && s.evidenceRefs.length > 0
      )
    );
  });

  it('fails interchangeability: name-swap alone is not enough (ADR-014)', () => {
    const { composeProposal, buildDiscoverySummary, assertPersonalized } =
      require('../proposal');
    const rich = buildDiscoverySummary(asCleaningSummary);
    const doc = composeProposal(rich, {
      profile: {
        name: 'Commercial Cleaning — Greater Toronto Area',
        industryTargets: rich.icp,
      },
    });
    const swapped = buildDiscoverySummary({
      companyName: 'Other Cleaning LLC',
    });
    const check = assertPersonalized(doc, swapped);
    assert.equal(check.ok, false);
    assert.ok(check.reasons.includes('company_name_not_referenced'));
  });

  it('states uncertainty instead of inventing markets', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.PROPOSAL_GENERATOR,
      context: {
        missionId: 'm_thin',
        tenantId: '1',
        objective: 'Generate proposal for Thin Co.',
        inputs: {
          discoverySummary: {
            companyName: 'Thin Co.',
            companyStage: 'Early stage',
            geography: 'Manchester NH',
          },
        },
      },
    });
    const strategy = out.result.outputs.document.sections.find(
      (s) => s.id === 'recommended_strategy'
    );
    assert.ok(strategy.uncertain);
    assert.match(strategy.body, /will not invent|not confirmed/i);
  });
});

describe('SPEC-028 Client Playbooks', () => {
  it('seeds AS Cleaning Co and Anchor playbooks', () => {
    const store = playbook.createClientPlaybookStore();
    const as = store.get('pb_as_cleaning_co');
    assert.ok(as);
    assert.equal(as.version, '1.0');
    assert.equal(as.brandVoice, 'relationship_first');
    assert.ok(as.valuePropositions.includes('Owner-operated quality'));
    assert.ok(as.preferredChannels[0] === 'direct_mail');
    assert.equal(as.outreachSequence.length, 5);
    assert.equal(as.outreachSequence[0].day, 1);

    const anchor = store.getForClient(10);
    assert.ok(anchor);
    assert.equal(anchor.id, 'pb_anchor_cleaning');
    assert.match(anchor.idealCustomer.geographicCoverage, /Manchester/);
  });

  it('versions immutably and requires approval', () => {
    const store = playbook.createClientPlaybookStore();
    const v11 = store.createVersion(
      'pb_as_cleaning_co',
      { notes: 'Updated Tuesday morning call preference' },
      { autoActivate: false }
    );
    assert.equal(v11.version, '1.1');
    assert.equal(v11.status, 'pending_review');
    assert.equal(store.get('pb_as_cleaning_co').version, '1.0');
    const approved = store.approveVersion('pb_as_cleaning_co', '1.1');
    assert.equal(approved.status, 'active');
    assert.equal(store.get('pb_as_cleaning_co').version, '1.1');
    assert.equal(store.get('pb_as_cleaning_co', '1.0').status, 'superseded');
  });

  it('Campaign Builder uses playbook channels, sequence, and offers', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const pb = playbook.createClientPlaybookStore().get('pb_anchor_cleaning');
    const campaign = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_BUILDER,
      context: {
        missionId: 'm_pb',
        tenantId: '10',
        clientId: 10,
        objective: 'Build Campaign 001 for Anchor Cleaning',
        constraints: { clientPlaybook: pb },
        inputs: {
          prospects: [
            {
              id: 'p1',
              companyName: 'Bedford Law',
              industry: 'Law Firms',
              confidence: 0.9,
            },
            {
              id: 'p2',
              companyName: 'Downtown Diner',
              industry: 'Restaurants',
              confidence: 0.8,
            },
          ],
        },
      },
    });
    const out = campaign.result.outputs.campaign;
    assert.ok(out.playbook);
    assert.equal(out.playbook.playbookId, 'pb_anchor_cleaning');
    assert.deepEqual(out.preferredChannels.slice(0, 2), ['Direct Mail', 'Phone']);
    assert.equal(out.outreachSequence[0].day, 1);
    assert.ok(out.offers.includes('Free walkthrough'));
    assert.equal(out.prospectCount, 1);
    assert.equal(out.excludedProspects.length, 1);
    assert.match(out.excludedProspects[0].exclusionReason, /restaurant/i);
    assert.ok(out.mailMerge[0].personalizationSentence);
    assert.doesNotMatch(
      out.mailMerge[0].openingHook || '',
      /Quick question about your current vendor setup/
    );
    assert.equal(campaign.result.outputs.clientPlaybookId, 'pb_anchor_cleaning');
  });

  it('Proposal Generator consumes playbook voice, value props, and offers', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const pb = playbook.createClientPlaybookStore().get('pb_as_cleaning_co');
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.PROPOSAL_GENERATOR,
      context: {
        missionId: 'm_prop_pb',
        tenantId: '1',
        objective: 'Generate proposal for AS Cleaning Co.',
        constraints: { clientPlaybook: pb },
        inputs: {
          discoverySummary: {
            companyName: 'AS Cleaning Co.',
            contactName: 'Alex',
            industry: 'Commercial Cleaning',
            geography: 'Greater Toronto Area',
            companyStage: 'Four months in business with a first commercial client',
            goals: ['Commercial growth focus'],
            challenges: ['No predictable pipeline'],
            icp: ['Medical Offices', 'Dental Practices'],
          },
        },
      },
    });
    assert.equal(out.result.outputs.clientPlaybookId, 'pb_as_cleaning_co');
    const doc = out.result.outputs.document;
    assert.equal(doc.playbookId, 'pb_as_cleaning_co');
    const why = doc.sections.find((s) => s.id === 'why_pulseforge');
    assert.match(why.body, /Owner-operated quality|Reliable recurring service/i);
    assert.ok(why.bullets.some((b) => /Relationship-first/i.test(b)));
    const strategy = doc.sections.find((s) => s.id === 'recommended_strategy');
    assert.match(strategy.body, /Client Playbook/i);
    assert.ok(strategy.bullets.some((b) => /Free walkthrough/i.test(b)));
    assert.ok(strategy.bullets.some((b) => /Walkthroughs booked/i.test(b)));
    const handle = doc.sections.find((s) => s.id === 'what_we_handle');
    assert.ok(handle.bullets.some((b) => /Day 1/i.test(b)));
  });

  it('PlaybookSelector pins by client and objective hint', () => {
    const selector = playbook.createPlaybookSelector();
    const byClient = selector.select({
      objective: 'Build Campaign 001',
      clientId: 10,
    });
    assert.equal(byClient.playbook.id, 'pb_anchor_cleaning');
    assert.equal(byClient.selection, 'client');

    const byHint = selector.select({
      objective: 'Generate proposal for AS Cleaning Co.',
      clientId: 1,
    });
    assert.equal(byHint.playbook.id, 'pb_as_cleaning_co');
  });
});

describe('SPEC-033 Mail Package Generator', () => {
  const mail = require('../mail');

  const readyProspect = {
    id: 'p1',
    companyName: 'Bedford Law',
    industry: 'Law Firms',
    address: '5 Commerce Park N, Bedford, NH 03110',
    contactName: 'Jordan Hale',
    confidence: 0.9,
    overallScore: 0.86,
    opportunityBrief: {
      whyFit: 'Single-tenant law office in Manchester beachhead',
      bestOutreachAngle: 'Owner-attentive recurring service',
      talkingPoints: ['Local owner accountability', 'Consistent standards'],
    },
  };

  const thinProspect = {
    id: 'p2',
    companyName: 'No Address LLC',
    industry: 'Accounting Practices',
    confidence: 0.4,
  };

  it('generates complete packages, CSVs, and campaign HTML for approved prospects', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const pb = playbook.createClientPlaybookStore().get('pb_anchor_cleaning');
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
      context: {
        missionId: 'm_mail',
        tenantId: '10',
        clientId: 10,
        objective: 'Generate mail packages for Campaign 001',
        constraints: { clientPlaybook: pb },
        inputs: {
          campaign: {
            name: 'Campaign 001',
            status: 'approved',
            prospects: [readyProspect, thinProspect],
            mailMerge: [
              {
                companyName: 'Bedford Law',
                personalizationSentence:
                  'Reached out because Bedford Law looks like a strong fit for local owner accountability in Law Firms.',
                openingHook: 'Would a free walkthrough be useful?',
                recommendedOffer: 'Free walkthrough',
              },
            ],
          },
        },
      },
    });

    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    const packages = out.result.outputs.packages;
    assert.equal(packages.length, 2);

    const ready = packages.find((p) => p.prospectId === 'p1');
    const needs = packages.find((p) => p.prospectId === 'p2');
    assert.equal(ready.status, mail.PACKAGE_STATUS.READY_TO_PRINT);
    assert.equal(needs.status, mail.PACKAGE_STATUS.NEEDS_REVIEW);
    assert.match(ready.letter.body, /Bedford Law/);
    assert.match(ready.letter.body, /Jordan Hale/);
    assert.ok(ready.envelope.mailingAddress.includes('Bedford'));
    assert.ok(ready.insertChecklist.some((i) => i.label === 'Letter'));
    assert.ok(ready.insertChecklist.some((i) => i.label === 'Microfiber Cloth'));

    const summary = out.result.outputs.campaignSummary;
    assert.equal(summary.prospects, 2);
    assert.equal(summary.readyToPrint, 1);
    assert.equal(summary.needsReview, 1);
    assert.equal(summary.missingAddresses, 1);
    assert.ok(summary.estimatedPrintTimeSec > 0);
    assert.ok(summary.estimatedAssemblyTimeSec > 0);

    assert.ok(out.result.outputs.campaignHtml.includes('Campaign 001'));
    assert.ok(out.result.outputs.mailMergeCsv.includes('recipient_name'));
    assert.ok(out.result.outputs.mailMergeCsv.includes('Bedford Law'));
    assert.ok(out.result.outputs.addressLabelCsv.includes('print_ready'));
    assert.ok(out.result.outputs.campaignDocxHtml.includes('Bedford Law'));
    assert.ok(
      out.result.outputs.reviewPackage.operatorActions.includes('approve_package')
    );
    assert.ok(
      out.result.artifacts.some((a) => a.type === 'mail_merge_csv')
    );
    assert.ok(
      out.result.artifacts.some((a) => a.type === 'address_label_csv')
    );
    assert.ok(out.result.artifacts.some((a) => a.type === 'campaign_pdf'));
    assert.equal(out.result.outputs.printBlocked, true);
  });

  it('blocks Ready to Print when mailing address is missing', () => {
    const result = mail.validateProspectForMail({
      companyName: 'Thin Co',
      contactName: 'Alex',
      confidence: 0.95,
    });
    assert.equal(result.ok, false);
    assert.equal(result.status, mail.PACKAGE_STATUS.NEEDS_REVIEW);
    assert.ok(result.reasons.includes('missing_mailing_address'));
  });

  it('allows company fallback for recipient', () => {
    const result = mail.validateProspectForMail({
      companyName: 'Solo Office LLC',
      address: '1 Main St, Manchester, NH 03101',
      confidence: 0.9,
    });
    assert.equal(result.ok, true);
    assert.equal(result.recipientName, 'Solo Office LLC');
    assert.equal(result.usedCompanyFallback, true);
  });

  it('preserves revision history on regenerate', async () => {
    const store = mail.createInMemoryMailPackageStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      mail: { mailPackageStore: store },
    });
    const runner = createCapabilityRunner({ registry });
    const pb = playbook.createClientPlaybookStore().get('pb_anchor_cleaning');
    const ctx = {
      missionId: 'm_mail_rev',
      tenantId: '10',
      clientId: 10,
      objective: 'Generate mail packages for Campaign 001',
      constraints: { clientPlaybook: pb },
      inputs: {
        campaignId: 'camp_001',
        campaign: {
          name: 'Campaign 001',
          prospects: [readyProspect],
        },
      },
    };

    const first = await runner.run({
      capabilityId: BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
      context: ctx,
    });
    const second = await runner.run({
      capabilityId: BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
      context: ctx,
    });

    assert.equal(first.result.outputs.mailBatchRevision, 1);
    assert.equal(second.result.outputs.mailBatchRevision, 2);
    const history = store.listForCampaign('camp_001');
    assert.equal(history.length, 2);
    assert.ok(store.get(first.result.outputs.mailBatchId));
    assert.ok(store.getLatest('camp_001').revision === 2);
  });

  it('skips prospect and marks address invalid via overrides', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const pb = playbook.createClientPlaybookStore().get('pb_anchor_cleaning');
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
      context: {
        missionId: 'm_mail_skip',
        tenantId: '10',
        constraints: { clientPlaybook: pb },
        inputs: {
          prospects: [
            readyProspect,
            {
              id: 'p3',
              companyName: 'Hooksett Dental',
              address: '20 Alice Ave, Hooksett, NH 03106',
              contactName: 'Sam',
              confidence: 0.88,
            },
          ],
          packageOverrides: {
            p1: { skipped: true },
            p3: { addressInvalid: true },
          },
        },
      },
    });
    const packages = out.result.outputs.packages;
    assert.equal(
      packages.find((p) => p.prospectId === 'p1').status,
      mail.PACKAGE_STATUS.SKIPPED
    );
    assert.equal(
      packages.find((p) => p.prospectId === 'p3').status,
      mail.PACKAGE_STATUS.NEEDS_REVIEW
    );
    assert.equal(out.result.outputs.campaignSummary.prospects, 1);
    assert.equal(out.result.outputs.campaignSummary.readyToPrint, 0);
  });
});
