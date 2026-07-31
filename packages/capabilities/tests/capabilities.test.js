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
    assert.equal(list.length, 15);
    assert.ok(registry.get(BUILTIN_IDS.PROSPECT_DISCOVERY));
    assert.ok(registry.get(BUILTIN_IDS.BUSINESS_INTELLIGENCE));
    assert.ok(registry.get(BUILTIN_IDS.CAMPAIGN_BUILDER));
    assert.ok(registry.get(BUILTIN_IDS.PROPOSAL_GENERATOR));
    assert.ok(registry.get(BUILTIN_IDS.MAIL_PACKAGE_GENERATOR));
    assert.ok(registry.get(BUILTIN_IDS.CAMPAIGN_REVIEW));
    assert.ok(registry.get(BUILTIN_IDS.DIRECT_MAIL_EXECUTION));
    assert.ok(registry.get(BUILTIN_IDS.OUTCOME_INTELLIGENCE));
    assert.ok(registry.get(BUILTIN_IDS.OPERATOR_INBOX));
    assert.ok(registry.get(BUILTIN_IDS.DISCOVERY_DIAGNOSTICS));
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
      /Capability not registered|Recommended Action/
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

describe('SPEC-034 Campaign Review Workspace', () => {
  const campaignReview = require('../campaignReview');

  const readyPkg = {
    id: 'pkg_p1',
    prospectId: 'p1',
    status: 'ready_to_print',
    confidence: 0.9,
    letter: {
      recipientName: 'Jordan Hale',
      companyName: 'Bedford Law',
      personalizedOpening: 'Noticed Bedford Law expanding.',
      valueProposition: 'Local owner accountability',
      cta: 'Free walkthrough',
      signature: 'Anchor Cleaning',
      body: 'Dear Jordan Hale,\n\nBedford Law looks like a strong fit.\n',
    },
    envelope: {
      recipientName: 'Jordan Hale',
      companyName: 'Bedford Law',
      mailingAddress: '5 Commerce Park N, Bedford, NH 03110',
      returnAddress: 'Anchor Cleaning, Manchester NH',
    },
    personalizationSummary: {
      whySelected: 'Single-tenant law office',
      personalizationFacts: ['Expanded office', 'Medical practice nearby', 'Recently hiring'],
      letterConfidence: 0.9,
      missingDataWarnings: [],
    },
    insertChecklist: [
      { id: 'letter', label: 'Letter', required: true, included: true },
      { id: 'business_card', label: 'Business Card', required: true, included: true },
    ],
    warnings: [],
  };

  const blockedPkg = {
    id: 'pkg_p2',
    prospectId: 'p2',
    status: 'needs_review',
    confidence: 0.4,
    letter: {
      recipientName: '',
      companyName: 'Thin Co',
      body: 'Hello',
    },
    envelope: {
      recipientName: '',
      companyName: 'Thin Co',
      mailingAddress: '',
      returnAddress: '',
    },
    personalizationSummary: {
      personalizationFacts: [],
      letterConfidence: 0.4,
      missingDataWarnings: ['missing address'],
    },
    insertChecklist: [],
    warnings: ['Missing mailing address'],
  };

  const campaign = {
    id: 'camp_001',
    name: 'Campaign 001',
    status: 'approved',
    prospects: [
      {
        id: 'p1',
        companyName: 'Bedford Law',
        contactName: 'Jordan Hale',
        address: '5 Commerce Park N, Bedford, NH 03110',
        overallScore: 0.86,
        confidence: 0.9,
        opportunityBrief: {
          whyFit: 'Single-tenant law office in Manchester beachhead',
          talkingPoints: ['Local owner accountability'],
        },
      },
      {
        id: 'p2',
        companyName: 'Thin Co',
        overallScore: 0.4,
        confidence: 0.4,
      },
    ],
  };

  it('assembles a single review workspace with queue and summary', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const pb = playbook.createClientPlaybookStore().get('pb_anchor_cleaning');
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
      context: {
        missionId: 'm_review',
        tenantId: '10',
        clientId: 10,
        objective: 'Review Campaign 001',
        constraints: { clientPlaybook: pb },
        inputs: {
          campaign,
          mailBatch: { id: 'mail_1', packages: [readyPkg, blockedPkg] },
          packages: [readyPkg, blockedPkg],
        },
      },
    });

    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    const summary = out.result.outputs.summary;
    assert.equal(summary.campaignName, 'Campaign 001');
    assert.equal(summary.prospectCount, 2);
    assert.ok(summary.blockedCount >= 1);
    assert.ok(
      summary.status === campaignReview.CAMPAIGN_REVIEW_STATUS.IN_REVIEW ||
        summary.status === campaignReview.CAMPAIGN_REVIEW_STATUS.BLOCKED
    );
    assert.equal(out.result.outputs.outboundBlocked, true);
    assert.ok(out.result.outputs.workspace.queue.length === 2);
    assert.ok(
      out.result.outputs.reviewPackage.operatorActions.includes('approve_campaign')
    );
    assert.ok(
      out.result.artifacts.some((a) => a.type === 'campaign_review_workspace')
    );
  });

  it('blocks prospect approval when address/company/recipient/confidence fail', () => {
    const result = campaignReview.validateProspectForApproval({
      company: '',
      recipient: '',
      address: '',
      confidence: 0.2,
    });
    assert.equal(result.ok, false);
    assert.ok(result.errors.includes('missing_address'));
    assert.ok(result.errors.includes('missing_company'));
    assert.ok(result.errors.includes('missing_recipient'));
    assert.ok(result.errors.includes('confidence_below_threshold'));
  });

  it('supports per-prospect approve and blocks campaign Ready to Print until gates pass', async () => {
    const store = campaignReview.createInMemoryCampaignReviewStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      campaignReview: { campaignReviewStore: store },
    });
    const runner = createCapabilityRunner({ registry });
    const pb = playbook.createClientPlaybookStore().get('pb_anchor_cleaning');

    const first = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
      context: {
        missionId: 'm_review_approve',
        tenantId: '10',
        clientId: 10,
        constraints: { clientPlaybook: pb },
        inputs: {
          campaignId: 'camp_001',
          campaign,
          packages: [readyPkg, blockedPkg],
          mailBatch: { id: 'mail_1', packages: [readyPkg, blockedPkg] },
          reviewActions: [
            { type: 'approve', prospectId: 'p1', operator: 'jacob' },
            { type: 'approve_campaign', operator: 'jacob' },
          ],
        },
      },
    });

    assert.equal(first.result.outputs.campaignApproved, false);
    assert.ok(
      first.result.outputs.reviewPackage.campaignApprovalErrors.includes(
        'required_prospects_not_approved'
      )
    );
    const p1 = first.result.outputs.queue.find((r) => r.prospectId === 'p1');
    assert.equal(p1.status, campaignReview.PROSPECT_REVIEW_STATUS.APPROVED);
    assert.ok(
      first.result.outputs.missionDecisions.some((d) => d.action === 'approve')
    );
  });

  it('transitions to Ready to Print after all required prospects approved', async () => {
    const store = campaignReview.createInMemoryCampaignReviewStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      campaignReview: { campaignReviewStore: store },
    });
    const runner = createCapabilityRunner({ registry });
    const pb = playbook.createClientPlaybookStore().get('pb_anchor_cleaning');

    const out = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
      context: {
        missionId: 'm_review_ready',
        tenantId: '10',
        clientId: 10,
        constraints: { clientPlaybook: pb },
        inputs: {
          campaignId: 'camp_ready',
          campaign: {
            ...campaign,
            prospects: [campaign.prospects[0]],
          },
          packages: [readyPkg],
          mailBatch: { id: 'mail_ready', packages: [readyPkg] },
          reviewActions: [
            { type: 'approve', prospectId: 'p1', operator: 'jacob' },
            { type: 'skip', prospectId: 'p2', operator: 'jacob' },
            { type: 'approve_campaign', operator: 'jacob' },
          ],
        },
      },
    });

    // Only p1 in packages — approve + campaign approve
    assert.equal(out.result.outputs.campaignApproved, true);
    assert.equal(
      out.result.outputs.summary.status,
      campaignReview.CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT
    );
    assert.ok(out.result.outputs.executionPackage);
    assert.ok(out.result.outputs.executionPackage.printPackage.html.includes('Bedford Law'));
    assert.ok(out.result.outputs.executionPackage.mailMerge.csv.includes('Jordan Hale'));
    assert.ok(out.result.outputs.executionPackage.addressLabels.csv.includes('Bedford'));
    assert.ok(
      out.result.outputs.missionDecisions.some((d) => d.action === 'approve_campaign')
    );
    assert.ok(
      out.result.artifacts.some((a) => a.type === 'execution_package')
    );
  });

  it('supports bulk approve, inline edit, and revision history', async () => {
    const store = campaignReview.createInMemoryCampaignReviewStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      campaignReview: { campaignReviewStore: store },
    });
    const runner = createCapabilityRunner({ registry });
    const pb = playbook.createClientPlaybookStore().get('pb_anchor_cleaning');
    const ctxBase = {
      missionId: 'm_review_rev',
      tenantId: '10',
      clientId: 10,
      constraints: { clientPlaybook: pb },
      inputs: {
        campaignId: 'camp_rev',
        campaign: {
          ...campaign,
          prospects: [campaign.prospects[0]],
        },
        packages: [readyPkg],
        mailBatch: { id: 'mail_rev', packages: [readyPkg] },
      },
    };

    const first = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
      context: ctxBase,
    });
    assert.equal(first.result.outputs.reviewRevision, 1);

    const second = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
      context: {
        ...ctxBase,
        inputs: {
          ...ctxBase.inputs,
          reviewActions: [
            {
              type: 'edit_letter',
              prospectId: 'p1',
              body: 'Dear Jordan,\n\nUpdated letter body for Bedford Law.\n',
              operator: 'jacob',
            },
            { type: 'approve_selected', prospectIds: ['p1'], operator: 'jacob' },
            { type: 'approve_campaign', operator: 'jacob' },
          ],
        },
      },
    });

    assert.equal(second.result.outputs.reviewRevision, 2);
    const edited = second.result.outputs.queue.find((r) => r.prospectId === 'p1');
    assert.match(edited.letter.body, /Updated letter body/);
    assert.equal(edited.status, campaignReview.PROSPECT_REVIEW_STATUS.APPROVED);
    assert.ok(
      second.result.outputs.missionRevisions.some((r) => r.reason === 'letter_edit')
    );
    assert.ok(second.result.outputs.revisionHistory.length >= 2);
    assert.equal(second.result.outputs.campaignApproved, true);

    const compared = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
      context: {
        ...ctxBase,
        inputs: {
          ...ctxBase.inputs,
          reviewActions: [
            { type: 'compare_revisions', revisionA: 1, revisionB: 2 },
          ],
        },
      },
    });
    assert.ok(compared.result.outputs.compareResult);
    assert.equal(compared.result.outputs.compareResult.revisionA, 1);
    assert.equal(compared.result.outputs.compareResult.revisionB, 2);
  });

  it('exports and prints selected prospects', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const pb = playbook.createClientPlaybookStore().get('pb_anchor_cleaning');
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
      context: {
        missionId: 'm_export',
        tenantId: '10',
        constraints: { clientPlaybook: pb },
        inputs: {
          campaign,
          packages: [readyPkg],
          mailBatch: { id: 'm', packages: [readyPkg] },
          reviewActions: [
            {
              type: 'export_selected',
              prospectIds: ['p1'],
            },
            {
              type: 'print_selected',
              prospectIds: ['p1'],
            },
          ],
        },
      },
    });
    assert.ok(out.result.outputs.exportArtifacts.length >= 2);
    assert.ok(
      out.result.outputs.exportArtifacts.some((e) => e.type === 'export_selected')
    );
    assert.ok(
      out.result.outputs.exportArtifacts.some(
        (e) => e.type === 'print_selected' && e.printableHtml
      )
    );
  });
});

describe('Campaign Review canRun diagnostics', () => {
  const campaignReview = require('../campaignReview');

  it('diagnoseCanRun explains missing Campaign precondition (SPEC-058)', () => {
    const cap = campaignReview.createCampaignReviewCapability();
    const diagnosis = cap.diagnoseCanRun({ inputs: {} });
    assert.equal(diagnosis.runnable, false);
    assert.equal(cap.canRun({ inputs: {} }), false);
    assert.equal(diagnosis.failedPrecondition, 'Campaign artifact required');
    assert.equal(diagnosis.expectedArtifact, 'Campaign');
    assert.equal(diagnosis.producer, 'Campaign Builder');
    assert.match(String(diagnosis.actualState), /Not Present/i);
    assert.match(
      String(diagnosis.recommendedNextAction),
      /Campaign Builder/i
    );
  });

  it('diagnostic mode returns blocked structured explanation', async () => {
    const registry = createBuiltinRegistry({ discovery: { useFixture: true } });
    const runner = createCapabilityRunner({ registry });
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
      context: {
        missionId: 'm_review_diag',
        tenantId: '10',
        clientId: 10,
        executionMode: 'diagnostic',
        missionIntent: {
          matchedIntent: 'campaign_diagnostics',
          diagnostics: true,
          mode: 'diagnostics',
        },
        inputs: {
          discoveryDiagnostics: {
            artifactType: 'DiscoveryDiagnostics',
            summary: 'Discovery blocked',
            blocked: true,
          },
        },
      },
    });

    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.BLOCKED);
    assert.equal(out.executionMode, 'diagnostic');
    const err = out.result.errors[0];
    assert.ok(err);
    assert.notEqual(err.message, 'canRun returned false');
    assert.equal(err.failedPrecondition, 'Campaign artifact required');
    assert.equal(
      out.result.outputs.preconditionDiagnostics.expectedArtifact,
      'Campaign'
    );
    assert.equal(
      out.result.outputs.preconditionDiagnostics.producer,
      'Campaign Builder'
    );
    assert.ok(out.result.outputs.preconditionDiagnostics.actualState);
    assert.ok(
      out.result.outputs.preconditionDiagnostics.recommendedNextAction
    );
    assert.equal(out.result.outputs.reviewPackage, null);
    assert.equal(out.result.outputs.reviewDecision, null);
    assert.equal(out.result.artifacts.length, 0);
  });

  it('execution mode preserves canRun gate with structured failure', async () => {
    const registry = createBuiltinRegistry({ discovery: { useFixture: true } });
    const runner = createCapabilityRunner({ registry });
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
      context: {
        missionId: 'm_review_empty',
        tenantId: '10',
        executionMode: 'execution',
        inputs: {},
      },
    });
    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.FAILED);
    assert.equal(out.executionMode, 'execution');
    const err = out.result.errors[0];
    assert.notEqual(err.message, 'canRun returned false');
    assert.equal(err.expectedArtifact, 'Campaign');
    assert.equal(err.producer, 'Campaign Builder');
    assert.ok(out.result.outputs.preconditionDiagnostics);
    assert.equal(out.result.outputs.reviewPackage, null);
  });
});

describe('SPEC-035 Direct Mail Execution', () => {
  const dmx = require('../directMailExecution');

  const readyPkg = {
    id: 'pkg_p1',
    prospectId: 'p1',
    status: 'ready_to_print',
    letter: {
      recipientName: 'Jordan Hale',
      companyName: 'Bedford Law',
      body: 'Dear Jordan Hale,\n\nBedford Law looks like a strong fit.\n',
    },
    envelope: {
      recipientName: 'Jordan Hale',
      companyName: 'Bedford Law',
      mailingAddress: '5 Commerce Park N, Bedford, NH 03110',
      returnAddress: 'Anchor Cleaning, Manchester NH',
    },
    insertChecklist: [
      { id: 'letter', label: 'Letter', required: true, included: true },
      { id: 'business_card', label: 'Business Card', required: true, included: true },
    ],
  };

  const approvedInputs = {
    campaignId: 'camp_exec_001',
    campaign: {
      id: 'camp_exec_001',
      name: 'Campaign 001',
      status: 'ready_to_print',
      revision: 2,
      prospects: [
        {
          id: 'p1',
          companyName: 'Bedford Law',
          contactName: 'Jordan Hale',
          address: '5 Commerce Park N, Bedford, NH 03110',
        },
      ],
    },
    campaignApproved: true,
    approvedRevision: 2,
    campaignStatus: 'ready_to_print',
    packages: [readyPkg],
    mailBatch: { id: 'mail_batch_1', packages: [readyPkg] },
    executionPackage: {
      id: 'ep_1',
      printPackage: { html: '<html>Bedford Law</html>' },
      mailMerge: { csv: 'name,company\nJordan Hale,Bedford Law' },
      addressLabels: { csv: 'Bedford Law,5 Commerce Park N' },
    },
  };

  it('rejects execution without approved revision (ADR-021/022)', async () => {
    const registry = testRegistry();
    const runner = createCapabilityRunner({ registry });
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.DIRECT_MAIL_EXECUTION,
      context: {
        missionId: 'm_dmx_block',
        tenantId: '10',
        clientId: 10,
        inputs: {
          packages: [readyPkg],
          mailBatch: { id: 'm', packages: [readyPkg] },
          campaignApproved: false,
        },
      },
    });
    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.FAILED);
    assert.ok(
      out.result.errors.some((e) => e.code === 'approved_revision_required')
    );
  });

  it('enforces deterministic transitions and locks on Printing', async () => {
    const store = dmx.createInMemoryDirectMailExecutionStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      directMailExecution: { directMailExecutionStore: store },
    });
    const runner = createCapabilityRunner({ registry });

    const out = await runner.run({
      capabilityId: BUILTIN_IDS.DIRECT_MAIL_EXECUTION,
      context: {
        missionId: 'm_dmx_lock',
        tenantId: '10',
        clientId: 10,
        inputs: {
          ...approvedInputs,
          executionActions: [
            { type: 'start_execution', operator: 'jacob' },
            { type: 'start_print_session', operator: 'jacob' },
          ],
        },
      },
    });

    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    assert.equal(
      out.result.outputs.summary.status,
      dmx.EXECUTION_STATUS.PRINTING
    );
    assert.equal(out.result.outputs.lock.locked, true);
    assert.equal(out.result.outputs.lock.campaignRevision, 2);
    assert.ok(out.result.outputs.printSessions.length >= 1);
    assert.ok(
      out.result.outputs.auditLog.some(
        (a) =>
          a.previousState === dmx.EXECUTION_STATUS.READY_TO_PRINT &&
          a.newState === dmx.EXECUTION_STATUS.PRINTING
      )
    );

    // Illegal transition blocked
    assert.equal(
      dmx.canTransition(dmx.EXECUTION_STATUS.DRAFT, dmx.EXECUTION_STATUS.MAILED),
      false
    );
    assert.equal(
      dmx.canTransition(
        dmx.EXECUTION_STATUS.READY_TO_PRINT,
        dmx.EXECUTION_STATUS.PRINTING
      ),
      true
    );

    // Locked mutation rejected
    const locked = out.result.outputs.execution;
    const gate = dmx.validateArtifactMutation(locked, {
      replaceRevision: true,
      generateContent: true,
    });
    assert.equal(gate.ok, false);
    assert.ok(gate.errors.includes('campaign_revision_locked'));
    assert.ok(gate.errors.includes('execution_must_not_generate_content'));
  });

  it('tracks assembly, mailing, responses, metrics, and immutable audit', async () => {
    const store = dmx.createInMemoryDirectMailExecutionStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      directMailExecution: { directMailExecutionStore: store },
    });
    const runner = createCapabilityRunner({ registry });

    const out = await runner.run({
      capabilityId: BUILTIN_IDS.DIRECT_MAIL_EXECUTION,
      context: {
        missionId: 'm_dmx_flow',
        tenantId: '10',
        clientId: 10,
        inputs: {
          ...approvedInputs,
          campaignId: 'camp_exec_flow',
          executionActions: [
            { type: 'start_execution', operator: 'jacob' },
            { type: 'start_print_session', operator: 'jacob' },
            { type: 'complete_print_session', operator: 'jacob' },
            {
              type: 'assembly_complete',
              prospectId: 'p1',
              operator: 'jacob',
            },
            {
              type: 'mark_all_mailed',
              operator: 'jacob',
              uspsBatchId: 'USPS-991',
              notes: 'Dropped at post office',
              date: '2026-07-27T15:00:00.000Z',
            },
            {
              type: 'set_response',
              prospectId: 'p1',
              responseStatus: 'walkthrough_scheduled',
              operator: 'jacob',
            },
            { type: 'complete_campaign', operator: 'jacob' },
          ],
        },
      },
    });

    assert.equal(
      out.result.outputs.summary.status,
      dmx.EXECUTION_STATUS.COMPLETED
    );
    const metrics = out.result.outputs.metrics;
    assert.equal(metrics.mailed, 1);
    assert.equal(metrics.assembled, 1);
    assert.equal(metrics.responses, 1);
    assert.equal(metrics.meetings, 1);
    assert.equal(metrics.responseRate, 1);

    const p1 = out.result.outputs.prospects.find((p) => p.prospectId === 'p1');
    assert.equal(p1.mailed, true);
    assert.equal(p1.uspsBatchId, 'USPS-991');
    assert.equal(
      p1.responseStatus,
      dmx.RESPONSE_STATUS.WALKTHROUGH_SCHEDULED
    );
    assert.equal(p1.assemblyComplete, true);

    // Immutable audit: entries frozen / append-only history present
    const audit = out.result.outputs.auditLog;
    assert.ok(audit.length >= 4);
    const first = audit[0];
    assert.ok(first.previousState != null || first.newState);
    assert.ok(first.timestamp);
    assert.ok(first.operator);

    // Mission timeline / events updated
    assert.ok(out.result.outputs.missionEvents.length >= 1);
    assert.ok(out.result.outputs.timeline.length >= 1);
    assert.ok(
      out.result.outputs.timeline.some(
        (t) => t.kind === 'mission_timeline' && t.stage === 'direct_mail_execution'
      )
    );
    assert.ok(
      out.result.artifacts.some((a) => a.type === 'direct_mail_execution')
    );
  });

  it('supports assembly skip / reopen and mark selected mailed', async () => {
    const pkg2 = {
      ...readyPkg,
      id: 'pkg_p2',
      prospectId: 'p2',
      letter: { ...readyPkg.letter, companyName: 'Thin Co', recipientName: 'Sam Lee' },
      envelope: {
        ...readyPkg.envelope,
        companyName: 'Thin Co',
        recipientName: 'Sam Lee',
      },
    };
    const store = dmx.createInMemoryDirectMailExecutionStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      directMailExecution: { directMailExecutionStore: store },
    });
    const runner = createCapabilityRunner({ registry });

    const out = await runner.run({
      capabilityId: BUILTIN_IDS.DIRECT_MAIL_EXECUTION,
      context: {
        missionId: 'm_dmx_skip',
        tenantId: '10',
        clientId: 10,
        inputs: {
          ...approvedInputs,
          campaignId: 'camp_skip',
          packages: [readyPkg, pkg2],
          mailBatch: { id: 'mail_skip', packages: [readyPkg, pkg2] },
          campaign: {
            ...approvedInputs.campaign,
            id: 'camp_skip',
            prospects: [
              ...approvedInputs.campaign.prospects,
              { id: 'p2', companyName: 'Thin Co', contactName: 'Sam Lee' },
            ],
          },
          executionActions: [
            { type: 'start_print_session', operator: 'jacob' },
            { type: 'complete_print_session', operator: 'jacob' },
            { type: 'assembly_skip', prospectId: 'p2', operator: 'jacob' },
            { type: 'assembly_complete', prospectId: 'p1', operator: 'jacob' },
            {
              type: 'mark_selected_mailed',
              prospectIds: ['p1'],
              operator: 'jacob',
            },
          ],
        },
      },
    });

    const p2 = out.result.outputs.prospects.find((p) => p.prospectId === 'p2');
    const p1 = out.result.outputs.prospects.find((p) => p.prospectId === 'p1');
    assert.equal(p2.skipped, true);
    assert.equal(p1.mailed, true);
    assert.equal(out.result.outputs.metrics.mailed, 1);
    assert.equal(
      out.result.outputs.summary.status,
      dmx.EXECUTION_STATUS.MAILED
    );
  });
});

describe('SPEC-036 Outcome Intelligence', () => {
  const oi = require('../outcomeIntelligence');

  function sampleProspects() {
    return [
      {
        prospectId: 'p1',
        company: 'PM Alpha',
        responseStatus: 'called',
        mailed: true,
        delivered: true,
        attributes: { vertical: 'property_management', handwritten: true },
        vertical: 'property_management',
        industry: 'property_management',
        region: 'manchester',
      },
      {
        prospectId: 'p2',
        company: 'PM Beta',
        responseStatus: 'walkthrough_scheduled',
        mailed: true,
        attributes: { handwritten: true },
        vertical: 'property_management',
        industry: 'property_management',
        region: 'manchester',
      },
      {
        prospectId: 'p3',
        company: 'PM Gamma',
        responseStatus: 'closed_won',
        mailed: true,
        attributes: { handwritten: true, offer: 'audit', cta: 'call' },
        vertical: 'property_management',
        industry: 'property_management',
        region: 'manchester',
      },
      {
        prospectId: 'p4',
        company: 'Dental One',
        responseStatus: 'no_response',
        mailed: true,
        vertical: 'dental',
        industry: 'dental',
        region: 'manchester',
      },
      {
        prospectId: 'p5',
        company: 'Dental Two',
        responseStatus: 'returned_mail',
        mailed: true,
        vertical: 'dental',
        industry: 'dental',
        region: 'nashua',
      },
      {
        prospectId: 'p6',
        company: 'Dental Three',
        responseStatus: 'not_interested',
        mailed: true,
        vertical: 'dental',
        industry: 'dental',
        region: 'nashua',
      },
    ];
  }

  it('captures every execution outcome', async () => {
    const store = oi.createInMemoryOutcomeIntelligenceStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      outcomeIntelligence: { outcomeIntelligenceStore: store },
    });
    const runner = createCapabilityRunner({ registry });

    const out = await runner.run({
      capabilityId: BUILTIN_IDS.OUTCOME_INTELLIGENCE,
      context: {
        missionId: 'm_oi_1',
        tenantId: '10',
        clientId: 10,
        objective: 'Capture outcomes for Campaign 001',
        inputs: {
          campaignId: 'camp_oi_1',
          campaignName: 'Campaign 001',
          prospects: sampleProspects(),
          metrics: { mailed: 6 },
          cost: 600,
          revenue: 2400,
          operator: 'jacob',
        },
      },
    });

    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    assert.ok(out.result.outputs.outcomes.length >= 6);
    assert.ok(out.result.outputs.analytics);
    assert.equal(out.result.outputs.analytics.mailed, 6);
    assert.ok(out.result.outputs.analytics.responseRate > 0);
    assert.ok(out.result.outputs.outcomeSummary);
    assert.equal(out.result.outputs.outcomeSummary.kind, 'mission_outcome_summary');
    assert.ok(
      out.result.outputs.timeline.some(
        (t) => t.kind === 'mission_timeline' && t.stage === 'outcome_intelligence'
      )
    );
    assert.ok(
      out.result.artifacts.some((a) => a.type === 'outcome_intelligence')
    );
  });

  it('generates learnings only from evidence', async () => {
    const store = oi.createInMemoryOutcomeIntelligenceStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      outcomeIntelligence: { outcomeIntelligenceStore: store },
    });
    const runner = createCapabilityRunner({ registry });

    const out = await runner.run({
      capabilityId: BUILTIN_IDS.OUTCOME_INTELLIGENCE,
      context: {
        missionId: 'm_oi_learn',
        tenantId: '10',
        clientId: 10,
        inputs: {
          campaignId: 'camp_oi_learn',
          prospects: sampleProspects(),
          metrics: { mailed: 6 },
        },
      },
    });

    const learnings = out.result.outputs.learnings;
    assert.ok(learnings.length > 0);
    const backed = learnings.filter(
      (l) => l.status === oi.LEARNING_STATUS.EVIDENCE_BACKED
    );
    const candidates = learnings.filter(
      (l) => l.status === oi.LEARNING_STATUS.CANDIDATE
    );
    assert.ok(backed.length >= 1);
    // Thin segments stay candidates
    assert.ok(
      backed.every((l) => l.sampleSize >= oi.MIN_EVIDENCE_SAMPLES)
    );
    assert.ok(
      backed.every((l) => Math.abs(l.lift) >= oi.MIN_EVIDENCE_LIFT)
    );
    void candidates;
  });

  it('keeps recommendations pending until approval; apply requires approve', async () => {
    const store = oi.createInMemoryOutcomeIntelligenceStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      outcomeIntelligence: { outcomeIntelligenceStore: store },
    });
    const runner = createCapabilityRunner({ registry });

    const first = await runner.run({
      capabilityId: BUILTIN_IDS.OUTCOME_INTELLIGENCE,
      context: {
        missionId: 'm_oi_rec',
        tenantId: '10',
        clientId: 10,
        inputs: {
          campaignId: 'camp_oi_rec',
          prospects: sampleProspects(),
          metrics: { mailed: 6 },
        },
      },
    });

    const pending = first.result.outputs.recommendations.filter(
      (r) => r.status === oi.RECOMMENDATION_STATUS.PENDING
    );
    assert.ok(pending.length >= 1);
    assert.ok(pending.every((r) => r.evidenceBacked === true));

    const recId = pending[0].id;

    // Apply without approve must fail closed
    const blocked = await runner.run({
      capabilityId: BUILTIN_IDS.OUTCOME_INTELLIGENCE,
      context: {
        missionId: 'm_oi_rec',
        tenantId: '10',
        clientId: 10,
        inputs: {
          campaignId: 'camp_oi_rec',
          outcomeActions: [
            { type: 'apply_recommendation', recommendationId: recId },
          ],
          operator: 'jacob',
        },
      },
    });
    assert.ok(
      blocked.result.outputs.actionErrors.includes('recommendation_not_approved') ||
        blocked.result.status === CAPABILITY_RESULT_STATUS.PARTIAL
    );

    const approved = await runner.run({
      capabilityId: BUILTIN_IDS.OUTCOME_INTELLIGENCE,
      context: {
        missionId: 'm_oi_rec',
        tenantId: '10',
        clientId: 10,
        inputs: {
          campaignId: 'camp_oi_rec',
          outcomeActions: [
            { type: 'approve_recommendation', recommendationId: recId },
            { type: 'apply_recommendation', recommendationId: recId },
            { type: 'conclude_mission', objectiveAchieved: true },
          ],
          operator: 'jacob',
        },
      },
    });

    const rec = approved.result.outputs.recommendations.find((r) => r.id === recId);
    assert.equal(rec.status, oi.RECOMMENDATION_STATUS.APPLIED);
    assert.equal(
      approved.result.outputs.outcomeSummary.kind,
      'mission_outcome_summary'
    );
    assert.equal(approved.result.outputs.outcomeSummary.objectiveAchieved, true);
  });

  it('feeds structured ranking feedback', async () => {
    const store = oi.createInMemoryOutcomeIntelligenceStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      outcomeIntelligence: { outcomeIntelligenceStore: store },
    });
    const runner = createCapabilityRunner({ registry });

    const out = await runner.run({
      capabilityId: BUILTIN_IDS.OUTCOME_INTELLIGENCE,
      context: {
        missionId: 'm_oi_rank',
        tenantId: '10',
        clientId: 10,
        inputs: {
          campaignId: 'camp_oi_rank',
          prospects: sampleProspects(),
        },
      },
    });

    assert.ok(out.result.outputs.rankingFeedback.length > 0);
    assert.ok(
      out.result.outputs.rankingFeedback.every(
        (f) => f.kind === 'ranking_feedback' && typeof f.scoreDelta === 'number'
      )
    );
    assert.ok(out.result.outputs.historicalOutcomes.length >= 6);
    assert.ok(
      out.result.outputs.historicalOutcomes.some((h) => h.successful === true)
    );
  });

  it('tracks personalization feedback dimensions', () => {
    const outcomes = sampleProspects().map((p) =>
      oi.buildOutcomeRecord({
        ...p,
        id: `out_${p.prospectId}`,
        outcomeType: oi.RESPONSE_STATUS_TO_OUTCOME[p.responseStatus],
      })
    );
    const feedback = oi.trackPersonalization(outcomes);
    assert.equal(feedback.kind, 'personalization_feedback');
    assert.ok(feedback.dimensions.personalization_facts.exposures >= 1);
    assert.ok(feedback.dimensions.offer.exposures >= 1);
  });
});

describe('SPEC-037 Operator Inbox', () => {
  const inbox = require('../operatorInbox');

  it('assembles a single operational inbox from capability sources', async () => {
    const store = inbox.createInMemoryOperatorInboxStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      operatorInbox: { operatorInboxStore: store },
    });
    const runner = createCapabilityRunner({ registry });

    const out = await runner.run({
      capabilityId: BUILTIN_IDS.OPERATOR_INBOX,
      context: {
        missionId: 'm_inbox_1',
        tenantId: '10',
        clientId: 10,
        inputs: {
          campaignId: 'camp_001',
          campaignName: 'Campaign 001',
          reviewSummary: { status: 'in_review' },
          execution: { summary: { status: 'ready_to_print', campaignName: 'Campaign 001' } },
          recommendations: [
            {
              id: 'rec_1',
              summary: 'Increase property manager targeting.',
              status: 'pending',
              target: 'discovery_strategy',
              evidenceBacked: true,
            },
          ],
          validationResults: [
            {
              code: 'missing_address',
              prospectId: 'p9',
              message: 'Missing address — Acme LLC',
            },
          ],
          operator: 'jacob',
        },
      },
    });

    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    assert.equal(out.result.outputs.coordinationOnly, true);
    assert.ok(out.result.outputs.activeItems.length >= 3);
    assert.ok(
      out.result.outputs.activeItems.every((i) => i.deepLink && i.deepLink.workspace)
    );
    assert.ok(
      out.result.outputs.timeline.some(
        (t) => t.kind === 'mission_timeline' && t.stage === 'operator_inbox'
      )
    );
    assert.ok(out.result.artifacts.some((a) => a.type === 'operator_inbox'));
  });

  it('deduplicates identical work items', () => {
    const a = inbox.buildCandidate(
      {
        kind: inbox.INBOX_KINDS.CAMPAIGN_APPROVAL,
        title: 'Campaign approval — Campaign 001',
        sourceCapability: 'campaign_review',
      },
      { clientId: 10, missionId: 'm1', campaignId: 'camp_001' }
    );
    const b = inbox.buildCandidate(
      {
        kind: inbox.INBOX_KINDS.CAMPAIGN_APPROVAL,
        title: 'Campaign approval — Campaign 001',
        sourceCapability: 'mission_memory',
      },
      { clientId: 10, missionId: 'm1', campaignId: 'camp_001' }
    );
    const { items, merged, created } = inbox.dedupeInboxItems([a], [b]);
    assert.equal(items.length, 1);
    assert.equal(merged, 1);
    assert.equal(created, 0);
    assert.ok(items[0].sources.length >= 2);
  });

  it('applies deterministic prioritization', () => {
    const now = new Date('2026-07-27T12:00:00Z');
    const approval = inbox.buildInboxItem({
      id: '1',
      kind: inbox.INBOX_KINDS.CAMPAIGN_APPROVAL,
      category: inbox.INBOX_CATEGORIES.APPROVAL_REQUIRED,
    });
    const action = inbox.buildInboxItem({
      id: '2',
      kind: inbox.INBOX_KINDS.PRINT_CAMPAIGN,
      category: inbox.INBOX_CATEGORIES.ACTION_REQUIRED,
    });
    const completed = inbox.buildInboxItem({
      id: '3',
      kind: inbox.INBOX_KINDS.OUTCOME_SUMMARY_AVAILABLE,
      category: inbox.INBOX_CATEGORIES.COMPLETED,
    });
    const overdue = inbox.buildInboxItem({
      id: '4',
      kind: inbox.INBOX_KINDS.CAMPAIGN_APPROVAL,
      category: inbox.INBOX_CATEGORIES.APPROVAL_REQUIRED,
      dueDate: '2026-07-20T00:00:00Z',
    });

    assert.equal(inbox.computePriority(approval, now), inbox.INBOX_PRIORITY.HIGH);
    assert.equal(inbox.computePriority(action, now), inbox.INBOX_PRIORITY.NORMAL);
    assert.equal(inbox.computePriority(completed, now), inbox.INBOX_PRIORITY.LOW);
    assert.equal(inbox.computePriority(overdue, now), inbox.INBOX_PRIORITY.CRITICAL);

    const sorted = inbox.sortInboxItems(
      inbox.prioritizeItems([action, completed, overdue, approval], now)
    );
    assert.equal(sorted[0].id, '4');
    assert.equal(sorted[sorted.length - 1].id, '3');
  });

  it('completing an item updates Mission Memory shapes and removes from active', async () => {
    const store = inbox.createInMemoryOperatorInboxStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      operatorInbox: { operatorInboxStore: store },
    });
    const runner = createCapabilityRunner({ registry });

    const first = await runner.run({
      capabilityId: BUILTIN_IDS.OPERATOR_INBOX,
      context: {
        missionId: 'm_inbox_complete',
        tenantId: '10',
        clientId: 10,
        inputs: {
          campaignId: 'camp_c',
          campaignName: 'Campaign C',
          workItems: [
            {
              kind: inbox.INBOX_KINDS.PRINT_CAMPAIGN,
              title: 'Print campaign — Campaign C',
              sourceCapability: 'direct_mail_execution',
            },
          ],
        },
      },
    });

    const itemId = first.result.outputs.activeItems[0].id;
    assert.ok(itemId);

    const done = await runner.run({
      capabilityId: BUILTIN_IDS.OPERATOR_INBOX,
      context: {
        missionId: 'm_inbox_complete',
        tenantId: '10',
        clientId: 10,
        inputs: {
          inboxActions: [
            { type: 'complete', itemId, notes: 'Printed offline' },
          ],
          operator: 'jacob',
        },
      },
    });

    assert.ok(
      !done.result.outputs.activeItems.some((i) => i.id === itemId)
    );
    const completed = done.result.outputs.items.find((i) => i.id === itemId);
    assert.equal(completed.status, inbox.INBOX_STATUS.COMPLETED);
    assert.ok(done.result.outputs.completionEvents.length >= 1);
    assert.ok(done.result.outputs.auditLog.some((a) => a.action === 'complete'));
    assert.ok(
      done.result.outputs.missionEvents.some(
        (e) => e.eventType === 'inbox_completed'
      )
    );
    assert.ok(
      done.result.outputs.timeline.some((t) => t.status === 'item_completed')
    );
  });

  it('refuses to perform workflow processing (ADR-024)', async () => {
    const store = inbox.createInMemoryOperatorInboxStore();
    const registry = createBuiltinRegistry({
      discovery: { useFixture: true },
      operatorInbox: { operatorInboxStore: store },
    });
    const runner = createCapabilityRunner({ registry });

    const out = await runner.run({
      capabilityId: BUILTIN_IDS.OPERATOR_INBOX,
      context: {
        missionId: 'm_inbox_block',
        tenantId: '10',
        clientId: 10,
        inputs: {
          executeWorkflow: true,
          runCapability: 'direct_mail_execution',
        },
      },
    });

    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.FAILED);
    assert.ok(
      out.result.errors.some((e) => e.code === 'inbox_must_not_perform_workflow')
    );
  });
});
