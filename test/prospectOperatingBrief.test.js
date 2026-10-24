'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  ACTION_TYPES,
  ProspectOperatingBriefError,
  formatOperatingBriefReport,
  getProspectOperatingBrief,
  suggestNextAction,
} = require('../services/prospectOperatingBrief');
const {
  createMemoryStore,
  startRelationshipInterview,
  summarizeRelationshipInterview,
  commitRelationshipInterview,
  listInteractions,
} = require('../services/relationshipIntelligenceInterview');

function torontoSnapshot(overrides = {}) {
  return {
    found: true,
    companyId: 'co-toronto-cleaning',
    prospectId: 'prospect-toronto-1',
    contactId: 'prospect-toronto-1',
    opportunityId: null,
    clientId: 1,
    companyName: 'Toronto Shine Cleaning',
    contactName: 'Alex Rivera',
    contactRole: 'Owner',
    email: 'alex@torontoshine.example',
    phone: '+1-416-555-0100',
    website: 'https://torontoshine.example',
    location: 'Toronto, ON',
    industry: 'cleaning',
    vertical: 'cleaning',
    source: 'discovery',
    status: 'warm',
    setterStatus: 'contacted',
    score: 82,
    doNotContact: false,
    notes: null,
    opportunityStage: null,
    ...overrides,
  };
}

function fakeMarketService({ empty = false } = {}) {
  return {
    async getTopCtas() {
      if (empty) return [];
      return [
        {
          cta: 'book a walkthrough',
          count: 12,
          companies: ['Vendor A'],
          exampleObservationIds: ['obs-cta-1'],
        },
      ];
    },
    async getTopOffers() {
      if (empty) return [];
      return [
        {
          label: 'free first clean',
          count: 8,
          companies: ['Vendor B'],
          exampleObservationIds: ['obs-offer-1'],
        },
      ];
    },
    async getMessagingThemes() {
      if (empty) return { items: [] };
      return {
        items: [
          {
            theme: 'reliability',
            field: 'positioning',
            count: 5,
            companies: ['Vendor A'],
            exampleObservationIds: ['obs-theme-1'],
          },
        ],
      };
    },
    async getCorpusSummary() {
      if (empty) {
        return {
          emailCount: 0,
          companyCount: 0,
          observationCount: 0,
          readinessStatus: 'partial',
        };
      }
      return {
        emailCount: 40,
        companyCount: 10,
        observationCount: 120,
        readinessStatus: 'ready',
      };
    },
  };
}

async function seedCommittedInterview(store, notes, context = {}) {
  const opts = { store };
  const started = await startRelationshipInterview(
    {
      type: 'discovery_call',
      companyId: context.companyId || 'co-toronto-cleaning',
      contactId: context.contactId || 'prospect-toronto-1',
      clientId: context.clientId || 1,
      notes,
    },
    opts
  );
  await summarizeRelationshipInterview(started.interviewId, opts);
  const committed = await commitRelationshipInterview(started.interviewId, opts);
  return { interviewId: started.interviewId, committed };
}

describe('SPEC-074 prospectOperatingBrief', () => {
  it('requires at least one target identifier', async () => {
    await assert.rejects(
      () => getProspectOperatingBrief({ loadCompanySnapshot: async () => ({ found: false }) }),
      (err) =>
        err instanceof ProspectOperatingBriefError && err.code === 'target_required'
    );
  });

  it('returns caveats when no relationship intelligence exists', async () => {
    const store = createMemoryStore();
    const brief = await getProspectOperatingBrief({
      companyId: 'co-toronto-cleaning',
      store,
      loadCompanySnapshot: async () => torontoSnapshot(),
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(brief.ok, true);
    assert.equal(brief.kind, 'prospect_operating_brief');
    assert.equal(brief.isEvidence, false);
    assert.equal(brief.autonomousExecution, false);
    assert.ok(
      brief.caveats.some((c) => c.includes('relationship_intelligence_missing'))
    );
    assert.equal(brief.sections.buyingSignals.length, 0);
    assert.equal(brief.sections.relationshipSummary.interactionCount, 0);
    assert.ok(brief.sections.suggestedNextAction.actionType);
    assert.ok(ACTION_TYPES.includes(brief.sections.suggestedNextAction.actionType));
  });

  it('uses only committed relationship interactions by default', async () => {
    const store = createMemoryStore();
    const opts = { store };

    const draft = await startRelationshipInterview(
      {
        type: 'discovery_call',
        companyId: 'co-toronto-cleaning',
        contactId: 'prospect-toronto-1',
        clientId: 1,
        notes:
          'Draft only. Owner is interested and ready. Next step: send proposal. Pain is missed night cleaning.',
      },
      opts
    );
    await summarizeRelationshipInterview(draft.interviewId, opts);
    // leave as draft — do not commit

    await seedCommittedInterview(
      store,
      'Committed discovery. Owner is interested and ready to move. Goal is consistent coverage. Next step: schedule walkthrough Friday. We promised references.',
      { companyId: 'co-toronto-cleaning', contactId: 'prospect-toronto-1' }
    );

    const listed = await listInteractions({ status: 'committed', companyId: 'co-toronto-cleaning' }, opts);
    assert.equal(listed.length, 1);

    const brief = await getProspectOperatingBrief({
      companyId: 'co-toronto-cleaning',
      prospectId: 'prospect-toronto-1',
      store,
      loadCompanySnapshot: async () => torontoSnapshot(),
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(brief.sections.relationshipSummary.interactionCount, 1);
    assert.equal(brief.sourceRefs.relationshipInteractionIds.length, 1);
    assert.equal(
      brief.sourceRefs.relationshipInteractionIds[0],
      listed[0].id
    );
    assert.ok(!brief.sourceRefs.relationshipInteractionIds.includes(draft.interviewId));
  });

  it('extracts buying signals and next steps from relationship insights', async () => {
    const store = createMemoryStore();
    await seedCommittedInterview(
      store,
      'Discovery call with cleaning company owner in Toronto. They are interested and ready. Main pain is inconsistent night staff. Goal is reliable coverage. Next step: send a proposal with pricing. We promised the estimate by Thursday.'
    );

    const brief = await getProspectOperatingBrief({
      companyId: 'co-toronto-cleaning',
      prospectId: 'prospect-toronto-1',
      store,
      loadCompanySnapshot: async () => torontoSnapshot(),
      marketBriefingService: fakeMarketService(),
    });

    assert.ok(brief.sections.buyingSignals.length >= 1);
    assert.ok(brief.sections.commitmentsAndNextSteps.length >= 1);
    assert.ok(
      brief.sections.buyingSignals.some((i) => i.kind === 'buying_signal')
    );
    assert.ok(
      brief.sections.commitmentsAndNextSteps.some((i) =>
        ['next_step', 'commitment'].includes(i.kind)
      )
    );
    assert.ok(brief.sections.painsAndGoals.length >= 1);
  });

  it('includes market context when available', async () => {
    const store = createMemoryStore();
    const brief = await getProspectOperatingBrief({
      companyId: 'co-toronto-cleaning',
      store,
      loadCompanySnapshot: async () => torontoSnapshot(),
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(brief.readiness.marketIntelligence, 'ready');
    assert.ok(brief.sections.marketContext.topCtas.length >= 1);
    assert.ok(brief.sections.marketContext.topOffers.length >= 1);
    assert.ok(brief.sections.marketContext.messagingThemes.length >= 1);
    assert.equal(brief.sections.marketContext.generalCorpusOnly, true);
    assert.ok(brief.sourceRefs.marketObservationIds.includes('obs-cta-1'));
    assert.ok(
      brief.caveats.some((c) => c.includes('market_context_general'))
    );
  });

  it('excludes autonomous execution and keeps suggestion explainable', async () => {
    const store = createMemoryStore();
    await seedCommittedInterview(
      store,
      'Owner is interested. Next step: send follow-up with proposal timing. Pain is staffing.'
    );

    const brief = await getProspectOperatingBrief({
      companyId: 'co-toronto-cleaning',
      store,
      loadCompanySnapshot: async () => torontoSnapshot(),
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(brief.autonomousExecution, false);
    assert.equal(brief.isEvidence, false);
    const action = brief.sections.suggestedNextAction;
    assert.ok(ACTION_TYPES.includes(action.actionType));
    assert.ok(action.rationale && action.rationale.length > 10);
    assert.ok(Array.isArray(action.cautions));
    assert.ok(
      action.cautions.some((c) => /manual|autonomous/i.test(c))
    );
    assert.equal(
      Object.prototype.hasOwnProperty.call(brief, 'execute'),
      false
    );
  });

  it('includes source refs for company and relationship', async () => {
    const store = createMemoryStore();
    await seedCommittedInterview(
      store,
      'Discovery. Owner interested. Next step book walkthrough. Goal reliable nights.'
    );

    const brief = await getProspectOperatingBrief({
      companyId: 'co-toronto-cleaning',
      prospectId: 'prospect-toronto-1',
      store,
      loadCompanySnapshot: async () => torontoSnapshot(),
      marketBriefingService: fakeMarketService(),
    });

    assert.deepEqual(brief.sourceRefs.companyProfileIds, ['co-toronto-cleaning']);
    assert.ok(brief.sourceRefs.relationshipInteractionIds.length >= 1);
    assert.equal(brief.target.companyName, 'Toronto Shine Cleaning');
    assert.equal(brief.target.contactName, 'Alex Rivera');
  });

  it('suggestNextAction prefers walkthrough when recorded', () => {
    const action = suggestNextAction({
      snapshot: torontoSnapshot(),
      relationship: {
        interactionCount: 1,
        buyingSignals: [{ kind: 'buying_signal', value: 'interested' }],
        commitmentsAndNextSteps: [
          { kind: 'next_step', value: 'Schedule walkthrough Friday' },
        ],
        openQuestions: [],
        objectionsAndRisks: [],
      },
      caveats: [],
    });
    assert.equal(action.actionType, 'schedule_walkthrough');
    assert.ok(action.rationale.includes('walkthrough'));
  });

  it('human report is service-friendly', async () => {
    const store = createMemoryStore();
    await seedCommittedInterview(
      store,
      'Owner is interested and ready. Next step: send proposal. Pain is missed nights.'
    );
    const brief = await getProspectOperatingBrief({
      companyId: 'co-toronto-cleaning',
      store,
      loadCompanySnapshot: async () => torontoSnapshot(),
      marketBriefingService: fakeMarketService(),
    });
    const text = formatOperatingBriefReport(brief);
    assert.match(text, /Prospect Operating Brief/);
    assert.match(text, /Target:/);
    assert.match(text, /Buying Signals:/);
    assert.match(text, /Risks \/ Open Questions:/);
    assert.match(text, /Next Best Manual Action:/);
    assert.match(text, /Caveats:/);
    assert.match(text, /Autonomous execution: disabled/);
    assert.match(text, /isEvidence: false/);
  });

  it('accepts relationshipInteractionId as a sole target', async () => {
    const store = createMemoryStore();
    const opts = { store };
    const started = await startRelationshipInterview(
      {
        type: 'discovery_call',
        companyId: 'co-from-interaction',
        contactId: 'contact-from-interaction',
        opportunityId: 'opp-from-interaction',
        clientId: 1,
        notes:
          'Discovery call with cleaning company owner. They are interested and ready. Main pain is inconsistent night staff. Goal is reliable coverage. Next step: send a proposal with pricing. We promised the estimate by Thursday.',
      },
      opts
    );
    await summarizeRelationshipInterview(started.interviewId, opts);
    await commitRelationshipInterview(started.interviewId, opts);

    let snapshotCalls = 0;
    const brief = await getProspectOperatingBrief({
      relationshipInteractionId: started.interviewId,
      store,
      loadCompanySnapshot: async (loadOpts) => {
        snapshotCalls += 1;
        assert.equal(loadOpts.companyId, 'co-from-interaction');
        assert.equal(loadOpts.contactId, 'contact-from-interaction');
        return torontoSnapshot({
          companyId: 'co-from-interaction',
          contactId: 'contact-from-interaction',
          prospectId: 'contact-from-interaction',
          companyName: 'From Interaction Co',
        });
      },
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(brief.ok, true);
    assert.equal(brief.target.relationshipInteractionId, started.interviewId);
    assert.equal(brief.target.companyId, 'co-from-interaction');
    assert.equal(brief.target.contactId, 'contact-from-interaction');
    assert.equal(brief.target.opportunityId, 'opp-from-interaction');
    assert.equal(brief.sections.relationshipSummary.interactionCount, 1);
    assert.deepEqual(brief.sourceRefs.relationshipInteractionIds, [
      started.interviewId,
    ]);
    assert.ok(brief.sections.buyingSignals.length >= 1);
    assert.ok(brief.sections.commitmentsAndNextSteps.length >= 1);
    assert.ok(ACTION_TYPES.includes(brief.sections.suggestedNextAction.actionType));
    assert.equal(brief.autonomousExecution, false);
    assert.ok(snapshotCalls >= 1);
    assert.ok(!brief.caveats.includes('target_not_matched_to_company_record'));
  });

  it('marks companyIntelligence unknown when interaction has no company_id', async () => {
    const store = createMemoryStore();
    const opts = { store };
    const started = await startRelationshipInterview(
      {
        type: 'discovery_call',
        // intentionally no companyId / contactId / opportunityId
        clientId: 1,
        notes:
          'Orphan discovery. Owner is interested and ready. Next step: schedule walkthrough Friday. Goal is reliable coverage.',
      },
      opts
    );
    await summarizeRelationshipInterview(started.interviewId, opts);
    await commitRelationshipInterview(started.interviewId, opts);

    let snapshotCalls = 0;
    const brief = await getProspectOperatingBrief({
      relationshipInteractionId: started.interviewId,
      store,
      loadCompanySnapshot: async () => {
        snapshotCalls += 1;
        return { found: false };
      },
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(brief.readiness.companyIntelligence, 'unknown');
    assert.ok(brief.caveats.includes('target_not_matched_to_company_record'));
    assert.equal(brief.target.companyId, null);
    assert.equal(brief.target.relationshipInteractionId, started.interviewId);
    assert.equal(brief.sections.relationshipSummary.interactionCount, 1);
    assert.ok(
      brief.sections.buyingSignals.length >= 1 ||
        brief.sections.commitmentsAndNextSteps.length >= 1
    );
    assert.ok(brief.sections.marketContext.topCtas.length >= 1);
    assert.equal(brief.sections.suggestedNextAction.actionType, 'schedule_walkthrough');
    assert.equal(brief.autonomousExecution, false);
    assert.equal(snapshotCalls, 0);
  });

  it('rejects non-committed relationshipInteractionId', async () => {
    const store = createMemoryStore();
    const opts = { store };
    const started = await startRelationshipInterview(
      {
        type: 'discovery_call',
        companyId: 'co-draft',
        notes:
          'Draft notes with enough detail. Owner interested. Next step send proposal. Pain staffing.',
      },
      opts
    );
    await summarizeRelationshipInterview(started.interviewId, opts);

    await assert.rejects(
      () =>
        getProspectOperatingBrief({
          relationshipInteractionId: started.interviewId,
          store,
          loadCompanySnapshot: async () => ({ found: false }),
          marketBriefingService: fakeMarketService(),
        }),
      (err) =>
        err instanceof ProspectOperatingBriefError &&
        err.code === 'relationship_interaction_not_committed'
    );
  });

  it('AS Cleaning brief synthesizes buying signals from raw_summary fallback', async () => {
    const AS_CLEANING_RAW =
      'Aji is the owner of AS Cleaning Co. The company is less than 6 months old, focused on commercial cleaning clients, and currently doing about ,500 in monthly recurring revenue. Aji expressed interest after the discovery call, asked for more information, and received a personalized 2-page overview for AS Cleaning Co. Need to follow up to confirm interest, clarify target client type, budget/timeline, decision process, and whether they want help generating commercial cleaning leads.';

    const store = createMemoryStore();
    // Simulate a committed interaction whose stored insights missed commercial kinds
    // (legacy heuristics) but still have the raw_summary text.
    const started = await startRelationshipInterview(
      {
        type: 'discovery_call',
        companyId: 'co-as-cleaning',
        contactId: 'aji',
        clientId: 1,
        notes: AS_CLEANING_RAW,
      },
      { store }
    );
    await summarizeRelationshipInterview(started.interviewId, { store });
    await commitRelationshipInterview(started.interviewId, { store });

    // Strip commercial kinds from stored insights to force brief fallback synthesis.
    const row = await store.getInteraction(started.interviewId);
    const legacyInsights = (await store.listInsights(started.interviewId))
      .filter((i) =>
        ['decision_maker', 'context'].includes(i.kind)
      )
      .map((i) => ({
        kind: i.kind,
        label: i.label,
        value: i.value,
        confidence: i.confidence,
        sourceQuote: i.source_quote,
      }));
    assert.ok(row.raw_summary && String(row.raw_summary).includes('expressed interest'));
    await store.replaceInsights(started.interviewId, legacyInsights);

    const brief = await getProspectOperatingBrief({
      relationshipInteractionId: started.interviewId,
      store,
      loadCompanySnapshot: async () =>
        torontoSnapshot({
          companyId: 'co-as-cleaning',
          companyName: 'AS Cleaning Co.',
          contactName: 'Aji',
        }),
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(brief.ok, true);
    assert.ok(brief.sections.buyingSignals.length >= 1);
    assert.ok(
      brief.sections.buyingSignals.some((i) =>
        /expressed interest|asked for more info/i.test(String(i.value || ''))
      )
    );
    assert.ok(brief.sections.commitmentsAndNextSteps.length >= 1);
    assert.ok(
      brief.sections.commitmentsAndNextSteps.some((i) =>
        /overview|follow-up|follow up/i.test(String(i.value || ''))
      )
    );
    assert.ok(
      brief.sections.openQuestions.some((i) =>
        /budget|timeline|decision process|lead-gen|client type|help generating/i.test(
          String(i.value || '')
        )
      )
    );
    assert.ok(
      brief.sections.decisionProcess.some((i) => /owner|aji/i.test(String(i.value || ''))) ||
        brief.sections.painsAndGoals.some((i) => /commercial cleaning/i.test(String(i.value || '')))
    );
    assert.equal(
      brief.sections.relationshipSummary.rawSummaryFallbackApplied,
      true
    );
    assert.ok(
      brief.caveats.some((c) => c.includes('relationship_raw_summary_fallback'))
    );
    // Stored raw_summary / observations remain the source text — brief does not rewrite them.
    const stored = await store.getInteraction(started.interviewId);
    assert.equal(String(stored.raw_summary).trim(), AS_CLEANING_RAW.trim());
  });

  it('AS Cleaning email-thread proposal stage yields high-priority seller next action', async () => {
    const AS_CLEANING_EMAIL_THREAD =
      'Follow-up / proposal review with Aji at AS Cleaning Co. Aji reviewed the proposal and asked detailed buying questions before moving forward. He liked the 30-day pilot idea and had final questions before moving forward. Next steps: send service agreement and schedule kickoff. Also awaiting his reply on one timing question.';

    const store = createMemoryStore();
    const started = await startRelationshipInterview(
      {
        type: 'proposal_review',
        companyId: 'co-as-cleaning',
        contactId: 'aji',
        clientId: 1,
        notes: AS_CLEANING_EMAIL_THREAD,
      },
      { store }
    );
    await summarizeRelationshipInterview(started.interviewId, { store });
    await commitRelationshipInterview(started.interviewId, { store });

    const brief = await getProspectOperatingBrief({
      relationshipInteractionId: started.interviewId,
      store,
      loadCompanySnapshot: async () =>
        torontoSnapshot({
          companyId: 'co-as-cleaning',
          companyName: 'AS Cleaning Co.',
          contactName: 'Aji',
        }),
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(brief.ok, true);
    assert.equal(brief.target.relationshipInteractionId, started.interviewId);

    const buyingValues = brief.sections.buyingSignals.map((i) =>
      String(i.value || '').toLowerCase()
    );
    assert.ok(buyingValues.some((v) => v.includes('reviewed') && v.includes('proposal')));
    assert.ok(
      buyingValues.some((v) => v.includes('detailed buying questions') || v.includes('before moving forward'))
    );
    assert.ok(buyingValues.some((v) => v.includes('30-day pilot') || v.includes('pilot idea')));
    assert.ok(
      buyingValues.some((v) => v.includes('final questions') || v.includes('before moving forward'))
    );

    const nextValues = brief.sections.commitmentsAndNextSteps.map((i) =>
      String(i.value || '').toLowerCase()
    );
    assert.ok(nextValues.some((v) => v.includes('service agreement')));
    assert.ok(nextValues.some((v) => v.includes('kickoff')));

    const action = brief.sections.suggestedNextAction;
    assert.ok(
      ['prepare_proposal', 'schedule_kickoff'].includes(action.actionType),
      `expected prepare_proposal or schedule_kickoff, got ${action.actionType}`
    );
    assert.equal(action.priority, 'high');
    assert.notEqual(action.actionType, 'wait_for_reply');
  });

  it('does not wait_for_reply when seller-side next steps remain', () => {
    const action = suggestNextAction({
      snapshot: torontoSnapshot(),
      relationship: {
        interactionCount: 1,
        buyingSignals: [
          { kind: 'buying_signal', value: 'liked the 30-day pilot idea' },
        ],
        commitmentsAndNextSteps: [
          { kind: 'commitment', value: 'send service agreement' },
          { kind: 'next_step', value: 'awaiting his reply on timing' },
        ],
        openQuestions: [],
        objectionsAndRisks: [],
      },
      caveats: [],
    });
    assert.equal(action.actionType, 'prepare_proposal');
    assert.equal(action.priority, 'high');
    assert.notEqual(action.actionType, 'wait_for_reply');
  });

  it('suggestNextAction schedules kickoff when recorded', () => {
    const action = suggestNextAction({
      snapshot: torontoSnapshot(),
      relationship: {
        interactionCount: 1,
        buyingSignals: [
          { kind: 'buying_signal', value: 'final questions before moving forward' },
        ],
        commitmentsAndNextSteps: [
          { kind: 'next_step', value: 'schedule kickoff' },
        ],
        openQuestions: [],
        objectionsAndRisks: [],
      },
      caveats: [],
    });
    assert.equal(action.actionType, 'schedule_kickoff');
    assert.equal(action.priority, 'high');
  });
});
