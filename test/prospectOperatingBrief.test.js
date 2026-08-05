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
});
