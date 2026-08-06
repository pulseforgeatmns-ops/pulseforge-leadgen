'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  ACTION_TYPES,
  formatOperatorLoopReport,
  getServiceModeOperatorLoop,
  isPlaceholderNotes,
  isReadinessFixture,
  mapBriefActionType,
} = require('../services/serviceModeOperatorLoop');
const {
  createMemoryStore,
  startRelationshipInterview,
  summarizeRelationshipInterview,
  commitRelationshipInterview,
  listInteractions,
} = require('../services/relationshipIntelligenceInterview');

function fakeMarketService() {
  return {
    async getTopCtas() {
      return [];
    },
    async getTopOffers() {
      return [];
    },
    async getMessagingThemes() {
      return { items: [] };
    },
    async getCorpusSummary() {
      return {
        emailCount: 0,
        companyCount: 0,
        observationCount: 0,
        readinessStatus: 'partial',
      };
    },
  };
}

async function seedCommittedInterview(store, notes, context = {}) {
  const opts = { store };
  const started = await startRelationshipInterview(
    {
      type: context.type || 'discovery_call',
      companyId: context.companyId,
      contactId: context.contactId,
      opportunityId: context.opportunityId,
      clientId: context.clientId || 1,
      notes,
      source: context.source,
    },
    opts
  );
  await summarizeRelationshipInterview(started.interviewId, opts);
  await commitRelationshipInterview(started.interviewId, opts);
  return started.interviewId;
}

describe('SPEC-075 serviceModeOperatorLoop', () => {
  it('returns empty queue with caveats when no candidates exist', async () => {
    const store = createMemoryStore();
    const loop = await getServiceModeOperatorLoop({
      store,
      days: 14,
      limit: 10,
      includeMarketContext: false,
      loadCompanySnapshot: async () => ({ found: false }),
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(loop.ok, true);
    assert.equal(loop.kind, 'service_mode_operator_loop');
    assert.equal(loop.isEvidence, false);
    assert.equal(loop.autonomousExecution, false);
    assert.deepEqual(loop.actions, []);
    assert.ok(loop.caveats.some((c) => c.includes('no_operator_candidates')));
    assert.equal(loop.summary.actionsReturned, 0);
  });

  it('scans committed Relationship Intelligence only', async () => {
    const store = createMemoryStore();
    const opts = { store };

    const draft = await startRelationshipInterview(
      {
        type: 'discovery_call',
        companyId: 'co-draft',
        contactId: 'c-draft',
        clientId: 1,
        notes:
          'Draft only. Owner is interested and ready. Next step: send proposal. Pain is missed night cleaning.',
      },
      opts
    );
    await summarizeRelationshipInterview(draft.interviewId, opts);

    const committedId = await seedCommittedInterview(
      store,
      'Committed discovery. Owner is interested and ready to move. Goal is consistent coverage. Next step: send a proposal with pricing. We promised the estimate by Thursday.',
      { companyId: 'co-committed', contactId: 'c-committed' }
    );

    const listed = await listInteractions({ status: 'committed' }, opts);
    assert.equal(listed.length, 1);
    assert.equal(listed[0].id, committedId);

    const loop = await getServiceModeOperatorLoop({
      store,
      days: 14,
      limit: 10,
      includeMarketContext: false,
      loadCompanySnapshot: async () => ({
        found: true,
        companyId: 'co-committed',
        companyName: 'Committed Co',
        contactName: 'Pat',
        doNotContact: false,
      }),
      marketBriefingService: fakeMarketService(),
    });

    assert.ok(loop.actions.length >= 1);
    const ids = loop.actions.flatMap(
      (a) => a.sourceRefs.relationshipInteractionIds || []
    );
    assert.ok(ids.includes(committedId));
    assert.ok(!ids.includes(draft.interviewId));
  });

  it('ignores placeholder notes', async () => {
    assert.equal(isPlaceholderNotes('Paste notes here'), true);
    assert.equal(isPlaceholderNotes('Paste notes here.'), true);

    const store = createMemoryStore();
    await seedCommittedInterview(store, 'Paste notes here', {
      companyId: 'co-placeholder',
      contactId: 'c-ph',
    });

    const loop = await getServiceModeOperatorLoop({
      store,
      days: 14,
      includeMarketContext: false,
      loadCompanySnapshot: async () => ({ found: false }),
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(loop.actions.length, 0);
    assert.ok(loop.caveats.some((c) => c.includes('skipped_placeholder_notes')));
  });

  it('ignores readiness fixtures', async () => {
    assert.equal(
      isReadinessFixture({
        source: 'readiness_acceptance',
        rawSummary: 'anything',
      }),
      true
    );

    const store = createMemoryStore();
    await seedCommittedInterview(
      store,
      'Readiness acceptance fixture. Discovery-style debrief with the office manager. Main pain is inconsistent night cleaning. Goal is reliable coverage. Next step is a walkthrough Friday.',
      {
        companyId: 'co-ready',
        contactId: 'c-ready',
        source: 'readiness_acceptance',
      }
    );

    const loop = await getServiceModeOperatorLoop({
      store,
      days: 14,
      includeMarketContext: false,
      loadCompanySnapshot: async () => ({ found: false }),
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(loop.actions.length, 0);
    assert.ok(loop.caveats.some((c) => c.includes('skipped_readiness_fixtures')));
  });

  it('de-dupes repeated raw summaries', async () => {
    const store = createMemoryStore();
    const notes =
      'Owner is interested and ready. Next step: send a proposal with pricing. Pain is inconsistent night staff. Goal is reliable coverage.';
    await seedCommittedInterview(store, notes, {
      companyId: 'co-dup-1',
      contactId: 'c-1',
    });
    await seedCommittedInterview(store, notes, {
      companyId: 'co-dup-2',
      contactId: 'c-2',
    });

    let briefCalls = 0;
    const loop = await getServiceModeOperatorLoop({
      store,
      days: 14,
      limit: 10,
      includeMarketContext: false,
      loadCompanySnapshot: async () => ({
        found: true,
        companyName: 'Dup Co',
        doNotContact: false,
      }),
      marketBriefingService: fakeMarketService(),
      getProspectOperatingBrief: async (opts) => {
        briefCalls += 1;
        return {
          ok: true,
          kind: 'prospect_operating_brief',
          isEvidence: false,
          target: {
            companyId: opts.companyId || null,
            prospectId: null,
            opportunityId: null,
            contactId: null,
            relationshipInteractionId: opts.relationshipInteractionId,
            companyName: 'Dup Co',
            contactName: null,
          },
          sections: {
            buyingSignals: [{ kind: 'buying_signal', value: 'interested' }],
            commitmentsAndNextSteps: [
              { kind: 'next_step', value: 'send a proposal' },
            ],
            openQuestions: [],
            objectionsAndRisks: [],
            suggestedNextAction: {
              actionType: 'prepare_proposal',
              priority: 'medium',
              rationale: 'Proposal next step recorded.',
              suggestedMessageAngle: 'Send proposal',
              requiredInputs: [],
              cautions: [],
            },
          },
          sourceRefs: {
            relationshipInteractionIds: [opts.relationshipInteractionId],
            relationshipInsightIds: [],
            marketObservationIds: [],
          },
          caveats: [],
          autonomousExecution: false,
        };
      },
    });

    assert.equal(briefCalls, 1);
    assert.equal(
      loop.actions.filter((a) => a.actionType !== 'link_crm_record').length,
      1
    );
    assert.ok(loop.caveats.some((c) => c.includes('deduped_raw_summaries')));
  });

  it('uses Prospect Operating Brief service', async () => {
    const store = createMemoryStore();
    const id = await seedCommittedInterview(
      store,
      'Owner is interested. Next step: send follow-up with proposal timing. Pain is staffing.',
      { companyId: 'co-brief', contactId: 'c-brief' }
    );

    let called = false;
    const loop = await getServiceModeOperatorLoop({
      store,
      days: 14,
      includeMarketContext: false,
      getProspectOperatingBrief: async (opts) => {
        called = true;
        assert.equal(opts.relationshipInteractionId, id);
        return {
          ok: true,
          kind: 'prospect_operating_brief',
          isEvidence: false,
          target: {
            companyId: 'co-brief',
            prospectId: null,
            opportunityId: null,
            contactId: 'c-brief',
            relationshipInteractionId: id,
            companyName: 'Brief Co',
            contactName: 'Sam',
          },
          sections: {
            buyingSignals: [{ kind: 'buying_signal', value: 'interested' }],
            commitmentsAndNextSteps: [
              { kind: 'next_step', value: 'send follow-up' },
            ],
            openQuestions: [],
            objectionsAndRisks: [],
            suggestedNextAction: {
              actionType: 'send_follow_up',
              priority: 'high',
              rationale: 'Buying signal plus follow-up next step.',
              suggestedMessageAngle: 'Reference interest',
              requiredInputs: ['email'],
              cautions: ['Manual only'],
            },
          },
          sourceRefs: {
            relationshipInteractionIds: [id],
            relationshipInsightIds: ['ins-1'],
            marketObservationIds: [],
          },
          caveats: [],
          autonomousExecution: false,
        };
      },
    });

    assert.equal(called, true);
    assert.equal(loop.actions[0].actionType, 'send_follow_up');
    assert.equal(loop.actions[0].priority, 'high');
    assert.deepEqual(loop.actions[0].sourceRefs.relationshipInsightIds, ['ins-1']);
    assert.equal(loop.actions[0].sourceRefs.prospectBriefId, null);
    assert.equal(loop.actions[0].autonomousExecution, false);
  });

  it('AS Cleaning follow-up produces high-priority prepare_service_agreement or schedule_kickoff', async () => {
    const AS_CLEANING_EMAIL_THREAD =
      'Follow-up / proposal review with Aji at AS Cleaning Co. Aji reviewed the proposal and asked detailed buying questions before moving forward. He liked the 30-day pilot idea and had final questions before moving forward. Next steps: send service agreement and schedule kickoff. Also awaiting his reply on one timing question.';

    const store = createMemoryStore();
    const id = await seedCommittedInterview(store, AS_CLEANING_EMAIL_THREAD, {
      type: 'proposal_review',
      companyId: 'co-as-cleaning',
      contactId: 'aji',
    });

    const loop = await getServiceModeOperatorLoop({
      store,
      relationshipInteractionId: id,
      days: 14,
      includeMarketContext: false,
      loadCompanySnapshot: async () => ({
        found: true,
        companyId: 'co-as-cleaning',
        companyName: 'AS Cleaning Co.',
        contactName: 'Aji',
        contactId: 'aji',
        doNotContact: false,
      }),
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(loop.ok, true);
    assert.ok(loop.actions.length >= 1);
    const primary = loop.actions.find((a) => a.actionType !== 'link_crm_record');
    assert.ok(primary);
    assert.ok(
      ['prepare_service_agreement', 'schedule_kickoff'].includes(primary.actionType),
      `expected prepare_service_agreement or schedule_kickoff, got ${primary.actionType}`
    );
    assert.equal(primary.priority, 'high');
    assert.match(primary.title, /AS Cleaning/);
    assert.ok(primary.rationale && primary.rationale.length > 10);
    assert.ok(primary.sourceRefs.relationshipInteractionIds.includes(id));
    assert.equal(primary.autonomousExecution, false);
  });

  it('target_not_matched_to_company_record surfaces as caveat/manual need', async () => {
    const store = createMemoryStore();
    const id = await seedCommittedInterview(
      store,
      'Orphan discovery. Owner is interested and ready. Next step: send service agreement and schedule kickoff. Liked the 30-day pilot. Final questions before moving forward.',
      {
        // intentionally no companyId
        contactId: null,
      }
    );

    const loop = await getServiceModeOperatorLoop({
      store,
      relationshipInteractionId: id,
      days: 14,
      includeMarketContext: false,
      loadCompanySnapshot: async () => ({ found: false }),
      marketBriefingService: fakeMarketService(),
    });

    assert.ok(
      loop.caveats.includes('target_not_matched_to_company_record') ||
        loop.actions.some((a) =>
          (a.caveats || []).includes('target_not_matched_to_company_record')
        )
    );
    assert.ok(loop.actions.some((a) => a.actionType === 'link_crm_record'));
  });

  it('maps prepare_proposal + service agreement language to prepare_service_agreement', () => {
    const mapped = mapBriefActionType({
      sections: {
        buyingSignals: [],
        commitmentsAndNextSteps: [
          { kind: 'next_step', value: 'send service agreement' },
        ],
        openQuestions: [],
        objectionsAndRisks: [],
        suggestedNextAction: {
          actionType: 'prepare_proposal',
          rationale: 'Service agreement next step',
        },
      },
      caveats: [],
    });
    assert.equal(mapped, 'prepare_service_agreement');
    assert.ok(ACTION_TYPES.includes(mapped));
  });

  it('performs no CRM writes and no outbound sends', async () => {
    const store = createMemoryStore();
    await seedCommittedInterview(
      store,
      'Owner interested. Next step send proposal. Pain staffing. Goal coverage.',
      { companyId: 'co-safe', contactId: 'c-safe' }
    );

    const queries = [];
    const fakePool = {
      async query(sql) {
        queries.push(String(sql));
        return { rows: [] };
      },
    };

    const loop = await getServiceModeOperatorLoop({
      pool: fakePool,
      store,
      days: 14,
      includeMarketContext: false,
      loadCompanySnapshot: async () => ({
        found: true,
        companyId: 'co-safe',
        companyName: 'Safe Co',
        doNotContact: false,
      }),
      marketBriefingService: fakeMarketService(),
    });

    assert.equal(loop.autonomousExecution, false);
    assert.ok(loop.actions.every((a) => a.autonomousExecution === false));
    assert.equal(
      queries.every((sql) => !/\b(INSERT|UPDATE|DELETE|ALTER)\b/i.test(sql)),
      true
    );
    assert.equal(Object.prototype.hasOwnProperty.call(loop, 'execute'), false);
  });

  it('human report groups priorities and denies autonomy', async () => {
    const text = formatOperatorLoopReport({
      generatedAt: '2026-08-04T12:00:00.000Z',
      isEvidence: false,
      window: { days: 14 },
      summary: {
        candidatesScanned: 1,
        actionsReturned: 1,
        highPriorityCount: 1,
        caveatCount: 0,
      },
      actions: [
        {
          title: 'AS Cleaning Co. — Prepare service agreement',
          priority: 'high',
          rationale: 'Aji liked the 30-day pilot and asked final questions.',
          suggestedManualStep:
            'Prepare/send service agreement, then schedule kickoff after acceptance.',
          caveats: ['target_not_matched_to_company_record'],
          target: { relationshipInteractionId: 'ri-1' },
        },
      ],
      caveats: [],
    });

    assert.match(text, /Service Mode Operator Loop/);
    assert.match(text, /High Priority:/);
    assert.match(text, /AS Cleaning Co\. — Prepare service agreement/);
    assert.match(text, /Why:/);
    assert.match(text, /Manual step:/);
    assert.match(text, /Caveats: target_not_matched_to_company_record/);
    assert.match(text, /No autonomous execution performed\./);
  });
});
