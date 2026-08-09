'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  ARTIFACT_KIND,
  PREVIEW_TITLE,
  PREVIEW_DISCLAIMER,
  SECTION_TITLES,
  CONVERSATION_STEPS,
  DEFAULT_PROOF_ASSETS,
  DEFAULT_APPROVAL_CHECKPOINTS,
  buildCampaignPlanningContext,
  buildCampaignPlanningOpening,
  buildCampaignPlanningReply,
  buildFirstCampaignPlanPreview,
  formatFirstCampaignPlanPreviewMessage,
  containsForbiddenCampaignPlanningLanguage,
  humanizeStatusLabel,
} = require('../services/clientIntelligenceCampaignPlanning');
const {
  createMemoryStore,
  startCampaignPlanningConversation,
  postCampaignPlanningMessage,
} = require('../services/clientIntelligenceInterview');

const ANCHOR_BLUEPRINT = {
  id: 'bp-anchor-campaign',
  version: '1.0',
  status: 'approved',
  sections: {
    identity: {
      summary: 'Anchor Cleaning is a commercial-focused cleaning company.',
      confidence: 0.9,
      evidenceIds: [],
      unknowns: [],
    },
    services: {
      summary: 'Recurring commercial cleaning for offices.',
      confidence: 0.9,
      evidenceIds: [],
      unknowns: [],
    },
    idealCustomers: {
      summary: 'Property managers and professional offices.',
      confidence: 0.88,
      evidenceIds: [],
      unknowns: [],
    },
    avoidCustomers: {
      summary: 'Lowest-price shoppers.',
      confidence: 0.8,
      evidenceIds: [],
      unknowns: [],
    },
    targetMarkets: {
      summary: 'Greater Manchester including Bedford and Hooksett.',
      confidence: 0.9,
      evidenceIds: [],
      unknowns: [],
    },
    competitiveAdvantages: {
      summary: 'Reliable work without chasing the team.',
      confidence: 0.85,
      evidenceIds: [],
      unknowns: [],
    },
    brandVoice: {
      summary: 'Calm, professional, reliable.',
      confidence: 0.8,
      evidenceIds: [],
      unknowns: [],
    },
    campaignGoals: {
      summary: 'Commercial cleaning growth in Greater Manchester.',
      confidence: 0.85,
      evidenceIds: [],
      unknowns: [],
    },
    successMetrics: {
      summary: 'Clearer path to commercial opportunities in 90 days.',
      confidence: 0.8,
      evidenceIds: [],
      unknowns: [],
    },
  },
};

async function seedApprovedSession(store) {
  await store.insertBlueprint({
    ...ANCHOR_BLUEPRINT,
    client_id: 10,
    interview_session_id: 'int-campaign-1',
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  });
  const session = await store.insertSession({
    id: 'int-campaign-1',
    client_id: 10,
    status: 'APPROVED',
    interview_state: {
      blueprintId: ANCHOR_BLUEPRINT.id,
      blueprintVersion: ANCHOR_BLUEPRINT.version,
      initialGrowthDirection: {
        businessName: 'Anchor Cleaning',
        primaryArea: 'Greater Manchester',
        firstFocus: 'property managers in Greater Manchester',
        segmentsToInspect: ['property managers', 'professional offices'],
      },
      segmentRanking: {
        kind: 'segment_ranking',
        rankings: [
          { segment: 'property managers', role: 'best_first' },
          { segment: 'professional offices', role: 'second_best' },
        ],
      },
      validationTarget: {
        kind: 'validation_target',
        title: 'Property Manager Validation Target',
        best_fit_subtype:
          'Multi-family / HOA property managers with recurring building needs',
        credibility_proof_needed:
          'service checklist, photos/examples, clear response-time expectation, service area, walkthrough/estimate process',
      },
      firstGrowthPlanPreview: {
        kind: 'first_growth_plan_preview',
        title: 'First Growth Plan Preview',
        businessName: 'Anchor Cleaning',
        primarySegmentDisplay: 'property managers',
        secondarySegmentDisplay: 'professional offices',
        primaryArea: 'Greater Manchester',
        first_subtype_to_test:
          'Multi-family / HOA property managers with recurring building needs',
        credibility_proof_needed:
          'service checklist, photos/examples, clear response-time expectation',
      },
      growthConversation: {
        status: 'preview_ready',
        primary_segment: 'property_managers',
        secondary_segment: 'professional_offices',
        first_growth_plan_preview: null,
      },
      growthInfrastructureReadinessReport: {
        kind: 'growth_infrastructure_readiness_report',
        overallStatus: 'partial',
        title: 'Growth Infrastructure Readiness Report',
      },
      growthWork: {
        completedTaskIds: ['growth_focus', 'infra_report', 'setup_domain'],
        history: [],
      },
    },
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  });
  return session;
}

describe('First Campaign Planning domain (SPEC-089)', () => {
  it('defines conversation steps through preview', () => {
    assert.ok(CONVERSATION_STEPS.includes('campaign_objective'));
    assert.ok(CONVERSATION_STEPS.includes('hypothesis'));
    assert.ok(CONVERSATION_STEPS.includes('approval_checkpoints'));
    assert.ok(CONVERSATION_STEPS.includes('preview'));
  });

  it('opening carries approved focus and states planning-not-launch', () => {
    const ctx = buildCampaignPlanningContext(
      {
        interview_state: {
          initialGrowthDirection: {
            businessName: 'Anchor Cleaning',
            primaryArea: 'Greater Manchester',
          },
          firstGrowthPlanPreview: {
            businessName: 'Anchor Cleaning',
            primarySegmentDisplay: 'property managers',
            secondarySegmentDisplay: 'professional offices',
            primaryArea: 'Greater Manchester',
          },
        },
      },
      ANCHOR_BLUEPRINT
    );
    const opening = buildCampaignPlanningOpening(ctx);
    assert.match(opening, /Anchor Cleaning is ready to plan the first campaign/i);
    assert.match(opening, /review-first/i);
    assert.match(opening, /no prospect list/i);
    assert.match(opening, /outreach copy/i);
    assert.match(opening, /launch steps yet/i);
    assert.match(opening, /property managers in Greater Manchester/i);
    assert.match(opening, /professional offices as a secondary path/i);
    assert.match(opening, /campaign hypothesis/i);
    assert.match(
      opening,
      /plan around property managers exactly as defined|narrow the first test/i
    );
    assert.doesNotMatch(opening, /what is your business name/i);
    assert.doesNotMatch(opening, /tell me about your services/i);
  });

  it('reuses prior artifacts instead of re-interviewing', () => {
    const session = {
      interview_state: {
        initialGrowthDirection: { businessName: 'Anchor Cleaning' },
        segmentRanking: { kind: 'segment_ranking' },
        validationTarget: {
          title: 'Property Manager Validation Target',
          best_fit_subtype: 'HOA managers',
        },
        firstGrowthPlanPreview: {
          primarySegmentDisplay: 'property managers',
          secondarySegmentDisplay: 'professional offices',
          primaryArea: 'Greater Manchester',
        },
        growthInfrastructureReadinessReport: { overallStatus: 'partial' },
        growthWork: { completedTaskIds: ['a', 'b'] },
      },
    };
    const ctx = buildCampaignPlanningContext(session, ANCHOR_BLUEPRINT);
    assert.equal(ctx.businessName, 'Anchor Cleaning');
    assert.equal(ctx.primarySegment, 'property managers');
    assert.equal(ctx.secondarySegment, 'professional offices');
    assert.equal(ctx.targetMarket, 'Greater Manchester');
    assert.ok(ctx.segmentRanking);
    assert.ok(ctx.validationTarget);
    assert.ok(ctx.firstGrowthPlanPreview);
    assert.ok(ctx.readinessReport);
    assert.equal(ctx.completedSetupChecklist, true);
  });

  it('builds First Campaign Plan Preview with preferred polished shape', () => {
    const ctx = buildCampaignPlanningContext(
      {
        interview_state: {
          firstGrowthPlanPreview: {
            businessName: 'Anchor Cleaning',
            primarySegmentDisplay: 'property managers',
            secondarySegmentDisplay: 'professional offices',
            primaryArea: 'Greater Manchester',
          },
          initialGrowthDirection: {
            businessName: 'Anchor Cleaning',
            primaryArea: 'Greater Manchester',
            towns: ['Bedford', 'Hooksett', 'Londonderry', 'Auburn', 'Goffstown'],
          },
          growthInfrastructureReadinessReport: { overallStatus: 'not_ready' },
          growthWork: { completedTaskIds: ['t1'] },
        },
      },
      ANCHOR_BLUEPRINT
    );
    const answers = {
      opening: { raw: 'Keep property managers as defined.' },
      campaign_objective: {
        raw: 'Prove property managers will book walkthroughs.',
      },
      target_segment: {
        raw: 'Property managers — multi-family / HOA subtype',
      },
      market_bounds: { raw: 'Greater Manchester' },
      proof_assets: {
        raw: 'Checklist and photos ready; still need a reference.',
      },
      hypothesis: {
        raw: 'If we approach local property managers in Greater Manchester in Greater Manchester with proof, we expect walkthroughs.',
      },
      validation_metrics: {
        raw: '3 conversations',
      },
      approval_checkpoints: {
        raw: 'Preview sign-off',
      },
    };
    const preview = buildFirstCampaignPlanPreview(ctx, answers, {
      blueprintId: ANCHOR_BLUEPRINT.id,
      blueprintVersion: ANCHOR_BLUEPRINT.version,
    });
    assert.equal(preview.kind, ARTIFACT_KIND);
    assert.equal(preview.title, PREVIEW_TITLE);
    assert.equal(preview.sectionTitles.hypothesis, 'Campaign hypothesis');
    assert.equal(preview.sectionTitles.risksCautions, 'Risks and cautions');
    assert.match(preview.campaignObjective, /Core question:/i);
    assert.match(preview.campaignObjective, /walkthrough/i);
    assert.match(
      preview.targetSegment,
      /Small to mid-sized local property managers in Greater Manchester/i
    );
    assert.doesNotMatch(preview.targetSegment, /—/);
    assert.match(preview.targetSegmentAvoid, /Avoid large institutional/i);
    assert.match(preview.marketBound, /early attention on Bedford/i);
    assert.match(preview.hypothesis, /local property managers/i);
    assert.doesNotMatch(
      preview.hypothesis,
      /Greater Manchester in Greater Manchester/i
    );
    assert.deepEqual(preview.proofAssetsNeeded, [...DEFAULT_PROOF_ASSETS]);
    assert.ok(preview.validationMetrics.includes('Qualified replies'));
    assert.ok(
      preview.risksCautions.some((r) =>
        /growth infrastructure items may still need review/i.test(r)
      )
    );
    assert.ok(
      preview.risksCautions.every((r) => !/\bnot_ready\b/.test(r))
    );
    assert.deepEqual(
      preview.approvalCheckpoints,
      [...DEFAULT_APPROVAL_CHECKPOINTS]
    );
    assert.match(
      preview.recommendedNextStep,
      /Review and approve the campaign plan preview/i
    );
    assert.match(
      preview.recommendedNextStep,
      /prospect-list criteria before any list is built/i
    );
    assert.equal(preview.planningOnly, true);
    assert.equal(preview.campaignsGenerated, false);
    assert.equal(preview.prospectListGenerated, false);
    assert.equal(preview.outreachCopyGenerated, false);
    assert.equal(preview.accountChangesMade, false);
    assert.equal(preview.disclaimer, PREVIEW_DISCLAIMER);
    assert.equal(humanizeStatusLabel('not_ready'), 'not ready');
    assert.equal(
      preview.context.readinessOverallStatusLabel,
      'not ready'
    );

    const formatted = formatFirstCampaignPlanPreviewMessage(preview);
    assert.match(formatted, /1\. Campaign objective/);
    assert.match(formatted, /2\. Target segment/);
    assert.match(formatted, /4\. Campaign hypothesis/);
    assert.match(formatted, /7\. Risks and cautions/);
    assert.match(formatted, /Planning preview only\./);
    assert.equal(
      (formatted.match(/Planning preview only/gi) || []).length,
      1
    );
    assert.doesNotMatch(formatted, /This stays planning-only/i);
    assert.doesNotMatch(formatted, /Planning only — not a launch/i);
    assert.doesNotMatch(formatted, /\bnot_ready\b/);
    assert.doesNotMatch(formatted, /campaign is live/i);
  });

  it('advances steps then produces preview without forbidden launch language', () => {
    const ctx = {
      businessName: 'Anchor Cleaning',
      primarySegment: 'property managers',
      secondarySegment: 'professional offices',
      targetMarket: 'Greater Manchester',
      towns: ['Bedford', 'Hooksett', 'Londonderry', 'Auburn', 'Goffstown'],
      completedSetupChecklist: true,
      readinessOverallStatus: 'partial',
      blueprintId: 'bp-1',
      blueprintVersion: '1.0',
    };
    let state = { step: 'opening', answers: {} };
    const r1 = buildCampaignPlanningReply(
      'Keep property managers exactly as defined.',
      state,
      ctx
    );
    assert.equal(r1.step, 'campaign_objective');
    assert.equal(r1.intent, 'advance');
    assert.equal(r1.preview, null);
    assert.equal(containsForbiddenCampaignPlanningLanguage(r1.message), false);

    state = { step: r1.step, answers: r1.answers };
    const steps = [
      'Prove walkthrough demand.',
      'Property managers — HOA subtype',
      'Greater Manchester',
      'Checklist and photos available',
      'If we approach HOA PMs with proof, we get walkthroughs',
      '2 conversations and 1 walkthrough',
    ];
    for (const msg of steps) {
      const reply = buildCampaignPlanningReply(msg, state, ctx);
      assert.equal(containsForbiddenCampaignPlanningLanguage(reply.message), false);
      state = { step: reply.step, answers: reply.answers };
    }
    const final = buildCampaignPlanningReply(
      'Preview sign-off before any list or copy',
      state,
      ctx
    );
    assert.equal(final.intent, 'produce_preview');
    assert.ok(final.preview);
    assert.equal(final.preview.kind, ARTIFACT_KIND);
    assert.match(final.message, /Planning preview only\./i);
    assert.equal(
      (final.message.match(/Planning preview only/gi) || []).length,
      1
    );
    assert.doesNotMatch(final.message, /This stays planning-only/i);
    assert.equal(containsForbiddenCampaignPlanningLanguage(final.message), false);
    assert.equal(
      containsForbiddenCampaignPlanningLanguage('I built a prospect list'),
      true
    );
  });
});

describe('First Campaign Planning session APIs', () => {
  it('starts conversation from approved session using prior artifacts', async () => {
    const store = createMemoryStore();
    await seedApprovedSession(store);
    const started = await startCampaignPlanningConversation('int-campaign-1', {
      store,
    });
    assert.equal(started.ok, true);
    assert.equal(started.status, 'CAMPAIGN_PLANNING');
    assert.match(started.message, /review-first/i);
    assert.match(started.message, /property managers in Greater Manchester/i);
    assert.equal(started.campaignContext.completedSetupChecklist, true);
    assert.ok(started.campaignPlanning.turns.length >= 1);
  });

  it('completes flow to First Campaign Plan Preview without mutations', async () => {
    const store = createMemoryStore();
    await seedApprovedSession(store);
    await startCampaignPlanningConversation('int-campaign-1', { store });

    const answers = [
      'Keep property managers as defined.',
      'Prove PMs will request walkthroughs.',
      'Property managers — multi-family HOA',
      'Greater Manchester',
      'Checklist ready; need one reference',
      'If we approach HOA PMs in Greater Manchester with a checklist, we expect walkthrough requests.',
      '3 conversations; 1 walkthrough',
      'Preview approval; proof ready; no list until signed off',
    ];
    let last = null;
    for (const msg of answers) {
      last = await postCampaignPlanningMessage('int-campaign-1', msg, { store });
    }
    assert.ok(last);
    assert.equal(last.status, 'CAMPAIGN_PLANNING');
    assert.ok(last.firstCampaignPlanPreview);
    assert.equal(last.firstCampaignPlanPreview.kind, ARTIFACT_KIND);
    assert.equal(last.firstCampaignPlanPreview.campaignsGenerated, false);
    assert.equal(last.firstCampaignPlanPreview.prospectListGenerated, false);
    assert.equal(last.firstCampaignPlanPreview.outreachCopyGenerated, false);
    assert.equal(last.firstCampaignPlanPreview.accountChangesMade, false);
    assert.equal(last.campaignPlanning.status, 'preview_ready');

    const session = await store.getSession('int-campaign-1');
    assert.ok(session.interview_state.campaignPlanning);
    assert.ok(session.interview_state.firstCampaignPlanPreview);
    // No CRM / prospect writes — only interview_state keys.
    assert.equal(session.status, 'APPROVED');
  });

  it('rejects unapproved sessions', async () => {
    const store = createMemoryStore();
    await store.insertSession({
      id: 'int-draft',
      client_id: 10,
      status: 'CLIENT_REVIEW',
      interview_state: {},
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    });
    await assert.rejects(
      () => startCampaignPlanningConversation('int-draft', { store }),
      (err) => err && err.code === 'invalid_status'
    );
  });
});

describe('First Campaign Planning UI markers', () => {
  it('wires Plan First Campaign to campaign start and preview panel', () => {
    const uiPath = path.join(__dirname, '..', 'public', 'client-intel.html');
    const uiSource = fs.readFileSync(uiPath, 'utf8');
    assert.match(uiSource, /Plan First Campaign/);
    assert.match(uiSource, /launch_campaign/);
    assert.match(uiSource, /startCampaignPlanningConversation/);
    assert.match(uiSource, /\/campaign\/start/);
    assert.match(uiSource, /\/campaign\/message/);
    assert.match(uiSource, /campaign_planning/);
    assert.match(uiSource, /renderFirstCampaignPlanPreview/);
    assert.match(uiSource, /First Campaign Plan Preview/);
    assert.match(uiSource, /Campaign hypothesis/);
    assert.match(uiSource, /Risks and cautions/);
    assert.doesNotMatch(uiSource, /Planning only — not a launch/);
  });
});
