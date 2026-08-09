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
  DEFAULT_PROOF_ASSETS_AVAILABLE,
  DEFAULT_PROOF_ASSETS_MISSING,
  DEFAULT_INCLUSION_CRITERIA,
  DEFAULT_APPROVAL_BEFORE_LIST,
  DEFAULT_APPROVAL_BEFORE_LAUNCH,
  VALIDATION_SUCCESS_STATEMENT,
  buildCampaignPlanningContext,
  buildCampaignPlanningOpening,
  buildCampaignPlanningReply,
  buildFirstCampaignPlanPreview,
  formatFirstCampaignPlanPreviewMessage,
  extractCampaignPlanFields,
  stripCampaignWrappers,
  extractAvoidPhrase,
  containsForbiddenCampaignPlanningLanguage,
  humanizeStatusLabel,
  sanitizeTargetSegmentText,
  stripFirstPersonArtifactLanguage,
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
      summary:
        'the business prefers to avoid Anchor should avoid buyers focused only on the lowest price',
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
    assert.equal(preview.sectionTitles.proofAssets, 'Proof assets');
    assert.match(preview.campaignObjective, /Core validation question:/i);
    assert.doesNotMatch(preview.campaignObjective, /Core question:/i);
    assert.doesNotMatch(preview.campaignObjective, /\bI'd\b/);
    assert.match(
      preview.campaignObjective,
      /^Prove that property managers will book walkthroughs\./i
    );
    assert.doesNotMatch(
      preview.campaignObjective,
      /Prove that Prove|The first campaign should prove/i
    );
    assert.doesNotMatch(
      preview.campaignObjective,
      /Include property managers who/i
    );
    assert.match(
      preview.targetSegment,
      /^Small to mid-sized local property managers in Greater Manchester who oversee/i
    );
    assert.doesNotMatch(preview.targetSegment, /^property managers/i);
    assert.doesNotMatch(preview.targetSegment, /—/);
    assert.equal(preview.targetSegmentAvoid, null);
    assert.deepEqual(preview.inclusionCriteria, [...DEFAULT_INCLUSION_CRITERIA]);
    assert.ok(
      preview.exclusionCriteria.some((x) => /lowest price/i.test(x))
    );
    assert.doesNotMatch(
      preview.exclusionCriteria.join(' '),
      /prefers to avoid|should avoid/i
    );
    assert.match(preview.marketBound, /^Start with Bedford/i);
    assert.match(preview.marketBound, /tight enough to learn quickly/i);
    assert.match(
      preview.hypothesis,
      /^If Anchor approaches small to mid-sized local property managers/i
    );
    assert.doesNotMatch(
      preview.hypothesis,
      /Greater Manchester in Greater Manchester/i
    );
    assert.doesNotMatch(preview.hypothesis, /Primary signals|If we approach/i);
    assert.deepEqual(
      preview.proofAssetsAvailable,
      [...DEFAULT_PROOF_ASSETS_AVAILABLE]
    );
    assert.deepEqual(
      preview.proofAssetsMissing,
      [...DEFAULT_PROOF_ASSETS_MISSING]
    );
    assert.ok(
      preview.validationMetricsPrimary.some((m) =>
        /Qualified replies from property managers/i.test(m)
      )
    );
    assert.ok(preview.validationMetricsSecondary.length >= 2);
    assert.equal(
      preview.validationSuccessStatement,
      VALIDATION_SUCCESS_STATEMENT
    );
    assert.ok(
      preview.risksCautions.some((r) =>
        /growth infrastructure items may still need review/i.test(r)
      )
    );
    assert.ok(
      preview.risksCautions.every((r) => !/\bnot_ready\b/.test(r))
    );
    assert.deepEqual(
      preview.approvalCheckpointsBeforeListBuilding,
      [...DEFAULT_APPROVAL_BEFORE_LIST]
    );
    assert.deepEqual(
      preview.approvalCheckpointsBeforeLaunch,
      [...DEFAULT_APPROVAL_BEFORE_LAUNCH]
    );
    assert.match(
      preview.recommendedNextStep,
      /Review and approve this preview/i
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
    assert.match(formatted, /Include property managers who:/);
    assert.match(formatted, /Exclude property managers who:/);
    assert.match(formatted, /4\. Campaign hypothesis/);
    assert.match(formatted, /5\. Proof assets/);
    assert.match(formatted, /Available or close to ready:/);
    assert.match(formatted, /Still needed or should be packaged:/);
    assert.match(formatted, /Primary signals:/);
    assert.match(formatted, /Secondary signals:/);
    assert.match(formatted, /Before list-building:/);
    assert.match(formatted, /Before launch:/);
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

  it('Anchor demo answers normalize without raw transcript stitching', () => {
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
          growthInfrastructureReadinessReport: { overallStatus: 'partial' },
          growthWork: { completedTaskIds: ['growth_focus', 'infra_report'] },
        },
      },
      ANCHOR_BLUEPRINT
    );

    assert.equal(
      extractAvoidPhrase(ANCHOR_BLUEPRINT.sections.avoidCustomers.summary),
      'buyers focused only on the lowest price'
    );
    assert.equal(
      stripCampaignWrappers(
        'Prove that The first campaign should prove that small PMs will engage'
      ),
      'small PMs will engage'
    );

    const answers = {
      opening: { raw: 'Keep property managers as defined.' },
      campaign_objective: {
        raw: [
          'The first campaign should prove that small to mid-sized property managers in Greater Manchester will engage in qualified conversations about recurring cleaning.',
          '',
          'Include property managers who:',
          '- Manage offices, mixed-use buildings, small commercial properties, or multi-tenant spaces',
          'Exclude property managers who:',
          '- Focus only on the lowest price',
          '',
          'Core validation question:',
          'Can Anchor create qualified property-manager conversations that turn into walkthroughs or estimates?',
        ].join('\n'),
      },
      target_segment: {
        raw: 'Property managers — multi-family / HOA subtype',
      },
      market_bounds: {
        raw: 'Start with Bedford, Hooksett, Londonderry, Auburn, and Goffstown. Keep Greater Manchester in scope.',
      },
      proof_assets: {
        raw: [
          'Available or close to ready:',
          '- Clear service mix and commercial cleaning focus',
          '- The positioning is clear: reliability / Responsiveness',
          '- Defined service area',
          'Still needed or should be packaged:',
          '- Commercial cleaning checklist',
          '- Before/after photos',
        ].join('\n'),
      },
      hypothesis: {
        raw: [
          'If Anchor approaches small to mid-sized local property managers with clear proof of reliability, responsiveness, and a simple walkthrough path, the campaign should produce qualified conversations and at least some walkthrough or estimate interest within the first validation window.',
          '',
          'Primary signals:',
          '- Qualified replies from property managers',
          'Secondary signals:',
          '- Questions about recurring schedule, reliability, response time, or cleaning frustrations',
        ].join('\n'),
      },
      validation_metrics: {
        raw: '3 conversations',
      },
      approval_checkpoints: {
        raw: [
          'Before list-building:',
          '- Campaign plan preview is approved.',
          '- Target segment. subtype. and market bound are confirmed.',
          'Before launch:',
          '- Prospect list criteria are approved.',
        ].join('\n'),
      },
    };

    const fields = extractCampaignPlanFields(ctx, answers);
    assert.doesNotMatch(fields.objective, /Include property managers/i);
    assert.doesNotMatch(fields.hypothesis, /Primary signals/i);
    assert.ok(fields.inclusionCriteria.length >= 1);
    assert.ok(fields.exclusionCriteria.length >= 1);

    const preview = buildFirstCampaignPlanPreview(ctx, answers, {
      blueprintId: ANCHOR_BLUEPRINT.id,
      blueprintVersion: ANCHOR_BLUEPRINT.version,
    });
    const formatted = formatFirstCampaignPlanPreviewMessage(preview);

    assert.match(
      preview.objective,
      /^Prove that small to mid-sized property managers in Greater Manchester will engage in qualified conversations about recurring cleaning\.?$/i
    );
    assert.doesNotMatch(
      preview.campaignObjective,
      /Prove that The first campaign should prove/i
    );
    assert.doesNotMatch(
      preview.campaignObjective,
      /Include property managers who/i
    );
    assert.match(
      preview.coreValidationQuestion,
      /^Can Anchor create qualified property-manager conversations/
    );
    assert.match(
      preview.targetSegment,
      /that likely need recurring cleaning weekly or multiple times per week/i
    );
    assert.match(preview.marketBound, /^Start with Bedford/i);
    assert.match(preview.hypothesis, /^If Anchor approaches/i);
    assert.doesNotMatch(preview.hypothesis, /Primary signals|Qualified replies/i);
    assert.ok(
      preview.proofAssetsAvailable.some((x) =>
        /Positioning around reliability, responsiveness, and accountability/i.test(
          x
        )
      )
    );
    assert.doesNotMatch(
      preview.proofAssetsAvailable.join(' '),
      /The positioning is clear:/i
    );
    assert.ok(
      preview.approvalCheckpointsBeforeListBuilding.includes(
        'Target segment, subtype, and market bound are confirmed.'
      )
    );
    assert.ok(
      preview.approvalCheckpointsBeforeListBuilding.every(
        (x) => !/Target segment\. subtype\./i.test(x)
      )
    );
    assert.ok(
      preview.exclusionCriteria.every(
        (x) => !/prefers to avoid|should avoid/i.test(x)
      )
    );
    assert.ok(
      preview.risksCautions.every(
        (x) => !/prefers to avoid|Anchor should avoid/i.test(x)
      )
    );

    assert.match(formatted, /Include property managers who:/);
    assert.match(formatted, /Exclude property managers who:/);
    assert.match(formatted, /Available or close to ready:/);
    assert.match(formatted, /Still needed or should be packaged:/);
    assert.match(formatted, /Primary signals:/);
    assert.match(formatted, /Before list-building:/);
    assert.doesNotMatch(formatted, /Prove that The first campaign should prove/i);
    assert.doesNotMatch(formatted, /the business prefers to avoid/i);
    assert.doesNotMatch(formatted, /Anchor should avoid/i);
    assert.doesNotMatch(formatted, /Target segment\. subtype\./i);
    assert.doesNotMatch(formatted, /The positioning is clear:/i);
    assert.equal(preview.campaignsGenerated, false);
    assert.equal(preview.prospectListGenerated, false);
    assert.equal(preview.outreachCopyGenerated, false);
    assert.equal(preview.accountChangesMade, false);
  });

  it('sanitizes awkward target-segment joins and first-person objective copy', () => {
    assert.equal(
      sanitizeTargetSegmentText(
        'property managers — Small to mid-sized local property managers in Greater Manchester who manage offices.',
        { primarySegment: 'property managers', targetMarket: 'Greater Manchester' }
      ),
      'Small to mid-sized local property managers in Greater Manchester who oversee offices, mixed-use buildings, small commercial properties, or multi-tenant spaces that likely need recurring cleaning weekly or multiple times per week.'
    );
    assert.match(
      stripFirstPersonArtifactLanguage(
        "For the first test, I'd treat the goal as: Can Anchor win?"
      ),
      /^Core validation question:\nCan Anchor win\?$/
    );
    assert.match(
      stripFirstPersonArtifactLanguage(
        'willing to talk, not just ignore the outreach or shop on price'
      ),
      /rather than ignoring the outreach or responding only on price/i
    );

    const preview = buildFirstCampaignPlanPreview(
      {
        businessName: 'Anchor Cleaning',
        primarySegment: 'property managers',
        targetMarket: 'Greater Manchester',
        subtype:
          'property managers — Small to mid-sized local property managers overseeing offices in Greater Manchester',
        readinessOverallStatus: 'partial',
        completedSetupChecklist: true,
        avoidPhrase:
          'the business prefers to avoid Anchor should avoid buyers focused only on the lowest price',
      },
      {
        opening: { raw: 'as defined' },
        campaign_objective: {
          raw: "For the first test, I'd treat the goal as: prove they will talk, not just ignore the outreach or shop on price",
        },
        target_segment: {
          raw: 'property managers — Small to mid-sized local PMs',
        },
      }
    );
    assert.match(
      preview.targetSegment,
      /^Small to mid-sized local property managers in Greater Manchester who oversee/i
    );
    assert.doesNotMatch(preview.targetSegment, /^property managers/i);
    assert.match(preview.campaignObjective, /Core validation question:/i);
    assert.doesNotMatch(preview.campaignObjective, /\bI'd\b/);
    assert.doesNotMatch(
      preview.campaignObjective,
      /Prove that The first campaign should prove/i
    );
    assert.ok(
      preview.exclusionCriteria.every(
        (x) => !/prefers to avoid|should avoid/i.test(x)
      )
    );
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
    assert.match(uiSource, /Available or close to ready/);
    assert.match(uiSource, /Before list-building/);
    assert.match(uiSource, /inclusionCriteria/);
    assert.match(
      uiSource,
      /Hypothesis and validation gates before any build/
    );
    assert.doesNotMatch(uiSource, /Planning only — not a launch/);
  });
});
