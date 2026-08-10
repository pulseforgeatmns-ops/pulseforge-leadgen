'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  ARTIFACT_KIND,
  PREVIEW_TITLE,
  PREVIEW_DISCLAIMER,
  CRITERIA_ARTIFACT_KIND,
  CRITERIA_PREVIEW_TITLE,
  SECTION_TITLES,
  CONVERSATION_STEPS,
  PROSPECT_LIST_CRITERIA_STEP,
  SLOT_KEYS,
  DEFAULT_PROOF_ASSETS,
  DEFAULT_PROOF_ASSETS_AVAILABLE,
  DEFAULT_PROOF_ASSETS_MISSING,
  DEFAULT_INCLUSION_CRITERIA,
  DEFAULT_REQUIRED_PROSPECT_FIELDS,
  DEFAULT_REVIEW_GATE,
  DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS,
  DEFAULT_APPROVAL_BEFORE_LIST,
  DEFAULT_APPROVAL_BEFORE_LAUNCH,
  DEFAULT_APPROVAL_CHECKPOINTS,
  VALIDATION_SUCCESS_STATEMENT,
  buildCampaignPlanningContext,
  buildCampaignPlanningOpening,
  buildCampaignPlanningReply,
  buildFirstCampaignPlanPreview,
  buildProspectListCriteriaPreview,
  formatFirstCampaignPlanPreviewMessage,
  formatProspectListCriteriaPreviewMessage,
  extractCampaignPlanFields,
  stripCampaignWrappers,
  extractAvoidPhrase,
  seedSlotsFromContext,
  isSlotSatisfied,
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
    assert.match(
      preview.campaignObjective,
      /^Prove that property managers will book walkthroughs\./i
    );
    assert.doesNotMatch(preview.campaignObjective, /Core validation question/i);
    assert.doesNotMatch(preview.campaignObjective, /\bI'd\b/);
    assert.doesNotMatch(
      preview.campaignObjective,
      /Prove that Prove|The first campaign should prove/i
    );
    assert.doesNotMatch(
      preview.campaignObjective,
      /Include property managers who/i
    );
    assert.match(
      preview.coreValidationQuestion,
      /^Can Anchor create qualified property-manager conversations/
    );
    assert.equal(preview.campaignHypothesis, preview.hypothesis);
    assert.deepEqual(preview.risks, preview.risksCautions);
    assert.deepEqual(
      preview.approvalCheckpointsBeforeList,
      preview.approvalCheckpointsBeforeListBuilding
    );
    assert.match(
      preview.targetSegment,
      /^Small to mid-sized local property managers in Greater Manchester who oversee/i
    );
    assert.doesNotMatch(preview.targetSegment, /^property managers/i);
    assert.doesNotMatch(preview.targetSegment, /—/);
    assert.equal(preview.targetSegmentAvoid, null);
    assert.deepEqual(preview.inclusionCriteria, [...DEFAULT_INCLUSION_CRITERIA]);
    assert.equal(
      preview.targetSubtype,
      DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS
    );
    assert.doesNotMatch(
      preview.targetSubtype,
      /price buyers|service area|decision-maker/i
    );
    assert.deepEqual(preview.exclusionCriteria, [
      'Large institutional property managers',
      'Highly complex properties',
      'Lowest-price buyers',
      "Properties outside Anchor's service area",
      'Prospects with no clear decision-maker or contact path',
    ]);
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
    assert.match(formatted, /Core validation question:/);
    assert.match(formatted, /2\. Target segment/);
    assert.match(formatted, /Include property managers who:/);
    assert.match(formatted, /Exclude property managers who:/);
    assert.match(
      formatted,
      /Subtype: property managers overseeing offices, mixed-use buildings/
    );
    assert.doesNotMatch(
      formatted,
      /Subtype:.*price buyers|Subtype:.*service area/i
    );
    assert.match(formatted, /Large institutional property managers/);
    assert.match(formatted, /Lowest-price buyers/);
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
    assert.doesNotMatch(
      formatted,
      /Prove that[\s\S]*Include property managers who manage/i
    );
  });

  it('rejects exclusion-bleed subtype and keeps polished exclusion bullets', () => {
    const preview = buildFirstCampaignPlanPreview(
      {
        businessName: 'Anchor Cleaning',
        primarySegment: 'property managers',
        targetMarket: 'Greater Manchester',
        towns: ['Bedford', 'Hooksett', 'Londonderry', 'Auburn', 'Goffstown'],
        subtype:
          'price buyers, properties outside Anchor’s service area, and prospects with no clear decision-maker or contact path',
        avoidPhrase:
          'the business prefers to avoid Anchor should avoid buyers focused only on the lowest price',
        readinessOverallStatus: 'partial',
        completedSetupChecklist: true,
      },
      {
        opening: { raw: 'Keep property managers as defined.' },
        target_segment: {
          raw: 'Property managers — price buyers, properties outside Anchor’s service area, and prospects with no clear decision-maker or contact path',
        },
      }
    );
    const formatted = formatFirstCampaignPlanPreviewMessage(preview);
    assert.equal(
      preview.targetSubtype,
      DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS
    );
    assert.doesNotMatch(
      preview.targetSubtype,
      /price buyers|service area|decision-maker/i
    );
    assert.deepEqual(preview.exclusionCriteria, [
      'Large institutional property managers',
      'Highly complex properties',
      'Lowest-price buyers',
      "Properties outside Anchor's service area",
      'Prospects with no clear decision-maker or contact path',
    ]);
    assert.match(
      formatted,
      /Subtype: property managers overseeing offices, mixed-use buildings/
    );
    assert.doesNotMatch(
      formatted,
      /Subtype:\s*price buyers/i
    );
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
      preview.campaignObjective,
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
    assert.doesNotMatch(preview.campaignObjective, /Core validation question/i);
    assert.match(
      preview.coreValidationQuestion,
      /^Can Anchor create qualified property-manager conversations/
    );
    assert.match(
      preview.targetSegment,
      /that likely need recurring cleaning weekly or multiple times per week/i
    );
    assert.match(preview.marketBound, /^Start with Bedford/i);
    assert.match(preview.campaignHypothesis, /^If Anchor approaches/i);
    assert.doesNotMatch(
      preview.campaignHypothesis,
      /Primary signals|Qualified replies/i
    );
    assert.ok(
      preview.proofAssetsAvailable.some((x) =>
        /Positioning around reliability, responsiveness, and accountability/i.test(
          x
        )
      )
    );
    assert.doesNotMatch(
      preview.proofAssetsAvailable.join(' '),
      /The positioning is clear:|reliability\s*\/\s*Responsiveness/i
    );
    assert.ok(
      preview.approvalCheckpointsBeforeList.includes(
        'Target segment, subtype, and market bound are confirmed.'
      )
    );
    assert.ok(
      preview.approvalCheckpointsBeforeList.every(
        (x) => !/Target segment\. subtype\./i.test(x)
      )
    );
    assert.ok(
      preview.exclusionCriteria.every(
        (x) => !/prefers to avoid|should avoid/i.test(x)
      )
    );
    assert.ok(
      preview.risks.every(
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
    assert.doesNotMatch(formatted, /reliability\s*\/\s*Responsiveness/i);
    assert.equal(preview.campaignsGenerated, false);
    assert.equal(preview.prospectListGenerated, false);
    assert.equal(preview.outreachCopyGenerated, false);
    assert.equal(preview.accountChangesMade, false);
  });

  it('peels unlabeled include/exclude bleed from conversational objective answers', () => {
    const preview = buildFirstCampaignPlanPreview(
      {
        businessName: 'Anchor Cleaning',
        primarySegment: 'property managers',
        targetMarket: 'Greater Manchester',
        towns: ['Bedford', 'Hooksett', 'Londonderry', 'Auburn', 'Goffstown'],
        avoidPhrase:
          'the business prefers to avoid Anchor should avoid buyers focused only on the lowest price',
        readinessOverallStatus: 'not_ready',
        completedSetupChecklist: true,
      },
      {
        opening: { raw: 'Keep property managers as defined.' },
        campaign_objective: {
          raw:
            'The first campaign should prove that small to mid-sized property managers in Greater Manchester will engage. ' +
            'We should include property managers who manage offices and exclude property managers who only care about price. ' +
            'Core validation question: Can Anchor create qualified conversations?',
        },
        target_segment: { raw: 'Property managers — multi-family / HOA subtype' },
        market_bounds: { raw: 'Greater Manchester' },
        proof_assets: {
          raw:
            'The positioning is clear: reliability / Responsiveness. Also service area and checklist. ' +
            'Still need references and before/after photos.',
        },
        hypothesis: {
          raw:
            'If we approach local property managers in Greater Manchester in Greater Manchester with proof, we expect walkthroughs. ' +
            'Primary signals: Qualified replies. Secondary signals: Questions about price.',
        },
        validation_metrics: { raw: '3 conversations' },
        approval_checkpoints: {
          raw: 'Target segment. subtype. and market bound are confirmed. Preview sign-off. Proof ready.',
        },
      }
    );
    const formatted = formatFirstCampaignPlanPreviewMessage(preview);

    assert.match(preview.campaignObjective, /^Prove that /i);
    assert.doesNotMatch(preview.campaignObjective, /\binclude\b|\bexclude\b/i);
    assert.doesNotMatch(
      preview.campaignObjective,
      /Prove that The first campaign should prove/i
    );
    assert.doesNotMatch(preview.campaignObjective, /Core validation question/i);
    assert.match(preview.coreValidationQuestion, /^Can Anchor create/i);
    assert.match(preview.campaignHypothesis, /^If Anchor approaches/i);
    assert.doesNotMatch(
      preview.campaignHypothesis,
      /Primary signals|Qualified replies/i
    );
    assert.ok(
      preview.exclusionCriteria.every(
        (x) => !/prefers to avoid|should avoid/i.test(x)
      )
    );
    assert.doesNotMatch(
      preview.proofAssetsAvailable.join(' '),
      /The positioning is clear:|reliability\s*\/\s*Responsiveness/i
    );
    assert.ok(
      preview.approvalCheckpointsBeforeList.every(
        (x) => !/Target segment\. subtype\./i.test(x)
      )
    );
    assert.doesNotMatch(formatted, /\bWe should include\b/i);
    assert.doesNotMatch(formatted, /the business prefers to avoid/i);
    assert.doesNotMatch(formatted, /Target segment\. subtype\./i);
    assert.doesNotMatch(formatted, /reliability\s*\/\s*Responsiveness/i);
    assert.equal(preview.notes, null);
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
    assert.match(preview.coreValidationQuestion, /Can Anchor/i);
    assert.doesNotMatch(preview.campaignObjective, /Core validation question/i);
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

  it('advances unsatisfied slots then produces preview without forbidden launch language', () => {
    const ctx = {
      businessName: 'Anchor Cleaning',
      primarySegment: 'property managers',
      secondarySegment: 'professional offices',
      targetMarket: 'Greater Manchester',
      subtype: 'Multi-family / HOA property managers',
      proofFromPrior: 'checklist, photos, response-time expectation',
      towns: ['Bedford', 'Hooksett', 'Londonderry', 'Auburn', 'Goffstown'],
      completedSetupChecklist: true,
      readinessOverallStatus: 'partial',
      blueprintId: 'bp-1',
      blueprintVersion: '1.0',
    };
    let state = {
      step: 'opening',
      answers: {},
      slots: seedSlotsFromContext(ctx, null),
    };
    const r1 = buildCampaignPlanningReply(
      'Keep property managers exactly as defined.',
      state,
      ctx
    );
    assert.equal(r1.step, 'campaign_objective');
    assert.equal(r1.intent, 'advance');
    assert.equal(r1.preview, null);
    assert.equal(isSlotSatisfied(r1.slots, 'targetSegment'), true);
    assert.equal(isSlotSatisfied(r1.slots, 'targetSubtype'), true);
    assert.equal(containsForbiddenCampaignPlanningLanguage(r1.message), false);

    state = {
      step: r1.step,
      answers: r1.answers,
      slots: r1.slots,
      currentAsk: r1.currentAsk,
    };
    const steps = [
      'Prove walkthrough demand.',
      'If we approach HOA PMs with proof, we get walkthroughs',
      '2 conversations and 1 walkthrough',
      'Preview sign-off before any list or copy',
    ];
    let final = null;
    for (const msg of steps) {
      final = buildCampaignPlanningReply(msg, state, ctx);
      assert.equal(containsForbiddenCampaignPlanningLanguage(final.message), false);
      state = {
        step: final.step,
        answers: final.answers,
        slots: final.slots,
        currentAsk: final.currentAsk,
      };
      if (final.preview) break;
    }
    assert.ok(final);
    assert.ok(
      final.intent === 'produce_preview' ||
        final.slots.previewGenerated === true
    );
    assert.ok(final.preview || final.slots.previewGenerated);
    if (final.preview) {
      assert.equal(final.preview.kind, ARTIFACT_KIND);
      assert.match(final.message, /Planning preview only\./i);
      assert.doesNotMatch(final.message, /This stays planning-only/i);
    }
    assert.equal(containsForbiddenCampaignPlanningLanguage(final.message), false);
    assert.equal(
      containsForbiddenCampaignPlanningLanguage('I built a prospect list'),
      true
    );
  });

  it('Anchor path does not re-ask objective or segment after they are captured', () => {
    const ctx = buildCampaignPlanningContext(
      {
        interview_state: {
          initialGrowthDirection: {
            businessName: 'Anchor Cleaning',
            primaryArea: 'Greater Manchester',
            towns: ['Bedford', 'Hooksett', 'Londonderry', 'Auburn', 'Goffstown'],
          },
          validationTarget: {
            best_fit_subtype:
              'Multi-family / HOA property managers with recurring building needs',
            credibility_proof_needed:
              'service checklist, photos/examples, clear response-time expectation',
          },
          firstGrowthPlanPreview: {
            businessName: 'Anchor Cleaning',
            primarySegmentDisplay: 'property managers',
            secondarySegmentDisplay: 'professional offices',
            primaryArea: 'Greater Manchester',
            first_subtype_to_test:
              'Multi-family / HOA property managers with recurring building needs',
            credibility_proof_needed:
              'service checklist, photos/examples, clear response-time expectation',
          },
        },
      },
      ANCHOR_BLUEPRINT
    );

    let state = {
      step: 'opening',
      answers: {},
      slots: seedSlotsFromContext(ctx, null),
    };
    const openingReply = buildCampaignPlanningReply(
      'Keep property managers as defined.',
      state,
      ctx
    );
    assert.equal(openingReply.currentAsk, 'campaignObjective');
    assert.match(openingReply.message, /What should this first campaign prove/i);
    assert.doesNotMatch(
      openingReply.message,
      /Confirm the first target segment/i
    );

    state = {
      step: openingReply.step,
      answers: openingReply.answers,
      slots: openingReply.slots,
      currentAsk: openingReply.currentAsk,
    };

    const multi = buildCampaignPlanningReply(
      [
        'Prove that multi-family / HOA property managers will request walkthroughs.',
        'Inclusion: HOA and multi-family managers with recurring building needs in Greater Manchester.',
        'Exclusion: national firms and price-only buyers.',
      ].join(' '),
      state,
      ctx
    );

    assert.equal(isSlotSatisfied(multi.slots, 'campaignObjective'), true);
    assert.equal(isSlotSatisfied(multi.slots, 'inclusionCriteria'), true);
    assert.equal(isSlotSatisfied(multi.slots, 'exclusionCriteria'), true);
    assert.equal(isSlotSatisfied(multi.slots, 'targetSegment'), true);
    assert.doesNotMatch(multi.message, /What should this first campaign prove/i);
    assert.doesNotMatch(
      multi.message,
      /Confirm the first target segment and subtype/i
    );
    assert.equal(multi.slots.previewGenerated, true);
    assert.ok(multi.preview);
    assert.equal(multi.preview.kind, ARTIFACT_KIND);
    assert.ok(multi.criteriaPreview);
    assert.equal(multi.criteriaPreview.kind, CRITERIA_ARTIFACT_KIND);
    assert.equal(multi.criteriaPreview.title, CRITERIA_PREVIEW_TITLE);
    assert.equal(multi.criteriaPreview.prospectListGenerated, false);
    assert.equal(multi.criteriaPreview.outreachCopyGenerated, false);
    assert.equal(multi.criteriaPreview.accountChangesMade, false);
    assert.match(multi.message, /Prospect List Criteria Preview/i);
    assert.equal(containsForbiddenCampaignPlanningLanguage(multi.message), false);

    // Structured-field criteria preview — no stitched/truncated fragments.
    const criteria = multi.criteriaPreview;
    assert.deepEqual(criteria.inclusionCriteria, [...DEFAULT_INCLUSION_CRITERIA]);
    assert.ok(criteria.exclusionCriteria.length >= 3);
    assert.ok(
      criteria.exclusionCriteria.every(
        (item) => !/each prospect record should include/i.test(item)
      )
    );
    assert.ok(Array.isArray(criteria.requiredProspectFields));
    assert.deepEqual(criteria.requiredProspectFields, [
      ...DEFAULT_REQUIRED_PROSPECT_FIELDS,
    ]);
    assert.equal(criteria.sectionTitles.requiredProspectFields, 'Required prospect record fields');
    assert.equal(criteria.sectionTitles.reviewGate, 'Review gate');
    assert.match(criteria.reviewGate, /before any list is built/i);
    assert.doesNotMatch(
      criteria.requiredProspectFields.join(' '),
      /questions about|reliability|responsiveness|vague interest/i
    );
    assert.doesNotMatch(criteria.targetSubtype || '', /-\s*Li\b|Inclusion:|Exclusion:/i);
    assert.doesNotMatch(
      criteria.marketBound || '',
      /will engage|recurring clea(?!ning)/i
    );
    assert.doesNotMatch(
      formatProspectListCriteriaPreviewMessage(criteria),
      /-\s*Li\b|recurring clea(?!ning)/i
    );
    assert.match(
      formatProspectListCriteriaPreviewMessage(criteria),
      /7\.\s+Required prospect record fields/
    );
    assert.match(
      formatProspectListCriteriaPreviewMessage(criteria),
      /8\.\s+Review gate/
    );

    // After criteria preview, do not loop back to objective.
    state = {
      step: multi.step,
      answers: multi.answers,
      slots: multi.slots,
      currentAsk: multi.currentAsk,
      status: 'preview_ready',
    };
    const followUp = buildCampaignPlanningReply(
      'Looks good',
      state,
      ctx
    );
    assert.doesNotMatch(followUp.message, /What should this first campaign prove/i);
    assert.doesNotMatch(
      followUp.message,
      /Confirm the first target segment and subtype/i
    );
  });

  it('defines required planning slots including preview flags', () => {
    for (const key of [
      'campaignObjective',
      'targetSegment',
      'targetSubtype',
      'marketBound',
      'campaignHypothesis',
      'proofAssets',
      'validationMetrics',
      'inclusionCriteria',
      'exclusionCriteria',
      'approvalCheckpoints',
      'previewGenerated',
      'previewApproved',
    ]) {
      assert.ok(SLOT_KEYS.includes(key), `missing slot ${key}`);
    }
  });

  it('approving First Campaign Plan Preview advances to prospect_list_criteria', () => {
    const ctx = buildCampaignPlanningContext(
      {
        interview_state: {
          initialGrowthDirection: {
            businessName: 'Anchor Cleaning',
            primaryArea: 'Greater Manchester',
            towns: ['Bedford', 'Hooksett'],
          },
          firstGrowthPlanPreview: {
            businessName: 'Anchor Cleaning',
            primarySegmentDisplay: 'property managers',
            secondarySegmentDisplay: 'professional offices',
            primaryArea: 'Greater Manchester',
            first_subtype_to_test:
              'property managers overseeing offices and mixed-use buildings',
          },
        },
      },
      ANCHOR_BLUEPRINT
    );

    const answers = {
      opening: { raw: 'Keep property managers as defined.', at: 't0' },
      campaign_objective: {
        raw: 'Prove that property managers will request walkthroughs.',
        at: 't1',
      },
      target_segment: { raw: 'property managers as defined', at: 't2' },
      market_bounds: { raw: 'Greater Manchester', at: 't3' },
      proof_assets: {
        raw: 'Checklist available; photos still needed.',
        at: 't4',
      },
      hypothesis: {
        raw:
          'If we approach property managers in Greater Manchester with a checklist, we expect walkthrough requests.',
        at: 't5',
      },
      validation_metrics: {
        raw: 'Qualified replies and walkthroughs in 30 days.',
        at: 't6',
      },
      approval_checkpoints: {
        raw: 'Preview sign-off before list; copy review before launch.',
        at: 't7',
      },
    };

    const previewReply = buildCampaignPlanningReply(
      answers.approval_checkpoints.raw,
      {
        step: 'approval_checkpoints',
        answers: Object.fromEntries(
          Object.entries(answers).filter(([k]) => k !== 'approval_checkpoints')
        ),
        slots: seedSlotsFromContext(ctx, {
          campaignObjective: answers.campaign_objective.raw,
          targetSegment: 'property managers',
          marketBound: 'Greater Manchester',
          proofAssets: answers.proof_assets.raw,
          campaignHypothesis: answers.hypothesis.raw,
          validationMetrics: answers.validation_metrics.raw,
        }),
        context: ctx,
      },
      ctx
    );

    assert.equal(previewReply.step, 'preview');
    assert.ok(previewReply.preview);
    assert.equal(previewReply.preview.status, 'draft');
    assert.equal(previewReply.slots.previewGenerated, true);
    assert.equal(previewReply.slots.previewApproved, false);
    const draftPreview = previewReply.preview;

    const approveReply = buildCampaignPlanningReply(
      'Approve',
      {
        step: 'preview',
        status: 'preview_ready',
        answers: previewReply.answers,
        slots: previewReply.slots,
        currentAsk: 'previewApproved',
        context: ctx,
        firstCampaignPlanPreview: draftPreview,
      },
      ctx,
      { priorPreview: draftPreview }
    );

    assert.equal(approveReply.step, PROSPECT_LIST_CRITERIA_STEP);
    assert.equal(approveReply.previewApproved, true);
    assert.equal(approveReply.slots.previewApproved, true);
    assert.equal(approveReply.preview.status, 'approved');
    assert.equal(approveReply.preview.kind, draftPreview.kind);
    assert.equal(
      approveReply.preview.campaignObjective,
      draftPreview.campaignObjective
    );
    assert.match(
      approveReply.message,
      /Before building a prospect list, define what should qualify or disqualify a property manager for this first test\./
    );
    assert.doesNotMatch(
      approveReply.message,
      /What should this first campaign prove/
    );
    assert.doesNotMatch(
      approveReply.message,
      /Confirm the first target segment/
    );
    assert.doesNotMatch(approveReply.message, /Confirm the market bounds/);
    assert.doesNotMatch(approveReply.message, /proof assets are already available/i);
    assert.doesNotMatch(approveReply.message, /campaign hypothesis/i);
    assert.doesNotMatch(approveReply.message, /approval checkpoints should block/i);
    assert.equal(approveReply.intent, 'preview_approved');
    // Approved preview is not regenerated as a new draft artifact.
    assert.notEqual(approveReply.preview.status, 'draft');
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
      'If we approach HOA PMs in Greater Manchester with a checklist, we expect walkthrough requests.',
      '3 conversations; 1 walkthrough',
      'Preview approval; proof ready; no list until signed off',
    ];
    let last = null;
    for (const msg of answers) {
      last = await postCampaignPlanningMessage('int-campaign-1', msg, { store });
      if (last.firstCampaignPlanPreview) break;
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
    assert.equal(last.campaignPlanning.slots.previewGenerated, true);

    const session = await store.getSession('int-campaign-1');
    assert.ok(session.interview_state.campaignPlanning);
    assert.ok(session.interview_state.campaignPlanning.slots);
    assert.ok(session.interview_state.firstCampaignPlanPreview);
    // No CRM / prospect writes — only interview_state keys.
    assert.equal(session.status, 'APPROVED');
  });

  it('persists preview approval and advances session to prospect_list_criteria', async () => {
    const store = createMemoryStore();
    await seedApprovedSession(store);
    await startCampaignPlanningConversation('int-campaign-1', { store });

    const answers = [
      'Keep property managers as defined.',
      'Prove PMs will request walkthroughs.',
      'If we approach HOA PMs in Greater Manchester with a checklist, we expect walkthrough requests.',
      '3 conversations; 1 walkthrough',
      'Preview approval; proof ready; no list until signed off',
    ];
    let previewMsg = null;
    for (const msg of answers) {
      previewMsg = await postCampaignPlanningMessage('int-campaign-1', msg, {
        store,
      });
      if (previewMsg.firstCampaignPlanPreview) break;
    }
    assert.ok(previewMsg && previewMsg.firstCampaignPlanPreview);
    const draftObjective =
      previewMsg.firstCampaignPlanPreview.campaignObjective;

    const approved = await postCampaignPlanningMessage(
      'int-campaign-1',
      'Approve',
      { store }
    );

    assert.equal(approved.campaignPlanning.step, PROSPECT_LIST_CRITERIA_STEP);
    assert.equal(approved.campaignPlanning.previewApproved, true);
    assert.equal(approved.campaignPlanning.slots.previewApproved, true);
    assert.equal(approved.firstCampaignPlanPreview.status, 'approved');
    assert.equal(
      approved.firstCampaignPlanPreview.campaignObjective,
      draftObjective
    );
    assert.match(
      approved.message,
      /Before building a prospect list, define what should qualify or disqualify a property manager for this first test\./
    );
    assert.doesNotMatch(
      approved.message,
      /What should this first campaign prove/
    );

    const session = await store.getSession('int-campaign-1');
    assert.equal(
      session.interview_state.firstCampaignPlanPreview.status,
      'approved'
    );
    assert.equal(
      session.interview_state.campaignPlanning.previewApproved,
      true
    );
    assert.equal(
      session.interview_state.campaignPlanning.step,
      PROSPECT_LIST_CRITERIA_STEP
    );
  });

  it('fills objective + criteria in one message and returns criteria preview', async () => {
    const store = createMemoryStore();
    await seedApprovedSession(store);
    await startCampaignPlanningConversation('int-campaign-1', { store });
    await postCampaignPlanningMessage(
      'int-campaign-1',
      'Keep property managers as defined.',
      { store }
    );
    const last = await postCampaignPlanningMessage(
      'int-campaign-1',
      [
        'Prove that multi-family property managers will book walkthroughs.',
        'Inclusion: local HOA / multi-family managers with recurring needs.',
        'Exclusion: national property firms and lowest-price shoppers.',
        'Each prospect record should include: business name, contact name, email, phone, property type, town.',
      ].join(' '),
      { store }
    );
    assert.ok(last.firstCampaignPlanPreview);
    assert.ok(last.prospectListCriteriaPreview);
    assert.equal(last.prospectListCriteriaPreview.kind, CRITERIA_ARTIFACT_KIND);
    assert.equal(last.prospectListCriteriaPreview.prospectListGenerated, false);
    assert.equal(last.prospectListCriteriaPreview.outreachCopyGenerated, false);
    assert.equal(last.prospectListCriteriaPreview.accountChangesMade, false);
    assert.match(last.message, /Prospect List Criteria Preview/i);
    assert.doesNotMatch(last.message, /What should this first campaign prove/i);
    assert.doesNotMatch(
      last.message,
      /Confirm the first target segment and subtype/i
    );

    const criteria = last.prospectListCriteriaPreview;
    assert.deepEqual(criteria.inclusionCriteria, [...DEFAULT_INCLUSION_CRITERIA]);
    assert.ok(
      criteria.exclusionCriteria.every(
        (item) => !/each prospect record should include/i.test(item)
      )
    );
    assert.ok(
      criteria.requiredProspectFields.some((f) =>
        /company or property manager name/i.test(f)
      )
    );
    assert.ok(
      criteria.requiredProspectFields.some((f) => /confidence level/i.test(f))
    );
    assert.deepEqual(
      criteria.requiredProspectFields,
      [...DEFAULT_REQUIRED_PROSPECT_FIELDS]
    );
    assert.doesNotMatch(
      criteria.requiredProspectFields.join(' '),
      /questions about|reliability|responsiveness|vague interest|photos?\/examples?|business name|contact name/i
    );
    assert.match(criteria.reviewGate || '', /before any list is built/i);
    assert.doesNotMatch(criteria.targetSubtype || '', /-\s*Li\b|Inclusion:/i);
    assert.doesNotMatch(
      criteria.marketBound || '',
      /will engage|recurring clea(?!ning)/i
    );
    // Comma-rich location bullets stay intact (not split into town fragments).
    assert.ok(
      criteria.inclusionCriteria.some((item) =>
        /Bedford,\s*Hooksett,\s*Londonderry/i.test(item)
      )
    );

    const session = await store.getSession('int-campaign-1');
    assert.ok(session.interview_state.prospectListCriteriaPreview);
    assert.equal(
      session.interview_state.campaignPlanning.slots.previewGenerated,
      true
    );
    assert.ok(
      session.interview_state.campaignPlanning.slots.inclusionCriteria
    );
    assert.ok(
      session.interview_state.campaignPlanning.slots.exclusionCriteria
    );
  });

  it('renders Prospect List Criteria Preview from structured fields only', () => {
    const preview = buildProspectListCriteriaPreview(
      {
        businessName: 'Anchor Cleaning',
        primarySegment: 'property managers',
        subtype: DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS,
        targetMarket: 'Greater Manchester',
        towns: ['Bedford', 'Hooksett', 'Londonderry', 'Auburn', 'Goffstown'],
      },
      {
        campaignObjective: null,
        targetSegment: 'property managers',
        targetSubtype:
          'family / HOA property managers will request walkthroughs. Inclusion: HOA - Li',
        marketBound:
          'Greater Manchester will engage in qualified conversations about recurring clea',
        inclusionCriteria:
          'Small to mid-sized local property managers - Located in Bedford, Hooksett, Londonderry, Auburn, Goffstown, or nearby Greater Manchester markets - Likely need recurring cleaning',
        exclusionCriteria:
          'national firms and price-only buyers. Each prospect record should include: business name, email, phone. Review gate: approve before list.',
        requiredProspectFields:
          'Questions about recurring service, reliability, responsiveness, scheduling, or current cleaning frustrations, Vague interest with no next step',
      }
    );

    assert.equal(preview.kind, CRITERIA_ARTIFACT_KIND);
    assert.equal(preview.prospectListGenerated, false);
    assert.equal(preview.outreachCopyGenerated, false);
    assert.equal(preview.accountChangesMade, false);
    assert.deepEqual(preview.inclusionCriteria, [...DEFAULT_INCLUSION_CRITERIA]);
    assert.ok(
      preview.exclusionCriteria.every(
        (item) => !/each prospect record should include/i.test(item)
      )
    );
    assert.deepEqual(preview.requiredProspectFields, [
      ...DEFAULT_REQUIRED_PROSPECT_FIELDS,
    ]);
    assert.doesNotMatch(
      preview.requiredProspectFields.join(' '),
      /questions about|reliability|responsiveness|scheduling|vague interest|cleaning frustrations/i
    );
    assert.equal(preview.reviewGate, DEFAULT_REVIEW_GATE);
    assert.doesNotMatch(preview.targetSubtype, /-\s*Li\b|Inclusion:/i);
    assert.doesNotMatch(
      preview.marketBound,
      /will engage|recurring clea(?!ning)/i
    );
    assert.match(preview.marketBound, /Bedford|Greater Manchester/i);
    assert.ok(
      preview.inclusionCriteria.every(
        (item) => !/^Bedford$|^Hooksett$|^Londonderry$/i.test(item)
      )
    );

    const rendered = formatProspectListCriteriaPreviewMessage(preview);
    assert.match(rendered, /1\.\s+Campaign objective/);
    assert.match(rendered, /7\.\s+Required prospect record fields/);
    assert.match(rendered, /8\.\s+Review gate/);
    assert.match(rendered, /9\.\s+Recommended next step/);
    assert.doesNotMatch(rendered, /-\s*Li\b|recurring clea(?!ning)/i);
    assert.equal(
      preview.requiredProspectFields.length >= 1,
      true
    );
    assert.ok(DEFAULT_REQUIRED_PROSPECT_FIELDS.length >= 3);
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
    assert.match(uiSource, /renderProspectListCriteriaPreview/);
    assert.match(uiSource, /First Campaign Plan Preview/);
    assert.match(uiSource, /campaignHypothesis/);
    assert.match(uiSource, /approvalCheckpointsBeforeList/);
    assert.match(uiSource, /coreValidationQuestion/);
    assert.match(uiSource, /Structured fields only/);
    assert.match(uiSource, /Prospect List Criteria Preview/);
    assert.match(uiSource, /Campaign hypothesis/);
    assert.match(uiSource, /Risks and cautions/);
    assert.match(uiSource, /Available or close to ready/);
    assert.match(uiSource, /Before list-building/);
    assert.match(uiSource, /inclusionCriteria/);
    assert.match(uiSource, /requiredProspectFields/);
    assert.match(uiSource, /reviewGate/);
    assert.match(uiSource, /Required prospect record fields/);
    assert.match(uiSource, /Review gate/);
    assert.match(uiSource, /isTruncatedCriteriaText/);
    assert.match(uiSource, /criteriaListItems/);
    assert.match(uiSource, /prospectListCriteriaPreview/);
    assert.match(
      uiSource,
      /Hypothesis and validation gates before any build/
    );
    assert.doesNotMatch(uiSource, /Planning only — not a launch/);
  });
});

describe('SPEC-091 Prospect List Build Proposal progression', () => {
  const APPROVAL_PLUS =
    'Approved. Before we build anything, tell me how you would approach building the first prospect list for this test...';

  async function reachCriteriaPreview(store) {
    await seedApprovedSession(store);
    await startCampaignPlanningConversation('int-campaign-1', { store });
    await postCampaignPlanningMessage(
      'int-campaign-1',
      'Keep property managers as defined.',
      { store }
    );
    return postCampaignPlanningMessage(
      'int-campaign-1',
      [
        'Prove that multi-family property managers will book walkthroughs.',
        'Inclusion: local HOA / multi-family managers with recurring needs.',
        'Exclusion: national property firms and lowest-price shoppers.',
      ].join(' '),
      { store }
    );
  }

  it('approval_plus_next_request produces Prospect List Build Proposal, not criteria replay', async () => {
    const store = createMemoryStore();
    const criteriaTurn = await reachCriteriaPreview(store);
    assert.ok(criteriaTurn.prospectListCriteriaPreview);
    assert.match(criteriaTurn.message, /Prospect List Criteria Preview/i);
    assert.equal(
      criteriaTurn.prospectListCriteriaPreview.prospectListGenerated,
      false
    );

    const next = await postCampaignPlanningMessage(
      'int-campaign-1',
      APPROVAL_PLUS,
      { store }
    );

    assert.equal(next.messageClass, 'approval_plus_next_request');
    assert.equal(next.intent, 'produce_build_proposal');
    assert.ok(next.prospectListBuildProposal);
    assert.equal(
      next.prospectListBuildProposal.kind,
      'prospect_list_build_proposal'
    );
    assert.equal(next.prospectListBuildProposal.title, 'Prospect List Build Proposal');
    assert.equal(next.prospectListBuildProposal.prospectListGenerated, false);
    assert.equal(next.prospectListBuildProposal.outreachCopyGenerated, false);
    assert.equal(next.prospectListBuildProposal.accountChangesMade, false);
    assert.match(next.message, /Prospect List Build Proposal/i);
    assert.doesNotMatch(
      next.message,
      /^Prospect List Criteria Preview$/m
    );
    // Must not be a criteria-title-led replay of the prior artifact.
    assert.equal(
      /^Prospect List Criteria Preview/m.test(next.message.split('\n\n').pop() || ''),
      false
    );
    assert.ok(
      next.prospectListCriteriaPreview &&
        next.prospectListCriteriaPreview.status === 'approved'
    );

    const session = await store.getSession('int-campaign-1');
    assert.equal(
      session.interview_state.prospectListCriteriaPreview.status,
      'approved'
    );
    assert.ok(session.interview_state.prospectListBuildProposal);
    assert.ok(
      session.interview_state.reasoningMemory.approvedArtifacts.includes(
        'prospect_criteria'
      )
    );
    assert.equal(
      session.interview_state.campaignPlanning.step,
      'prospect_list_build_proposal'
    );
  });

  it('does not repeat approved criteria on a second approval-style turn', async () => {
    const store = createMemoryStore();
    await reachCriteriaPreview(store);
    const first = await postCampaignPlanningMessage(
      'int-campaign-1',
      APPROVAL_PLUS,
      { store }
    );
    assert.ok(first.prospectListBuildProposal);

    const second = await postCampaignPlanningMessage(
      'int-campaign-1',
      'Approved',
      { store }
    );
    assert.doesNotMatch(second.message, /^Prospect List Criteria Preview/m);
    assert.notEqual(second.intent, 'produce_criteria_preview');
  });

  it('buildCampaignPlanningReply unit: approval+next after criteria advances', () => {
    const {
      BUILD_PROPOSAL_ARTIFACT_KIND,
      BUILD_PROPOSAL_TITLE,
    } = require('../services/clientIntelligenceCampaignPlanning');
    const ctx = {
      businessName: 'Anchor Cleaning',
      primarySegment: 'property_managers',
      marketLabel: 'Greater Manchester',
    };
    const slots = {
      campaignObjective: 'Prove walkthroughs',
      targetSegment: 'property managers',
      marketBound: 'Greater Manchester',
      proofAssets: 'photos',
      campaignHypothesis: 'If we approach PMs we expect walkthroughs',
      validationMetrics: 'walkthroughs booked',
      approvalCheckpoints: 'preview sign-off',
      inclusionCriteria: ['local managers'],
      exclusionCriteria: ['national firms'],
      previewGenerated: true,
      previewApproved: true,
      criteriaGenerated: true,
    };
    const priorCriteria = buildProspectListCriteriaPreview(ctx, slots, {
      answers: {},
    });
    const reply = buildCampaignPlanningReply(
      APPROVAL_PLUS,
      {
        step: 'prospect_list_criteria_preview',
        slots,
        prospectListCriteriaPreview: priorCriteria,
        previewApproved: true,
      },
      ctx,
      {
        priorCriteriaPreview: priorCriteria,
        messageClass: 'approval_plus_next_request',
      }
    );
    assert.equal(reply.intent, 'produce_build_proposal');
    assert.ok(reply.buildProposal);
    assert.equal(reply.buildProposal.kind, BUILD_PROPOSAL_ARTIFACT_KIND);
    assert.equal(reply.buildProposal.title, BUILD_PROPOSAL_TITLE);
    assert.match(reply.message, /Prospect List Build Proposal/i);
    assert.equal(reply.criteriaPreview.status, 'approved');
  });

  it('UI renders Prospect List Build Proposal markers', () => {
    const uiSource = fs.readFileSync(
      path.join(__dirname, '../public/client-intel.html'),
      'utf8'
    );
    assert.match(uiSource, /renderProspectListBuildProposal/);
    assert.match(uiSource, /Prospect List Build Proposal/);
    assert.match(uiSource, /prospectListBuildProposal/);
    assert.match(uiSource, /Approach only — no prospect list built yet/);
  });
});

describe('Reviewable prospect list draft progression', () => {
  const APPROVAL_PLUS =
    'Approved. Before we build anything, tell me how you would approach building the first prospect list for this test...';
  const DRAFT_REQ =
    'Now generate the first reviewable prospect list batch. This is a reviewable list draft only. No outreach copy, sends, CRM writes, or account changes.';

  async function reachBuildProposal(store) {
    await seedApprovedSession(store);
    await startCampaignPlanningConversation('int-campaign-1', { store });
    await postCampaignPlanningMessage(
      'int-campaign-1',
      'Keep property managers as defined.',
      { store }
    );
    await postCampaignPlanningMessage(
      'int-campaign-1',
      [
        'Prove that multi-family property managers will book walkthroughs.',
        'Inclusion: local HOA / multi-family managers with recurring needs.',
        'Exclusion: national property firms and lowest-price shoppers.',
      ].join(' '),
      { store }
    );
    return postCampaignPlanningMessage('int-campaign-1', APPROVAL_PLUS, {
      store,
    });
  }

  it('transcript: approve criteria → build proposal → approve → draft request advances to draft', async () => {
    const store = createMemoryStore();
    const build = await reachBuildProposal(store);
    assert.ok(build.prospectListBuildProposal);
    assert.equal(build.intent, 'produce_build_proposal');

    const approvedBuild = await postCampaignPlanningMessage(
      'int-campaign-1',
      'Approved',
      { store }
    );
    assert.equal(
      approvedBuild.campaignPlanning.step,
      'prospect_list_build_proposal_approved'
    );
    assert.equal(approvedBuild.intent, 'build_proposal_approved');
    assert.equal(
      approvedBuild.prospectListBuildProposal.status,
      'approved'
    );
    assert.doesNotMatch(
      approvedBuild.message,
      /Before building a prospect list, define what should qualify or disqualify/
    );

    const draft = await postCampaignPlanningMessage(
      'int-campaign-1',
      DRAFT_REQ,
      { store }
    );

    assert.ok(
      draft.intent === 'produce_prospect_list_draft' ||
        draft.campaignPlanning.step === 'prospect_list_draft_requested' ||
        draft.campaignPlanning.step === 'prospect_list_draft_generated'
    );
    assert.ok(
      draft.prospectListDraft ||
        draft.reviewableProspectListDraft ||
        draft.campaignPlanning.step === 'prospect_list_draft_requested' ||
        draft.campaignPlanning.step === 'prospect_list_draft_generated'
    );
    assert.equal(
      draft.prospectListDraft && draft.prospectListDraft.kind,
      'reviewable_prospect_list_draft'
    );
    assert.equal(draft.prospectListDraft.outreachCopyGenerated, false);
    assert.equal(draft.prospectListDraft.accountChangesMade, false);
    assert.equal(draft.prospectListDraft.crmWritesMade, false);
    assert.match(draft.message, /Reviewable Prospect List Draft/i);
    assert.doesNotMatch(
      draft.message,
      /Before building a prospect list, define what should qualify or disqualify/
    );

    const session = await store.getSession('int-campaign-1');
    const approved = session.interview_state.reasoningMemory.approvedArtifacts;
    assert.ok(
      approved.includes('prospect_list_criteria_preview') ||
        approved.includes('prospect_criteria')
    );
    assert.ok(approved.includes('prospect_list_build_proposal'));
  });

  it('does not re-ask criteria after criteria + build proposal approval', async () => {
    const store = createMemoryStore();
    await reachBuildProposal(store);
    await postCampaignPlanningMessage('int-campaign-1', 'Approved', { store });
    const draft = await postCampaignPlanningMessage(
      'int-campaign-1',
      DRAFT_REQ,
      { store }
    );
    assert.doesNotMatch(
      draft.message,
      /Before building a prospect list, define what should qualify or disqualify/
    );
    assert.notEqual(draft.intent, 'preview_approved');
    assert.notEqual(draft.campaignPlanning.currentAsk, 'prospectListCriteria');
  });

  it('exact confusion prompt advances to draft, never revise-criteria fallback', async () => {
    const store = createMemoryStore();
    await reachBuildProposal(store);
    await postCampaignPlanningMessage('int-campaign-1', 'Approved', { store });

    const confusion =
      'We already completed validation metrics and approved the Prospect List Build Proposal.\n\n' +
      'Current state:\n' +
      '- Prospect List Criteria Preview approved\n' +
      '- Prospect List Build Proposal approved\n' +
      '- Next step is a reviewable prospect list draft\n\n' +
      'Now generate the first reviewable prospect list batch...\n\n' +
      'This is a reviewable list draft only.\n' +
      'No outreach copy.\n' +
      'No sends.\n' +
      'No CRM writes.\n' +
      'No account changes.\n' +
      'No DNS / GBP / social / tracking changes.';

    const draft = await postCampaignPlanningMessage(
      'int-campaign-1',
      confusion,
      { store }
    );

    assert.doesNotMatch(draft.message, /revise the prospect-list criteria/i);
    assert.doesNotMatch(
      draft.message,
      /define what should qualify or disqualify/i
    );
    assert.doesNotMatch(draft.message, /^Prospect List Criteria Preview$/m);
    assert.notEqual(draft.intent, 'revise_criteria');
    assert.notEqual(draft.campaignPlanning.currentAsk, 'prospectListCriteria');
    assert.ok(
      draft.intent === 'produce_prospect_list_draft' ||
        draft.campaignPlanning.step === 'prospect_list_draft_requested' ||
        draft.campaignPlanning.step === 'prospect_list_draft_generated'
    );
    assert.equal(
      draft.prospectListDraft && draft.prospectListDraft.kind,
      'reviewable_prospect_list_draft'
    );
    assert.match(draft.message, /Reviewable Prospect List Draft/i);
  });

  it('unit: confusion prompt with declared state forces draft even if slots stale', () => {
    const {
      buildCampaignPlanningReply,
    } = require('../services/clientIntelligenceCampaignPlanning');
    const confusion =
      'We already completed validation metrics and approved the Prospect List Build Proposal.\n\n' +
      'Current state:\n' +
      '- Prospect List Criteria Preview approved\n' +
      '- Prospect List Build Proposal approved\n' +
      '- Next step is a reviewable prospect list draft\n\n' +
      'Now generate the first reviewable prospect list batch...';
    const reply = buildCampaignPlanningReply(
      confusion,
      {
        step: 'prospect_list_build_proposal',
        slots: {
          previewGenerated: true,
          previewApproved: true,
          criteriaGenerated: true,
          criteriaApproved: true,
          inclusionCriteria: ['local managers'],
          exclusionCriteria: ['national firms'],
          buildProposalGenerated: true,
          buildProposalApproved: true,
        },
        prospectListCriteriaPreview: {
          kind: 'prospect_list_criteria_preview',
          status: 'approved',
        },
        prospectListBuildProposal: {
          kind: 'prospect_list_build_proposal',
          status: 'approved',
        },
      },
      {
        businessName: 'Anchor Cleaning',
        primarySegment: 'property_managers',
        targetMarket: 'Greater Manchester',
      }
    );
    assert.doesNotMatch(reply.message, /revise the prospect-list criteria/i);
    assert.doesNotMatch(
      reply.message,
      /define what should qualify or disqualify/i
    );
    assert.equal(reply.intent, 'produce_prospect_list_draft');
    assert.ok(reply.prospectListDraft);
  });
});
