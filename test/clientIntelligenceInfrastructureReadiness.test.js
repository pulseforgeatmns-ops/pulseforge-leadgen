'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  ARTIFACT_KIND,
  OWNERS,
  READINESS_AREAS,
  SECTION_TITLES,
  TOP_SETUP_PRIORITIES,
  MANUAL_ESTIMATE_NOTE,
  buildEmptyAreas,
  buildInfrastructureReadinessOpening,
  buildInfrastructureReadinessReply,
  buildGrowthInfrastructureReadinessReport,
  formatReadinessReportMessage,
  formatOwnerLabel,
  containsForbiddenReadinessLanguage,
  applyAnswerToAreas,
  statusLabelForItem,
} = require('../services/clientIntelligenceInfrastructureReadiness');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
  startInfrastructureReadinessConversation,
  postInfrastructureReadinessMessage,
  getInterview,
} = require('../services/clientIntelligenceInterview');

const ANCHOR_BLUEPRINT = {
  id: 'bp-anchor-ready',
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

const ANSWERS = [
  'Aji Home Services',
  'Residential cleaning',
  'Homeowners',
  'Warehouses',
  'Greater Manchester',
  'We show up consistently',
  'Friendly and clear',
  'More recurring jobs',
  'Booked walkthroughs',
];

describe('Growth Infrastructure Readiness domain', () => {
  it('defines ten readiness areas with owner/priority on every item', () => {
    assert.equal(READINESS_AREAS.length, 10);
    for (const area of READINESS_AREAS) {
      assert.ok(area.items.length >= 5, area.id);
      for (const item of area.items) {
        assert.ok(OWNERS.includes(item.owner), `${item.id} owner`);
        assert.ok(['high', 'medium', 'low'].includes(item.priority));
      }
    }
  });

  it('opening is diagnostic and names Anchor without campaign language', () => {
    const opening = buildInfrastructureReadinessOpening(ANCHOR_BLUEPRINT);
    assert.match(opening, /Anchor Cleaning|Anchor/);
    assert.match(opening, /capture and convert/i);
    assert.match(opening, /website URL|domain/i);
    assert.match(opening, /never ask for passwords/i);
    assert.doesNotMatch(opening, /what(?:'| i)?s your password|send (?:me )?your password/i);
    assert.doesNotMatch(opening, /campaign is live|prospect list/i);
    assert.equal(containsForbiddenReadinessLanguage(opening), false);
  });

  it('opening with Growth Conversation handoff bridges before campaign/prospect list', () => {
    const opening = buildInfrastructureReadinessOpening(ANCHOR_BLUEPRINT, {
      businessName: 'Anchor Cleaning',
      growthHandoff: {
        primarySegment: 'property managers',
        secondarySegment: 'professional offices',
        targetMarket: 'Greater Manchester',
        conversionGoal:
          'qualified conversations, walkthroughs, estimate requests',
        proofNeeded: [
          'service checklist',
          'photos/examples',
          'clear response-time expectation',
          'service area',
          'walkthrough/estimate process',
        ],
        noCampaignOrProspectListYet: true,
      },
    });
    assert.match(
      opening,
      /Before we build a campaign or prospect list, I'd check whether Anchor Cleaning has the infrastructure/i
    );
    assert.match(opening, /First Growth Plan focus: property managers/i);
    assert.match(opening, /Secondary path: professional offices/i);
    assert.match(opening, /Greater Manchester/i);
    assert.match(opening, /clear response-time expectation/i);
    assert.match(opening, /No campaign or prospect list yet/i);
    assert.doesNotMatch(opening, /property_managers|professional_offices/);
    assert.equal(containsForbiddenReadinessLanguage(opening), false);
  });

  it('website/domain answer marks website + domain items from URL', () => {
    const areas = applyAnswerToAreas(
      buildEmptyAreas(),
      'website_domain',
      'Our site is https://anchorcleaning.example and we own the domain at Cloudflare.'
    );
    assert.equal(areas.website.items.website_exists.status, 'ready');
    assert.equal(areas.domain_dns.items.domain_owned.status, 'ready');
    assert.equal(areas.domain_dns.items.dns_provider_known.status, 'ready');
    // URL alone does not prove DNS connection — needs verification.
    assert.equal(areas.domain_dns.items.domain_connected.status, 'unknown');
    assert.equal(areas.website.items.website_exists.owner, 'max_can_check');
    assert.equal(areas.domain_dns.items.domain_owned.owner, 'client_required');
    assert.equal(areas.website.items.website_exists.source, 'client_stated');
  });

  it('does not mark binary facts partial/missing without evidence', () => {
    const urlOnly = applyAnswerToAreas(
      buildEmptyAreas(),
      'website_domain',
      'Site is https://anchorcleaning.example'
    );
    assert.equal(urlOnly.domain_dns.items.domain_owned.status, 'unknown');
    assert.equal(urlOnly.domain_dns.items.domain_connected.status, 'unknown');
    assert.equal(
      statusLabelForItem('domain_owned', urlOnly.domain_dns.items.domain_owned.status),
      'unconfirmed'
    );
    assert.equal(
      statusLabelForItem(
        'domain_connected',
        urlOnly.domain_dns.items.domain_connected.status
      ),
      'needs verification'
    );

    const reviews = applyAnswerToAreas(
      buildEmptyAreas(),
      'gbp',
      'GBP exists and is claimed. About 12 reviews.'
    );
    assert.equal(reviews.gbp.items.gbp_reviews.status, 'unknown');
    assert.equal(reviews.reviews.items.review_count.status, 'unknown');
    assert.match(
      reviews.gbp.items.gbp_reviews.evidence,
      /not independently checked/i
    );
    assert.equal(
      statusLabelForItem('gbp_reviews', reviews.gbp.items.gbp_reviews.status),
      'needs verification'
    );

    const noReviews = applyAnswerToAreas(
      buildEmptyAreas(),
      'gbp',
      'No Google Business Profile yet and no reviews.'
    );
    assert.equal(noReviews.gbp.items.gbp_reviews.status, 'unknown');
    assert.equal(noReviews.reviews.items.review_count.status, 'unknown');
    assert.notEqual(noReviews.gbp.items.gbp_reviews.status, 'missing');

    const vagueTracking = applyAnswerToAreas(
      buildEmptyAreas(),
      'tracking',
      'Not sure what tracking we have.'
    );
    assert.equal(vagueTracking.tracking.items.google_analytics.status, 'unknown');
    assert.notEqual(vagueTracking.tracking.items.google_analytics.status, 'missing');

    const noGa = applyAnswerToAreas(
      buildEmptyAreas(),
      'tracking',
      'No Google Analytics, no Search Console, no UTMs.'
    );
    assert.equal(noGa.tracking.items.google_analytics.status, 'unknown');
    assert.equal(noGa.tracking.items.search_console.status, 'unknown');
    assert.equal(noGa.tracking.items.utm_discipline.status, 'missing');
    assert.equal(
      statusLabelForItem('google_analytics', noGa.tracking.items.google_analytics.status),
      'needs verification'
    );
    assert.equal(
      statusLabelForItem('search_console', noGa.tracking.items.search_console.status),
      'needs verification'
    );

    const mixedTracking = applyAnswerToAreas(
      buildEmptyAreas(),
      'tracking',
      'We have GA4 but no UTMs or call tracking yet.'
    );
    assert.equal(mixedTracking.tracking.items.google_analytics.status, 'ready');
    assert.equal(mixedTracking.tracking.items.utm_discipline.status, 'missing');
    assert.equal(mixedTracking.tracking.items.call_tracking.status, 'missing');
  });

  it('marks manual estimate process as partial, not missing', () => {
    const areas = applyAnswerToAreas(
      buildEmptyAreas(),
      'estimates',
      'Estimates are handled manually with walkthroughs. No proposal template yet.'
    );
    assert.equal(areas.sales_process.items.estimate_process.status, 'partial');
    assert.equal(areas.sales_process.items.estimate_process.evidence, MANUAL_ESTIMATE_NOTE);
    assert.match(
      areas.sales_process.items.estimate_process.evidence,
      /proposal template and follow-up cadence need setup/i
    );
  });

  it('formats owner labels for display', () => {
    assert.equal(formatOwnerLabel('client_required'), 'Client/operator');
    assert.equal(formatOwnerLabel('operator_guided'), 'Operator guided');
    assert.equal(formatOwnerLabel('max_can_check'), 'Max can check');
  });

  it('uses clean status labels for domain and branded email', () => {
    assert.equal(statusLabelForItem('domain_owned', 'ready'), 'confirmed');
    assert.equal(statusLabelForItem('domain_owned', 'missing'), 'not owned');
    assert.equal(statusLabelForItem('domain_owned', 'unknown'), 'unconfirmed');
    assert.equal(statusLabelForItem('domain_connected', 'ready'), 'connected');
    assert.equal(statusLabelForItem('domain_connected', 'missing'), 'not connected');
    assert.equal(
      statusLabelForItem('domain_connected', 'unknown'),
      'needs verification'
    );
    assert.equal(statusLabelForItem('branded_email', 'ready'), 'present');
    assert.equal(statusLabelForItem('branded_email', 'missing'), 'not present');
    assert.equal(
      statusLabelForItem('branded_email', 'missing', { domainOwnedReady: true }),
      'needs setup'
    );
    assert.equal(statusLabelForItem('branded_email', 'unknown'), 'unknown');
  });

  it('builds a Growth Infrastructure Readiness Report with required sections', () => {
    let areas = buildEmptyAreas();
    areas = applyAnswerToAreas(
      areas,
      'website_domain',
      'No website yet. We own the domain but use Gmail.'
    );
    areas = applyAnswerToAreas(
      areas,
      'gbp',
      'No Google Business Profile yet and no reviews.'
    );
    areas = applyAnswerToAreas(
      areas,
      'lead_flow',
      'Leads come by phone into a personal cell. No CRM — just the inbox. No missed-lead process.'
    );
    areas = applyAnswerToAreas(
      areas,
      'estimates',
      'Estimates are handled manually with walkthroughs.'
    );
    areas = applyAnswerToAreas(
      areas,
      'tracking',
      'No Google Analytics, no Search Console, no UTMs.'
    );
    const report = buildGrowthInfrastructureReadinessReport(areas, {
      businessName: 'Anchor Cleaning',
      blueprintId: 'bp-anchor-ready',
      blueprintVersion: '1.0',
    });
    assert.equal(report.kind, ARTIFACT_KIND);
    assert.equal(report.title, 'Growth Infrastructure Readiness Report');
    assert.ok(['ready', 'partial', 'not_ready', 'unknown'].includes(report.overallStatus));
    assert.match(
      report.executiveSummary,
      /Before outreach, Anchor should confirm three things/i
    );
    assert.match(report.executiveSummary, /trust signals are visible/i);
    assert.deepEqual(report.topSetupPriorities, [...TOP_SETUP_PRIORITIES]);
    assert.ok(Array.isArray(report.demandCaptureRisks));
    assert.ok(Array.isArray(report.trustDiscoverabilityGaps));
    assert.ok(Array.isArray(report.trackingGaps));
    assert.ok(Array.isArray(report.conversionFollowUpGaps));
    assert.ok(Array.isArray(report.maxCanCheck));
    assert.ok(Array.isArray(report.operatorClientMustComplete));
    assert.ok(report.recommendedSetupSequence.length >= 1);
    assert.equal(report.campaignsGenerated, false);
    assert.equal(report.assessmentOnly, true);
    assert.match(report.disclaimer, /Assessment only/i);
    assert.match(report.disclaimer, /without explicit approval/i);
    assert.match(report.disclaimer, /No passwords requested/i);
    assert.match(report.disclaimer, /No campaigns or prospect lists generated/i);
    assert.equal(
      report.sectionTitles.demandCaptureRisks,
      SECTION_TITLES.demandCaptureRisks
    );
    assert.equal(report.sectionTitles.demandCaptureRisks, 'Can prospects reach you?');
    assert.equal(
      report.sectionTitles.trustDiscoverabilityGaps,
      'Can prospects trust you?'
    );
    assert.equal(
      report.sectionTitles.trackingGaps,
      'Can we measure what works?'
    );
    assert.equal(
      report.sectionTitles.conversionFollowUpGaps,
      'Can inquiries become booked opportunities?'
    );
    assert.equal(report.sectionTitles.topSetupPriorities, 'Top Setup Priorities');
    // Practical blockers first: domain ownership before tracking/sales.
    assert.equal(report.recommendedSetupSequence[0].itemId, 'domain_connected');
    assert.ok(
      report.recommendedSetupSequence.some((s) => s.itemId === 'branded_email')
    );
    assert.ok(
      report.operatorClientMustComplete.some((g) => g.owner === 'client_required')
    );
    assert.ok(
      report.maxCanCheck.some((g) => g.owner === 'max_can_check') ||
        report.demandCaptureRisks.some((g) => g.owner === 'max_can_check')
    );
    assert.equal(
      areas.sales_process.items.estimate_process.status,
      'partial'
    );

    const formatted = formatReadinessReportMessage(report);
    assert.match(formatted, /Before outreach, Anchor should confirm three things/i);
    assert.match(formatted, /Top Setup Priorities/i);
    assert.match(formatted, /Confirm domain \+ website connection/i);
    assert.match(formatted, /Can prospects reach you\?/i);
    assert.match(formatted, /Can prospects trust you\?/i);
    assert.match(formatted, /Can we measure what works\?/i);
    assert.match(formatted, /Can inquiries become booked opportunities\?/i);
    assert.doesNotMatch(formatted, /Demand capture risks/i);
    assert.doesNotMatch(formatted, /\bclient_required\b/);
    assert.doesNotMatch(formatted, /\boperator_guided\b/);
    assert.doesNotMatch(formatted, /\bmax_can_check\b/);
    assert.match(formatted, /Client\/operator|Operator guided|Max can check/);
    assert.match(formatted, /no passwords requested/i);
    assert.match(formatted, /No campaigns or prospect lists generated/i);
  });

  it('reply advances steps then produces report without forbidden language', () => {
    let state = {
      status: 'active',
      step: 'website_domain',
      answers: {},
      areas: buildEmptyAreas(),
    };
    const steps = [
      ['website_domain', 'Site is https://anchorcleaning.example — domain owned at Cloudflare.'],
      ['gbp', 'GBP exists and is claimed. About 12 reviews. Photos are thin.'],
      ['lead_flow', 'Phone and form leads go into our CRM. Response same day. Missed calls get a callback process.'],
      ['estimates', 'We do walkthroughs, have pricing inputs, and follow up twice.'],
      ['tracking', 'We have GA4 but no UTMs or call tracking yet.'],
      ['assets', 'Logo and photos ready. Facebook present. No before/after yet.'],
    ];
    let report = null;
    for (const [expectedStep, msg] of steps) {
      assert.equal(state.step, expectedStep);
      const reply = buildInfrastructureReadinessReply(msg, state, ANCHOR_BLUEPRINT, {
        businessName: 'Anchor Cleaning',
      });
      assert.equal(containsForbiddenReadinessLanguage(reply.message), false);
      // Guardrail may say "No passwords requested"; forbid asking for one.
      assert.doesNotMatch(
        reply.message,
        /what(?:'| i)?s your password|send (?:me )?your password|login password/i
      );
      // Guardrail may say "No campaigns or prospect lists generated".
      assert.doesNotMatch(
        reply.message,
        /campaign is live|I built a prospect list|launching outreach now/i
      );
      state = {
        ...state,
        step: reply.step,
        answers: reply.answers,
        areas: reply.areas,
      };
      if (reply.report) report = reply.report;
    }
    assert.equal(state.step, 'report');
    assert.ok(report);
    assert.equal(report.kind, ARTIFACT_KIND);
    assert.ok(report.recommendedSetupSequence.length >= 1);
  });
});

describe('approve → Infrastructure Readiness conversation', () => {
  it('requires approved Blueprint and exposes report on getInterview', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const started = await startClientInterview({ clientId: 410 }, opts);

    await assert.rejects(
      () => startInfrastructureReadinessConversation(started.interviewId, opts),
      (err) => err && err.code === 'invalid_status'
    );

    let turn = started;
    for (const a of ANSWERS) {
      turn = await postInterviewMessage(started.interviewId, a, opts);
    }
    await approveBlueprint(turn.blueprint.id, opts);

    const ready = await startInfrastructureReadinessConversation(
      started.interviewId,
      opts
    );
    assert.equal(ready.status, 'INFRASTRUCTURE_READINESS');
    assert.match(ready.message, /capture and convert/i);
    assert.ok(ready.infrastructureReadiness);
    assert.equal(ready.infrastructureReadiness.status, 'active');
    assert.equal(ready.blueprint.status, 'approved');

    const mid = await postInfrastructureReadinessMessage(
      started.interviewId,
      'Website is https://anchorcleaning.example and we own the domain.',
      opts
    );
    assert.equal(mid.status, 'INFRASTRUCTURE_READINESS');
    assert.equal(
      mid.infrastructureReadiness.areas.website.items.website_exists.status,
      'ready'
    );

    const wrap = await postInfrastructureReadinessMessage(
      started.interviewId,
      'Please wrap up with the readiness report.',
      opts
    );
    assert.ok(wrap.growthInfrastructureReadinessReport);
    assert.equal(
      wrap.growthInfrastructureReadinessReport.kind,
      ARTIFACT_KIND
    );
    assert.equal(wrap.infrastructureReadiness.status, 'report_ready');
    assert.equal(wrap.growthInfrastructureReadinessReport.campaignsGenerated, false);

    const detail = await getInterview(started.interviewId, opts);
    assert.ok(detail.infrastructureReadiness);
    assert.ok(detail.growthInfrastructureReadinessReport);
    assert.equal(
      detail.growthInfrastructureReadinessReport.kind,
      ARTIFACT_KIND
    );
  });
});

describe('client-intel UI markers for infrastructure readiness', () => {
  it('surfaces Check Growth Infrastructure CTA and report sections', () => {
    const uiSource = fs.readFileSync(
      path.join(__dirname, '..', 'public', 'client-intel.html'),
      'utf8'
    );
    assert.match(uiSource, /Check Growth Infrastructure/);
    assert.match(uiSource, /startInfrastructureReadiness|\/readiness\/start/);
    assert.match(uiSource, /Growth Infrastructure Readiness Report/);
    assert.match(uiSource, /Can prospects reach you\?/);
    assert.match(uiSource, /Can prospects trust you\?/);
    assert.match(uiSource, /Can we measure what works\?/);
    assert.match(uiSource, /Can inquiries become booked opportunities\?/);
    assert.doesNotMatch(uiSource, /Demand capture risks/);
    assert.match(uiSource, /Recommended setup sequence/);
    assert.match(uiSource, /Top Setup Priorities/);
    assert.match(uiSource, /executiveSummary|growth-exec-summary/);
    assert.match(uiSource, /formatOwnerLabel/);
    assert.match(uiSource, /statusLabel/);
    assert.match(uiSource, /phase === 'readiness'|setPhase\('readiness'\)/);
    assert.match(uiSource, /growthPreviewActions/);
    assert.match(uiSource, /Use this focus/);
    assert.match(uiSource, /Refine first segment/);
  });
});
