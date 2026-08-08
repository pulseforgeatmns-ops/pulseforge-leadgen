'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  ARTIFACT_KIND,
  OWNERS,
  READINESS_AREAS,
  buildEmptyAreas,
  buildInfrastructureReadinessOpening,
  buildInfrastructureReadinessReply,
  buildGrowthInfrastructureReadinessReport,
  containsForbiddenReadinessLanguage,
  applyAnswerToAreas,
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

  it('website/domain answer marks website + domain items from URL', () => {
    const areas = applyAnswerToAreas(
      buildEmptyAreas(),
      'website_domain',
      'Our site is https://anchorcleaning.example and we own the domain at Cloudflare.'
    );
    assert.equal(areas.website.items.website_exists.status, 'ready');
    assert.equal(areas.domain_dns.items.domain_owned.status, 'ready');
    assert.equal(areas.domain_dns.items.dns_provider_known.status, 'ready');
    assert.equal(areas.website.items.website_exists.owner, 'max_can_check');
    assert.equal(areas.domain_dns.items.domain_owned.owner, 'client_required');
    assert.equal(areas.website.items.website_exists.source, 'client_stated');
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
    assert.ok(Array.isArray(report.demandCaptureRisks));
    assert.ok(Array.isArray(report.trustDiscoverabilityGaps));
    assert.ok(Array.isArray(report.trackingGaps));
    assert.ok(Array.isArray(report.conversionFollowUpGaps));
    assert.ok(Array.isArray(report.maxCanCheck));
    assert.ok(Array.isArray(report.operatorClientMustComplete));
    assert.ok(report.recommendedSetupSequence.length >= 1);
    assert.equal(report.campaignsGenerated, false);
    assert.match(report.disclaimer, /Assessment only/i);
    assert.ok(
      report.operatorClientMustComplete.some((g) => g.owner === 'client_required')
    );
    assert.ok(
      report.maxCanCheck.some((g) => g.owner === 'max_can_check') ||
        report.demandCaptureRisks.some((g) => g.owner === 'max_can_check')
    );
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
      assert.doesNotMatch(reply.message, /password/i);
      assert.doesNotMatch(reply.message, /campaign is live|prospect list/i);
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
    assert.match(uiSource, /Demand capture risks/);
    assert.match(uiSource, /Recommended setup sequence/);
    assert.match(uiSource, /phase === 'readiness'|setPhase\('readiness'\)/);
  });
});
