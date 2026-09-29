'use strict';

/**
 * SPEC-259 — Qualified paid lead → canonical revenue opportunity admission.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const pool = require('../db');
const {
  applyQualificationDecision,
  QUALIFICATION_STATUS,
  ERROR: REVIEW_ERROR,
} = require('../lib/leadQualificationReview');
const {
  admitOpportunityFromQualificationReview,
  admissionIdempotencyKey,
  mapFirstPartyAttributionStatus,
  buildAttributionMetadataSnapshot,
  ERROR,
} = require('../lib/leadQualificationOpportunityAdmission');
const { captureWalkthroughLead } = require('../lib/walkthroughCapture');
const { buildAttributionRecord, SOURCE_KIND } = require('../lib/walkthroughAttribution');
const { validateWalkthroughPayload } = require('../lib/walkthroughValidate');
const { normalizeLeadSource } = require('../utils/revenueDomain');
const leadQualificationRouter = require('../routes/leadQualificationReviews');
const {
  createRevenueAdmissionMockPool,
  qualificationReviews,
  WALKTHROUGH_MOCK_PROSPECT_ID: PROSPECT_A,
} = require('./helpers/revenueAdmissionMockPool');

const CLIENT_10 = 10;
const CLIENT_1 = 1;
const OPERATOR = { id: 42, name: 'Jacob Maynard', role: 'admin', email: 'jacob@gopulseforge.com' };
const ADMISSION_INPUT = {
  estimatedValueCents: 450000,
  serviceType: 'commercial cleaning',
};

const ENABLED_FLAGS = {
  revenue_schema_enabled: true,
  revenue_operator_reads_enabled: true,
  revenue_operator_writes_enabled: true,
  revenue_max_reads_enabled: false,
  revenue_followup_recommendations_enabled: false,
};

const DISABLED_WRITE_FLAGS = {
  ...ENABLED_FLAGS,
  revenue_operator_writes_enabled: false,
};

function basePayload(overrides = {}) {
  return {
    name: 'Alex Owner',
    business_name: 'Riverside Law',
    phone: '(603) 555-0142',
    email: 'alex@riverside.example',
    city: 'Manchester',
    space_type: 'law_office',
    company_website: '',
    ...overrides,
  };
}

function chatgptAttribution(overrides = {}) {
  return buildAttributionRecord({
    campaign_id: 'cmpn_abc123',
    ad_group_id: 'ag-259',
    ad_id: 'ad-259',
    opref: 'opref-259',
    oppref: 'oppref-259',
    utm_source: 'chatgpt',
    ...overrides,
  });
}

function inferredAttribution() {
  return buildAttributionRecord({
    utm_medium: 'organic',
    referrer: 'https://www.google.com/search?q=cleaning',
    landing_page_url: 'https://anchor.example/walkthrough',
  });
}

function unattributedAttribution() {
  return buildAttributionRecord({
    landing_page_url: 'https://anchor.example/walkthrough',
  });
}

function patchPool(db) {
  pool.query = db.query.bind(db);
  pool.connect = db.connect.bind(db);
}

async function seedQualifiedReview(db, attribution = chatgptAttribution()) {
  patchPool(db);
  const values = validateWalkthroughPayload(basePayload()).values;
  await captureWalkthroughLead(values, attribution);
  const reviewId = qualificationReviews(db.state)[0].id;
  const decision = await applyQualificationDecision(db, {
    reviewId,
    action: 'QUALIFY',
    operator: OPERATOR,
    clientId: CLIENT_10,
  });
  return { reviewId, payload: decision.payload, review: qualificationReviews(db.state)[0] };
}

async function admit(db, reviewId, input = ADMISSION_INPUT, flags = ENABLED_FLAGS) {
  return admitOpportunityFromQualificationReview(db, {
    reviewId,
    clientId: CLIENT_10,
    operator: OPERATOR,
    revenueFlags: flags,
    ...input,
  });
}

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise((resolve) => {
    server.on('listening', () => {
      const { port } = server.address();
      resolve({ port, close: () => new Promise((r) => server.close(r)) });
    });
  });
}

describe('SPEC-259 — admission preconditions', () => {
  let originalQuery;
  let originalConnect;

  beforeEach(() => {
    originalQuery = pool.query;
    originalConnect = pool.connect;
  });

  afterEach(() => {
    pool.query = originalQuery;
    pool.connect = originalConnect;
  });

  it('QUALIFIED_BY_OPERATOR review is admissible', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId);
    assert.equal(result.ok, true);
    assert.equal(result.opportunity.stage, 'identified');
  });

  it('NURTURE review is rejected', async () => {
    const db = createRevenueAdmissionMockPool();
    patchPool(db);
    const values = validateWalkthroughPayload(basePayload()).values;
    await captureWalkthroughLead(values, chatgptAttribution());
    const reviewId = qualificationReviews(db.state)[0].id;
    await applyQualificationDecision(db, { reviewId, action: 'NURTURE', operator: OPERATOR, clientId: CLIENT_10 });
    const result = await admit(db, reviewId);
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.REVIEW_NOT_QUALIFIED);
  });

  it('DISQUALIFY review is rejected', async () => {
    const db = createRevenueAdmissionMockPool();
    patchPool(db);
    const values = validateWalkthroughPayload(basePayload()).values;
    await captureWalkthroughLead(values, chatgptAttribution());
    const reviewId = qualificationReviews(db.state)[0].id;
    await applyQualificationDecision(db, { reviewId, action: 'DISQUALIFY', operator: OPERATOR, clientId: CLIENT_10 });
    const result = await admit(db, reviewId);
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.REVIEW_NOT_QUALIFIED);
  });

  it('pending review is rejected', async () => {
    const db = createRevenueAdmissionMockPool();
    patchPool(db);
    const values = validateWalkthroughPayload(basePayload()).values;
    await captureWalkthroughLead(values, chatgptAttribution());
    const reviewId = qualificationReviews(db.state)[0].id;
    const result = await admit(db, reviewId);
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.REVIEW_NOT_EXECUTABLE);
  });

  it('review from another client is rejected', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admitOpportunityFromQualificationReview(db, {
      reviewId,
      clientId: CLIENT_1,
      operator: OPERATOR,
      revenueFlags: ENABLED_FLAGS,
      ...ADMISSION_INPUT,
    });
    assert.equal(result.ok, false);
    assert.equal(result.error, REVIEW_ERROR.REVIEW_NOT_FOUND);
  });

  it('missing prospect is rejected', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const review = qualificationReviews(db.state)[0];
    review.payload = { ...review.payload, prospect_id: null };
    const result = await admit(db, reviewId);
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.PROSPECT_NOT_FOUND);
  });

  it('prospect from another client is rejected', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    db.state.prospects[0].client_id = CLIENT_1;
    const result = await admit(db, reviewId);
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.PROSPECT_CLIENT_MISMATCH);
  });
});

describe('SPEC-259 — required operator input', () => {
  let originalQuery;
  let originalConnect;

  beforeEach(() => {
    originalQuery = pool.query;
    originalConnect = pool.connect;
  });

  afterEach(() => {
    pool.query = originalQuery;
    pool.connect = originalConnect;
  });

  it('estimatedValueCents is required', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId, { serviceType: 'commercial cleaning' });
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.ESTIMATED_VALUE_REQUIRED);
  });

  it('serviceType is required', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId, { estimatedValueCents: 450000 });
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.SERVICE_TYPE_REQUIRED);
  });

  it('value is never invented', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId, { serviceType: 'commercial cleaning' });
    assert.equal(db.state.opportunities.length, 0);
    assert.equal(result.ok, false);
  });

  it('service type is never invented', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId, { estimatedValueCents: 450000 });
    assert.equal(db.state.opportunities.length, 0);
    assert.equal(result.ok, false);
  });
});

describe('SPEC-259 — revenue source and attribution mapping', () => {
  let originalQuery;
  let originalConnect;

  beforeEach(() => {
    originalQuery = pool.query;
    originalConnect = pool.connect;
  });

  afterEach(() => {
    pool.query = originalQuery;
    pool.connect = originalConnect;
  });

  it('chatgpt_ads is accepted by revenue lead-source normalization', () => {
    assert.equal(normalizeLeadSource('chatgpt_ads'), 'chatgpt_ads');
  });

  it('source becomes chatgpt_ads for deterministic ChatGPT attribution', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db, chatgptAttribution());
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.source, 'chatgpt_ads');
  });

  it('lead_source_detail = website_walkthrough', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.lead_source_detail, 'website_walkthrough');
  });

  it('deterministic stays deterministic', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db, chatgptAttribution());
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.attribution_status, 'deterministic');
    assert.notEqual(result.opportunity.attribution_status, 'confirmed');
  });

  it('inferred stays inferred', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db, inferredAttribution());
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.attribution_status, 'inferred');
  });

  it('unattributed stays unattributed', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db, unattributedAttribution());
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.attribution_status, 'unattributed');
  });

  it('first-party attribution never becomes confirmed', () => {
    assert.equal(mapFirstPartyAttributionStatus('deterministic'), 'deterministic');
    assert.equal(mapFirstPartyAttributionStatus('confirmed'), 'unattributed');
  });
});

describe('SPEC-259 — external attribution persistence', () => {
  let originalQuery;
  let originalConnect;

  beforeEach(() => {
    originalQuery = pool.query;
    originalConnect = pool.connect;
  });

  afterEach(() => {
    pool.query = originalQuery;
    pool.connect = originalConnect;
  });

  it('external campaign ID is NOT written to opportunities.campaign_id', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db, chatgptAttribution({ campaign_id: 'cmpn_ext_259' }));
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.campaign_id, null);
  });

  it('opportunities.campaign_id remains null without internal resolver', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.campaign_id, null);
  });

  it('external campaign/ad IDs persist in attribution_metadata', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId, payload } = await seedQualifiedReview(db, chatgptAttribution());
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.attribution_metadata.external.campaign_id, 'cmpn_abc123');
    assert.equal(result.opportunity.attribution_metadata.external.ad_id, 'ad-259');
    assert.equal(result.opportunity.attribution_metadata.lineage.qualification_review_id, reviewId);
    assert.equal(
      result.opportunity.attribution_metadata.lineage.originating_walkthrough_action_id,
      payload.originating_agent_action_id
    );
  });

  it('opref/oppref persist', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.attribution_metadata.external.opref, 'opref-259');
    assert.equal(result.opportunity.attribution_metadata.external.oppref, 'oppref-259');
  });

  it('FIRST_PARTY_ATTRIBUTION provenance persists', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.attribution_metadata.sourceKind, SOURCE_KIND);
    assert.notEqual(result.opportunity.attribution_metadata.sourceKind, 'PLATFORM_API');
  });

  it('qualification_review_id persists', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.attribution_metadata.lineage.qualification_review_id, reviewId);
  });

  it('originating walkthrough action ID persists', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId, payload } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId);
    assert.equal(
      result.opportunity.attribution_metadata.lineage.originating_walkthrough_action_id,
      payload.originating_agent_action_id
    );
  });
});

describe('SPEC-259 — opportunity semantics and dedup', () => {
  let originalQuery;
  let originalConnect;

  beforeEach(() => {
    originalQuery = pool.query;
    originalConnect = pool.connect;
  });

  afterEach(() => {
    pool.query = originalQuery;
    pool.connect = originalConnect;
  });

  it('opportunity starts at identified', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId);
    assert.equal(result.opportunity.stage, 'identified');
  });

  it('no auto-transition to qualified', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId);
    assert.notEqual(result.opportunity.stage, 'qualified');
  });

  it('no customer is created', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    await admit(db, reviewId);
    assert.equal(db.state.customers?.length || 0, 0);
  });

  it('same review replay does not create duplicate opportunity', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const first = await admit(db, reviewId);
    const second = await admit(db, reviewId);
    assert.equal(first.ok, true);
    assert.equal(second.ok, true);
    assert.equal(second.idempotent, true);
    assert.equal(db.state.opportunities.length, 1);
    assert.equal(first.opportunity.id, second.opportunity.id);
  });

  it('existing non-terminal opportunity blocks admission from different review', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    db.state.opportunities.push({
      id: '00000000-0000-4000-8000-000000000099',
      client_id: CLIENT_10,
      prospect_id: PROSPECT_A,
      stage: 'contacted',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      attribution_metadata: { lineage: { qualification_review_id: 'other-review' } },
    });
    const result = await admit(db, reviewId);
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.ALREADY_HAS_OPEN_OPPORTUNITY);
  });

  it('prior won opportunity does not silently create a new opportunity', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    db.state.opportunities.push({
      id: '00000000-0000-4000-8000-000000000088',
      client_id: CLIENT_10,
      prospect_id: PROSPECT_A,
      stage: 'won',
      closed_at: new Date(Date.now() - 86400000).toISOString(),
      created_at: new Date(Date.now() - 86400000 * 2).toISOString(),
      updated_at: new Date(Date.now() - 86400000).toISOString(),
    });
    const result = await admit(db, reviewId);
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.PRIOR_WON_OPPORTUNITY);
    assert.equal(db.state.opportunities.length, 1);
  });

  it('lost/cancelled allows admission when review is newer than terminal opportunity', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId, review } = await seedQualifiedReview(db);
    db.state.opportunities.push({
      id: '00000000-0000-4000-8000-000000000077',
      client_id: CLIENT_10,
      prospect_id: PROSPECT_A,
      stage: 'lost',
      closed_at: new Date(Date.now() - 86400000 * 10).toISOString(),
      created_at: new Date(Date.now() - 86400000 * 20).toISOString(),
      updated_at: new Date(Date.now() - 86400000 * 10).toISOString(),
    });
    review.executed_at = new Date().toISOString();
    const result = await admit(db, reviewId);
    assert.equal(result.ok, true);
    assert.equal(db.state.opportunities.length, 2);
  });

  it('lost/cancelled blocks admission when review is older than terminal opportunity', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId, review } = await seedQualifiedReview(db);
    const terminalAt = new Date().toISOString();
    db.state.opportunities.push({
      id: '00000000-0000-4000-8000-000000000066',
      client_id: CLIENT_10,
      prospect_id: PROSPECT_A,
      stage: 'cancelled',
      closed_at: terminalAt,
      created_at: new Date(Date.now() - 86400000).toISOString(),
      updated_at: terminalAt,
    });
    review.executed_at = new Date(Date.now() - 86400000 * 5).toISOString();
    const result = await admit(db, reviewId);
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.TERMINAL_OPPORTUNITY_BLOCKS_ADMISSION);
  });
});

describe('SPEC-259 — feature flags and events', () => {
  let originalQuery;
  let originalConnect;

  beforeEach(() => {
    originalQuery = pool.query;
    originalConnect = pool.connect;
  });

  afterEach(() => {
    pool.query = originalQuery;
    pool.connect = originalConnect;
  });

  it('revenue feature flags are enforced', async () => {
    const db = createRevenueAdmissionMockPool({ featureFlags: DISABLED_WRITE_FLAGS });
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId, ADMISSION_INPUT, DISABLED_WRITE_FLAGS);
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.REVENUE_OPERATOR_WRITES_DISABLED);
  });

  it('disabled flags block admission', async () => {
    const db = createRevenueAdmissionMockPool({
      featureFlags: { ...ENABLED_FLAGS, revenue_schema_enabled: false },
    });
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId, ADMISSION_INPUT, { ...ENABLED_FLAGS, revenue_schema_enabled: false });
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.REVENUE_SCHEMA_DISABLED);
  });

  it('existing revenue_event is emitted', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    await admit(db, reviewId);
    assert.equal(db.state.revenueEvents.length, 1);
    assert.equal(db.state.revenueEvents[0].event_type, 'opportunity_created');
  });

  it('event retains attribution lineage', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    await admit(db, reviewId);
    const payload = db.state.revenueEvents[0].payload_json;
    assert.equal(payload.admission.qualification_review_id, reviewId);
    assert.equal(payload.admission.first_party_attribution.sourceKind, SOURCE_KIND);
  });

  it('review payload records opportunityAdmission after success', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);
    const result = await admit(db, reviewId);
    assert.equal(result.payload.opportunityAdmission.status, 'ADMITTED');
    assert.equal(result.payload.opportunityAdmission.opportunityId, result.opportunity.id);
    assert.equal(result.payload.qualification_status, QUALIFICATION_STATUS.QUALIFIED_BY_OPERATOR);
  });

  it('admission idempotency key is review-scoped', () => {
    assert.equal(admissionIdempotencyKey('review-123'), 'qualified_review_opportunity:review-123');
  });
});

describe('SPEC-259 — HTTP opportunity route', () => {
  let originalQuery;
  let originalConnect;
  let server;

  beforeEach(() => {
    originalQuery = pool.query;
    originalConnect = pool.connect;
  });

  afterEach(async () => {
    pool.query = originalQuery;
    pool.connect = originalConnect;
    if (server) await server.close();
  });

  it('rejects body-supplied client and attribution authority fields', async () => {
    const db = createRevenueAdmissionMockPool();
    const { reviewId } = await seedQualifiedReview(db);

    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.session = { active_client_id: CLIENT_10, user: OPERATOR };
      req.user = OPERATOR;
      next();
    });
    app.use(leadQualificationRouter);
    server = await listen(app);

    const res = await fetch(`http://127.0.0.1:${server.port}/api/v1/lead-qualification-reviews/${reviewId}/opportunity`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({
        ...ADMISSION_INPUT,
        client_id: CLIENT_1,
        source: 'manual',
        campaignId: 'cmpn_hijack',
      }),
    });
    assert.equal(res.status, 400);
  });
});

describe('SPEC-259 — attribution metadata builder', () => {
  it('buildAttributionMetadataSnapshot preserves lineage fields', () => {
    const payload = {
      prospect_id: PROSPECT_A,
      originating_agent_action_id: 'walk-1',
      attribution: chatgptAttribution(),
    };
    const snapshot = buildAttributionMetadataSnapshot('review-1', payload);
    assert.equal(snapshot.lineage.prospect_id, PROSPECT_A);
    assert.equal(snapshot.normalized.lead_source, 'chatgpt_ads');
  });
});
