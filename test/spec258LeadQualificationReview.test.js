'use strict';

/**
 * SPEC-258 — Paid web lead qualification admission boundary.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const pool = require('../db');
const {
  ACTION_TYPE,
  QUALIFICATION_STATUS,
  DECISION_ACTIONS,
  ERROR,
  ensureQualificationReviewForWalkthrough,
  applyQualificationDecision,
  buildContactSummary,
} = require('../lib/leadQualificationReview');
const { captureWalkthroughLead, ANCHOR_CLIENT_ID } = require('../lib/walkthroughCapture');
const { buildAttributionRecord, SOURCE_KIND } = require('../lib/walkthroughAttribution');
const { validateWalkthroughPayload } = require('../lib/walkthroughValidate');
const leadQualificationRouter = require('../routes/leadQualificationReviews');
const {
  createWalkthroughCaptureMockPool,
  walkthroughActions,
  qualificationReviews,
  WALKTHROUGH_MOCK_PROSPECT_ID: PROSPECT_A,
  WALKTHROUGH_MOCK_ACTION_ID: ACTION_1,
  WALKTHROUGH_MOCK_REVIEW_ID: REVIEW_1,
} = require('./helpers/walkthroughCaptureMockPool');

const CLIENT_10 = 10;
const CLIENT_1 = 1;
const ACTION_2 = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const OPERATOR = { id: 42, name: 'Jacob Maynard', role: 'admin', email: 'jacob@gopulseforge.com' };

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

function attributionRecord(overrides = {}) {
  return buildAttributionRecord({
    campaign_id: 'camp-258',
    ad_group_id: 'ag-258',
    ad_id: 'ad-258',
    opref: 'opref-258',
    oppref: 'oppref-258',
    utm_source: 'chatgpt',
    ...overrides,
  });
}

function createMockPool(initial = {}) {
  const db = createWalkthroughCaptureMockPool(initial);
  const originalQuery = db.query.bind(db);
  db.query = async (sql, params = []) => {
    const text = String(sql);
    if (/FROM opportunities|INSERT INTO opportunities|INSERT INTO customers|INSERT INTO revenue_|INSERT INTO ao_leads|UPDATE prospects[\s\S]*setter_status/i.test(text)) {
      throw new Error(`Unexpected downstream write: ${text.slice(0, 100)}`);
    }
    return originalQuery(sql, params);
  };
  return db;
}

async function seedWalkthroughWithReview(db, overrides = {}) {
  pool.query = db.query.bind(db);
  const values = validateWalkthroughPayload(basePayload()).values;
  const record = attributionRecord(overrides.attribution);
  const stored = await captureWalkthroughLead(values, record);
  const review = qualificationReviews(db.state)[0];
  return { stored, review, reviewId: review.id, values, record };
}

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise((resolve) => {
    server.on('listening', () => {
      const { port } = server.address();
      resolve({
        port,
        close: () => new Promise((r) => server.close(r)),
      });
    });
  });
}

describe('SPEC-258 — qualification review creation', () => {
  let originalQuery;

  beforeEach(() => {
    originalQuery = pool.query;
  });

  afterEach(() => {
    pool.query = originalQuery;
  });

  it('linked website prospect gets a qualification review', async () => {
    const db = createMockPool();
    const { review } = await seedWalkthroughWithReview(db);
    assert.ok(review);
    assert.equal(review.action_type, ACTION_TYPE);
    assert.equal(review.status, 'pending');
  });

  it('review references prospect_id', async () => {
    const db = createMockPool();
    const { review } = await seedWalkthroughWithReview(db);
    assert.equal(review.payload.prospect_id, PROSPECT_A);
  });

  it('review references originating walkthrough_request', async () => {
    const db = createMockPool();
    const { stored, review } = await seedWalkthroughWithReview(db);
    assert.equal(review.payload.originating_agent_action_id, stored.id);
    const walkthrough = walkthroughActions(db.state)[0];
    assert.equal(walkthrough.id, stored.id);
  });

  it('preserves FIRST_PARTY_ATTRIBUTION lineage', async () => {
    const db = createMockPool();
    const { review, record } = await seedWalkthroughWithReview(db);
    assert.equal(review.payload.attribution.provenance.sourceKind, SOURCE_KIND);
    assert.equal(review.payload.attribution.raw.campaign_id, record.raw.campaign_id);
    assert.equal(review.payload.attribution.normalized.lead_source, 'chatgpt_ads');
    assert.notEqual(review.payload.attribution.provenance.sourceKind, 'PLATFORM_API');
  });

  it('does not create duplicate open review on repeated walkthrough', async () => {
    const db = createMockPool({ nextActionId: ACTION_1 });
    pool.query = db.query.bind(db);
    const values = validateWalkthroughPayload(basePayload()).values;
    await captureWalkthroughLead(values, attributionRecord({ campaign_id: 'camp-a' }));
    db.state.nextActionId = ACTION_2;
    await captureWalkthroughLead(values, attributionRecord({ campaign_id: 'camp-b' }));

    assert.equal(walkthroughActions(db.state).length, 2);
    assert.equal(qualificationReviews(db.state).length, 1);
    const review = qualificationReviews(db.state)[0];
    assert.equal(review.payload.additional_originations.length, 1);
    assert.equal(review.payload.additional_originations[0].attribution.raw.campaign_id, 'camp-b');
  });

  it('rejects qualification review for another client prospect', async () => {
    const db = createMockPool({
      prospects: [{
        id: PROSPECT_A,
        client_id: CLIENT_1,
        email: 'alex@riverside.example',
        status: 'cold',
        setter_status: 'new',
      }],
    });
    const result = await ensureQualificationReviewForWalkthrough(db, {
      clientId: CLIENT_10,
      prospectId: PROSPECT_A,
      originatingActionId: ACTION_1,
      contactSummary: buildContactSummary(validateWalkthroughPayload(basePayload()).values),
    });
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.PROSPECT_CLIENT_MISMATCH);
  });

  it('missing prospect fails closed', async () => {
    const db = createMockPool({ prospects: [] });
    const result = await ensureQualificationReviewForWalkthrough(db, {
      clientId: CLIENT_10,
      prospectId: PROSPECT_A,
      originatingActionId: ACTION_1,
      contactSummary: buildContactSummary(validateWalkthroughPayload(basePayload()).values),
    });
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.PROSPECT_NOT_FOUND);
  });
});

describe('SPEC-258 — operator decisions', () => {
  let originalQuery;

  beforeEach(() => {
    originalQuery = pool.query;
  });

  afterEach(() => {
    pool.query = originalQuery;
  });

  async function withReview(db) {
    const seeded = await seedWalkthroughWithReview(db);
    return { db, reviewId: seeded.review.id, prospect: db.state.prospects[0] };
  }

  it('authenticated operator can QUALIFY', async () => {
    const db = createMockPool();
    const { reviewId } = await withReview(db);
    const result = await applyQualificationDecision(db, {
      reviewId,
      action: 'QUALIFY',
      operator: OPERATOR,
      note: 'Strong fit',
      clientId: CLIENT_10,
    });
    assert.equal(result.ok, true);
    assert.equal(result.qualificationStatus, QUALIFICATION_STATUS.QUALIFIED_BY_OPERATOR);
    assert.equal(result.opportunityCreationReady, true);
  });

  it('authenticated operator can NURTURE', async () => {
    const db = createMockPool();
    const { reviewId } = await withReview(db);
    const result = await applyQualificationDecision(db, {
      reviewId,
      action: 'NURTURE',
      operator: OPERATOR,
      clientId: CLIENT_10,
    });
    assert.equal(result.ok, true);
    assert.equal(result.qualificationStatus, QUALIFICATION_STATUS.NURTURE);
    assert.equal(result.opportunityCreationReady, false);
  });

  it('authenticated operator can DISQUALIFY', async () => {
    const db = createMockPool();
    const { reviewId } = await withReview(db);
    const result = await applyQualificationDecision(db, {
      reviewId,
      action: 'DISQUALIFY',
      operator: OPERATOR,
      clientId: CLIENT_10,
    });
    assert.equal(result.ok, true);
    assert.equal(result.qualificationStatus, QUALIFICATION_STATUS.DISQUALIFIED);
    assert.equal(result.opportunityCreationReady, false);
  });

  it('arbitrary free-text action is rejected', async () => {
    const db = createMockPool();
    const { reviewId } = await withReview(db);
    const result = await applyQualificationDecision(db, {
      reviewId,
      action: 'MAYBE_LATER',
      operator: OPERATOR,
      clientId: CLIENT_10,
    });
    assert.equal(result.ok, false);
    assert.equal(result.error, ERROR.UNSUPPORTED_ACTION);
    assert.ok(!DECISION_ACTIONS.includes('MAYBE_LATER'));
  });

  it('persists operator identity', async () => {
    const db = createMockPool();
    const { reviewId } = await withReview(db);
    const result = await applyQualificationDecision(db, {
      reviewId,
      action: 'QUALIFY',
      operator: OPERATOR,
      clientId: CLIENT_10,
    });
    assert.equal(result.payload.decision.decidedBy.id, OPERATOR.id);
    assert.equal(result.payload.decision.decidedBy.name, OPERATOR.name);
    assert.equal(result.payload.decision.decidedBy.role, OPERATOR.role);
    assert.equal(result.payload.qualifiedBy.id, OPERATOR.id);
  });

  it('persists decision timestamp', async () => {
    const db = createMockPool();
    const { reviewId } = await withReview(db);
    const result = await applyQualificationDecision(db, {
      reviewId,
      action: 'QUALIFY',
      operator: OPERATOR,
      clientId: CLIENT_10,
    });
    assert.match(result.payload.decision.decidedAt, /^\d{4}-\d{2}-\d{2}T/);
    assert.match(result.payload.qualifiedAt, /^\d{4}-\d{2}-\d{2}T/);
  });

  it('persists optional operator note', async () => {
    const db = createMockPool();
    const { reviewId } = await withReview(db);
    const result = await applyQualificationDecision(db, {
      reviewId,
      action: 'NURTURE',
      operator: OPERATOR,
      note: 'Needs timing confirmation',
      clientId: CLIENT_10,
    });
    assert.equal(result.payload.decision.note, 'Needs timing confirmation');
  });
});

describe('SPEC-258 — isolation and attribution immutability', () => {
  let originalQuery;

  beforeEach(() => {
    originalQuery = pool.query;
  });

  afterEach(() => {
    pool.query = originalQuery;
  });

  it('QUALIFY does not create opportunity, AO lead, customer, revenue, or setter mutation', async () => {
    const db = createMockPool({
      prospects: [{
        id: PROSPECT_A,
        client_id: CLIENT_10,
        email: 'alex@riverside.example',
        status: 'warm',
        setter_status: 'new',
        source: 'website_walkthrough',
        acquisition_metadata: {},
        acquisition_source: 'chatgpt_ads',
      }],
    });
    pool.query = db.query.bind(db);
    const values = validateWalkthroughPayload(basePayload()).values;
    await captureWalkthroughLead(values, attributionRecord());
    const reviewId = qualificationReviews(db.state)[0].id;
    const before = { ...db.state.prospects[0] };

    const result = await applyQualificationDecision(db, {
      reviewId,
      action: 'QUALIFY',
      operator: OPERATOR,
      clientId: CLIENT_10,
    });
    assert.equal(result.ok, true);
    assert.equal(db.state.prospects[0].setter_status, before.setter_status);
    assert.equal(db.state.prospects[0].status, before.status);
    assert.notEqual(result.payload.decision.action, 'walkthrough_completed');
  });

  it('attribution campaign/ad IDs remain unchanged after QUALIFY', async () => {
    const db = createMockPool();
    const { reviewId, record } = await seedWalkthroughWithReview(db);
    const result = await applyQualificationDecision(db, {
      reviewId,
      action: 'QUALIFY',
      operator: OPERATOR,
      clientId: CLIENT_10,
    });
    assert.equal(result.payload.attribution.raw.campaign_id, record.raw.campaign_id);
    assert.equal(result.payload.attribution.raw.ad_id, record.raw.ad_id);
    assert.equal(result.payload.attribution.provenance.sourceKind, SOURCE_KIND);
  });

  it('terminal review cannot be silently overwritten by conflicting decision', async () => {
    const db = createMockPool();
    const { reviewId } = await seedWalkthroughWithReview(db);
    await applyQualificationDecision(db, {
      reviewId,
      action: 'QUALIFY',
      operator: OPERATOR,
      clientId: CLIENT_10,
    });
    const conflict = await applyQualificationDecision(db, {
      reviewId,
      action: 'DISQUALIFY',
      operator: OPERATOR,
      clientId: CLIENT_10,
    });
    assert.equal(conflict.ok, false);
    assert.equal(conflict.error, ERROR.CONFLICTING_DECISION);
  });

  it('repeated identical decision is idempotent', async () => {
    const db = createMockPool();
    const { reviewId } = await seedWalkthroughWithReview(db);
    const first = await applyQualificationDecision(db, {
      reviewId,
      action: 'NURTURE',
      operator: OPERATOR,
      clientId: CLIENT_10,
    });
    const second = await applyQualificationDecision(db, {
      reviewId,
      action: 'NURTURE',
      operator: OPERATOR,
      clientId: CLIENT_10,
    });
    assert.equal(first.ok, true);
    assert.equal(second.ok, true);
    assert.equal(second.idempotent, true);
    assert.equal(second.qualificationStatus, QUALIFICATION_STATUS.NURTURE);
  });
});

describe('SPEC-258 — HTTP decision route', () => {
  let originalQuery;
  let server;

  beforeEach(() => {
    originalQuery = pool.query;
  });

  afterEach(async () => {
    pool.query = originalQuery;
    if (server) await server.close();
  });

  it('rejects body-supplied operator identity', async () => {
    const db = createMockPool();
    const { reviewId } = await seedWalkthroughWithReview(db);

    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.session = { active_client_id: CLIENT_10, user: OPERATOR };
      req.user = OPERATOR;
      next();
    });
    app.use(leadQualificationRouter);
    server = await listen(app);

    const res = await fetch(`http://127.0.0.1:${server.port}/api/v1/lead-qualification-reviews/${reviewId}/decision`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ action: 'QUALIFY', operator_id: 999, client_id: CLIENT_1 }),
    });
    assert.equal(res.status, 400);
  });
});
