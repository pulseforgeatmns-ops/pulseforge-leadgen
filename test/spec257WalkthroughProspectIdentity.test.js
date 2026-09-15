'use strict';

/**
 * SPEC-257 — Paid web lead identity bridge (walkthrough → prospect).
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  PROSPECT_LINK_STATUS,
  UNRESOLVED_REASON,
  resolveWalkthroughProspect,
  buildWalkthroughIdentityPayload,
  emitWalkthroughLeadCreatedEvent,
} = require('../lib/walkthroughProspectIdentity');
const {
  captureWalkthroughLead,
  buildWalkthroughActionPayload,
  mirrorAttributionToProspect,
  ANCHOR_CLIENT_ID,
  SOURCE,
} = require('../lib/walkthroughCapture');
const { buildAttributionRecord, SOURCE_KIND } = require('../lib/walkthroughAttribution');
const { validateWalkthroughPayload } = require('../lib/walkthroughValidate');
const pool = require('../db');
const {
  createWalkthroughCaptureMockPool,
  WALKTHROUGH_MOCK_PROSPECT_ID: PROSPECT_A,
  WALKTHROUGH_MOCK_ACTION_ID: ACTION_1,
} = require('./helpers/walkthroughCaptureMockPool');

const CLIENT_10 = 10;
const CLIENT_1 = 1;
const PROSPECT_B = '22222222-2222-4222-8222-222222222222';
const ACTION_2 = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';

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
    campaign_id: 'camp-257',
    ad_group_id: 'ag-257',
    ad_id: 'ad-257',
    opref: 'opref-257',
    oppref: 'oppref-257',
    utm_source: 'chatgpt',
    ...overrides,
  });
}

function createMockPool(initial = {}) {
  const db = createWalkthroughCaptureMockPool(initial);
  const originalQuery = db.query.bind(db);
  db.query = async (sql, params = []) => {
    const text = String(sql);
    if (/FROM opportunities|INSERT INTO opportunities|INSERT INTO customers|INSERT INTO revenue_/i.test(text)) {
      throw new Error(`Unexpected downstream write: ${text.slice(0, 80)}`);
    }
    if (/INSERT INTO prospects/i.test(text) && !db.state.prospects.length && initial.nextProspectId !== PROSPECT_B) {
      const result = await originalQuery(sql, params);
      if (result.rows[0]?.id) db.state.nextProspectId = PROSPECT_B;
      return result;
    }
    return originalQuery(sql, params);
  };
  return db;
}

describe('SPEC-257 — walkthrough prospect resolution', () => {
  it('LINKED_NEW when insert succeeds', async () => {
    const db = createMockPool();
    const values = validateWalkthroughPayload(basePayload()).values;
    const result = await resolveWalkthroughProspect(db, values, CLIENT_10, {
      serviceAreaMatch: () => 'Manchester',
    });
    assert.equal(result.linkStatus, PROSPECT_LINK_STATUS.LINKED_NEW);
    assert.equal(result.prospectId, PROSPECT_A);
    assert.equal(result.isNew, true);
  });

  it('LINKED_EXISTING when email already exists for same client', async () => {
    const db = createMockPool({
      prospects: [{
        id: PROSPECT_A,
        client_id: CLIENT_10,
        email: 'alex@riverside.example',
        status: 'contacted',
        setter_status: 'contacted',
        source: 'website_walkthrough',
        acquisition_metadata: { prior: true },
        acquisition_source: 'organic_search',
      }],
    });
    const values = validateWalkthroughPayload(basePayload()).values;
    const result = await resolveWalkthroughProspect(db, values, CLIENT_10, {
      serviceAreaMatch: () => 'Manchester',
    });
    assert.equal(result.linkStatus, PROSPECT_LINK_STATUS.LINKED_EXISTING);
    assert.equal(result.prospectId, PROSPECT_A);
    assert.equal(result.isNew, false);
    assert.equal(db.state.prospects.length, 1);
  });

  it('UNRESOLVED when email belongs to another client (tenant conflict)', async () => {
    const db = createMockPool({
      prospects: [{
        id: PROSPECT_A,
        client_id: CLIENT_1,
        email: 'alex@riverside.example',
        status: 'cold',
        setter_status: 'new',
      }],
    });
    const values = validateWalkthroughPayload(basePayload()).values;
    const result = await resolveWalkthroughProspect(db, values, CLIENT_10, {
      serviceAreaMatch: () => 'Manchester',
    });
    assert.equal(result.linkStatus, PROSPECT_LINK_STATUS.UNRESOLVED);
    assert.equal(result.prospectId, null);
    assert.equal(result.unresolvedReason, UNRESOLVED_REASON.EMAIL_TENANT_CONFLICT);
  });
});

describe('SPEC-257 — captureWalkthroughLead identity bridge', () => {
  let originalQuery;

  beforeEach(() => {
    originalQuery = pool.query;
  });

  afterEach(() => {
    pool.query = originalQuery;
  });

  it('persists prospect_id and LINKED_NEW on new submission', async () => {
    const db = createMockPool();
    pool.query = db.query.bind(db);
    const values = validateWalkthroughPayload(basePayload()).values;
    const record = attributionRecord();
    const stored = await captureWalkthroughLead(values, record);

    assert.equal(stored.prospect_id, PROSPECT_A);
    assert.equal(stored.prospect_link_status, PROSPECT_LINK_STATUS.LINKED_NEW);
    assert.equal(db.state.agentActions.length, 1);
    const payload = db.state.agentActions[0].payload;
    assert.equal(payload.prospect_id, PROSPECT_A);
    assert.equal(payload.identity.prospectLinkStatus, PROSPECT_LINK_STATUS.LINKED_NEW);
    assert.match(payload.identity.linkedAt, /^\d{4}-\d{2}-\d{2}T/);
    assert.equal(stored.prospect_id, payload.prospect_id);
  });

  it('always creates agent_action even when prospect unresolved', async () => {
    const db = createMockPool({
      prospects: [{
        id: PROSPECT_A,
        client_id: CLIENT_1,
        email: 'alex@riverside.example',
        status: 'cold',
        setter_status: 'new',
      }],
    });
    pool.query = db.query.bind(db);
    const values = validateWalkthroughPayload(basePayload()).values;
    const stored = await captureWalkthroughLead(values, attributionRecord());

    assert.equal(stored.prospect_id, null);
    assert.equal(stored.prospect_link_status, PROSPECT_LINK_STATUS.UNRESOLVED);
    assert.equal(db.state.agentActions.length, 1);
    const payload = db.state.agentActions[0].payload;
    assert.equal(payload.prospect_id, null);
    assert.equal(payload.identity.prospectLinkStatus, PROSPECT_LINK_STATUS.UNRESOLVED);
    assert.equal(payload.identity.unresolvedReason, UNRESOLVED_REASON.EMAIL_TENANT_CONFLICT);
  });

  it('repeat submission links existing prospect without duplicate insert', async () => {
    const db = createMockPool({
      prospects: [{
        id: PROSPECT_A,
        client_id: CLIENT_10,
        email: 'alex@riverside.example',
        status: 'contacted',
        setter_status: 'contacted',
        source: 'website_walkthrough',
        acquisition_metadata: { keep: true },
        acquisition_source: 'referral',
      }],
      nextActionId: ACTION_1,
    });
    pool.query = db.query.bind(db);
    const values = validateWalkthroughPayload(basePayload()).values;

    const first = await captureWalkthroughLead(values, attributionRecord({ campaign_id: 'camp-first' }));
    db.state.nextActionId = ACTION_2;
    const second = await captureWalkthroughLead(values, attributionRecord({ campaign_id: 'camp-second' }));

    assert.equal(first.prospect_id, PROSPECT_A);
    assert.equal(second.prospect_id, PROSPECT_A);
    assert.equal(second.prospect_link_status, PROSPECT_LINK_STATUS.LINKED_EXISTING);
    assert.equal(db.state.prospects.length, 1);
    assert.equal(db.state.agentActions.length, 2);
    assert.equal(db.state.agentActions[0].payload.attribution.raw.campaign_id, 'camp-first');
    assert.equal(db.state.agentActions[1].payload.attribution.raw.campaign_id, 'camp-second');
    assert.equal(db.state.agentActions[1].payload.identity.prospectLinkStatus, PROSPECT_LINK_STATUS.LINKED_EXISTING);
    assert.equal(db.state.prospects[0].status, 'contacted');
    assert.equal(db.state.prospects[0].source, 'website_walkthrough');
    assert.equal(db.state.prospects[0].acquisition_metadata.keep, true);
  });

  it('preserves FIRST_PARTY_ATTRIBUTION fields on agent_action payload', async () => {
    const db = createMockPool();
    pool.query = db.query.bind(db);
    const record = attributionRecord();
    const values = validateWalkthroughPayload(basePayload()).values;
    await captureWalkthroughLead(values, record);
    const payload = db.state.agentActions[0].payload;
    assert.equal(payload.attribution.provenance.sourceKind, SOURCE_KIND);
    assert.equal(payload.attribution.raw.campaign_id, 'camp-257');
    assert.equal(payload.attribution.raw.ad_group_id, 'ag-257');
    assert.equal(payload.attribution.raw.ad_id, 'ad-257');
    assert.equal(payload.attribution.raw.opref, 'opref-257');
    assert.equal(payload.attribution.raw.oppref, 'oppref-257');
    assert.equal(payload.attribution.normalized.lead_source, 'chatgpt_ads');
    assert.notEqual(payload.attribution.provenance.sourceKind, 'PLATFORM_API');
  });

  it('emits lead_created lifecycle evidence referencing prospect and action', async () => {
    const db = createMockPool();
    pool.query = db.query.bind(db);
    const values = validateWalkthroughPayload(basePayload()).values;
    const stored = await captureWalkthroughLead(values, attributionRecord());

    assert.equal(db.state.lifecycleEvents.length, 1);
    const evt = db.state.lifecycleEvents[0];
    assert.equal(evt.prospect_id, PROSPECT_A);
    assert.equal(evt.client_id, ANCHOR_CLIENT_ID);
    assert.equal(evt.source, SOURCE);
    assert.equal(evt.reason, 'lead_created');
    assert.equal(evt.payload.kind, 'lead_created');
    assert.equal(evt.payload.agent_action_id, stored.id);
    assert.equal(evt.payload.prospect_link_status, PROSPECT_LINK_STATUS.LINKED_NEW);
    assert.notEqual(evt.payload.kind, 'qualified');
    assert.notEqual(evt.payload.kind, 'walkthrough_completed');
  });

  it('does not overwrite existing prospect metadata destructively on repeat submit', async () => {
    const db = createMockPool({
      prospects: [{
        id: PROSPECT_A,
        client_id: CLIENT_10,
        email: 'alex@riverside.example',
        status: 'contacted',
        setter_status: 'contacted',
        source: 'website_walkthrough',
        acquisition_metadata: { keep: true, attribution: { old: true } },
        acquisition_source: 'referral',
      }],
    });
    pool.query = db.query.bind(db);
    const values = validateWalkthroughPayload(basePayload()).values;
    await captureWalkthroughLead(values, attributionRecord());

    const row = db.state.prospects[0];
    assert.equal(row.status, 'contacted');
    assert.equal(row.source, 'website_walkthrough');
    assert.equal(row.acquisition_metadata.keep, true);
    assert.equal(row.acquisition_source, 'referral');
    assert.equal(row.acquisition_metadata.attribution.normalized.lead_source, 'chatgpt_ads');
  });
});

describe('SPEC-257 — payload builder', () => {
  it('buildWalkthroughActionPayload includes identity block', () => {
    const values = validateWalkthroughPayload(basePayload()).values;
    const linkedAt = '2026-09-15T12:00:00.000Z';
    const payload = buildWalkthroughActionPayload(
      values,
      attributionRecord(),
      { prospectId: PROSPECT_A, linkStatus: PROSPECT_LINK_STATUS.LINKED_NEW },
      linkedAt
    );
    assert.equal(payload.source, SOURCE);
    assert.equal(payload.prospect_id, PROSPECT_A);
    assert.equal(payload.identity.prospectLinkStatus, PROSPECT_LINK_STATUS.LINKED_NEW);
    assert.equal(payload.identity.linkedAt, linkedAt);
  });

  it('buildWalkthroughIdentityPayload records unresolved reason', () => {
    const identity = buildWalkthroughIdentityPayload({
      linkStatus: PROSPECT_LINK_STATUS.UNRESOLVED,
      unresolvedReason: UNRESOLVED_REASON.EMAIL_TENANT_CONFLICT,
    }, '2026-09-15T12:00:00.000Z');
    assert.equal(identity.prospectLinkStatus, PROSPECT_LINK_STATUS.UNRESOLVED);
    assert.equal(identity.unresolvedReason, UNRESOLVED_REASON.EMAIL_TENANT_CONFLICT);
  });
});
