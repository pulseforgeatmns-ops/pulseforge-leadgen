'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  seedTenant,
  registerScoreWatches,
  TENANT,
  AS_OF,
} = require('../../briefing/tests/helpers');
const { createMaxReasoningRuntime } = require('../..');
const {
  parseRecommendationId,
  buildRecommendationId,
  pushTrail,
  popTrailTo,
  focusFromTrail,
  TRAIL_KINDS,
  NAV_TYPES,
  buildNavRef,
} = require('..');

const BRIEF_WINDOW = {
  periodStart: '2026-07-19T00:00:00.000Z',
  periodEnd: AS_OF,
};

describe('IntelligenceTypes — trail helpers', () => {
  it('parses recommendation ids with colon-bearing company ids', () => {
    const parsed = parseRecommendationId('rec:10:co:abc');
    assert.deepEqual(parsed, { tenantId: '10', companyId: 'co:abc' });
    assert.equal(buildRecommendationId('10', 'co:abc'), 'rec:10:co:abc');
  });

  it('pushes and pops trail without duplicate tips', () => {
    let trail = pushTrail([], {
      kind: TRAIL_KINDS.DECK,
      id: null,
      label: "Today's Brief",
    });
    trail = pushTrail(trail, {
      kind: TRAIL_KINDS.RECOMMENDATION,
      id: 'rec:10:c1',
      label: 'Staffing',
    });
    trail = pushTrail(trail, {
      kind: TRAIL_KINDS.RECOMMENDATION,
      id: 'rec:10:c1',
      label: 'Staffing',
    });
    assert.equal(trail.length, 2);
    trail = popTrailTo(trail, 0);
    assert.equal(trail.length, 1);
    assert.equal(trail[0].kind, TRAIL_KINDS.DECK);
  });

  it('derives Max focus from trail tip', () => {
    const trail = [
      { kind: TRAIL_KINDS.DECK, id: null, label: "Today's Brief" },
      {
        kind: TRAIL_KINDS.RECOMMENDATION,
        id: 'rec:10:co-1',
        label: 'Staffing Expansion',
      },
      { kind: TRAIL_KINDS.EVIDENCE, id: 'ev-1', label: 'Hiring signal' },
    ];
    const focus = focusFromTrail(trail);
    assert.equal(focus.page, 'recommendation');
    assert.equal(focus.recommendationId, 'rec:10:co-1');
    assert.equal(focus.companyId, 'co-1');
    assert.equal(focus.selectedEntity.type, 'evidence');
  });

  it('buildNavRef rejects empty ids', () => {
    assert.equal(buildNavRef({ type: NAV_TYPES.COMPANY, id: '' }), null);
    assert.ok(buildNavRef({ type: NAV_TYPES.COMPANY, id: 'c1', label: 'Acme' }));
  });
});

describe('Intelligence composers — recommendation + company', () => {
  it('exposes composeRecommendation and composeCompany on runtime', () => {
    const max = createMaxReasoningRuntime({ startIngestor: false });
    assert.equal(typeof max.composeRecommendation, 'function');
    assert.equal(typeof max.composeCompany, 'function');
    assert.ok(max.intelligence);
  });

  it('composes recommendation detail from memory without inventing scores', async () => {
    const { max, companies } = await seedTenant({ companyCount: 2 });
    registerScoreWatches(max, companies);
    max.policy.configureTenant(TENANT, {
      minimumConfidence: 0.1,
      maxEvidenceAgeDays: 0,
      cooldownHours: 0,
      blockedDays: [],
      approvalRequired: [],
    });

    const company = companies[1];
    const recommendationId = buildRecommendationId(TENANT, company.id);
    const model = await max.composeRecommendation({
      tenantId: TENANT,
      recommendationId,
      asOf: AS_OF,
    });

    assert.equal(model.kind, 'recommendation_detail');
    assert.equal(model.empty, false);
    assert.equal(model.recommendationId, recommendationId);
    assert.equal(model.companyId, company.id);
    assert.ok(Number.isFinite(model.confidence) || model.confidence === null);
    assert.ok(Array.isArray(model.supportingSignals));
    assert.ok(model.related);
    assert.ok(Array.isArray(model.related.alternativeRecommendations));
    assert.ok(model.actions.some((a) => a.type === 'ask_max'));
    assert.ok(model.meta.buildTimeMs >= 0);
  });

  it('fail-closes unknown recommendation', async () => {
    const max = createMaxReasoningRuntime({ startIngestor: false });
    const model = await max.composeRecommendation({
      tenantId: TENANT,
      recommendationId: 'rec:10:missing-company',
      asOf: AS_OF,
    });
    assert.equal(model.empty, true);
    assert.ok(model.emptyReason);
    assert.equal(model.opportunity, null);
  });

  it('composes company intelligence with related section', async () => {
    const { max, companies } = await seedTenant({ companyCount: 3 });
    registerScoreWatches(max, companies);
    max.policy.configureTenant(TENANT, {
      minimumConfidence: 0.1,
      maxEvidenceAgeDays: 0,
      cooldownHours: 0,
      blockedDays: [],
      approvalRequired: [],
    });

    const company = companies[0];
    const model = await max.composeCompany({
      tenantId: TENANT,
      companyId: company.id,
      asOf: AS_OF,
      ...BRIEF_WINDOW,
    });

    assert.equal(model.kind, 'company_intelligence');
    assert.equal(model.empty, false);
    assert.equal(model.companyId, company.id);
    assert.equal(model.companyName, company.name);
    assert.ok(model.overview);
    assert.ok(model.related);
    assert.ok(Array.isArray(model.evidence));
    assert.ok(model.actions.some((a) => a.type === 'ask_max'));
  });

  it('fail-closes missing company', async () => {
    const max = createMaxReasoningRuntime({ startIngestor: false });
    const model = await max.composeCompany({
      tenantId: TENANT,
      companyId: 'does-not-exist',
      asOf: AS_OF,
    });
    assert.equal(model.empty, true);
    assert.equal(model.emptyReason, 'company_not_found');
  });

  it('related intelligence only references known company ids', async () => {
    const { max, companies } = await seedTenant({ companyCount: 3 });
    const company = companies[0];
    const related = await max.intelligence.related.forCompany({
      tenantId: TENANT,
      companyId: company.id,
    });
    const known = new Set(companies.map((c) => c.id));
    for (const ref of related.similarCompanies) {
      assert.ok(known.has(ref.id) || ref.id !== company.id);
      assert.ok(ref.id);
      assert.ok(ref.label);
    }
  });
});
