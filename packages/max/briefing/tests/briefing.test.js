'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  seedTenant,
  registerScoreWatches,
  TENANT,
  AS_OF,
} = require('./helpers');
const {
  BRIEFING_SECTIONS,
  BRIEFING_PERIODS,
  createPresentationAdapter,
  Prioritizer,
  resolvePeriodWindow,
} = require('..');
const { createMaxReasoningRuntime } = require('../..');
const { createKnowledgeRuntime } = require('../../../knowledge');

describe('BriefingEngine — daily / weekly / monthly digests', () => {
  it('builds a daily briefing with all template sections', async () => {
    const { max, companies } = await seedTenant({ companyCount: 3 });
    registerScoreWatches(max, companies);

    const briefing = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'daily',
      // daily window ends at AS_OF; extend start so Jul 20→25 snapshots are visible
      periodStart: '2026-07-19T00:00:00.000Z',
      periodEnd: AS_OF,
    });

    for (const key of BRIEFING_SECTIONS) {
      assert.ok(key in briefing, `missing section ${key}`);
    }
    assert.equal(briefing.summary.period, 'daily');
    assert.equal(briefing.summary.companiesMonitored, 3);
    assert.equal(briefing.summary.companiesWithMemory, 3);
    assert.ok(briefing.priorities.length >= 1);
    assert.ok(briefing.changes.total >= 1);
    assert.ok(briefing.metrics.buildTimeMs >= 0);
    assert.equal(briefing.metrics.queryCount, 1);
    assert.ok(briefing.metrics.recommendationCount >= 1);
    assert.ok(briefing.metrics.memoryLookups >= 1);
  });

  it('supports weekly and monthly period windows', async () => {
    const { max } = await seedTenant({ companyCount: 2 });

    const weekly = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      period: BRIEFING_PERIODS.WEEKLY,
    });
    assert.equal(weekly.summary.period, 'weekly');
    assert.ok(Date.parse(weekly.meta.windowEnd) - Date.parse(weekly.meta.windowStart) >= 6 * 86400000);

    const monthly = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      period: BRIEFING_PERIODS.MONTHLY,
    });
    assert.equal(monthly.summary.period, 'monthly');
    assert.ok(Date.parse(monthly.meta.windowEnd) - Date.parse(monthly.meta.windowStart) >= 29 * 86400000);
  });

  it('rejects unknown periods', () => {
    assert.throws(
      () => resolvePeriodWindow({ period: 'quarterly', asOf: AS_OF }),
      /Unsupported briefing period/
    );
  });
});

describe('BriefingEngine — empty tenant', () => {
  it('returns empty sections for a tenant with no companies', async () => {
    const knowledge = createKnowledgeRuntime({
      withSync: false,
      startIngestor: false,
    }).knowledge;
    const max = createMaxReasoningRuntime({ knowledge });

    const briefing = await max.brief({
      tenantId: 'empty-tenant',
      asOf: AS_OF,
      period: 'daily',
    });

    assert.equal(briefing.summary.companiesMonitored, 0);
    assert.equal(briefing.priorities.length, 0);
    assert.equal(briefing.changes.total, 0);
    assert.equal(briefing.watchAlerts.total, 0);
    assert.equal(briefing.risks.total, 0);
    assert.equal(briefing.recommendations.total, 0);
    assert.equal(briefing.metrics.recommendationCount, 0);
  });

  it('lists companies without memory as monitored but empty queues', async () => {
    const { max } = await seedTenant({ companyCount: 2, withMemory: false });
    const briefing = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'weekly',
    });
    assert.equal(briefing.summary.companiesMonitored, 2);
    assert.equal(briefing.summary.companiesWithMemory, 0);
    assert.equal(briefing.priorities.length, 0);
  });
});

describe('BriefingEngine — watch alerts + changes + risks', () => {
  it('includes watch alerts when score delta exceeds threshold', async () => {
    const { max, companies } = await seedTenant({ companyCount: 3 });
    registerScoreWatches(max, companies);

    const briefing = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      periodStart: '2026-07-19T00:00:00.000Z',
      periodEnd: AS_OF,
    });

    assert.ok(briefing.watchAlerts.total >= 1);
    assert.ok(briefing.watchAlerts.items.every((a) => a.watchId && a.companyId));
  });

  it('summarizes memory changes deterministically', async () => {
    const { max } = await seedTenant({ companyCount: 3 });
    const a = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      periodStart: '2026-07-19T00:00:00.000Z',
      periodEnd: AS_OF,
    });
    const b = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      periodStart: '2026-07-19T00:00:00.000Z',
      periodEnd: AS_OF,
    });
    assert.deepEqual(a.changes, b.changes);
    assert.ok(a.changes.byType);
    assert.ok(Array.isArray(a.changes.highlights));
  });

  it('surfaces risks from deteriorations', async () => {
    const { max } = await seedTenant({ companyCount: 3 });
    const briefing = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      periodStart: '2026-07-19T00:00:00.000Z',
      periodEnd: AS_OF,
    });
    assert.ok(briefing.risks.total >= 1);
    assert.ok(briefing.risks.items[0].severity >= briefing.risks.items.at(-1).severity);
  });
});

describe('BriefingEngine — recommendation / priority ordering', () => {
  it('orders priorities and recommendations deterministically', async () => {
    const { max } = await seedTenant({ companyCount: 5 });
    const a = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'monthly',
    });
    const b = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'monthly',
    });

    assert.deepEqual(
      a.priorities.map((p) => p.companyId),
      b.priorities.map((p) => p.companyId)
    );
    assert.deepEqual(
      a.recommendations.items.map((p) => p.companyId),
      b.recommendations.items.map((p) => p.companyId)
    );

    // ranks are 1..n contiguous
    a.priorities.forEach((p, i) => assert.equal(p.rank, i + 1));
    // rankScore non-increasing
    for (let i = 1; i < a.priorities.length; i += 1) {
      assert.ok(a.priorities[i - 1].rankScore >= a.priorities[i].rankScore);
    }
  });

  it('Prioritizer is stable on ties via companyId', () => {
    const p = new Prioritizer();
    const ordered = p.order([
      { companyId: 'b', score: 80, confidence: 50, trend: 'flat', urgency: 0, contradictionSeverity: 0 },
      { companyId: 'a', score: 80, confidence: 50, trend: 'flat', urgency: 0, contradictionSeverity: 0 },
    ]);
    assert.equal(ordered[0].companyId, 'a');
    assert.equal(ordered[1].companyId, 'b');
  });
});

describe('BriefingEngine — presentation adapter', () => {
  it('returns structured domain object by default (no UI formatting)', async () => {
    const { max } = await seedTenant({ companyCount: 1 });
    const briefing = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'monthly',
    });
    assert.equal(briefing.format, undefined);
    assert.ok(briefing.summary);
  });

  it('wraps via PresentationAdapter when present:true', async () => {
    const { max } = await seedTenant({ companyCount: 1 });
    const presented = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'monthly',
      present: true,
      format: 'markdown',
    });
    assert.equal(presented.format, 'markdown');
    assert.ok(presented.body.includes('# Max Briefing'));
    assert.ok(presented.briefing.summary);

    const structured = createPresentationAdapter('structured').present(
      presented.briefing
    );
    assert.equal(structured.format, 'structured');
  });
});

describe('BriefingEngine — does not call reasoning', () => {
  it('assembles from memory without evaluate()', async () => {
    const { max, companies } = await seedTenant({ companyCount: 1 });
    let evaluateCalls = 0;
    const original = max.engine.evaluate.bind(max.engine);
    max.engine.evaluate = async (...args) => {
      evaluateCalls += 1;
      return original(...args);
    };

    await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'monthly',
    });
    assert.equal(evaluateCalls, 0);
    assert.equal(companies.length, 1);
  });
});
