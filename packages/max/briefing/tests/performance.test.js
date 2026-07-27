'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { seedTenant, TENANT, AS_OF } = require('./helpers');
const { BRIEFING_PERFORMANCE_TARGET_MS } = require('..');

describe('BriefingEngine — large tenant + performance', () => {
  it('handles a large tenant with stable ordering under target', async () => {
    const { max } = await seedTenant({ companyCount: 40 });

    const started = process.hrtime.bigint();
    const a = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'monthly',
    });
    const elapsedMs = Number(process.hrtime.bigint() - started) / 1e6;

    const b = await max.brief({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'monthly',
    });

    assert.equal(a.summary.companiesMonitored, 40);
    assert.deepEqual(
      a.priorities.map((p) => `${p.rank}:${p.companyId}:${p.rankScore}`),
      b.priorities.map((p) => `${p.rank}:${p.companyId}:${p.rankScore}`)
    );
    assert.ok(
      a.metrics.buildTimeMs <= BRIEFING_PERFORMANCE_TARGET_MS,
      `briefing metrics buildTimeMs ${a.metrics.buildTimeMs} exceeded target ${BRIEFING_PERFORMANCE_TARGET_MS}`
    );
    assert.ok(
      elapsedMs <= BRIEFING_PERFORMANCE_TARGET_MS * 2,
      `wall time ${elapsedMs}ms too slow`
    );
  });
});
