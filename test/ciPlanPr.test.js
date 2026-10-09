'use strict';

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');
const { buildPlan, pathMatchesPattern } = require('../.github/scripts/ci-plan-pr.js');

describe('CI plan PR selector (SPEC-CI-LEAN-001)', () => {
  it('matches glob and directory patterns', () => {
    assert.equal(pathMatchesPattern('packages/signal-v1/foo.js', 'packages/signal-v1/'), true);
    assert.equal(pathMatchesPattern('scripts/runSignalProbe.js', 'scripts/*Signal*'), true);
    assert.equal(pathMatchesPattern('test/paigeAgent.test.js', 'test/paige'), true);
    assert.equal(pathMatchesPattern('routes/revenue.js', 'packages/signal-v1/'), false);
  });

  const cases = [
    {
      name: 'Signal-only (PR #899 shape)',
      files: [
        'packages/signal-v1/operator/alertText.js',
        'services/telegramCallerFeed/feedAuth.js',
        'services/signalV1ShadowScheduler.js',
      ],
      expect: ['global', 'signal-v1'],
      forbid: ['paige-social', 'revenue-postgres', 'anchor-outbound'],
    },
    {
      name: 'Paige-only',
      files: ['paigeAgent.js', 'test/paigeSocial.test.js'],
      expect: ['global', 'paige-social'],
      forbid: ['signal-v1', 'revenue-postgres'],
    },
    {
      name: 'AO-only',
      files: ['routes/aoProspectRouting.js', 'test/aoProspectRouting.test.js'],
      expect: ['global', 'revenue-postgres'],
      forbid: ['signal-v1', 'paige-social'],
    },
    {
      name: 'Shared package dependency',
      files: ['package.json'],
      expect: ['global', 'signal-v1', 'paige-social', 'anchor-outbound', 'decision-shadow', 'revenue-postgres'],
      forbid: [],
    },
    {
      name: 'Server startup',
      files: ['server.js'],
      expect: ['global', 'anchor-outbound', 'signal-v1', 'revenue-postgres', 'decision-shadow'],
      forbid: ['paige-social'],
    },
    {
      name: 'Migration',
      files: ['migrations/2026-10-01-example.sql'],
      expect: ['global', 'revenue-postgres', 'spec245-evidence'],
      forbid: ['paige-social'],
    },
    {
      name: 'CI workflow change',
      files: ['.github/workflows/pr-ci-lean.yml'],
      expect: ['global', 'signal-v1', 'paige-social', 'anchor-outbound', 'decision-shadow', 'revenue-postgres'],
      forbid: ['max'],
    },
  ];

  for (const row of cases) {
    it(row.name, () => {
      const plan = buildPlan(row.files);
      for (const suite of row.expect) {
        assert.ok(plan.suites.includes(suite), `expected ${suite} in ${plan.suites.join(', ')}`);
      }
      for (const suite of row.forbid) {
        assert.equal(plan.suites.includes(suite), false, `did not expect ${suite}`);
      }
    });
  }

  it('fail-closed selects the full safe set', () => {
    const plan = buildPlan([], { failClosed: true });
    assert.equal(plan.failClosed, true);
    assert.ok(plan.suites.includes('paige-social'));
    assert.ok(plan.suites.includes('revenue-postgres'));
    assert.ok(plan.suites.length >= 8);
  });
});
