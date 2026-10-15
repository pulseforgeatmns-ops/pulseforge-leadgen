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

  it('spreadsheet gate is selected for isolated parser, approval, UI, guard and fixture edits', () => {
    const files = [
      'packages/max/composer/adapters/spreadsheet.js',
      'packages/max/composer/headerMap.js',
      'packages/max/stateIngestion/spreadsheetProposalStore.js',
      'routes/maxStateIngestion.js',
      'services/maxSpreadsheetService.js',
      'services/maxStateIngestionService.js',
      'services/maxComposerService.js',
      'public/shared/spreadsheetReview.js',
      'public/shared/spreadsheetReview.css',
      'public/ao-dashboard.html',
      'public/command-deck/command-deck.js',
      'utils/maxSpreadsheetAuthorization.js',
      'utils/spreadsheetCrmEvidence.js',
      'utils/callEligibility.js',
      'utils/canonicalEmailEligibility.js',
      'utils/sendingReadiness.js',
      'test/maxSpreadsheetFixtureScenarios.test.js',
      'test/spreadsheetCallSuppression.test.js',
      'test/fixtures/anchor-cleaning-actual.xlsx',
      'test/fixtures/maxSpreadsheetBaseSchema.sql',
      'test/helpers/puppeteerTestBrowser.js',
    ];
    for (const file of files) {
      const plan = buildPlan([file]);
      assert.ok(plan.suites.includes('revenue-postgres'), `${file} must run the real spreadsheet gate`);
      assert.equal(plan.suites.includes('signal-v1'), false, `${file} must preserve lean domain selection`);
    }
  });

  it('fail-closed selects the full safe set', () => {
    const plan = buildPlan([], { failClosed: true });
    assert.equal(plan.failClosed, true);
    assert.ok(plan.suites.includes('paige-social'));
    assert.ok(plan.suites.includes('revenue-postgres'));
    assert.ok(plan.suites.length >= 8);
  });
});

// Exercise the actual workflow gate body so a skipped planned suite cannot be
// mistaken for an intentionally unselected job that GitHub marks as skipped.
describe('PR CI gate selected-suite enforcement', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const vm = require('node:vm');
  const workflow = fs.readFileSync(path.join(__dirname, '../.github/workflows/pr-ci-lean.yml'), 'utf8');
  const body = workflow.match(/node <<'NODE'\n([\s\S]*?)\n          NODE/)[1];
  function run(suites, results) {
    let exitCode = 0;
    try {
      vm.runInNewContext(body, {
        process: {
          env: { SUITES: JSON.stringify(suites), ...results },
          exit: code => { exitCode = code; throw new Error('gate exit'); },
        },
        console: { log() {}, error() {} },
      });
    } catch (error) {
      if (error.message !== 'gate exit') throw error;
    }
    return exitCode;
  }
  it('accepts successful selected jobs and skips only unselected jobs', () => {
    assert.equal(run(['global', 'revenue-postgres'], { GLOBAL: 'success', REVENUE: 'success', SIGNAL: 'skipped' }), 0);
  });
  for (const outcome of ['skipped', 'cancelled', 'failure', undefined]) {
    it(`blocks selected spreadsheet/Revenue PostgreSQL job result ${outcome || 'missing'}`, () => {
      assert.equal(run(['global', 'revenue-postgres'], { GLOBAL: 'success', REVENUE: outcome }), 1);
    });
  }
  it('rejects a selected unknown job', () => {
    assert.equal(run(['not-wired'], {}), 1);
  });
});
