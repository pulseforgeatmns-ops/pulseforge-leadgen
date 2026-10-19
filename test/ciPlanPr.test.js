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

  it('runs Max and its browser acceptance for changes to Living Command and content review', () => {
    for (const file of [
      'public/command-deck/command-deck.js', 'public/command-deck.html',
      'public/shared/content-review.js', 'public/shared/content-review.css', 'public/shared/shell.js',
      'services/contentReview.js', 'services/livingCommandBrief.js', 'routes/contentReview.js',
      'test/contentReview.test.js', 'test/livingCommandBrief.test.js', 'test/browser/livingCommandUi.test.js',
    ]) assert.ok(buildPlan([file]).suites.includes('max'), `${file} must run review browser coverage`);
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

  it('Places cache, client, provider and focused tests select the outbound gate', () => {
    for (const file of ['utils/placesApi.js', 'utils/placesQueryCache.js',
      'services/scoutPublicSourcing.js',
      'packages/capabilities/discovery/providers/PlacesProvider.js',
      'test/placesFailureCache.test.js', 'test/placesCostAttribution.test.js']) {
      assert.ok(buildPlan([file]).suites.includes('anchor-outbound'), file);
    }
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

describe('outbound diagnostic accuracy CI coverage', () => {
  for (const file of ['services/outboundControlDiagnostics.js', 'test/outboundControlDiagnostics.test.js']) {
    it(`selects governed outbound for ${file}`, () => {
      const plan = buildPlan([file]);
      assert.ok(plan.suites.includes('anchor-outbound'));
    });
  }
  it('runs diagnostic regression in the governed outbound workflow', () => {
    const workflow = require('node:fs').readFileSync(require('node:path').join(__dirname, '../.github/workflows/pr-ci-lean.yml'), 'utf8');
    assert.ok(workflow.includes('test/outboundControlDiagnostics.test.js'));
  });
});

it('selects governed outbound for Babrun provider-chain persistence and both regression files', () => {
  for (const file of ['scripts/lib/anchorMissionBoundEnrichment.js',
    'test/babrunGovernedEnrichmentEvidence.test.js', 'test/babrunGovernedEnrichmentEvidencePostgres.test.js']) {
    assert.ok(buildPlan([file]).suites.includes('anchor-outbound'), file);
  }
  const workflow = require('node:fs').readFileSync(require('node:path').join(__dirname, '../.github/workflows/pr-ci-lean.yml'), 'utf8');
  assert.match(workflow, /node --test test\/babrunGovernedEnrichmentEvidence\.test\.js test\/babrunGovernedEnrichmentEvidencePostgres\.test\.js/);
});

describe('Substral preparation-only CI coverage', () => {
  for (const file of ['services/substralPreparationGrant.js',
    'test/anchorDailyOutboundSubstralPreparationOnly.test.js',
    'test/anchorDailyOutboundSubstralPreparationPostgres.test.js']) {
    it(`selects governed outbound for ${file}`, () => {
      assert.ok(buildPlan([file]).suites.includes('anchor-outbound'));
    });
  }
  it('runs both new regressions through the PostgreSQL-enabled outbound glob', () => {
    const command = require('../package.json').scripts['test:anchor-outbound'];
    assert.ok(command.includes('test/anchorDailyOutbound*.test.js'));
    assert.ok(command.includes('ANCHOR_OUTBOUND_TEST_POSTGRES=true'));
  });
});


describe('Babrun owner evidence and promotion safety CI coverage', () => {
  for (const file of ['utils/websiteOwnerEvidence.js', 'test/websiteOwnerEvidence.test.js',
    'test/scoutPromotionEvidence.test.js', 'test/scoutPromotionRetention.test.js',
    'test/scoutPromotionCohort.test.js', 'test/helpers/denyWebsiteEvidenceNetwork.cjs']) {
    it(`selects governed outbound for ${file}`, () => {
      assert.ok(buildPlan([file]).suites.includes('anchor-outbound'), file);
    });
  }
  it('executes both new suites through the network-denying preload', () => {
    const workflow = require('node:fs').readFileSync(require('node:path').join(__dirname, '../.github/workflows/pr-ci-lean.yml'), 'utf8');
    assert.ok(workflow.includes('node --require ./test/helpers/denyWebsiteEvidenceNetwork.cjs --test test/websiteOwnerEvidence.test.js test/scoutPromotionEvidence.test.js test/scoutPromotionRetention.test.js test/scoutPromotionCohort.test.js'));
  });
});


describe('Scout business evidence cursor CI coverage', () => {
  for (const file of ['services/scoutBusinessEvidenceCursor.js', 'test/scoutBusinessEvidenceCursor.test.js',
    'test/scoutBusinessEvidenceCursorRamp.test.js', 'test/scoutBusinessEvidenceCursorPostgres.test.js',
    'test/scoutCheckpointStallDiagnosis.test.js', 'test/helpers/scoutEvidenceCursorFakePool.js']) {
    it(`selects governed outbound for ${file}`, () => {
      assert.ok(buildPlan([file]).suites.includes('anchor-outbound'), file);
    });
  }
  it('runs pure and mocked cursor contracts with the network-denying preload', () => {
    const workflow = require('node:fs').readFileSync(require('node:path').join(__dirname, '../.github/workflows/pr-ci-lean.yml'), 'utf8');
    assert.ok(workflow.includes('node --require ./test/helpers/denyWebsiteEvidenceNetwork.cjs --test test/scoutBusinessEvidenceCursor.test.js test/scoutBusinessEvidenceCursorRamp.test.js test/scoutCheckpointStallDiagnosis.test.js'));
  });
  it('requires disposable PostgreSQL for the native cursor CAS contract', () => {
    const workflow = require('node:fs').readFileSync(require('node:path').join(__dirname, '../.github/workflows/pr-ci-lean.yml'), 'utf8');
    const gate = workflow.split('      - name: Scout evidence cursor required disposable PostgreSQL CAS contract (no providers)')[1]?.split('      - name:')[0];
    assert.ok(gate?.includes("SCOUT_EVIDENCE_CURSOR_REQUIRE_POSTGRES: 'true'"));
    assert.ok(gate?.includes('run: node --test test/scoutBusinessEvidenceCursorPostgres.test.js'));
    const native = require('node:fs').readFileSync(require('node:path').join(__dirname, 'scoutBusinessEvidenceCursorPostgres.test.js'), 'utf8');
    assert.ok(native.includes("process.env.SCOUT_EVIDENCE_CURSOR_REQUIRE_POSTGRES !== 'true'"));
    assert.ok(native.includes("await startDisposablePostgres('scout-evidence-cursor-')"));
    assert.equal(native.includes('t.skip('), false, 'native prerequisites must throw, not silently skip');
  });
});


describe('SPEC262 scoped recovery CI coverage', () => {
  it('maps runtime, native tests and manual schema to the real PostgreSQL Anchor suite', () => {
    for (const file of ['scripts/anchorCrmPreparationOnce.js','services/anchorScopedResume.js','services/anchorScopedResumeControl.js',
      'test/anchorScopedResumeLifecyclePostgres.test.js','test/helpers/anchorScopedResumePostgresFixture.js',
      'docs/sql/anchor-scoped-resume-schema-proposal.sql']) {
      assert.ok(buildPlan([file]).suites.includes('anchor-outbound'), file);
    }
    assert.match(require('../package.json').scripts['test:anchor-outbound'], /test\/anchorScopedResume\*\.test\.js/);
  });
});

// SPEC-265: the reviewed provider pause cannot change without its regressions.
it('research provider pause leaves and callers select the Anchor suite', () => {
  for (const file of [
    "leadgen.js",
    "lib/serpapi.js",
    "packages/capabilities/acquisition/ProspectAcquisition.js",
    "packages/capabilities/acquisition/providerContract.js",
    "packages/capabilities/discovery/ProspectDiscovery.js",
    "packages/capabilities/discovery/providers/PlacesProvider.js",
    "packages/max/scoutAcquisition/DiscoveryAdapters.js",
    "packages/max/scoutAcquisition/ScoutAdapter.js",
    "packages/max/workspace/MissionBoundWebsiteDiscovery.js",
    "packages/max/workspace/ScoutDiscoveryExecutor.js",
    "packages/scout/DiscoveryPipeline.js",
    "packages/scout/coverage/DiscoveryCoverageEngine.js",
    "packages/scout/coverage/HypothesisDrivenDiscoveryEngine.js",
    "packages/scout/investigation/CandidateInvestigation.js",
    "phoneEnrich.js",
    "routes/clientIntelligence.js",
    "routes/cron.js",
    "scoutUnenrichedEnrichmentAgent.js",
    "scripts/backfill_email_verification.js",
    "scripts/diagnoseScoutPlaces.js",
    "scripts/importAnchorCallList.js",
    "scripts/lib/anchorMissionBoundEnrichment.js",
    "scripts/lib/babrunCohort002.js",
    "scripts/lib/babrunContactResolution.js",
    "scripts/lib/webCohort002.js",
    "scripts/lib/webCohort003.js",
    "scripts/promoteUnenriched.js",
    "scripts/resolveBabrunFirstTenContacts.js",
    "server.js",
    "services/emailVerificationRecovery.js",
    "services/governedOutboundAdapters.js",
    "services/maxOutboundControlLoop.js",
    "services/sameCompanyContactRecovery.js",
    "services/scoutPlacesDiagnostic.js",
    "services/scoutPublicSourcing.js",
    "tieredEnrichmentAgent.js",
    "utils/anchorResearchProviderPause.js",
    "utils/emailVerifier.js",
    "utils/placesApi.js",
    "test/anchorResearchProviderPauseCore.test.js"
]) {
    assert.ok(buildPlan([file]).suites.includes('anchor-outbound'), file);
  }
  assert.match(require('../package.json').scripts['test:anchor-outbound'], /test\/anchorResearchProviderPause\*\.test\.js/);
});

describe('promotion schema lifecycle CI coverage', () => {
  it('runs both Max CRM admission and native Anchor coverage for the shared schema helper', () => {
    const suites = buildPlan(['utils/businessNameShort.js']).suites;
    assert.ok(suites.includes('max'), 'shared CRM admission schema changes must select Max');
    assert.ok(suites.includes('anchor-outbound'), 'shared promotion schema changes must select native Anchor');
  });
  it('runs native Anchor regressions for both runtime paths and the concurrency test', () => {
    for (const file of ['utils/businessNameShort.js', 'scripts/promoteUnenriched.js',
      'test/anchorDailyOutboundPromotionSchemaPostgres.test.js']) {
      assert.ok(buildPlan([file]).suites.includes('anchor-outbound'), file);
    }
    assert.match(require('../package.json').scripts['test:anchor-outbound'], /test\/anchorDailyOutbound\*\.test\.js/);
  });
});
