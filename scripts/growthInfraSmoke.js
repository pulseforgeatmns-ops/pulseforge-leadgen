#!/usr/bin/env node
'use strict';

/**
 * SPEC-087 — Growth Infrastructure Readiness CLI smoke.
 *
 * Exercises fixture → same report path → report_ready.
 * Does not mutate DNS, GBP, social, analytics, or CRM.
 *
 * Usage:
 *   npm run growth:infra:smoke -- --fixture=anchor
 *   npm run growth:infra:smoke -- --fixture=anchor --json
 *   npm run growth:infra:smoke -- --fixture=anchor --check
 */

const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
  startInfrastructureReadinessConversation,
  applyInfrastructureReadinessFixture,
  isGrowthInfraDevFixturesEnabled,
  ClientIntelligenceError,
} = require('../services/clientIntelligenceInterview');
const {
  ARTIFACT_KIND,
} = require('../services/clientIntelligenceInfrastructureReadiness');

const INTERVIEW_ANSWERS = [
  'Anchor Cleaning — commercial cleaning for offices.',
  'Recurring commercial cleaning and detail cleans.',
  'Property managers and professional offices.',
  'Lowest-price shoppers and one-off bargains.',
  'Greater Manchester including Bedford and Hooksett.',
  'Reliable crews without chasing the team.',
  'Calm, professional, reliable.',
  'More recurring commercial jobs.',
  'Booked walkthroughs and signed cleaning agreements.',
];

function argValue(name) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  if (hit) return hit.slice(prefix.length);
  const idx = process.argv.indexOf(`--${name}`);
  if (idx >= 0 && process.argv[idx + 1] && !process.argv[idx + 1].startsWith('--')) {
    return process.argv[idx + 1];
  }
  return null;
}

function printHelp() {
  console.log(`Growth Infrastructure Readiness smoke (SPEC-087)

Usage:
  npm run growth:infra:smoke -- --fixture=anchor
  npm run growth:infra:smoke -- --fixture=anchor --json --check

Options:
  --fixture=<id>   Sample/dev fixture id (default: anchor)
  --json           Print full JSON result
  --check          Exit 1 when smoke assertions fail
  --help           Show this help

Dev gate:
  Enabled when NODE_ENV is development|test, or CIE_GROWTH_INFRA_DEV_FIXTURES=1.
  Production/staging/unset NODE_ENV are fail-closed unless the flag is set.
  Uses in-memory store only — no DNS/GBP/social/analytics/CRM writes.
`);
}

async function runSmoke(options) {
  if (!isGrowthInfraDevFixturesEnabled(process.env)) {
    return {
      ok: false,
      error: 'dev_fixtures_disabled',
      message:
        'Dev fixtures disabled. Set CIE_GROWTH_INFRA_DEV_FIXTURES=1 or use non-production NODE_ENV.',
    };
  }

  const store = createMemoryStore();
  const opts = { store, useMemoryPlaybookStore: true };
  const fixtureId = options.fixture || 'anchor';

  const started = await startClientInterview({ clientId: 910 }, opts);
  let turn = started;
  for (const answer of INTERVIEW_ANSWERS) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  if (!turn.blueprint || !turn.blueprint.id) {
    return {
      ok: false,
      error: 'no_blueprint',
      message: 'Interview did not produce a Blueprint',
      status: turn.status,
    };
  }

  await approveBlueprint(turn.blueprint.id, opts);
  await startInfrastructureReadinessConversation(started.interviewId, opts);

  const applied = await applyInfrastructureReadinessFixture(
    started.interviewId,
    fixtureId,
    opts
  );

  const report = applied.growthInfrastructureReadinessReport;
  const readiness = applied.infrastructureReadiness;
  const checks = {
    hasReport: Boolean(report),
    kindOk: Boolean(report && report.kind === ARTIFACT_KIND),
    reportReady: Boolean(readiness && readiness.status === 'report_ready'),
    stepReport: Boolean(readiness && readiness.step === 'report'),
    sampleMarked: Boolean(
      readiness &&
        readiness.devFixture &&
        readiness.devFixture.sample === true &&
        readiness.devFixture.id === fixtureId
    ),
    answersMarked: Boolean(
      readiness &&
        readiness.answers &&
        Object.values(readiness.answers).every(
          (a) => a && a.sample === true && a.source === 'dev_fixture'
        )
    ),
    noCampaigns: Boolean(report && report.campaignsGenerated === false),
    assessmentOnly: Boolean(report && report.assessmentOnly === true),
    reportDevFixture: Boolean(report && report.devFixture && report.devFixture.sample),
  };

  const failed = Object.entries(checks)
    .filter(([, v]) => !v)
    .map(([k]) => k);

  return {
    ok: failed.length === 0,
    fixture: fixtureId,
    interviewId: started.interviewId,
    status: applied.status,
    readinessStatus: readiness && readiness.status,
    step: readiness && readiness.step,
    overallStatus: report && report.overallStatus,
    reportKind: report && report.kind,
    campaignsGenerated: report && report.campaignsGenerated,
    sample: true,
    checks,
    failed,
    message:
      failed.length === 0
        ? `OK — fixture=${fixtureId} report_ready via real report path`
        : `FAIL — ${failed.join(', ')}`,
  };
}

async function main() {
  if (process.argv.includes('--help') || process.argv.includes('-h')) {
    printHelp();
    return;
  }

  // This CLI is itself a dev/test tool — opt in for this process when unset.
  if (!String(process.env.CIE_GROWTH_INFRA_DEV_FIXTURES || '').trim()) {
    process.env.CIE_GROWTH_INFRA_DEV_FIXTURES = '1';
  }

  const options = {
    fixture: argValue('fixture') || 'anchor',
    json: process.argv.includes('--json'),
    check: process.argv.includes('--check'),
  };

  let result;
  try {
    result = await runSmoke(options);
  } catch (err) {
    result = {
      ok: false,
      error: err instanceof ClientIntelligenceError ? err.code : 'smoke_failed',
      message: err && err.message ? String(err.message) : 'failed',
    };
  }

  if (options.json) {
    console.log(JSON.stringify(result, null, 2));
  } else {
    console.log(result.message || (result.ok ? 'OK' : 'FAIL'));
    if (result.overallStatus) {
      console.log(`overallStatus=${result.overallStatus}`);
    }
    if (result.failed && result.failed.length) {
      console.log(`failed checks: ${result.failed.join(', ')}`);
    }
  }

  if (options.check && !result.ok) {
    process.exitCode = 1;
  }
  if (!options.check && !result.ok) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
