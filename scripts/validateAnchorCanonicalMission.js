#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — canonical AMO production validation to READY.
 *
 * Intended for execution inside the Railway production container where
 * DATABASE_URL and GOOGLE_PLACES_KEY are already present. Uses the live
 * canonical AMO runtime; does not duplicate business logic.
 *
 * Usage (from repo root inside Railway SSH):
 *   node scripts/validateAnchorCanonicalMission.js --confirm-production
 */

require('dotenv').config();

const { EXECUTION_INTENTS, STAGES, CONTRIBUTION_KINDS } = require('../packages/acquisition-mission');
const pool = require('../db');
const { createMission, executeCanonical, inspectMission } = require('../services/acquisitionMission');

const TENANT_ID = '10';
const CLIENT_ID = 10;
const OBJECTIVE =
  'Acquire recurring commercial cleaning customers from law firms in Greater Manchester, NH.';
const TARGET_SEGMENT = 'Law Firms';
const OPERATOR_ID = 'anchor-canonical-mission-validation';

const STEPS = Object.freeze([
  {
    label: 'APPROVE_PLAN',
    intent: EXECUTION_INTENTS.APPROVE_PLAN,
    question: 'Approved.',
    assertContribution: null,
  },
  {
    label: 'APPROVE_DISCOVERY',
    intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
    question: 'Approved. Begin Discovery.',
    assertContribution: { specialist: 'scout', kind: CONTRIBUTION_KINDS.DISCOVERY },
    rejectFixtures: true,
  },
  {
    label: 'APPROVE_PRIORITIZATION',
    intent: EXECUTION_INTENTS.APPROVE_PRIORITIZATION,
    question: 'Approved prioritization.',
    assertContribution: { specialist: 'max', kind: CONTRIBUTION_KINDS.PRIORITIZATION },
  },
  {
    label: 'DECIDE_ACQUISITION_APPROACH',
    intent: EXECUTION_INTENTS.DECIDE_ACQUISITION_APPROACH,
    question: 'Proceed with outbound email for ranked law firm prospects.',
    payload: { approach: 'outbound' },
    assertContribution: { specialist: 'max', kind: CONTRIBUTION_KINDS.ACQUISITION_APPROACH },
  },
  {
    label: 'GENERATE_VARIANTS',
    intent: EXECUTION_INTENTS.GENERATE_VARIANTS,
    question: 'Generate outreach variants.',
    assertContribution: { specialist: 'paige', kind: CONTRIBUTION_KINDS.VARIANTS },
  },
  {
    label: 'GENERATE_CAPACITY',
    intent: EXECUTION_INTENTS.GENERATE_CAPACITY,
    question: 'Plan outbound capacity.',
    assertContribution: { specialist: 'emmett', kind: CONTRIBUTION_KINDS.CAPACITY },
  },
]);

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const help = argv.includes('--help') || argv.includes('-h');
  const unknown = argv.filter(
    (arg) => arg !== '--confirm-production' && arg !== '--help' && arg !== '-h'
  );
  if (unknown.length) {
    throw new Error(
      `Unknown argument(s): ${unknown.join(', ')}. Usage: node scripts/validateAnchorCanonicalMission.js --confirm-production`
    );
  }
  return { confirmProduction, help };
}

function printUsage() {
  console.log(`Anchor canonical AMO production validation (tenant ${TENANT_ID})

Usage:
  node scripts/validateAnchorCanonicalMission.js --confirm-production

Safety:
  Refuses to run without --confirm-production.
  Requires DATABASE_URL and GOOGLE_PLACES_KEY in the container environment.
  Never passes allowFixtureFallback: true.
  Stops at READY; does not call EXECUTE_OUTBOUND or enable autosend.
`);
}

function assertRuntimeEnv() {
  const missing = [];
  if (!process.env.DATABASE_URL) missing.push('DATABASE_URL');
  if (!process.env.GOOGLE_PLACES_KEY) missing.push('GOOGLE_PLACES_KEY');
  if (missing.length) {
    const err = new Error(`Missing required runtime env: ${missing.join(', ')}`);
    err.code = 'runtime_env_missing';
    throw err;
  }
  if (
    process.env.ALLOW_FIXTURE_FALLBACK === 'true'
    || process.env.allowFixtureFallback === 'true'
  ) {
    const err = new Error('Refusing to run with ALLOW_FIXTURE_FALLBACK enabled.');
    err.code = 'fixture_fallback_env';
    throw err;
  }
}

async function loadMissionRow(missionId) {
  const { rows } = await pool.query(
    `SELECT id, stage, status, objective, updated_at
     FROM acquisition_missions
     WHERE id = $1 AND tenant_id = $2`,
    [missionId, TENANT_ID]
  );
  if (!rows.length) {
    const err = new Error(`Mission ${missionId} not durably persisted for tenant ${TENANT_ID}.`);
    err.code = 'mission_not_persisted';
    throw err;
  }
  return rows[0];
}

async function loadContribution(missionId, specialist, kind) {
  const { rows } = await pool.query(
    `SELECT id, specialist, kind, payload, at
     FROM acquisition_mission_contributions
     WHERE mission_id = $1 AND tenant_id = $2 AND specialist = $3 AND kind = $4
     ORDER BY at ASC`,
    [missionId, TENANT_ID, specialist, kind]
  );
  return rows;
}

async function assertContributionPersisted(missionId, specialist, kind) {
  const rows = await loadContribution(missionId, specialist, kind);
  if (!rows.length) {
    const err = new Error(`Missing persisted contribution ${specialist}/${kind} for mission ${missionId}.`);
    err.code = 'contribution_not_persisted';
    throw err;
  }
  return rows[rows.length - 1];
}

/**
 * Durable AMO contributions store the full contribution row in the payload column.
 * Unwrap to the canonical discovery/prioritization payload contract.
 * @param {object} rowOrPayload
 * @returns {object}
 */
function unwrapContributionPayload(rowOrPayload) {
  if (!rowOrPayload || typeof rowOrPayload !== 'object') return rowOrPayload || {};
  const outer = rowOrPayload.payload && typeof rowOrPayload.payload === 'object'
    ? rowOrPayload.payload
    : rowOrPayload;
  if (
    outer.payload &&
    typeof outer.payload === 'object' &&
    (outer.specialist || outer.kind || outer.missionId)
  ) {
    return outer.payload;
  }
  return outer;
}

function scoutCandidateCount(payload) {
  const body = unwrapContributionPayload(payload);
  if (!body || typeof body !== 'object') return null;
  if (body.qualifiedCount != null && Number.isFinite(Number(body.qualifiedCount))) {
    return Number(body.qualifiedCount);
  }
  if (body.candidateUniverseCount != null && Number.isFinite(Number(body.candidateUniverseCount))) {
    return Number(body.candidateUniverseCount);
  }
  if (Array.isArray(body.candidateUniverse) && body.candidateUniverse.length) {
    return body.candidateUniverse.length;
  }
  if (body.rankedProspectCount != null && Number.isFinite(Number(body.rankedProspectCount))) {
    return Number(body.rankedProspectCount);
  }
  if (Array.isArray(body.rankedProspects) && body.rankedProspects.length) {
    return body.rankedProspects.length;
  }
  if (Array.isArray(body.opportunities) && body.opportunities.length) {
    return body.opportunities.length;
  }
  const artifact = body.discoveryArtifact;
  if (artifact && artifact.rankedProspects && artifact.rankedProspects.length) {
    return artifact.rankedProspects.length;
  }
  return null;
}

function itemUsesFixture(item) {
  if (item == null) return false;
  if (typeof item !== 'object') {
    const text = String(item).toLowerCase();
    return text === 'fixture' || /test fixture/i.test(text);
  }
  const source = String(item.source || item.provider || '');
  if (/^fixture$/i.test(source) || /test fixture/i.test(source)) return true;
  if (item.provenance && item.provenance.fixture === true) return true;
  if (item.fixture === true || item.fixtureFallback === true) return true;
  return false;
}

function discoveryUsedFixture(payload) {
  const body = unwrapContributionPayload(payload);
  if (!body || typeof body !== 'object') return false;
  if (
    body.fixtureFallback === true
    || body.usedFixtureFallback === true
    || body.source === 'fixture'
    || body.provenance?.fixture === true
  ) {
    return true;
  }

  const collections = [
    body.evidence,
    body.opportunities,
    body.companies,
    body.providerExecution,
    body.rankedProspects,
    body.discoveryArtifact?.evidence,
    body.discoveryArtifact?.opportunities,
  ];

  return collections.some((items) => Array.isArray(items) && items.some(itemUsesFixture));
}

async function loadOrderedAuditSequence(missionId) {
  const [eventsResult, auditResult] = await Promise.all([
    pool.query(
      `SELECT id, kind, specialist, label, at
       FROM acquisition_mission_events
       WHERE mission_id = $1
       ORDER BY at ASC, id ASC`,
      [missionId]
    ),
    pool.query(
      `SELECT id, transaction_id, specialist, stage, commit_status, rollback_reason, error_class, at
       FROM acquisition_mission_execution_audit
       WHERE mission_id = $1
       ORDER BY at ASC, id ASC`,
      [missionId]
    ).catch(() => ({ rows: [] })),
  ]);

  const sequence = [
    ...eventsResult.rows.map((row) => ({
      sequenceType: 'event',
      id: row.id,
      kind: row.kind,
      specialist: row.specialist,
      label: row.label,
      at: row.at,
    })),
    ...auditResult.rows.map((row) => ({
      sequenceType: 'execution_audit',
      id: row.id,
      transactionId: row.transaction_id,
      specialist: row.specialist,
      stage: row.stage,
      commitStatus: row.commit_status,
      rollbackReason: row.rollback_reason,
      errorClass: row.error_class,
      at: row.at,
    })),
  ];

  sequence.sort((a, b) => {
    const delta = new Date(a.at).getTime() - new Date(b.at).getTime();
    if (delta !== 0) return delta;
    return String(a.id).localeCompare(String(b.id));
  });

  return sequence;
}

function buildVerdict(report) {
  if (report.error) {
    return {
      success: false,
      message: report.error.message,
      scoutDiscoveryProductionValidated: false,
      emmettPrepareProductionValidated: false,
    };
  }
  if (report.finalStage !== STAGES.READY) {
    return {
      success: false,
      message: `Mission stopped at stage ${report.finalStage}; expected ${STAGES.READY}.`,
      scoutDiscoveryProductionValidated: false,
      emmettPrepareProductionValidated: false,
    };
  }
  if (report.scoutCandidateCount == null || report.scoutCandidateCount <= 0) {
    return {
      success: false,
      message: 'Scout discovery persisted but candidate count is zero.',
      scoutDiscoveryProductionValidated: false,
      emmettPrepareProductionValidated: false,
    };
  }
  return {
    success: true,
    message: 'Scout DISCOVERY and Emmett PREPARE production-validated for Anchor tenant 10.',
    scoutDiscoveryProductionValidated: true,
    emmettPrepareProductionValidated: true,
  };
}

async function run(options = {}) {
  if (options.help) {
    printUsage();
    return { help: true };
  }
  if (!options.confirmProduction) {
    const err = new Error('Refusing to run without --confirm-production.');
    err.code = 'confirm_production_required';
    throw err;
  }

  assertRuntimeEnv();

  const report = {
    tenantId: TENANT_ID,
    startedAt: new Date().toISOString(),
    steps: [],
    failures: [],
    retries: [],
  };

  let mission;
  try {
    mission = await createMission({
      tenantId: TENANT_ID,
      clientId: CLIENT_ID,
      objective: OBJECTIVE,
      targetSegment: TARGET_SEGMENT,
      createdBy: OPERATOR_ID,
      owner: 'Operator',
      title: 'Anchor canonical mission production validation — law firms Greater Manchester NH',
    }, { pool, production: true });

    report.missionId = mission.id;
    report.objective = mission.objective;

    for (const step of STEPS) {
      const stepRecord = {
        label: step.label,
        intent: step.intent,
        startedAt: new Date().toISOString(),
      };
      report.steps.push(stepRecord);

      let routed;
      try {
        routed = await executeCanonical({
          tenantId: TENANT_ID,
          missionId: mission.id,
          intent: step.intent,
          operatorId: OPERATOR_ID,
          question: step.question,
          payload: step.payload || {},
          allowFixtureFallback: false,
        }, { pool, production: true });
      } catch (err) {
        stepRecord.error = { code: err.code || null, message: err.message };
        report.failures.push({
          step: step.label,
          code: err.code || null,
          message: err.message,
        });
        throw err;
      }

      stepRecord.completedAt = new Date().toISOString();
      stepRecord.specialist = routed.specialist || null;
      stepRecord.action = routed.action || null;
      stepRecord.executionOutcome = routed.executionResult?.executionOutcome || null;
      stepRecord.rolledBack = routed.executionResult?.rolledBack === true;

      if (routed.executionResult?.alreadyExecuted === true) {
        report.retries.push({
          step: step.label,
          note: 'Stage reported alreadyExecuted=true (idempotent replay).',
        });
      }
      if (stepRecord.rolledBack) {
        report.failures.push({
          step: step.label,
          code: 'stage_rolled_back',
          message: 'Stage execution rolled back.',
        });
      }

      const missionRow = await loadMissionRow(mission.id);
      stepRecord.persistedStage = missionRow.stage;
      stepRecord.persistedStatus = missionRow.status;

      if (step.assertContribution) {
        const contribution = await assertContributionPersisted(
          mission.id,
          step.assertContribution.specialist,
          step.assertContribution.kind
        );
        stepRecord.contributionId = contribution.id;

        if (step.rejectFixtures && discoveryUsedFixture(contribution.payload)) {
          const err = new Error('Scout discovery used fixture data — production validation invalid.');
          err.code = 'fixture_data_detected';
          report.failures.push({
            step: step.label,
            code: err.code,
            message: err.message,
          });
          throw err;
        }

        if (step.assertContribution.specialist === 'scout') {
          stepRecord.scoutCandidateCount = scoutCandidateCount(contribution.payload);
        }
      }
    }

    const finalSnapshot = await inspectMission(mission.id, { tenantId: TENANT_ID, pool });
    report.finalStage = finalSnapshot.mission.stage;
    report.finalStatus = finalSnapshot.mission.status;

    const scout = await assertContributionPersisted(mission.id, 'scout', CONTRIBUTION_KINDS.DISCOVERY);
    const max = await assertContributionPersisted(mission.id, 'max', CONTRIBUTION_KINDS.PRIORITIZATION);
    const paige = await assertContributionPersisted(mission.id, 'paige', CONTRIBUTION_KINDS.VARIANTS);
    const emmett = await assertContributionPersisted(mission.id, 'emmett', CONTRIBUTION_KINDS.CAPACITY);

    if (discoveryUsedFixture(scout.payload)) {
      const err = new Error('Final Scout discovery payload contains fixture provenance.');
      err.code = 'fixture_data_detected';
      throw err;
    }

    report.scoutContributionId = scout.id;
    report.scoutCandidateCount = scoutCandidateCount(scout.payload);
    report.maxContributionId = max.id;
    report.paigeContributionId = paige.id;
    report.emmettContributionId = emmett.id;
    report.auditSequence = await loadOrderedAuditSequence(mission.id);
    report.verdict = buildVerdict(report);
    report.completedAt = new Date().toISOString();

    return report;
  } catch (err) {
    report.error = { code: err.code || null, message: err.message };
    report.completedAt = new Date().toISOString();
    report.verdict = buildVerdict(report);
    throw Object.assign(err, { report });
  }
}

module.exports = {
  TENANT_ID,
  OBJECTIVE,
  TARGET_SEGMENT,
  STEPS,
  parseArgs,
  run,
  unwrapContributionPayload,
  scoutCandidateCount,
  discoveryUsedFixture,
  buildVerdict,
};

if (require.main === module) {
  const options = parseArgs();
  run(options)
    .then((report) => {
      if (report.help) return;
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.verdict?.success ? 0 : 2;
    })
    .catch((err) => {
      const report = err.report || {
        error: { code: err.code || null, message: err.message },
        verdict: buildVerdict({ error: { message: err.message } }),
        completedAt: new Date().toISOString(),
      };
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
