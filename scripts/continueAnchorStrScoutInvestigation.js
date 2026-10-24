#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — one canonical Scout CONTINUE_INVESTIGATION for a specific mission.
 *
 * Entity-scoped / gap-directed continuation only. Preserves existing Scout candidates.
 * Never APPROVE_EXECUTION, EXECUTE_OUTBOUND, or autosend.
 *
 * Usage (Railway container):
 *   node scripts/continueAnchorStrScoutInvestigation.js --confirm-production \
 *     --mission-id mission_82e8102f-249c-4f44-b88e-2de76b13898e
 */

require('dotenv').config();

const {
  EXECUTION_INTENTS,
  OPERATOR_DECISION_KINDS,
  intentFromPendingDecision,
} = require('../packages/acquisition-mission');
const { evaluatePrioritizationReadiness } = require('../packages/acquisition-mission/DecisionReadiness');
const { presentationFromDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPresentation');
const pool = require('../db');
const { executeCanonical, inspectMission } = require('../services/acquisitionMission');
const {
  TENANT_ID,
  CLIENT_ID,
  assertNotSendingIntent,
} = require('./lib/anchorCanonicalOutbound');
const {
  unwrapContributionPayload,
  scoutCandidateCount,
} = require('./validateAnchorCanonicalMission');

const DEFAULT_MISSION_ID = 'mission_82e8102f-249c-4f44-b88e-2de76b13898e';
const OPERATOR_ID = 'anchor-str-scout-investigation-continuation';

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const help = argv.includes('--help') || argv.includes('-h');
  const missionIdx = argv.indexOf('--mission-id');
  const missionId = missionIdx >= 0 ? String(argv[missionIdx + 1] || '').trim() : DEFAULT_MISSION_ID;
  const unknown = argv.filter(
    (arg, idx) =>
      arg !== '--confirm-production'
      && arg !== '--help'
      && arg !== '-h'
      && arg !== '--mission-id'
      && !(missionIdx >= 0 && (idx === missionIdx + 1 || idx === missionIdx))
  );
  if (unknown.length) {
    throw Object.assign(
      new Error(`Unknown argument(s): ${unknown.join(', ')}.`),
      { code: 'unknown_args' }
    );
  }
  return { confirmProduction, help, missionId };
}

function printUsage() {
  console.log(`Anchor STR Scout investigation continuation (tenant ${TENANT_ID})

Usage:
  node scripts/continueAnchorStrScoutInvestigation.js --confirm-production [--mission-id <id>]

Safety:
  Refuses without --confirm-production.
  CONTINUE_INVESTIGATION only — never APPROVE_EXECUTION or EXECUTE_OUTBOUND.
  Requires pending discovery_investigation (Continue investigation).
  Refuses when autosend_enabled is true.
  Never uses fixture fallback.
`);
}

function assertRuntimeEnv() {
  const missing = [];
  if (!process.env.DATABASE_URL) missing.push('DATABASE_URL');
  if (!process.env.GOOGLE_PLACES_KEY) missing.push('GOOGLE_PLACES_KEY');
  if (missing.length) {
    throw Object.assign(
      new Error(`Missing required runtime env: ${missing.join(', ')}`),
      { code: 'runtime_env_missing' }
    );
  }
  if (
    process.env.ALLOW_FIXTURE_FALLBACK === 'true'
    || process.env.allowFixtureFallback === 'true'
  ) {
    throw Object.assign(
      new Error('Refusing to run with ALLOW_FIXTURE_FALLBACK enabled.'),
      { code: 'fixture_fallback_env' }
    );
  }
}

async function assertAutosendOff(db) {
  const { rows } = await db.query(
    `SELECT id, autosend_enabled FROM clients WHERE id = $1`,
    [CLIENT_ID]
  );
  const client = rows[0];
  if (!client) {
    throw Object.assign(new Error('Anchor client 10 was not found.'), { code: 'anchor_client_missing' });
  }
  if (client.autosend_enabled === true) {
    throw Object.assign(new Error('Anchor autosend_enabled is true. Aborting.'), { code: 'autosend_enabled' });
  }
  return client;
}

function latestScoutDiscovery(contributions = []) {
  return [...contributions]
    .reverse()
    .find((row) => row.specialist === 'scout' && row.kind === 'discovery') || null;
}

function scoutPayloadFromSnapshot(snapshot = {}) {
  const scout = latestScoutDiscovery(snapshot.contributions || []);
  return scout ? unwrapContributionPayload(scout.payload || scout) : {};
}

function extractCoverageSummary(payload = {}) {
  const coverage = payload.coverage && typeof payload.coverage === 'object' ? payload.coverage : {};
  const cities = coverage.cities && typeof coverage.cities === 'object' ? coverage.cities : {};
  const names = []
    .concat(Array.isArray(cities.names) ? cities.names : [])
    .concat(Array.isArray(cities.searchedCities) ? cities.searchedCities : [])
    .concat(Array.isArray(cities.list) ? cities.list : [])
    .filter(Boolean);
  return {
    complete: coverage.complete === true,
    searched: cities.searched != null ? Number(cities.searched) : names.length || null,
    planned: cities.planned != null ? Number(cities.planned) : null,
    names: [...new Set(names.map(String))],
    warnings: Array.isArray(coverage.warnings) ? coverage.warnings : [],
  };
}

function extractEvidenceGaps(payload = {}) {
  const readiness = evaluatePrioritizationReadiness(payload);
  const presentation = readiness.presentation || presentationFromDiscoveryPayload(payload);
  const gaps = [];

  for (const blocker of readiness.blockers || []) {
    gaps.push({
      code: blocker.code || null,
      label: blocker.label || null,
      reason: blocker.reason || null,
    });
  }

  const investigation = payload.candidateInvestigation || payload.discoveryArtifact?.candidateInvestigation || null;
  if (investigation && Array.isArray(investigation.remainingGaps)) {
    for (const gap of investigation.remainingGaps) {
      gaps.push({ code: 'entity_gap', label: String(gap), reason: 'Entity-scoped investigation gap.' });
    }
  }
  if (investigation && Array.isArray(investigation.missingEvidence)) {
    for (const gap of investigation.missingEvidence) {
      gaps.push({ code: 'missing_evidence', label: String(gap), reason: 'Candidate missing evidence.' });
    }
  }
  if (Array.isArray(payload.missingEvidence?.missing)) {
    for (const gap of payload.missingEvidence.missing) {
      gaps.push({ code: 'missing_evidence', label: String(gap), reason: 'Investigation loop missing evidence.' });
    }
  }

  const deduped = [];
  const seen = new Set();
  for (const row of gaps) {
    const key = `${row.code}:${row.label}:${row.reason}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(row);
  }

  return {
    sufficientForPrioritization: readiness.sufficient === true,
    gaps: deduped,
    discoveryStatus: presentation.discoveryStatus || payload.discoveryStatus || null,
  };
}

function snapshotMetrics(snapshot = {}) {
  const mission = snapshot.mission || {};
  const payload = scoutPayloadFromSnapshot(snapshot);
  const pending = mission.pendingOperatorDecision || null;
  const pendingIntent = intentFromPendingDecision(pending);
  const candidateCount = scoutCandidateCount({ payload });
  const evidence = extractEvidenceGaps(payload);
  return {
    missionId: mission.id || null,
    stage: mission.stage || null,
    status: mission.status || null,
    candidateCount,
    coverage: extractCoverageSummary(payload),
    discoveryStatus: evidence.discoveryStatus,
    pendingIntent,
    pendingOperatorDecision: pending
      ? {
        kind: pending.kind || null,
        prompt: pending.prompt || null,
        reason: pending.reason || null,
      }
      : null,
    prioritizationApprovalPending:
      pendingIntent === EXECUTION_INTENTS.APPROVE_PRIORITIZATION
      || pending?.kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
    investigationContinuationPending:
      pendingIntent === EXECUTION_INTENTS.CONTINUE_INVESTIGATION
      || pending?.kind === OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
    evidenceGaps: evidence.gaps,
    sufficientForPrioritization: evidence.sufficientForPrioritization,
  };
}

function exactNextOperatorAction(after = {}) {
  if (after.prioritizationApprovalPending) {
    return 'Approve prioritization (APPROVE_PRIORITIZATION) when ready to rank prospects.';
  }
  if (after.investigationContinuationPending) {
    return 'Continue investigation again (CONTINUE_INVESTIGATION) or modify/cancel the mission.';
  }
  if (after.stage === 'ready') {
    return 'Review prepared outreach at READY. Do not APPROVE_EXECUTION until operator is ready to send.';
  }
  return after.pendingOperatorDecision?.prompt
    || 'Inspect the acquisition mission workspace for the next pending operator decision.';
}

async function run(options = {}) {
  if (options.help) {
    printUsage();
    return { help: true };
  }
  if (!options.confirmProduction) {
    throw Object.assign(
      new Error('Refusing to run without --confirm-production.'),
      { code: 'confirm_production_required' }
    );
  }

  assertRuntimeEnv();
  const db = options.pool || pool;
  await assertAutosendOff(db);

  const missionId = options.missionId || DEFAULT_MISSION_ID;
  const beforeSnapshot = await inspectMission(missionId, { tenantId: TENANT_ID, pool: db, production: true });
  const before = snapshotMetrics(beforeSnapshot);

  if (!before.investigationContinuationPending) {
    throw Object.assign(
      new Error(
        `Mission ${missionId} does not have a pending Continue investigation decision. `
        + `pendingIntent=${before.pendingIntent || 'null'}`
      ),
      { code: 'no_pending_investigation', before }
    );
  }

  const beforeCount = Number(before.candidateCount || 0);
  if (beforeCount <= 0) {
    throw Object.assign(
      new Error(
        `Mission ${missionId} has no Scout candidates to preserve (count=${beforeCount}). `
        + 'Use broad discovery recovery instead of entity continuation.'
      ),
      { code: 'empty_candidate_set', before }
    );
  }

  assertNotSendingIntent(EXECUTION_INTENTS.CONTINUE_INVESTIGATION);

  const routed = await executeCanonical({
    tenantId: TENANT_ID,
    missionId,
    intent: EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
    operatorId: OPERATOR_ID,
    question: 'Continue investigation. Run Scout discovery for short-term rental operators.',
    allowFixtureFallback: false,
  }, { pool: db, production: true });

  const afterSnapshot = routed.snapshot || await inspectMission(missionId, { tenantId: TENANT_ID, pool: db, production: true });
  const after = snapshotMetrics(afterSnapshot);
  const afterCount = Number(after.candidateCount || 0);
  const candidatesPreserved = afterCount >= beforeCount && afterCount > 0;

  const report = {
    missionId,
    tenantId: TENANT_ID,
    executedAt: new Date().toISOString(),
    intent: EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
    candidateCountBefore: beforeCount,
    candidateCountAfter: afterCount,
    candidatesPreserved,
    coverageBefore: before.coverage,
    coverageAfter: after.coverage,
    discoveryStatusBefore: before.discoveryStatus,
    discoveryStatusAfter: after.discoveryStatus,
    remainingEvidenceGaps: after.evidenceGaps,
    pendingIntent: after.pendingIntent,
    prioritizationApprovalPending: after.prioritizationApprovalPending,
    executionOutcome:
      routed.executionResult?.executionOutcome
      || routed.audit?.outcome
      || null,
    rolledBack: routed.executionResult?.rolledBack === true,
    exactNextOperatorAction: exactNextOperatorAction(after),
    sent: false,
    autosendEnabled: false,
  };

  return report;
}

if (require.main === module) {
  const args = parseArgs();
  run(args)
    .then((report) => {
      if (report.help) return;
      console.log(JSON.stringify(report, null, 2));
    })
    .catch((err) => {
      const out = {
        error: { code: err.code || null, message: err.message },
        before: err.before || null,
      };
      console.error(JSON.stringify(out, null, 2));
      process.exit(typeof err.code === 'string' ? 1 : 1);
    });
}

module.exports = { run, snapshotMetrics, extractCoverageSummary, extractEvidenceGaps };
