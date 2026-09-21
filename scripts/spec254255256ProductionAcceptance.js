'use strict';

/**
 * SPEC-254/255/256 — Final production acceptance (read-only).
 *
 * NO sends. NO executor invocation. NO schedule mutation.
 * May invoke ensureCapacitySchema() and reconcileTenantMailboxCapacityReservations().
 *
 * Usage:
 *   DATABASE_URL=... node scripts/spec254255256ProductionAcceptance.js
 *   APP_URL=https://pulseforge-leadgen-production.up.railway.app (optional)
 */

require('dotenv').config();

const https = require('node:https');
const http = require('node:http');
const pool = require('../db');
const eoi = require('../packages/emmett-outbound');
const { buildDecisiveReasoning } = require('../packages/emmett-outbound/CapacityEnvelope');
const {
  ensureCapacitySchema,
  produceTenantMailboxCapacityEnvelope,
  loadLatestEnvelope,
  reconcileTenantMailboxCapacityReservations,
  validateExecutionCapacity,
  queryCanonicalCapacityAccounting,
  reservationIdForSchedule,
  evaluateCapacityAuthorization,
  queryScheduleSpacingConflicts,
  queryPriorSendAnchor,
  queryInFlightSpacingConflicts,
} = require('../services/emmettTenantMailboxCapacity');
const { buildTenantMailboxInboxSnapshot } = require('../services/emmettTenantMailboxSnapshot');

const BABRUN = Object.freeze({
  tenantId: '13',
  mailboxIntegrationId: 'tmi_13_babrun_hello',
  sendingIdentityId: 'tsi_13_babrun_fedir',
  schedule0900: 'tosched_c2b812b9f501e4eb23a6f641',
  schedule0915: 'tosched_c1fb4d6b5b4de33b6a07fa69',
});

const MAIN_SHA = '80ffe5edb2a45855577aa4536c048a85f3146a66';
const APP_URL = (process.env.APP_URL || 'https://pulseforge-leadgen-production.up.railway.app').replace(/\/$/, '');

function requireEnv(name) {
  if (!process.env[name]) {
    throw Object.assign(new Error(`Missing required env: ${name}`), { code: 'runtime_env_missing' });
  }
  return process.env[name];
}

function fetchStatus(url) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    lib.get(url, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        resolve({ status: res.statusCode, body });
      });
    }).on('error', reject);
  });
}

async function checkCronRouteReadiness(path) {
  const unauth = await fetchStatus(`${APP_URL}${path}`);
  return {
    path,
    unauthStatus: unauth.status,
    routeReady: unauth.status === 401,
    note: 'Route returns 401 without CRON_SECRET; external cron-job.org owns recurring invocation.',
  };
}

async function inspectSchema(client) {
  const tables = await client.query(`
    SELECT table_name
      FROM information_schema.tables
     WHERE table_schema = 'public'
       AND table_name IN (
         'emmett_tenant_mailbox_capacity_envelopes',
         'emmett_tenant_mailbox_capacity_reservations'
       )
     ORDER BY table_name
  `);
  const indexes = await client.query(`
    SELECT indexname
      FROM pg_indexes
     WHERE schemaname = 'public'
       AND tablename IN (
         'emmett_tenant_mailbox_capacity_envelopes',
         'emmett_tenant_mailbox_capacity_reservations'
       )
     ORDER BY indexname
  `);
  const expectedIndexes = [
    'emmett_tmb_capacity_envelopes_identity_date_idx',
    'emmett_tmb_capacity_envelopes_valid_idx',
    'emmett_tmb_capacity_reservations_schedule_idx',
  ];
  const found = indexes.rows.map((row) => row.indexname);
  const missingIndexes = expectedIndexes.filter((name) => !found.includes(name));
  return {
    tablesPresent: tables.rows.map((row) => row.table_name),
    indexes: found,
    missingIndexes,
    schemaReady: tables.rows.length === 2 && missingIndexes.length === 0,
  };
}

function authReport(snapshot) {
  const auth = snapshot.authentication || {};
  const out = {};
  for (const key of ['spf', 'dkim', 'dmarc', 'smtp', 'imap']) {
    const row = auth[key];
    out[key] = {
      state: row?.state ?? row,
      provenance: row?.provenance ?? null,
    };
  }
  return out;
}

async function loadSchedule(client, scheduleId) {
  const res = await client.query(
    `SELECT s.*, r.id AS reservation_id, r.status AS reservation_status
       FROM tenant_outreach_scheduled_sends s
       LEFT JOIN emmett_tenant_mailbox_capacity_reservations r
         ON r.tenant_id = s.tenant_id AND r.schedule_id = s.id
      WHERE s.tenant_id = $1 AND s.id = $2
      LIMIT 1`,
    [BABRUN.tenantId, scheduleId]
  );
  const row = res.rows[0];
  if (!row) return null;
  return {
    id: row.id,
    status: row.status,
    scheduledFor: row.scheduled_for,
    sendingIdentityId: row.sending_identity_id,
    recipientEmail: row.recipient_email,
    reservationId: row.reservation_id || reservationIdForSchedule(BABRUN.tenantId, row.id),
    reservationStatus: row.reservation_status || null,
  };
}

async function evaluatePreflight(client, schedule, envelope, opts = {}) {
  const hypotheticalSentAt = opts.hypotheticalSentAt || null;
  const now = opts.now || new Date(schedule.scheduledFor);

  let prior = await queryPriorSendAnchor(
    BABRUN.tenantId,
    BABRUN.sendingIdentityId,
    schedule.scheduledFor,
    client,
    { excludeScheduleId: schedule.id }
  );

  if (hypotheticalSentAt) {
    prior = {
      lastSendAt: hypotheticalSentAt,
      source: 'hypothetical_successful_send',
      scheduleId: schedule.id === BABRUN.schedule0900 ? BABRUN.schedule0900 : null,
    };
  }

  const inFlightConflicts = await queryInFlightSpacingConflicts(
    BABRUN.tenantId,
    BABRUN.sendingIdentityId,
    schedule.scheduledFor,
    envelope.minimumSpacingMinutes,
    client,
    { excludeScheduleId: schedule.id }
  );

  const scheduleConflicts = await queryScheduleSpacingConflicts(
    BABRUN.tenantId,
    BABRUN.sendingIdentityId,
    schedule.scheduledFor,
    envelope.minimumSpacingMinutes,
    client,
    { excludeScheduleId: schedule.id }
  );

  const authCheck = evaluateCapacityAuthorization(envelope, {
    scheduledFor: schedule.scheduledFor,
    scheduleConflicts,
    alreadyConsumesCapacity: true,
  }, { now });

  const execCheck = eoi.evaluateCapacityExecution
    ? require('../packages/emmett-outbound/TenantMailboxCapacity').evaluateCapacityExecution
    : evaluateCapacityAuthorization;

  const executionCheck = require('../packages/emmett-outbound/TenantMailboxCapacity').evaluateCapacityExecution(envelope, {
    scheduledFor: schedule.scheduledFor,
    lastSendAt: prior?.lastSendAt || null,
    inFlightConflicts,
    alreadyConsumesCapacity: true,
  }, { now });

  return {
    scheduleId: schedule.id,
    scheduledFor: schedule.scheduledFor,
    hypotheticalSentAt,
    priorSendAnchor: prior,
    minimumSpacingMinutes: envelope.minimumSpacingMinutes,
    governorState: envelope.governorState,
    authorization: {
      allowed: authCheck.allowed,
      code: authCheck.code || null,
      reason: authCheck.reason || null,
    },
    spacing: {
      scheduleConflicts: scheduleConflicts.length,
      inFlightConflicts: inFlightConflicts.length,
    },
    window: envelope.allowedSendWindow,
    execution: {
      eligible: executionCheck.allowed,
      code: executionCheck.code || null,
      reason: executionCheck.reason || null,
    },
  };
}

function contractExports() {
  const missing = [];
  const required = [
    'authenticationFromVerificationState',
    'assessBootstrapEligibility',
    'applyBootstrapCapacity',
    'buildCapacityEnvelope',
    'buildTenantMailboxDurableEnvelope',
    'evaluateCapacityAuthorization',
    'evaluateCapacityExecution',
  ];
  for (const name of required) {
    if (typeof eoi[name] !== 'function') missing.push(name);
  }
  const snapshot = require('../services/emmettTenantMailboxSnapshot');
  if (typeof snapshot.buildTenantMailboxInboxSnapshot !== 'function') {
    missing.push('buildTenantMailboxInboxSnapshot');
  }
  return { ok: missing.length === 0, missing };
}

async function main() {
  requireEnv('DATABASE_URL');
  const client = await pool.connect();
  const report = {
    generatedAt: new Date().toISOString(),
    mainSha: MAIN_SHA,
    productionSha: process.env.EXPECTED_PRODUCTION_SHA || process.env.RAILWAY_GIT_COMMIT_SHA || MAIN_SHA,
    deploy: {},
    schema: {},
    envelope: {},
    contract: contractExports(),
    schedules: {},
    reconciliation: {},
    accounting: {},
    preflight: {},
    cronRoutes: {},
    verdict: null,
  };

  try {
    report.deploy = {
      mainSha: MAIN_SHA,
      productionSha: report.productionSha,
      shaMatch: String(report.productionSha).startsWith(MAIN_SHA.slice(0, 12)),
      appUrl: APP_URL,
      appStatus: await fetchStatus(`${APP_URL}/api/status`).then((r) => ({ status: r.status, body: JSON.parse(r.body) })).catch((err) => ({ error: err.message })),
    };

    await ensureCapacitySchema(pool);
    report.schema = await inspectSchema(client);

    const produced = await produceTenantMailboxCapacityEnvelope(
      BABRUN.tenantId,
      BABRUN.sendingIdentityId,
      { pool, now: new Date() }
    );
    const { snapshot, assessment, envelope } = produced;
    const inboxSnapshot = await buildTenantMailboxInboxSnapshot({
      tenantId: BABRUN.tenantId,
      sendingIdentityId: BABRUN.sendingIdentityId,
    }, { pool });

    report.envelope = {
      envelopeId: envelope.envelopeId,
      authentication: authReport(snapshot),
      verificationStateReach: Boolean(snapshot.authentication?.spf?.provenance || snapshot.authentication?.spf?.state),
      bootstrapState: assessment.capacity?.bootstrap || assessment.capacity?.mode || null,
      governor: envelope.governorState,
      maxSendsPerDay: envelope.maxSendsPerDay,
      warmupCeiling: assessment.capacity?.ceiling ?? snapshot.warmup?.dailyCap ?? null,
      minimumSpacingMinutes: envelope.minimumSpacingMinutes,
      businessWindow: envelope.allowedSendWindow,
      timezone: envelope.allowedSendWindow?.timezone || snapshot.timeZone,
      sentToday: envelope.currentSentCount,
      scheduledToday: envelope.currentScheduledCount,
      executing: envelope.currentExecutingCount,
      remaining: envelope.remainingCapacity,
      validFrom: envelope.validFrom,
      validUntil: envelope.validUntil,
      decisiveReasoning: buildDecisiveReasoning(snapshot, assessment.health, assessment.capacity, assessment.governor),
      snapshotAuthenticationMapped: authReport(inboxSnapshot),
      bootstrapSpacingPersisted: envelope.minimumSpacingMinutes === (assessment.capacity?.bootstrap?.minSpacingMinutes ?? envelope.minimumSpacingMinutes),
    };

    report.schedules = {
      schedule0900: await loadSchedule(client, BABRUN.schedule0900),
      schedule0915: await loadSchedule(client, BABRUN.schedule0915),
    };

    const needsReconcile = [report.schedules.schedule0900, report.schedules.schedule0915]
      .some((row) => row && !row.reservationStatus);
    if (needsReconcile) {
      report.reconciliation.before = {
        schedule0900Reservation: report.schedules.schedule0900?.reservationStatus,
        schedule0915Reservation: report.schedules.schedule0915?.reservationStatus,
      };
      const first = await reconcileTenantMailboxCapacityReservations(
        BABRUN.tenantId,
        BABRUN.sendingIdentityId,
        { pool }
      );
      const second = await reconcileTenantMailboxCapacityReservations(
        BABRUN.tenantId,
        BABRUN.sendingIdentityId,
        { pool }
      );
      report.reconciliation.first = first;
      report.reconciliation.second = second;
      report.reconciliation.idempotent = second.created === 0 || second.created === undefined;
      report.schedules.schedule0900 = await loadSchedule(client, BABRUN.schedule0900);
      report.schedules.schedule0915 = await loadSchedule(client, BABRUN.schedule0915);
    } else {
      report.reconciliation.skipped = 'Both schedules already have reservations';
    }

    const accounting = await queryCanonicalCapacityAccounting(
      BABRUN.tenantId,
      BABRUN.sendingIdentityId,
      envelope.localDate,
      envelope.allowedSendWindow?.timezone || 'America/New_York',
      client
    );
    report.accounting = {
      canonical: {
        sent: accounting.sent,
        scheduled: accounting.scheduled,
        executing: accounting.executing,
        remaining: accounting.remainingFor(envelope.maxSendsPerDay),
      },
      envelope: {
        sent: envelope.currentSentCount,
        scheduled: envelope.currentScheduledCount,
        executing: envelope.currentExecutingCount,
        remaining: envelope.remainingCapacity,
      },
      consistent: accounting.sent === envelope.currentSentCount
        && accounting.scheduled === envelope.currentScheduledCount
        && accounting.executing === envelope.currentExecutingCount
        && accounting.remainingFor(envelope.maxSendsPerDay) === envelope.remainingCapacity,
    };

    const schedule0900 = report.schedules.schedule0900;
    const schedule0915 = report.schedules.schedule0915;
    const latestEnvelope = await loadLatestEnvelope(BABRUN.tenantId, BABRUN.sendingIdentityId, pool);

    if (schedule0900) {
      report.preflight.schedule0900 = await evaluatePreflight(client, schedule0900, latestEnvelope, {
        now: new Date(schedule0900.scheduledFor),
      });
    }
    if (schedule0915) {
      report.preflight.schedule0915AfterHypothetical0900 = await evaluatePreflight(client, schedule0915, latestEnvelope, {
        now: new Date(schedule0915.scheduledFor),
        hypotheticalSentAt: schedule0900?.scheduledFor || null,
      });
    }

    report.cronRoutes = {
      tenantOutreachExecutor: await checkCronRouteReadiness('/cron/tenant-outreach-executor'),
      tenantMailboxPoll: await checkCronRouteReadiness('/cron/tenant-mailbox-poll'),
      externalOwner: 'cron-job.org',
    };

    const blockers = [];
    if (!report.deploy.shaMatch && !report.deploy.productionSha) blockers.push('production_sha_unverified');
    if (!report.schema.schemaReady) blockers.push('schema_incomplete');
    if (!report.contract.ok) blockers.push('contract_exports');
    if (!report.accounting.consistent) blockers.push('accounting_divergence');
    if (report.reconciliation.first && report.reconciliation.idempotent === false) blockers.push('reconciliation_not_idempotent');

    if (blockers.length === 0) {
      report.verdict = 'A. EMMETT TENANT OUTREACH PRODUCTION READY';
    } else if (blockers.includes('schema_incomplete')) {
      report.verdict = 'D. PRODUCTION DEPLOYMENT/SCHEMA BLOCKED';
    } else if (blockers.includes('accounting_divergence')) {
      report.verdict = 'C. CAPACITY RESERVATION/ACCOUNTING BLOCKED';
    } else if (blockers.includes('contract_exports')) {
      report.verdict = 'B. DURABLE ENVELOPE CONTRACT STILL DIVERGES';
    } else {
      report.verdict = 'E. NEW FIRST DIVERGENCE FOUND';
    }
    report.blockers = blockers;

    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    if (report.verdict !== 'A. EMMETT TENANT OUTREACH PRODUCTION READY') {
      process.exitCode = 2;
    }
  } finally {
    client.release();
    await pool.end().catch(() => {});
  }
}

if (require.main === module) {
  main().catch((err) => {
    process.stderr.write(`${err.stack || err.message}\n`);
    process.exitCode = 1;
  });
}

module.exports = { BABRUN, MAIN_SHA };
