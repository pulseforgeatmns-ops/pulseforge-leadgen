#!/usr/bin/env node
'use strict';

/**
 * Babrun tenant 13 — MJ Electric canonical outreach reschedule.
 *
 * Dry-run by default (read-only preflight + eligible time computation).
 * Production apply requires explicit confirmation flags.
 *
 * Usage:
 *   node scripts/scheduleBabrunMjElectric.js
 *   node scripts/scheduleBabrunMjElectric.js \
 *     --confirm-production \
 *     --confirm=tenant_13-mj-electric-next-send
 *
 * Does NOT send email directly. Does NOT mutate prior skipped schedules.
 */

require('dotenv').config();

const pool = require('../db');
const { resolveOutreachAssetMessage } = require('../packages/acquisition-knowledge/resolveOutreachAssetMessage');
const { evaluateCapacityAuthorization } = require('../packages/emmett-outbound/TenantMailboxCapacity');
const {
  ensureCapacitySchema,
  produceTenantMailboxCapacityEnvelope,
  loadLatestEnvelope,
  queryPriorSendAnchor,
  queryScheduleSpacingConflicts,
} = require('../services/emmettTenantMailboxCapacity');
const {
  authorizeScheduledOutreachSend,
  evaluateSchedulingEligibility,
  buildIdempotencyKey,
  PostgresScheduleStore,
  PAST_DUE_POLICY,
} = require('../services/tenantOutreachScheduler');
const { PostgresTenantMailboxStore } = require('../services/tenantMailbox');

const MJ_ELECTRIC = Object.freeze({
  tenantId: '13',
  prospectId: 'c8c0282f-056b-42a3-8555-47c50d00c8a2',
  acquisitionKnowledgeObjectId: 'ak_babrun_prospect_p025',
  outreachAssetId: 'ak_babrun_outreach_final_10',
  sendingIdentityId: 'tsi_13_babrun_fedir',
  recipientEmail: 'contact@mjelectricsandiego.com',
  sequenceStep: 1,
  subject: 'Quick question, Roque',
  authorizedBy: 'jacob@gopulseforge.com',
  authorizationSource: 'operator_approved_batch_continuation',
});

const PRIOR_SKIPPED_SCHEDULE_ID = 'tosched_c1fb4d6b5b4de33b6a07fa69';
const CONFIRM_TOKEN = 'tenant_13-mj-electric-next-send';
const BUSINESS_TZ = 'America/New_York';
const MIN_LEAD_MINUTES = 10;
const EXECUTOR_OWNERSHIP = 'Railway cron → POST /cron/tenant-outreach-executor (every 1–5 minutes)';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    confirmProduction: false,
    confirmToken: null,
    help: false,
  };
  for (const arg of argv) {
    if (arg === '--confirm-production') args.confirmProduction = true;
    else if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg.startsWith('--confirm=')) args.confirmToken = arg.slice('--confirm='.length);
    else {
      throw Object.assign(new Error(`Unknown argument: ${arg}`), { code: 'unknown_argument' });
    }
  }
  return args;
}

function printUsage() {
  process.stdout.write(`Babrun MJ Electric canonical outreach reschedule (tenant ${MJ_ELECTRIC.tenantId})

Dry-run (default):
  node scripts/scheduleBabrunMjElectric.js

Production apply:
  node scripts/scheduleBabrunMjElectric.js \\
    --confirm-production \\
    --confirm=${CONFIRM_TOKEN}

Safety:
  Dry-run by default — no schedule rows created.
  Does NOT send email. Does NOT invoke SMTP.
  Does NOT mutate ${PRIOR_SKIPPED_SCHEDULE_ID}.
  Uses authorizeScheduledOutreachSend() only on confirmed apply.
`);
}

function requireEnv(name) {
  if (!process.env[name]) {
    throw Object.assign(new Error(`Missing required env: ${name}`), { code: 'runtime_env_missing' });
  }
}

function assertApplyAllowed(args) {
  if (!args.confirmProduction) {
    throw Object.assign(new Error('Refusing production apply without --confirm-production.'), {
      code: 'confirm_production_required',
    });
  }
  if (args.confirmToken !== CONFIRM_TOKEN) {
    throw Object.assign(new Error(`Refusing production apply without --confirm=${CONFIRM_TOKEN}.`), {
      code: 'confirm_token_required',
    });
  }
}

function zonedParts(date, timeZone) {
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
  const parts = Object.fromEntries(
    fmt.formatToParts(date).filter((p) => p.type !== 'literal').map((p) => [p.type, p.value])
  );
  return {
    year: Number(parts.year),
    month: Number(parts.month),
    day: Number(parts.day),
    hour: Number(parts.hour),
    minute: Number(parts.minute),
  };
}

function offsetMinutesForTimeZone(date, timeZone) {
  const parts = zonedParts(date, timeZone);
  const asUtc = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, 0, 0);
  return Math.round((asUtc - date.getTime()) / 60000);
}

function zonedDateTimeToUtc({ year, month, day, hour, minute }, timeZone) {
  const guess = new Date(Date.UTC(year, month - 1, day, hour, minute, 0, 0));
  const offset = offsetMinutesForTimeZone(guess, timeZone);
  return new Date(guess.getTime() - offset * 60000);
}

function roundUpToQuarterHour(date) {
  const ms = 15 * 60 * 1000;
  return new Date(Math.ceil(date.getTime() / ms) * ms);
}

function formatLocalEt(date, timeZone = BUSINESS_TZ) {
  return new Intl.DateTimeFormat('en-US', {
    timeZone,
    weekday: 'short',
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    timeZoneName: 'shortGeneric',
  }).format(date);
}

function windowBounds(timeZone, allowedSendWindow = {}) {
  const startHour = Number(allowedSendWindow.startHour ?? 9);
  const endHour = Number(allowedSendWindow.endHour ?? 16);
  return { startHour, endHour, timeZone: allowedSendWindow.timezone || timeZone };
}

function isWithinBusinessWindow(date, allowedSendWindow, timeZone = BUSINESS_TZ) {
  const { startHour, endHour } = windowBounds(timeZone, allowedSendWindow);
  const parts = zonedParts(date, allowedSendWindow?.timezone || timeZone);
  return parts.hour >= startHour && parts.hour < endHour;
}

function nextWindowOpenUtc(fromDate, allowedSendWindow, timeZone = BUSINESS_TZ) {
  const { startHour, endHour } = windowBounds(timeZone, allowedSendWindow);
  const tz = allowedSendWindow?.timezone || timeZone;
  let candidate = roundUpToQuarterHour(new Date(fromDate.getTime() + MIN_LEAD_MINUTES * 60000));

  for (let i = 0; i < 96 * 14; i += 1) {
    const parts = zonedParts(candidate, tz);
    if (parts.hour >= startHour && parts.hour < endHour) {
      return zonedDateTimeToUtc({
        year: parts.year,
        month: parts.month,
        day: parts.day,
        hour: parts.hour,
        minute: parts.minute,
      }, tz);
    }
    if (parts.hour < startHour) {
      return zonedDateTimeToUtc({
        year: parts.year,
        month: parts.month,
        day: parts.day,
        hour: startHour,
        minute: 0,
      }, tz);
    }
    candidate = zonedDateTimeToUtc({
      year: parts.year,
      month: parts.month,
      day: parts.day,
      hour: startHour,
      minute: 0,
    }, tz);
    candidate = new Date(candidate.getTime() + 24 * 60 * 60 * 1000);
  }
  throw new Error('Unable to locate next business window within 14 days.');
}

async function fetchOutreachAssetRow(client, tenantId, assetId) {
  const res = await client.query(
    `SELECT id, object_type, title, channel, lifecycle_state, validation_state, status,
            content, provenance, relationships, version, updated_at
     FROM acquisition_knowledge_objects
     WHERE tenant_id = $1 AND id = $2 AND object_type = 'outreach_asset'
     LIMIT 1`,
    [tenantId, assetId]
  );
  return res.rows[0] || null;
}

async function loadValidatedAsset(client) {
  const row = await fetchOutreachAssetRow(client, MJ_ELECTRIC.tenantId, MJ_ELECTRIC.outreachAssetId);
  if (!row) {
    throw Object.assign(new Error(`Outreach asset not found: ${MJ_ELECTRIC.outreachAssetId}`), {
      code: 'outreach_asset_not_found',
    });
  }
  const resolved = resolveOutreachAssetMessage(row, { requireStakeholderValidated: true });
  const statementBody = typeof row.content?.statement === 'string' ? row.content.statement.trim() : '';
  const body = statementBody || resolved.body;
  return {
    lifecycleState: row.lifecycle_state,
    validationState: row.validation_state,
    status: row.status,
    outreachAssetVersion: resolved.revision,
    subject: MJ_ELECTRIC.subject || resolved.subject,
    body,
    copySource: statementBody ? 'content.statement' : resolved.source,
    assetVersion: resolved.version,
    updatedAt: row.updated_at,
  };
}

function extractRecipientEmail(recipients) {
  const list = Array.isArray(recipients) ? recipients : [];
  for (const entry of list) {
    if (typeof entry === 'string' && entry.includes('@')) {
      return entry.trim().toLowerCase();
    }
    if (entry && typeof entry === 'object') {
      const email = entry.email || entry.address;
      if (typeof email === 'string' && email.includes('@')) {
        return email.trim().toLowerCase();
      }
    }
  }
  return null;
}

function deriveScheduleIdFromMessage(row) {
  if (!row || typeof row !== 'object') return null;
  if (row.schedule_id) return row.schedule_id;
  const metadata = row.metadata && typeof row.metadata === 'object' ? row.metadata : {};
  if (typeof metadata.scheduleId === 'string' && metadata.scheduleId) return metadata.scheduleId;
  if (typeof metadata.schedule_id === 'string' && metadata.schedule_id) return metadata.schedule_id;
  return null;
}

function normalizeLatestSuccessfulSend(row) {
  if (!row) return null;
  return {
    id: row.id,
    prospectId: row.prospect_id,
    sentAt: row.sent_at,
    recipients: row.recipients,
    recipientEmail: extractRecipientEmail(row.recipients),
    outreachAssetId: row.outreach_asset_id,
    threadId: row.thread_id,
    status: row.status,
    scheduleId: deriveScheduleIdFromMessage(row),
  };
}

async function loadSuppression(db = pool) {
  const mailboxStore = new PostgresTenantMailboxStore(db);
  return mailboxStore.findSuppression(MJ_ELECTRIC.tenantId, MJ_ELECTRIC.recipientEmail);
}

async function loadProspectEligibility(db = pool) {
  const scheduleStore = new PostgresScheduleStore(db);
  return scheduleStore.getProspectEligibility(MJ_ELECTRIC.tenantId, MJ_ELECTRIC.prospectId);
}

async function loadMissionId(db = pool) {
  const res = await db.query(
    `SELECT ako.mission_id
       FROM acquisition_prospect_projections app
       JOIN acquisition_knowledge_objects ako
         ON ako.id = app.acquisition_knowledge_object_id
        AND ako.tenant_id = app.tenant_id
      WHERE app.tenant_id = $1
        AND app.prospect_id::text = $2
      LIMIT 1`,
    [MJ_ELECTRIC.tenantId, MJ_ELECTRIC.prospectId]
  );
  return res.rows[0]?.mission_id || null;
}

async function loadActiveSchedules(db = pool) {
  const res = await db.query(
    `SELECT id, status, scheduled_for, recipient_email, prospect_id, skip_reason
       FROM tenant_outreach_scheduled_sends
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND status IN ('SCHEDULED', 'EXECUTING')
      ORDER BY scheduled_for ASC`,
    [MJ_ELECTRIC.tenantId, MJ_ELECTRIC.sendingIdentityId]
  );
  return res.rows;
}

async function loadActiveReservations(db = pool) {
  const res = await db.query(
    `SELECT id, schedule_id, status, scheduled_for
       FROM emmett_tenant_mailbox_capacity_reservations
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND status IN ('scheduled', 'executing')
      ORDER BY scheduled_for ASC`,
    [MJ_ELECTRIC.tenantId, MJ_ELECTRIC.sendingIdentityId]
  );
  return res.rows;
}

async function loadLatestSuccessfulSend(db = pool) {
  const res = await db.query(
    `SELECT
       m.id,
       m.prospect_id,
       m.sent_at,
       m.recipients,
       m.outreach_asset_id,
       m.thread_id,
       m.status,
       m.metadata,
       s.id AS schedule_id
       FROM tenant_outreach_messages m
      LEFT JOIN tenant_outreach_scheduled_sends s
        ON s.tenant_id = m.tenant_id
       AND s.outbound_message_id = m.id
      WHERE m.tenant_id = $1
        AND m.sending_identity_id = $2
        AND m.direction = 'OUTBOUND'
        AND m.status = 'sent'
      ORDER BY m.sent_at DESC
      LIMIT 1`,
    [MJ_ELECTRIC.tenantId, MJ_ELECTRIC.sendingIdentityId]
  );
  return normalizeLatestSuccessfulSend(res.rows[0] || null);
}

async function loadPriorSkippedSchedule(db = pool) {
  const res = await db.query(
    `SELECT id, status, skip_reason, scheduled_for, updated_at
       FROM tenant_outreach_scheduled_sends
      WHERE tenant_id = $1 AND id = $2
      LIMIT 1`,
    [MJ_ELECTRIC.tenantId, PRIOR_SKIPPED_SCHEDULE_ID]
  );
  return res.rows[0] || null;
}

async function loadCapacityEnvelope(now) {
  await ensureCapacitySchema(pool);
  let envelope = await loadLatestEnvelope(
    MJ_ELECTRIC.tenantId,
    MJ_ELECTRIC.sendingIdentityId,
    pool,
    now
  );
  if (!envelope) {
    const produced = await produceTenantMailboxCapacityEnvelope(
      MJ_ELECTRIC.tenantId,
      MJ_ELECTRIC.sendingIdentityId,
      { pool, now }
    );
    envelope = produced.envelope;
  }
  return envelope;
}

async function evaluateCandidate(client, envelope, scheduledForIso, now) {
  const scheduleConflicts = await queryScheduleSpacingConflicts(
    MJ_ELECTRIC.tenantId,
    MJ_ELECTRIC.sendingIdentityId,
    scheduledForIso,
    envelope.minimumSpacingMinutes,
    client
  );
  const prior = await queryPriorSendAnchor(
    MJ_ELECTRIC.tenantId,
    MJ_ELECTRIC.sendingIdentityId,
    scheduledForIso,
    client
  );
  const authCheck = evaluateCapacityAuthorization(envelope, {
    scheduledFor: scheduledForIso,
    scheduleConflicts,
    lastSendAt: prior?.lastSendAt || null,
  }, { now });
  return { authCheck, scheduleConflicts, priorSendAnchor: prior };
}

async function findEligibleScheduleTime(client, envelope, now) {
  const allowedSendWindow = envelope.allowedSendWindow || { startHour: 9, endHour: 16, timezone: BUSINESS_TZ };
  const tz = allowedSendWindow.timezone || BUSINESS_TZ;
  let candidate = nextWindowOpenUtc(now, allowedSendWindow, tz);

  for (let attempt = 0; attempt < 96 * 14; attempt += 1) {
    if (!isWithinBusinessWindow(candidate, allowedSendWindow, tz)) {
      candidate = nextWindowOpenUtc(candidate, allowedSendWindow, tz);
      continue;
    }

    const scheduledForIso = candidate.toISOString();
    const { authCheck, scheduleConflicts, priorSendAnchor } = await evaluateCandidate(
      client,
      envelope,
      scheduledForIso,
      now
    );

    if (authCheck.allowed) {
      const leadMs = candidate.getTime() - now.getTime();
      const verdict = leadMs <= MIN_LEAD_MINUTES * 60000 + 60 * 1000
        ? 'AUTHORIZED_NOW'
        : `AUTHORIZED_AT ${scheduledForIso}`;
      return {
        scheduledForIso,
        scheduledLocalEt: formatLocalEt(candidate, tz),
        verdict,
        scheduleConflicts,
        priorSendAnchor,
        authCheck,
      };
    }

    if (authCheck.code === 'emmett_spacing_violation') {
      const minSpacingMs = Number(envelope.minimumSpacingMinutes || 0) * 60000;
      const anchorMs = priorSendAnchor?.lastSendAt
        ? new Date(priorSendAnchor.lastSendAt).getTime()
        : null;
      if (anchorMs != null && Number.isFinite(anchorMs)) {
        candidate = new Date(Math.max(candidate.getTime() + 15 * 60000, anchorMs + minSpacingMs));
      } else if (scheduleConflicts.length) {
        const latestConflict = scheduleConflicts.reduce((best, row) => {
          const at = new Date(row.at).getTime();
          return !best || at > best ? at : best;
        }, null);
        candidate = new Date(Math.max(candidate.getTime() + 15 * 60000, latestConflict + minSpacingMs));
      } else {
        candidate = new Date(candidate.getTime() + 15 * 60000);
      }
      continue;
    }

    if (authCheck.code === 'emmett_capacity_exhausted') {
      candidate = new Date(candidate.getTime() + 24 * 60 * 60 * 1000);
      candidate = nextWindowOpenUtc(candidate, allowedSendWindow, tz);
      continue;
    }

    if (authCheck.code === 'emmett_outside_send_window') {
      candidate = nextWindowOpenUtc(new Date(candidate.getTime() + 15 * 60000), allowedSendWindow, tz);
      continue;
    }

    return {
      blocked: true,
      verdict: `BLOCKED ${authCheck.code || authCheck.reason || 'capacity_denied'}`,
      authCheck,
      scheduledForIso: null,
    };
  }

  return {
    blocked: true,
    verdict: 'BLOCKED no_eligible_window',
    authCheck: { allowed: false, code: 'no_eligible_window', reason: 'No eligible window found within 14 days.' },
    scheduledForIso: null,
  };
}

async function runPreflight(client, asset, now) {
  const [
    envelope,
    prospectEligibility,
    suppression,
    latestSuccessfulSend,
    activeSchedules,
    activeReservations,
    priorSkippedSchedule,
    missionId,
  ] = await Promise.all([
    loadCapacityEnvelope(now),
    loadProspectEligibility(pool),
    loadSuppression(pool),
    loadLatestSuccessfulSend(pool),
    loadActiveSchedules(pool),
    loadActiveReservations(pool),
    loadPriorSkippedSchedule(pool),
    loadMissionId(pool),
  ]);

  const schedulingBlocked = [];
  if (!prospectEligibility.exists) schedulingBlocked.push('prospect_not_found');
  if (prospectEligibility.doNotContact) schedulingBlocked.push('prospect_dnc');
  if (prospectEligibility.booked) schedulingBlocked.push('prospect_booked');
  if (suppression) schedulingBlocked.push(`suppressed:${suppression.reason || 'unknown'}`);
  if (String(asset.lifecycleState || '').toUpperCase() !== 'STAKEHOLDER_VALIDATED') {
    schedulingBlocked.push('asset_not_stakeholder_validated');
  }
  if (['pause', 'emergency'].includes(String(envelope.governorState || '').toLowerCase())) {
    schedulingBlocked.push(`governor_${envelope.governorState}`);
  }

  const eligibleTime = schedulingBlocked.length
    ? {
      blocked: true,
      verdict: `BLOCKED ${schedulingBlocked.join(',')}`,
      scheduledForIso: null,
    }
    : await findEligibleScheduleTime(client, envelope, now);

  let schedulingEligibility = null;
  if (eligibleTime.scheduledForIso && !eligibleTime.blocked) {
    const scheduleStore = new PostgresScheduleStore(pool);
    const mailboxStore = new PostgresTenantMailboxStore(pool);
    schedulingEligibility = await evaluateSchedulingEligibility({
      tenantId: MJ_ELECTRIC.tenantId,
      prospectId: MJ_ELECTRIC.prospectId,
      outreachAssetId: MJ_ELECTRIC.outreachAssetId,
      outreachAssetVersion: asset.outreachAssetVersion,
      sendingIdentityId: MJ_ELECTRIC.sendingIdentityId,
      recipientEmail: MJ_ELECTRIC.recipientEmail,
      sequenceStep: MJ_ELECTRIC.sequenceStep,
      scheduledFor: eligibleTime.scheduledForIso,
      timezone: BUSINESS_TZ,
      pastDuePolicy: PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
      maxLatenessMinutes: 30,
      authorizationSnapshot: {
        recipientEmail: MJ_ELECTRIC.recipientEmail,
        outreachAssetId: MJ_ELECTRIC.outreachAssetId,
        outreachAssetVersion: asset.outreachAssetVersion,
        sendingIdentityId: MJ_ELECTRIC.sendingIdentityId,
        scheduledFor: eligibleTime.scheduledForIso,
        timezone: BUSINESS_TZ,
        sequenceStep: MJ_ELECTRIC.sequenceStep,
        subject: asset.subject,
        body: asset.body,
      },
    }, { scheduleStore, mailboxStore });
    if (!schedulingEligibility.eligible) {
      eligibleTime.blocked = true;
      eligibleTime.verdict = `BLOCKED ${schedulingEligibility.reason}`;
    }
  }

  return {
    now: now.toISOString(),
    missionId,
    prospectEligibility,
    suppression,
    asset: {
      lifecycleState: asset.lifecycleState,
      validationState: asset.validationState,
      outreachAssetVersion: asset.outreachAssetVersion,
      subject: asset.subject,
      bodyLength: asset.body.length,
      copySource: asset.copySource,
    },
    latestSuccessfulSend,
    envelope: {
      envelopeId: envelope.envelopeId,
      governorState: envelope.governorState,
      minimumSpacingMinutes: envelope.minimumSpacingMinutes,
      remainingCapacity: envelope.remainingCapacity,
      maxSendsPerDay: envelope.maxSendsPerDay,
      currentSentCount: envelope.currentSentCount,
      currentScheduledCount: envelope.currentScheduledCount,
      currentExecutingCount: envelope.currentExecutingCount,
      allowedSendWindow: envelope.allowedSendWindow,
      validUntil: envelope.validUntil,
    },
    activeSchedules,
    activeReservations,
    priorSkippedSchedule,
    eligibleTime,
    schedulingEligibility,
    schedulingBlocked,
  };
}

async function loadReservationForSchedule(client, scheduleId) {
  const res = await client.query(
    `SELECT id, envelope_id, status, scheduled_for
       FROM emmett_tenant_mailbox_capacity_reservations
      WHERE tenant_id = $1 AND schedule_id = $2
      LIMIT 1`,
    [MJ_ELECTRIC.tenantId, scheduleId]
  );
  return res.rows[0] || null;
}

async function applySchedule(client, preflight, asset) {
  const scheduledFor = preflight.eligibleTime.scheduledForIso;
  const idempotencyKey = buildIdempotencyKey({
    tenantId: MJ_ELECTRIC.tenantId,
    prospectId: MJ_ELECTRIC.prospectId,
    outreachAssetId: MJ_ELECTRIC.outreachAssetId,
    outreachAssetVersion: asset.outreachAssetVersion,
    sendingIdentityId: MJ_ELECTRIC.sendingIdentityId,
    recipientEmail: MJ_ELECTRIC.recipientEmail,
    sequenceStep: MJ_ELECTRIC.sequenceStep,
    scheduledFor,
  });

  const auth = await authorizeScheduledOutreachSend({
    tenantId: MJ_ELECTRIC.tenantId,
    prospectId: MJ_ELECTRIC.prospectId,
    acquisitionKnowledgeObjectId: MJ_ELECTRIC.acquisitionKnowledgeObjectId,
    outreachAssetId: MJ_ELECTRIC.outreachAssetId,
    outreachAssetVersion: asset.outreachAssetVersion,
    sendingIdentityId: MJ_ELECTRIC.sendingIdentityId,
    recipientEmail: MJ_ELECTRIC.recipientEmail,
    missionId: preflight.missionId || null,
    sequenceStep: MJ_ELECTRIC.sequenceStep,
    scheduledFor,
    timezone: BUSINESS_TZ,
    authorizedBy: MJ_ELECTRIC.authorizedBy,
    authorizationSource: MJ_ELECTRIC.authorizationSource,
    subject: asset.subject,
    body: asset.body,
    pastDuePolicy: PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
    maxLatenessMinutes: 30,
    idempotencyKey,
  }, { pool });

  const reservation = auth.schedule?.id
    ? await loadReservationForSchedule(client, auth.schedule.id)
    : null;

  return {
    authorization: auth,
    idempotencyKey,
    reservation,
  };
}

function buildReport(preflight, applyResult, mode) {
  const prior = preflight.priorSkippedSchedule;
  return {
    generatedAt: new Date().toISOString(),
    mode,
    target: MJ_ELECTRIC,
    priorSkippedScheduleGuard: {
      scheduleId: PRIOR_SKIPPED_SCHEDULE_ID,
      unchanged: prior
        ? prior.status === 'SKIPPED' && prior.skip_reason === 'emmett_spacing_violation'
        : null,
      observed: prior
        ? { status: prior.status, skipReason: prior.skip_reason, scheduledFor: prior.scheduled_for }
        : null,
    },
    dryRunVerdict: preflight.eligibleTime.verdict,
    A_prospectEligibility: preflight.prospectEligibility,
    B_suppressionState: preflight.suppression
      ? { suppressed: true, reason: preflight.suppression.reason, email: preflight.suppression.email }
      : { suppressed: false },
    C_assetValidationState: preflight.asset,
    D_latestSuccessfulSend: preflight.latestSuccessfulSend,
    E_currentGovernor: preflight.envelope.governorState,
    F_minimumSpacingMinutes: preflight.envelope.minimumSpacingMinutes,
    G_remainingCapacity: preflight.envelope.remainingCapacity,
    H_eligibleTime: {
      verdict: preflight.eligibleTime.verdict,
      scheduledFor: preflight.eligibleTime.scheduledForIso || null,
      scheduledLocalEt: preflight.eligibleTime.scheduledLocalEt || null,
      priorSendAnchor: preflight.eligibleTime.priorSendAnchor || null,
      scheduleConflicts: preflight.eligibleTime.scheduleConflicts || [],
    },
    I_authorizationResult: applyResult
      ? {
        created: applyResult.authorization.created,
        duplicate: applyResult.authorization.duplicate,
        scheduleId: applyResult.authorization.schedule?.id || null,
        status: applyResult.authorization.schedule?.status || null,
        idempotencyKey: applyResult.idempotencyKey,
      }
      : { applied: false, note: 'Dry-run — authorizeScheduledOutreachSend() not invoked.' },
    J_newScheduleId: applyResult?.authorization?.schedule?.id || null,
    K_reservation: applyResult?.reservation
      ? {
        id: applyResult.reservation.id,
        status: applyResult.reservation.status,
        envelopeId: applyResult.reservation.envelope_id,
      }
      : null,
    L_executorOwnership: EXECUTOR_OWNERSHIP,
    M_directSendOccurred: false,
    N_priorSkippedScheduleUnchanged: prior
      ? prior.status === 'SKIPPED' && prior.skip_reason === 'emmett_spacing_violation'
      : null,
    emmettEnvelope: preflight.envelope,
    activeSchedules: preflight.activeSchedules,
    activeReservations: preflight.activeReservations,
    schedulingEligibility: preflight.schedulingEligibility,
    schedulingBlocked: preflight.schedulingBlocked,
  };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    printUsage();
    return;
  }

  requireEnv('DATABASE_URL');
  const now = new Date();
  const client = await pool.connect();

  try {
    const asset = await loadValidatedAsset(client);
    const preflight = await runPreflight(client, asset, now);

    let applyResult = null;
    const canApply = args.confirmProduction
      && !preflight.eligibleTime.blocked
      && preflight.eligibleTime.scheduledForIso
      && (!preflight.schedulingEligibility || preflight.schedulingEligibility.eligible);

    if (args.confirmProduction) {
      assertApplyAllowed(args);
      if (!canApply) {
        throw Object.assign(
          new Error(`Refusing apply: ${preflight.eligibleTime.verdict}`),
          { code: 'apply_blocked', preflight }
        );
      }
      applyResult = await applySchedule(client, preflight, asset);
    }

    const postSkipped = await loadPriorSkippedSchedule(client);
    preflight.priorSkippedSchedule = postSkipped;

    const mode = applyResult ? 'production_apply' : 'dry_run';
    const report = buildReport(preflight, applyResult, mode);
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);

    if (preflight.eligibleTime.blocked || report.schedulingBlocked?.length) {
      process.exitCode = 2;
    }
  } finally {
    client.release();
  }
}

if (require.main === module) {
  main().catch(async (err) => {
    process.stderr.write(`${err.stack || err.message}\n`);
    process.exitCode = 1;
  }).finally(async () => {
    await pool.end().catch(() => {});
  });
}

module.exports = {
  MJ_ELECTRIC,
  PRIOR_SKIPPED_SCHEDULE_ID,
  extractRecipientEmail,
  deriveScheduleIdFromMessage,
  normalizeLatestSuccessfulSend,
  loadLatestSuccessfulSend,
  loadActiveSchedules,
  loadActiveReservations,
  loadPriorSkippedSchedule,
  loadMissionId,
  findEligibleScheduleTime,
  runPreflight,
  buildReport,
};
