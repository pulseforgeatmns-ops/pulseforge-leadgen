'use strict';

/**
 * SPEC-254 — Emmett tenant-mailbox capacity envelope producer and gate.
 * SPEC-256 — temporally correct spacing + single capacity-accounting contract.
 */

const crypto = require('crypto');
const defaultPool = require('../db');
const { buildTenantMailboxSnapshot } = require('./emmettTenantMailboxSnapshot');
const {
  assessTenantMailboxCapacity,
  buildCapacityEnvelope,
  evaluateCapacityAuthorization,
  evaluateCapacityExecution,
} = require('../packages/emmett-outbound/TenantMailboxCapacity');
const {
  findPriorSendAnchor,
  findScheduleSpacingConflicts,
  findInFlightSpacingConflicts,
  accountCapacityUnits,
  LIVE_SCHEDULE_STATUSES,
  LIVE_RESERVATION_STATUSES,
} = require('../packages/emmett-outbound/TenantMailboxSpacing');
const { GOVERNOR_OUTCOMES } = require('../packages/emmett-outbound');

const ADVISORY_LOCK_NAMESPACE = 702254;

let schemaPromise;

function tenantKey(value) {
  if (value == null || value === '') return '';
  return String(value);
}

function capacityError(code, message, extras = {}) {
  const err = new Error(message || code);
  err.code = code;
  Object.assign(err, extras);
  return err;
}

function normalizeEnvelope(row) {
  if (!row) return null;
  const payload = typeof row.payload === 'object' ? row.payload : {};
  return {
    envelopeId: row.envelope_id || row.envelopeId || row.id,
    tenantId: tenantKey(row.tenant_id ?? row.tenantId),
    mailboxIntegrationId: row.mailbox_integration_id || row.mailboxIntegrationId,
    sendingIdentityId: row.sending_identity_id || row.sendingIdentityId,
    senderEmail: row.sender_email || row.senderEmail,
    sendingDomain: row.sending_domain || row.sendingDomain,
    localDate: row.local_date || row.localDate,
    computedAt: row.computed_at || row.computedAt,
    validFrom: row.valid_from || row.validFrom,
    validUntil: row.valid_until || row.validUntil,
    maxSendsPerDay: Number(row.max_sends_per_day ?? row.maxSendsPerDay ?? 0),
    maxSendsPerWindow: Number(row.max_sends_per_window ?? row.maxSendsPerWindow ?? row.max_sends_per_day ?? 0),
    minimumSpacingMinutes: Number(row.minimum_spacing_minutes ?? row.minimumSpacingMinutes ?? 30),
    allowedSendWindow: row.allowed_send_window || row.allowedSendWindow || { startHour: 9, endHour: 17 },
    rampStage: row.ramp_stage || row.rampStage || null,
    currentSentCount: Number(row.current_sent_count ?? row.currentSentCount ?? 0),
    currentScheduledCount: Number(row.current_scheduled_count ?? row.currentScheduledCount ?? 0),
    currentExecutingCount: Number(row.current_executing_count ?? row.currentExecutingCount ?? 0),
    remainingCapacity: Number(row.remaining_capacity ?? row.remainingCapacity ?? 0),
    governorState: row.governor_state || row.governorState,
    riskFlags: row.risk_flags || row.riskFlags || [],
    evidenceSnapshot: row.evidence_snapshot || row.evidenceSnapshot || {},
    emmettContribution: row.emmett_contribution || row.emmettContribution || {},
    version: Number(row.version || 1),
    payload,
  };
}

async function applyCapacitySchema(query) {
  await query(`
    CREATE TABLE IF NOT EXISTS emmett_tenant_mailbox_capacity_envelopes (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      mailbox_integration_id TEXT NOT NULL,
      sending_identity_id TEXT NOT NULL,
      sender_email TEXT NOT NULL,
      sending_domain TEXT,
      local_date DATE NOT NULL,
      computed_at TIMESTAMPTZ NOT NULL,
      valid_from TIMESTAMPTZ NOT NULL,
      valid_until TIMESTAMPTZ NOT NULL,
      max_sends_per_day INTEGER NOT NULL,
      max_sends_per_window INTEGER,
      minimum_spacing_minutes INTEGER NOT NULL DEFAULT 30,
      allowed_send_window JSONB NOT NULL DEFAULT '{"startHour":9,"endHour":17}'::jsonb,
      timezone TEXT NOT NULL DEFAULT 'America/New_York',
      ramp_stage TEXT,
      governor_state TEXT NOT NULL,
      current_sent_count INTEGER NOT NULL DEFAULT 0,
      current_scheduled_count INTEGER NOT NULL DEFAULT 0,
      current_executing_count INTEGER NOT NULL DEFAULT 0,
      remaining_capacity INTEGER NOT NULL DEFAULT 0,
      risk_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
      evidence_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
      emmett_contribution JSONB NOT NULL DEFAULT '{}'::jsonb,
      version INTEGER NOT NULL DEFAULT 1,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS emmett_tmb_capacity_envelopes_identity_date_idx
      ON emmett_tenant_mailbox_capacity_envelopes (tenant_id, sending_identity_id, local_date DESC)
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS emmett_tmb_capacity_envelopes_valid_idx
      ON emmett_tenant_mailbox_capacity_envelopes (tenant_id, sending_identity_id, valid_until DESC)
  `);
  await query(`
    CREATE TABLE IF NOT EXISTS emmett_tenant_mailbox_capacity_reservations (
      id TEXT PRIMARY KEY,
      envelope_id TEXT NOT NULL REFERENCES emmett_tenant_mailbox_capacity_envelopes(id),
      tenant_id TEXT NOT NULL,
      sending_identity_id TEXT NOT NULL,
      schedule_id TEXT,
      status TEXT NOT NULL,
      scheduled_for TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS emmett_tmb_capacity_reservations_schedule_idx
      ON emmett_tenant_mailbox_capacity_reservations (tenant_id, schedule_id)
      WHERE schedule_id IS NOT NULL
  `);
}

function ensureCapacitySchema(pool = defaultPool) {
  if (!schemaPromise) {
    schemaPromise = applyCapacitySchema(pool.query.bind(pool)).catch((err) => {
      schemaPromise = null;
      throw err;
    });
  }
  return schemaPromise;
}

function advisoryLockKey(tenantId, sendingIdentityId) {
  const hash = crypto.createHash('sha256').update(`${tenantId}:${sendingIdentityId}`).digest();
  return hash.readInt32BE(0);
}

async function loadLatestEnvelope(tenantId, sendingIdentityId, pool = defaultPool, now = new Date()) {
  await ensureCapacitySchema(pool);
  const res = await pool.query(
    `SELECT * FROM emmett_tenant_mailbox_capacity_envelopes
      WHERE tenant_id = $1 AND sending_identity_id = $2
        AND valid_until > $3::timestamptz
      ORDER BY computed_at DESC
      LIMIT 1`,
    [tenantKey(tenantId), tenantKey(sendingIdentityId), now.toISOString()]
  );
  return normalizeEnvelope(res.rows[0]);
}

async function persistEnvelope(envelope, pool = defaultPool) {
  await ensureCapacitySchema(pool);
  await pool.query(
    `INSERT INTO emmett_tenant_mailbox_capacity_envelopes (
        id, tenant_id, mailbox_integration_id, sending_identity_id, sender_email, sending_domain,
        local_date, computed_at, valid_from, valid_until, max_sends_per_day, max_sends_per_window,
        minimum_spacing_minutes, allowed_send_window, timezone, ramp_stage, governor_state,
        current_sent_count, current_scheduled_count, current_executing_count, remaining_capacity,
        risk_flags, evidence_snapshot, emmett_contribution, version, payload, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,NOW()
      )
      ON CONFLICT (id) DO UPDATE SET
        computed_at = EXCLUDED.computed_at,
        valid_from = EXCLUDED.valid_from,
        valid_until = EXCLUDED.valid_until,
        max_sends_per_day = EXCLUDED.max_sends_per_day,
        max_sends_per_window = EXCLUDED.max_sends_per_window,
        minimum_spacing_minutes = EXCLUDED.minimum_spacing_minutes,
        allowed_send_window = EXCLUDED.allowed_send_window,
        ramp_stage = EXCLUDED.ramp_stage,
        governor_state = EXCLUDED.governor_state,
        current_sent_count = EXCLUDED.current_sent_count,
        current_scheduled_count = EXCLUDED.current_scheduled_count,
        current_executing_count = EXCLUDED.current_executing_count,
        remaining_capacity = EXCLUDED.remaining_capacity,
        risk_flags = EXCLUDED.risk_flags,
        evidence_snapshot = EXCLUDED.evidence_snapshot,
        emmett_contribution = EXCLUDED.emmett_contribution,
        version = EXCLUDED.version,
        payload = EXCLUDED.payload,
        updated_at = NOW()`,
    [
      envelope.envelopeId,
      envelope.tenantId,
      envelope.mailboxIntegrationId,
      envelope.sendingIdentityId,
      envelope.senderEmail,
      envelope.sendingDomain,
      envelope.localDate,
      envelope.computedAt,
      envelope.validFrom,
      envelope.validUntil,
      envelope.maxSendsPerDay,
      envelope.maxSendsPerWindow,
      envelope.minimumSpacingMinutes,
      JSON.stringify(envelope.allowedSendWindow || {}),
      envelope.allowedSendWindow?.timezone || 'America/New_York',
      envelope.rampStage,
      envelope.governorState,
      envelope.currentSentCount,
      envelope.currentScheduledCount,
      envelope.currentExecutingCount,
      envelope.remainingCapacity,
      JSON.stringify(envelope.riskFlags || []),
      JSON.stringify(envelope.evidenceSnapshot || {}),
      JSON.stringify(envelope.emmettContribution || {}),
      envelope.version,
      JSON.stringify(envelope),
    ]
  );
  return envelope;
}

async function produceTenantMailboxCapacityEnvelope(tenantId, sendingIdentityId, opts = {}) {
  const pool = opts.pool || defaultPool;
  const snapshot = await buildTenantMailboxSnapshot(tenantId, sendingIdentityId, opts);
  const assessment = assessTenantMailboxCapacity(snapshot, opts);
  const envelope = buildCapacityEnvelope(snapshot, assessment, opts);
  await persistEnvelope(envelope, pool);
  return { snapshot, assessment, envelope };
}

function reservationIdForSchedule(tenantId, scheduleId) {
  return `res_${crypto.createHash('sha256').update(`${tenantKey(tenantId)}:${scheduleId}`).digest('hex').slice(0, 24)}`;
}

const CANONICAL_ACCOUNTING_SQL = `
  WITH sent AS (
    SELECT id, NULL::text AS schedule_id
      FROM tenant_outreach_messages
     WHERE tenant_id = $1 AND sending_identity_id = $2
       AND direction = 'OUTBOUND' AND status = 'sent'
       AND (sent_at AT TIME ZONE $4)::date = $3::date
  ),
  live_schedules AS (
    SELECT id, status, outbound_message_id, scheduled_for
      FROM tenant_outreach_scheduled_sends
     WHERE tenant_id = $1 AND sending_identity_id = $2
       AND status IN ('SCHEDULED', 'EXECUTING', 'SENT')
       AND (scheduled_for AT TIME ZONE $4)::date = $3::date
  ),
  live_reservations AS (
    SELECT id, schedule_id, status, scheduled_for
      FROM emmett_tenant_mailbox_capacity_reservations
     WHERE tenant_id = $1 AND sending_identity_id = $2
       AND status IN ('scheduled', 'executing', 'sent')
       AND (scheduled_for AT TIME ZONE $4)::date = $3::date
  )
  SELECT
    (SELECT COALESCE(json_agg(json_build_object('id', id)), '[]'::json) FROM sent) AS sent_messages,
    (SELECT COALESCE(json_agg(json_build_object(
        'id', id, 'status', status, 'outboundMessageId', outbound_message_id
      )), '[]'::json) FROM live_schedules) AS schedules,
    (SELECT COALESCE(json_agg(json_build_object(
        'id', id, 'scheduleId', schedule_id, 'status', status
      )), '[]'::json) FROM live_reservations) AS reservations
`;

function parseJsonArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_err) {
      return [];
    }
  }
  return [];
}

async function queryCanonicalCapacityAccounting(tenantId, sendingIdentityId, localDate, timeZone, client) {
  const res = await client.query(CANONICAL_ACCOUNTING_SQL, [
    tenantKey(tenantId),
    tenantKey(sendingIdentityId),
    localDate,
    timeZone || 'America/New_York',
  ]);
  const row = res.rows[0] || {};
  return accountCapacityUnits({
    sentMessages: parseJsonArray(row.sent_messages),
    schedules: parseJsonArray(row.schedules),
    reservations: parseJsonArray(row.reservations),
  });
}

async function loadLiveCommitments(tenantId, sendingIdentityId, client) {
  const tid = tenantKey(tenantId);
  const sid = tenantKey(sendingIdentityId);
  const [messages, schedules, reservations] = await Promise.all([
    client.query(
      `SELECT id, sent_at, 'SENT' AS status
         FROM tenant_outreach_messages
        WHERE tenant_id = $1 AND sending_identity_id = $2
          AND direction = 'OUTBOUND' AND status = 'sent'`,
      [tid, sid]
    ),
    client.query(
      `SELECT id, scheduled_for, status, outbound_message_id
         FROM tenant_outreach_scheduled_sends
        WHERE tenant_id = $1 AND sending_identity_id = $2
          AND status = ANY($3::text[])`,
      [tid, sid, LIVE_SCHEDULE_STATUSES]
    ),
    client.query(
      `SELECT id, schedule_id, scheduled_for, status
         FROM emmett_tenant_mailbox_capacity_reservations
        WHERE tenant_id = $1 AND sending_identity_id = $2
          AND status = ANY($3::text[])`,
      [tid, sid, LIVE_RESERVATION_STATUSES]
    ),
  ]);

  return [
    ...messages.rows.map((row) => ({
      id: row.id,
      messageId: row.id,
      sentAt: row.sent_at,
      status: 'SENT',
      kind: 'message',
    })),
    ...schedules.rows.map((row) => ({
      id: row.id,
      scheduleId: row.id,
      scheduledFor: row.scheduled_for,
      status: row.status,
      outboundMessageId: row.outbound_message_id,
      kind: 'schedule',
    })),
    ...reservations.rows.map((row) => ({
      id: row.id,
      reservationId: row.id,
      scheduleId: row.schedule_id,
      scheduledFor: row.scheduled_for,
      status: row.status,
      kind: 'reservation',
    })),
  ];
}

async function queryPriorSendAnchor(tenantId, sendingIdentityId, evaluatedScheduledFor, client, opts = {}) {
  const commitments = await loadLiveCommitments(tenantId, sendingIdentityId, client);
  return findPriorSendAnchor(commitments, evaluatedScheduledFor, opts);
}

async function queryScheduleSpacingConflicts(tenantId, sendingIdentityId, requestedScheduledFor, minSpacingMinutes, client, opts = {}) {
  const commitments = await loadLiveCommitments(tenantId, sendingIdentityId, client);
  return findScheduleSpacingConflicts(requestedScheduledFor, commitments, minSpacingMinutes, opts);
}

async function queryInFlightSpacingConflicts(tenantId, sendingIdentityId, requestedScheduledFor, minSpacingMinutes, client, opts = {}) {
  const commitments = await loadLiveCommitments(tenantId, sendingIdentityId, client);
  return findInFlightSpacingConflicts(requestedScheduledFor, commitments, minSpacingMinutes, opts);
}

async function scheduleAlreadyConsumesCapacity(schedule, client) {
  const tid = tenantKey(schedule.tenantId);
  const scheduleId = schedule.id;
  if (!scheduleId) return false;
  const res = await client.query(
    `SELECT
        EXISTS (
          SELECT 1 FROM tenant_outreach_scheduled_sends
           WHERE tenant_id = $1 AND id = $2
             AND status IN ('SCHEDULED', 'EXECUTING', 'SENT')
        ) OR EXISTS (
          SELECT 1 FROM emmett_tenant_mailbox_capacity_reservations
           WHERE tenant_id = $1 AND schedule_id = $2
             AND status IN ('scheduled', 'executing', 'sent')
        ) AS consumed`,
    [tid, scheduleId]
  );
  return res.rows[0]?.consumed === true;
}

async function reconcileLegacyReservations(tenantId, sendingIdentityId, envelopeId, client) {
  const tid = tenantKey(tenantId);
  const sid = tenantKey(sendingIdentityId);
  const live = await client.query(
    `SELECT id, status, scheduled_for
       FROM tenant_outreach_scheduled_sends
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND status IN ('SCHEDULED', 'EXECUTING')`,
    [tid, sid]
  );
  let created = 0;
  for (const row of live.rows) {
    const reservationId = reservationIdForSchedule(tid, row.id);
    const mapped = row.status === 'EXECUTING' ? 'executing' : 'scheduled';
    const inserted = await client.query(
      `INSERT INTO emmett_tenant_mailbox_capacity_reservations (
          id, envelope_id, tenant_id, sending_identity_id, schedule_id, status, scheduled_for, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
        ON CONFLICT (id) DO NOTHING
        RETURNING id`,
      [reservationId, envelopeId, tid, sid, row.id, mapped, row.scheduled_for]
    );
    if (inserted.rowCount) created += 1;
  }
  return { created, examined: live.rows.length };
}

async function refreshEnvelopeAccounting(tenantId, sendingIdentityId, envelopeId, client, timeZone = 'America/New_York') {
  const envRes = await client.query(
    `SELECT * FROM emmett_tenant_mailbox_capacity_envelopes WHERE id = $1 FOR UPDATE`,
    [envelopeId]
  );
  const envelope = normalizeEnvelope(envRes.rows[0]);
  if (!envelope) return null;

  await reconcileLegacyReservations(tenantId, sendingIdentityId, envelopeId, client);
  const accounting = await queryCanonicalCapacityAccounting(
    tenantId,
    sendingIdentityId,
    envelope.localDate,
    timeZone,
    client
  );
  const remaining = accounting.remainingFor(envelope.maxSendsPerDay);

  await client.query(
    `UPDATE emmett_tenant_mailbox_capacity_envelopes
        SET current_sent_count = $2,
            current_scheduled_count = $3,
            current_executing_count = $4,
            remaining_capacity = $5,
            updated_at = NOW()
      WHERE id = $1`,
    [envelopeId, accounting.sent, accounting.scheduled, accounting.executing, remaining]
  );

  return normalizeEnvelope({
    ...envRes.rows[0],
    current_sent_count: accounting.sent,
    current_scheduled_count: accounting.scheduled,
    current_executing_count: accounting.executing,
    remaining_capacity: remaining,
  });
}

async function authorizeTenantMailboxCapacity(input = {}, opts = {}) {
  const pool = opts.pool || defaultPool;
  const tenantId = tenantKey(input.tenantId);
  const sendingIdentityId = tenantKey(input.sendingIdentityId);
  const scheduledFor = input.scheduledFor;
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());

  let envelope = await loadLatestEnvelope(tenantId, sendingIdentityId, pool, now);
  if (!envelope) {
    const produced = await produceTenantMailboxCapacityEnvelope(tenantId, sendingIdentityId, { ...opts, now });
    envelope = produced.envelope;
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock($1, $2)', [
      ADVISORY_LOCK_NAMESPACE,
      advisoryLockKey(tenantId, sendingIdentityId),
    ]);

    envelope = await refreshEnvelopeAccounting(
      tenantId,
      sendingIdentityId,
      envelope.envelopeId,
      client,
      envelope.allowedSendWindow?.timezone
    );
    const scheduleConflicts = await queryScheduleSpacingConflicts(
      tenantId,
      sendingIdentityId,
      scheduledFor,
      envelope.minimumSpacingMinutes,
      client,
      { excludeScheduleId: input.excludeScheduleId || input.scheduleId || null }
    );
    const recheck = evaluateCapacityAuthorization(envelope, {
      scheduledFor,
      scheduleConflicts,
    }, { now });
    if (!recheck.allowed) {
      throw capacityError(recheck.code, recheck.reason, { envelope });
    }

    await client.query(
      `UPDATE emmett_tenant_mailbox_capacity_envelopes
          SET current_scheduled_count = current_scheduled_count + 1,
              remaining_capacity = GREATEST(0, remaining_capacity - 1),
              updated_at = NOW()
        WHERE id = $1`,
      [envelope.envelopeId]
    );

    await client.query('COMMIT');
    envelope = await loadLatestEnvelope(tenantId, sendingIdentityId, pool, now);
    return { envelope, reserved: true };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

async function reserveCapacityForSchedule(schedule, envelopeId, opts = {}) {
  const pool = opts.pool || defaultPool;
  const reservationId = reservationIdForSchedule(schedule.tenantId, schedule.id);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock($1, $2)', [
      ADVISORY_LOCK_NAMESPACE,
      advisoryLockKey(schedule.tenantId, schedule.sendingIdentityId),
    ]);

    const envelope = await refreshEnvelopeAccounting(
      schedule.tenantId,
      schedule.sendingIdentityId,
      envelopeId,
      client,
      schedule.timezone
    );
    const scheduleConflicts = await queryScheduleSpacingConflicts(
      schedule.tenantId,
      schedule.sendingIdentityId,
      schedule.scheduledFor,
      envelope.minimumSpacingMinutes,
      client,
      { excludeScheduleId: schedule.id }
    );
    const recheck = evaluateCapacityAuthorization(envelope, {
      scheduledFor: schedule.scheduledFor,
      scheduleConflicts,
      alreadyConsumesCapacity: true,
    }, { now: opts.now });
    if (!recheck.allowed) {
      throw capacityError(recheck.code, recheck.reason, { envelope });
    }

    await client.query(
      `INSERT INTO emmett_tenant_mailbox_capacity_reservations (
          id, envelope_id, tenant_id, sending_identity_id, schedule_id, status, scheduled_for, updated_at
        ) VALUES ($1,$2,$3,$4,$5,'scheduled',$6,NOW())
        ON CONFLICT (id) DO NOTHING`,
      [
        reservationId,
        envelopeId,
        tenantKey(schedule.tenantId),
        tenantKey(schedule.sendingIdentityId),
        schedule.id,
        schedule.scheduledFor,
      ]
    );

    await refreshEnvelopeAccounting(
      schedule.tenantId,
      schedule.sendingIdentityId,
      envelopeId,
      client,
      schedule.timezone
    );

    await client.query('COMMIT');
    return { reservationId, envelopeId };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

async function validateExecutionCapacity(schedule, opts = {}) {
  const pool = opts.pool || defaultPool;
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());

  let envelope = await loadLatestEnvelope(schedule.tenantId, schedule.sendingIdentityId, pool, now);
  if (!envelope) {
    const produced = await produceTenantMailboxCapacityEnvelope(
      schedule.tenantId,
      schedule.sendingIdentityId,
      { ...opts, now, pool }
    );
    envelope = produced.envelope;
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock($1, $2)', [
      ADVISORY_LOCK_NAMESPACE,
      advisoryLockKey(schedule.tenantId, schedule.sendingIdentityId),
    ]);

    envelope = await refreshEnvelopeAccounting(
      schedule.tenantId,
      schedule.sendingIdentityId,
      envelope.envelopeId,
      client,
      schedule.timezone
    );
    const alreadyConsumesCapacity = await scheduleAlreadyConsumesCapacity(schedule, client);
    const prior = await queryPriorSendAnchor(
      schedule.tenantId,
      schedule.sendingIdentityId,
      schedule.scheduledFor,
      client,
      { excludeScheduleId: schedule.id }
    );
    const inFlightConflicts = await queryInFlightSpacingConflicts(
      schedule.tenantId,
      schedule.sendingIdentityId,
      schedule.scheduledFor,
      envelope.minimumSpacingMinutes,
      client,
      { excludeScheduleId: schedule.id }
    );
    const check = evaluateCapacityExecution(envelope, {
      scheduledFor: schedule.scheduledFor,
      lastSendAt: prior?.lastSendAt || null,
      inFlightConflicts,
      alreadyConsumesCapacity,
    }, { now });

    if (!check.allowed) {
      await client.query('COMMIT');
      return {
        eligible: false,
        action: 'SKIPPED',
        reason: check.code,
        message: check.reason,
        envelope,
      };
    }

    await upsertExecutingReservation(schedule, envelope.envelopeId, client);
    await client.query('COMMIT');
    return { eligible: true, action: 'send', envelope };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

async function upsertExecutingReservation(schedule, envelopeId, client) {
  const reservationId = reservationIdForSchedule(schedule.tenantId, schedule.id);
  await client.query(
    `INSERT INTO emmett_tenant_mailbox_capacity_reservations (
        id, envelope_id, tenant_id, sending_identity_id, schedule_id, status, scheduled_for, updated_at
      ) VALUES ($1,$2,$3,$4,$5,'executing',$6,NOW())
      ON CONFLICT (id) DO UPDATE
        SET status = 'executing',
            envelope_id = EXCLUDED.envelope_id,
            scheduled_for = EXCLUDED.scheduled_for,
            updated_at = NOW()`,
    [
      reservationId,
      envelopeId,
      tenantKey(schedule.tenantId),
      tenantKey(schedule.sendingIdentityId),
      schedule.id,
      schedule.scheduledFor,
    ]
  );
}

async function markReservationExecuting(schedule, opts = {}) {
  const pool = opts.pool || defaultPool;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock($1, $2)', [
      ADVISORY_LOCK_NAMESPACE,
      advisoryLockKey(schedule.tenantId, schedule.sendingIdentityId),
    ]);
    let envelopeId = opts.envelopeId;
    if (!envelopeId) {
      const existing = await client.query(
        `SELECT envelope_id FROM emmett_tenant_mailbox_capacity_reservations
          WHERE tenant_id = $1 AND schedule_id = $2
          LIMIT 1`,
        [tenantKey(schedule.tenantId), schedule.id]
      );
      envelopeId = existing.rows[0]?.envelope_id;
    }
    if (!envelopeId) {
      const latest = await loadLatestEnvelope(schedule.tenantId, schedule.sendingIdentityId, pool, opts.now || new Date());
      envelopeId = latest?.envelopeId;
    }
    if (envelopeId) {
      await upsertExecutingReservation(schedule, envelopeId, client);
    } else {
      await client.query(
        `UPDATE emmett_tenant_mailbox_capacity_reservations
            SET status = 'executing', updated_at = NOW()
          WHERE tenant_id = $1 AND schedule_id = $2 AND status = 'scheduled'`,
        [tenantKey(schedule.tenantId), schedule.id]
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

async function finalizeReservation(schedule, status, opts = {}) {
  const pool = opts.pool || defaultPool;
  const mapped = status === 'SENT' ? 'sent' : status === 'SKIPPED' ? 'skipped' : status === 'FAILED' ? 'released' : 'released';
  const res = await pool.query(
    `UPDATE emmett_tenant_mailbox_capacity_reservations
        SET status = $3, updated_at = NOW()
      WHERE tenant_id = $1 AND schedule_id = $2
      RETURNING envelope_id`,
    [tenantKey(schedule.tenantId), schedule.id, mapped]
  );
  const envelopeId = res.rows[0]?.envelope_id;
  if (envelopeId) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await refreshEnvelopeAccounting(
        schedule.tenantId,
        schedule.sendingIdentityId,
        envelopeId,
        client,
        schedule.timezone
      );
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
    } finally {
      client.release();
    }
  }
}

async function ingestTenantMailboxOutcome(event = {}, opts = {}) {
  const pool = opts.pool || defaultPool;
  const tenantId = tenantKey(event.tenantId);
  const sendingIdentityId = tenantKey(event.sendingIdentityId);
  if (!tenantId || !sendingIdentityId) return null;
  return produceTenantMailboxCapacityEnvelope(tenantId, sendingIdentityId, { ...opts, pool });
}

function createPermissiveEnvelope(tenantId, sendingIdentityId, opts = {}) {
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());
  const validUntil = new Date(now.getTime() + 6 * 3600000);
  return {
    envelopeId: `env_permissive_${tenantKey(tenantId)}_${tenantKey(sendingIdentityId)}`,
    tenantId: tenantKey(tenantId),
    mailboxIntegrationId: opts.mailboxIntegrationId || 'tmi_test',
    sendingIdentityId: tenantKey(sendingIdentityId),
    senderEmail: opts.senderEmail || 'hello@example.com',
    sendingDomain: opts.sendingDomain || 'example.com',
    localDate: opts.localDate || now.toISOString().slice(0, 10),
    computedAt: now.toISOString(),
    validFrom: now.toISOString(),
    validUntil: validUntil.toISOString(),
    maxSendsPerDay: 100,
    maxSendsPerWindow: 100,
    minimumSpacingMinutes: 0,
    allowedSendWindow: { startHour: 0, endHour: 24, timezone: 'UTC' },
    rampStage: 'test',
    currentSentCount: 0,
    currentScheduledCount: 0,
    currentExecutingCount: 0,
    remainingCapacity: 100,
    governorState: GOVERNOR_OUTCOMES.PROCEED,
    riskFlags: [],
    evidenceSnapshot: {},
    emmettContribution: {},
    version: 1,
  };
}

function createMemoryCapacityGate(seed = {}) {
  const envelopes = new Map();
  const reservations = new Map();
  const commitments = [...(seed.commitments || [])];
  const sentMessages = [...(seed.sentMessages || [])];
  let lock = Promise.resolve();

  for (const envelope of seed.envelopes || []) {
    envelopes.set(`${envelope.tenantId}:${envelope.sendingIdentityId}`, envelope);
  }

  function identityKey(tenantId, sendingIdentityId) {
    return `${tenantKey(tenantId)}:${tenantKey(sendingIdentityId)}`;
  }

  function withLock(fn) {
    const run = lock.then(fn, fn);
    lock = run.then(() => undefined, () => undefined);
    return run;
  }

  function scopedCommitments(tenantId, sendingIdentityId) {
    const tid = tenantKey(tenantId);
    const sid = tenantKey(sendingIdentityId);
    return [
      ...sentMessages.filter((row) => (!row.tenantId || tenantKey(row.tenantId) === tid)
        && (!row.sendingIdentityId || tenantKey(row.sendingIdentityId) === sid)),
      ...commitments.filter((row) => (!row.tenantId || tenantKey(row.tenantId) === tid)
        && (!row.sendingIdentityId || tenantKey(row.sendingIdentityId) === sid)),
      ...[...reservations.values()].filter((row) => tenantKey(row.tenantId) === tid
        && tenantKey(row.sendingIdentityId) === sid),
    ];
  }

  function refreshEnvelopeFromLedger(envelope) {
    const accounting = accountCapacityUnits({
      sentMessages: sentMessages.filter((row) => tenantKey(row.tenantId || envelope.tenantId) === envelope.tenantId
        && tenantKey(row.sendingIdentityId || envelope.sendingIdentityId) === envelope.sendingIdentityId),
      schedules: commitments.filter((row) => tenantKey(row.tenantId || envelope.tenantId) === envelope.tenantId
        && tenantKey(row.sendingIdentityId || envelope.sendingIdentityId) === envelope.sendingIdentityId),
      reservations: [...reservations.values()].filter((row) => tenantKey(row.tenantId) === envelope.tenantId
        && tenantKey(row.sendingIdentityId) === envelope.sendingIdentityId),
    });
    envelope.currentSentCount = accounting.sent;
    envelope.currentScheduledCount = accounting.scheduled;
    envelope.currentExecutingCount = accounting.executing;
    envelope.remainingCapacity = accounting.remainingFor(envelope.maxSendsPerDay);
    return envelope;
  }

  return {
    async authorize(input = {}, opts = {}) {
      return withLock(async () => {
        const key = identityKey(input.tenantId, input.sendingIdentityId);
        let envelope = envelopes.get(key);
        if (!envelope) envelope = createPermissiveEnvelope(input.tenantId, input.sendingIdentityId, opts);
        refreshEnvelopeFromLedger(envelope);
        const scheduleConflicts = findScheduleSpacingConflicts(
          input.scheduledFor,
          scopedCommitments(input.tenantId, input.sendingIdentityId),
          envelope.minimumSpacingMinutes
        );
        const authCheck = evaluateCapacityAuthorization(envelope, {
          scheduledFor: input.scheduledFor,
          scheduleConflicts: scheduleConflicts.length ? scheduleConflicts : undefined,
          lastSendAt: seed.lastSendAt || null,
        }, opts);
        if (!authCheck.allowed) {
          throw capacityError(authCheck.code, authCheck.reason, { envelope });
        }
        envelope.currentScheduledCount += 1;
        envelope.remainingCapacity = Math.max(0, envelope.remainingCapacity - 1);
        commitments.push({
          id: input.scheduleId || `pending_${commitments.length + 1}`,
          scheduleId: input.scheduleId || null,
          tenantId: tenantKey(input.tenantId),
          sendingIdentityId: tenantKey(input.sendingIdentityId),
          scheduledFor: input.scheduledFor,
          status: 'SCHEDULED',
          pending: true,
        });
        envelopes.set(key, envelope);
        return { envelope };
      });
    },
    async reserve(schedule, envelopeId) {
      return withLock(async () => {
        const key = identityKey(schedule.tenantId, schedule.sendingIdentityId);
        reservations.set(schedule.id, {
          id: reservationIdForSchedule(schedule.tenantId, schedule.id),
          envelopeId,
          tenantId: tenantKey(schedule.tenantId),
          sendingIdentityId: tenantKey(schedule.sendingIdentityId),
          scheduleId: schedule.id,
          status: 'scheduled',
          scheduledFor: schedule.scheduledFor,
        });
        const pending = commitments.find((row) => row.pending && tenantKey(row.tenantId) === tenantKey(schedule.tenantId)
          && tenantKey(row.sendingIdentityId) === tenantKey(schedule.sendingIdentityId)
          && !row.scheduleId);
        if (pending) {
          pending.id = schedule.id;
          pending.scheduleId = schedule.id;
          pending.scheduledFor = schedule.scheduledFor;
          pending.pending = false;
        } else if (!commitments.some((row) => row.scheduleId === schedule.id || row.id === schedule.id)) {
          commitments.push({
            id: schedule.id,
            scheduleId: schedule.id,
            tenantId: tenantKey(schedule.tenantId),
            sendingIdentityId: tenantKey(schedule.sendingIdentityId),
            scheduledFor: schedule.scheduledFor,
            status: 'SCHEDULED',
          });
        }
        const envelope = envelopes.get(key);
        if (envelope) refreshEnvelopeFromLedger(envelope);
        return { reservationId: reservationIdForSchedule(schedule.tenantId, schedule.id), envelopeId };
      });
    },
    async validateExecution(schedule, opts = {}) {
      return withLock(async () => {
        const key = identityKey(schedule.tenantId, schedule.sendingIdentityId);
        const envelope = envelopes.get(key) || createPermissiveEnvelope(schedule.tenantId, schedule.sendingIdentityId, opts);
        refreshEnvelopeFromLedger(envelope);
        const scoped = scopedCommitments(schedule.tenantId, schedule.sendingIdentityId);
        const prior = findPriorSendAnchor(scoped, schedule.scheduledFor, { excludeScheduleId: schedule.id });
        const inFlightConflicts = findInFlightSpacingConflicts(
          schedule.scheduledFor,
          scoped,
          envelope.minimumSpacingMinutes,
          { excludeScheduleId: schedule.id }
        );
        const alreadyConsumesCapacity = scoped.some((row) => (
          row.scheduleId === schedule.id || row.id === schedule.id
        ) && (LIVE_SCHEDULE_STATUSES.includes(String(row.status || '').toUpperCase())
          || LIVE_RESERVATION_STATUSES.includes(String(row.status || '').toLowerCase())));
        const check = evaluateCapacityExecution(envelope, {
          scheduledFor: schedule.scheduledFor,
          lastSendAt: prior?.lastSendAt || seed.lastSendAt || null,
          inFlightConflicts,
          alreadyConsumesCapacity,
        }, opts);
        if (!check.allowed) {
          return { eligible: false, action: 'SKIPPED', reason: check.code, message: check.reason, envelope };
        }
        const reservation = reservations.get(schedule.id);
        if (reservation) reservation.status = 'executing';
        const commitment = commitments.find((row) => row.scheduleId === schedule.id || row.id === schedule.id);
        if (commitment) commitment.status = 'EXECUTING';
        refreshEnvelopeFromLedger(envelope);
        envelopes.set(key, envelope);
        return { eligible: true, action: 'send', envelope };
      });
    },
    async markExecuting(schedule) {
      return withLock(async () => {
        const reservation = reservations.get(schedule.id);
        if (reservation) reservation.status = 'executing';
        const commitment = commitments.find((row) => row.scheduleId === schedule.id || row.id === schedule.id);
        if (commitment) commitment.status = 'EXECUTING';
      });
    },
    async finalize(schedule, status) {
      return withLock(async () => {
        const mapped = status === 'SENT' ? 'sent' : status === 'SKIPPED' ? 'skipped' : 'released';
        const reservation = reservations.get(schedule.id);
        if (reservation) reservation.status = mapped;
        const commitment = commitments.find((row) => row.scheduleId === schedule.id || row.id === schedule.id);
        if (commitment) {
          commitment.status = status;
          if (status === 'SENT') {
            commitment.sentAt = schedule.sentAt || schedule.scheduledFor;
            sentMessages.push({
              id: schedule.outboundMessageId || `msg_${schedule.id}`,
              tenantId: tenantKey(schedule.tenantId),
              sendingIdentityId: tenantKey(schedule.sendingIdentityId),
              sentAt: commitment.sentAt,
              scheduleId: schedule.id,
            });
          }
        }
        const key = identityKey(schedule.tenantId, schedule.sendingIdentityId);
        const envelope = envelopes.get(key);
        if (envelope) refreshEnvelopeFromLedger(envelope);
      });
    },
    async ingestOutcome() {},
    setEnvelope(tenantId, sendingIdentityId, envelope) {
      envelopes.set(identityKey(tenantId, sendingIdentityId), envelope);
    },
    getEnvelope(tenantId, sendingIdentityId) {
      return envelopes.get(identityKey(tenantId, sendingIdentityId)) || null;
    },
    getReservations() {
      return [...reservations.values()];
    },
    getCommitments() {
      return commitments;
    },
  };
}

function createPostgresCapacityGate(opts = {}) {
  const pool = opts.pool || defaultPool;
  return {
    authorize: (input, gateOpts) => authorizeTenantMailboxCapacity(input, { ...opts, ...gateOpts, pool }),
    reserve: (schedule, envelopeId, gateOpts) => reserveCapacityForSchedule(schedule, envelopeId, { ...opts, ...gateOpts, pool }),
    validateExecution: (schedule, gateOpts) => validateExecutionCapacity(schedule, { ...opts, ...gateOpts, pool }),
    markExecuting: (schedule, gateOpts) => markReservationExecuting(schedule, { ...opts, ...gateOpts, pool }),
    finalize: (schedule, status, gateOpts) => finalizeReservation(schedule, status, { ...opts, ...gateOpts, pool }),
    ingestOutcome: (event, gateOpts) => ingestTenantMailboxOutcome(event, { ...opts, ...gateOpts, pool }),
  };
}

function envEnabled(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase());
}

function isMemoryTestContext(opts = {}) {
  if (opts.pool) return false;
  const storeName = opts.scheduleStore?.constructor?.name || '';
  const mailboxName = opts.mailboxStore?.constructor?.name || '';
  return storeName === 'MemoryScheduleStore' || mailboxName === 'MemoryTenantMailboxStore';
}

function resolveCapacityGate(opts = {}) {
  if (opts.emmettCapacity) return opts.emmettCapacity;
  if (opts.emmettCapacityEnabled === false || !envEnabled(process.env.TENANT_EMMETT_CAPACITY_ENABLED ?? 'true')) {
    return createMemoryCapacityGate();
  }
  if (opts.capacityGate) return opts.capacityGate;
  if (isMemoryTestContext(opts)) return createMemoryCapacityGate(opts.memoryCapacitySeed);
  return createPostgresCapacityGate(opts);
}

async function reconcileTenantMailboxCapacityReservations(tenantId, sendingIdentityId, opts = {}) {
  const pool = opts.pool || defaultPool;
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());
  let envelope = opts.envelope || await loadLatestEnvelope(tenantId, sendingIdentityId, pool, now);
  if (!envelope) {
    const produced = await produceTenantMailboxCapacityEnvelope(tenantId, sendingIdentityId, { ...opts, now, pool });
    envelope = produced.envelope;
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock($1, $2)', [
      ADVISORY_LOCK_NAMESPACE,
      advisoryLockKey(tenantId, sendingIdentityId),
    ]);
    const result = await reconcileLegacyReservations(tenantId, sendingIdentityId, envelope.envelopeId, client);
    const accounting = await refreshEnvelopeAccounting(
      tenantId,
      sendingIdentityId,
      envelope.envelopeId,
      client,
      envelope.allowedSendWindow?.timezone
    );
    await client.query('COMMIT');
    return { ...result, envelope: accounting, idempotent: true };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  ensureCapacitySchema,
  buildTenantMailboxSnapshot,
  produceTenantMailboxCapacityEnvelope,
  loadLatestEnvelope,
  authorizeTenantMailboxCapacity,
  reserveCapacityForSchedule,
  validateExecutionCapacity,
  markReservationExecuting,
  finalizeReservation,
  ingestTenantMailboxOutcome,
  reconcileTenantMailboxCapacityReservations,
  reconcileLegacyReservations,
  queryCanonicalCapacityAccounting,
  queryPriorSendAnchor,
  queryScheduleSpacingConflicts,
  queryInFlightSpacingConflicts,
  reservationIdForSchedule,
  evaluateCapacityAuthorization,
  evaluateCapacityExecution,
  createPermissiveEnvelope,
  createMemoryCapacityGate,
  createPostgresCapacityGate,
  resolveCapacityGate,
  GOVERNOR_OUTCOMES,
  normalizeEnvelope,
  ADVISORY_LOCK_NAMESPACE,
};
