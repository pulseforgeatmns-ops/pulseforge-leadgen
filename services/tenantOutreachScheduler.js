'use strict';

/**
 * SPEC-252 — Durable tenant outreach scheduling and execution.
 *
 * Operator authorization becomes durable system state. An executor claims due
 * SCHEDULED rows atomically, re-validates eligibility, and dispatches via
 * sendTenantEmail().
 */

const crypto = require('crypto');
const defaultPool = require('../db');
const {
  sendTenantEmail,
  PostgresTenantMailboxStore,
  MemoryTenantMailboxStore,
  SEQUENCE_STATE,
  THREAD_STATUS,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
} = require('./tenantMailbox');

const SCHEDULE_STATUS = Object.freeze({
  SCHEDULED: 'SCHEDULED',
  EXECUTING: 'EXECUTING',
  SENT: 'SENT',
  PAUSED: 'PAUSED',
  CANCELLED: 'CANCELLED',
  FAILED: 'FAILED',
  SKIPPED: 'SKIPPED',
});

const PAST_DUE_POLICY = Object.freeze({
  EXECUTE_WITHIN_WINDOW: 'execute_within_window',
  SKIP_PAST_DUE: 'skip_past_due',
  EXECUTE_ANYTIME: 'execute_anytime',
});

const EXECUTOR_LOCK_NAMESPACE = 701252;
const DEFAULT_MAX_LATENESS_MINUTES = 30;
const DEFAULT_TIMEZONE = 'America/New_York';

let schemaPromise;

function clean(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function tenantKey(value) {
  if (value == null || value === '') return '';
  return String(value);
}

function lower(value) {
  return clean(value).toLowerCase();
}

function nowIso(opts = {}) {
  if (typeof opts.now === 'function') return new Date(opts.now()).toISOString();
  return opts.now ? new Date(opts.now).toISOString() : new Date().toISOString();
}

function stableHash(value) {
  return crypto.createHash('sha256').update(String(value || '')).digest('hex');
}

function prefixedId(prefix, value) {
  return `${prefix}_${stableHash(value || `${Date.now()}_${Math.random()}`).slice(0, 24)}`;
}

function asJson(value, fallback) {
  if (value == null) return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_err) {
    return fallback;
  }
}

function schedulerError(code, message, extras = {}) {
  const err = new Error(message || code);
  err.code = code;
  Object.assign(err, extras);
  return err;
}

function envEnabled(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase());
}

function normalizeSchedule(row = null) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: tenantKey(row.tenantId ?? row.tenant_id),
    prospectId: row.prospectId || row.prospect_id,
    acquisitionKnowledgeObjectId: row.acquisitionKnowledgeObjectId || row.acquisition_knowledge_object_id || null,
    outreachAssetId: row.outreachAssetId || row.outreach_asset_id,
    outreachAssetVersion: row.outreachAssetVersion || row.outreach_asset_version || null,
    sendingIdentityId: row.sendingIdentityId || row.sending_identity_id,
    recipientEmail: row.recipientEmail || row.recipient_email,
    missionId: row.missionId || row.mission_id || null,
    threadId: row.threadId || row.thread_id || null,
    sequenceStep: Number(row.sequenceStep ?? row.sequence_step ?? 1),
    scheduledFor: row.scheduledFor || row.scheduled_for,
    timezone: row.timezone || DEFAULT_TIMEZONE,
    status: row.status || SCHEDULE_STATUS.SCHEDULED,
    authorizationSource: row.authorizationSource || row.authorization_source,
    authorizedBy: row.authorizedBy || row.authorized_by,
    authorizedAt: row.authorizedAt || row.authorized_at,
    authorizationSnapshot: asJson(row.authorizationSnapshot || row.authorization_snapshot, {}),
    pastDuePolicy: row.pastDuePolicy || row.past_due_policy || PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
    maxLatenessMinutes: Number(row.maxLatenessMinutes ?? row.max_lateness_minutes ?? DEFAULT_MAX_LATENESS_MINUTES),
    idempotencyKey: row.idempotencyKey || row.idempotency_key,
    claimToken: row.claimToken || row.claim_token || null,
    claimedAt: row.claimedAt || row.claimed_at || null,
    outboundMessageId: row.outboundMessageId || row.outbound_message_id || null,
    failureCode: row.failureCode || row.failure_code || null,
    failureMessage: row.failureMessage || row.failure_message || null,
    skipReason: row.skipReason || row.skip_reason || null,
    createdAt: row.createdAt || row.created_at || null,
    updatedAt: row.updatedAt || row.updated_at || null,
    executedAt: row.executedAt || row.executed_at || null,
    cancelledAt: row.cancelledAt || row.cancelled_at || null,
  };
}

function publicSchedule(row) {
  const schedule = normalizeSchedule(row);
  if (!schedule) return null;
  return schedule;
}

function buildIdempotencyKey(input = {}) {
  return clean(input.idempotencyKey)
    || stableHash([
      input.tenantId,
      input.prospectId,
      input.outreachAssetId,
      input.outreachAssetVersion,
      input.sendingIdentityId,
      lower(input.recipientEmail),
      input.sequenceStep,
      input.scheduledFor,
    ].join('|'));
}

function buildAuthorizationSnapshot(input = {}) {
  return {
    recipientEmail: lower(input.recipientEmail),
    outreachAssetId: input.outreachAssetId,
    outreachAssetVersion: input.outreachAssetVersion || null,
    sendingIdentityId: input.sendingIdentityId,
    scheduledFor: input.scheduledFor,
    timezone: input.timezone || DEFAULT_TIMEZONE,
    sequenceStep: Number(input.sequenceStep || 1),
    subject: clean(input.subject),
    body: input.body || '',
    missionId: input.missionId || null,
    threadId: input.threadId || null,
    authorizedAt: input.authorizedAt || nowIso(input),
  };
}

function latenessMinutes(scheduledFor, now) {
  const scheduledMs = new Date(scheduledFor).getTime();
  const nowMs = new Date(now).getTime();
  return Math.max(0, Math.floor((nowMs - scheduledMs) / 60000));
}

function isPastDueWindowExceeded(schedule, now) {
  const policy = schedule.pastDuePolicy || PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW;
  if (policy === PAST_DUE_POLICY.EXECUTE_ANYTIME) return false;
  if (policy === PAST_DUE_POLICY.SKIP_PAST_DUE) {
    return new Date(now).getTime() > new Date(schedule.scheduledFor).getTime();
  }
  const lateness = latenessMinutes(schedule.scheduledFor, now);
  return lateness > Number(schedule.maxLatenessMinutes ?? DEFAULT_MAX_LATENESS_MINUTES);
}

function isFutureSend(schedule, now) {
  return new Date(schedule.scheduledFor).getTime() > new Date(now).getTime();
}

async function applySchedulerSchema(query) {
  await query(`
    CREATE TABLE IF NOT EXISTS tenant_outreach_scheduled_sends (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      prospect_id TEXT NOT NULL,
      acquisition_knowledge_object_id TEXT,
      outreach_asset_id TEXT NOT NULL,
      outreach_asset_version TEXT,
      sending_identity_id TEXT NOT NULL,
      recipient_email TEXT NOT NULL,
      mission_id TEXT,
      thread_id TEXT,
      sequence_step INTEGER NOT NULL DEFAULT 1,
      scheduled_for TIMESTAMPTZ NOT NULL,
      timezone TEXT NOT NULL DEFAULT 'America/New_York',
      status TEXT NOT NULL DEFAULT 'SCHEDULED',
      authorization_source TEXT NOT NULL,
      authorized_by TEXT NOT NULL,
      authorized_at TIMESTAMPTZ NOT NULL,
      authorization_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
      past_due_policy TEXT NOT NULL DEFAULT 'execute_within_window',
      max_lateness_minutes INTEGER NOT NULL DEFAULT 30,
      idempotency_key TEXT NOT NULL,
      claim_token TEXT,
      claimed_at TIMESTAMPTZ,
      outbound_message_id TEXT,
      failure_code TEXT,
      failure_message TEXT,
      skip_reason TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      executed_at TIMESTAMPTZ,
      cancelled_at TIMESTAMPTZ
    )
  `);
  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS tenant_outreach_scheduled_sends_idempotency_idx
      ON tenant_outreach_scheduled_sends (tenant_id, idempotency_key)
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS tenant_outreach_scheduled_sends_due_idx
      ON tenant_outreach_scheduled_sends (status, scheduled_for)
      WHERE status IN ('SCHEDULED', 'EXECUTING')
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS tenant_outreach_scheduled_sends_tenant_idx
      ON tenant_outreach_scheduled_sends (tenant_id, status, scheduled_for DESC)
  `);
}

function ensureSchedulerSchema(query) {
  if (query) return applySchedulerSchema(query);
  if (!schemaPromise) {
    schemaPromise = applySchedulerSchema(defaultPool.query.bind(defaultPool)).catch((err) => {
      schemaPromise = null;
      throw err;
    });
  }
  return schemaPromise;
}

class MemoryScheduleStore {
  constructor(seed = {}) {
    this.schedules = new Map((seed.schedules || []).map((row) => [row.id, normalizeSchedule(row)]));
    this.tenants = new Map((seed.tenants || []).map((row) => [tenantKey(row.tenantId ?? row.tenant_id), row]));
    this.prospects = new Map((seed.prospects || []).map((row) => [`${tenantKey(row.tenantId)}:${row.prospectId}`, row]));
    this.outreachAssets = new Map((seed.outreachAssets || []).map((row) => [`${tenantKey(row.tenantId)}:${row.id}`, row]));
  }

  async ensureSchema() {}

  async findByIdempotencyKey(tenantId, key) {
    return [...this.schedules.values()].find((row) => (
      row.tenantId === tenantKey(tenantId) && row.idempotencyKey === key
    )) || null;
  }

  async getSchedule(tenantId, scheduleId) {
    const row = this.schedules.get(scheduleId);
    return row && row.tenantId === tenantKey(tenantId) ? row : null;
  }

  async saveSchedule(input) {
    const row = normalizeSchedule({
      ...input,
      id: input.id || prefixedId('tosched', `${input.tenantId}:${input.idempotencyKey}`),
      createdAt: input.createdAt || nowIso(),
      updatedAt: nowIso(),
    });
    this.schedules.set(row.id, row);
    return row;
  }

  async listSchedules(tenantId, filters = {}) {
    let rows = [...this.schedules.values()].filter((row) => row.tenantId === tenantKey(tenantId));
    if (filters.status) rows = rows.filter((row) => row.status === filters.status);
    if (filters.prospectId) rows = rows.filter((row) => row.prospectId === filters.prospectId);
    rows.sort((a, b) => new Date(b.scheduledFor) - new Date(a.scheduledFor));
    if (filters.limit) rows = rows.slice(0, Number(filters.limit));
    return rows;
  }

  async claimDueSchedules({ now, limit = 20, claimToken }) {
    const due = [...this.schedules.values()]
      .filter((row) => row.status === SCHEDULE_STATUS.SCHEDULED && new Date(row.scheduledFor) <= new Date(now))
      .sort((a, b) => new Date(a.scheduledFor) - new Date(b.scheduledFor))
      .slice(0, limit);
    const claimed = [];
    for (const row of due) {
      if (row.status !== SCHEDULE_STATUS.SCHEDULED) continue;
      const updated = normalizeSchedule({
        ...row,
        status: SCHEDULE_STATUS.EXECUTING,
        claimToken,
        claimedAt: nowIso({ now }),
        updatedAt: nowIso({ now }),
      });
      this.schedules.set(updated.id, updated);
      claimed.push(updated);
    }
    return claimed;
  }

  async updateSchedule(tenantId, scheduleId, patch) {
    const existing = await this.getSchedule(tenantId, scheduleId);
    if (!existing) return null;
    const updated = normalizeSchedule({ ...existing, ...patch, updatedAt: nowIso() });
    this.schedules.set(updated.id, updated);
    return updated;
  }

  async getTenantActive(tenantId) {
    const row = this.tenants.get(tenantKey(tenantId));
    if (row == null) return true;
    return row.active !== false;
  }

  async getProspectEligibility(tenantId, prospectId) {
    const row = this.prospects.get(`${tenantKey(tenantId)}:${prospectId}`);
    if (!row) {
      return { exists: false, email: null, doNotContact: false, booked: false, active: false };
    }
    return {
      exists: true,
      email: lower(row.email),
      doNotContact: Boolean(row.doNotContact),
      booked: Boolean(row.booked),
      active: row.active !== false,
    };
  }

  async getOutreachAsset(tenantId, assetId) {
    return this.outreachAssets.get(`${tenantKey(tenantId)}:${assetId}`) || null;
  }

  async findPriorSequenceStep(tenantId, prospectId, sequenceStep) {
    if (sequenceStep <= 1) return { satisfied: true, schedule: null };
    const prior = [...this.schedules.values()].find((row) => (
      row.tenantId === tenantKey(tenantId)
      && row.prospectId === prospectId
      && row.sequenceStep === sequenceStep - 1
    ));
    if (!prior) return { satisfied: false, schedule: null, reason: 'prior_step_not_scheduled' };
    if (prior.status !== SCHEDULE_STATUS.SENT) {
      return { satisfied: false, schedule: prior, reason: 'prior_step_not_sent' };
    }
    return { satisfied: true, schedule: prior };
  }

  async findSentScheduleByIdempotency(tenantId, key) {
    return [...this.schedules.values()].find((row) => (
      row.tenantId === tenantKey(tenantId)
      && row.idempotencyKey === key
      && row.status === SCHEDULE_STATUS.SENT
    )) || null;
  }
}

class PostgresScheduleStore {
  constructor(pool = defaultPool) {
    this.pool = pool;
  }

  async ensureSchema() {
    await ensureSchedulerSchema(this.pool.query.bind(this.pool));
  }

  async findByIdempotencyKey(tenantId, key) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_outreach_scheduled_sends WHERE tenant_id = $1 AND idempotency_key = $2 LIMIT 1`,
      [tenantKey(tenantId), key]
    );
    return normalizeSchedule(res.rows[0]);
  }

  async getSchedule(tenantId, scheduleId) {
    await this.ensureSchema();
    const res = await this.pool.query(
      `SELECT * FROM tenant_outreach_scheduled_sends WHERE tenant_id = $1 AND id = $2 LIMIT 1`,
      [tenantKey(tenantId), scheduleId]
    );
    return normalizeSchedule(res.rows[0]);
  }

  async saveSchedule(input) {
    await this.ensureSchema();
    const row = normalizeSchedule({
      ...input,
      id: input.id || prefixedId('tosched', `${input.tenantId}:${input.idempotencyKey}`),
    });
    const res = await this.pool.query(
      `INSERT INTO tenant_outreach_scheduled_sends (
         id, tenant_id, prospect_id, acquisition_knowledge_object_id, outreach_asset_id,
         outreach_asset_version, sending_identity_id, recipient_email, mission_id, thread_id,
         sequence_step, scheduled_for, timezone, status, authorization_source, authorized_by,
         authorized_at, authorization_snapshot, past_due_policy, max_lateness_minutes,
         idempotency_key, created_at, updated_at
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,NOW(),NOW()
       )
       ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
       RETURNING *`,
      [
        row.id, row.tenantId, row.prospectId, row.acquisitionKnowledgeObjectId, row.outreachAssetId,
        row.outreachAssetVersion, row.sendingIdentityId, lower(row.recipientEmail), row.missionId,
        row.threadId, row.sequenceStep, row.scheduledFor, row.timezone, row.status,
        row.authorizationSource, row.authorizedBy, row.authorizedAt,
        JSON.stringify(row.authorizationSnapshot || {}), row.pastDuePolicy, row.maxLatenessMinutes,
        row.idempotencyKey,
      ]
    );
    if (res.rows[0]) return normalizeSchedule(res.rows[0]);
    return this.findByIdempotencyKey(row.tenantId, row.idempotencyKey);
  }

  async listSchedules(tenantId, filters = {}) {
    await this.ensureSchema();
    const params = [tenantKey(tenantId)];
    let sql = `SELECT * FROM tenant_outreach_scheduled_sends WHERE tenant_id = $1`;
    if (filters.status) {
      params.push(filters.status);
      sql += ` AND status = $${params.length}`;
    }
    if (filters.prospectId) {
      params.push(filters.prospectId);
      sql += ` AND prospect_id = $${params.length}`;
    }
    sql += ` ORDER BY scheduled_for DESC`;
    if (filters.limit) {
      params.push(Number(filters.limit));
      sql += ` LIMIT $${params.length}`;
    }
    const res = await this.pool.query(sql, params);
    return res.rows.map(normalizeSchedule);
  }

  async claimDueSchedules({ now, limit = 20, claimToken }) {
    await this.ensureSchema();
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const due = await client.query(
        `SELECT id FROM tenant_outreach_scheduled_sends
         WHERE status = 'SCHEDULED'
           AND scheduled_for <= $1::timestamptz
         ORDER BY scheduled_for
         LIMIT $2
         FOR UPDATE SKIP LOCKED`,
        [nowIso({ now }), limit]
      );
      const claimed = [];
      for (const row of due.rows) {
        const updated = await client.query(
          `UPDATE tenant_outreach_scheduled_sends
           SET status = 'EXECUTING', claim_token = $2, claimed_at = NOW(), updated_at = NOW()
           WHERE id = $1 AND status = 'SCHEDULED'
           RETURNING *`,
          [row.id, claimToken]
        );
        if (updated.rows[0]) claimed.push(normalizeSchedule(updated.rows[0]));
      }
      await client.query('COMMIT');
      return claimed;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  async updateSchedule(tenantId, scheduleId, patch) {
    await this.ensureSchema();
    const fields = [];
    const params = [tenantKey(tenantId), scheduleId];
    const map = {
      status: 'status',
      threadId: 'thread_id',
      outboundMessageId: 'outbound_message_id',
      failureCode: 'failure_code',
      failureMessage: 'failure_message',
      skipReason: 'skip_reason',
      executedAt: 'executed_at',
      cancelledAt: 'cancelled_at',
      claimToken: 'claim_token',
      claimedAt: 'claimed_at',
    };
    for (const [key, column] of Object.entries(map)) {
      if (patch[key] !== undefined) {
        params.push(patch[key]);
        fields.push(`${column} = $${params.length}`);
      }
    }
    if (!fields.length) return this.getSchedule(tenantId, scheduleId);
    fields.push('updated_at = NOW()');
    const res = await this.pool.query(
      `UPDATE tenant_outreach_scheduled_sends
       SET ${fields.join(', ')}
       WHERE tenant_id = $1 AND id = $2
       RETURNING *`,
      params
    );
    return normalizeSchedule(res.rows[0]);
  }

  async getTenantActive(tenantId) {
    const clientId = Number(tenantId);
    if (!Number.isInteger(clientId) || clientId <= 0) return true;
    const res = await this.pool.query(`SELECT active FROM clients WHERE id = $1 LIMIT 1`, [clientId]);
    if (!res.rows[0]) return true;
    return res.rows[0].active === true;
  }

  async getProspectEligibility(tenantId, prospectId) {
    const res = await this.pool.query(
      `SELECT p.email, p.do_not_contact, p.setter_status, p.closer_status
       FROM acquisition_prospect_projections app
       JOIN prospects p ON p.client_id = app.client_id AND p.id = app.prospect_id
       WHERE app.tenant_id = $1 AND app.prospect_id::text = $2
       LIMIT 1`,
      [tenantKey(tenantId), String(prospectId)]
    );
    if (!res.rows[0]) {
      return { exists: false, email: null, doNotContact: false, booked: false, active: false };
    }
    const row = res.rows[0];
    return {
      exists: true,
      email: lower(row.email),
      doNotContact: row.do_not_contact === true,
      booked: row.setter_status === 'booked' || Boolean(row.closer_status),
      active: true,
    };
  }

  async getOutreachAsset(tenantId, assetId) {
    const res = await this.pool.query(
      `SELECT id, tenant_id, object_type, lifecycle_state, content, updated_at
       FROM acquisition_knowledge_objects
       WHERE tenant_id = $1 AND id = $2 AND object_type = 'outreach_asset'
       LIMIT 1`,
      [tenantKey(tenantId), assetId]
    );
    if (!res.rows[0]) return null;
    const row = res.rows[0];
    return {
      id: row.id,
      tenantId: row.tenant_id,
      lifecycleState: row.lifecycle_state,
      content: asJson(row.content, {}),
      updatedAt: row.updated_at,
    };
  }

  async findPriorSequenceStep(tenantId, prospectId, sequenceStep) {
    if (sequenceStep <= 1) return { satisfied: true, schedule: null };
    const res = await this.pool.query(
      `SELECT * FROM tenant_outreach_scheduled_sends
       WHERE tenant_id = $1 AND prospect_id = $2 AND sequence_step = $3
       ORDER BY created_at DESC LIMIT 1`,
      [tenantKey(tenantId), prospectId, sequenceStep - 1]
    );
    const prior = normalizeSchedule(res.rows[0]);
    if (!prior) return { satisfied: false, schedule: null, reason: 'prior_step_not_scheduled' };
    if (prior.status !== SCHEDULE_STATUS.SENT) {
      return { satisfied: false, schedule: prior, reason: 'prior_step_not_sent' };
    }
    return { satisfied: true, schedule: prior };
  }

  async findSentScheduleByIdempotency(tenantId, key) {
    const res = await this.pool.query(
      `SELECT * FROM tenant_outreach_scheduled_sends
       WHERE tenant_id = $1 AND idempotency_key = $2 AND status = 'SENT'
       LIMIT 1`,
      [tenantKey(tenantId), key]
    );
    return normalizeSchedule(res.rows[0]);
  }
}

async function authorizeScheduledOutreachSend(input = {}, opts = {}) {
  const store = opts.scheduleStore || new PostgresScheduleStore(opts.pool || defaultPool);
  const tenantId = tenantKey(input.tenantId);
  if (!tenantId) throw schedulerError('tenant_required', 'tenantId is required.');
  if (!input.prospectId) throw schedulerError('prospect_required', 'prospectId is required.');
  if (!input.outreachAssetId) throw schedulerError('outreach_asset_required', 'outreachAssetId is required.');
  if (!input.sendingIdentityId) throw schedulerError('sending_identity_required', 'sendingIdentityId is required.');
  if (!input.recipientEmail) throw schedulerError('recipient_required', 'recipientEmail is required.');
  if (!input.scheduledFor) throw schedulerError('scheduled_for_required', 'scheduledFor is required.');
  if (!input.authorizedBy) throw schedulerError('authorized_by_required', 'authorizedBy is required.');
  if (!input.authorizationSource) throw schedulerError('authorization_source_required', 'authorizationSource is required.');
  if (!clean(input.subject)) throw schedulerError('subject_required', 'subject is required for authorization snapshot.');
  if (input.body == null) throw schedulerError('body_required', 'body is required for authorization snapshot.');

  const mailboxStore = opts.mailboxStore || new PostgresTenantMailboxStore(opts.pool || defaultPool);
  const identity = await mailboxStore.getIdentity(tenantId, input.sendingIdentityId);
  if (!identity) throw schedulerError('sending_identity_tenant_mismatch', 'Sending identity does not belong to this tenant.');

  const idempotencyKey = buildIdempotencyKey(input);
  const existing = await store.findByIdempotencyKey(tenantId, idempotencyKey);
  if (existing) {
    return { schedule: existing, duplicate: true, created: false };
  }

  const authorizedAt = input.authorizedAt || nowIso(opts);
  const snapshot = buildAuthorizationSnapshot({ ...input, authorizedAt });
  const schedule = await store.saveSchedule({
    tenantId,
    prospectId: String(input.prospectId),
    acquisitionKnowledgeObjectId: input.acquisitionKnowledgeObjectId || null,
    outreachAssetId: input.outreachAssetId,
    outreachAssetVersion: input.outreachAssetVersion || null,
    sendingIdentityId: input.sendingIdentityId,
    recipientEmail: lower(input.recipientEmail),
    missionId: input.missionId || null,
    threadId: input.threadId || null,
    sequenceStep: Number(input.sequenceStep || 1),
    scheduledFor: input.scheduledFor,
    timezone: input.timezone || DEFAULT_TIMEZONE,
    status: SCHEDULE_STATUS.SCHEDULED,
    authorizationSource: input.authorizationSource,
    authorizedBy: input.authorizedBy,
    authorizedAt,
    authorizationSnapshot: snapshot,
    pastDuePolicy: input.pastDuePolicy || PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
    maxLatenessMinutes: Number(input.maxLatenessMinutes ?? DEFAULT_MAX_LATENESS_MINUTES),
    idempotencyKey,
  });

  return { schedule, duplicate: false, created: true };
}

async function cancelScheduledOutreachSend(input = {}, opts = {}) {
  const store = opts.scheduleStore || new PostgresScheduleStore(opts.pool || defaultPool);
  const tenantId = tenantKey(input.tenantId);
  const schedule = await store.getSchedule(tenantId, input.scheduleId);
  if (!schedule) throw schedulerError('schedule_not_found', 'Scheduled send not found.');
  if (schedule.status === SCHEDULE_STATUS.SENT) {
    throw schedulerError('schedule_already_sent', 'Cannot cancel a send that has already been sent.');
  }
  if (schedule.status === SCHEDULE_STATUS.CANCELLED) {
    return { schedule, duplicate: true };
  }
  const updated = await store.updateSchedule(tenantId, schedule.id, {
    status: SCHEDULE_STATUS.CANCELLED,
    cancelledAt: nowIso(opts),
  });
  return { schedule: updated, duplicate: false };
}

const BLOCKED_SEQUENCE_STATES = new Set([
  SEQUENCE_STATE.PAUSED,
  SEQUENCE_STATE.REPLY_RECEIVED,
  SEQUENCE_STATE.MANUALLY_STOPPED,
  SEQUENCE_STATE.BOUNCED,
  SEQUENCE_STATE.COMPLETED,
  SEQUENCE_STATE.QUALIFIED_HANDOFF,
]);

async function evaluateSendEligibility(schedule, opts = {}) {
  const store = opts.scheduleStore;
  const mailboxStore = opts.mailboxStore;
  const now = opts.now || new Date();

  if (schedule.status === SCHEDULE_STATUS.CANCELLED) {
    return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: 'cancelled' };
  }
  if (schedule.status === SCHEDULE_STATUS.SENT) {
    return { eligible: false, action: 'noop', reason: 'already_sent' };
  }
  if (isFutureSend(schedule, now)) {
    return { eligible: false, action: 'defer', reason: 'not_due' };
  }
  if (isPastDueWindowExceeded(schedule, now)) {
    return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: 'missed_execution_window' };
  }

  const tenantActive = await store.getTenantActive(schedule.tenantId);
  if (!tenantActive) {
    return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: 'tenant_inactive' };
  }

  const identity = await mailboxStore.getIdentity(schedule.tenantId, schedule.sendingIdentityId);
  if (!identity || identity.status !== IDENTITY_STATUS.ACTIVE) {
    return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: 'sending_identity_inactive' };
  }
  const integration = await mailboxStore.getIntegration(schedule.tenantId, identity.mailboxIntegrationId);
  if (!integration || integration.status !== MAILBOX_STATUS.ACTIVE) {
    return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: 'mailbox_inactive' };
  }
  if (identity.tenantId !== schedule.tenantId) {
    return { eligible: false, action: SCHEDULE_STATUS.FAILED, reason: 'sending_identity_tenant_mismatch' };
  }

  const prospect = await store.getProspectEligibility(schedule.tenantId, schedule.prospectId);
  const snapshotEmail = lower(schedule.authorizationSnapshot?.recipientEmail || schedule.recipientEmail);
  if (prospect.exists) {
    if (prospect.doNotContact) {
      return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: 'prospect_dnc' };
    }
    if (prospect.booked) {
      return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: 'prospect_booked' };
    }
    if (prospect.email && prospect.email !== snapshotEmail) {
      return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: 'recipient_changed' };
    }
  }
  if (!snapshotEmail) {
    return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: 'recipient_missing' };
  }

  const suppression = await mailboxStore.findSuppression(schedule.tenantId, snapshotEmail);
  if (suppression) {
    const reason = suppression.reason === 'bounce' ? 'hard_bounce' : 'suppressed';
    return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason };
  }

  if (schedule.threadId) {
    const thread = await mailboxStore.getThread(schedule.tenantId, schedule.threadId);
    if (thread) {
      if (thread.replyState === 'reply_received' || thread.currentStatus === THREAD_STATUS.REPLIED) {
        return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: 'thread_replied' };
      }
      if (BLOCKED_SEQUENCE_STATES.has(thread.sequenceState)) {
        return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: `thread_sequence_${thread.sequenceState}` };
      }
    }
  }

  const priorStep = await store.findPriorSequenceStep(schedule.tenantId, schedule.prospectId, schedule.sequenceStep);
  if (!priorStep.satisfied) {
    return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: priorStep.reason || 'sequence_order_blocked' };
  }

  const asset = await store.getOutreachAsset(schedule.tenantId, schedule.outreachAssetId);
  if (asset && ['archived', 'retired'].includes(lower(asset.lifecycleState))) {
    return { eligible: false, action: SCHEDULE_STATUS.SKIPPED, reason: 'outreach_asset_invalid' };
  }

  const sentSchedule = await store.findSentScheduleByIdempotency(schedule.tenantId, schedule.idempotencyKey);
  if (sentSchedule) {
    return { eligible: false, action: 'recover_sent', reason: 'already_sent', outboundMessageId: sentSchedule.outboundMessageId };
  }

  const alreadySentMessage = await mailboxStore.findSentByIdempotencyKey(schedule.tenantId, schedule.idempotencyKey);
  if (alreadySentMessage) {
    return {
      eligible: false,
      action: 'recover_sent',
      reason: 'provider_message_exists',
      outboundMessageId: alreadySentMessage.id,
    };
  }

  return { eligible: true, action: 'send', recipientEmail: snapshotEmail };
}

async function finalizeSkipped(scheduleStore, schedule, reason, opts = {}) {
  return scheduleStore.updateSchedule(schedule.tenantId, schedule.id, {
    status: SCHEDULE_STATUS.SKIPPED,
    skipReason: reason,
    executedAt: nowIso(opts),
  });
}

async function finalizeFailed(scheduleStore, schedule, err, opts = {}) {
  return scheduleStore.updateSchedule(schedule.tenantId, schedule.id, {
    status: SCHEDULE_STATUS.FAILED,
    failureCode: err.code || 'send_failed',
    failureMessage: String(err.message || err).slice(0, 500),
    executedAt: nowIso(opts),
  });
}

async function finalizeSent(scheduleStore, schedule, messageId, threadId, opts = {}) {
  return scheduleStore.updateSchedule(schedule.tenantId, schedule.id, {
    status: SCHEDULE_STATUS.SENT,
    outboundMessageId: messageId,
    threadId: threadId || schedule.threadId,
    executedAt: nowIso(opts),
  });
}

async function executeScheduledSend(schedule, opts = {}) {
  if (!schedule) throw schedulerError('schedule_required', 'schedule is required.');
  const scheduleStore = opts.scheduleStore;
  const mailboxStore = opts.mailboxStore;
  const now = opts.now || new Date();

  if (schedule.outboundMessageId) {
    return { schedule, result: 'already_sent', outboundMessageId: schedule.outboundMessageId };
  }

  const eligibility = await evaluateSendEligibility(schedule, { ...opts, now });
  if (eligibility.action === 'defer' || eligibility.action === 'noop') {
    return { schedule, result: eligibility.action, reason: eligibility.reason };
  }
  if (eligibility.action === 'recover_sent') {
    const updated = await finalizeSent(
      scheduleStore,
      schedule,
      eligibility.outboundMessageId,
      schedule.threadId,
      opts
    );
    return { schedule: updated, result: 'recovered_sent', reason: eligibility.reason };
  }
  if (!eligibility.eligible) {
    const updated = await finalizeSkipped(scheduleStore, schedule, eligibility.reason, opts);
    return { schedule: updated, result: 'skipped', reason: eligibility.reason };
  }

  const snapshot = schedule.authorizationSnapshot || {};
  try {
    const sendResult = await sendTenantEmail({
      tenantId: schedule.tenantId,
      sendingIdentityId: schedule.sendingIdentityId,
      missionId: schedule.missionId || snapshot.missionId || null,
      prospectId: schedule.prospectId,
      outreachAssetId: schedule.outreachAssetId,
      threadId: schedule.threadId || snapshot.threadId || null,
      to: eligibility.recipientEmail,
      subject: snapshot.subject,
      body: snapshot.body,
      sequenceStepRef: String(schedule.sequenceStep),
      metadata: {
        idempotencyKey: schedule.idempotencyKey,
        scheduleId: schedule.id,
        outreachAssetVersion: schedule.outreachAssetVersion,
      },
    }, {
      store: mailboxStore,
      ...opts,
      now,
    });

    if (sendResult.duplicate) {
      const updated = await finalizeSent(
        scheduleStore,
        schedule,
        sendResult.message.id,
        sendResult.message.threadId,
        opts
      );
      return { schedule: updated, result: 'recovered_sent', message: sendResult.message };
    }

    const updated = await finalizeSent(
      scheduleStore,
      schedule,
      sendResult.message.id,
      sendResult.message.threadId,
      opts
    );
    return { schedule: updated, result: 'sent', message: sendResult.message };
  } catch (err) {
    if (err.outboundMessage?.status === 'sent' || err.code === 'duplicate') {
      const updated = await finalizeSent(
        scheduleStore,
        schedule,
        err.outboundMessage?.id,
        err.outboundMessage?.threadId,
        opts
      );
      return { schedule: updated, result: 'recovered_sent', reason: err.code };
    }
    const updated = await finalizeFailed(scheduleStore, schedule, err, opts);
    return { schedule: updated, result: 'failed', error: err };
  }
}

async function executeDueScheduledSends(opts = {}) {
  const pool = opts.pool || defaultPool;
  const query = opts.query || pool.query.bind(pool);
  const scheduleStore = opts.scheduleStore || new PostgresScheduleStore(pool);
  const mailboxStore = opts.mailboxStore || new PostgresTenantMailboxStore(pool);
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());
  const limit = Number(opts.limit || 20);
  const base = {
    claimed: 0,
    sent: 0,
    skipped: 0,
    failed: 0,
    recovered: 0,
    deferred: 0,
    halted_reason: null,
  };

  await ensureSchedulerSchema(query);

  if (!envEnabled(opts.globalEnabled ?? process.env.TENANT_OUTREACH_EXECUTOR_ENABLED ?? 'true')) {
    return { ...base, halted_reason: 'disabled' };
  }

  const lockClient = opts.lockClient || await pool.connect();
  const releaseClient = !opts.lockClient;
  let locked = false;
  try {
    const lock = await lockClient.query(
      'SELECT pg_try_advisory_lock($1, $2) AS locked',
      [EXECUTOR_LOCK_NAMESPACE, 0]
    );
    locked = lock.rows[0]?.locked === true;
    if (!locked) {
      return { ...base, halted_reason: 'overlap' };
    }

    const claimToken = prefixedId('claim', `${nowIso({ now })}:${Math.random()}`);
    const claimed = await scheduleStore.claimDueSchedules({ now, limit, claimToken });
    base.claimed = claimed.length;

    for (const schedule of claimed) {
      const outcome = await executeScheduledSend(schedule, {
        ...opts,
        scheduleStore,
        mailboxStore,
        now,
      });
      if (outcome.result === 'sent') base.sent += 1;
      else if (outcome.result === 'skipped') base.skipped += 1;
      else if (outcome.result === 'failed') base.failed += 1;
      else if (outcome.result === 'recovered_sent') base.recovered += 1;
      else if (outcome.result === 'defer' || outcome.result === 'noop') {
        base.deferred += 1;
        await scheduleStore.updateSchedule(schedule.tenantId, schedule.id, {
          status: SCHEDULE_STATUS.SCHEDULED,
          claimToken: null,
          claimedAt: null,
        });
      }
    }

    return base;
  } finally {
    if (locked) {
      await lockClient.query('SELECT pg_advisory_unlock($1, $2)', [EXECUTOR_LOCK_NAMESPACE, 0]).catch(() => {});
    }
    if (releaseClient) lockClient.release();
  }
}

async function listScheduledOutreachSends(tenantId, filters = {}, opts = {}) {
  const store = opts.scheduleStore || new PostgresScheduleStore(opts.pool || defaultPool);
  const rows = await store.listSchedules(tenantKey(tenantId), filters);
  return rows.map(publicSchedule);
}

async function getScheduledOutreachSend(tenantId, scheduleId, opts = {}) {
  const store = opts.scheduleStore || new PostgresScheduleStore(opts.pool || defaultPool);
  return publicSchedule(await store.getSchedule(tenantKey(tenantId), scheduleId));
}

module.exports = {
  SCHEDULE_STATUS,
  PAST_DUE_POLICY,
  EXECUTOR_LOCK_NAMESPACE,
  MemoryScheduleStore,
  PostgresScheduleStore,
  ensureSchedulerSchema,
  authorizeScheduledOutreachSend,
  cancelScheduledOutreachSend,
  evaluateSendEligibility,
  executeScheduledSend,
  executeDueScheduledSends,
  listScheduledOutreachSends,
  getScheduledOutreachSend,
  buildIdempotencyKey,
  buildAuthorizationSnapshot,
  normalizeSchedule,
  publicSchedule,
  isPastDueWindowExceeded,
  isFutureSend,
};
