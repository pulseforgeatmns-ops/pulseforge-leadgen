'use strict';

const { normalizeKnowledgeEvent, FLIGHT_STAGES } = require('./operationalEvents');
const {
  envelopeForCompany,
  envelopeForProspect,
  envelopeForTouchpoint,
  envelopeForOperationalEvent,
} = require('./envelopes');
const { recordFlightStage } = require('./flightRecorder');

const MAX_ATTEMPTS = 8;
const BASE_RETRY_MS = 5_000;

/**
 * Knowledge Writer — outbox-first dual-write into GraphSyncEngine (SPEC-014).
 *
 * Contract:
 * 1. Persist to outbox (never silently discard)
 * 2. Attempt sync.apply immediately
 * 3. On failure leave queued for retry
 * 4. Idempotent via ledger + outbox unique key
 */
class KnowledgeWriter {
  /**
   * @param {object} deps
   * @param {{ query: Function }} deps.pool
   * @param {{ apply: Function }} deps.sync - GraphSyncEngine
   * @param {(m: object) => void} [deps.onLog]
   */
  constructor(deps) {
    if (!deps.pool || typeof deps.pool.query !== 'function') {
      throw new Error('KnowledgeWriter requires pool');
    }
    if (!deps.sync || typeof deps.sync.apply !== 'function') {
      throw new Error('KnowledgeWriter requires GraphSyncEngine');
    }
    this.pool = deps.pool;
    this.sync = deps.sync;
    this.onLog = typeof deps.onLog === 'function' ? deps.onLog : () => {};
  }

  /**
   * Dual-write a sync envelope (already mapped).
   * @param {object} syncEnvelope
   * @param {object} [meta] - KnowledgeEvent-shaped metadata for outbox + flight
   */
  async writeEnvelope(syncEnvelope, meta = {}) {
    if (!syncEnvelope || !syncEnvelope.id || syncEnvelope.tenantId == null) {
      throw new Error('writeEnvelope requires sync envelope with id + tenantId');
    }
    const tenantId = String(syncEnvelope.tenantId);
    const idempotencyKey = String(syncEnvelope.id);
    const eventType =
      meta.eventType ||
      syncEnvelope.payload?.knowledgeEvent?.payload?.metadata?.operationalEventType ||
      syncEnvelope.type;
    const entityId =
      meta.entityId != null
        ? String(meta.entityId)
        : syncEnvelope.payload?.entityId != null
          ? String(syncEnvelope.payload.entityId)
          : null;
    const entityType = meta.entityType || syncEnvelope.payload?.entityKind || null;
    const source = meta.source || 'dual_write';
    const flightId =
      meta.flightId ||
      (entityId ? `flight:${tenantId}:${entityType || 'entity'}:${entityId}` : `flight:${idempotencyKey}`);

    const inserted = await this._enqueue({
      tenantId,
      idempotencyKey,
      eventType,
      entityId,
      entityType,
      source,
      payload: meta.payload || syncEnvelope.payload || {},
      evidence: meta.evidence || {},
      syncEnvelope,
    });

    if (meta.markDiscovered) {
      await this._safeFlight(flightId, tenantId, entityId, entityType, FLIGHT_STAGES.PROSPECT_DISCOVERED, {
        eventType,
        source,
      });
    }

    // Already applied previously — idempotent success
    if (inserted.status === 'applied') {
      return { status: 'skipped', reason: 'already_applied', outboxId: inserted.id, flightId };
    }

    return this._attemptApply(inserted.id, syncEnvelope, {
      flightId,
      tenantId,
      entityId,
      entityType,
      eventType,
    });
  }

  async writeCompany(row, options = {}) {
    const envelope = envelopeForCompany(row, options);
    return this.writeEnvelope(envelope, {
      eventType: options.operationalEventType || 'prospect.company_discovered',
      entityId: row.id,
      entityType: 'company',
      source: options.source || 'crm',
      markDiscovered: options.markDiscovered !== false,
      flightId: options.flightId,
      payload: { company: { id: row.id, name: row.name } },
      evidence: options.evidence || {},
    });
  }

  async writeProspect(row, options = {}) {
    const envelope = envelopeForProspect(row, options);
    return this.writeEnvelope(envelope, {
      eventType: options.operationalEventType || 'prospect.contact_discovered',
      entityId: row.id,
      entityType: 'prospect',
      source: options.source || 'crm',
      markDiscovered: options.markDiscovered !== false,
      flightId:
        options.flightId ||
        (row.id != null
          ? `flight:${row.client_id}:prospect:${row.id}`
          : undefined),
      payload: {
        prospect: {
          id: row.id,
          email: row.email,
          company_id: row.company_id,
        },
      },
      evidence: options.evidence || {},
    });
  }

  async writeTouchpoint(row, options = {}) {
    const envelope = envelopeForTouchpoint(row, options);
    return this.writeEnvelope(envelope, {
      eventType: options.operationalEventType || 'ops.touchpoint',
      entityId: row.prospect_id || row.id,
      entityType: 'prospect',
      source: options.source || 'crm',
      markDiscovered: false,
      flightId:
        options.flightId ||
        (row.prospect_id != null
          ? `flight:${row.client_id}:prospect:${row.prospect_id}`
          : undefined),
      payload: { touchpoint: { id: row.id, channel: row.channel, action: row.action_type } },
      evidence: options.evidence || {},
    });
  }

  async writeOperational(knowledgeEvent, options = {}) {
    const evt = normalizeKnowledgeEvent(knowledgeEvent);
    const envelope = envelopeForOperationalEvent(evt, options);
    return this.writeEnvelope(envelope, {
      eventType: evt.eventType,
      entityId: evt.entityId,
      entityType: evt.entityType,
      source: evt.source,
      markDiscovered: options.markDiscovered === true,
      flightId: options.flightId,
      payload: evt.payload,
      evidence: evt.evidence,
    });
  }

  /**
   * Drain pending/failed outbox rows due for retry.
   */
  async processOutbox(options = {}) {
    const limit = Math.min(Number(options.limit) || 50, 200);
    const now = new Date().toISOString();
    const res = await this.pool.query(
      `SELECT id, sync_envelope, tenant_id, entity_id, entity_type, event_type, attempts
       FROM knowledge_outbox
       WHERE status IN ('pending', 'failed')
         AND (next_retry_at IS NULL OR next_retry_at <= $1::timestamptz)
         AND attempts < $2
       ORDER BY created_at ASC
       LIMIT $3
       FOR UPDATE SKIP LOCKED`,
      [now, MAX_ATTEMPTS, limit]
    );

    const summary = { claimed: res.rows.length, applied: 0, failed: 0, dead: 0, skipped: 0 };
    for (const row of res.rows) {
      const result = await this._attemptApply(row.id, row.sync_envelope, {
        flightId: `flight:${row.tenant_id}:${row.entity_type || 'entity'}:${row.entity_id || row.id}`,
        tenantId: row.tenant_id,
        entityId: row.entity_id,
        entityType: row.entity_type,
        eventType: row.event_type,
      });
      if (result.status === 'applied' || result.status === 'skipped') {
        if (result.status === 'applied') summary.applied += 1;
        else summary.skipped += 1;
      } else if (result.status === 'dead') {
        summary.dead += 1;
      } else {
        summary.failed += 1;
      }
    }
    return summary;
  }

  /**
   * Admin health snapshot.
   */
  async health(options = {}) {
    const tenantFilter = options.tenantId != null ? String(options.tenantId) : null;
    const params = [];
    const tenantClause = tenantFilter
      ? (() => {
          params.push(tenantFilter);
          return ` AND tenant_id = $${params.length}`;
        })()
      : '';

    const todayRes = await this.pool.query(
      `SELECT COUNT(*)::int AS n FROM knowledge_outbox
       WHERE created_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
       ${tenantClause}`,
      params
    );
    const queueRes = await this.pool.query(
      `SELECT COUNT(*)::int AS n FROM knowledge_outbox
       WHERE status IN ('pending', 'failed', 'processing')
       ${tenantClause}`,
      params
    );
    const failRes = await this.pool.query(
      `SELECT COUNT(*)::int AS n FROM knowledge_outbox
       WHERE status IN ('failed', 'dead')
         AND created_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
       ${tenantClause}`,
      params
    );
    const appliedRes = await this.pool.query(
      `SELECT COUNT(*)::int AS n FROM knowledge_outbox
       WHERE status = 'applied'
         AND created_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
       ${tenantClause}`,
      params
    );
    const lastRes = await this.pool.query(
      `SELECT applied_at, created_at, event_type, tenant_id
       FROM knowledge_outbox
       WHERE status = 'applied' ${tenantClause}
       ORDER BY applied_at DESC NULLS LAST
       LIMIT 1`,
      params
    );

    let evidenceToday = 0;
    try {
      const evParams = [];
      let evClause = `WHERE created_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC')`;
      if (tenantFilter) {
        evParams.push(tenantFilter);
        evClause += ` AND tenant_id = $1`;
      }
      const ev = await this.pool.query(
        `SELECT COUNT(*)::int AS n FROM knowledge_evidence ${evClause}`,
        evParams
      );
      evidenceToday = ev.rows[0]?.n || 0;
    } catch {
      evidenceToday = 0;
    }

    return {
      knowledgeEventsToday: todayRes.rows[0]?.n || 0,
      knowledgeQueueDepth: queueRes.rows[0]?.n || 0,
      dualWriteFailures: failRes.rows[0]?.n || 0,
      evidenceCreated: evidenceToday,
      appliedToday: appliedRes.rows[0]?.n || 0,
      lastSuccessfulWrite: lastRes.rows[0]
        ? {
            at: lastRes.rows[0].applied_at || lastRes.rows[0].created_at,
            eventType: lastRes.rows[0].event_type,
            tenantId: lastRes.rows[0].tenant_id,
          }
        : null,
      asOf: new Date().toISOString(),
      tenantId: tenantFilter,
    };
  }

  async _enqueue(row) {
    const res = await this.pool.query(
      `INSERT INTO knowledge_outbox
         (tenant_id, idempotency_key, event_type, entity_id, entity_type, source,
          payload, evidence, sync_envelope, status, next_retry_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, 'pending', NOW())
       ON CONFLICT (tenant_id, idempotency_key) DO UPDATE
         SET updated_at = knowledge_outbox.updated_at
       RETURNING id, status, attempts`,
      [
        row.tenantId,
        row.idempotencyKey,
        row.eventType,
        row.entityId,
        row.entityType,
        row.source,
        JSON.stringify(row.payload || {}),
        JSON.stringify(row.evidence || {}),
        JSON.stringify(row.syncEnvelope),
      ]
    );
    return res.rows[0];
  }

  async _attemptApply(outboxId, syncEnvelope, ctx) {
    await this.pool.query(
      `UPDATE knowledge_outbox
       SET status = 'processing', attempts = attempts + 1, updated_at = NOW()
       WHERE id = $1`,
      [outboxId]
    );

    try {
      const result = await this.sync.apply(syncEnvelope);
      await this.pool.query(
        `UPDATE knowledge_outbox
         SET status = 'applied', applied_at = NOW(), updated_at = NOW(),
             last_error = NULL, next_retry_at = NULL
         WHERE id = $1`,
        [outboxId]
      );
      await this._safeFlight(
        ctx.flightId,
        ctx.tenantId,
        ctx.entityId,
        ctx.entityType,
        FLIGHT_STAGES.KNOWLEDGE_WRITTEN,
        { eventType: ctx.eventType, syncStatus: result.status }
      );
      return { status: result.status === 'skipped' ? 'skipped' : 'applied', result, outboxId, flightId: ctx.flightId };
    } catch (err) {
      const attemptsRes = await this.pool.query(
        `SELECT attempts FROM knowledge_outbox WHERE id = $1`,
        [outboxId]
      );
      const attempts = attemptsRes.rows[0]?.attempts || 1;
      const dead = attempts >= MAX_ATTEMPTS;
      const delay = BASE_RETRY_MS * Math.pow(2, Math.min(attempts - 1, 6));
      await this.pool.query(
        `UPDATE knowledge_outbox
         SET status = $2,
             last_error = $3,
             next_retry_at = NOW() + ($4 || ' milliseconds')::interval,
             updated_at = NOW()
         WHERE id = $1`,
        [outboxId, dead ? 'dead' : 'failed', String(err.message || err), String(delay)]
      );
      this.onLog({
        level: 'error',
        msg: 'knowledge_dual_write_failed',
        outboxId,
        error: String(err.message || err),
        attempts,
        dead,
      });
      await this._safeFlight(
        ctx.flightId,
        ctx.tenantId,
        ctx.entityId,
        ctx.entityType,
        FLIGHT_STAGES.KNOWLEDGE_WRITTEN,
        { eventType: ctx.eventType, error: String(err.message || err) },
        dead ? 'failed' : 'pending'
      );
      return { status: dead ? 'dead' : 'failed', error: String(err.message || err), outboxId, flightId: ctx.flightId };
    }
  }

  async _safeFlight(flightId, tenantId, entityId, entityType, stage, metadata, status = 'complete') {
    try {
      await recordFlightStage(this.pool, {
        flightId,
        tenantId,
        entityId,
        entityType,
        stage,
        status,
        metadata,
      });
    } catch (err) {
      this.onLog({
        level: 'warn',
        msg: 'flight_stage_failed',
        error: String(err.message || err),
        stage,
      });
    }
  }
}

module.exports = {
  KnowledgeWriter,
  MAX_ATTEMPTS,
  BASE_RETRY_MS,
};
