'use strict';

const { ATTENTION_STATUS, UNRESOLVED_STATUSES } = require('../types');
const { DEFAULT_LEASE_MS, validateLeaseMs, requireClaimToken, claimLost } = require('../claimPolicy');
const { withAttentionTransaction } = require('../transaction');

function mapRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    client_id: row.client_id,
    subject_type: row.subject_type,
    subject_id: row.subject_id,
    reason: row.reason,
    source_decision_id: row.source_decision_id,
    supporting_evidence: row.supporting_evidence || [],
    status: row.status,
    priority: row.priority || {},
    next_review_at: row.next_review_at ? row.next_review_at.toISOString?.() || row.next_review_at : null,
    review_trigger: row.review_trigger,
    owner: row.owner,
    audience_tier: row.audience_tier,
    parent_attention_id: row.parent_attention_id,
    dedup_fingerprint: row.dedup_fingerprint,
    resolution_evidence: row.resolution_evidence || [],
    last_evaluation_id: row.last_evaluation_id,
    claim_token: row.claim_token,
    claimed_at: row.claimed_at ? row.claimed_at.toISOString?.() || row.claimed_at : null,
    execution_failures: row.execution_failures,
    created_at: row.created_at?.toISOString?.() || row.created_at,
    last_reviewed_at: row.last_reviewed_at?.toISOString?.() || row.last_reviewed_at,
    resolved_at: row.resolved_at?.toISOString?.() || row.resolved_at,
  };
}

class PostgresAttentionStore {
  constructor(db, { clientId, leaseMs = DEFAULT_LEASE_MS } = {}) {
    this.db = db;
    this.clientId = clientId;
    this.leaseMs = validateLeaseMs(leaseMs);
  }

  async init() {
    const { ensureMaxAttentionSchema } = require('../../../../utils/maxAttentionSchema');
    await ensureMaxAttentionSchema(this.db);
    return this;
  }

  async findByFingerprint(clientId, fingerprint, { includeResolved = false } = {}) {
    const { rows } = await this.db.query(
      `SELECT * FROM max_attention_items
       WHERE client_id = $1 AND dedup_fingerprint = $2
         AND ($4::boolean OR status = ANY($3::text[]))
       LIMIT 1`,
      [clientId, fingerprint, UNRESOLVED_STATUSES, includeResolved]
    );
    return mapRow(rows[0]);
  }

  async findById(id) {
    const { rows } = await this.db.query(`SELECT * FROM max_attention_items WHERE id = $1`, [id]);
    return mapRow(rows[0]);
  }

  async createAttention(item) {
    await this.db.query(
      `INSERT INTO max_attention_items (
        id, client_id, subject_type, subject_id, reason, source_decision_id,
        supporting_evidence, status, priority, next_review_at, review_trigger,
        owner, audience_tier, parent_attention_id, dedup_fingerprint,
        resolution_evidence, last_evaluation_id, created_at, last_reviewed_at, resolved_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20
      )`,
      [
        item.id,
        item.client_id,
        item.subject_type,
        item.subject_id,
        item.reason,
        item.source_decision_id || null,
        JSON.stringify(item.supporting_evidence || []),
        item.status,
        JSON.stringify(item.priority || {}),
        item.next_review_at || null,
        item.review_trigger,
        item.owner || null,
        item.audience_tier,
        item.parent_attention_id || null,
        item.dedup_fingerprint,
        JSON.stringify(item.resolution_evidence || []),
        item.last_evaluation_id || null,
        item.created_at || new Date(),
        item.last_reviewed_at || null,
        item.resolved_at || null,
      ]
    );
    return item;
  }

  async updateAttention(id, patch) {
    const current = await this.findById(id);
    if (!current) return null;
    const merged = { ...current, ...patch, id };
    await this.db.query(
      `UPDATE max_attention_items SET
        reason = $2,
        source_decision_id = $3,
        supporting_evidence = $4,
        status = $5,
        priority = $6,
        next_review_at = $7,
        review_trigger = $8,
        owner = $9,
        audience_tier = $10,
        last_evaluation_id = $11,
        last_reviewed_at = $12,
        resolved_at = $13,
        resolution_evidence = $14
       WHERE id = $1`,
      [
        id,
        merged.reason,
        merged.source_decision_id || null,
        JSON.stringify(merged.supporting_evidence || []),
        merged.status,
        JSON.stringify(merged.priority || {}),
        merged.next_review_at || null,
        merged.review_trigger,
        merged.owner || null,
        merged.audience_tier,
        merged.last_evaluation_id || null,
        merged.last_reviewed_at || null,
        merged.resolved_at || null,
        JSON.stringify(merged.resolution_evidence || []),
      ]
    );
    return this.findById(id);
  }

  async resolveAttention(id, patch) {
    return this.updateAttention(id, {
      status: patch.status || ATTENTION_STATUS.RESOLVED,
      resolution_evidence: patch.resolution_evidence || [],
      resolved_at: patch.resolved_at || new Date().toISOString(),
      last_evaluation_id: patch.last_evaluation_id,
      next_review_at: null,
    });
  }

  async listUnresolved({ clientId, subjectType, subjectId } = {}) {
    const params = [clientId, UNRESOLVED_STATUSES];
    let sql = `SELECT * FROM max_attention_items WHERE client_id = $1 AND status = ANY($2::text[])`;
    if (subjectType) {
      params.push(subjectType);
      sql += ` AND subject_type = $${params.length}`;
    }
    if (subjectId) {
      params.push(subjectId);
      sql += ` AND subject_id = $${params.length}`;
    }
    const { rows } = await this.db.query(sql, params);
    return rows.map(mapRow);
  }

  async claimDueItems({ clientId, now = new Date(), limit = 20, claimToken }) {
    requireClaimToken(claimToken);
    return withAttentionTransaction(this.db, async client => {
      const due = await client.query(
        `SELECT id FROM max_attention_items
         WHERE client_id = $1
           AND status = ANY($2::text[])
           AND next_review_at IS NOT NULL
           AND next_review_at <= $3::timestamptz
           AND (claim_token IS NULL OR claimed_at IS NULL
             OR claimed_at <= clock_timestamp() - $5::double precision * interval '1 millisecond')
         ORDER BY next_review_at
         LIMIT $4
         FOR UPDATE SKIP LOCKED`,
        [clientId, UNRESOLVED_STATUSES, now.toISOString(), limit, this.leaseMs]
      );
      const claimed = [];
      for (const row of due.rows) {
        const updated = await client.query(
          `UPDATE max_attention_items
           SET claim_token = $2,
               claimed_at = clock_timestamp(),
               status = CASE WHEN next_review_at < $3::timestamptz THEN 'OVERDUE' ELSE status END
           WHERE id = $1
           RETURNING *`,
          [row.id, claimToken, now.toISOString()]
        );
        if (updated.rows[0]) claimed.push(mapRow(updated.rows[0]));
      }
      return claimed;
    });
  }

  // The callback must use this connection for every effect. Never call init() here:
  // migration BEGIN/COMMIT statements would break the transaction fence.
  async withClaim(id, { claimToken, clientId }, callback) {
    requireClaimToken(claimToken);
    return withAttentionTransaction(this.db, async client => {
      const { rows } = await client.query(
        `SELECT * FROM max_attention_items
         WHERE id = $1 AND claim_token = $2 AND client_id = $3
           AND claimed_at > clock_timestamp() - $4::double precision * interval '1 millisecond'
         FOR UPDATE`, [id, claimToken, clientId, this.leaseMs]
      );
      if (!rows[0]) throw claimLost();
      const scoped = new PostgresAttentionStore(client, { clientId, leaseMs: this.leaseMs });
      const result = await callback({ db: client, attentionStore: scoped, item: mapRow(rows[0]) });
      return result;
    }, { bounded: true });
  }

  async releaseClaim(id, { claimToken, patch = {} } = {}) {
    requireClaimToken(claimToken);
    const failures = patch.execution_failures_increment ? 1 : 0;
    const fields = [
      'claim_token = NULL',
      'claimed_at = NULL',
      `execution_failures = execution_failures + ${failures}`,
    ];
    const params = [id, claimToken, this.leaseMs];
    const add = (column, value, { allowNull = false } = {}) => {
      if (value === undefined && !allowNull) return;
      if (value === undefined) return;
      params.push(value);
      fields.push(`${column} = $${params.length}`);
    };
    add('status', patch.status);
    if ('next_review_at' in patch) add('next_review_at', patch.next_review_at, { allowNull: true });
    add('last_reviewed_at', patch.last_reviewed_at);
    add('last_evaluation_id', patch.last_evaluation_id);
    if ('resolved_at' in patch) add('resolved_at', patch.resolved_at, { allowNull: true });
    if (patch.resolution_evidence) {
      params.push(JSON.stringify(patch.resolution_evidence));
      fields.push(`resolution_evidence = $${params.length}`);
    }
    const { rows } = await this.db.query(
      `UPDATE max_attention_items SET ${fields.join(', ')}
       WHERE id = $1 AND claim_token = $2
         AND claimed_at > clock_timestamp() - $3::double precision * interval '1 millisecond'
       RETURNING *`,
      params
    );
    return mapRow(rows[0]);
  }

  async recordSchedulerRun(run) {
    if (run.status !== 'running') return this.finishSchedulerRun(run);
    const { rows } = await this.db.query(
      `INSERT INTO max_attention_scheduler_runs (
        id, client_id, started_at, status, items_claimed, items_evaluated, items_failed, telemetry
      ) VALUES ($1,$2,$3,'running',0,0,0,$4)
      ON CONFLICT (id) DO NOTHING RETURNING *`,
      [run.id, run.client_id ?? null, run.started_at, JSON.stringify(run.telemetry || {})]
    );
    if (rows[0]) return rows[0];
    return this._readSchedulerRun(run, this.db);
  }

  async _readSchedulerRun(run, db) {
    const { rows } = await db.query(
      `SELECT * FROM max_attention_scheduler_runs
       WHERE id = $1 AND client_id IS NOT DISTINCT FROM $2 AND started_at = $3::timestamptz`,
      [run.id, run.client_id ?? null, run.started_at]
    );
    if (!rows[0]) throw new Error('Scheduler run identity does not match or has not started');
    return rows[0];
  }

  async finishSchedulerRun(run) {
    if (!['completed', 'failed'].includes(run.status)) throw new Error('Terminal scheduler status required');
    return withAttentionTransaction(this.db, async client => {
      const { rows } = await client.query(
        `UPDATE max_attention_scheduler_runs SET
           completed_at = $4, status = $5, items_claimed = $6, items_evaluated = $7,
           items_failed = $8, error_message = $9, telemetry = $10
         WHERE id = $1 AND client_id IS NOT DISTINCT FROM $2 AND started_at = $3::timestamptz
           AND status = 'running'
         RETURNING *`,
        [run.id, run.client_id ?? null, run.started_at, run.completed_at, run.status,
          run.items_claimed || 0, run.items_evaluated || 0, run.items_failed || 0,
          run.error_message || null, JSON.stringify(run.telemetry || {})]
      );
      const saved = rows[0] || await this._readSchedulerRun(run, client);
      // Only the first terminal transition updates the heartbeat. Atomic arithmetic
      // avoids lost increments between concurrent failed cycles.
      if (rows[0]) {
        await client.query(
          `UPDATE max_attention_heartbeats SET
             last_successful_cycle_at = CASE WHEN $2 = 'completed' THEN $3::timestamptz
               ELSE last_successful_cycle_at END,
             last_run_id = $1,
             consecutive_failures = CASE WHEN $2 = 'failed' THEN consecutive_failures + 1 ELSE 0 END,
             updated_at = clock_timestamp()
           WHERE id = 'global'`, [run.id, run.status, run.completed_at]
        );
      }
      return saved;
    });
  }

  async updateHeartbeat(patch) {
    await this.db.query(
      `UPDATE max_attention_heartbeats SET
         last_successful_cycle_at = COALESCE($1, last_successful_cycle_at),
         last_run_id = COALESCE($2, last_run_id),
         consecutive_failures = COALESCE($3, consecutive_failures),
         updated_at = NOW()
       WHERE id = 'global'`,
      [
        patch.last_successful_cycle_at || null,
        patch.last_run_id || null,
        patch.consecutive_failures ?? null,
      ]
    );
    return this.getHeartbeat();
  }

  async getHeartbeat() {
    const { rows } = await this.db.query(`SELECT * FROM max_attention_heartbeats WHERE id = 'global'`);
    const row = rows[0];
    if (!row) return null;
    return {
      id: row.id,
      last_successful_cycle_at: row.last_successful_cycle_at?.toISOString?.() || row.last_successful_cycle_at,
      last_run_id: row.last_run_id,
      consecutive_failures: row.consecutive_failures,
      updated_at: row.updated_at?.toISOString?.() || row.updated_at,
    };
  }
}

module.exports = {
  PostgresAttentionStore,
};
