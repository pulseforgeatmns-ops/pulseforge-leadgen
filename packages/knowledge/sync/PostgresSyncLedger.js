'use strict';

/**
 * Postgres-backed sync idempotency ledger (SPEC-014).
 * Same interface as InMemorySyncLedger — GraphSyncEngine is storage-agnostic.
 */
class PostgresSyncLedger {
  /**
   * @param {{ query: Function }} pool
   */
  constructor(pool) {
    if (!pool || typeof pool.query !== 'function') {
      throw new Error('PostgresSyncLedger requires a pg pool');
    }
    this.pool = pool;
  }

  async has(tenantId, idempotencyKey) {
    const res = await this.pool.query(
      `SELECT 1 FROM knowledge_sync_ledger
       WHERE tenant_id = $1 AND idempotency_key = $2
       LIMIT 1`,
      [String(tenantId), String(idempotencyKey)]
    );
    return res.rowCount > 0;
  }

  async get(tenantId, idempotencyKey) {
    const res = await this.pool.query(
      `SELECT record, marked_at FROM knowledge_sync_ledger
       WHERE tenant_id = $1 AND idempotency_key = $2
       LIMIT 1`,
      [String(tenantId), String(idempotencyKey)]
    );
    if (!res.rows[0]) return null;
    return {
      key: String(idempotencyKey),
      tenantId: String(tenantId),
      markedAt: res.rows[0].marked_at,
      ...(res.rows[0].record || {}),
    };
  }

  async mark(tenantId, idempotencyKey, record = {}) {
    const entry = {
      key: String(idempotencyKey),
      tenantId: String(tenantId),
      markedAt: new Date().toISOString(),
      ...record,
    };
    await this.pool.query(
      `INSERT INTO knowledge_sync_ledger (tenant_id, idempotency_key, marked_at, record)
       VALUES ($1, $2, NOW(), $3::jsonb)
       ON CONFLICT (tenant_id, idempotency_key) DO UPDATE
         SET marked_at = EXCLUDED.marked_at,
             record = EXCLUDED.record`,
      [String(tenantId), String(idempotencyKey), JSON.stringify(record)]
    );
    return entry;
  }

  async clearTenant(tenantId) {
    await this.pool.query(
      `DELETE FROM knowledge_sync_ledger WHERE tenant_id = $1`,
      [String(tenantId)]
    );
  }

  async size(tenantId) {
    const res = await this.pool.query(
      `SELECT COUNT(*)::int AS n FROM knowledge_sync_ledger WHERE tenant_id = $1`,
      [String(tenantId)]
    );
    return res.rows[0]?.n || 0;
  }
}

module.exports = {
  PostgresSyncLedger,
};
