'use strict';

const crypto = require('crypto');
const { SEMANTIC_TYPE } = require('./conversationMemoryTypes');

function mapRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: row.client_id,
    conversationId: row.conversation_id,
    actorId: row.actor_id || null,
    actorRole: row.actor_role || null,
    entityType: row.entity_type || null,
    entityId: row.entity_id || null,
    semanticType: row.semantic_type,
    payload: row.payload || {},
    sourceInputId: row.source_input_id || null,
    sourceSituationModelId: row.source_situation_model_id || null,
    recordFingerprint: row.record_fingerprint,
    confidence: row.confidence != null ? Number(row.confidence) : null,
    occurredAt: row.occurred_at?.toISOString?.() || row.occurred_at || null,
    createdAt: row.created_at?.toISOString?.() || row.created_at,
    lastReferencedAt: row.last_referenced_at?.toISOString?.() || row.last_referenced_at || null,
    expiresAt: row.expires_at?.toISOString?.() || row.expires_at || null,
    supersededAt: row.superseded_at?.toISOString?.() || row.superseded_at || null,
  };
}

class MemoryConversationMemoryRepository {
  constructor() {
    this.records = [];
  }

  reset() {
    this.records = [];
  }

  async loadActive({ tenantId, conversationId, actorId = null, now = new Date() }) {
    const nowMs = now.getTime();
    let rows = this.records.filter(r =>
      r.tenantId === tenantId
      && r.conversationId === conversationId
      && !r.supersededAt
    );
    if (actorId != null) {
      rows = rows.filter(r => r.actorId == null || String(r.actorId) === String(actorId));
    }
    let expired = 0;
    rows = rows.filter(r => {
      if (r.expiresAt && new Date(r.expiresAt).getTime() < nowMs) {
        expired += 1;
        return false;
      }
      return true;
    });
    return { records: rows, expiredCount: expired };
  }

  async upsertRecords(records = []) {
    let written = 0;
    for (const rec of records) {
      const idx = this.records.findIndex(r =>
        r.tenantId === rec.tenantId
        && r.conversationId === rec.conversationId
        && r.recordFingerprint === rec.recordFingerprint
      );
      if (idx >= 0) {
        this.records[idx] = { ...this.records[idx], ...rec, id: this.records[idx].id };
      } else {
        this.records.push({ id: crypto.randomUUID(), ...rec });
        written += 1;
      }
    }
    return { written, duplicates: records.length - written };
  }

  async supersedeActive({ tenantId, conversationId, actorId = null, semanticTypes = [], bindingKeys = [] }) {
    const nowIso = new Date().toISOString();
    let count = 0;
    for (const rec of this.records) {
      if (rec.tenantId !== tenantId || rec.conversationId !== conversationId || rec.supersededAt) continue;
      if (actorId != null && rec.actorId != null && String(rec.actorId) !== String(actorId)) continue;
      if (semanticTypes.length && !semanticTypes.includes(rec.semanticType)) continue;
      if (bindingKeys.length) {
        const key = rec.payload?.bindingKey;
        if (!key || !bindingKeys.includes(key)) continue;
      }
      rec.supersededAt = nowIso;
      count += 1;
    }
    return count;
  }
}

class PostgresConversationMemoryRepository {
  constructor(db) {
    this.db = db;
  }

  async init() {
    const { ensureMaxConversationMemorySchema } = require('../../../utils/maxConversationMemorySchema');
    await ensureMaxConversationMemorySchema(this.db);
  }

  async loadActive({ tenantId, conversationId, actorId = null, now = new Date() }) {
    const params = [tenantId, conversationId, now.toISOString()];
    let actorClause = '';
    if (actorId != null) {
      actorClause = ' AND (actor_id IS NULL OR actor_id = $4)';
      params.push(String(actorId));
    }
    const { rows } = await this.db.query(
      `SELECT * FROM max_conversation_memory
       WHERE client_id = $1
         AND conversation_id = $2
         AND superseded_at IS NULL
         AND (expires_at IS NULL OR expires_at > $3::timestamptz)
         ${actorClause}
       ORDER BY created_at ASC`,
      params
    );
    const expiredResult = await this.db.query(
      `SELECT COUNT(*)::int AS n FROM max_conversation_memory
       WHERE client_id = $1 AND conversation_id = $2
         AND superseded_at IS NULL
         AND expires_at IS NOT NULL AND expires_at <= $3::timestamptz`,
      [tenantId, conversationId, now.toISOString()]
    );
    return {
      records: rows.map(mapRow),
      expiredCount: expiredResult.rows[0]?.n || 0,
    };
  }

  async upsertRecords(records = [], { dbClient = null } = {}) {
    const q = dbClient || this.db;
    let written = 0;
    for (const rec of records) {
      const result = await q.query(
        `INSERT INTO max_conversation_memory (
          id, client_id, conversation_id, actor_id, actor_role,
          entity_type, entity_id, semantic_type, payload,
          source_input_id, source_situation_model_id, record_fingerprint,
          confidence, occurred_at, expires_at, last_referenced_at
        ) VALUES (
          COALESCE($1::uuid, gen_random_uuid()), $2, $3, $4, $5,
          $6, $7, $8, $9::jsonb,
          $10, $11, $12,
          $13, $14::timestamptz, $15::timestamptz, $16::timestamptz
        )
        ON CONFLICT (client_id, conversation_id, record_fingerprint) DO UPDATE SET
          payload = EXCLUDED.payload,
          last_referenced_at = EXCLUDED.last_referenced_at,
          expires_at = EXCLUDED.expires_at,
          superseded_at = NULL
        RETURNING (xmax = 0) AS inserted`,
        [
          rec.id || null,
          rec.tenantId,
          rec.conversationId,
          rec.actorId != null ? String(rec.actorId) : null,
          rec.actorRole || null,
          rec.entityType || null,
          rec.entityId || null,
          rec.semanticType,
          JSON.stringify(rec.payload || {}),
          rec.sourceInputId || null,
          rec.sourceSituationModelId || null,
          rec.recordFingerprint,
          rec.confidence != null ? rec.confidence : null,
          rec.occurredAt || null,
          rec.expiresAt || null,
          rec.lastReferencedAt || new Date().toISOString(),
        ]
      );
      if (result.rows[0]?.inserted) written += 1;
    }
    return { written, duplicates: records.length - written };
  }

  async supersedeActive({ tenantId, conversationId, actorId = null, semanticTypes = [], bindingKeys = [] }, { dbClient = null } = {}) {
    const q = dbClient || this.db;
    const params = [tenantId, conversationId, new Date().toISOString()];
    let actorClause = '';
    if (actorId != null) {
      actorClause = ' AND (actor_id IS NULL OR actor_id = $4)';
      params.push(String(actorId));
    }
    let typeClause = '';
    if (semanticTypes.length) {
      params.push(semanticTypes);
      typeClause = ` AND semantic_type = ANY($${params.length}::text[])`;
    }
    let bindingClause = '';
    if (bindingKeys.length) {
      params.push(bindingKeys);
      bindingClause = ` AND payload->>'bindingKey' = ANY($${params.length}::text[])`;
    }
    const { rowCount } = await q.query(
      `UPDATE max_conversation_memory
       SET superseded_at = $3::timestamptz
       WHERE client_id = $1
         AND conversation_id = $2
         AND superseded_at IS NULL
         ${actorClause}${typeClause}${bindingClause}`,
      params
    );
    return rowCount || 0;
  }
}

function recordFingerprint(parts) {
  const raw = JSON.stringify(parts);
  return crypto.createHash('sha256').update(raw).digest('hex').slice(0, 48);
}

module.exports = {
  MemoryConversationMemoryRepository,
  PostgresConversationMemoryRepository,
  mapRow,
  recordFingerprint,
  SEMANTIC_TYPE,
};
