'use strict';

const crypto = require('crypto');
const {
  APPROVAL_STATES,
  PUBLISH_STATES,
  buildSocialContentArtifact,
} = require('./types');

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function newId() {
  return crypto.randomUUID();
}

function applyArtifactPatch(row, patch = {}) {
  const now = new Date().toISOString();
  if (patch.approvalBinding !== undefined) row.approvalBinding = clone(patch.approvalBinding);
  if (patch.publication !== undefined) row.publication = clone(patch.publication);
  if (patch.approvalState != null) row.approvalState = patch.approvalState;
  if (patch.publishState != null) row.publishState = patch.publishState;
  if (patch.rejectionReason !== undefined) row.rejectionReason = patch.rejectionReason;
  if (patch.publishError !== undefined) row.publishError = patch.publishError;
  if (patch.publishedUrl !== undefined) row.publishedUrl = patch.publishedUrl;
  if (patch.approvedAt !== undefined) row.approvedAt = patch.approvedAt;
  if (patch.rejectedAt !== undefined) row.rejectedAt = patch.rejectedAt;
  if (patch.publishedAt !== undefined) row.publishedAt = patch.publishedAt;
  if (patch.sourceType != null) row.sourceType = patch.sourceType;
  if (patch.businessId !== undefined) row.businessId = patch.businessId;
  if (patch.mediaRefs !== undefined) row.mediaRefs = patch.mediaRefs;
  row.updatedAt = now;
  return row;
}

function createInMemorySocialContentStore() {
  /** @type {Map<string, object>} */
  const artifacts = new Map();
  const locks = new Map();

  return {
    kind: 'memory',
    async withLockedArtifact(id, tenantId, clientId, fn) {
      const previous = locks.get(id) || Promise.resolve();
      let release;
      const current = new Promise(resolve => { release = resolve; });
      locks.set(id, current);
      await previous;
      try {
        const artifact = await this.getById(id, tenantId, clientId);
        if (!artifact) throw new Error('artifact_not_found');
        return await fn(artifact, this);
      } finally { release(); if (locks.get(id) === current) locks.delete(id); }
    },
    async ensureSchema() {},
    async insertBatch(rows, { client } = {}) {
      void client;
      const inserted = [];
      for (const row of rows) {
        const artifact = buildSocialContentArtifact({ ...row, id: row.id || newId() });
        artifacts.set(artifact.id, clone(artifact));
        inserted.push(clone(artifact));
      }
      return inserted;
    },
    async listByTenant(tenantId, clientId, { missionId } = {}) {
      return [...artifacts.values()]
        .filter((a) => {
          if (a.tenantId !== String(tenantId) || a.clientId !== Number(clientId)) return false;
          if (missionId && a.missionId !== missionId) return false;
          return true;
        })
        .map(clone);
    },
    async getById(id, tenantId, clientId) {
      const row = artifacts.get(id);
      if (!row) return null;
      if (row.tenantId !== String(tenantId) || row.clientId !== Number(clientId)) return null;
      return clone(row);
    },
    async attachPendingCommentId(id, tenantId, clientId, pendingCommentId) {
      const row = artifacts.get(id);
      if (!row) return null;
      if (row.tenantId !== String(tenantId) || row.clientId !== Number(clientId)) return null;
      row.pendingCommentId = pendingCommentId;
      row.updatedAt = new Date().toISOString();
      return clone(row);
    },
    async getByPendingCommentId(pendingCommentId, tenantId, clientId) {
      const row = [...artifacts.values()].find(
        (a) =>
          a.pendingCommentId === pendingCommentId &&
          a.tenantId === String(tenantId) &&
          a.clientId === Number(clientId)
      );
      return row ? clone(row) : null;
    },
    async transitionApprovalState(id, tenantId, clientId, toState, { allowedFrom = [] } = {}) {
      const row = artifacts.get(id);
      if (!row) return { ok: false, reason: 'artifact_not_found' };
      if (row.tenantId !== String(tenantId) || row.clientId !== Number(clientId)) {
        return { ok: false, reason: 'tenant_scope_mismatch' };
      }
      if (!allowedFrom.includes(row.approvalState)) {
        return { ok: false, reason: 'invalid_transition', from: row.approvalState, to: toState };
      }
      row.approvalState = toState;
      row.updatedAt = new Date().toISOString();
      return { ok: true, artifact: clone(row) };
    },
    async transitionPublishState(id, tenantId, clientId, toState, { allowedFrom = [] } = {}) {
      const row = artifacts.get(id);
      if (!row) return { ok: false, reason: 'artifact_not_found' };
      if (row.tenantId !== String(tenantId) || row.clientId !== Number(clientId)) {
        return { ok: false, reason: 'tenant_scope_mismatch' };
      }
      if (!allowedFrom.includes(row.publishState)) {
        return { ok: false, reason: 'invalid_publish_transition', from: row.publishState, to: toState };
      }
      row.publishState = toState;
      row.updatedAt = new Date().toISOString();
      return { ok: true, artifact: clone(row) };
    },
    async updateArtifactMetadata(id, tenantId, clientId, patch = {}) {
      const row = artifacts.get(id);
      if (!row) return null;
      if (row.tenantId !== String(tenantId) || row.clientId !== Number(clientId)) return null;
      applyArtifactPatch(row, patch);
      return clone(row);
    },
    _resetForTests() {
      artifacts.clear();
    },
  };
}

function mapRow(row) {
  if (!row) return null;
  return buildSocialContentArtifact({
    id: row.id,
    tenantId: row.tenant_id,
    clientId: row.client_id,
    sourceType: row.source_type,
    businessId: row.business_id,
    platform: row.platform,
    contentObjective: row.content_objective,
    missionId: row.mission_id,
    companyName: row.company_name,
    contentType: row.content_type,
    label: row.label,
    body: row.body,
    mediaRefs: row.media_refs,
    meta: row.meta || {},
    provenance: row.provenance || {},
    approvalState: row.approval_state,
    approvalBinding: row.approval_binding,
    publication: row.publication,
    publishState: row.publish_state,
    rejectionReason: row.rejection_reason,
    publishError: row.publish_error,
    publishedUrl: row.published_url,
    pendingCommentId: row.pending_comment_id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    approvedAt: row.approved_at,
    rejectedAt: row.rejected_at,
    publishedAt: row.published_at,
  });
}

async function ensureSocialContentArtifactsTable(db) {
  await db.query(`
    CREATE TABLE IF NOT EXISTS paige_social_content_artifacts (
      id UUID PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      client_id INTEGER NOT NULL,
      platform TEXT NOT NULL,
      content_objective TEXT,
      mission_id TEXT,
      company_name TEXT,
      content_type TEXT,
      label TEXT NOT NULL,
      body TEXT NOT NULL,
      meta JSONB NOT NULL DEFAULT '{}'::jsonb,
      provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
      approval_state TEXT NOT NULL DEFAULT 'PENDING_APPROVAL',
      pending_comment_id UUID,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT paige_social_content_artifacts_approval_state_check
        CHECK (approval_state IN ('DRAFT', 'PENDING_APPROVAL', 'APPROVED', 'REJECTED', 'PUBLISHED'))
    );
  `);

  await db.query(`
    ALTER TABLE paige_social_content_artifacts
      ADD COLUMN IF NOT EXISTS approval_binding JSONB,
      ADD COLUMN IF NOT EXISTS publication JSONB NOT NULL DEFAULT '{}'::jsonb,
      ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'unknown',
      ADD COLUMN IF NOT EXISTS business_id TEXT,
      ADD COLUMN IF NOT EXISTS media_refs JSONB,
      ADD COLUMN IF NOT EXISTS publish_state TEXT NOT NULL DEFAULT 'NOT_PUBLISHED',
      ADD COLUMN IF NOT EXISTS rejection_reason TEXT,
      ADD COLUMN IF NOT EXISTS publish_error TEXT,
      ADD COLUMN IF NOT EXISTS published_url TEXT,
      ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS rejected_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;
  `);

  await db.query(`
    UPDATE paige_social_content_artifacts
       SET approval_state = 'APPROVED',
           publish_state = 'PUBLISHED',
           published_at = COALESCE(published_at, updated_at)
     WHERE approval_state = 'PUBLISHED';
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS paige_social_content_artifacts_tenant_client_idx
      ON paige_social_content_artifacts (tenant_id, client_id, created_at DESC);
  `);
}

function createPostgresSocialContentStore(pool) {
  const db = pool;
  return {
    kind: 'postgres',
    async withLockedArtifact(id, tenantId, clientId, fn) {
      const conn = await pool.connect();
      try {
        await conn.query('BEGIN');
        const result = await conn.query('SELECT * FROM paige_social_content_artifacts WHERE id=$1 AND tenant_id=$2 AND client_id=$3 FOR UPDATE', [id, String(tenantId), Number(clientId)]);
        if (!result.rows[0]) throw new Error('artifact_not_found');
        const output = await fn(mapRow(result.rows[0]), createPostgresSocialContentStore(conn));
        await conn.query('COMMIT');
        return output;
      } catch (err) { await conn.query('ROLLBACK'); throw err; }
      finally { conn.release(); }
    },
    ensureSchema(client) {
      return ensureSocialContentArtifactsTable(client || db);
    },
    async insertBatch(rows, { client: txClient } = {}) {
      const conn = txClient || db;
      const inserted = [];
      for (const row of rows) {
        const artifact = buildSocialContentArtifact({ ...row, id: row.id || newId() });
        const result = await conn.query(
          `INSERT INTO paige_social_content_artifacts (
            id, tenant_id, client_id, source_type, business_id, platform, content_objective, mission_id,
            company_name, content_type, label, body, media_refs, meta, provenance,
            approval_state, publish_state, pending_comment_id, created_at, updated_at
          ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15::jsonb,$16,$17,$18,$19,$20
          ) RETURNING *`,
          [
            artifact.id,
            artifact.tenantId,
            artifact.clientId,
            artifact.sourceType,
            artifact.businessId,
            artifact.platform,
            artifact.contentObjective,
            artifact.missionId,
            artifact.companyName,
            artifact.contentType,
            artifact.label,
            artifact.body,
            artifact.mediaRefs ? JSON.stringify(artifact.mediaRefs) : null,
            JSON.stringify(artifact.meta || {}),
            JSON.stringify(artifact.provenance || {}),
            artifact.approvalState || APPROVAL_STATES.PENDING_APPROVAL,
            artifact.publishState || PUBLISH_STATES.NOT_PUBLISHED,
            artifact.pendingCommentId,
            artifact.createdAt,
            artifact.updatedAt,
          ]
        );
        inserted.push(mapRow(result.rows[0]));
      }
      return inserted;
    },
    async listByTenant(tenantId, clientId, { missionId } = {}) {
      const params = [String(tenantId), Number(clientId)];
      let sql = `SELECT * FROM paige_social_content_artifacts
         WHERE tenant_id = $1 AND client_id = $2`;
      if (missionId) {
        params.push(missionId);
        sql += ` AND mission_id = $${params.length}`;
      }
      sql += ' ORDER BY created_at DESC';
      const result = await db.query(sql, params);
      return result.rows.map(mapRow);
    },
    async getById(id, tenantId, clientId) {
      const result = await db.query(
        `SELECT * FROM paige_social_content_artifacts
         WHERE id = $1 AND tenant_id = $2 AND client_id = $3`,
        [id, String(tenantId), Number(clientId)]
      );
      return mapRow(result.rows[0]);
    },
    async attachPendingCommentId(id, tenantId, clientId, pendingCommentId, { client: txClient } = {}) {
      const result = await (txClient || db).query(
        `UPDATE paige_social_content_artifacts
         SET pending_comment_id = $4, updated_at = NOW()
         WHERE id = $1 AND tenant_id = $2 AND client_id = $3
         RETURNING *`,
        [id, String(tenantId), Number(clientId), pendingCommentId]
      );
      return mapRow(result.rows[0]);
    },
    async getByPendingCommentId(pendingCommentId, tenantId, clientId) {
      const result = await db.query(
        `SELECT * FROM paige_social_content_artifacts
         WHERE pending_comment_id = $1 AND tenant_id = $2 AND client_id = $3
         LIMIT 1`,
        [pendingCommentId, String(tenantId), Number(clientId)]
      );
      return mapRow(result.rows[0]);
    },
    async transitionApprovalState(id, tenantId, clientId, toState, { allowedFrom = [], client: txClient } = {}) {
      const conn = txClient || db;
      const result = await conn.query(
        `UPDATE paige_social_content_artifacts
         SET approval_state = $4, updated_at = NOW()
         WHERE id = $1 AND tenant_id = $2 AND client_id = $3
           AND approval_state = ANY($5::text[])
         RETURNING *`,
        [id, String(tenantId), Number(clientId), toState, allowedFrom]
      );
      if (!result.rows[0]) {
        const existing = await this.getById(id, tenantId, clientId);
        if (!existing) return { ok: false, reason: 'artifact_not_found' };
        return {
          ok: false,
          reason: 'invalid_transition',
          from: existing.approvalState,
          to: toState,
        };
      }
      return { ok: true, artifact: mapRow(result.rows[0]) };
    },
    async transitionPublishState(id, tenantId, clientId, toState, { allowedFrom = [], client: txClient } = {}) {
      const conn = txClient || db;
      const result = await conn.query(
        `UPDATE paige_social_content_artifacts
         SET publish_state = $4, updated_at = NOW()
         WHERE id = $1 AND tenant_id = $2 AND client_id = $3
           AND publish_state = ANY($5::text[])
         RETURNING *`,
        [id, String(tenantId), Number(clientId), toState, allowedFrom]
      );
      if (!result.rows[0]) {
        const existing = await this.getById(id, tenantId, clientId);
        if (!existing) return { ok: false, reason: 'artifact_not_found' };
        return {
          ok: false,
          reason: 'invalid_publish_transition',
          from: existing.publishState,
          to: toState,
        };
      }
      return { ok: true, artifact: mapRow(result.rows[0]) };
    },
    async updateArtifactMetadata(id, tenantId, clientId, patch = {}, { client: txClient } = {}) {
      const conn = txClient || db;
      const sets = [];
      const params = [id, String(tenantId), Number(clientId)];
      let idx = 4;

      const fieldMap = [
        ['approvalState', 'approval_state'],
        ['publishState', 'publish_state'],
        ['rejectionReason', 'rejection_reason'],
        ['publishError', 'publish_error'],
        ['publishedUrl', 'published_url'],
        ['approvedAt', 'approved_at'],
        ['rejectedAt', 'rejected_at'],
        ['publishedAt', 'published_at'],
        ['sourceType', 'source_type'],
        ['businessId', 'business_id'],
      ];
      for (const [jsKey, col] of fieldMap) {
        if (patch[jsKey] !== undefined) {
          sets.push(`${col} = $${idx++}`);
          params.push(patch[jsKey]);
        }
      }
      for (const [key, column] of [['approvalBinding', 'approval_binding'], ['publication', 'publication']]) {
        if (patch[key] !== undefined) { sets.push(`${column} = $${idx++}::jsonb`); params.push(JSON.stringify(patch[key])); }
      }
      if (patch.mediaRefs !== undefined) {
        sets.push(`media_refs = $${idx++}::jsonb`);
        params.push(patch.mediaRefs ? JSON.stringify(patch.mediaRefs) : null);
      }
      if (!sets.length) {
        return this.getById(id, tenantId, clientId);
      }
      sets.push('updated_at = NOW()');
      const result = await conn.query(
        `UPDATE paige_social_content_artifacts
         SET ${sets.join(', ')}
         WHERE id = $1 AND tenant_id = $2 AND client_id = $3
         RETURNING *`,
        params
      );
      return mapRow(result.rows[0]);
    },
  };
}

module.exports = {
  createInMemorySocialContentStore,
  createPostgresSocialContentStore,
  ensureSocialContentArtifactsTable,
};
