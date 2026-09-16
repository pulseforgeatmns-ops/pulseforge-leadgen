'use strict';

const crypto = require('crypto');
const {
  APPROVAL_STATES,
  buildSocialContentArtifact,
} = require('./types');

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function newId() {
  return crypto.randomUUID();
}

function createInMemorySocialContentStore() {
  /** @type {Map<string, object>} */
  const artifacts = new Map();

  return {
    kind: 'memory',
    async ensureSchema() {},
    async insertBatch(rows, { client } = {}) {
      void client;
      const inserted = [];
      for (const row of rows) {
        const artifact = buildSocialContentArtifact(row);
        artifacts.set(artifact.id, clone(artifact));
        inserted.push(clone(artifact));
      }
      return inserted;
    },
    async listByTenant(tenantId, clientId) {
      return [...artifacts.values()]
        .filter((a) => a.tenantId === String(tenantId) && a.clientId === Number(clientId))
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
    platform: row.platform,
    contentObjective: row.content_objective,
    missionId: row.mission_id,
    companyName: row.company_name,
    contentType: row.content_type,
    label: row.label,
    body: row.body,
    meta: row.meta || {},
    provenance: row.provenance || {},
    approvalState: row.approval_state,
    pendingCommentId: row.pending_comment_id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
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
    CREATE INDEX IF NOT EXISTS paige_social_content_artifacts_tenant_client_idx
      ON paige_social_content_artifacts (tenant_id, client_id, created_at DESC);
  `);
}

function createPostgresSocialContentStore(pool) {
  const db = pool;
  return {
    kind: 'postgres',
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
            id, tenant_id, client_id, platform, content_objective, mission_id,
            company_name, content_type, label, body, meta, provenance,
            approval_state, pending_comment_id, created_at, updated_at
          ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb,$13,$14,$15,$16
          ) RETURNING *`,
          [
            artifact.id,
            artifact.tenantId,
            artifact.clientId,
            artifact.platform,
            artifact.contentObjective,
            artifact.missionId,
            artifact.companyName,
            artifact.contentType,
            artifact.label,
            artifact.body,
            JSON.stringify(artifact.meta || {}),
            JSON.stringify(artifact.provenance || {}),
            artifact.approvalState || APPROVAL_STATES.PENDING_APPROVAL,
            artifact.pendingCommentId,
            artifact.createdAt,
            artifact.updatedAt,
          ]
        );
        inserted.push(mapRow(result.rows[0]));
      }
      return inserted;
    },
    async listByTenant(tenantId, clientId) {
      const result = await db.query(
        `SELECT * FROM paige_social_content_artifacts
         WHERE tenant_id = $1 AND client_id = $2
         ORDER BY created_at DESC`,
        [String(tenantId), Number(clientId)]
      );
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
    async attachPendingCommentId(id, tenantId, clientId, pendingCommentId) {
      const result = await db.query(
        `UPDATE paige_social_content_artifacts
         SET pending_comment_id = $4, updated_at = NOW()
         WHERE id = $1 AND tenant_id = $2 AND client_id = $3
         RETURNING *`,
        [id, String(tenantId), Number(clientId), pendingCommentId]
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
