'use strict';

const fs = require('fs');
const path = require('path');
const { ContentOutcomeError } = require('./types');

const MIGRATION_PATH = path.join(
  __dirname,
  '..',
  '..',
  'migrations',
  '2026-08-13-content-outcome-intelligence.sql'
);

/**
 * @param {import('pg').Pool} pool
 */
function createPostgresContentOutcomeStore(pool) {
  if (!pool) throw new Error('pool required');
  let schemaReady = false;

  async function ensureSchema() {
    if (schemaReady) return true;
    const sql = fs
      .readFileSync(MIGRATION_PATH, 'utf8')
      .replace(/^\s*BEGIN;\s*$/gim, '')
      .replace(/^\s*COMMIT;\s*$/gim, '');
    await pool.query(sql);
    schemaReady = true;
    return true;
  }

  return {
    ensureSchema,

    async insertArtifact(row) {
      await ensureSchema();
      const result = await pool.query(
        `INSERT INTO content_artifacts (
           tenant_id, client_id, pending_comment_id, title, body, channel, format, metadata
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
         RETURNING *`,
        [
          row.tenant_id,
          row.client_id,
          row.pending_comment_id ?? null,
          row.title ?? null,
          row.body ?? null,
          row.channel ?? null,
          row.format ?? null,
          JSON.stringify(row.metadata || {}),
        ]
      );
      return result.rows[0];
    },

    async getArtifact(tenantId, id) {
      await ensureSchema();
      const result = await pool.query(
        `SELECT * FROM content_artifacts WHERE id = $1 AND tenant_id = $2`,
        [id, String(tenantId)]
      );
      return result.rows[0] || null;
    },

    async insertPublication(row) {
      await ensureSchema();
      const artifact = await pool.query(
        `SELECT id FROM content_artifacts WHERE id = $1 AND tenant_id = $2`,
        [row.content_artifact_id, row.tenant_id]
      );
      if (!artifact.rows.length) {
        throw new ContentOutcomeError('artifact_not_found', 'content artifact not found', 404);
      }
      const result = await pool.query(
        `INSERT INTO content_publications (
           tenant_id, client_id, content_artifact_id, channel, external_post_id, external_url,
           published_at, objective, topic, thesis, format, intended_audience, campaign_id,
           linkedin_post_stats_id
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
         RETURNING *`,
        [
          row.tenant_id,
          row.client_id,
          row.content_artifact_id,
          row.channel,
          row.external_post_id ?? null,
          row.external_url ?? null,
          row.published_at,
          row.objective ?? null,
          row.topic ?? null,
          row.thesis ?? null,
          row.format ?? null,
          row.intended_audience || [],
          row.campaign_id ?? null,
          row.linkedin_post_stats_id ?? null,
        ]
      );
      return result.rows[0];
    },

    async getPublication(tenantId, id) {
      await ensureSchema();
      const result = await pool.query(
        `SELECT * FROM content_publications WHERE id = $1 AND tenant_id = $2`,
        [id, String(tenantId)]
      );
      return result.rows[0] || null;
    },

    async listPublications(tenantId, filters = {}) {
      await ensureSchema();
      const clauses = ['tenant_id = $1'];
      const params = [String(tenantId)];
      let n = 2;

      if (filters.channel) {
        clauses.push(`channel = $${n++}`);
        params.push(filters.channel);
      }
      if (filters.objective) {
        clauses.push(`objective = $${n++}`);
        params.push(filters.objective);
      }
      if (filters.topic) {
        clauses.push(`topic = $${n++}`);
        params.push(filters.topic);
      }
      if (filters.format) {
        clauses.push(`format = $${n++}`);
        params.push(filters.format);
      }
      if (filters.dateFrom) {
        clauses.push(`published_at >= $${n++}`);
        params.push(filters.dateFrom);
      }
      if (filters.dateTo) {
        clauses.push(`published_at <= $${n++}`);
        params.push(filters.dateTo);
      }

      let sql = `SELECT * FROM content_publications WHERE ${clauses.join(' AND ')}
                 ORDER BY published_at DESC`;
      if (filters.limit != null && Number(filters.limit) > 0) {
        sql += ` LIMIT $${n++}`;
        params.push(Number(filters.limit));
      }
      const result = await pool.query(sql, params);
      return result.rows;
    },

    async insertSnapshot(row) {
      await ensureSchema();
      const pub = await pool.query(
        `SELECT id FROM content_publications WHERE id = $1 AND tenant_id = $2`,
        [row.publication_id, row.tenant_id]
      );
      if (!pub.rows.length) {
        throw new ContentOutcomeError('publication_not_found', 'publication not found', 404);
      }
      const result = await pool.query(
        `INSERT INTO content_performance_snapshots (
           tenant_id, client_id, publication_id, observed_at,
           impressions, members_reached, reactions, comments, reposts, saves,
           profile_views_attributed, followers_gained, connection_requests, metadata
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb)
         RETURNING *`,
        [
          row.tenant_id,
          row.client_id,
          row.publication_id,
          row.observed_at,
          row.impressions ?? null,
          row.members_reached ?? null,
          row.reactions ?? null,
          row.comments ?? null,
          row.reposts ?? null,
          row.saves ?? null,
          row.profile_views_attributed ?? null,
          row.followers_gained ?? null,
          row.connection_requests ?? null,
          JSON.stringify(row.metadata || {}),
        ]
      );
      return result.rows[0];
    },

    async listSnapshots(tenantId, publicationId) {
      await ensureSchema();
      const result = await pool.query(
        `SELECT * FROM content_performance_snapshots
         WHERE tenant_id = $1 AND publication_id = $2
         ORDER BY observed_at ASC, created_at ASC`,
        [String(tenantId), publicationId]
      );
      return result.rows;
    },

    async listSnapshotsForPublications(tenantId, publicationIds) {
      await ensureSchema();
      if (!publicationIds || !publicationIds.length) return [];
      const result = await pool.query(
        `SELECT * FROM content_performance_snapshots
         WHERE tenant_id = $1 AND publication_id = ANY($2::uuid[])
         ORDER BY observed_at ASC`,
        [String(tenantId), publicationIds]
      );
      return result.rows;
    },

    async insertBusinessOutcome(row) {
      await ensureSchema();
      const pub = await pool.query(
        `SELECT id FROM content_publications WHERE id = $1 AND tenant_id = $2`,
        [row.publication_id, row.tenant_id]
      );
      if (!pub.rows.length) {
        throw new ContentOutcomeError('publication_not_found', 'publication not found', 404);
      }
      const result = await pool.query(
        `INSERT INTO content_business_outcomes (
           tenant_id, client_id, publication_id, outcome_type, occurred_at,
           company_id, person_id, interaction_id, evidence_id,
           description, confidence, attribution, canonical_outcome_id
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
         RETURNING *`,
        [
          row.tenant_id,
          row.client_id,
          row.publication_id,
          row.outcome_type,
          row.occurred_at,
          row.company_id ?? null,
          row.person_id ?? null,
          row.interaction_id ?? null,
          row.evidence_id ?? null,
          row.description ?? null,
          row.confidence ?? null,
          row.attribution || 'unknown',
          row.canonical_outcome_id ?? null,
        ]
      );
      return result.rows[0];
    },

    async listBusinessOutcomes(tenantId, publicationId) {
      await ensureSchema();
      const result = await pool.query(
        `SELECT * FROM content_business_outcomes
         WHERE tenant_id = $1 AND publication_id = $2
         ORDER BY occurred_at ASC, created_at ASC`,
        [String(tenantId), publicationId]
      );
      return result.rows;
    },

    async listBusinessOutcomesForPublications(tenantId, publicationIds) {
      await ensureSchema();
      if (!publicationIds || !publicationIds.length) return [];
      const result = await pool.query(
        `SELECT * FROM content_business_outcomes
         WHERE tenant_id = $1 AND publication_id = ANY($2::uuid[])
         ORDER BY occurred_at ASC`,
        [String(tenantId), publicationIds]
      );
      return result.rows;
    },

    async insertSignal(row) {
      await ensureSchema();
      const pub = await pool.query(
        `SELECT id FROM content_publications WHERE id = $1 AND tenant_id = $2`,
        [row.publication_id, row.tenant_id]
      );
      if (!pub.rows.length) {
        throw new ContentOutcomeError('publication_not_found', 'publication not found', 404);
      }
      const result = await pool.query(
        `INSERT INTO content_qualitative_signals (
           tenant_id, client_id, publication_id, observed_at, signal_type, description,
           audience_type, sentiment, strength, evidence_id
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         RETURNING *`,
        [
          row.tenant_id,
          row.client_id,
          row.publication_id,
          row.observed_at,
          row.signal_type,
          row.description,
          row.audience_type ?? null,
          row.sentiment ?? null,
          row.strength ?? null,
          row.evidence_id ?? null,
        ]
      );
      return result.rows[0];
    },

    async listSignals(tenantId, publicationId) {
      await ensureSchema();
      const result = await pool.query(
        `SELECT * FROM content_qualitative_signals
         WHERE tenant_id = $1 AND publication_id = $2
         ORDER BY observed_at ASC, created_at ASC`,
        [String(tenantId), publicationId]
      );
      return result.rows;
    },

    async listSignalsForPublications(tenantId, publicationIds) {
      await ensureSchema();
      if (!publicationIds || !publicationIds.length) return [];
      const result = await pool.query(
        `SELECT * FROM content_qualitative_signals
         WHERE tenant_id = $1 AND publication_id = ANY($2::uuid[])
         ORDER BY observed_at ASC`,
        [String(tenantId), publicationIds]
      );
      return result.rows;
    },
  };
}

module.exports = {
  createPostgresContentOutcomeStore,
};
