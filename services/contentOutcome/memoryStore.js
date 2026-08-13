'use strict';

const crypto = require('crypto');
const { ContentOutcomeError } = require('./types');

function newId() {
  return crypto.randomUUID();
}

function clone(row) {
  return row == null ? null : JSON.parse(JSON.stringify(row));
}

/**
 * Process-scoped store for unit tests (mirrors Postgres semantics).
 */
function createMemoryContentOutcomeStore() {
  /** @type {Map<string, object>} */
  const artifacts = new Map();
  /** @type {Map<string, object>} */
  const publications = new Map();
  /** @type {Map<string, object>} */
  const snapshots = new Map();
  /** @type {Map<string, object>} */
  const outcomes = new Map();
  /** @type {Map<string, object>} */
  const signals = new Map();

  return {
    async ensureSchema() {
      return true;
    },

    async insertArtifact(row) {
      const id = row.id || newId();
      const now = new Date().toISOString();
      const record = {
        id,
        tenant_id: row.tenant_id,
        client_id: row.client_id,
        pending_comment_id: row.pending_comment_id ?? null,
        title: row.title ?? null,
        body: row.body ?? null,
        channel: row.channel ?? null,
        format: row.format ?? null,
        metadata: row.metadata || {},
        created_at: now,
        updated_at: now,
      };
      artifacts.set(id, record);
      return clone(record);
    },

    async getArtifact(tenantId, id) {
      const row = artifacts.get(String(id));
      if (!row || row.tenant_id !== String(tenantId)) return null;
      return clone(row);
    },

    async insertPublication(row) {
      const artifact = artifacts.get(String(row.content_artifact_id));
      if (!artifact || artifact.tenant_id !== String(row.tenant_id)) {
        throw new ContentOutcomeError('artifact_not_found', 'content artifact not found', 404);
      }
      const id = row.id || newId();
      const now = new Date().toISOString();
      const record = {
        id,
        tenant_id: row.tenant_id,
        client_id: row.client_id,
        content_artifact_id: row.content_artifact_id,
        channel: row.channel,
        external_post_id: row.external_post_id ?? null,
        external_url: row.external_url ?? null,
        published_at: row.published_at,
        objective: row.objective ?? null,
        topic: row.topic ?? null,
        thesis: row.thesis ?? null,
        format: row.format ?? null,
        intended_audience: row.intended_audience || [],
        campaign_id: row.campaign_id ?? null,
        linkedin_post_stats_id: row.linkedin_post_stats_id ?? null,
        created_at: now,
        updated_at: now,
      };
      publications.set(id, record);
      return clone(record);
    },

    async getPublication(tenantId, id) {
      const row = publications.get(String(id));
      if (!row || row.tenant_id !== String(tenantId)) return null;
      return clone(row);
    },

    async listPublications(tenantId, filters = {}) {
      let rows = [...publications.values()].filter(
        (r) => r.tenant_id === String(tenantId)
      );
      if (filters.channel) {
        rows = rows.filter((r) => r.channel === filters.channel);
      }
      if (filters.objective) {
        rows = rows.filter((r) => r.objective === filters.objective);
      }
      if (filters.topic) {
        rows = rows.filter((r) => r.topic === filters.topic);
      }
      if (filters.format) {
        rows = rows.filter((r) => r.format === filters.format);
      }
      if (filters.dateFrom) {
        const from = new Date(filters.dateFrom).getTime();
        rows = rows.filter((r) => new Date(r.published_at).getTime() >= from);
      }
      if (filters.dateTo) {
        const to = new Date(filters.dateTo).getTime();
        rows = rows.filter((r) => new Date(r.published_at).getTime() <= to);
      }
      rows.sort(
        (a, b) => new Date(b.published_at).getTime() - new Date(a.published_at).getTime()
      );
      const limit = filters.limit != null ? Number(filters.limit) : null;
      if (limit && limit > 0) rows = rows.slice(0, limit);
      return rows.map(clone);
    },

    async insertSnapshot(row) {
      const pub = publications.get(String(row.publication_id));
      if (!pub || pub.tenant_id !== String(row.tenant_id)) {
        throw new ContentOutcomeError('publication_not_found', 'publication not found', 404);
      }
      const id = row.id || newId();
      const record = {
        id,
        tenant_id: row.tenant_id,
        client_id: row.client_id,
        publication_id: row.publication_id,
        observed_at: row.observed_at,
        impressions: row.impressions ?? null,
        members_reached: row.members_reached ?? null,
        reactions: row.reactions ?? null,
        comments: row.comments ?? null,
        reposts: row.reposts ?? null,
        saves: row.saves ?? null,
        profile_views_attributed: row.profile_views_attributed ?? null,
        followers_gained: row.followers_gained ?? null,
        connection_requests: row.connection_requests ?? null,
        metadata: row.metadata || {},
        created_at: new Date().toISOString(),
      };
      snapshots.set(id, record);
      return clone(record);
    },

    async listSnapshots(tenantId, publicationId) {
      return [...snapshots.values()]
        .filter(
          (r) =>
            r.tenant_id === String(tenantId) &&
            r.publication_id === String(publicationId)
        )
        .sort(
          (a, b) =>
            new Date(a.observed_at).getTime() - new Date(b.observed_at).getTime()
        )
        .map(clone);
    },

    async listSnapshotsForPublications(tenantId, publicationIds) {
      const ids = new Set((publicationIds || []).map(String));
      return [...snapshots.values()]
        .filter((r) => r.tenant_id === String(tenantId) && ids.has(r.publication_id))
        .map(clone);
    },

    async insertBusinessOutcome(row) {
      const pub = publications.get(String(row.publication_id));
      if (!pub || pub.tenant_id !== String(row.tenant_id)) {
        throw new ContentOutcomeError('publication_not_found', 'publication not found', 404);
      }
      const id = row.id || newId();
      const record = {
        id,
        tenant_id: row.tenant_id,
        client_id: row.client_id,
        publication_id: row.publication_id,
        outcome_type: row.outcome_type,
        occurred_at: row.occurred_at,
        company_id: row.company_id ?? null,
        person_id: row.person_id ?? null,
        interaction_id: row.interaction_id ?? null,
        evidence_id: row.evidence_id ?? null,
        description: row.description ?? null,
        confidence: row.confidence ?? null,
        attribution: row.attribution || 'unknown',
        canonical_outcome_id: row.canonical_outcome_id ?? null,
        created_at: new Date().toISOString(),
      };
      outcomes.set(id, record);
      return clone(record);
    },

    async listBusinessOutcomes(tenantId, publicationId) {
      return [...outcomes.values()]
        .filter(
          (r) =>
            r.tenant_id === String(tenantId) &&
            r.publication_id === String(publicationId)
        )
        .sort(
          (a, b) =>
            new Date(a.occurred_at).getTime() - new Date(b.occurred_at).getTime()
        )
        .map(clone);
    },

    async listBusinessOutcomesForPublications(tenantId, publicationIds) {
      const ids = new Set((publicationIds || []).map(String));
      return [...outcomes.values()]
        .filter((r) => r.tenant_id === String(tenantId) && ids.has(r.publication_id))
        .map(clone);
    },

    async insertSignal(row) {
      const pub = publications.get(String(row.publication_id));
      if (!pub || pub.tenant_id !== String(row.tenant_id)) {
        throw new ContentOutcomeError('publication_not_found', 'publication not found', 404);
      }
      const id = row.id || newId();
      const record = {
        id,
        tenant_id: row.tenant_id,
        client_id: row.client_id,
        publication_id: row.publication_id,
        observed_at: row.observed_at,
        signal_type: row.signal_type,
        description: row.description,
        audience_type: row.audience_type ?? null,
        sentiment: row.sentiment ?? null,
        strength: row.strength ?? null,
        evidence_id: row.evidence_id ?? null,
        created_at: new Date().toISOString(),
      };
      signals.set(id, record);
      return clone(record);
    },

    async listSignals(tenantId, publicationId) {
      return [...signals.values()]
        .filter(
          (r) =>
            r.tenant_id === String(tenantId) &&
            r.publication_id === String(publicationId)
        )
        .sort(
          (a, b) =>
            new Date(a.observed_at).getTime() - new Date(b.observed_at).getTime()
        )
        .map(clone);
    },

    async listSignalsForPublications(tenantId, publicationIds) {
      const ids = new Set((publicationIds || []).map(String));
      return [...signals.values()]
        .filter((r) => r.tenant_id === String(tenantId) && ids.has(r.publication_id))
        .map(clone);
    },
  };
}

module.exports = {
  createMemoryContentOutcomeStore,
};
