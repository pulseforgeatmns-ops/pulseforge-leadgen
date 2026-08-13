'use strict';

/**
 * SPEC-092 — Content Outcome Intelligence (v1 thin slice).
 * Product brief used "SPEC-085"; repository SPEC-085 is Executive Business Brief.
 *
 * Records: ContentArtifact → ContentPublication → Performance / Business /
 * Qualitative signals. Extends Outcome Intelligence (SPEC-013 / SPEC-036).
 * Manual capture only — no LinkedIn API, no Paige strategy mutation.
 */

const crypto = require('crypto');

const CHANNELS = Object.freeze([
  'linkedin',
  'facebook',
  'gbp',
  'instagram',
  'blog',
  'other',
]);

const OBJECTIVES = Object.freeze([
  'awareness',
  'category_creation',
  'audience_growth',
  'engagement',
  'thought_leadership',
  'lead_generation',
  'partnership_generation',
  'launch_runway',
]);

const BUSINESS_OUTCOME_TYPES = Object.freeze([
  'qualified_dm',
  'prospect_conversation',
  'partner_conversation',
  'builder_connection',
  'demo_interest',
  'meeting_booked',
  'pilot_interest',
  'customer_opportunity',
  'other',
]);

const ATTRIBUTION_LEVELS = Object.freeze([
  'direct',
  'likely',
  'possible',
  'unknown',
]);

const SIGNAL_TYPES = Object.freeze([
  'message_resonance',
  'audience_signal',
  'objection',
  'question',
  'language_adoption',
  'partnership_signal',
  'buyer_signal',
  'technical_interest',
  'unexpected_response',
  'other',
]);

const METRIC_FIELDS = Object.freeze([
  'impressions',
  'members_reached',
  'reactions',
  'comments',
  'reposts',
  'saves',
  'profile_views_attributed',
  'followers_gained',
  'connection_requests',
]);

class ContentOutcomeError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {number} [status]
   */
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'ContentOutcomeError';
    this.code = code;
    this.status = status;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function newId() {
  return crypto.randomUUID();
}

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function asClientId(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function tenantForClient(clientId) {
  return String(clientId);
}

function parseTimestamp(value, fieldName) {
  if (value == null || value === '') return new Date();
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) {
    throw new ContentOutcomeError(
      'invalid_timestamp',
      `${fieldName} must be a valid ISO timestamp`
    );
  }
  return d;
}

function optionalNonNegInt(value, fieldName) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0) {
    throw new ContentOutcomeError(
      'invalid_metric',
      `${fieldName} must be a non-negative integer when provided`
    );
  }
  return n;
}

function optionalConfidence(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0 || n > 1) {
    throw new ContentOutcomeError(
      'invalid_confidence',
      'confidence must be between 0 and 1 when provided'
    );
  }
  return n;
}

function assertEnum(value, allowed, fieldName, code) {
  const v = asText(value);
  if (!v || !allowed.includes(v)) {
    throw new ContentOutcomeError(
      code || `invalid_${fieldName}`,
      `${fieldName} must be one of: ${allowed.join(', ')}`
    );
  }
  return v;
}

function optionalEnum(value, allowed, fieldName) {
  const v = asText(value);
  if (!v) return null;
  return assertEnum(v, allowed, fieldName);
}

function asAudience(value) {
  if (value == null) return [];
  if (Array.isArray(value)) {
    return value.map((x) => String(x).trim()).filter(Boolean);
  }
  if (typeof value === 'string') {
    return value
      .split(',')
      .map((x) => x.trim())
      .filter(Boolean);
  }
  return [];
}

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function median(nums) {
  if (!nums.length) return null;
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

function average(nums) {
  if (!nums.length) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function latestSnapshotMetrics(snapshots) {
  if (!snapshots || !snapshots.length) return null;
  const sorted = [...snapshots].sort(
    (a, b) => new Date(a.observedAt) - new Date(b.observedAt)
  );
  return sorted[sorted.length - 1];
}

function countOutcomes(outcomes, type) {
  return (outcomes || []).filter((o) => o.outcomeType === type).length;
}

/**
 * In-memory store for unit tests / process-local fallback.
 */
function createMemoryStore() {
  /** @type {Map<string, object>} */
  const publications = new Map();
  /** @type {Map<string, object[]>} */
  const snapshots = new Map();
  /** @type {Map<string, object[]>} */
  const outcomes = new Map();
  /** @type {Map<string, object[]>} */
  const signals = new Map();

  return {
    kind: 'memory',
    async insertPublication(row) {
      publications.set(row.id, clone(row));
      snapshots.set(row.id, []);
      outcomes.set(row.id, []);
      signals.set(row.id, []);
      return clone(row);
    },
    async getPublication(id, clientId) {
      const row = publications.get(id);
      if (!row) return null;
      if (clientId != null && row.clientId !== clientId) return null;
      return clone(row);
    },
    async listPublications(filter) {
      let rows = [...publications.values()];
      if (filter.clientId != null) {
        rows = rows.filter((r) => r.clientId === filter.clientId);
      }
      if (filter.channel) {
        rows = rows.filter((r) => r.channel === filter.channel);
      }
      if (filter.objective) {
        rows = rows.filter((r) => r.objective === filter.objective);
      }
      if (filter.topic) {
        rows = rows.filter(
          (r) =>
            r.topic &&
            String(r.topic).toLowerCase().includes(String(filter.topic).toLowerCase())
        );
      }
      if (filter.format) {
        rows = rows.filter((r) => r.format === filter.format);
      }
      if (filter.intendedAudience) {
        const want = String(filter.intendedAudience).toLowerCase();
        rows = rows.filter((r) =>
          (r.intendedAudience || []).some((a) => String(a).toLowerCase() === want)
        );
      }
      if (filter.from) {
        const from = new Date(filter.from).getTime();
        rows = rows.filter((r) => new Date(r.publishedAt).getTime() >= from);
      }
      if (filter.to) {
        const to = new Date(filter.to).getTime();
        rows = rows.filter((r) => new Date(r.publishedAt).getTime() <= to);
      }
      rows.sort(
        (a, b) => new Date(b.publishedAt) - new Date(a.publishedAt)
      );
      if (filter.limit != null) rows = rows.slice(0, filter.limit);
      return rows.map(clone);
    },
    async insertSnapshot(row) {
      const list = snapshots.get(row.publicationId) || [];
      list.push(clone(row));
      snapshots.set(row.publicationId, list);
      return clone(row);
    },
    async listSnapshots(publicationId, clientId) {
      const pub = publications.get(publicationId);
      if (!pub) return [];
      if (clientId != null && pub.clientId !== clientId) return [];
      return (snapshots.get(publicationId) || [])
        .map(clone)
        .sort((a, b) => new Date(a.observedAt) - new Date(b.observedAt));
    },
    async insertOutcome(row) {
      const list = outcomes.get(row.publicationId) || [];
      list.push(clone(row));
      outcomes.set(row.publicationId, list);
      return clone(row);
    },
    async listOutcomes(publicationId, clientId) {
      const pub = publications.get(publicationId);
      if (!pub) return [];
      if (clientId != null && pub.clientId !== clientId) return [];
      return (outcomes.get(publicationId) || [])
        .map(clone)
        .sort((a, b) => new Date(a.occurredAt) - new Date(b.occurredAt));
    },
    async insertSignal(row) {
      const list = signals.get(row.publicationId) || [];
      list.push(clone(row));
      signals.set(row.publicationId, list);
      return clone(row);
    },
    async listSignals(publicationId, clientId) {
      const pub = publications.get(publicationId);
      if (!pub) return [];
      if (clientId != null && pub.clientId !== clientId) return [];
      return (signals.get(publicationId) || [])
        .map(clone)
        .sort((a, b) => new Date(a.observedAt) - new Date(b.observedAt));
    },
  };
}

function mapPublicationRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    clientId: row.client_id,
    tenantId: row.tenant_id,
    contentArtifactId: row.content_artifact_id,
    channel: row.channel,
    externalPostId: row.external_post_id,
    externalUrl: row.external_url,
    publishedAt: row.published_at instanceof Date
      ? row.published_at.toISOString()
      : String(row.published_at),
    objective: row.objective,
    topic: row.topic,
    thesis: row.thesis,
    format: row.format,
    intendedAudience: row.intended_audience || [],
    campaignId: row.campaign_id,
    title: row.title,
    createdAt: row.created_at instanceof Date
      ? row.created_at.toISOString()
      : String(row.created_at),
    updatedAt: row.updated_at instanceof Date
      ? row.updated_at.toISOString()
      : String(row.updated_at),
  };
}

function mapSnapshotRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    clientId: row.client_id,
    tenantId: row.tenant_id,
    publicationId: row.publication_id,
    observedAt: row.observed_at instanceof Date
      ? row.observed_at.toISOString()
      : String(row.observed_at),
    impressions: row.impressions,
    membersReached: row.members_reached,
    reactions: row.reactions,
    comments: row.comments,
    reposts: row.reposts,
    saves: row.saves,
    profileViewsAttributed: row.profile_views_attributed,
    followersGained: row.followers_gained,
    connectionRequests: row.connection_requests,
    metadata: row.metadata || {},
    createdAt: row.created_at instanceof Date
      ? row.created_at.toISOString()
      : String(row.created_at),
  };
}

function mapOutcomeRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    clientId: row.client_id,
    tenantId: row.tenant_id,
    publicationId: row.publication_id,
    outcomeType: row.outcome_type,
    occurredAt: row.occurred_at instanceof Date
      ? row.occurred_at.toISOString()
      : String(row.occurred_at),
    companyId: row.company_id,
    personId: row.person_id,
    interactionId: row.interaction_id,
    evidenceId: row.evidence_id,
    description: row.description,
    confidence: row.confidence == null ? null : Number(row.confidence),
    attribution: row.attribution,
    createdAt: row.created_at instanceof Date
      ? row.created_at.toISOString()
      : String(row.created_at),
  };
}

function mapSignalRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    clientId: row.client_id,
    tenantId: row.tenant_id,
    publicationId: row.publication_id,
    observedAt: row.observed_at instanceof Date
      ? row.observed_at.toISOString()
      : String(row.observed_at),
    signalType: row.signal_type,
    description: row.description,
    audienceType: row.audience_type,
    sentiment: row.sentiment,
    strength: row.strength,
    evidenceId: row.evidence_id,
    createdAt: row.created_at instanceof Date
      ? row.created_at.toISOString()
      : String(row.created_at),
  };
}

/**
 * @param {import('pg').Pool} [pool]
 */
function createPostgresStore(pool) {
  const db = pool || require('../db');
  return {
    kind: 'postgres',
    async insertPublication(row) {
      const result = await db.query(
        `INSERT INTO content_publications (
          id, client_id, tenant_id, content_artifact_id, channel,
          external_post_id, external_url, published_at, objective, topic,
          thesis, format, intended_audience, campaign_id, title,
          created_at, updated_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
        ) RETURNING *`,
        [
          row.id,
          row.clientId,
          row.tenantId,
          row.contentArtifactId,
          row.channel,
          row.externalPostId,
          row.externalUrl,
          row.publishedAt,
          row.objective,
          row.topic,
          row.thesis,
          row.format,
          row.intendedAudience,
          row.campaignId,
          row.title,
          row.createdAt,
          row.updatedAt,
        ]
      );
      return mapPublicationRow(result.rows[0]);
    },
    async getPublication(id, clientId) {
      const result = await db.query(
        `SELECT * FROM content_publications
         WHERE id = $1 AND ($2::int IS NULL OR client_id = $2)`,
        [id, clientId]
      );
      return mapPublicationRow(result.rows[0]);
    },
    async listPublications(filter) {
      const clauses = ['client_id = $1'];
      const params = [filter.clientId];
      let i = 2;
      if (filter.channel) {
        clauses.push(`channel = $${i++}`);
        params.push(filter.channel);
      }
      if (filter.objective) {
        clauses.push(`objective = $${i++}`);
        params.push(filter.objective);
      }
      if (filter.topic) {
        clauses.push(`topic ILIKE $${i++}`);
        params.push(`%${filter.topic}%`);
      }
      if (filter.format) {
        clauses.push(`format = $${i++}`);
        params.push(filter.format);
      }
      if (filter.intendedAudience) {
        clauses.push(`$${i++} = ANY(intended_audience)`);
        params.push(filter.intendedAudience);
      }
      if (filter.from) {
        clauses.push(`published_at >= $${i++}`);
        params.push(filter.from);
      }
      if (filter.to) {
        clauses.push(`published_at <= $${i++}`);
        params.push(filter.to);
      }
      let sql = `SELECT * FROM content_publications WHERE ${clauses.join(' AND ')}
                 ORDER BY published_at DESC`;
      if (filter.limit != null) {
        sql += ` LIMIT $${i++}`;
        params.push(filter.limit);
      }
      const result = await db.query(sql, params);
      return result.rows.map(mapPublicationRow);
    },
    async insertSnapshot(row) {
      const result = await db.query(
        `INSERT INTO content_performance_snapshots (
          id, client_id, tenant_id, publication_id, observed_at,
          impressions, members_reached, reactions, comments, reposts, saves,
          profile_views_attributed, followers_gained, connection_requests,
          metadata, created_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
        ) RETURNING *`,
        [
          row.id,
          row.clientId,
          row.tenantId,
          row.publicationId,
          row.observedAt,
          row.impressions,
          row.membersReached,
          row.reactions,
          row.comments,
          row.reposts,
          row.saves,
          row.profileViewsAttributed,
          row.followersGained,
          row.connectionRequests,
          JSON.stringify(row.metadata || {}),
          row.createdAt,
        ]
      );
      return mapSnapshotRow(result.rows[0]);
    },
    async listSnapshots(publicationId, clientId) {
      const result = await db.query(
        `SELECT s.* FROM content_performance_snapshots s
         JOIN content_publications p ON p.id = s.publication_id
         WHERE s.publication_id = $1
           AND ($2::int IS NULL OR p.client_id = $2)
         ORDER BY s.observed_at ASC`,
        [publicationId, clientId]
      );
      return result.rows.map(mapSnapshotRow);
    },
    async insertOutcome(row) {
      const result = await db.query(
        `INSERT INTO content_business_outcomes (
          id, client_id, tenant_id, publication_id, outcome_type, occurred_at,
          company_id, person_id, interaction_id, evidence_id, description,
          confidence, attribution, created_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14
        ) RETURNING *`,
        [
          row.id,
          row.clientId,
          row.tenantId,
          row.publicationId,
          row.outcomeType,
          row.occurredAt,
          row.companyId,
          row.personId,
          row.interactionId,
          row.evidenceId,
          row.description,
          row.confidence,
          row.attribution,
          row.createdAt,
        ]
      );
      return mapOutcomeRow(result.rows[0]);
    },
    async listOutcomes(publicationId, clientId) {
      const result = await db.query(
        `SELECT o.* FROM content_business_outcomes o
         JOIN content_publications p ON p.id = o.publication_id
         WHERE o.publication_id = $1
           AND ($2::int IS NULL OR p.client_id = $2)
         ORDER BY o.occurred_at ASC`,
        [publicationId, clientId]
      );
      return result.rows.map(mapOutcomeRow);
    },
    async insertSignal(row) {
      const result = await db.query(
        `INSERT INTO content_qualitative_signals (
          id, client_id, tenant_id, publication_id, observed_at, signal_type,
          description, audience_type, sentiment, strength, evidence_id, created_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
        ) RETURNING *`,
        [
          row.id,
          row.clientId,
          row.tenantId,
          row.publicationId,
          row.observedAt,
          row.signalType,
          row.description,
          row.audienceType,
          row.sentiment,
          row.strength,
          row.evidenceId,
          row.createdAt,
        ]
      );
      return mapSignalRow(result.rows[0]);
    },
    async listSignals(publicationId, clientId) {
      const result = await db.query(
        `SELECT s.* FROM content_qualitative_signals s
         JOIN content_publications p ON p.id = s.publication_id
         WHERE s.publication_id = $1
           AND ($2::int IS NULL OR p.client_id = $2)
         ORDER BY s.observed_at ASC`,
        [publicationId, clientId]
      );
      return result.rows.map(mapSignalRow);
    },
  };
}

function resolveStore(opts = {}) {
  if (opts.store) return opts.store;
  return createPostgresStore(opts.pool);
}

function dualWriteContentEvent(event) {
  try {
    const {
      safeWriteOperational,
      OPERATIONAL_EVENTS,
    } = require('../utils/knowledgeDualWrite');
    safeWriteOperational({
      id: event.id,
      tenantId: event.tenantId,
      entityId: event.entityId,
      entityType: 'content_publication',
      eventType: event.eventType || OPERATIONAL_EVENTS.OUTCOME_INCONCLUSIVE,
      source: 'content_outcome_intelligence',
      payload: event.payload || {},
      evidence: event.evidence || {
        summary: event.summary || 'Content outcome recorded',
      },
    });
  } catch {
    // never block content outcome path
  }
}

/**
 * Create a publication record for a Paige (or manually backfilled) artifact.
 * @param {object} input
 * @param {object} [opts]
 */
async function createContentPublication(input = {}, opts = {}) {
  const clientId = asClientId(input.clientId ?? input.client_id ?? input.tenantId);
  if (clientId == null) {
    throw new ContentOutcomeError('client_id_required', 'client_id required');
  }
  const contentArtifactId = asText(
    input.contentArtifactId ?? input.content_artifact_id
  );
  if (!contentArtifactId) {
    throw new ContentOutcomeError(
      'content_artifact_required',
      'content_artifact_id required'
    );
  }
  const channel = assertEnum(
    input.channel || 'linkedin',
    CHANNELS,
    'channel',
    'invalid_channel'
  );
  const publishedAt = parseTimestamp(
    input.publishedAt ?? input.published_at,
    'published_at'
  ).toISOString();
  const objective = optionalEnum(
    input.objective,
    OBJECTIVES,
    'objective'
  );
  const now = nowIso();
  const row = {
    id: asText(input.id) || newId(),
    clientId,
    tenantId: asText(input.tenantId) || tenantForClient(clientId),
    contentArtifactId,
    channel,
    externalPostId: asText(input.externalPostId ?? input.external_post_id),
    externalUrl: asText(input.externalUrl ?? input.external_url),
    publishedAt,
    objective,
    topic: asText(input.topic),
    thesis: asText(input.thesis),
    format: asText(input.format),
    intendedAudience: asAudience(
      input.intendedAudience ?? input.intended_audience
    ),
    campaignId: asText(input.campaignId ?? input.campaign_id),
    title: asText(input.title),
    createdAt: now,
    updatedAt: now,
  };

  const store = resolveStore(opts);
  const created = await store.insertPublication(row);
  dualWriteContentEvent({
    id: `content-pub:${clientId}:${created.id}`,
    tenantId: created.tenantId,
    entityId: created.id,
    summary: `Content published on ${created.channel}`,
    payload: {
      publicationId: created.id,
      contentArtifactId: created.contentArtifactId,
      channel: created.channel,
      objective: created.objective,
    },
  });
  return created;
}

async function requirePublication(publicationId, clientId, store) {
  const id = asText(publicationId);
  if (!id) {
    throw new ContentOutcomeError(
      'publication_id_required',
      'publication id required'
    );
  }
  const pub = await store.getPublication(id, clientId);
  if (!pub) {
    throw new ContentOutcomeError(
      'publication_not_found',
      'publication not found for tenant',
      404
    );
  }
  return pub;
}

/**
 * Immutable performance snapshot. Partial metrics allowed.
 */
async function addPerformanceSnapshot(publicationId, input = {}, opts = {}) {
  const store = resolveStore(opts);
  const clientId = asClientId(input.clientId ?? input.client_id ?? input.tenantId);
  const pub = await requirePublication(publicationId, clientId, store);
  const observedAt = parseTimestamp(
    input.observedAt ?? input.observed_at,
    'observed_at'
  ).toISOString();

  const metrics = {};
  for (const field of METRIC_FIELDS) {
    const snake = field.replace(/[A-Z]/g, (m) => `_${m.toLowerCase()}`);
    const raw = input[field] ?? input[snake];
    metrics[field] = optionalNonNegInt(raw, field);
  }

  const row = {
    id: asText(input.id) || newId(),
    clientId: pub.clientId,
    tenantId: pub.tenantId,
    publicationId: pub.id,
    observedAt,
    impressions: metrics.impressions,
    membersReached: metrics.membersReached,
    reactions: metrics.reactions,
    comments: metrics.comments,
    reposts: metrics.reposts,
    saves: metrics.saves,
    profileViewsAttributed: metrics.profileViewsAttributed,
    followersGained: metrics.followersGained,
    connectionRequests: metrics.connectionRequests,
    metadata:
      input.metadata && typeof input.metadata === 'object'
        ? input.metadata
        : {},
    createdAt: nowIso(),
  };

  return store.insertSnapshot(row);
}

/**
 * Record a downstream business outcome (distinct from vanity metrics).
 */
async function addBusinessOutcome(publicationId, input = {}, opts = {}) {
  const store = resolveStore(opts);
  const clientId = asClientId(input.clientId ?? input.client_id ?? input.tenantId);
  const pub = await requirePublication(publicationId, clientId, store);
  const outcomeType = assertEnum(
    input.outcomeType ?? input.outcome_type,
    BUSINESS_OUTCOME_TYPES,
    'outcome_type',
    'invalid_outcome_type'
  );
  const attribution = assertEnum(
    input.attribution || 'unknown',
    ATTRIBUTION_LEVELS,
    'attribution',
    'invalid_attribution'
  );
  const occurredAt = parseTimestamp(
    input.occurredAt ?? input.occurred_at,
    'occurred_at'
  ).toISOString();

  const row = {
    id: asText(input.id) || newId(),
    clientId: pub.clientId,
    tenantId: pub.tenantId,
    publicationId: pub.id,
    outcomeType,
    occurredAt,
    companyId: asText(input.companyId ?? input.company_id),
    personId: asText(input.personId ?? input.person_id),
    interactionId: asText(input.interactionId ?? input.interaction_id),
    evidenceId: asText(input.evidenceId ?? input.evidence_id),
    description: asText(input.description),
    confidence: optionalConfidence(input.confidence),
    attribution,
    createdAt: nowIso(),
  };

  const created = await store.insertOutcome(row);
  dualWriteContentEvent({
    id: `content-outcome:${pub.clientId}:${created.id}`,
    tenantId: pub.tenantId,
    entityId: pub.id,
    summary: `Content business outcome: ${outcomeType} (${attribution})`,
    payload: {
      publicationId: pub.id,
      outcomeId: created.id,
      outcomeType,
      attribution,
      companyId: created.companyId,
      personId: created.personId,
      interactionId: created.interactionId,
      evidenceId: created.evidenceId,
    },
  });
  return created;
}

/**
 * Qualitative observation — not an automatic conclusion.
 */
async function addQualitativeSignal(publicationId, input = {}, opts = {}) {
  const store = resolveStore(opts);
  const clientId = asClientId(input.clientId ?? input.client_id ?? input.tenantId);
  const pub = await requirePublication(publicationId, clientId, store);
  const signalType = assertEnum(
    input.signalType ?? input.signal_type,
    SIGNAL_TYPES,
    'signal_type',
    'invalid_signal_type'
  );
  const description = asText(input.description);
  if (!description) {
    throw new ContentOutcomeError(
      'description_required',
      'description required for qualitative signal'
    );
  }
  const observedAt = parseTimestamp(
    input.observedAt ?? input.observed_at,
    'observed_at'
  ).toISOString();

  const row = {
    id: asText(input.id) || newId(),
    clientId: pub.clientId,
    tenantId: pub.tenantId,
    publicationId: pub.id,
    observedAt,
    signalType,
    description,
    audienceType: asText(input.audienceType ?? input.audience_type),
    sentiment: asText(input.sentiment),
    strength: asText(input.strength),
    evidenceId: asText(input.evidenceId ?? input.evidence_id),
    createdAt: nowIso(),
  };

  return store.insertSignal(row);
}

/**
 * Complete outcome history for one publication.
 */
async function getPublicationOutcome(publicationId, opts = {}) {
  const store = resolveStore(opts);
  const clientId = asClientId(opts.clientId ?? opts.client_id ?? opts.tenantId);
  const pub = await requirePublication(publicationId, clientId, store);
  const [performanceSnapshots, businessOutcomes, qualitativeSignals] =
    await Promise.all([
      store.listSnapshots(pub.id, pub.clientId),
      store.listOutcomes(pub.id, pub.clientId),
      store.listSignals(pub.id, pub.clientId),
    ]);

  const evidenceIds = new Set();
  for (const o of businessOutcomes) {
    if (o.evidenceId) evidenceIds.add(o.evidenceId);
  }
  for (const s of qualitativeSignals) {
    if (s.evidenceId) evidenceIds.add(s.evidenceId);
  }

  return {
    publication: pub,
    contentArtifact: {
      id: pub.contentArtifactId,
      source: 'paige_or_manual',
    },
    performanceSnapshots,
    businessOutcomes,
    qualitativeSignals,
    evidenceReferences: [...evidenceIds],
  };
}

/**
 * Timeline view: ordered events for a publication.
 */
async function getContentOutcomeTimeline(publicationId, opts = {}) {
  const full = await getPublicationOutcome(publicationId, opts);
  const events = [];
  events.push({
    kind: 'publication',
    at: full.publication.publishedAt,
    data: full.publication,
  });
  for (const s of full.performanceSnapshots) {
    events.push({ kind: 'performance', at: s.observedAt, data: s });
  }
  for (const o of full.businessOutcomes) {
    events.push({ kind: 'business_outcome', at: o.occurredAt, data: o });
  }
  for (const s of full.qualitativeSignals) {
    events.push({ kind: 'qualitative_signal', at: s.observedAt, data: s });
  }
  events.sort((a, b) => new Date(a.at) - new Date(b.at));
  return {
    publicationId: full.publication.id,
    clientId: full.publication.clientId,
    tenantId: full.publication.tenantId,
    events,
  };
}

async function hydratePublications(pubs, store) {
  const results = [];
  for (const pub of pubs) {
    const [performanceSnapshots, businessOutcomes, qualitativeSignals] =
      await Promise.all([
        store.listSnapshots(pub.id, pub.clientId),
        store.listOutcomes(pub.id, pub.clientId),
        store.listSignals(pub.id, pub.clientId),
      ]);
    results.push({
      publication: pub,
      contentArtifact: {
        id: pub.contentArtifactId,
        source: 'paige_or_manual',
      },
      performanceSnapshots,
      businessOutcomes,
      qualitativeSignals,
      evidenceReferences: [
        ...new Set(
          [
            ...businessOutcomes.map((o) => o.evidenceId),
            ...qualitativeSignals.map((s) => s.evidenceId),
          ].filter(Boolean)
        ),
      ],
      latestPerformance: latestSnapshotMetrics(performanceSnapshots),
    });
  }
  return results;
}

/**
 * List content outcomes with optional filters.
 */
async function listContentOutcomes(filter = {}, opts = {}) {
  const store = resolveStore(opts);
  const clientId = asClientId(
    filter.clientId ?? filter.client_id ?? filter.tenantId ?? opts.clientId
  );
  if (clientId == null) {
    throw new ContentOutcomeError('client_id_required', 'client_id / tenantId required');
  }
  const pubs = await store.listPublications({
    clientId,
    channel: asText(filter.channel),
    objective: asText(filter.objective),
    topic: asText(filter.topic),
    format: asText(filter.format),
    intendedAudience: asText(
      filter.intendedAudience ?? filter.intended_audience
    ),
    from: filter.from ?? filter.dateFrom ?? filter.date_from,
    to: filter.to ?? filter.dateTo ?? filter.date_to,
    limit: filter.limit != null ? Number(filter.limit) : null,
  });
  return hydratePublications(pubs, store);
}

async function getRecentContentOutcomes(tenantIdOrClientId, limit = 5, opts = {}) {
  const clientId = asClientId(tenantIdOrClientId);
  if (clientId == null) {
    throw new ContentOutcomeError('client_id_required', 'tenantId / client_id required');
  }
  return listContentOutcomes(
    { clientId, limit: Math.max(1, Math.min(Number(limit) || 5, 50)) },
    { ...opts, store: opts.store }
  );
}

/**
 * Deterministic aggregates only — no recommendations (SPEC-086 territory).
 */
async function compareContentOutcomes(filter = {}, opts = {}) {
  const items = await listContentOutcomes(filter, opts);
  const groupBy = asText(filter.groupBy ?? filter.group_by) || 'objective';
  const allowedGroups = new Set([
    'objective',
    'topic',
    'format',
    'intendedAudience',
    'channel',
  ]);
  if (!allowedGroups.has(groupBy)) {
    throw new ContentOutcomeError(
      'invalid_group_by',
      `groupBy must be one of: ${[...allowedGroups].join(', ')}`
    );
  }

  const impressionValues = [];
  const commentValues = [];
  let totalQualified = 0;
  let totalPartner = 0;
  let totalMeetings = 0;

  /** @type {Map<string, object[]>} */
  const groups = new Map();

  for (const item of items) {
    const latest = item.latestPerformance;
    if (latest && latest.impressions != null) {
      impressionValues.push(latest.impressions);
    }
    if (latest && latest.comments != null) {
      commentValues.push(latest.comments);
    }
    totalQualified += countOutcomes(item.businessOutcomes, 'qualified_dm');
    totalQualified += countOutcomes(
      item.businessOutcomes,
      'prospect_conversation'
    );
    totalPartner += countOutcomes(
      item.businessOutcomes,
      'partner_conversation'
    );
    totalMeetings += countOutcomes(item.businessOutcomes, 'meeting_booked');

    let key;
    if (groupBy === 'intendedAudience') {
      key = (item.publication.intendedAudience || []).join(',') || '(none)';
    } else {
      key = item.publication[groupBy] || '(none)';
    }
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(item);
  }

  const grouped = [...groups.entries()].map(([key, groupItems]) => {
    const gImpressions = [];
    const gComments = [];
    let gQualified = 0;
    let gPartner = 0;
    let gMeetings = 0;
    for (const item of groupItems) {
      const latest = item.latestPerformance;
      if (latest && latest.impressions != null) gImpressions.push(latest.impressions);
      if (latest && latest.comments != null) gComments.push(latest.comments);
      gQualified +=
        countOutcomes(item.businessOutcomes, 'qualified_dm') +
        countOutcomes(item.businessOutcomes, 'prospect_conversation');
      gPartner += countOutcomes(item.businessOutcomes, 'partner_conversation');
      gMeetings += countOutcomes(item.businessOutcomes, 'meeting_booked');
    }
    return {
      key,
      totalPublications: groupItems.length,
      medianImpressions: median(gImpressions),
      averageComments: average(gComments),
      totalQualifiedConversations: gQualified,
      totalPartnerConversations: gPartner,
      totalMeetings: gMeetings,
    };
  });

  return {
    totalPublications: items.length,
    medianImpressions: median(impressionValues),
    averageComments: average(commentValues),
    totalQualifiedConversations: totalQualified,
    totalPartnerConversations: totalPartner,
    totalMeetings,
    groupBy,
    groups: grouped,
    // Explicit: no vanity composite score
    vanityScore: null,
  };
}

/**
 * Shape suitable for Max / intelligence consumers (read-only evidence).
 * Does not mutate SPEC-013 OutcomeEngine or Paige strategy.
 */
function toIntelligencePayload(publicationOutcome) {
  if (!publicationOutcome || !publicationOutcome.publication) return null;
  const pub = publicationOutcome.publication;
  return {
    kind: 'content_outcome',
    tenantId: pub.tenantId,
    clientId: pub.clientId,
    publicationId: pub.id,
    contentArtifactId: pub.contentArtifactId,
    channel: pub.channel,
    objective: pub.objective,
    topic: pub.topic,
    thesis: pub.thesis,
    format: pub.format,
    publishedAt: pub.publishedAt,
    performanceSnapshots: publicationOutcome.performanceSnapshots || [],
    businessOutcomes: publicationOutcome.businessOutcomes || [],
    qualitativeSignals: publicationOutcome.qualitativeSignals || [],
    evidenceReferences: publicationOutcome.evidenceReferences || [],
    latestPerformance: latestSnapshotMetrics(
      publicationOutcome.performanceSnapshots || []
    ),
  };
}

module.exports = {
  CHANNELS,
  OBJECTIVES,
  BUSINESS_OUTCOME_TYPES,
  ATTRIBUTION_LEVELS,
  SIGNAL_TYPES,
  METRIC_FIELDS,
  ContentOutcomeError,
  createMemoryStore,
  createPostgresStore,
  createContentPublication,
  addPerformanceSnapshot,
  addBusinessOutcome,
  addQualitativeSignal,
  getPublicationOutcome,
  getContentOutcomeTimeline,
  listContentOutcomes,
  getRecentContentOutcomes,
  compareContentOutcomes,
  toIntelligencePayload,
};
