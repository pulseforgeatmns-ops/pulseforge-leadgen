'use strict';

/**
 * SPEC-092 — Content Outcome Intelligence
 * Records Content → Intent → Publication → Observed Response → Business Outcome.
 * Extends SPEC-013 Outcome Intelligence (evaluate-only; no Paige strategy mutation).
 * Manual capture is sufficient for v1. No LinkedIn API. No vanity score.
 */

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { normalizeClientId } = require('../utils/clientContext');

function getDefaultPool() {
  return require('../db');
}

const CHANNELS = Object.freeze([
  'linkedin',
  'facebook',
  'gbp',
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
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'ContentOutcomeError';
    this.code = code;
    this.status = status;
  }
}

function newId() {
  return crypto.randomUUID();
}

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function asTenantId(value) {
  if (value == null || value === '') {
    throw new ContentOutcomeError('tenant_required', 'tenant_id is required');
  }
  return String(normalizeClientId(value));
}

function requireEnum(value, allowed, label) {
  const v = asText(value);
  if (!v) {
    throw new ContentOutcomeError(`${label}_required`, `${label} is required`);
  }
  if (!allowed.includes(v)) {
    throw new ContentOutcomeError(
      `${label}_invalid`,
      `${label} must be one of: ${allowed.join(', ')}`
    );
  }
  return v;
}

function optionalEnum(value, allowed, label) {
  const v = asText(value);
  if (!v) return null;
  if (!allowed.includes(v)) {
    throw new ContentOutcomeError(
      `${label}_invalid`,
      `${label} must be one of: ${allowed.join(', ')}`
    );
  }
  return v;
}

function parseTimestamp(value, label, { required = false, fallback = null } = {}) {
  if (value == null || value === '') {
    if (required) {
      throw new ContentOutcomeError(
        `${label}_required`,
        `${label} is required`
      );
    }
    return fallback || new Date().toISOString();
  }
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) {
    throw new ContentOutcomeError(
      `${label}_invalid`,
      `${label} must be a valid timestamp`
    );
  }
  return d.toISOString();
}

function optionalNonNegativeInt(value, label) {
  if (value === undefined || value === null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0) {
    throw new ContentOutcomeError(
      `${label}_invalid`,
      `${label} must be a non-negative integer`
    );
  }
  return n;
}

function optionalConfidence(value) {
  if (value === undefined || value === null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0 || n > 1) {
    throw new ContentOutcomeError(
      'confidence_invalid',
      'confidence must be between 0 and 1'
    );
  }
  return n;
}

function asAudienceArray(value) {
  if (value == null || value === '') return [];
  if (Array.isArray(value)) {
    return value.map((v) => String(v).trim()).filter(Boolean);
  }
  return String(value)
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
}

function asMetadata(value) {
  if (value == null) return {};
  if (typeof value === 'object' && !Array.isArray(value)) return { ...value };
  throw new ContentOutcomeError('metadata_invalid', 'metadata must be an object');
}

function median(numbers) {
  if (!numbers.length) return null;
  const sorted = [...numbers].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

function average(numbers) {
  if (!numbers.length) return null;
  return numbers.reduce((a, b) => a + b, 0) / numbers.length;
}

function latestSnapshotMetrics(snapshots) {
  if (!snapshots || !snapshots.length) return null;
  const sorted = [...snapshots].sort(
    (a, b) => new Date(a.observed_at) - new Date(b.observed_at)
  );
  return sorted[sorted.length - 1];
}

function createMemoryStore() {
  return {
    kind: 'memory',
    publications: new Map(),
    snapshots: new Map(),
    outcomes: new Map(),
    signals: new Map(),
  };
}

function publicationRow(input) {
  const now = new Date().toISOString();
  return {
    id: input.id || newId(),
    tenant_id: asTenantId(input.tenant_id ?? input.tenantId),
    content_artifact_id: asText(
      input.content_artifact_id ?? input.contentArtifactId
    ),
    channel: optionalEnum(input.channel, CHANNELS, 'channel') || 'linkedin',
    external_post_id: asText(input.external_post_id ?? input.externalPostId),
    external_url: asText(input.external_url ?? input.externalUrl),
    published_at: parseTimestamp(
      input.published_at ?? input.publishedAt,
      'published_at',
      { required: true }
    ),
    objective: optionalEnum(input.objective, OBJECTIVES, 'objective'),
    topic: asText(input.topic),
    thesis: asText(input.thesis),
    format: asText(input.format),
    intended_audience: asAudienceArray(
      input.intended_audience ?? input.intendedAudience
    ),
    campaign_id: asText(input.campaign_id ?? input.campaignId),
    canonical_outcome_id: asText(
      input.canonical_outcome_id ?? input.canonicalOutcomeId
    ),
    metadata: asMetadata(input.metadata),
    created_at: input.created_at || now,
    updated_at: input.updated_at || now,
  };
}

function snapshotRow(input, publication) {
  const metrics = {};
  for (const field of METRIC_FIELDS) {
    const camel = field.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
    metrics[field] = optionalNonNegativeInt(
      input[field] ?? input[camel],
      field
    );
  }
  return {
    id: input.id || newId(),
    tenant_id: publication.tenant_id,
    publication_id: publication.id,
    observed_at: parseTimestamp(
      input.observed_at ?? input.observedAt,
      'observed_at'
    ),
    ...metrics,
    metadata: asMetadata(input.metadata),
    created_at: input.created_at || new Date().toISOString(),
  };
}

function businessOutcomeRow(input, publication) {
  return {
    id: input.id || newId(),
    tenant_id: publication.tenant_id,
    publication_id: publication.id,
    outcome_type: requireEnum(
      input.outcome_type ?? input.outcomeType,
      BUSINESS_OUTCOME_TYPES,
      'outcome_type'
    ),
    occurred_at: parseTimestamp(
      input.occurred_at ?? input.occurredAt,
      'occurred_at'
    ),
    company_id: asText(input.company_id ?? input.companyId),
    person_id: asText(input.person_id ?? input.personId),
    interaction_id: asText(input.interaction_id ?? input.interactionId),
    evidence_id: asText(input.evidence_id ?? input.evidenceId),
    description: asText(input.description),
    confidence: optionalConfidence(input.confidence),
    attribution:
      optionalEnum(
        input.attribution,
        ATTRIBUTION_LEVELS,
        'attribution'
      ) || 'unknown',
    canonical_outcome_id: asText(
      input.canonical_outcome_id ?? input.canonicalOutcomeId
    ),
    created_at: input.created_at || new Date().toISOString(),
  };
}

function qualitativeSignalRow(input, publication) {
  const description = asText(input.description);
  if (!description) {
    throw new ContentOutcomeError(
      'description_required',
      'description is required'
    );
  }
  return {
    id: input.id || newId(),
    tenant_id: publication.tenant_id,
    publication_id: publication.id,
    observed_at: parseTimestamp(
      input.observed_at ?? input.observedAt,
      'observed_at'
    ),
    signal_type: requireEnum(
      input.signal_type ?? input.signalType,
      SIGNAL_TYPES,
      'signal_type'
    ),
    description,
    audience_type: asText(input.audience_type ?? input.audienceType),
    sentiment: asText(input.sentiment),
    strength: asText(input.strength),
    evidence_id: asText(input.evidence_id ?? input.evidenceId),
    created_at: input.created_at || new Date().toISOString(),
  };
}

function resolveDeps(options = {}) {
  if (options.store) return { kind: 'memory', store: options.store };
  return { kind: 'postgres', pool: options.pool || getDefaultPool() };
}

async function ensureContentOutcomeSchema(options = {}) {
  const deps = resolveDeps(options);
  if (deps.kind === 'memory') return { ok: true, kind: 'memory' };
  const sqlPath = path.join(
    __dirname,
    '..',
    'migrations',
    '2026-08-13-content-outcome-intelligence.sql'
  );
  const sql = fs.readFileSync(sqlPath, 'utf8');
  await deps.pool.query(sql);
  return { ok: true, kind: 'postgres' };
}

async function getPublicationById(publicationId, tenantId, deps) {
  const id = asText(publicationId);
  if (!id) {
    throw new ContentOutcomeError(
      'publication_id_required',
      'publication_id is required'
    );
  }
  if (deps.kind === 'memory') {
    const row = deps.store.publications.get(id);
    if (!row || row.tenant_id !== tenantId) return null;
    return { ...row, intended_audience: [...(row.intended_audience || [])] };
  }
  const result = await deps.pool.query(
    `SELECT * FROM content_publications WHERE id = $1 AND tenant_id = $2`,
    [id, tenantId]
  );
  return result.rows[0] || null;
}

async function requirePublication(publicationId, tenantId, deps) {
  const pub = await getPublicationById(publicationId, tenantId, deps);
  if (!pub) {
    throw new ContentOutcomeError(
      'publication_not_found',
      'publication not found',
      404
    );
  }
  return pub;
}

/**
 * Create a content publication from a Paige artifact (or manual identity).
 */
async function createContentPublication(input = {}, options = {}) {
  const deps = resolveDeps(options);
  const row = publicationRow(input);
  if (!row.content_artifact_id) {
    throw new ContentOutcomeError(
      'content_artifact_id_required',
      'content_artifact_id is required'
    );
  }

  if (deps.kind === 'memory') {
    deps.store.publications.set(row.id, row);
    return { ...row, intended_audience: [...row.intended_audience] };
  }

  await ensureContentOutcomeSchema(options);
  const result = await deps.pool.query(
    `INSERT INTO content_publications (
      id, tenant_id, content_artifact_id, channel, external_post_id, external_url,
      published_at, objective, topic, thesis, format, intended_audience,
      campaign_id, canonical_outcome_id, metadata, created_at, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
    ) RETURNING *`,
    [
      row.id,
      row.tenant_id,
      row.content_artifact_id,
      row.channel,
      row.external_post_id,
      row.external_url,
      row.published_at,
      row.objective,
      row.topic,
      row.thesis,
      row.format,
      row.intended_audience,
      row.campaign_id,
      row.canonical_outcome_id,
      JSON.stringify(row.metadata),
      row.created_at,
      row.updated_at,
    ]
  );
  return result.rows[0];
}

/**
 * Immutable performance snapshot. Never overwrites prior observations.
 */
async function addPerformanceSnapshot(
  publicationId,
  input = {},
  options = {}
) {
  const deps = resolveDeps(options);
  const tenantId = asTenantId(input.tenant_id ?? input.tenantId ?? options.tenantId);
  const publication = await requirePublication(publicationId, tenantId, deps);
  const row = snapshotRow(input, publication);

  if (deps.kind === 'memory') {
    deps.store.snapshots.set(row.id, row);
    return { ...row };
  }

  const result = await deps.pool.query(
    `INSERT INTO content_performance_snapshots (
      id, tenant_id, publication_id, observed_at,
      impressions, members_reached, reactions, comments, reposts, saves,
      profile_views_attributed, followers_gained, connection_requests,
      metadata, created_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
    ) RETURNING *`,
    [
      row.id,
      row.tenant_id,
      row.publication_id,
      row.observed_at,
      row.impressions,
      row.members_reached,
      row.reactions,
      row.comments,
      row.reposts,
      row.saves,
      row.profile_views_attributed,
      row.followers_gained,
      row.connection_requests,
      JSON.stringify(row.metadata),
      row.created_at,
    ]
  );
  return result.rows[0];
}

/**
 * Record a downstream business outcome (distinct from vanity metrics).
 * Optionally links a SPEC-013 RecommendationOutcome id via canonical_outcome_id.
 */
async function addBusinessOutcome(publicationId, input = {}, options = {}) {
  const deps = resolveDeps(options);
  const tenantId = asTenantId(input.tenant_id ?? input.tenantId ?? options.tenantId);
  const publication = await requirePublication(publicationId, tenantId, deps);
  const row = businessOutcomeRow(input, publication);

  // Soft link into SPEC-013 Outcome Intelligence when a runtime is provided.
  // Never mutates Paige strategy. Evaluate-only.
  if (options.outcomeEngine && !row.canonical_outcome_id) {
    try {
      const recorded = options.outcomeEngine.record({
        recommendationId: `content:${publication.id}`,
        tenantId: publication.tenant_id,
        lifecycle: 'observed',
        outcome: 'inconclusive',
        confidenceAtRecommendation: 0,
        notes: row.description || row.outcome_type,
        meta: {
          kind: 'content_business_outcome',
          publicationId: publication.id,
          outcomeType: row.outcome_type,
          attribution: row.attribution,
        },
        evidenceSourceIds: row.evidence_id ? [row.evidence_id] : [],
      });
      row.canonical_outcome_id = recorded.id;
    } catch {
      // never block content outcome path
    }
  }

  if (deps.kind === 'memory') {
    deps.store.outcomes.set(row.id, row);
    return { ...row };
  }

  const result = await deps.pool.query(
    `INSERT INTO content_business_outcomes (
      id, tenant_id, publication_id, outcome_type, occurred_at,
      company_id, person_id, interaction_id, evidence_id,
      description, confidence, attribution, canonical_outcome_id, created_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14
    ) RETURNING *`,
    [
      row.id,
      row.tenant_id,
      row.publication_id,
      row.outcome_type,
      row.occurred_at,
      row.company_id,
      row.person_id,
      row.interaction_id,
      row.evidence_id,
      row.description,
      row.confidence,
      row.attribution,
      row.canonical_outcome_id,
      row.created_at,
    ]
  );
  return result.rows[0];
}

async function addQualitativeSignal(publicationId, input = {}, options = {}) {
  const deps = resolveDeps(options);
  const tenantId = asTenantId(input.tenant_id ?? input.tenantId ?? options.tenantId);
  const publication = await requirePublication(publicationId, tenantId, deps);
  const row = qualitativeSignalRow(input, publication);

  if (deps.kind === 'memory') {
    deps.store.signals.set(row.id, row);
    return { ...row };
  }

  const result = await deps.pool.query(
    `INSERT INTO content_qualitative_signals (
      id, tenant_id, publication_id, observed_at, signal_type, description,
      audience_type, sentiment, strength, evidence_id, created_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
    ) RETURNING *`,
    [
      row.id,
      row.tenant_id,
      row.publication_id,
      row.observed_at,
      row.signal_type,
      row.description,
      row.audience_type,
      row.sentiment,
      row.strength,
      row.evidence_id,
      row.created_at,
    ]
  );
  return result.rows[0];
}

async function loadChildren(publicationId, tenantId, deps) {
  if (deps.kind === 'memory') {
    const snapshots = [...deps.store.snapshots.values()]
      .filter(
        (r) => r.publication_id === publicationId && r.tenant_id === tenantId
      )
      .sort((a, b) => new Date(a.observed_at) - new Date(b.observed_at))
      .map((r) => ({ ...r }));
    const businessOutcomes = [...deps.store.outcomes.values()]
      .filter(
        (r) => r.publication_id === publicationId && r.tenant_id === tenantId
      )
      .sort((a, b) => new Date(a.occurred_at) - new Date(b.occurred_at))
      .map((r) => ({ ...r }));
    const qualitativeSignals = [...deps.store.signals.values()]
      .filter(
        (r) => r.publication_id === publicationId && r.tenant_id === tenantId
      )
      .sort((a, b) => new Date(a.observed_at) - new Date(b.observed_at))
      .map((r) => ({ ...r }));
    return { snapshots, businessOutcomes, qualitativeSignals };
  }

  const [snapRes, outRes, sigRes] = await Promise.all([
    deps.pool.query(
      `SELECT * FROM content_performance_snapshots
       WHERE publication_id = $1 AND tenant_id = $2
       ORDER BY observed_at ASC`,
      [publicationId, tenantId]
    ),
    deps.pool.query(
      `SELECT * FROM content_business_outcomes
       WHERE publication_id = $1 AND tenant_id = $2
       ORDER BY occurred_at ASC`,
      [publicationId, tenantId]
    ),
    deps.pool.query(
      `SELECT * FROM content_qualitative_signals
       WHERE publication_id = $1 AND tenant_id = $2
       ORDER BY observed_at ASC`,
      [publicationId, tenantId]
    ),
  ]);
  return {
    snapshots: snapRes.rows,
    businessOutcomes: outRes.rows,
    qualitativeSignals: sigRes.rows,
  };
}

function collectEvidenceRefs(businessOutcomes, qualitativeSignals) {
  const refs = [];
  const seen = new Set();
  for (const row of [...businessOutcomes, ...qualitativeSignals]) {
    if (!row.evidence_id) continue;
    if (seen.has(row.evidence_id)) continue;
    seen.add(row.evidence_id);
    refs.push({
      evidence_id: row.evidence_id,
      source: row.outcome_type ? 'business_outcome' : 'qualitative_signal',
    });
  }
  return refs;
}

function buildTimeline(publication, children) {
  const events = [];
  events.push({
    kind: 'publication',
    at: publication.published_at,
    id: publication.id,
    summary: `Published to ${publication.channel}`,
  });
  for (const s of children.snapshots) {
    events.push({
      kind: 'performance_snapshot',
      at: s.observed_at,
      id: s.id,
      summary: [
        s.impressions != null ? `${s.impressions} impressions` : null,
        s.reactions != null ? `${s.reactions} reactions` : null,
        s.comments != null ? `${s.comments} comments` : null,
      ]
        .filter(Boolean)
        .join(', ') || 'Performance observed',
    });
  }
  for (const o of children.businessOutcomes) {
    events.push({
      kind: 'business_outcome',
      at: o.occurred_at,
      id: o.id,
      summary: `${o.outcome_type} (${o.attribution})`,
    });
  }
  for (const q of children.qualitativeSignals) {
    events.push({
      kind: 'qualitative_signal',
      at: q.observed_at,
      id: q.id,
      summary: `${q.signal_type}: ${q.description}`,
    });
  }
  return events.sort((a, b) => new Date(a.at) - new Date(b.at));
}

async function getPublicationOutcome(publicationId, options = {}) {
  const deps = resolveDeps(options);
  const tenantId = asTenantId(options.tenantId ?? options.tenant_id);
  const publication = await requirePublication(publicationId, tenantId, deps);
  const children = await loadChildren(publication.id, tenantId, deps);
  return {
    publication,
    contentArtifact: {
      id: publication.content_artifact_id,
      kind: 'paige_pending_comment',
    },
    performanceSnapshots: children.snapshots,
    businessOutcomes: children.businessOutcomes,
    qualitativeSignals: children.qualitativeSignals,
    evidenceReferences: collectEvidenceRefs(
      children.businessOutcomes,
      children.qualitativeSignals
    ),
    timeline: buildTimeline(publication, children),
  };
}

async function getContentOutcomeTimeline(publicationId, options = {}) {
  const full = await getPublicationOutcome(publicationId, options);
  return {
    publicationId: full.publication.id,
    tenantId: full.publication.tenant_id,
    timeline: full.timeline,
  };
}

async function listPublications(filters = {}, options = {}) {
  const deps = resolveDeps(options);
  const tenantId = asTenantId(filters.tenantId ?? filters.tenant_id);
  const channel = optionalEnum(filters.channel, CHANNELS, 'channel');
  const objective = optionalEnum(filters.objective, OBJECTIVES, 'objective');
  const topic = asText(filters.topic);
  const from = filters.from
    ? parseTimestamp(filters.from, 'from', { required: true })
    : null;
  const to = filters.to
    ? parseTimestamp(filters.to, 'to', { required: true })
    : null;
  const limit = Math.min(
    Math.max(parseInt(filters.limit, 10) || 50, 1),
    200
  );

  if (deps.kind === 'memory') {
    let rows = [...deps.store.publications.values()].filter(
      (r) => r.tenant_id === tenantId
    );
    if (channel) rows = rows.filter((r) => r.channel === channel);
    if (objective) rows = rows.filter((r) => r.objective === objective);
    if (topic) rows = rows.filter((r) => r.topic === topic);
    if (from) rows = rows.filter((r) => r.published_at >= from);
    if (to) rows = rows.filter((r) => r.published_at <= to);
    return rows
      .sort((a, b) => new Date(b.published_at) - new Date(a.published_at))
      .slice(0, limit)
      .map((r) => ({
        ...r,
        intended_audience: [...(r.intended_audience || [])],
      }));
  }

  const clauses = ['tenant_id = $1'];
  const params = [tenantId];
  if (channel) {
    params.push(channel);
    clauses.push(`channel = $${params.length}`);
  }
  if (objective) {
    params.push(objective);
    clauses.push(`objective = $${params.length}`);
  }
  if (topic) {
    params.push(topic);
    clauses.push(`topic = $${params.length}`);
  }
  if (from) {
    params.push(from);
    clauses.push(`published_at >= $${params.length}`);
  }
  if (to) {
    params.push(to);
    clauses.push(`published_at <= $${params.length}`);
  }
  params.push(limit);
  const result = await deps.pool.query(
    `SELECT * FROM content_publications
     WHERE ${clauses.join(' AND ')}
     ORDER BY published_at DESC
     LIMIT $${params.length}`,
    params
  );
  return result.rows;
}

async function listContentOutcomes(filters = {}, options = {}) {
  const publications = await listPublications(filters, options);
  const results = [];
  for (const publication of publications) {
    results.push(
      await getPublicationOutcome(publication.id, {
        ...options,
        tenantId: publication.tenant_id,
      })
    );
  }
  return results;
}

async function getRecentContentOutcomes(tenantId, limit = 5, options = {}) {
  return listContentOutcomes(
    { tenantId, limit },
    options
  );
}

/**
 * Deterministic aggregates only. No recommendations (that is SPEC-086).
 */
async function compareContentOutcomes(filters = {}, options = {}) {
  const items = await listContentOutcomes(
    { ...filters, limit: filters.limit || 200 },
    options
  );

  const impressions = [];
  const comments = [];
  let qualifiedConversations = 0;
  let partnerConversations = 0;
  let meetings = 0;

  const byObjective = {};
  const byTopic = {};
  const byFormat = {};
  const byAudience = {};

  for (const item of items) {
    const latest = latestSnapshotMetrics(item.performanceSnapshots);
    if (latest?.impressions != null) impressions.push(Number(latest.impressions));
    if (latest?.comments != null) comments.push(Number(latest.comments));

    for (const o of item.businessOutcomes) {
      if (
        o.outcome_type === 'qualified_dm' ||
        o.outcome_type === 'prospect_conversation'
      ) {
        qualifiedConversations += 1;
      }
      if (o.outcome_type === 'partner_conversation') partnerConversations += 1;
      if (o.outcome_type === 'meeting_booked') meetings += 1;
    }

    const pub = item.publication;
    const objKey = pub.objective || 'unspecified';
    byObjective[objKey] = (byObjective[objKey] || 0) + 1;
    const topicKey = pub.topic || 'unspecified';
    byTopic[topicKey] = (byTopic[topicKey] || 0) + 1;
    const formatKey = pub.format || 'unspecified';
    byFormat[formatKey] = (byFormat[formatKey] || 0) + 1;
    for (const audience of pub.intended_audience || []) {
      byAudience[audience] = (byAudience[audience] || 0) + 1;
    }
  }

  return {
    totalPublications: items.length,
    medianImpressions: median(impressions),
    averageComments: average(comments),
    totalQualifiedConversations: qualifiedConversations,
    totalPartnerConversations: partnerConversations,
    totalMeetings: meetings,
    groupedBy: {
      objective: byObjective,
      topic: byTopic,
      format: byFormat,
      intendedAudience: byAudience,
    },
    // Explicitly no vanity composite score and no strategy recommendation.
    recommendsStrategy: false,
  };
}

/**
 * Read-only accessor for Max / intelligence consumers.
 * Does not alter Paige configuration.
 */
async function getContentOutcomesForIntelligence(tenantId, options = {}) {
  const limit = Math.min(Math.max(parseInt(options.limit, 10) || 5, 1), 50);
  const recent = await getRecentContentOutcomes(tenantId, limit, options);
  const comparison = await compareContentOutcomes(
    { tenantId, limit: 200 },
    options
  );
  return {
    kind: 'content_outcome_intelligence',
    isEvidence: true,
    recommendsStrategy: false,
    mutatesPaige: false,
    tenantId: String(normalizeClientId(tenantId)),
    recent,
    comparison,
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
  ensureContentOutcomeSchema,
  createContentPublication,
  addPerformanceSnapshot,
  addBusinessOutcome,
  addQualitativeSignal,
  getPublicationOutcome,
  getContentOutcomeTimeline,
  listPublications,
  listContentOutcomes,
  getRecentContentOutcomes,
  compareContentOutcomes,
  getContentOutcomesForIntelligence,
};
