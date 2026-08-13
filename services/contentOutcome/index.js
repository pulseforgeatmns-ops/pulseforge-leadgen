'use strict';

/**
 * Content Outcome Intelligence service (SPEC-092 / planning draft SPEC-085).
 *
 * Extends Outcome Intelligence (SPEC-013):
 * - tenant isolation via tenantId = String(clientId)
 * - evidence references (soft)
 * - evaluate / record only — never mutates Paige strategy
 *
 * Durable content-domain tables connect to canonical outcomes via
 * canonical_outcome_id when dual-write succeeds.
 */

const {
  CHANNELS,
  OBJECTIVES,
  BUSINESS_OUTCOME_TYPES,
  ATTRIBUTION_LEVELS,
  SIGNAL_TYPES,
  EVIDENCE_KINDS,
  ContentOutcomeError,
  normalizeTenantId,
  normalizeClientId,
  optionalNonNegativeInt,
  optionalText,
  optionalEnum,
  requireIsoTimestamp,
  optionalIsoTimestamp,
  normalizeAudience,
  optionalConfidence,
} = require('./types');
const { buildComparisonSummary, groupByDimension } = require('./aggregates');
const { createMemoryContentOutcomeStore } = require('./memoryStore');
const { createPostgresContentOutcomeStore } = require('./postgresStore');

/**
 * @param {object} [options]
 * @param {object} [options.store]
 * @param {import('pg').Pool} [options.pool]
 * @param {{ record?: Function, get?: Function }} [options.outcomeEngine] SPEC-013 engine (optional)
 * @param {{ writeEvidence?: Function }} [options.knowledge] optional evidence dual-write
 */
function createContentOutcomeService(options = {}) {
  const store =
    options.store ||
    (options.pool
      ? createPostgresContentOutcomeStore(options.pool)
      : createMemoryContentOutcomeStore());
  const outcomeEngine = options.outcomeEngine || null;
  const knowledge = options.knowledge || null;

  async function ensureReady() {
    if (typeof store.ensureSchema === 'function') {
      await store.ensureSchema();
    }
  }

  /**
   * Create or reuse a content artifact, then record a publication.
   * Body may include artifact fields inline for fast capture / backfill.
   */
  async function createPublication(input) {
    await ensureReady();
    const clientId = normalizeClientId(input.clientId ?? input.client_id);
    const tenantId = normalizeTenantId(input.tenantId ?? input.tenant_id ?? clientId);

    let artifactId = optionalText(input.content_artifact_id ?? input.contentArtifactId, 'content_artifact_id');
    let artifact = null;

    if (artifactId) {
      artifact = await store.getArtifact(tenantId, artifactId);
      if (!artifact) {
        throw new ContentOutcomeError('artifact_not_found', 'content artifact not found', 404);
      }
    } else {
      artifact = await store.insertArtifact({
        tenant_id: tenantId,
        client_id: clientId,
        pending_comment_id: optionalText(
          input.pending_comment_id ?? input.pendingCommentId,
          'pending_comment_id'
        ),
        title: optionalText(input.title, 'title', 500),
        body: optionalText(input.body ?? input.content, 'body'),
        channel: optionalEnum(
          input.artifact_channel ?? input.channel,
          Object.values(CHANNELS),
          'channel',
          { allowUnknown: true }
        ),
        format: optionalText(input.format, 'format', 120),
        metadata: {
          source: input.pending_comment_id || input.pendingCommentId ? 'paige' : 'manual',
          ...(input.artifact_metadata && typeof input.artifact_metadata === 'object'
            ? input.artifact_metadata
            : {}),
        },
      });
      artifactId = artifact.id;
    }

    const channel =
      optionalEnum(input.channel, Object.values(CHANNELS), 'channel', {
        allowUnknown: true,
      }) || CHANNELS.LINKEDIN;

    const objective = optionalEnum(input.objective, OBJECTIVES, 'objective', {
      allowUnknown: true,
    });

    const publication = await store.insertPublication({
      tenant_id: tenantId,
      client_id: clientId,
      content_artifact_id: artifactId,
      channel,
      external_post_id: optionalText(input.external_post_id ?? input.externalPostId, 'external_post_id'),
      external_url: optionalText(input.external_url ?? input.externalUrl, 'external_url', 2000),
      published_at: requireIsoTimestamp(
        input.published_at ?? input.publishedAt ?? new Date().toISOString(),
        'published_at'
      ),
      objective,
      topic: optionalText(input.topic, 'topic', 500),
      thesis: optionalText(input.thesis, 'thesis'),
      format: optionalText(input.format, 'format', 120),
      intended_audience: normalizeAudience(input.intended_audience ?? input.intendedAudience),
      campaign_id: optionalText(input.campaign_id ?? input.campaignId, 'campaign_id'),
      linkedin_post_stats_id:
        input.linkedin_post_stats_id != null || input.linkedinPostStatsId != null
          ? Number(input.linkedin_post_stats_id ?? input.linkedinPostStatsId)
          : null,
    });

    // SPEC-013 observe: content published (executed) — evaluate-only, no strategy mutation.
    let canonicalOutcomeId = null;
    if (outcomeEngine && typeof outcomeEngine.record === 'function') {
      try {
        const rec = outcomeEngine.record({
          tenantId,
          recommendationId: `content-pub:${publication.id}`,
          lifecycle: 'executed',
          confidenceAtRecommendation: 0,
          notes: `Content published on ${channel}`,
          meta: {
            kind: 'content_publication',
            publicationId: publication.id,
            contentArtifactId: artifactId,
            objective,
            topic: publication.topic,
            channel,
          },
          evidenceSourceIds: [],
        });
        canonicalOutcomeId = rec && rec.id ? String(rec.id) : null;
      } catch {
        // never block content outcome path
      }
    }

    return {
      publication,
      artifact,
      canonical_outcome_id: canonicalOutcomeId,
    };
  }

  async function getPublication(tenantIdRaw, publicationId) {
    await ensureReady();
    const tenantId = normalizeTenantId(tenantIdRaw);
    const publication = await store.getPublication(tenantId, publicationId);
    if (!publication) {
      throw new ContentOutcomeError('publication_not_found', 'publication not found', 404);
    }
    return publication;
  }

  async function addPerformanceSnapshot(tenantIdRaw, publicationId, input = {}) {
    await ensureReady();
    const tenantId = normalizeTenantId(tenantIdRaw);
    const publication = await store.getPublication(tenantId, publicationId);
    if (!publication) {
      throw new ContentOutcomeError('publication_not_found', 'publication not found', 404);
    }

    const snapshot = await store.insertSnapshot({
      tenant_id: tenantId,
      client_id: publication.client_id,
      publication_id: publication.id,
      observed_at: requireIsoTimestamp(
        input.observed_at ?? input.observedAt ?? new Date().toISOString(),
        'observed_at'
      ),
      impressions: optionalNonNegativeInt(input.impressions, 'impressions'),
      members_reached: optionalNonNegativeInt(
        input.members_reached ?? input.membersReached,
        'members_reached'
      ),
      reactions: optionalNonNegativeInt(input.reactions, 'reactions'),
      comments: optionalNonNegativeInt(input.comments, 'comments'),
      reposts: optionalNonNegativeInt(input.reposts, 'reposts'),
      saves: optionalNonNegativeInt(input.saves, 'saves'),
      profile_views_attributed: optionalNonNegativeInt(
        input.profile_views_attributed ?? input.profileViewsAttributed,
        'profile_views_attributed'
      ),
      followers_gained: optionalNonNegativeInt(
        input.followers_gained ?? input.followersGained,
        'followers_gained'
      ),
      connection_requests: optionalNonNegativeInt(
        input.connection_requests ?? input.connectionRequests,
        'connection_requests'
      ),
      metadata:
        input.metadata && typeof input.metadata === 'object' ? input.metadata : {},
    });

    return snapshot;
  }

  async function addBusinessOutcome(tenantIdRaw, publicationId, input = {}) {
    await ensureReady();
    const tenantId = normalizeTenantId(tenantIdRaw);
    const publication = await store.getPublication(tenantId, publicationId);
    if (!publication) {
      throw new ContentOutcomeError('publication_not_found', 'publication not found', 404);
    }

    const outcomeType = optionalEnum(
      input.outcome_type ?? input.outcomeType,
      BUSINESS_OUTCOME_TYPES,
      'outcome_type',
      { required: true, allowUnknown: true }
    );
    const attribution =
      optionalEnum(
        input.attribution,
        ATTRIBUTION_LEVELS,
        'attribution',
        { allowUnknown: false }
      ) || 'unknown';

    let evidenceId = optionalText(input.evidence_id ?? input.evidenceId, 'evidence_id');
    const evidenceKind =
      optionalEnum(
        input.evidence_kind ?? input.evidenceKind,
        Object.values(EVIDENCE_KINDS),
        'evidence_kind',
        { allowUnknown: true }
      ) || EVIDENCE_KINDS.OPERATOR_OBSERVATION;

    if (!evidenceId && knowledge && typeof knowledge.writeEvidence === 'function') {
      try {
        const ev = await knowledge.writeEvidence({
          tenantId,
          sourceType:
            evidenceKind === EVIDENCE_KINDS.EXTERNAL_OBSERVATION
              ? 'content_outcome_external'
              : 'content_outcome_operator',
          sourceId: `content-outcome:${publication.id}`,
          summary:
            optionalText(input.description, 'description') ||
            `Business outcome ${outcomeType} for publication ${publication.id}`,
          confidence: optionalConfidence(input.confidence) ?? 0.6,
          payload: {
            publicationId: publication.id,
            outcomeType,
            attribution,
            evidenceKind,
          },
        });
        if (ev && ev.id) evidenceId = String(ev.id);
      } catch {
        // never block
      }
    }

    let canonicalOutcomeId = optionalText(
      input.canonical_outcome_id ?? input.canonicalOutcomeId,
      'canonical_outcome_id'
    );

    if (outcomeEngine && typeof outcomeEngine.record === 'function') {
      try {
        const rec = outcomeEngine.record({
          tenantId,
          recommendationId: `content-pub:${publication.id}:outcome:${Date.now()}`,
          lifecycle: 'observed',
          confidenceAtRecommendation: 0,
          confidenceAtOutcome: (optionalConfidence(input.confidence) ?? 0.5) * 100,
          notes: optionalText(input.description, 'description'),
          evidenceSourceIds: evidenceId ? [evidenceId] : [],
          meta: {
            kind: 'content_business_outcome',
            publicationId: publication.id,
            outcomeType,
            attribution,
          },
        });
        if (!canonicalOutcomeId && rec && rec.id) {
          canonicalOutcomeId = String(rec.id);
        }
      } catch {
        // never block
      }
    }

    const outcome = await store.insertBusinessOutcome({
      tenant_id: tenantId,
      client_id: publication.client_id,
      publication_id: publication.id,
      outcome_type: outcomeType,
      occurred_at: requireIsoTimestamp(
        input.occurred_at ?? input.occurredAt ?? new Date().toISOString(),
        'occurred_at'
      ),
      company_id: optionalText(input.company_id ?? input.companyId, 'company_id'),
      person_id: optionalText(input.person_id ?? input.personId, 'person_id'),
      interaction_id: optionalText(
        input.interaction_id ?? input.interactionId,
        'interaction_id'
      ),
      evidence_id: evidenceId,
      description: optionalText(input.description, 'description'),
      confidence: optionalConfidence(input.confidence),
      attribution,
      canonical_outcome_id: canonicalOutcomeId,
    });

    return outcome;
  }

  async function addQualitativeSignal(tenantIdRaw, publicationId, input = {}) {
    await ensureReady();
    const tenantId = normalizeTenantId(tenantIdRaw);
    const publication = await store.getPublication(tenantId, publicationId);
    if (!publication) {
      throw new ContentOutcomeError('publication_not_found', 'publication not found', 404);
    }

    const description = optionalText(input.description, 'description');
    if (!description) {
      throw new ContentOutcomeError('required_field', 'description is required');
    }

    const signalType = optionalEnum(
      input.signal_type ?? input.signalType,
      SIGNAL_TYPES,
      'signal_type',
      { required: true, allowUnknown: true }
    );

    let evidenceId = optionalText(input.evidence_id ?? input.evidenceId, 'evidence_id');
    if (!evidenceId && knowledge && typeof knowledge.writeEvidence === 'function') {
      try {
        const ev = await knowledge.writeEvidence({
          tenantId,
          sourceType: 'content_qualitative_signal',
          sourceId: `content-signal:${publication.id}`,
          summary: description,
          confidence: 0.55,
          payload: {
            publicationId: publication.id,
            signalType,
            evidenceKind: EVIDENCE_KINDS.OPERATOR_OBSERVATION,
          },
        });
        if (ev && ev.id) evidenceId = String(ev.id);
      } catch {
        // never block
      }
    }

    return store.insertSignal({
      tenant_id: tenantId,
      client_id: publication.client_id,
      publication_id: publication.id,
      observed_at: requireIsoTimestamp(
        input.observed_at ?? input.observedAt ?? new Date().toISOString(),
        'observed_at'
      ),
      signal_type: signalType,
      description,
      audience_type: optionalText(input.audience_type ?? input.audienceType, 'audience_type'),
      sentiment: optionalText(input.sentiment, 'sentiment', 80),
      strength: optionalText(input.strength, 'strength', 80),
      evidence_id: evidenceId,
    });
  }

  /**
   * Complete outcome history for one publication.
   */
  async function getPublicationOutcome(tenantIdRaw, publicationId) {
    await ensureReady();
    const tenantId = normalizeTenantId(tenantIdRaw);
    const publication = await store.getPublication(tenantId, publicationId);
    if (!publication) {
      throw new ContentOutcomeError('publication_not_found', 'publication not found', 404);
    }
    const artifact = await store.getArtifact(tenantId, publication.content_artifact_id);
    const [
      performance_snapshots,
      business_outcomes,
      qualitative_signals,
    ] = await Promise.all([
      store.listSnapshots(tenantId, publication.id),
      store.listBusinessOutcomes(tenantId, publication.id),
      store.listSignals(tenantId, publication.id),
    ]);

    const evidence_references = collectEvidenceRefs(
      business_outcomes,
      qualitative_signals
    );

    return {
      publication,
      content_artifact: artifact,
      performance_snapshots,
      business_outcomes,
      qualitative_signals,
      evidence_references,
      timeline: buildTimeline(
        performance_snapshots,
        business_outcomes,
        qualitative_signals
      ),
    };
  }

  async function getContentOutcomeTimeline(tenantIdRaw, publicationId) {
    const full = await getPublicationOutcome(tenantIdRaw, publicationId);
    return {
      publication_id: full.publication.id,
      timeline: full.timeline,
    };
  }

  async function listContentOutcomes(filters = {}) {
    await ensureReady();
    const clientId =
      filters.clientId != null || filters.client_id != null
        ? normalizeClientId(filters.clientId ?? filters.client_id)
        : null;
    const tenantId = normalizeTenantId(
      filters.tenantId ?? filters.tenant_id ?? clientId
    );

    const publications = await store.listPublications(tenantId, {
      channel: optionalEnum(filters.channel, Object.values(CHANNELS), 'channel', {
        allowUnknown: true,
      }),
      objective: optionalEnum(filters.objective, OBJECTIVES, 'objective', {
        allowUnknown: true,
      }),
      topic: optionalText(filters.topic, 'topic', 500),
      format: optionalText(filters.format, 'format', 120),
      dateFrom: optionalIsoTimestamp(filters.dateFrom ?? filters.date_from, 'dateFrom'),
      dateTo: optionalIsoTimestamp(filters.dateTo ?? filters.date_to, 'dateTo'),
      limit: filters.limit != null ? Number(filters.limit) : 50,
    });

    const ids = publications.map((p) => p.id);
    const [snapshots, outcomes, signals] = await Promise.all([
      store.listSnapshotsForPublications(tenantId, ids),
      store.listBusinessOutcomesForPublications(tenantId, ids),
      store.listSignalsForPublications(tenantId, ids),
    ]);

    const byPub = (rows, key = 'publication_id') => {
      const map = new Map();
      for (const row of rows) {
        const k = String(row[key]);
        if (!map.has(k)) map.set(k, []);
        map.get(k).push(row);
      }
      return map;
    };

    const snapMap = byPub(snapshots);
    const outMap = byPub(outcomes);
    const sigMap = byPub(signals);

    const items = [];
    for (const publication of publications) {
      const artifact = await store.getArtifact(tenantId, publication.content_artifact_id);
      const performance_snapshots = snapMap.get(String(publication.id)) || [];
      const business_outcomes = outMap.get(String(publication.id)) || [];
      const qualitative_signals = sigMap.get(String(publication.id)) || [];
      items.push({
        publication,
        content_artifact: artifact,
        performance_snapshots,
        business_outcomes,
        qualitative_signals,
        evidence_references: collectEvidenceRefs(business_outcomes, qualitative_signals),
      });
    }

    const comparison = buildComparisonSummary(publications, snapshots, outcomes);
    const grouped = {
      objective: groupByDimension(publications, 'objective', snapshots, outcomes),
      topic: groupByDimension(publications, 'topic', snapshots, outcomes),
      format: groupByDimension(publications, 'format', snapshots, outcomes),
      intended_audience: groupByDimension(
        publications,
        'intended_audience',
        snapshots,
        outcomes
      ),
    };

    return {
      tenant_id: tenantId,
      items,
      comparison,
      grouped,
    };
  }

  async function getRecentContentOutcomes(tenantIdRaw, limit = 5) {
    return listContentOutcomes({
      tenantId: tenantIdRaw,
      limit: Math.min(Math.max(Number(limit) || 5, 1), 50),
    });
  }

  return {
    ensureReady,
    createPublication,
    getPublication,
    addPerformanceSnapshot,
    addBusinessOutcome,
    addQualitativeSignal,
    getPublicationOutcome,
    getContentOutcomeTimeline,
    listContentOutcomes,
    getRecentContentOutcomes,
    // constants for UI/CLI
    constants: {
      CHANNELS,
      OBJECTIVES,
      BUSINESS_OUTCOME_TYPES,
      ATTRIBUTION_LEVELS,
      SIGNAL_TYPES,
      EVIDENCE_KINDS,
    },
  };
}

function collectEvidenceRefs(businessOutcomes, qualitativeSignals) {
  const refs = [];
  for (const row of businessOutcomes || []) {
    if (row.evidence_id) {
      refs.push({
        evidence_id: row.evidence_id,
        source: 'business_outcome',
        source_id: row.id,
        kind: 'linked',
      });
    }
  }
  for (const row of qualitativeSignals || []) {
    if (row.evidence_id) {
      refs.push({
        evidence_id: row.evidence_id,
        source: 'qualitative_signal',
        source_id: row.id,
        kind: 'linked',
      });
    }
  }
  return refs;
}

function buildTimeline(snapshots, outcomes, signals) {
  const events = [];
  for (const s of snapshots || []) {
    events.push({
      at: s.observed_at,
      kind: 'performance_snapshot',
      id: s.id,
      summary: summarizeSnapshot(s),
      data: s,
    });
  }
  for (const o of outcomes || []) {
    events.push({
      at: o.occurred_at,
      kind: 'business_outcome',
      id: o.id,
      summary: `${o.outcome_type} (${o.attribution})`,
      data: o,
    });
  }
  for (const q of signals || []) {
    events.push({
      at: q.observed_at,
      kind: 'qualitative_signal',
      id: q.id,
      summary: `${q.signal_type}: ${truncate(q.description, 120)}`,
      data: q,
    });
  }
  events.sort((a, b) => new Date(a.at).getTime() - new Date(b.at).getTime());
  return events;
}

function summarizeSnapshot(s) {
  const parts = [];
  if (s.impressions != null) parts.push(`${s.impressions} impressions`);
  if (s.reactions != null) parts.push(`${s.reactions} reactions`);
  if (s.comments != null) parts.push(`${s.comments} comments`);
  if (s.reposts != null) parts.push(`${s.reposts} reposts`);
  if (s.followers_gained != null) parts.push(`${s.followers_gained} followers`);
  return parts.length ? parts.join(', ') : 'performance snapshot';
}

function truncate(text, n) {
  const s = String(text || '');
  return s.length <= n ? s : `${s.slice(0, n - 1)}…`;
}

/** Default singleton for routes/CLI (lazy Postgres). */
let _defaultService = null;

function getContentOutcomeService(options = {}) {
  if (options.store || options.pool || options.outcomeEngine || options.knowledge) {
    return createContentOutcomeService(options);
  }
  if (!_defaultService) {
    let pool = null;
    try {
      pool = require('../../db');
    } catch {
      pool = null;
    }
    _defaultService = createContentOutcomeService({ pool: pool || undefined });
  }
  return _defaultService;
}

function resetContentOutcomeServiceForTests() {
  _defaultService = null;
}

module.exports = {
  createContentOutcomeService,
  getContentOutcomeService,
  resetContentOutcomeServiceForTests,
  createMemoryContentOutcomeStore,
  createPostgresContentOutcomeStore,
  ContentOutcomeError,
  CHANNELS,
  OBJECTIVES,
  BUSINESS_OUTCOME_TYPES,
  ATTRIBUTION_LEVELS,
  SIGNAL_TYPES,
  EVIDENCE_KINDS,
};
