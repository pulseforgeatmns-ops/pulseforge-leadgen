'use strict';

/**
 * SPEC-096 — Max Specialist Direction & Operator Rationale (v1 thin slice).
 *
 * Max interprets natural-language operator feedback on specialist
 * recommendations, persists direction + rationale, delegates refinement
 * to Paige, and stores operator-sourced learnings. Read-only advisory —
 * no publishing, execution, or autonomous strategy mutation.
 */

const crypto = require('crypto');
const {
  refineContentRecommendation,
  createOperatorDirectionLearning,
  ContentLearningError,
} = require('./contentLearning');

const SPECIALISTS = Object.freeze(['paige']);
const DISPOSITIONS = Object.freeze(['accept', 'refine', 'reject']);
const SCOPES = Object.freeze([
  'recommendation_only',
  'experiment_campaign',
  'durable_preference',
  'business_constraint',
]);
const REFINEMENT_STATES = Object.freeze([
  'interpreted',
  'clarification_needed',
  'delegated',
  'completed',
  'failed',
]);
const RECOMMENDATION_STATUSES = Object.freeze([
  'pending',
  'accepted',
  'refined',
  'rejected',
  'superseded',
  'failed',
]);

class SpecialistDirectionError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {number} [status]
   */
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'SpecialistDirectionError';
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

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function normalizeMessage(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/['']/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function toPublicRecommendation(row) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: row.tenantId,
    clientId: row.clientId,
    specialist: row.specialist,
    kind: row.kind,
    parentRecommendationId: row.parentRecommendationId || null,
    campaignId: row.campaignId || null,
    objectiveId: row.objectiveId || null,
    objective: row.objective || null,
    channel: row.channel || null,
    recommendedDirection: row.recommendedDirection,
    reason: row.reason || '',
    confidence: row.confidence == null ? null : Number(row.confidence),
    payload: row.payload && typeof row.payload === 'object' ? row.payload : {},
    status: row.status,
    supportingLearningIds: Array.isArray(row.supportingLearningIds)
      ? row.supportingLearningIds.slice()
      : [],
    supportingPublicationIds: Array.isArray(row.supportingPublicationIds)
      ? row.supportingPublicationIds.slice()
      : [],
    provenance: row.provenance && typeof row.provenance === 'object' ? row.provenance : {},
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function toPublicDirection(row) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: row.tenantId,
    clientId: row.clientId,
    specialist: row.specialist,
    sourceRecommendationId: row.sourceRecommendationId,
    sourceObjectiveId: row.sourceObjectiveId || null,
    sourceCampaignId: row.sourceCampaignId || null,
    operatorMessage: row.operatorMessage,
    disposition: row.disposition,
    acceptedElements: Array.isArray(row.acceptedElements) ? row.acceptedElements.slice() : [],
    changedElements: Array.isArray(row.changedElements) ? row.changedElements.slice() : [],
    updatedDirection: row.updatedDirection || null,
    rationale: row.rationale || '',
    scope: row.scope,
    confidence: row.confidence == null ? null : Number(row.confidence),
    refinementState: row.refinementState,
    resultingRecommendationId: row.resultingRecommendationId || null,
    operatorLearningId: row.operatorLearningId || null,
    metadata: row.metadata && typeof row.metadata === 'object' ? row.metadata : {},
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

/* -------------------------------------------------------------------------- */
/* Persistence stores                                                          */
/* -------------------------------------------------------------------------- */

function createMemoryStore() {
  /** @type {Map<string, object>} */
  const recommendations = new Map();
  /** @type {Map<string, object>} */
  const directions = new Map();

  return {
    kind: 'memory',
    async insertRecommendation(row) {
      recommendations.set(row.id, clone(row));
      return clone(row);
    },
    async updateRecommendation(row) {
      if (!recommendations.has(row.id)) return null;
      recommendations.set(row.id, clone(row));
      return clone(row);
    },
    async getRecommendation(id, tenantId) {
      const row = recommendations.get(id);
      if (!row) return null;
      if (tenantId != null && String(row.tenantId) !== String(tenantId)) return null;
      return clone(row);
    },
    async listRecommendations(filter = {}) {
      let list = [...recommendations.values()];
      if (filter.tenantId != null) {
        list = list.filter((r) => String(r.tenantId) === String(filter.tenantId));
      }
      if (filter.campaignId) {
        list = list.filter((r) => r.campaignId === filter.campaignId);
      }
      if (filter.status) {
        list = list.filter((r) => r.status === filter.status);
      }
      list.sort(
        (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
      );
      if (filter.limit != null) list = list.slice(0, filter.limit);
      return list.map(clone);
    },
    async insertDirection(row) {
      directions.set(row.id, clone(row));
      return clone(row);
    },
    async updateDirection(row) {
      if (!directions.has(row.id)) return null;
      directions.set(row.id, clone(row));
      return clone(row);
    },
    async getDirection(id, tenantId) {
      const row = directions.get(id);
      if (!row) return null;
      if (tenantId != null && String(row.tenantId) !== String(tenantId)) return null;
      return clone(row);
    },
    async listDirections(filter = {}) {
      let list = [...directions.values()];
      if (filter.tenantId != null) {
        list = list.filter((r) => String(r.tenantId) === String(filter.tenantId));
      }
      if (filter.sourceRecommendationId) {
        list = list.filter(
          (r) => r.sourceRecommendationId === filter.sourceRecommendationId
        );
      }
      if (filter.sourceCampaignId) {
        list = list.filter((r) => r.sourceCampaignId === filter.sourceCampaignId);
      }
      if (filter.scope) {
        list = list.filter((r) => r.scope === filter.scope);
      }
      if (filter.refinementState) {
        list = list.filter((r) => r.refinementState === filter.refinementState);
      }
      list.sort(
        (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
      );
      if (filter.limit != null) list = list.slice(0, filter.limit);
      return list.map(clone);
    },
    clear() {
      recommendations.clear();
      directions.clear();
    },
  };
}

function mapRecommendationRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: row.tenant_id,
    clientId: Number(row.client_id),
    specialist: row.specialist,
    kind: row.kind,
    parentRecommendationId: row.parent_recommendation_id || null,
    campaignId: row.campaign_id || null,
    objectiveId: row.objective_id || null,
    objective: row.objective || null,
    channel: row.channel || null,
    recommendedDirection: row.recommended_direction,
    reason: row.reason || '',
    confidence: row.confidence == null ? null : Number(row.confidence),
    payload: row.payload || {},
    status: row.status,
    supportingLearningIds: row.supporting_learning_ids || [],
    supportingPublicationIds: row.supporting_publication_ids || [],
    provenance: row.provenance || {},
    createdAt:
      row.created_at instanceof Date
        ? row.created_at.toISOString()
        : String(row.created_at),
    updatedAt:
      row.updated_at instanceof Date
        ? row.updated_at.toISOString()
        : String(row.updated_at),
  };
}

function mapDirectionRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: row.tenant_id,
    clientId: Number(row.client_id),
    specialist: row.specialist,
    sourceRecommendationId: row.source_recommendation_id,
    sourceObjectiveId: row.source_objective_id || null,
    sourceCampaignId: row.source_campaign_id || null,
    operatorMessage: row.operator_message,
    disposition: row.disposition,
    acceptedElements: row.accepted_elements || [],
    changedElements: row.changed_elements || [],
    updatedDirection: row.updated_direction || null,
    rationale: row.rationale || '',
    scope: row.scope,
    confidence: row.confidence == null ? null : Number(row.confidence),
    refinementState: row.refinement_state,
    resultingRecommendationId: row.resulting_recommendation_id || null,
    operatorLearningId: row.operator_learning_id || null,
    metadata: row.metadata || {},
    createdAt:
      row.created_at instanceof Date
        ? row.created_at.toISOString()
        : String(row.created_at),
    updatedAt:
      row.updated_at instanceof Date
        ? row.updated_at.toISOString()
        : String(row.updated_at),
  };
}

function createPostgresStore(pool) {
  const db = pool || require('../db');
  return {
    kind: 'postgres',
    async insertRecommendation(row) {
      const result = await db.query(
        `INSERT INTO content_recommendations (
          id, tenant_id, client_id, specialist, kind, parent_recommendation_id,
          campaign_id, objective_id, objective, channel, recommended_direction,
          reason, confidence, payload, status, supporting_learning_ids,
          supporting_publication_ids, provenance, created_at, updated_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15,$16,$17,$18::jsonb,$19,$20
        ) RETURNING *`,
        [
          row.id,
          String(row.tenantId),
          row.clientId,
          row.specialist,
          row.kind,
          row.parentRecommendationId,
          row.campaignId,
          row.objectiveId,
          row.objective,
          row.channel,
          row.recommendedDirection,
          row.reason,
          row.confidence,
          JSON.stringify(row.payload || {}),
          row.status,
          row.supportingLearningIds || [],
          row.supportingPublicationIds || [],
          JSON.stringify(row.provenance || {}),
          row.createdAt,
          row.updatedAt,
        ]
      );
      return mapRecommendationRow(result.rows[0]);
    },
    async updateRecommendation(row) {
      const result = await db.query(
        `UPDATE content_recommendations SET
          status = $2,
          payload = $3::jsonb,
          provenance = $4::jsonb,
          updated_at = $5
        WHERE id = $1 AND tenant_id = $6
        RETURNING *`,
        [
          row.id,
          row.status,
          JSON.stringify(row.payload || {}),
          JSON.stringify(row.provenance || {}),
          row.updatedAt,
          String(row.tenantId),
        ]
      );
      return mapRecommendationRow(result.rows[0] || null);
    },
    async getRecommendation(id, tenantId) {
      const result = await db.query(
        `SELECT * FROM content_recommendations WHERE id = $1 AND tenant_id = $2`,
        [id, String(tenantId)]
      );
      return mapRecommendationRow(result.rows[0] || null);
    },
    async listRecommendations(filter = {}) {
      const params = [String(filter.tenantId)];
      let sql = `SELECT * FROM content_recommendations WHERE tenant_id = $1`;
      if (filter.campaignId) {
        params.push(filter.campaignId);
        sql += ` AND campaign_id = $${params.length}`;
      }
      if (filter.status) {
        params.push(filter.status);
        sql += ` AND status = $${params.length}`;
      }
      sql += ` ORDER BY created_at DESC`;
      if (filter.limit != null) {
        params.push(filter.limit);
        sql += ` LIMIT $${params.length}`;
      }
      const result = await db.query(sql, params);
      return result.rows.map(mapRecommendationRow);
    },
    async insertDirection(row) {
      const result = await db.query(
        `INSERT INTO specialist_directions (
          id, tenant_id, client_id, specialist, source_recommendation_id,
          source_objective_id, source_campaign_id, operator_message, disposition,
          accepted_elements, changed_elements, updated_direction, rationale, scope,
          confidence, refinement_state, resulting_recommendation_id,
          operator_learning_id, metadata, created_at, updated_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19::jsonb,$20,$21
        ) RETURNING *`,
        [
          row.id,
          String(row.tenantId),
          row.clientId,
          row.specialist,
          row.sourceRecommendationId,
          row.sourceObjectiveId,
          row.sourceCampaignId,
          row.operatorMessage,
          row.disposition,
          row.acceptedElements || [],
          row.changedElements || [],
          row.updatedDirection,
          row.rationale,
          row.scope,
          row.confidence,
          row.refinementState,
          row.resultingRecommendationId,
          row.operatorLearningId,
          JSON.stringify(row.metadata || {}),
          row.createdAt,
          row.updatedAt,
        ]
      );
      return mapDirectionRow(result.rows[0]);
    },
    async updateDirection(row) {
      const result = await db.query(
        `UPDATE specialist_directions SET
          refinement_state = $2,
          resulting_recommendation_id = $3,
          operator_learning_id = $4,
          metadata = $5::jsonb,
          updated_at = $6
        WHERE id = $1 AND tenant_id = $7
        RETURNING *`,
        [
          row.id,
          row.refinementState,
          row.resultingRecommendationId,
          row.operatorLearningId,
          JSON.stringify(row.metadata || {}),
          row.updatedAt,
          String(row.tenantId),
        ]
      );
      return mapDirectionRow(result.rows[0] || null);
    },
    async getDirection(id, tenantId) {
      const result = await db.query(
        `SELECT * FROM specialist_directions WHERE id = $1 AND tenant_id = $2`,
        [id, String(tenantId)]
      );
      return mapDirectionRow(result.rows[0] || null);
    },
    async listDirections(filter = {}) {
      const params = [String(filter.tenantId)];
      let sql = `SELECT * FROM specialist_directions WHERE tenant_id = $1`;
      if (filter.sourceRecommendationId) {
        params.push(filter.sourceRecommendationId);
        sql += ` AND source_recommendation_id = $${params.length}`;
      }
      if (filter.sourceCampaignId) {
        params.push(filter.sourceCampaignId);
        sql += ` AND source_campaign_id = $${params.length}`;
      }
      if (filter.scope) {
        params.push(filter.scope);
        sql += ` AND scope = $${params.length}`;
      }
      if (filter.refinementState) {
        params.push(filter.refinementState);
        sql += ` AND refinement_state = $${params.length}`;
      }
      sql += ` ORDER BY created_at DESC`;
      if (filter.limit != null) {
        params.push(filter.limit);
        sql += ` LIMIT $${params.length}`;
      }
      const result = await db.query(sql, params);
      return result.rows.map(mapDirectionRow);
    },
  };
}

function resolveStore(opts = {}) {
  if (opts.store) return opts.store;
  if (process.env.DATABASE_URL) {
    try {
      return createPostgresStore(opts.pool);
    } catch (_err) {
      return createMemoryStore();
    }
  }
  return createMemoryStore();
}

/* -------------------------------------------------------------------------- */
/* Interpretation — deterministic operator feedback parsing                    */
/* -------------------------------------------------------------------------- */

function looksLikeAcceptAction(message) {
  const q = normalizeMessage(message);
  return (
    /^(accept|yes|looks good|approved|go with that|that works)\b/.test(q) ||
    /\baccept (this )?(direction|recommendation)\b/.test(q)
  );
}

function looksLikeRejectAction(message) {
  const q = normalizeMessage(message);
  return (
    /\b(reject|no don t|don t proceed|not that|pass on)\b/.test(q) &&
    !/\bbut\b/.test(q)
  );
}

function looksLikeRefinementFeedback(message) {
  const q = normalizeMessage(message);
  if (looksLikeAcceptAction(q) || looksLikeRejectAction(q)) return false;
  return (
    /\b(don t like|don t want|not that|instead|rather|but|however|move|shift|aim|toward|without losing|keep|preserve|too technical|attracting)\b/.test(
      q
    ) || q.length > 20
  );
}

function looksLikeDirectionRecoveryQuestion(message) {
  const q = normalizeMessage(message);
  return (
    /\bwhy (is|are|did|does) paige\b/.test(q) ||
    /\bwhy (are we|did we|do we) (moving|aiming|shifting)\b/.test(q) ||
    /\bwhat did i (direct|tell|say|ask)\b/.test(q) ||
    /\bprevious(ly)? directed\b/.test(q) ||
    /\boperator direction\b/.test(q)
  );
}

function looksLikeScopeAmbiguity(message) {
  const q = normalizeMessage(message);
  // Short technical feedback without campaign/recommendation anchors
  if (q.length > 80) return false;
  return (
    /^(thats|that s)( too)? technical$/.test(q) ||
    /^(don t|do not) (be|get|go) (so )?technical$/.test(q) ||
    /^too (salesy|corporate|jargony)$/.test(q)
  );
}

/**
 * Interpret operator natural-language feedback against a recommendation.
 *
 * @param {object} input
 * @param {string} input.operatorMessage
 * @param {object} input.recommendation
 * @returns {object}
 */
function interpretOperatorFeedback(input = {}) {
  const message = asText(input.operatorMessage) || '';
  const recommendation = input.recommendation || {};
  const norm = normalizeMessage(message);
  const experiment = recommendation.payload?.experiment || recommendation.experiment || {};

  if (looksLikeScopeAmbiguity(message)) {
    return {
      disposition: 'refine',
      needsClarification: true,
      clarificationPrompt:
        'Should I treat that as feedback on this recommendation only, or do you want Paige to move the broader LinkedIn campaign away from technical arguments?',
      acceptedElements: [],
      changedElements: [],
      updatedDirection: null,
      rationale: '',
      scope: null,
      confidence: 0.4,
    };
  }

  let disposition = 'refine';
  if (looksLikeAcceptAction(message)) disposition = 'accept';
  else if (looksLikeRejectAction(message)) disposition = 'reject';

  const acceptedElements = [];
  const changedElements = [];

  if (
    /\b(experiment structure|controlled experiment|experiment)\b/.test(norm) ||
    /\blike the experiment\b/.test(norm)
  ) {
    acceptedElements.push('controlled-experiment structure');
  }
  if (
    /\b(operator first|operator centered|operator centered framing|operator first framing)\b/.test(
      norm
    ) ||
    /\bwithout losing the operator/.test(norm)
  ) {
    acceptedElements.push('operator-centered framing');
  }
  if (/\blike the (structure|framing|approach)\b/.test(norm)) {
    if (!acceptedElements.includes('controlled-experiment structure')) {
      acceptedElements.push('useful recommendation structure');
    }
  }

  if (
    /\b(argument|direction|thesis|hook|wording|angle)\b/.test(norm) ||
    /\bdon t like\b/.test(norm) ||
    /\btoo technical\b/.test(norm)
  ) {
    changedElements.push('specific argument');
  }
  if (/\b(smb|small business|operator|audience|buyer|accessible)\b/.test(norm)) {
    changedElements.push('audience accessibility');
  }
  if (/\b(attracting|attract)\b.*\b(engineer|builder|technical|ai builder)\b/.test(norm)) {
    changedElements.push('audience composition');
  }

  // Default preserve from recommendation experiment block
  if (!acceptedElements.length && Array.isArray(experiment.preserve)) {
    for (const p of experiment.preserve.slice(0, 3)) {
      acceptedElements.push(String(p));
    }
  }
  if (!changedElements.length && Array.isArray(experiment.vary)) {
    for (const v of experiment.vary.slice(0, 2)) {
      changedElements.push(String(v));
    }
  }

  let updatedDirection = null;
  let rationale = '';
  let scope = 'recommendation_only';

  if (disposition === 'refine') {
    if (/\b(smb|small business)\b/.test(norm)) {
      updatedDirection =
        'Shift the argument toward an SMB-accessible, operator-first AI problem without requiring technical fluency.';
    } else if (/\bmore (direct|accessible|operator)\b/.test(norm)) {
      updatedDirection =
        'Make the argument more immediately accessible to the target operator audience while preserving operator-centered framing.';
    } else if (/\btoo technical\b/.test(norm)) {
      updatedDirection =
        'Reduce technical terminology; explain through operating problems rather than AI/system mechanics.';
    } else {
      updatedDirection =
        'Revise the specific argument while preserving the useful structure and framing from the original recommendation.';
    }

    if (
      /\bwe re already attracting\b/.test(norm) ||
      /\b(already|successfully|strongly)\b.*\b(attracting|attract)\b.*\b(engineer|builder|technical|ai)\b/.test(
        norm
      ) ||
      /\b(attracting|attract)\b.*\b(engineer|builder|technical|ai)\b.*\b(already|successfully)\b/.test(
        norm
      )
    ) {
      rationale =
        'Technical audiences are already responding strongly; the current strategic need is greater relevance among potential SMB buyers.';
    } else if (/\bbecause\b/.test(norm)) {
      const becauseIdx = norm.indexOf('because');
      rationale = message.slice(becauseIdx).replace(/^because\s*/i, '').trim();
      if (rationale) {
        rationale = rationale.charAt(0).toUpperCase() + rationale.slice(1);
        if (!/[.!?]$/.test(rationale)) rationale += '.';
      }
    } else if (/\bwe re (trying|already|currently)\b/.test(norm)) {
      rationale =
        'Operator judgment indicates the proposed direction does not match the current audience objective.';
    } else {
      rationale =
        'Operator requested a refinement to better align the recommendation with current strategic priorities.';
    }

    if (
      /\b(this (one|post|recommendation) only|just this|only this recommendation)\b/.test(norm)
    ) {
      scope = 'recommendation_only';
    } else if (
      /\b(campaign|linkedin campaign|next few|broader|ongoing|objective)\b/.test(norm)
    ) {
      scope = 'experiment_campaign';
    } else if (
      /\b(always|generally|prefer|in general|by default|durable|going forward)\b/.test(norm)
    ) {
      scope = 'durable_preference';
    } else if (/\b(cannot|can t|must not|budget|constraint|limit)\b/.test(norm)) {
      scope = 'business_constraint';
    } else if (recommendation.campaignId) {
      scope = 'experiment_campaign';
    }
  }

  return {
    disposition,
    needsClarification: false,
    clarificationPrompt: null,
    acceptedElements: [...new Set(acceptedElements)],
    changedElements: [...new Set(changedElements)],
    updatedDirection,
    rationale,
    scope,
    confidence: disposition === 'accept' ? 0.95 : 0.82,
  };
}

function detectContradiction(newDirection, priorDirections = []) {
  const norm = normalizeMessage(
    [newDirection.operatorMessage, newDirection.updatedDirection].filter(Boolean).join(' ')
  );
  const campaignDirs = priorDirections.filter(
    (d) =>
      d.scope === 'experiment_campaign' &&
      d.refinementState === 'completed' &&
      d.disposition === 'refine'
  );
  for (const prior of campaignDirs) {
    const priorNorm = normalizeMessage(
      [prior.updatedDirection, prior.rationale, ...(prior.changedElements || [])].join(' ')
    );
    const priorMovedAwayFromTechnical =
      /\b(away from|move away|less|reduce|attracting|attract)\b.*\b(technical|engineer|builder|ai builder)\b/.test(
        priorNorm
      ) ||
      /\b(smb|small business|accessible|operator)\b/.test(priorNorm);
    const newWantsTechnical =
      /\b(deep technically|go technical|more technical|technical argument|go deep)\b/.test(norm);
    if (priorMovedAwayFromTechnical && newWantsTechnical) {
      return {
        contradictory: true,
        priorDirectionId: prior.id,
        kind: 'campaign_direction_change',
        prompt:
          'Your earlier direction moved this campaign away from technical arguments. Should I treat this as a campaign-direction change, or a one-off exception for this recommendation?',
      };
    }
  }
  return { contradictory: false };
}

/* -------------------------------------------------------------------------- */
/* Core operations                                                             */
/* -------------------------------------------------------------------------- */

/**
 * Persist a Paige (or other specialist) recommendation for later direction.
 */
async function persistContentRecommendation(input = {}, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = String(input.tenantId ?? input.clientId);
  const clientId = asClientId(input.clientId ?? input.tenantId);
  if (clientId == null) {
    throw new SpecialistDirectionError('tenant_required', 'tenantId / clientId required');
  }

  const payload = input.recommendation || input.payload || input;
  const ts = nowIso();
  const row = {
    id: input.id || newId(),
    tenantId,
    clientId,
    specialist: asText(input.specialist) || 'paige',
    kind: asText(input.kind) || 'paige_campaign_content_recommendation',
    parentRecommendationId: asText(input.parentRecommendationId) || null,
    campaignId:
      asText(input.campaignId) ||
      asText(payload.campaignId) ||
      asText(payload.campaign_id) ||
      null,
    objectiveId: asText(input.objectiveId) || null,
    objective: asText(input.objective) || asText(payload.objective) || null,
    channel: asText(input.channel) || asText(payload.channel) || 'linkedin',
    recommendedDirection:
      asText(input.recommendedDirection) ||
      asText(payload.recommendedDirection) ||
      asText(payload.recommended_direction) ||
      '',
    reason: asText(input.reason) || asText(payload.reason) || '',
    confidence:
      input.confidence != null
        ? Number(input.confidence)
        : payload.confidence != null
          ? Number(payload.confidence)
          : null,
    payload: typeof payload === 'object' ? payload : {},
    status: 'pending',
    supportingLearningIds: (
      input.supportingLearningIds ||
      payload.supportingLearningIds ||
      payload.supporting_learning_ids ||
      (payload.payload && payload.payload.supporting_learning_ids) ||
      []
    ).map(String),
    supportingPublicationIds: (
      input.supportingPublicationIds ||
      payload.supportingPublicationIds ||
      payload.supporting_publication_ids ||
      (payload.payload && payload.payload.supporting_publication_ids) ||
      []
    ).map(String),
    provenance: {
      source: asText(input.source) || 'spec_094_paige_delegation',
      generatedAt: ts,
      ...(input.provenance || {}),
    },
    createdAt: ts,
    updatedAt: ts,
  };

  if (!row.recommendedDirection) {
    throw new SpecialistDirectionError(
      'invalid_recommendation',
      'recommendedDirection required'
    );
  }

  const saved = await store.insertRecommendation(row);
  return toPublicRecommendation(saved);
}

async function getContentRecommendation(id, tenantId, opts = {}) {
  const store = resolveStore(opts);
  const row = await store.getRecommendation(id, tenantId);
  if (!row) {
    throw new SpecialistDirectionError('recommendation_not_found', 'recommendation not found', 404);
  }
  return toPublicRecommendation(row);
}

/**
 * Apply operator direction to a persisted recommendation.
 */
async function applyOperatorDirection(input = {}, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = String(input.tenantId ?? input.clientId);
  const clientId = asClientId(input.clientId ?? input.tenantId);
  const operatorMessage = asText(input.operatorMessage || input.question);
  const recommendationId = asText(
    input.recommendationId || input.sourceRecommendationId
  );

  if (!operatorMessage) {
    throw new SpecialistDirectionError('message_required', 'operator message required');
  }
  if (!recommendationId) {
    throw new SpecialistDirectionError(
      'recommendation_required',
      'recommendationId required'
    );
  }

  const recommendation = await store.getRecommendation(recommendationId, tenantId);
  if (!recommendation) {
    throw new SpecialistDirectionError('recommendation_not_found', 'recommendation not found', 404);
  }

  const interpretation = interpretOperatorFeedback({
    operatorMessage,
    recommendation: toPublicRecommendation(recommendation),
  });

  if (interpretation.needsClarification) {
    const ts = nowIso();
    const directionRow = {
      id: newId(),
      tenantId,
      clientId,
      specialist: recommendation.specialist,
      sourceRecommendationId: recommendation.id,
      sourceObjectiveId: recommendation.objectiveId,
      sourceCampaignId: recommendation.campaignId,
      operatorMessage,
      disposition: 'refine',
      acceptedElements: [],
      changedElements: [],
      updatedDirection: null,
      rationale: '',
      scope: 'recommendation_only',
      confidence: interpretation.confidence,
      refinementState: 'clarification_needed',
      resultingRecommendationId: null,
      operatorLearningId: null,
      metadata: { clarificationPrompt: interpretation.clarificationPrompt },
      createdAt: ts,
      updatedAt: ts,
    };
    await store.insertDirection(directionRow);
    return {
      ok: true,
      handled: true,
      needsClarification: true,
      clarificationPrompt: interpretation.clarificationPrompt,
      direction: toPublicDirection(directionRow),
      recommendation: toPublicRecommendation(recommendation),
    };
  }

  const priorDirections = await store.listDirections({
    tenantId,
    sourceCampaignId: recommendation.campaignId || undefined,
    limit: 10,
  });
  const contradiction = detectContradiction(
    { ...interpretation, operatorMessage },
    priorDirections
  );
  if (contradiction.contradictory) {
    return {
      ok: true,
      handled: true,
      needsClarification: true,
      clarificationPrompt: contradiction.prompt,
      contradiction: true,
      priorDirectionId: contradiction.priorDirectionId,
      recommendation: toPublicRecommendation(recommendation),
    };
  }

  const ts = nowIso();
  const directionRow = {
    id: newId(),
    tenantId,
    clientId,
    specialist: recommendation.specialist,
    sourceRecommendationId: recommendation.id,
    sourceObjectiveId: recommendation.objectiveId,
    sourceCampaignId: recommendation.campaignId,
    operatorMessage,
    disposition: interpretation.disposition,
    acceptedElements: interpretation.acceptedElements,
    changedElements: interpretation.changedElements,
    updatedDirection: interpretation.updatedDirection,
    rationale: interpretation.rationale,
    scope: interpretation.scope || 'recommendation_only',
    confidence: interpretation.confidence,
    refinementState: 'interpreted',
    resultingRecommendationId: null,
    operatorLearningId: null,
    metadata: {},
    createdAt: ts,
    updatedAt: ts,
  };

  if (interpretation.disposition === 'accept') {
    directionRow.refinementState = 'completed';
    await store.insertDirection(directionRow);
    recommendation.status = 'accepted';
    recommendation.updatedAt = ts;
    await store.updateRecommendation(recommendation);
    return {
      ok: true,
      handled: true,
      disposition: 'accept',
      direction: toPublicDirection(directionRow),
      recommendation: toPublicRecommendation(recommendation),
    };
  }

  if (interpretation.disposition === 'reject') {
    directionRow.refinementState = 'completed';
    await store.insertDirection(directionRow);
    recommendation.status = 'rejected';
    recommendation.updatedAt = ts;
    await store.updateRecommendation(recommendation);
    return {
      ok: true,
      handled: true,
      disposition: 'reject',
      direction: toPublicDirection(directionRow),
      recommendation: toPublicRecommendation(recommendation),
    };
  }

  // refine — delegate to Paige
  await store.insertDirection(directionRow);
  directionRow.refinementState = 'delegated';
  await store.updateDirection(directionRow);

  const refineFn =
    typeof opts.refineContentRecommendation === 'function'
      ? opts.refineContentRecommendation
      : refineContentRecommendation;

  try {
    const refined = await refineFn(
      {
        tenantId: clientId,
        clientId,
        recommendation: toPublicRecommendation(recommendation),
        direction: toPublicDirection(directionRow),
        campaignId: recommendation.campaignId,
        objective: recommendation.objective,
        channel: recommendation.channel,
      },
      opts.learningOpts || opts
    );

    const refinedRec = await persistContentRecommendation(
      {
        tenantId,
        clientId,
        specialist: recommendation.specialist,
        kind: recommendation.kind,
        parentRecommendationId: recommendation.id,
        campaignId: recommendation.campaignId,
        objectiveId: recommendation.objectiveId,
        objective: recommendation.objective,
        channel: recommendation.channel,
        recommendedDirection:
          refined.recommended_direction || refined.recommendedDirection,
        reason: refined.reason || '',
        confidence: refined.confidence,
        recommendation: refined,
        provenance: {
          source: 'spec_096_operator_refinement',
          parentRecommendationId: recommendation.id,
          directionId: directionRow.id,
        },
      },
      { store }
    );

    // Store operator-sourced learning for campaign/durable scopes
    let operatorLearningId = null;
    if (
      directionRow.scope === 'experiment_campaign' ||
      directionRow.scope === 'durable_preference'
    ) {
      const learningFn =
        typeof opts.createOperatorDirectionLearning === 'function'
          ? opts.createOperatorDirectionLearning
          : createOperatorDirectionLearning;
      try {
        const learning = await learningFn(
          {
            tenantId: clientId,
            clientId,
            direction: toPublicDirection(directionRow),
            campaignId: recommendation.campaignId,
            channel: recommendation.channel,
            objective: recommendation.objective,
          },
          opts.learningOpts || opts
        );
        operatorLearningId = learning.id;
      } catch (_err) {
        // Learning persistence failure must not block refinement
      }
    }

    directionRow.refinementState = 'completed';
    directionRow.resultingRecommendationId = refinedRec.id;
    directionRow.operatorLearningId = operatorLearningId;
    directionRow.updatedAt = nowIso();
    await store.updateDirection(directionRow);

    recommendation.status = 'refined';
    recommendation.updatedAt = nowIso();
    await store.updateRecommendation(recommendation);

    return {
      ok: true,
      handled: true,
      disposition: 'refine',
      direction: toPublicDirection(directionRow),
      originalRecommendation: toPublicRecommendation(recommendation),
      refinedRecommendation: refinedRec,
      refinedPayload: refined,
    };
  } catch (err) {
    directionRow.refinementState = 'failed';
    directionRow.metadata = {
      error: err.message || String(err),
      code: err.code || 'refinement_failed',
    };
    directionRow.updatedAt = nowIso();
    await store.updateDirection(directionRow);
    return {
      ok: false,
      handled: true,
      failed: true,
      error: err.message || 'Paige refinement failed',
      code: err.code || 'refinement_failed',
      direction: toPublicDirection(directionRow),
      recommendation: toPublicRecommendation(recommendation),
    };
  }
}

/**
 * Recover durable operator direction for fresh Max sessions.
 */
async function recoverOperatorDirectionContext(input = {}, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = String(input.tenantId ?? input.clientId);
  const campaignId = asText(input.campaignId);
  const question = asText(input.question || input.operatorMessage) || '';

  if (!looksLikeDirectionRecoveryQuestion(question)) {
    return { recovered: false };
  }

  const filter = { tenantId, limit: 5 };
  if (campaignId) filter.sourceCampaignId = campaignId;

  const directions = await store.listDirections({
    ...filter,
    refinementState: 'completed',
  });

  const campaignDirections = directions.filter(
    (d) =>
      d.disposition === 'refine' &&
      (d.scope === 'experiment_campaign' || d.scope === 'durable_preference')
  );

  if (!campaignDirections.length) {
    return { recovered: false };
  }

  const latest = campaignDirections[0];
  const explanation = [
    `You previously directed ${latest.sourceCampaignId ? 'the current LinkedIn campaign' : 'Paige'}`,
    `toward ${latest.updatedDirection || 'a revised audience focus'}`,
    latest.rationale ? `because ${latest.rationale.replace(/\.$/, '')}` : '',
    '.',
  ]
    .filter(Boolean)
    .join(' ')
    .replace(/\s+/g, ' ');

  return {
    recovered: true,
    direction: toPublicDirection(latest),
    explanation,
    directions: campaignDirections.map(toPublicDirection),
  };
}

/**
 * Get applicable campaign-level directions for Paige context.
 */
async function getApplicableDirections(input = {}, opts = {}) {
  const store = resolveStore(opts);
  const tenantId = String(input.tenantId ?? input.clientId);
  const campaignId = asText(input.campaignId);

  const scopes = ['experiment_campaign', 'durable_preference'];
  const out = [];
  for (const scope of scopes) {
    const rows = await store.listDirections({
      tenantId,
      sourceCampaignId: campaignId || undefined,
      scope,
      refinementState: 'completed',
      limit: 5,
    });
    out.push(...rows.map(toPublicDirection));
  }
  return out;
}

/**
 * Max-facing prose for refinement interpretation acknowledgment.
 */
function formatDirectionAcknowledgment(interpretation, recommendation) {
  const parts = ['Understood.'];
  if (interpretation.acceptedElements?.length) {
    parts.push(
      `I'll preserve: ${interpretation.acceptedElements.join(', ')}.`
    );
  }
  if (interpretation.changedElements?.length) {
    parts.push(
      `The ${interpretation.changedElements.join(' and ')} should change.`
    );
  }
  if (interpretation.updatedDirection) {
    parts.push(interpretation.updatedDirection);
  }
  if (interpretation.rationale) {
    parts.push(interpretation.rationale);
  }
  if (interpretation.scope === 'experiment_campaign') {
    parts.push(
      "I'm treating this as direction for the current campaign, not as a universal rule."
    );
  } else if (interpretation.scope === 'recommendation_only') {
    parts.push('This applies to the current recommendation only.');
  }
  if (recommendation?.recommendedDirection) {
    parts.push(
      `Original recommendation was: "${recommendation.recommendedDirection}".`
    );
  }
  return parts.join(' ');
}

/**
 * Max-facing prose for refined recommendation presentation.
 */
function formatRefinedRecommendationPresentation(refinedRec, direction) {
  const parts = [];
  parts.push('Paige revised the recommendation:');
  parts.push(`"${refinedRec.recommendedDirection}"`);
  if (refinedRec.reason) {
    parts.push(`Why: ${refinedRec.reason}`);
  }
  if (direction?.rationale) {
    parts.push(`Operator rationale preserved: ${direction.rationale}`);
  }
  if (refinedRec.confidence != null) {
    parts.push(`Confidence: ${refinedRec.confidence}.`);
  }
  parts.push('Review-first: accept or continue discussing with Max.');
  return parts.join('\n\n');
}

function composeDirectionStructuredResponse(result, opts = {}) {
  if (result.needsClarification) {
    return {
      answer: result.clarificationPrompt,
      reasoning: ['Max needs scope clarification before applying operator direction.'],
      supportingEvidence: [],
      contradictingEvidence: [],
      confidence: 0.5,
      nextInvestigations: [],
      recommendedActions: [],
      metadata: {
        specialistDirection: true,
        needsClarification: true,
        executionDomain: 'workspace',
        surface: 'specialist_direction',
      },
    };
  }

  if (result.failed) {
    return {
      answer: `I couldn't complete the refinement: ${result.error}. The original recommendation is unchanged.`,
      reasoning: ['Paige refinement failed — fail-closed per SPEC-096.'],
      supportingEvidence: [],
      contradictingEvidence: [],
      confidence: null,
      recommendedActions: [],
      metadata: {
        specialistDirection: true,
        refinementFailed: true,
        executionDomain: 'workspace',
        surface: 'specialist_direction',
      },
    };
  }

  if (result.disposition === 'accept') {
    return {
      answer: 'Direction accepted. Nothing will be published until you explicitly decide next steps.',
      reasoning: ['Operator accepted Paige recommendation.'],
      recommendedActions: [],
      metadata: { specialistDirection: true, disposition: 'accept' },
    };
  }

  if (result.disposition === 'refine' && result.refinedRecommendation) {
    const prose = formatRefinedRecommendationPresentation(
      result.refinedRecommendation,
      result.direction
    );
    const rec = result.refinedRecommendation;
    return {
      answer: prose,
      reasoning: [
        'Max interpreted operator direction and delegated refinement to Paige.',
        `Scope: ${result.direction.scope}.`,
        `Rationale: ${result.direction.rationale}`,
      ],
      supportingEvidence: (rec.supportingLearningIds || []).map((id) => ({
        id: String(id),
        summary: `SPEC-093 learning ${id}`,
        sourceType: 'content_learning',
      })),
      confidence: rec.confidence,
      recommendedActions: [
        {
          id: 'accept_direction',
          type: 'accept_recommendation',
          label: 'Accept',
          payload: {
            recommendationId: rec.id,
            kind: 'paige_campaign_content_recommendation',
            reviewFirst: true,
            autonomousPublish: false,
          },
        },
        {
          id: 'discuss_with_max',
          type: 'discuss_with_max',
          label: 'Discuss with Max',
          payload: {
            recommendationId: rec.id,
            kind: 'paige_campaign_content_recommendation',
            reviewFirst: true,
          },
        },
      ],
      metadata: {
        specialistDirection: true,
        disposition: 'refine',
        directionId: result.direction.id,
        recommendationId: rec.id,
        parentRecommendationId: result.originalRecommendation?.id,
        autonomousPublish: false,
        reviewFirst: true,
        executionDomain: 'workspace',
        surface: 'specialist_direction',
      },
      paigeRecommendation: rec.payload || rec,
      specialistDirection: result.direction,
    };
  }

  return {
    answer: formatDirectionAcknowledgment(
      result.direction || {},
      result.recommendation
    ),
    metadata: { specialistDirection: true },
  };
}

module.exports = {
  SPECIALISTS,
  DISPOSITIONS,
  SCOPES,
  REFINEMENT_STATES,
  RECOMMENDATION_STATUSES,
  SpecialistDirectionError,
  ContentLearningError,
  createMemoryStore,
  createPostgresStore,
  interpretOperatorFeedback,
  looksLikeRefinementFeedback,
  looksLikeAcceptAction,
  looksLikeDirectionRecoveryQuestion,
  persistContentRecommendation,
  getContentRecommendation,
  applyOperatorDirection,
  recoverOperatorDirectionContext,
  getApplicableDirections,
  formatDirectionAcknowledgment,
  formatRefinedRecommendationPresentation,
  composeDirectionStructuredResponse,
};
