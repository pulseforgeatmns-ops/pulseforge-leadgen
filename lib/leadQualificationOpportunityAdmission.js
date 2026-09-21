/**
 * SPEC-259 — Qualified paid lead → canonical revenue opportunity admission.
 *
 * Explicit operator admission after QUALIFY review. Does not auto-create on QUALIFY.
 */

const { randomUUID } = require('crypto');
const {
  ACTION_TYPE,
  QUALIFICATION_STATUS,
  REVIEW_SOURCE,
  ERROR: REVIEW_ERROR,
} = require('./leadQualificationReview');
const { SOURCE_KIND } = require('./walkthroughAttribution');
const { normalizeLeadSource, normalizeAttributionStatus, requireCents, domainError } = require('../utils/revenueDomain');
const { createOpportunity } = require('../services/revenueService');

const ADMISSION_SOURCE_SYSTEM = 'lead_qualification_admission';
const TERMINAL_STAGES = Object.freeze(['won', 'lost', 'cancelled']);

const ERROR = Object.freeze({
  ...REVIEW_ERROR,
  REVIEW_NOT_EXECUTABLE: 'REVIEW_NOT_EXECUTABLE',
  REVIEW_NOT_QUALIFIED: 'REVIEW_NOT_QUALIFIED',
  OPPORTUNITY_CREATION_NOT_READY: 'OPPORTUNITY_CREATION_NOT_READY',
  REVIEW_ALREADY_ADMITTED: 'REVIEW_ALREADY_ADMITTED',
  ESTIMATED_VALUE_REQUIRED: 'ESTIMATED_VALUE_REQUIRED',
  SERVICE_TYPE_REQUIRED: 'SERVICE_TYPE_REQUIRED',
  ALREADY_HAS_OPEN_OPPORTUNITY: 'ALREADY_HAS_OPEN_OPPORTUNITY',
  PRIOR_WON_OPPORTUNITY: 'PRIOR_WON_OPPORTUNITY',
  TERMINAL_OPPORTUNITY_BLOCKS_ADMISSION: 'TERMINAL_OPPORTUNITY_BLOCKS_ADMISSION',
  REVENUE_OPERATOR_WRITES_DISABLED: 'REVENUE_OPERATOR_WRITES_DISABLED',
  REVENUE_SCHEMA_DISABLED: 'REVENUE_SCHEMA_DISABLED',
  IDEMPOTENCY_REQUIRED: 'IDEMPOTENCY_REQUIRED',
});

const EXTERNAL_ATTRIBUTION_KEYS = Object.freeze([
  'campaign_id',
  'ad_group_id',
  'ad_id',
  'ad_account_id',
  'opref',
  'oppref',
  'utm_source',
  'utm_medium',
  'utm_campaign',
  'utm_content',
  'utm_term',
]);

function admissionIdempotencyKey(reviewId) {
  return `qualified_review_opportunity:${reviewId}`;
}

function mapFirstPartyAttributionStatus(value) {
  const status = String(value || '').trim().toLowerCase();
  if (status === 'deterministic' || status === 'inferred' || status === 'unattributed') {
    return status;
  }
  return 'unattributed';
}

function buildAttributionMetadataSnapshot(reviewId, payload) {
  const attribution = payload?.attribution || null;
  const raw = attribution?.raw || {};
  const external = {};
  for (const key of EXTERNAL_ATTRIBUTION_KEYS) {
    if (raw[key]) external[key] = raw[key];
  }
  return {
    sourceKind: SOURCE_KIND,
    normalized: attribution?.normalized
      ? {
        lead_source: attribution.normalized.lead_source,
        attribution_status: attribution.normalized.attribution_status,
        captured_at: attribution.normalized.captured_at,
      }
      : null,
    external,
    lineage: {
      qualification_review_id: reviewId,
      originating_walkthrough_action_id: payload.originating_agent_action_id,
      prospect_id: payload.prospect_id,
    },
  };
}

function deriveLeadSource(payload) {
  const leadSource = payload?.attribution?.normalized?.lead_source;
  if (!leadSource) return 'unknown';
  try {
    return normalizeLeadSource(leadSource);
  } catch (_) {
    return 'unknown';
  }
}

function validateServiceType(serviceType) {
  const value = String(serviceType || '').trim();
  if (!value) return null;
  return value.slice(0, 256);
}

async function loadQualificationReview(db, reviewId, clientId) {
  const res = await db.query(
    `SELECT id, payload, status, client_id, action_type, executed_at, created_at
       FROM agent_actions
      WHERE id = $1
      LIMIT 1`,
    [reviewId]
  );
  const review = res.rows[0];
  if (!review || review.action_type !== ACTION_TYPE) {
    return { ok: false, error: ERROR.REVIEW_NOT_FOUND };
  }
  if (Number(review.client_id) !== Number(clientId)) {
    return { ok: false, error: ERROR.REVIEW_NOT_FOUND };
  }
  return { ok: true, review };
}

async function loadProspectForClient(db, prospectId, clientId) {
  const res = await db.query(
    `SELECT id, client_id FROM prospects WHERE id = $1 LIMIT 1`,
    [prospectId]
  );
  const row = res.rows[0];
  if (!row) return { ok: false, error: ERROR.PROSPECT_NOT_FOUND };
  if (Number(row.client_id) !== Number(clientId)) {
    return { ok: false, error: ERROR.PROSPECT_CLIENT_MISMATCH };
  }
  return { ok: true, row };
}

async function findOpportunityById(db, clientId, opportunityId) {
  const res = await db.query(
    `SELECT * FROM opportunities WHERE client_id = $1 AND id = $2 LIMIT 1`,
    [clientId, opportunityId]
  );
  return res.rows[0] || null;
}

async function findOpenOpportunity(db, clientId, prospectId) {
  const res = await db.query(
    `SELECT * FROM opportunities
      WHERE client_id = $1
        AND prospect_id = $2
        AND stage NOT IN ('won', 'lost', 'cancelled')
      ORDER BY created_at ASC
      LIMIT 1`,
    [clientId, prospectId]
  );
  return res.rows[0] || null;
}

async function findLatestTerminalOpportunity(db, clientId, prospectId) {
  const res = await db.query(
    `SELECT * FROM opportunities
      WHERE client_id = $1
        AND prospect_id = $2
        AND stage IN ('won', 'lost', 'cancelled')
      ORDER BY COALESCE(closed_at, updated_at, created_at) DESC
      LIMIT 1`,
    [clientId, prospectId]
  );
  return res.rows[0] || null;
}

async function markReviewAdmitted(db, {
  reviewId,
  clientId,
  payload,
  opportunityId,
  operator,
  admittedAt,
}) {
  const nextPayload = {
    ...payload,
    opportunityAdmission: {
      status: 'ADMITTED',
      opportunityId,
      admittedAt,
      admittedBy: {
        id: operator.id,
        name: operator.name,
        role: operator.role,
        email: operator.email || null,
      },
    },
  };
  await db.query(
    `UPDATE agent_actions
        SET payload = $2::jsonb
      WHERE id = $1 AND client_id = $3 AND status = 'executed'`,
    [reviewId, JSON.stringify(nextPayload), clientId]
  );
  return nextPayload;
}

function buildAdmissionProvenance(reviewId, payload, attributionMetadata) {
  return {
    qualification_review_id: reviewId,
    originating_walkthrough_action_id: payload.originating_agent_action_id,
    first_party_attribution: attributionMetadata,
  };
}

async function admitOpportunityFromQualificationReview(db, {
  reviewId,
  clientId,
  operator,
  estimatedValueCents,
  serviceType,
  estimatedCostCents = null,
  expectedCloseDate = null,
  humanOwner = null,
  idempotencyKey = null,
  revenueFlags = null,
}) {
  if (!operator?.id) {
    return { ok: false, error: ERROR.OPERATOR_REQUIRED };
  }
  if (!revenueFlags?.revenue_schema_enabled) {
    return { ok: false, error: ERROR.REVENUE_SCHEMA_DISABLED };
  }
  if (!revenueFlags?.revenue_operator_writes_enabled) {
    return { ok: false, error: ERROR.REVENUE_OPERATOR_WRITES_DISABLED };
  }

  const reviewResult = await loadQualificationReview(db, reviewId, clientId);
  if (!reviewResult.ok) return reviewResult;
  const review = reviewResult.review;
  const payload = review.payload || {};

  if (review.status !== 'executed') {
    return { ok: false, error: ERROR.REVIEW_NOT_EXECUTABLE };
  }
  if (payload.qualification_status !== QUALIFICATION_STATUS.QUALIFIED_BY_OPERATOR) {
    return { ok: false, error: ERROR.REVIEW_NOT_QUALIFIED };
  }
  if (payload.opportunityCreationReady !== true) {
    return { ok: false, error: ERROR.OPPORTUNITY_CREATION_NOT_READY };
  }
  if (!payload.prospect_id) {
    return { ok: false, error: ERROR.PROSPECT_NOT_FOUND };
  }

  const prospect = await loadProspectForClient(db, payload.prospect_id, clientId);
  if (!prospect.ok) return prospect;

  const service = validateServiceType(serviceType);
  if (!service) {
    return { ok: false, error: ERROR.SERVICE_TYPE_REQUIRED };
  }
  if (estimatedValueCents == null || estimatedValueCents === '') {
    return { ok: false, error: ERROR.ESTIMATED_VALUE_REQUIRED };
  }
  try {
    requireCents(Number(estimatedValueCents), 'estimatedValueCents');
  } catch (_) {
    return { ok: false, error: ERROR.ESTIMATED_VALUE_REQUIRED };
  }
  if (estimatedCostCents != null && estimatedCostCents !== '') {
    try {
      requireCents(Number(estimatedCostCents), 'estimatedCostCents');
    } catch (_) {
      return { ok: false, error: ERROR.ESTIMATED_VALUE_REQUIRED };
    }
  }

  const resolvedIdempotencyKey = idempotencyKey || admissionIdempotencyKey(reviewId);

  if (payload.opportunityAdmission?.status === 'ADMITTED') {
    const existing = await findOpportunityById(db, clientId, payload.opportunityAdmission.opportunityId);
    if (existing) {
      return {
        ok: true,
        idempotent: true,
        reviewId,
        opportunity: existing,
        payload,
        reviewAlreadyAdmitted: true,
      };
    }
  }

  const openOpportunity = await findOpenOpportunity(db, clientId, payload.prospect_id);
  if (openOpportunity) {
    const linkedReviewId = openOpportunity.attribution_metadata?.lineage?.qualification_review_id;
    if (linkedReviewId === reviewId || payload.opportunityAdmission?.opportunityId === openOpportunity.id) {
      await markReviewAdmitted(db, {
        reviewId,
        clientId,
        payload,
        opportunityId: openOpportunity.id,
        operator,
        admittedAt: payload.opportunityAdmission?.admittedAt || new Date().toISOString(),
      });
      return {
        ok: true,
        idempotent: true,
        reviewId,
        opportunity: openOpportunity,
        reusedOpenOpportunity: true,
      };
    }
    return { ok: false, error: ERROR.ALREADY_HAS_OPEN_OPPORTUNITY, opportunity: openOpportunity };
  }

  const terminal = await findLatestTerminalOpportunity(db, clientId, payload.prospect_id);
  if (terminal) {
    if (terminal.stage === 'won') {
      return { ok: false, error: ERROR.PRIOR_WON_OPPORTUNITY, opportunity: terminal };
    }
    const reviewTs = new Date(review.executed_at || review.created_at).getTime();
    const terminalTs = new Date(terminal.closed_at || terminal.updated_at || terminal.created_at).getTime();
    if (!Number.isFinite(reviewTs) || reviewTs <= terminalTs) {
      return { ok: false, error: ERROR.TERMINAL_OPPORTUNITY_BLOCKS_ADMISSION, opportunity: terminal };
    }
  }

  const attributionMetadata = buildAttributionMetadataSnapshot(reviewId, payload);
  const firstPartyStatus = payload.attribution?.normalized?.attribution_status;
  const attributionStatus = mapFirstPartyAttributionStatus(firstPartyStatus);
  normalizeAttributionStatus(attributionStatus, true);

  const createInput = {
    prospectId: payload.prospect_id,
    serviceType: service,
    estimatedValueCents: Number(estimatedValueCents),
    estimatedCostCents: estimatedCostCents != null && estimatedCostCents !== ''
      ? Number(estimatedCostCents)
      : null,
    expectedCloseDate: expectedCloseDate || null,
    source: deriveLeadSource(payload),
    leadSourceDetail: REVIEW_SOURCE,
    attributionStatus,
    campaignId: null,
    humanOwner: humanOwner || operator.name || String(operator.id),
    attributionMetadata,
    admissionProvenance: buildAdmissionProvenance(reviewId, payload, attributionMetadata),
  };

  let createResult;
  try {
    createResult = await createOpportunity(clientId, createInput, {
      idempotencyKey: resolvedIdempotencyKey,
      sourceSystem: ADMISSION_SOURCE_SYSTEM,
      actorType: 'operator',
      actorId: String(operator.id),
      correlationId: randomUUID(),
    });
  } catch (error) {
    if (error.code === 'IDEMPOTENCY_REQUIRED') {
      return { ok: false, error: ERROR.IDEMPOTENCY_REQUIRED };
    }
    throw error;
  }

  const opportunity = createResult.opportunity;
  const admittedAt = new Date().toISOString();
  const nextPayload = await markReviewAdmitted(db, {
    reviewId,
    clientId,
    payload,
    opportunityId: opportunity.id,
    operator,
    admittedAt,
  });

  return {
    ok: true,
    idempotent: createResult.idempotentReplay === true,
    reviewId,
    opportunity,
    payload: nextPayload,
    qualificationStatus: payload.qualification_status,
  };
}

module.exports = {
  ADMISSION_SOURCE_SYSTEM,
  TERMINAL_STAGES,
  ERROR,
  EXTERNAL_ATTRIBUTION_KEYS,
  admissionIdempotencyKey,
  mapFirstPartyAttributionStatus,
  buildAttributionMetadataSnapshot,
  admitOpportunityFromQualificationReview,
};
