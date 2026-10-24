/**
 * SPEC-258 — Paid web lead qualification admission boundary.
 *
 * Durable website prospect → operator qualification review → disposition.
 * Does not create opportunities, mutate setter_status, or AO leads.
 */

const { SOURCE_KIND } = require('./walkthroughAttribution');

const ACTION_TYPE = 'lead_qualification_review';
const WALKTHROUGH_ACTION_TYPE = 'walkthrough_request';
const REVIEW_SOURCE = 'website_walkthrough';
const CREATED_BY = 'system';

const DECISION_ACTIONS = Object.freeze(['QUALIFY', 'NURTURE', 'DISQUALIFY']);

const QUALIFICATION_STATUS = Object.freeze({
  PENDING_REVIEW: 'PENDING_REVIEW',
  QUALIFIED_BY_OPERATOR: 'QUALIFIED_BY_OPERATOR',
  NURTURE: 'NURTURE',
  DISQUALIFIED: 'DISQUALIFIED',
});

const ERROR = Object.freeze({
  PROSPECT_NOT_FOUND: 'PROSPECT_NOT_FOUND',
  PROSPECT_CLIENT_MISMATCH: 'PROSPECT_CLIENT_MISMATCH',
  ORIGINATING_ACTION_NOT_FOUND: 'ORIGINATING_ACTION_NOT_FOUND',
  ORIGINATING_ACTION_CLIENT_MISMATCH: 'ORIGINATING_ACTION_CLIENT_MISMATCH',
  REVIEW_NOT_FOUND: 'REVIEW_NOT_FOUND',
  REVIEW_ALREADY_TERMINAL: 'REVIEW_ALREADY_TERMINAL',
  CONFLICTING_DECISION: 'CONFLICTING_DECISION',
  UNSUPPORTED_ACTION: 'UNSUPPORTED_ACTION',
  OPERATOR_REQUIRED: 'OPERATOR_REQUIRED',
});

function decisionToQualificationStatus(action) {
  switch (action) {
    case 'QUALIFY':
      return QUALIFICATION_STATUS.QUALIFIED_BY_OPERATOR;
    case 'NURTURE':
      return QUALIFICATION_STATUS.NURTURE;
    case 'DISQUALIFY':
      return QUALIFICATION_STATUS.DISQUALIFIED;
    default:
      return null;
  }
}

function opportunityCreationReadyForStatus(status) {
  return status === QUALIFICATION_STATUS.QUALIFIED_BY_OPERATOR;
}

function buildContactSummary(values) {
  return {
    name: values.name,
    business_name: values.business_name,
    phone: values.phone,
    email: values.email,
    city: values.city,
    space_type: values.space_type,
    space_type_label: values.space_type_label,
  };
}

function assertFirstPartyAttribution(attributionRecord) {
  if (!attributionRecord) return null;
  if (attributionRecord.provenance?.sourceKind !== SOURCE_KIND) {
    throw new Error('Attribution provenance must remain FIRST_PARTY_ATTRIBUTION');
  }
  return attributionRecord;
}

function buildReviewPayload({
  prospectId,
  originatingActionId,
  attributionRecord,
  contactSummary,
}) {
  const payload = {
    source: REVIEW_SOURCE,
    prospect_id: prospectId,
    originating_agent_action_id: originatingActionId,
    qualification_status: QUALIFICATION_STATUS.PENDING_REVIEW,
    opportunityCreationReady: false,
    contact_summary: contactSummary,
    additional_originations: [],
  };
  const attribution = assertFirstPartyAttribution(attributionRecord);
  if (attribution) payload.attribution = attribution;
  return payload;
}

function appendAdditionalOrigination(payload, {
  originatingActionId,
  attributionRecord,
  contactSummary,
  receivedAt,
}) {
  const next = { ...(payload || {}) };
  const additions = Array.isArray(next.additional_originations)
    ? [...next.additional_originations]
    : [];
  const entry = {
    originating_agent_action_id: originatingActionId,
    received_at: receivedAt,
  };
  const attribution = assertFirstPartyAttribution(attributionRecord);
  if (attribution) entry.attribution = attribution;
  if (contactSummary) entry.contact_summary = contactSummary;
  additions.push(entry);
  next.additional_originations = additions;
  return next;
}

async function loadProspectForClient(db, prospectId, clientId) {
  const res = await db.query(
    `SELECT id, client_id, setter_status FROM prospects WHERE id = $1 LIMIT 1`,
    [prospectId]
  );
  const row = res.rows[0];
  if (!row) return { ok: false, error: ERROR.PROSPECT_NOT_FOUND };
  if (Number(row.client_id) !== Number(clientId)) {
    return { ok: false, error: ERROR.PROSPECT_CLIENT_MISMATCH };
  }
  return { ok: true, row };
}

async function loadWalkthroughAction(db, actionId, clientId) {
  const res = await db.query(
    `SELECT id, client_id, action_type, payload
       FROM agent_actions
      WHERE id = $1
      LIMIT 1`,
    [actionId]
  );
  const row = res.rows[0];
  if (!row || row.action_type !== WALKTHROUGH_ACTION_TYPE) {
    return { ok: false, error: ERROR.ORIGINATING_ACTION_NOT_FOUND };
  }
  if (Number(row.client_id) !== Number(clientId)) {
    return { ok: false, error: ERROR.ORIGINATING_ACTION_CLIENT_MISMATCH };
  }
  return { ok: true, row };
}

async function findOpenQualificationReview(db, clientId, prospectId) {
  const res = await db.query(
    `SELECT id, payload, status
       FROM agent_actions
      WHERE client_id = $1
        AND action_type = $2
        AND status = 'pending'
        AND payload->>'prospect_id' = $3
      ORDER BY created_at ASC
      LIMIT 1`,
    [clientId, ACTION_TYPE, String(prospectId)]
  );
  return res.rows[0] || null;
}

async function ensureQualificationReviewForWalkthrough(db, {
  clientId,
  prospectId,
  originatingActionId,
  attributionRecord = null,
  contactSummary,
}) {
  if (!prospectId || !originatingActionId) {
    return { ok: false, error: ERROR.PROSPECT_NOT_FOUND };
  }

  const prospect = await loadProspectForClient(db, prospectId, clientId);
  if (!prospect.ok) return prospect;

  const origin = await loadWalkthroughAction(db, originatingActionId, clientId);
  if (!origin.ok) return origin;

  const originPayload = origin.row.payload || {};
  if (
    originPayload.prospect_id
    && String(originPayload.prospect_id) !== String(prospectId)
  ) {
    return { ok: false, error: ERROR.ORIGINATING_ACTION_NOT_FOUND };
  }

  const receivedAt = new Date().toISOString();
  const existing = await findOpenQualificationReview(db, clientId, prospectId);
  if (existing) {
    const updatedPayload = appendAdditionalOrigination(existing.payload, {
      originatingActionId,
      attributionRecord,
      contactSummary,
      receivedAt,
    });
    await db.query(
      `UPDATE agent_actions
          SET payload = $2::jsonb
        WHERE id = $1 AND client_id = $3 AND status = 'pending'`,
      [existing.id, JSON.stringify(updatedPayload), clientId]
    );
    return {
      ok: true,
      reviewId: existing.id,
      reused: true,
      payload: updatedPayload,
    };
  }

  const payload = buildReviewPayload({
    prospectId,
    originatingActionId,
    attributionRecord,
    contactSummary,
  });
  const title = `Lead qualification review — ${contactSummary.business_name || contactSummary.name}`;
  const description = [
    contactSummary.name,
    contactSummary.business_name,
    contactSummary.phone,
    contactSummary.email,
  ].filter(Boolean).join(' · ');

  const inserted = await db.query(
    `INSERT INTO agent_actions
       (created_by, action_type, title, description, payload, status, client_id)
     VALUES ($1, $2, $3, $4, $5::jsonb, 'pending', $6)
     RETURNING id, payload`,
    [CREATED_BY, ACTION_TYPE, title, description, JSON.stringify(payload), clientId]
  );

  return {
    ok: true,
    reviewId: inserted.rows[0].id,
    reused: false,
    payload: inserted.rows[0].payload,
  };
}

function buildDecisionPatch(action, operator, note, decidedAt) {
  const qualificationStatus = decisionToQualificationStatus(action);
  return {
    qualification_status: qualificationStatus,
    opportunityCreationReady: opportunityCreationReadyForStatus(qualificationStatus),
    decision: {
      action,
      decidedAt,
      decidedBy: {
        id: operator.id,
        name: operator.name,
        role: operator.role,
        email: operator.email || null,
      },
      note: note || null,
    },
    qualifiedAt: action === 'QUALIFY' ? decidedAt : null,
    qualifiedBy: action === 'QUALIFY' ? {
      id: operator.id,
      name: operator.name,
      role: operator.role,
      email: operator.email || null,
    } : null,
  };
}

async function applyQualificationDecision(db, {
  reviewId,
  action,
  operator,
  note = null,
  clientId,
}) {
  if (!operator?.id) {
    return { ok: false, error: ERROR.OPERATOR_REQUIRED };
  }
  if (!DECISION_ACTIONS.includes(action)) {
    return { ok: false, error: ERROR.UNSUPPORTED_ACTION };
  }

  const res = await db.query(
    `SELECT id, payload, status, client_id, action_type
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

  const payload = review.payload || {};
  if (payload.prospect_id) {
    const prospect = await loadProspectForClient(db, payload.prospect_id, clientId);
    if (!prospect.ok) return prospect;
  }

  if (review.status !== 'pending') {
    const priorAction = payload.decision?.action;
    if (priorAction === action) {
      return {
        ok: true,
        idempotent: true,
        reviewId: review.id,
        payload,
        qualificationStatus: payload.qualification_status,
        opportunityCreationReady: payload.opportunityCreationReady === true,
      };
    }
    return { ok: false, error: ERROR.CONFLICTING_DECISION };
  }

  const decidedAt = new Date().toISOString();
  const patch = buildDecisionPatch(action, operator, note, decidedAt);
  const nextPayload = {
    ...payload,
    ...patch,
    attribution: payload.attribution,
  };

  if (nextPayload.attribution?.provenance?.sourceKind !== SOURCE_KIND) {
    return { ok: false, error: ERROR.UNSUPPORTED_ACTION };
  }

  await db.query(
    `UPDATE agent_actions
        SET payload = $2::jsonb,
            status = 'executed',
            executed_at = $3,
            result = $4
      WHERE id = $1 AND client_id = $5 AND status = 'pending'`,
    [
      reviewId,
      JSON.stringify(nextPayload),
      decidedAt,
      action,
      clientId,
    ]
  );

  return {
    ok: true,
    idempotent: false,
    reviewId: review.id,
    payload: nextPayload,
    qualificationStatus: nextPayload.qualification_status,
    opportunityCreationReady: nextPayload.opportunityCreationReady === true,
  };
}

module.exports = {
  ACTION_TYPE,
  WALKTHROUGH_ACTION_TYPE,
  REVIEW_SOURCE,
  DECISION_ACTIONS,
  QUALIFICATION_STATUS,
  ERROR,
  buildContactSummary,
  buildReviewPayload,
  ensureQualificationReviewForWalkthrough,
  applyQualificationDecision,
  findOpenQualificationReview,
  opportunityCreationReadyForStatus,
  decisionToQualificationStatus,
};
