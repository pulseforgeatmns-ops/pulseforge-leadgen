/**
 * SPEC-257 — Paid web lead identity bridge (walkthrough → prospect).
 *
 * Resolves tenant-scoped prospect identity for walkthrough submissions,
 * persists linkage metadata, and emits lead_created lifecycle evidence.
 */

const { isPhase3dSetterSchemaPresent } = require('../utils/callDispositions');
const { ensureLifecycleSchema } = require('../utils/lifecycleSchema');
const { deriveCanonicalStage } = require('../services/lifecycleService');

const PROSPECT_LINK_STATUS = Object.freeze({
  LINKED_NEW: 'LINKED_NEW',
  LINKED_EXISTING: 'LINKED_EXISTING',
  UNRESOLVED: 'UNRESOLVED',
});

const UNRESOLVED_REASON = Object.freeze({
  EMAIL_TENANT_CONFLICT: 'EMAIL_OWNED_BY_OTHER_CLIENT',
  AMBIGUOUS_EMAIL_LOOKUP: 'AMBIGUOUS_EMAIL_LOOKUP',
  PROSPECT_INSERT_FAILED: 'PROSPECT_INSERT_FAILED',
  PROSPECT_LOOKUP_FAILED: 'PROSPECT_LOOKUP_FAILED',
});

function normalizeWalkthroughEmail(email) {
  return String(email || '').trim().toLowerCase();
}

function splitName(name) {
  const parts = String(name || '').trim().split(/\s+/);
  if (parts.length < 2) return { first_name: parts[0] || name, last_name: '' };
  return { first_name: parts[0], last_name: parts.slice(1).join(' ') };
}

async function insertWalkthroughProspect(db, values, clientId, serviceAreaMatch) {
  const { first_name, last_name } = splitName(values.name);
  const email = normalizeWalkthroughEmail(values.email);
  const baseValues = [
    null,
    first_name,
    last_name,
    email,
    values.phone || null,
    null,
    false,
    null,
    null,
    'website_walkthrough',
    80,
    clientId,
    serviceAreaMatch(values.city),
  ];

  if (await isPhase3dSetterSchemaPresent(db)) {
    const res = await db.query(
      `INSERT INTO prospects
         (company_id, first_name, last_name, email, phone, job_title, decision_maker,
          linkedin_url, facebook_url, source, icp_score, client_id, service_area_match,
          is_synthetic, synthetic_label, do_not_contact)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
       ON CONFLICT (email) DO NOTHING
       RETURNING id, client_id`,
      [...baseValues, false, null, false]
    );
    return res.rows[0] || null;
  }

  const res = await db.query(
    `INSERT INTO prospects
       (company_id, first_name, last_name, email, phone, job_title, decision_maker,
        linkedin_url, facebook_url, source, icp_score, client_id, service_area_match,
        do_not_contact)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
     ON CONFLICT (email) DO NOTHING
     RETURNING id, client_id`,
    [...baseValues, false]
  );
  return res.rows[0] || null;
}

/**
 * Resolve prospect identity for a walkthrough submission.
 *
 * @returns {Promise<{ prospectId: string|null, linkStatus: string, isNew: boolean, unresolvedReason?: string }>}
 */
async function resolveWalkthroughProspect(db, values, clientId, { serviceAreaMatch } = {}) {
  const email = normalizeWalkthroughEmail(values.email);
  if (!email) {
    return {
      prospectId: null,
      linkStatus: PROSPECT_LINK_STATUS.UNRESOLVED,
      isNew: false,
      unresolvedReason: UNRESOLVED_REASON.PROSPECT_LOOKUP_FAILED,
    };
  }

  const inserted = await insertWalkthroughProspect(db, values, clientId, serviceAreaMatch);
  if (inserted?.id) {
    if (Number(inserted.client_id) !== Number(clientId)) {
      return {
        prospectId: null,
        linkStatus: PROSPECT_LINK_STATUS.UNRESOLVED,
        isNew: false,
        unresolvedReason: UNRESOLVED_REASON.EMAIL_TENANT_CONFLICT,
      };
    }
    return {
      prospectId: inserted.id,
      linkStatus: PROSPECT_LINK_STATUS.LINKED_NEW,
      isNew: true,
    };
  }

  const byEmail = await db.query(
    `SELECT id, client_id FROM prospects WHERE email = $1`,
    [email]
  );

  if (byEmail.rows.length > 1) {
    return {
      prospectId: null,
      linkStatus: PROSPECT_LINK_STATUS.UNRESOLVED,
      isNew: false,
      unresolvedReason: UNRESOLVED_REASON.AMBIGUOUS_EMAIL_LOOKUP,
    };
  }

  if (byEmail.rows.length === 1) {
    const row = byEmail.rows[0];
    if (Number(row.client_id) !== Number(clientId)) {
      return {
        prospectId: null,
        linkStatus: PROSPECT_LINK_STATUS.UNRESOLVED,
        isNew: false,
        unresolvedReason: UNRESOLVED_REASON.EMAIL_TENANT_CONFLICT,
      };
    }
    return {
      prospectId: row.id,
      linkStatus: PROSPECT_LINK_STATUS.LINKED_EXISTING,
      isNew: false,
    };
  }

  return {
    prospectId: null,
    linkStatus: PROSPECT_LINK_STATUS.UNRESOLVED,
    isNew: false,
    unresolvedReason: UNRESOLVED_REASON.PROSPECT_INSERT_FAILED,
  };
}

function buildWalkthroughIdentityPayload(resolution, linkedAt) {
  const identity = {
    prospectLinkStatus: resolution.linkStatus,
    linkedAt,
  };
  if (resolution.unresolvedReason) {
    identity.unresolvedReason = resolution.unresolvedReason;
  }
  return identity;
}

async function emitWalkthroughLeadCreatedEvent(db, {
  clientId,
  prospectId,
  actionId,
  attributionRecord,
  linkStatus,
}) {
  try {
    await ensureLifecycleSchema(db);
    const current = await db.query(
      `SELECT status, setter_status FROM prospects WHERE id = $1 AND client_id = $2 LIMIT 1`,
      [prospectId, clientId]
    );
    if (!current.rows[0]) return { emitted: false };

    const row = current.rows[0];
    const stage = deriveCanonicalStage(row);
    const payload = {
      kind: 'lead_created',
      agent_action_id: actionId,
      prospect_link_status: linkStatus,
      source: 'website_walkthrough',
    };
    if (attributionRecord) {
      payload.attribution = {
        raw: attributionRecord.raw,
        normalized: attributionRecord.normalized,
        provenance: attributionRecord.provenance,
      };
    }

    await db.query(
      `INSERT INTO prospect_lifecycle_events
         (client_id, prospect_id, from_stage, to_stage, from_status, to_status,
          from_setter_status, to_setter_status, reason, actor_type, actor_id, actor_name,
          source, payload, idempotency_key)
       VALUES ($1,$2,$3,$3,$4,$4,$5,$5,$6,$7,$8,$9,$10,$11::jsonb,$12)
       ON CONFLICT DO NOTHING`,
      [
        clientId,
        prospectId,
        stage,
        row.status || null,
        row.setter_status || null,
        'lead_created',
        'system',
        'website_walkthrough',
        'Walkthrough Intake',
        'website_walkthrough',
        JSON.stringify(payload),
        `walkthrough_lead_created:${actionId}`,
      ]
    );
    return { emitted: true };
  } catch (err) {
    console.error('[walkthrough] lead_created lifecycle event failed:', err.message);
    return { emitted: false, error: err.message };
  }
}

module.exports = {
  PROSPECT_LINK_STATUS,
  UNRESOLVED_REASON,
  normalizeWalkthroughEmail,
  resolveWalkthroughProspect,
  buildWalkthroughIdentityPayload,
  emitWalkthroughLeadCreatedEvent,
};
