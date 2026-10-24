'use strict';

/**
 * SPEC-SUBSTRAL-PF-001 — reconcile public assessment intake into tenant-bound
 * company, prospect, and assessment opportunity records (idempotent).
 */

const { ensureStudioSubstralMission } = require('../utils/studioSubstralTenant');
const { upsertAssessmentOpportunity } = require('../services/studioSubstralPersistence');
const { ASSESSMENT_STAGE } = require('../utils/studioSubstralAssessmentWorkflow');
const { SOURCE } = require('./substralAssessmentIntake');

function normalizeEmail(email) {
  return String(email || '').trim().toLowerCase();
}

async function ensureCompanyForDomain(db, clientId, domain) {
  const existing = await db.query(
    `SELECT id FROM companies WHERE client_id = $1 AND lower(domain) = lower($2) LIMIT 1`,
    [clientId, domain]
  );
  if (existing.rows[0]?.id) return existing.rows[0].id;

  const inserted = await db.query(
    `INSERT INTO companies (name, domain, website, industry, client_id)
     VALUES ($1, $2, $3, 'unknown', $4)
     ON CONFLICT DO NOTHING
     RETURNING id`,
    [domain, domain, `https://${domain}`, clientId]
  );
  if (inserted.rows[0]?.id) return inserted.rows[0].id;

  const again = await db.query(
    `SELECT id FROM companies WHERE client_id = $1 AND lower(domain) = lower($2) LIMIT 1`,
    [clientId, domain]
  );
  return again.rows[0]?.id || null;
}

async function ensureProspectForAssessment(db, clientId, { email, domain, companyId, context }) {
  const normalized = normalizeEmail(email);
  if (!normalized) return null;

  const byEmail = await db.query(`SELECT id, client_id FROM prospects WHERE email = $1`, [normalized]);
  if (byEmail.rows.length > 1) return null;
  if (byEmail.rows[0]?.id) {
    if (Number(byEmail.rows[0].client_id) !== Number(clientId)) return null;
    return byEmail.rows[0].id;
  }

  const inserted = await db.query(
    `INSERT INTO prospects (
       company_id, first_name, last_name, email, source, icp_score, client_id, status, notes
     ) VALUES ($1, 'Assessment', 'Request', $2, $3, 0, $4, 'warm', $5)
     ON CONFLICT (email) DO NOTHING
     RETURNING id, client_id`,
    [
      companyId,
      normalized,
      SOURCE,
      clientId,
      context ? `Studio Substral assessment request context: ${context}` : 'Studio Substral paid website assessment request.',
    ]
  );
  if (inserted.rows[0]?.id) {
    if (Number(inserted.rows[0].client_id) !== Number(clientId)) return null;
    return inserted.rows[0].id;
  }
  return null;
}

/**
 * @param {object} db pg pool
 * @param {object} input
 * @param {number} input.clientId
 * @param {number} input.agentActionId
 * @param {string} input.domain
 * @param {string} input.email
 * @param {string|null} input.context
 * @param {string} input.requestKey
 * @param {object} input.actionPayload persisted agent_actions payload
 */
async function reconcileAssessmentIntake(db, input) {
  const { clientId, agentActionId, domain, email, context, requestKey, actionPayload } = input;
  const { missionId } = await ensureStudioSubstralMission(db);

  const companyId = await ensureCompanyForDomain(db, clientId, domain);
  const prospectId = await ensureProspectForAssessment(db, clientId, {
    email,
    domain,
    companyId,
    context,
  });

  const opportunity = await upsertAssessmentOpportunity(db, {
    client_id: clientId,
    agent_action_id: agentActionId,
    company_id: companyId,
    prospect_id: prospectId,
    mission_id: missionId,
    domain,
    request_key: requestKey,
    stage: ASSESSMENT_STAGE.REQUESTED,
    decision_context: context || null,
    evidence_summary: {
      intake_only: true,
      stated_decision_context: context || null,
      reply_to: email,
    },
    six_layer_findings: {},
    source: SOURCE,
    payload: {
      agent_action_id: agentActionId,
      requested_at: actionPayload?.requested_at || new Date().toISOString(),
      review_mode: 'human',
    },
  });

  await db.query(
    `UPDATE agent_actions
        SET payload = payload || $2::jsonb
      WHERE id = $1 AND client_id = $3`,
    [
      agentActionId,
      JSON.stringify({
        assessment_opportunity_id: opportunity.id,
        prospect_id: prospectId,
        company_id: companyId,
        mission_id: missionId,
      }),
      clientId,
    ]
  );

  return {
    companyId,
    prospectId,
    missionId,
    assessmentOpportunityId: opportunity.id,
  };
}

module.exports = {
  reconcileAssessmentIntake,
  ensureCompanyForDomain,
  ensureProspectForAssessment,
};
