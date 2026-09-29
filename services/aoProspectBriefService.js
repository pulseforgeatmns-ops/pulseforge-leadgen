'use strict';

const pool = require('../db');
const { fetchProspectBundle } = require('./aoProspectTaskService');
const { findAssignedLeadById } = require('./aoAccountIntelligence');
const { formatProspectBrief, formatLeadBrief } = require('../utils/aoProspectBrief');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');

async function fetchOpenTaskForProspect({ prospectId, clientId, aoOwnerId }) {
  const { rows } = await pool.query(`
    SELECT *
    FROM ao_prospect_tasks
    WHERE prospect_id = $1
      AND client_id = $2
      AND assigned_ao_id = $3
      AND status IN ('open', 'in_progress')
    ORDER BY created_at DESC
    LIMIT 1
  `, [prospectId, clientId, aoOwnerId]);
  return rows[0] || null;
}

async function buildProspectBriefById({ prospectId, clientId, aoOwnerId }) {
  const bundle = await fetchProspectBundle(prospectId, clientId);
  if (!bundle) return null;

  const task = await fetchOpenTaskForProspect({ prospectId, clientId, aoOwnerId });
  const brief = formatProspectBrief({
    prospect: bundle.prospect,
    company: bundle.company,
    touchpoints: bundle.touchpoints,
    task,
  });

  return {
    brief,
    prospect: bundle.prospect,
    company: bundle.company,
    task,
    prospect_id: prospectId,
    mission_id: task?.routing_snapshot?.mission_id || null,
  };
}

async function buildLeadBriefById({ leadId, aoOwnerId, clientId }) {
  const lead = await findAssignedLeadById({ aoOwnerId, clientId, leadId });
  if (!lead) return null;

  return {
    brief: formatLeadBrief(lead),
    lead_id: leadId,
    prospect_id: lead.crm_prospect_id || null,
    business_name: lead.business_name,
  };
}

async function requestProspectBrief({
  clientId,
  aoOwnerId,
  prospectId = null,
  leadId = null,
  source = 'prospect_card_brief_button',
}) {
  let result = null;

  if (prospectId) {
    result = await buildProspectBriefById({ prospectId, clientId, aoOwnerId });
  } else if (leadId) {
    result = await buildLeadBriefById({ leadId, aoOwnerId, clientId });
    if (result?.prospect_id) {
      const crmBrief = await buildProspectBriefById({
        prospectId: result.prospect_id,
        clientId,
        aoOwnerId,
      });
      if (crmBrief) result = { ...result, ...crmBrief, brief: crmBrief.brief };
    }
  }

  if (!result) {
    return { error: 'Prospect or lead not found for this AO', status: 404 };
  }

  await logAoAuditEvent({
    event: 'AO_PROSPECT_BRIEF_REQUESTED',
    clientId,
    aoUserId: aoOwnerId,
    prospectId: result.prospect_id || prospectId || null,
    missionId: result.mission_id || null,
    payload: {
      source,
      lead_id: leadId || null,
      action: 'prospect_brief',
    },
  });

  return {
    ok: true,
    action: 'prospect_brief',
    brief: result.brief,
    prospect_id: result.prospect_id || prospectId || null,
    lead_id: result.lead_id || leadId || null,
    mission_id: result.mission_id || null,
    business_name: result.business_name || result.company?.name || result.prospect?.name || null,
  };
}

module.exports = {
  buildProspectBriefById,
  buildLeadBriefById,
  requestProspectBrief,
};
