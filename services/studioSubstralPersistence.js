'use strict';

const fs = require('fs');
const path = require('path');
const {
  ASSESSMENT_STAGE,
  defaultNextAction,
} = require('../utils/studioSubstralAssessmentWorkflow');

async function ensureStudioSubstralSchema(pool) {
  const migrationPath = path.join(
    __dirname,
    '../migrations/2026-09-30-studio-substral-pf-001.sql'
  );
  const sql = fs.readFileSync(migrationPath, 'utf8');
  await pool.query(sql);
}

async function upsertAssessmentOpportunity(pool, row) {
  await ensureStudioSubstralSchema(pool);
  const stage = row.stage || ASSESSMENT_STAGE.REQUESTED;
  const nextAction = row.recommended_next_action || defaultNextAction(stage);

  const inserted = await pool.query(
    `INSERT INTO studio_substral_assessment_opportunities (
       client_id, agent_action_id, company_id, prospect_id, mission_id,
       domain, request_key, stage, decision_context, evidence_summary,
       six_layer_findings, evidence_class, observed_constraint,
       recommended_next_action, source, payload
     ) VALUES (
       $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb,
       $12, $13, $14, $15, $16::jsonb
     )
     ON CONFLICT (client_id, request_key)
       WHERE request_key IS NOT NULL
     DO UPDATE SET
       agent_action_id = COALESCE(EXCLUDED.agent_action_id, studio_substral_assessment_opportunities.agent_action_id),
       company_id = COALESCE(EXCLUDED.company_id, studio_substral_assessment_opportunities.company_id),
       prospect_id = COALESCE(EXCLUDED.prospect_id, studio_substral_assessment_opportunities.prospect_id),
       decision_context = COALESCE(EXCLUDED.decision_context, studio_substral_assessment_opportunities.decision_context),
       payload = studio_substral_assessment_opportunities.payload || EXCLUDED.payload,
       updated_at = NOW()
     RETURNING *`,
    [
      row.client_id,
      row.agent_action_id || null,
      row.company_id || null,
      row.prospect_id || null,
      row.mission_id || null,
      row.domain,
      row.request_key || null,
      stage,
      row.decision_context || null,
      JSON.stringify(row.evidence_summary || {}),
      JSON.stringify(row.six_layer_findings || {}),
      row.evidence_class || null,
      row.observed_constraint || null,
      nextAction,
      row.source,
      JSON.stringify(row.payload || {}),
    ]
  );
  return inserted.rows[0];
}

async function listAssessmentOpportunities(pool, clientId, { limit = 50 } = {}) {
  await ensureStudioSubstralSchema(pool);
  const res = await pool.query(
    `SELECT * FROM studio_substral_assessment_opportunities
      WHERE client_id = $1
      ORDER BY updated_at DESC
      LIMIT $2`,
    [clientId, limit]
  );
  return res.rows;
}

async function getAssessmentOpportunityByAction(pool, clientId, agentActionId) {
  await ensureStudioSubstralSchema(pool);
  const res = await pool.query(
    `SELECT * FROM studio_substral_assessment_opportunities
      WHERE client_id = $1 AND agent_action_id = $2
      LIMIT 1`,
    [clientId, agentActionId]
  );
  return res.rows[0] || null;
}

module.exports = {
  ensureStudioSubstralSchema,
  upsertAssessmentOpportunity,
  listAssessmentOpportunities,
  getAssessmentOpportunityByAction,
};
