'use strict';

const fs = require('fs');
const path = require('path');

async function ensureWebsiteOpportunitySchema(pool) {
  const migrationPath = path.join(
    __dirname,
    '../migrations/2026-09-24-spec-web-001-website-opportunity.sql'
  );
  const sql = fs.readFileSync(migrationPath, 'utf8');
  await pool.query(sql);
}

async function insertWebsiteOpportunityEvent(pool, event) {
  await pool.query(
    `INSERT INTO website_opportunity_events
      (event_type, client_id, mission_id, prospect_id, domain, payload)
     VALUES ($1, $2, $3, $4, $5, $6::jsonb)`,
    [
      event.event_type,
      event.tenant_id || event.client_id || null,
      event.mission_id || null,
      event.prospect_id || null,
      event.domain || null,
      JSON.stringify({
        timestamp: event.timestamp,
        capability_version: event.capability_version,
        evidence_counts: event.evidence_counts,
        score: event.score,
        confidence: event.confidence,
        recommended_action: event.recommended_action,
        ...event.payload,
      }),
    ]
  );
}

async function saveWebsiteOpportunityAssessment(pool, row) {
  await ensureWebsiteOpportunitySchema(pool);
  const existing = await pool.query(
    `SELECT id FROM website_opportunity_assessments
      WHERE client_id = $1 AND domain = $2 AND cohort_tag IS NOT DISTINCT FROM $3
      LIMIT 1`,
    [row.client_id, row.domain, row.cohort_tag || null]
  );

  const values = [
    row.client_id,
    row.mission_id || null,
    row.prospect_id || null,
    row.cohort_tag || null,
    row.business_name,
    row.domain,
    row.industry || null,
    row.location || null,
    JSON.stringify(row.payload || {}),
    row.opportunity_score ?? null,
    row.confidence ?? null,
    row.recommended_action || null,
    JSON.stringify(row.score_components || {}),
    JSON.stringify(row.economics || {}),
    row.capability_version || '1.0.0',
  ];

  if (existing.rows[0]) {
    const res = await pool.query(
      `UPDATE website_opportunity_assessments SET
        mission_id = $2, prospect_id = $3, business_name = $5, industry = $7, location = $8,
        payload = $9::jsonb, opportunity_score = $10, confidence = $11,
        recommended_action = $12, score_components = $13::jsonb, economics = $14::jsonb,
        capability_version = $15, updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [existing.rows[0].id, ...values.slice(1)]
    );
    return res.rows[0];
  }

  const res = await pool.query(
    `INSERT INTO website_opportunity_assessments (
      client_id, mission_id, prospect_id, cohort_tag,
      business_name, domain, industry, location,
      payload, opportunity_score, confidence, recommended_action,
      score_components, economics, capability_version
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13::jsonb,$14::jsonb,$15)
    RETURNING *`,
    values
  );
  return res.rows[0];
}

async function listAssessmentsForClient(pool, clientId, { cohortTag = null, limit = 100 } = {}) {
  await ensureWebsiteOpportunitySchema(pool);
  const params = [clientId];
  let sql = `SELECT * FROM website_opportunity_assessments WHERE client_id = $1`;
  if (cohortTag) {
    params.push(cohortTag);
    sql += ` AND cohort_tag = $${params.length}`;
  }
  params.push(limit);
  sql += ` ORDER BY opportunity_score DESC NULLS LAST, created_at DESC LIMIT $${params.length}`;
  const res = await pool.query(sql, params);
  return res.rows;
}

module.exports = {
  ensureWebsiteOpportunitySchema,
  insertWebsiteOpportunityEvent,
  saveWebsiteOpportunityAssessment,
  listAssessmentsForClient,
};
