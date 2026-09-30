'use strict';

const { ensureStudioSubstralScoutSchema } = require('../utils/studioSubstralScoutSchema');
const { studioOutreachStatusForScore } = require('./studioSubstralScoutIntelligence');

async function persistStudioSubstralScoutProspect(db, prospectId, clientId, { intelligence, outreachStatus } = {}) {
  if (!db || !prospectId || !intelligence) return null;
  await ensureStudioSubstralScoutSchema(db);

  const status = outreachStatus
    || studioOutreachStatusForScore(intelligence.studio_fit_score, intelligence.accepted);

  const websitePainSummary = (intelligence.website_issues_observed || []).slice(0, 5).join('; ')
    || null;
  const proofGapSummary = intelligence.score_breakdown?.proof_gap != null
    ? `Proof gap score ${intelligence.score_breakdown.proof_gap}/15 — ${intelligence.conversion_or_trust_risk}`
    : intelligence.conversion_or_trust_risk;

  await db.query(
    `UPDATE prospects SET
      studio_fit_score = $1,
      studio_category = $2,
      website_pain_summary = $3,
      business_strength_signals = $4::jsonb,
      proof_gap_summary = $5,
      recommended_outreach_angle = $6,
      studio_outreach_status = $7,
      studio_reject_reason = $8,
      studio_confidence = $9,
      studio_scout_intelligence = $10::jsonb,
      icp_score = CASE WHEN $11 THEN $1 ELSE icp_score END,
      setter_visible = CASE WHEN $11 THEN false ELSE setter_visible END
     WHERE id = $12 AND client_id = $13`,
    [
      intelligence.studio_fit_score,
      intelligence.studio_category,
      websitePainSummary,
      JSON.stringify(intelligence.business_strength_signals || []),
      proofGapSummary,
      intelligence.recommended_outreach_angle,
      status,
      intelligence.reject_reason || null,
      intelligence.confidence,
      JSON.stringify(intelligence),
      intelligence.accepted,
      prospectId,
      clientId,
    ]
  );

  return { prospectId, status, accepted: intelligence.accepted };
}

async function listStudioSubstralScoutProspects(db, clientId, { limit = 25, minScore = 65 } = {}) {
  await ensureStudioSubstralScoutSchema(db);
  const res = await db.query(
    `SELECT id, company_id, first_name, last_name, email, phone, website_url, vertical,
            studio_fit_score, studio_category, website_pain_summary, business_strength_signals,
            proof_gap_summary, recommended_outreach_angle, studio_outreach_status,
            studio_reject_reason, studio_confidence, studio_scout_intelligence, icp_score,
            google_rating, google_review_count, created_at
       FROM prospects
      WHERE client_id = $1
        AND studio_fit_score IS NOT NULL
        AND studio_fit_score >= $2
        AND COALESCE(studio_outreach_status, 'new') NOT IN ('not_fit', 'closed')
      ORDER BY studio_fit_score DESC, created_at DESC
      LIMIT $3`,
    [clientId, minScore, limit]
  );
  return res.rows;
}

module.exports = {
  persistStudioSubstralScoutProspect,
  listStudioSubstralScoutProspects,
};
