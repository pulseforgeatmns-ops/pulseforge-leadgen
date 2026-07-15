'use strict';

const pool = require('../db');

const REVIEW_OUTCOMES = new Set([
  'agree','disagree','uncertain','bad_data','wrong_signal_classification','wrong_score','wrong_transition',
]);

async function sampleRecommendations({ clientId = null, limit = 25, maxAgeDays = 30 } = {}, db = pool) {
  const result = await db.query(`
    WITH candidates AS (
      SELECT d.id AS decision_id, d.client_id, d.prospect_id, d.created_at,
             d.current_state, d.recommended_state, d.warmth_score, d.trigger_event_type,
             d.reason_codes, d.reason_summary, d.actions, d.operator_required,
             CASE WHEN d.recommended_state IN ('disqualified','null') THEN true ELSE false END AS terminal_recommendation,
             CASE WHEN p.email_verified AND p.email IS NOT NULL THEN 'verified_email'
                  WHEN p.phone IS NOT NULL THEN 'phone'
                  WHEN p.email IS NOT NULL THEN 'unverified_email' ELSE 'none' END AS available_channel,
             width_bucket(d.warmth_score, 0, 101, 5) AS score_bucket,
             CASE WHEN d.created_at >= NOW()-INTERVAL '1 day' THEN '0-1d'
                  WHEN d.created_at >= NOW()-INTERVAL '7 days' THEN '2-7d' ELSE '8d+' END AS age_bucket,
             ROW_NUMBER() OVER (
               PARTITION BY d.client_id,d.current_state,d.recommended_state,
                            width_bucket(d.warmth_score,0,101,5),d.trigger_event_type,
                            (d.recommended_state IN ('disqualified','null')),
                            CASE WHEN p.email_verified AND p.email IS NOT NULL THEN 'verified_email'
                                 WHEN p.phone IS NOT NULL THEN 'phone'
                                 WHEN p.email IS NOT NULL THEN 'unverified_email' ELSE 'none' END
               ORDER BY d.created_at DESC
             ) AS stratum_rank
      FROM max_decisions d JOIN prospects p ON p.id=d.prospect_id AND p.client_id=d.client_id
      WHERE d.is_shadow=true AND ($1::int IS NULL OR d.client_id=$1)
        AND d.created_at >= NOW()-($3::int*INTERVAL '1 day')
        AND NOT EXISTS (SELECT 1 FROM max_recommendation_reviews r WHERE r.decision_id=d.id)
    )
    SELECT * FROM candidates WHERE stratum_rank=1
    ORDER BY terminal_recommendation DESC, client_id, created_at DESC LIMIT $2
  `, [clientId, limit, maxAgeDays]);
  return result.rows;
}

async function recordRecommendationReview({ decisionId, reviewerIdentity, outcome, notes = null }, db = pool) {
  if (!decisionId) throw new Error('decisionId is required');
  if (!reviewerIdentity || !String(reviewerIdentity).trim()) throw new Error('reviewerIdentity is required');
  if (!REVIEW_OUTCOMES.has(outcome)) throw new Error(`Invalid review outcome: ${outcome}`);
  const result = await db.query(`
    INSERT INTO max_recommendation_reviews
      (client_id,decision_id,prospect_id,reviewer_identity,review_outcome,notes)
    SELECT d.client_id,d.id,d.prospect_id,$2,$3,$4
    FROM max_decisions d WHERE d.id=$1 AND d.is_shadow=true
    RETURNING id,client_id,decision_id,prospect_id,reviewer_identity,review_outcome,notes,reviewed_at
  `, [decisionId, String(reviewerIdentity).trim(), outcome, notes ? String(notes).slice(0, 4000) : null]);
  if (!result.rows[0]) throw new Error(`Shadow decision not found: ${decisionId}`);
  return result.rows[0];
}

module.exports = { REVIEW_OUTCOMES, recordRecommendationReview, sampleRecommendations };
