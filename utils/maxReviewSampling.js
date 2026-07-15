'use strict';

const pool = require('../db');

const REVIEW_OUTCOMES = new Set([
  'agree','disagree','uncertain','bad_data','wrong_signal_classification','wrong_score','wrong_transition',
]);

async function sampleRecommendations({ clientId = null, limit = 25, maxAgeDays = 30 } = {}, db = pool) {
  const result = await db.query(`
    WITH reviewed_transitions AS (
      SELECT d.client_id,d.current_state,d.recommended_state,COUNT(*)::int reviewed_count
      FROM max_recommendation_reviews r JOIN max_decisions d ON d.id=r.decision_id
      WHERE ($1::int IS NULL OR r.client_id=$1)
      GROUP BY d.client_id,d.current_state,d.recommended_state
    ), candidates AS (
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
             COALESCE(rt.reviewed_count,0) AS reviewed_transition_count,
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
      LEFT JOIN reviewed_transitions rt ON rt.client_id=d.client_id
        AND rt.current_state=d.current_state AND rt.recommended_state=d.recommended_state
      WHERE d.is_shadow=true AND ($1::int IS NULL OR d.client_id=$1)
        AND d.created_at >= NOW()-($3::int*INTERVAL '1 day')
        AND NOT EXISTS (SELECT 1 FROM max_recommendation_reviews r WHERE r.decision_id=d.id)
    )
    SELECT * FROM candidates WHERE stratum_rank=1
    ORDER BY terminal_recommendation DESC,
      CASE
        WHEN current_state='cold' AND recommended_state='warm' THEN 1
        WHEN current_state='heating' AND recommended_state='warm' THEN 2
        WHEN recommended_state='hot' THEN 3
        WHEN recommended_state='engaged' THEN 4
        WHEN recommended_state='recycle' THEN 5
        WHEN recommended_state='null' THEN 6
        ELSE 20
      END,
      reviewed_transition_count,client_id,created_at DESC LIMIT $2
  `, [clientId, limit, maxAgeDays]);
  return result.rows;
}

async function recordRecommendationReview({
  decisionId, reviewerIdentity, outcome, notes = null,
  scoreComponentExplanation = null, sourceDataTrustworthy = null, sourceDataNotes = null,
}, db = pool) {
  if (!decisionId) throw new Error('decisionId is required');
  if (!reviewerIdentity || !String(reviewerIdentity).trim()) throw new Error('reviewerIdentity is required');
  if (!REVIEW_OUTCOMES.has(outcome)) throw new Error(`Invalid review outcome: ${outcome}`);
  const result = await db.query(`
    INSERT INTO max_recommendation_reviews
      (client_id,decision_id,prospect_id,reviewer_identity,review_outcome,notes,
       score_component_explanation,source_data_trustworthy,source_data_notes)
    SELECT d.client_id,d.id,d.prospect_id,$2,$3,$4,$5::jsonb,$6,$7
    FROM max_decisions d WHERE d.id=$1 AND d.is_shadow=true
    RETURNING id,client_id,decision_id,prospect_id,reviewer_identity,review_outcome,notes,
      score_component_explanation,source_data_trustworthy,source_data_notes,reviewed_at
  `, [decisionId, String(reviewerIdentity).trim(), outcome,
    notes ? String(notes).slice(0, 4000) : null,
    scoreComponentExplanation == null ? null : JSON.stringify(scoreComponentExplanation),
    sourceDataTrustworthy, sourceDataNotes ? String(sourceDataNotes).slice(0, 4000) : null]);
  if (!result.rows[0]) throw new Error(`Shadow decision not found: ${decisionId}`);
  return result.rows[0];
}

module.exports = { REVIEW_OUTCOMES, recordRecommendationReview, sampleRecommendations };
