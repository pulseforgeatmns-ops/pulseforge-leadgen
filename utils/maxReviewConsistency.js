'use strict';

const pool = require('../db');

function number(value) { return Number(value || 0); }
function rate(numerator, denominator) {
  return denominator ? Number((100 * numerator / denominator).toFixed(2)) : null;
}

async function buildReviewConsistencyReport({ clientId = null, sinceDays = 365 } = {}, db = pool) {
  const params = [clientId, sinceDays];
  const [summary, transitions, duplicateDecisions, duplicateProspects, conflicts] = await Promise.all([
    db.query(`
      SELECT COUNT(*)::int total_rows,
             COUNT(DISTINCT r.decision_id)::int unique_decisions_reviewed,
             COUNT(DISTINCT r.prospect_id)::int unique_prospects_reviewed,
             COUNT(*) FILTER (WHERE r.review_outcome='agree')::int agree,
             COUNT(*) FILTER (WHERE r.review_outcome='disagree')::int disagree,
             COUNT(*) FILTER (WHERE r.review_outcome='disagree' AND NULLIF(BTRIM(r.notes),'') IS NULL)::int disagreements_missing_notes,
             COUNT(*) FILTER (WHERE r.review_outcome IN ('bad_data','uncertain')
               OR r.source_data_trustworthy=FALSE
               OR d.trigger_event_id IS NULL
               OR d.score_components IS NULL)::int reviews_based_on_incomplete_data,
             COUNT(*) FILTER (WHERE d.recommended_state IN ('disqualified','null'))::int terminal_rows,
             COUNT(*) FILTER (WHERE d.recommended_state IN ('disqualified','null') AND r.review_outcome='agree')::int terminal_agree
      FROM max_recommendation_reviews r
      JOIN max_decisions d ON d.id=r.decision_id
      WHERE ($1::int IS NULL OR r.client_id=$1)
        AND r.reviewed_at>=NOW()-($2::int*INTERVAL '1 day')
    `, params),
    db.query(`
      SELECT d.current_state||' -> '||d.recommended_state category,
             COUNT(*)::int total_rows,COUNT(DISTINCT r.decision_id)::int unique_decisions,
             COUNT(*) FILTER(WHERE r.review_outcome='agree')::int agree
      FROM max_recommendation_reviews r JOIN max_decisions d ON d.id=r.decision_id
      WHERE ($1::int IS NULL OR r.client_id=$1)
        AND r.reviewed_at>=NOW()-($2::int*INTERVAL '1 day')
      GROUP BY 1 ORDER BY unique_decisions DESC,category
    `, params),
    db.query(`
      SELECT decision_id,COUNT(*)::int review_rows,COUNT(DISTINCT review_outcome)::int distinct_outcomes
      FROM max_recommendation_reviews
      WHERE ($1::int IS NULL OR client_id=$1) AND reviewed_at>=NOW()-($2::int*INTERVAL '1 day')
      GROUP BY decision_id HAVING COUNT(*)>1 ORDER BY review_rows DESC
    `, params),
    db.query(`
      SELECT prospect_id,COUNT(*)::int review_rows,COUNT(DISTINCT decision_id)::int unique_decisions
      FROM max_recommendation_reviews
      WHERE ($1::int IS NULL OR client_id=$1) AND reviewed_at>=NOW()-($2::int*INTERVAL '1 day')
      GROUP BY prospect_id HAVING COUNT(DISTINCT decision_id)>1 ORDER BY unique_decisions DESC
    `, params),
    db.query(`
      SELECT decision_id,COUNT(DISTINCT review_outcome)::int distinct_outcomes,
             ARRAY_AGG(DISTINCT reviewer_identity ORDER BY reviewer_identity) reviewers,
             ARRAY_AGG(DISTINCT review_outcome ORDER BY review_outcome) outcomes
      FROM max_recommendation_reviews
      WHERE ($1::int IS NULL OR client_id=$1) AND reviewed_at>=NOW()-($2::int*INTERVAL '1 day')
      GROUP BY decision_id HAVING COUNT(DISTINCT review_outcome)>1
    `, params),
  ]);
  const row = summary.rows[0] || {};
  const total = number(row.total_rows);
  const terminal = number(row.terminal_rows);
  return {
    total_review_rows: total,
    unique_decisions_reviewed: number(row.unique_decisions_reviewed),
    unique_prospects_reviewed: number(row.unique_prospects_reviewed),
    agree: number(row.agree),
    disagree: number(row.disagree),
    agreement_rate_pct: rate(number(row.agree), total),
    terminal_agreement_rate_pct: rate(number(row.terminal_agree), terminal),
    same_decision_multiple_reviews: duplicateDecisions.rows,
    same_prospect_multiple_reviews: duplicateProspects.rows,
    conflicting_reviews: conflicts.rows,
    reviewer_disagreement_count: conflicts.rows.length,
    disagreements_missing_notes: number(row.disagreements_missing_notes),
    reviews_based_on_incomplete_data: number(row.reviews_based_on_incomplete_data),
    category_coverage: transitions.rows.map(item => ({
      ...item,
      total_rows: number(item.total_rows),
      unique_decisions: number(item.unique_decisions),
      agree: number(item.agree),
      agreement_rate_pct: rate(number(item.agree), number(item.total_rows)),
    })),
  };
}

module.exports = { buildReviewConsistencyReport };
