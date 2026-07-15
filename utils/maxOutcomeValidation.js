'use strict';

const pool = require('../db');

const OUTCOME_WINDOWS_DAYS = Object.freeze([7, 14, 30]);
const OUTCOME_TYPES = Object.freeze([
  'email_human_opened','email_clicked','email_meaningful_reply','email_positive_reply',
  'meeting_booked','meeting_showed','proposal','closed_won','email_negative_reply',
  'email_unsubscribed','email_hard_bounced','email_soft_bounced',
]);
const WARM_RECOMMENDATION_STATES = Object.freeze(['heating','warm','hot']);

async function buildWarmOutcomeValidation({ clientId = null, sinceDays = 90 } = {}, db = pool) {
  const result = await db.query(`
    WITH windows(days) AS (VALUES (7),(14),(30)),
    cohorts AS (
      SELECT d.id,d.client_id,d.prospect_id,d.current_state,d.recommended_state,d.created_at,
             CASE WHEN d.recommended_state=ANY($3::text[]) AND d.recommended_state IS DISTINCT FROM d.current_state
                  THEN 'recommended_warm' ELSE 'comparison_not_recommended_warm' END cohort
      FROM max_decisions d
      WHERE ($1::int IS NULL OR d.client_id=$1)
        AND d.created_at>=NOW()-($2::int*INTERVAL '1 day')
    ), evaluated AS (
      SELECT w.days,c.id,c.client_id,c.prospect_id,c.current_state,c.recommended_state,c.cohort,
             MIN(EXTRACT(EPOCH FROM (s.event_timestamp-c.created_at))/3600.0)
               FILTER(WHERE s.event_type=ANY($4::text[])) first_outcome_hours,
             ARRAY_REMOVE(ARRAY_AGG(DISTINCT s.event_type)
               FILTER(WHERE s.event_type=ANY($4::text[])),NULL) outcome_types
      FROM windows w CROSS JOIN cohorts c
      LEFT JOIN prospect_signal_events s ON s.client_id=c.client_id AND s.prospect_id=c.prospect_id
        AND s.event_timestamp>c.created_at
        AND s.event_timestamp<=c.created_at+(w.days*INTERVAL '1 day')
      GROUP BY w.days,c.id,c.client_id,c.prospect_id,c.current_state,c.recommended_state,c.cohort
    )
    SELECT days,cohort,recommended_state,id,first_outcome_hours,outcome_types
    FROM evaluated ORDER BY days,cohort,recommended_state,id
  `, [clientId, sinceDays, WARM_RECOMMENDATION_STATES, OUTCOME_TYPES]);
  const presence = await db.query(`
    SELECT event_type,COUNT(*)::int count FROM prospect_signal_events
    WHERE ($1::int IS NULL OR client_id=$1) AND event_type=ANY($2::text[])
    GROUP BY event_type ORDER BY event_type
  `, [clientId, OUTCOME_TYPES]);
  const present = new Set(presence.rows.map(row => row.event_type));
  const groups = new Map();
  for (const row of result.rows) {
    const key = `${row.days}:${row.cohort}:${row.recommended_state}`;
    if (!groups.has(key)) groups.set(key,{
      days:Number(row.days),cohort:row.cohort,recommended_state:row.recommended_state,
      sample_size:0,outcome_count:0,outcome_breakdown:{},hours:[],
    });
    const group = groups.get(key);
    group.sample_size++;
    const types = Array.isArray(row.outcome_types) ? row.outcome_types : [];
    if (types.length) group.outcome_count++;
    for (const type of types) group.outcome_breakdown[type]=(group.outcome_breakdown[type]||0)+1;
    if (row.first_outcome_hours != null) group.hours.push(Number(row.first_outcome_hours));
  }
  const rows = [...groups.values()].map(group => {
    group.hours.sort((a,b)=>a-b);
    const middle=Math.floor(group.hours.length/2);
    const median=group.hours.length ? (group.hours.length%2 ? group.hours[middle] : (group.hours[middle-1]+group.hours[middle])/2) : null;
    const { hours, ...row } = group;
    return {...row,outcome_rate_pct:row.sample_size?Number((100*row.outcome_count/row.sample_size).toFixed(2)):null,median_hours_to_outcome:median};
  });
  return {
    client_id: clientId,
    since_days: sinceDays,
    windows_days: OUTCOME_WINDOWS_DAYS,
    causal_interpretation: 'observational_only',
    comparison_definition: 'shadow decisions in the same reporting window that did not recommend a state change to heating, warm, or hot',
    rows,
    observed_outcome_types: OUTCOME_TYPES.filter(type => present.has(type)),
    unavailable_outcome_types: OUTCOME_TYPES.filter(type => !present.has(type)),
  };
}

module.exports = { OUTCOME_TYPES, OUTCOME_WINDOWS_DAYS, WARM_RECOMMENDATION_STATES, buildWarmOutcomeValidation };
