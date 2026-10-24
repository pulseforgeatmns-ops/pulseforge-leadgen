'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const { recordRecommendationReview, sampleRecommendations, REVIEW_OUTCOMES }=require('../utils/maxReviewSampling');

test('review outcomes are fixed and review writes do not alter decisions',async()=>{
  assert.equal(REVIEW_OUTCOMES.has('wrong_transition'),true);
  let sql;
  const db={query:async(text,params)=>{sql=text;return{rows:[{decision_id:params[0],review_outcome:params[2]}]};}};
  const row=await recordRecommendationReview({decisionId:'d1',reviewerIdentity:'operator@example.com',outcome:'agree'},db);
  assert.equal(row.review_outcome,'agree');
  assert.match(sql,/INSERT INTO max_recommendation_reviews/);
  assert.match(sql,/source_data_trustworthy/);
  assert.doesNotMatch(sql,/UPDATE/);
});

test('invalid review outcomes fail closed',async()=>{
  await assert.rejects(()=>recordRecommendationReview({decisionId:'d',reviewerIdentity:'r',outcome:'yes'},{query:async()=>({rows:[]})}));
});

test('underrepresented sampling exposes trigger, decision-time, and current ICP evidence',async()=>{
  let sql;
  const db={query:async text=>{sql=text;return{rows:[]};}};
  await sampleRecommendations({clientId:10,limit:20,maxAgeDays:30},db);
  assert.match(sql,/trigger_icp/);
  assert.match(sql,/decision_time_icp/);
  assert.match(sql,/current_prospect_icp/);
  assert.match(sql,/icp_source_timestamp/);
  assert.match(sql,/decision_timestamp/);
  assert.match(sql,/historical_trigger_vs_decision_snapshot_mismatch/);
  assert.match(sql,/NOT EXISTS \(SELECT 1 FROM max_recommendation_reviews/);
  assert.match(sql,/PARTITION BY d.client_id,d.prospect_id/);
  assert.match(sql,/prospect_rank/);
});
