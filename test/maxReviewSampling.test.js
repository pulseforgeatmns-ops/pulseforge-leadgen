'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const { recordRecommendationReview, REVIEW_OUTCOMES }=require('../utils/maxReviewSampling');

test('review outcomes are fixed and review writes do not alter decisions',async()=>{
  assert.equal(REVIEW_OUTCOMES.has('wrong_transition'),true);
  let sql;
  const db={query:async(text,params)=>{sql=text;return{rows:[{decision_id:params[0],review_outcome:params[2]}]};}};
  const row=await recordRecommendationReview({decisionId:'d1',reviewerIdentity:'operator@example.com',outcome:'agree'},db);
  assert.equal(row.review_outcome,'agree');
  assert.match(sql,/INSERT INTO max_recommendation_reviews/);
  assert.doesNotMatch(sql,/UPDATE/);
});

test('invalid review outcomes fail closed',async()=>{
  await assert.rejects(()=>recordRecommendationReview({decisionId:'d',reviewerIdentity:'r',outcome:'yes'},{query:async()=>({rows:[]})}));
});
