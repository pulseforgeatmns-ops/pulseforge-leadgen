'use strict';
const assert=require('node:assert/strict');
const test=require('node:test');
const {buildReviewConsistencyReport}=require('../utils/maxReviewConsistency');

test('review consistency distinguishes raw rows, unique decisions, prospects, and conflicts',async()=>{
  const responses=[
    [{total_rows:5,unique_decisions_reviewed:4,unique_prospects_reviewed:3,agree:3,disagree:1,disagreements_missing_notes:1,reviews_based_on_incomplete_data:1,terminal_rows:2,terminal_agree:2}],
    [{category:'cold -> warm',total_rows:2,unique_decisions:2,agree:2}],
    [{decision_id:'d1',review_rows:2,distinct_outcomes:2}],
    [{prospect_id:'p1',review_rows:2,unique_decisions:2}],
    [{decision_id:'d1',distinct_outcomes:2,reviewers:['a','b'],outcomes:['agree','disagree']}],
  ];
  const db={query:async()=>({rows:responses.shift()})};
  const report=await buildReviewConsistencyReport({clientId:10,sinceDays:30},db);
  assert.equal(report.total_review_rows,5);
  assert.equal(report.unique_decisions_reviewed,4);
  assert.equal(report.unique_prospects_reviewed,3);
  assert.equal(report.agreement_rate_pct,60);
  assert.equal(report.terminal_agreement_rate_pct,100);
  assert.equal(report.reviewer_disagreement_count,1);
  assert.equal(report.disagreements_missing_notes,1);
  assert.equal(report.category_coverage[0].unique_decisions,2);
});
