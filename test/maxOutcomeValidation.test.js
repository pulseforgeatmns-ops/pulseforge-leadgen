'use strict';
const assert=require('node:assert/strict');
const test=require('node:test');
const {buildWarmOutcomeValidation,OUTCOME_WINDOWS_DAYS}=require('../utils/maxOutcomeValidation');

test('outcome validation reports 7/14/30 day observational cohorts without causal claims',async()=>{
  let call=0;
  const db={query:async(sql,params)=>{
    call++;
    if(call===1){
      assert.deepEqual(params[2],['heating','warm','hot']);
      return {rows:[
        {days:7,cohort:'recommended_warm',recommended_state:'warm',id:'d1',first_outcome_hours:12,outcome_types:['email_clicked','email_positive_reply']},
        {days:7,cohort:'recommended_warm',recommended_state:'warm',id:'d2',first_outcome_hours:null,outcome_types:[]},
      ]};
    }
    return {rows:[{event_type:'email_clicked',count:1},{event_type:'email_positive_reply',count:1}]};
  }};
  const report=await buildWarmOutcomeValidation({clientId:10,sinceDays:90},db);
  assert.deepEqual(report.windows_days,OUTCOME_WINDOWS_DAYS);
  assert.equal(report.causal_interpretation,'observational_only');
  assert.equal(report.rows[0].sample_size,2);
  assert.equal(report.rows[0].outcome_rate_pct,50);
  assert.equal(report.rows[0].median_hours_to_outcome,12);
  assert.deepEqual(report.rows[0].outcome_breakdown,{email_clicked:1,email_positive_reply:1});
  assert.ok(report.unavailable_outcome_types.includes('meeting_showed'));
});
