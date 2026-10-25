'use strict';
const {sourcePerformance}=require('./measurement');
const {SOURCES}=require('./sourceUniverse');
async function readReleasedPerformance(pool,asOf){
 // Outcome columns are selected only through an explicitly released cohort.
 // An incomplete/absent release keeps every quality metric unavailable.
 const r=await pool.query(`SELECT DISTINCT o.id AS observation_id,o.metadata->>'sourceId' AS source_id,o.occurred_at AS started_at,
   j.completed_at,COALESCE(j.completed_at,x.created_at) AS knowledge_at,x.label,x.mfe,x.mae,x.time_to_2x_seconds
   FROM signal_research_cohorts c
   JOIN signal_research_cohort_members cm ON cm.cohort_id=c.id
   JOIN signal_research_observations o ON o.token_address=cm.token_address
   JOIN signal_prospective_research_jobs j ON j.observation_id=o.id AND j.job_type='OUTCOME_24H' AND j.status='COMPLETE'
   JOIN signal_research_observation_outcomes x ON x.observation_id=o.id AND x.execution_delay_seconds=60
   WHERE c.metadata->>'mode'='PROSPECTIVE' AND c.metadata->>'dataClass'='EMPIRICAL'
   AND c.metadata->>'blinded'='false' AND (c.metadata->>'unblindedAt')::timestamptz<$1
   AND o.metadata->>'mode'='PROSPECTIVE' AND o.observation_type='FIRST_CALLER'
   AND o.definition_version=c.definition_version AND j.completed_at<$1 AND x.created_at<$1 AND o.occurred_at<$1
   AND x.data_availability='AVAILABLE' AND x.label IN ('PASS','FAIL')
   AND EXISTS(SELECT 1 FROM signal_events e WHERE o.trigger_event_ids ? e.id AND e.source_id=o.metadata->>'sourceId'
     AND e.provenance->>'dataClass'='EMPIRICAL' AND COALESCE(e.provenance->>'synthetic','false')='false'
     AND COALESCE(e.provenance->>'testOnly','false')='false')`,[asOf]);
 const completed=r.rows.map(x=>({sourceId:x.source_id,observationId:x.observation_id,status:'COMPLETED',dataClass:'EMPIRICAL',blinded:false,prospective:true,
  startedAt:new Date(x.started_at).toISOString(),completedAt:new Date(x.completed_at).toISOString(),knowledgeAt:new Date(x.knowledge_at).toISOString(),
  twoXBefore30Percent:x.label==='PASS',threeX:typeof x.mfe==='number'?x.mfe>=2:null,mfe:x.mfe,mae:x.mae,timeTo2xSeconds:x.time_to_2x_seconds}));
 return {version:'released-source-performance-v1',asOf,blindedOutcomesRead:false,quality:completed.length?'RELEASED_EMPIRICAL_ONLY':'NO_RELEASED_COMPLETED_SAMPLES',
  sources:SOURCES.map(s=>sourcePerformance(s.sourceId,completed,asOf)),sampleBasis:'Completed prior, explicitly released canonical prospective FIRST_CALLER observations; new sources may have no canonical samples.'};
}
module.exports={readReleasedPerformance};
