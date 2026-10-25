'use strict';
const {feedEvents}=require('./events');
const {id}=require('./wallet');
// Read only existing empirical evidence. Never query cohort outcomes or performance.
async function readEmpiricalEvents(pool,tokenAddresses,{now=new Date(),registry={}}={}) {
  if(!tokenAddresses.length)return[];
  const result=await pool.query(`SELECT id,source_id,external_message_id,occurred_at,ingested_at,created_at,raw_text,raw_reference,extracted_ca,provenance
    FROM signal_raw_caller_evidence WHERE extracted_ca=ANY($1::text[]) AND provenance->>'dataClass'='EMPIRICAL'
      AND COALESCE(provenance->>'synthetic','false')='false' AND COALESCE(provenance->>'testOnly','false')='false'
      AND occurred_at<=$2 AND ingested_at<=$2 AND created_at<=$2 ORDER BY occurred_at,id LIMIT 5000`,[tokenAddresses,now]);
  const events=[];
  for(const r of result.rows) {
    const call={sourceId:r.source_id,externalMessageId:r.external_message_id,occurredAt:new Date(r.occurred_at).toISOString(),
      ingestedAt:new Date(r.ingested_at).toISOString(),text:r.raw_text,url:r.raw_reference,provenance:r.provenance};
    const knownAt=new Date(Math.max(+new Date(r.ingested_at),+new Date(r.created_at))).toISOString();
    for(const e of feedEvents(call,{now,registry}))events.push({...e,knownAt,provenance:{...e.provenance,existingEvidenceId:r.id,originalKnownAt:knownAt}});
  }
  const observations=await pool.query(`SELECT o.id,o.token_address,o.observation_type,o.occurred_at,o.created_at,o.definition_version,o.metadata
    FROM signal_research_observations o WHERE o.token_address=ANY($1::text[]) AND o.observation_type IN ('FIRST_CALLER','INDEPENDENT_CONVERGENCE')
      AND o.definition_version LIKE '%prospective%' AND o.occurred_at<=$2 AND o.created_at<=$2
      AND EXISTS(SELECT 1 FROM signal_events e WHERE e.id IN (SELECT jsonb_array_elements_text(o.evidence_event_ids))
        AND e.provenance->>'dataClass'='EMPIRICAL' AND e.ingested_at<=$2)
      AND jsonb_array_length(o.evidence_event_ids)>0
      AND (SELECT count(*) FROM signal_events e WHERE e.id IN (SELECT jsonb_array_elements_text(o.evidence_event_ids)))=jsonb_array_length(o.evidence_event_ids)
      AND NOT EXISTS(SELECT 1 FROM signal_events e WHERE e.id IN (SELECT jsonb_array_elements_text(o.evidence_event_ids))
        AND (e.provenance->>'dataClass' IS DISTINCT FROM 'EMPIRICAL' OR e.provenance->>'synthetic'='true' OR e.provenance->>'testOnly'='true'))
    ORDER BY o.occurred_at,o.id LIMIT 1000`,[tokenAddresses,now]);
  for(const o of observations.rows) {
    // Import the research engine's strict doctrine result; do not reclassify promotions.
    if(o.observation_type==='INDEPENDENT_CONVERGENCE' && !(o.metadata?.strictProvenIndependent===true&&o.metadata?.independentClusterCount>=2&&new Set(o.metadata?.clusterIds||[]).size>=2))continue;
    events.push({id:id(['research-observation',o.id]),tokenAddress:o.token_address,category:o.observation_type,researchState:o.observation_type,
      occurredAt:new Date(o.occurred_at).toISOString(),knownAt:new Date(o.created_at).toISOString(),source:'signal-prospective-research',evidence:o.metadata,
      provenance:{dataClass:'EMPIRICAL',existingObservationId:o.id,definitionVersion:o.definition_version,performanceBlinded:true}});
  }
  return events;
}
module.exports={readEmpiricalEvents};
