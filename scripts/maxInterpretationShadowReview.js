'use strict';
const {createShadowPool}=require('../packages/decision-service/ShadowEventSink');
async function review(db,tenantId) {
  if (typeof tenantId!=='string' || !/^[a-zA-Z0-9_.:-]{1,160}$/.test(tenantId)) throw new Error('A tenant identifier is required');
  const {rows}=await db.query(`SELECT observation->'legacy'->>'owner' AS legacy_owner,
    observation->'frame'->>'intent' AS intent,
    observation->'frame'->>'subjectKind' AS subject,
    count(*)::integer AS turns,
    count(*) FILTER (WHERE observation->>'disagreement'='true')::integer AS disagreements,
    count(*) FILTER (WHERE observation->>'failure' IS NOT NULL)::integer AS degraded,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY (observation->>'latencyMs')::numeric) AS p95_ms
    FROM max_interpretation_shadow WHERE tenant_id=$1 AND observed_at >= now()-interval '90 days'
    GROUP BY 1,2,3 ORDER BY turns DESC LIMIT 100`,[tenantId]);
  return rows;
}
if(require.main===module) {
  const tenant=process.argv[2];
  if(!process.env.DATABASE_URL) {console.error('DATABASE_URL is required in the managed environment.');process.exitCode=1;}
  else {
    const db=createShadowPool();
    review(db,tenant).then(rows=>console.log(JSON.stringify(rows,null,2)))
      .catch(()=>{console.error('Shadow review unavailable or invalid tenant.');process.exitCode=1;})
      .finally(()=>db.end());
  }
}
module.exports={review};
