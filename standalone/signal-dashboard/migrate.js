'use strict';
// Explicit pre-deploy operation for the isolated Signal service only.
const fs=require('node:fs'),path=require('node:path');
const FILES=['2026-10-25-signal-dashboard.sql','2026-10-25-signal-dashboard-auth.sql','2026-10-25-signal-dashboard-zones.sql','2026-10-25-signal-dashboard-opportunities.sql'];
async function migrate(pool,env=process.env){
 if(env.SIGNAL_DASHBOARD_SCHEMA_APPROVAL!=='SPEC-SIGNAL-DASH-001')throw Error('explicit_signal_schema_approval_required');
 const c=await pool.connect();
 try{await c.query('BEGIN');await c.query("SELECT pg_advisory_xact_lock(hashtextextended('signal-dashboard-schema-v1',0))");
  for(const file of FILES){const sql=fs.readFileSync(path.resolve(__dirname,'../../migrations',file),'utf8');
   if(/\b(?:DROP|ALTER|TRUNCATE|DELETE|UPDATE)\b/i.test(sql))throw Error('unexpected_signal_schema_operation');await c.query(sql);}
  await c.query('COMMIT');
 }catch(e){await c.query('ROLLBACK');throw e;}finally{c.release();}
}
if(require.main===module){const pool=new(require('pg').Pool)({connectionString:process.env.DATABASE_URL,ssl:process.env.DATABASE_SSL==='false'?false:{rejectUnauthorized:false},max:2});migrate(pool).then(()=>console.log('[signal-dashboard] isolated schema ready')).catch(()=>{console.error('[signal-dashboard] explicit migration unavailable');process.exitCode=1;}).finally(()=>pool.end());}
module.exports={migrate,FILES};
