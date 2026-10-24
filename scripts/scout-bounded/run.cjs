'use strict';
// Future separately approved execution only. No default activation, migrations,
// configuration changes, provider fallback or automatic uncertain-run retry.
const fs=require('node:fs');
async function main() {
  if (process.argv.length!==4 || process.argv[2]!=='--execute-approved') throw Error('bounded_explicit_invocation_required');
  const input=JSON.parse(fs.readFileSync(process.argv[3],'utf8'));
  if (process.env.SCOUT_BOUNDED_APPROVAL_REF!==input.approvalRef
    || process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED!=='false') throw Error('bounded_process_activation_required');
  if (!process.env.DATABASE_URL || !process.env.GOOGLE_PLACES_KEY) throw Error('bounded_existing_credentials_required');
  const fetchImpl=require('./transport.cjs').installTransport({databaseUrl:process.env.DATABASE_URL});
  // Preserve the app's existing connection policy; no credential export or
  // persistent setting change. Close only this newly created standalone pool.
  const existing=require('../../db');
  const {Pool}=require('pg');
  const pool=new Pool({...existing.options,max:6,connectionTimeoutMillis:2000,statement_timeout:30000,query_timeout:30000,lock_timeout:2000});
  try {
    const result=await require('../../services/scoutBoundedAcquisition').runBoundedAcquisition({pool,manifest:input,
      apiKey:process.env.GOOGLE_PLACES_KEY,fetchImpl});
    process.stdout.write(JSON.stringify(result)+'\n');
  } finally {await pool.end();}
}
if (require.main===module) main().catch(()=>{process.stderr.write('Bounded Scout stopped. Inspect durable run/page/attempt records before any further action.\n');process.exitCode=1;});
module.exports={main};
