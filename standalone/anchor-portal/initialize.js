'use strict';
const { config, MARKER } = require('./config');
const { poolFor, verifyDatabase } = require('./database');
async function transaction(pool, work) {
 const db = await pool.connect();
 try {
  await db.query('BEGIN');
  const { rows:[identity] } = await db.query('SELECT current_database() AS name,current_schema() AS schema');
  if(identity.name!=='anchor_portal_synthetic'||identity.schema!=='public')throw new Error('Dedicated public synthetic database required');
  await db.query('SELECT pg_advisory_xact_lock(91720261014)');
  const result=await work(db);await db.query('COMMIT');return result;
 }catch(error){await db.query('ROLLBACK');throw error;}finally{db.release();}
}
async function initializeSchema(pool, marker) {
 if(marker!==MARKER)throw new Error('Exact synthetic marker required');
 return transaction(pool,async db=>{
  const {rows}=await db.query("SELECT tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')");
  if(rows.length)throw new Error('Initializer requires an entirely empty dedicated database');
  await db.query("CREATE TABLE portal_demo_marker(marker TEXT PRIMARY KEY,expires_at TIMESTAMPTZ); INSERT INTO portal_demo_marker(marker) VALUES ('anchor-portal-synthetic-only-v1')");
  await db.query("CREATE TABLE users (id SERIAL PRIMARY KEY,name TEXT NOT NULL,email TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,role TEXT NOT NULL CHECK(role IN ('manager','cleaner','facility_client')),client_id INTEGER NOT NULL,active BOOLEAN NOT NULL DEFAULT TRUE,valid_until TIMESTAMPTZ)");
  await db.query('CREATE TABLE portal_sessions (sid VARCHAR NOT NULL PRIMARY KEY,sess JSON NOT NULL,expire TIMESTAMP(6) NOT NULL); CREATE INDEX portal_sessions_expire ON portal_sessions(expire)');
  await require('../../utils/anchorPortalSchema').createPortalSchema(db).ensureAnchorPortalTables(db);
  return {setupPending:true,accountsCreated:0};
 });
}
async function provisionDemo(pool, marker, hashes, expiresAt) {
 if(marker!==MARKER)throw new Error('Exact synthetic marker required');
 const keys=['client','operator'];
 if(!hashes||Object.keys(hashes).length!==2||keys.some(k=>!/^\$2[aby]\$12\$[./A-Za-z0-9]{53}$/.test(hashes[k]||'')))throw new Error('Two explicitly approved bcrypt cost-12 hashes required');
 const expiry=new Date(expiresAt);const remaining=expiry-Date.now();
 if(!Number.isFinite(remaining)||remaining<=0||remaining>48*60*60*1000)throw new Error('Explicit future expiry within 48 hours required');
 return transaction(pool,async db=>{
  await verifyDatabase(db,{requireAccounts:false});
  const {rows:[state]}=await db.query('SELECT expires_at,(SELECT count(*) FROM users)::int AS users,(SELECT count(*) FROM anchor_portal_locations)::int AS locations FROM portal_demo_marker FOR UPDATE');
  if(state.expires_at||state.users||state.locations)throw new Error('Provisioning requires untouched schema-only state');
  const fixtures=await require('./fixtures').seedDemo(db,hashes,expiry.toISOString());
  await db.query('UPDATE portal_demo_marker SET expires_at=$1',[expiry.toISOString()]);
  return fixtures;
 });
}
if(require.main===module){
 (async()=>{
  const cfg=config(process.env);const mode=process.argv[2];
  if(!['--schema-only','--provision-approved-demo'].includes(mode))throw new Error('Explicit schema or approved-account provisioning flag required');
  const pool=poolFor(cfg);
  try{
   if(mode==='--schema-only'){await initializeSchema(pool,cfg.marker);console.log('Synthetic schema initialized; zero accounts. Setup pending.');}
   else{
    let input='';for await(const chunk of process.stdin){input+=chunk;if(input.length>4096)throw new Error('Oversized provisioning input');}
    const {hashes,expires_at}=JSON.parse(input);await provisionDemo(pool,cfg.marker,hashes,expires_at);console.log('Approved two-account synthetic demo provisioned. No credentials printed.');
   }
  }finally{await pool.end();}
 })().catch(()=>{console.error('Synthetic initialization refused. Check exact schema state and explicit provisioning approval.');process.exitCode=1;});
}
module.exports={initializeSchema,provisionDemo};
