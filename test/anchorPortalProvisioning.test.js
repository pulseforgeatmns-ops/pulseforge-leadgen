'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const crypto=require('node:crypto');
const bcrypt=require('bcryptjs');
const {Pool}=require('pg');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {initializeSchema,provisionDemo}=require('../standalone/anchor-portal/initialize');
const {verifyDatabase}=require('../standalone/anchor-portal/database');
const {createApp}=require('../standalone/anchor-portal/runtime');
const {MARKER}=require('../standalone/anchor-portal/config');
test('schema-only creates no accounts; approved two-account demo expires and cannot be reprovisioned',{timeout:30000},async()=>{
 const pg=await startDisposablePostgres('portal-provision-');let pool,server,store;
 try{
  const admin=new Pool({connectionString:pg.connectionString});await admin.query('CREATE DATABASE anchor_portal_synthetic');await admin.end();
  pool=new Pool({connectionString:pg.connectionString.replace(/\/postgres$/,'/anchor_portal_synthetic')});
  await assert.rejects(verifyDatabase(pool));
  assert.deepEqual(await initializeSchema(pool,MARKER),{setupPending:true,accountsCreated:0});
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM users')).rows[0].n,0);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM anchor_portal_locations')).rows[0].n,0);
  await assert.rejects(createApp({},pool),e=>e.code==='PORTAL_SETUP_PENDING');
  await assert.rejects(initializeSchema(pool,MARKER),/empty/);
  const password=crypto.randomBytes(32).toString('hex');const hash=await bcrypt.hash(password,12);const hashes={client:hash,operator:hash};
  await assert.rejects(provisionDemo(pool,MARKER,{...hashes,foreignClient:hash},new Date(Date.now()+3600000)),/Two explicitly/);
  await assert.rejects(provisionDemo(pool,MARKER,hashes,new Date(Date.now()+49*3600000)),/48 hours/);
  await assert.rejects(provisionDemo(pool,MARKER,hashes,new Date(Date.now()-1000)),/expiry/);
  const fixture=await provisionDemo(pool,MARKER,hashes,new Date(Date.now()+3600000));
  const {rows:users}=await pool.query('SELECT email,role,client_id,valid_until FROM users ORDER BY email');
  assert.deepEqual(users.map(u=>[u.email,u.role,u.client_id]),[['client@exeter-fixture.invalid','facility_client',10],['operator@exeter-fixture.invalid','manager',10]]);
  assert.equal((await pool.query('SELECT count(*)::int n FROM anchor_portal_locations')).rows[0].n,1);
  assert.equal((await pool.query('SELECT count(*)::int n FROM anchor_portal_visits')).rows[0].n,3);
  assert.equal((await pool.query('SELECT count(*)::int n FROM anchor_portal_evidence')).rows[0].n,4);
  await verifyDatabase(pool);await assert.rejects(provisionDemo(pool,MARKER,hashes,new Date(Date.now()+3600000)),/untouched/);
  const outer=require('express')();server=await new Promise(r=>{const s=outer.listen(0,'127.0.0.1',()=>r(s));});const base=`http://127.0.0.1:${server.address().port}`;
  const runtime=await createApp({mode:'local',origin:base,secret:crypto.randomBytes(32).toString('hex')},pool);store=runtime.store;outer.use(runtime.app);
  async function request(path,cookie='',csrf='',body){const res=await fetch(base+path,{method:body?'POST':'GET',headers:{Cookie:cookie,Origin:base,'X-CSRF-Token':csrf,'Content-Type':'application/json'},body:body?JSON.stringify(body):undefined});return{status:res.status,body:await res.json(),cookie:res.headers.get('set-cookie')?.split(';')[0]};}
  const pre=await request('/auth/csrf');const login=await request('/login',pre.cookie,pre.body.csrf,{email:users[0].email,password});assert.equal(login.status,200);
  const token=await request('/auth/csrf',login.cookie);
  await pool.query("UPDATE users SET valid_until=NOW()-INTERVAL '1 second' WHERE email=$1",[users[0].email]);
  assert.equal((await request('/api/anchor-portal/me',login.cookie)).status,401);
  await pool.query("UPDATE users SET valid_until=NOW()+INTERVAL '1 hour' WHERE email=$1",[users[0].email]);
  await pool.query("UPDATE portal_demo_marker SET expires_at=NOW()-INTERVAL '1 second'");
  assert.equal((await request('/api/anchor-portal/me',login.cookie)).status,410);
  assert.equal((await request('/api/anchor-portal/issues',login.cookie,token.body.csrf,{location_id:fixture.location.id,description:'Must not write'})).status,410);
  assert.equal((await request('/login',pre.cookie,pre.body.csrf,{email:users[0].email,password})).status,410);
  await assert.rejects(verifyDatabase(pool),e=>e.code==='PORTAL_SETUP_PENDING');
 }finally{if(store)store.close();if(server)await new Promise(r=>server.close(r));if(pool)await pool.end();await pg.stop();}
});
