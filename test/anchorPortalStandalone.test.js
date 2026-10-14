'use strict';
const { test,before,after } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const bcrypt = require('bcryptjs');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { initialize } = require('./helpers/standaloneAcceptanceFixture');
const { createApp } = require('../standalone/anchor-portal/runtime');
const { config,MARKER } = require('../standalone/anchor-portal/config');
const { verifyDatabase } = require('../standalone/anchor-portal/database');
const { illustrativePhoto } = require('../standalone/anchor-portal/fixtures');
let pg,pool,server,base,fixture,password,hashes,store;
const keys=['client','operator','cleaner','unassigned','foreignOperator','foreignClient'];
before(async()=>{
 pg=await startDisposablePostgres('portal-standalone-');
 const admin=new Pool({connectionString:pg.connectionString});
 await admin.query('CREATE DATABASE anchor_portal_synthetic');await admin.end();
 pool=new Pool({connectionString:pg.connectionString.replace(/\/postgres$/,'/anchor_portal_synthetic')});
 password=crypto.randomBytes(32).toString('base64url');
 const hash=await bcrypt.hash(password,12); hashes=Object.fromEntries(keys.map(k=>[k,hash]));
 fixture=await initialize(pool,MARKER,hashes);
 // Reserve selected loopback listener before constructing canonical configuration.
 const express=require('express');const outer=express();
 server=await new Promise(resolve=>{const s=outer.listen(0,'127.0.0.1',()=>resolve(s));});
 base=`http://127.0.0.1:${server.address().port}`;
 const cfg=config({PORTAL_MODE:'local',PORT:String(server.address().port),PORTAL_ORIGIN:base,PORTAL_DATABASE_URL:pool.options.connectionString,PORTAL_SYNTHETIC_MARKER:MARKER,PORTAL_SESSION_SECRET:crypto.randomBytes(32).toString('hex')});
 const runtime=await createApp(cfg,pool);store=runtime.store;outer.use(runtime.app);
});
after(async()=>{if(store)store.close();if(server)await new Promise(r=>server.close(r));if(pool)await pool.end();if(pg)await pg.stop();password=undefined;});
async function call(path, actor={},method='GET',body,headers={}) {
 const r=await fetch(base+path,{method,redirect:'manual',headers:{Cookie:actor.cookie||'',Origin:base,'Content-Type':'application/json','X-CSRF-Token':actor.csrf||'',...headers},body:body?JSON.stringify(body):undefined});
 const text=await r.text();let value;try{value=JSON.parse(text);}catch{value=text;}
 return {status:r.status,body:value,cookie:r.headers.get('set-cookie')?.split(';')[0],headers:r.headers};
}
async function login(key) {
 const pre=await call('/auth/csrf');const old={cookie:pre.cookie,csrf:pre.body.csrf};
 const result=await call('/login',old,'POST',{email:`${key}@exeter-fixture.invalid`,password});assert.equal(result.status,200,JSON.stringify(result.body));assert.notEqual(result.cookie,old.cookie);assert.equal((await call('/api/anchor-portal/me',old)).status,401);
 const csrf=await call('/auth/csrf',{cookie:result.cookie});return {cookie:result.cookie,csrf:csrf.body.csrf};
}
test('failclosed config and initialization, safe import graph',async()=>{
 assert.throws(()=>config({}));
 const env={PORTAL_MODE:'local',PORT:'1234',PORTAL_ORIGIN:'http://127.0.0.1:1234',PORTAL_DATABASE_URL:'postgresql://postgres@127.0.0.1/anchor_portal_synthetic',PORTAL_SESSION_SECRET:'a'.repeat(64),PORTAL_SYNTHETIC_MARKER:MARKER};
 assert.equal(config(env).mode,'local');
 for(const changes of [{DATABASE_URL:'x'},{PORTAL_MODE:'production'},{PORTAL_ORIGIN:'http://evil.invalid:1234'},{PORTAL_DATABASE_URL:'postgresql://localhost/production'},{PORTAL_UNKNOWN:'x'},{PORTAL_SESSION_SECRET:'weak'}])assert.throws(()=>config({...env,...changes}));
 await assert.rejects(initialize(pool,MARKER,hashes),/empty/);
 await assert.rejects(initialize(pool,'wrong',hashes),/marker/);
 const imports=Object.keys(require.cache).filter(p=>p.startsWith(require('node:path').resolve(__dirname,'..'))&&!p.includes('/node_modules/'));
 for(const banned of ['/server.js','/db.js','/middleware/auth.js','/services/pilotOnboarding.js'])assert.ok(!imports.some(p=>p.endsWith(banned)),banned);
 await pool.query('ALTER TABLE portal_sessions RENAME TO missing_sessions');await assert.rejects(verifyDatabase(pool),/Unexpected/);await pool.query('ALTER TABLE missing_sessions RENAME TO portal_sessions');
 await pool.query('CREATE SCHEMA shadow; CREATE TABLE shadow.users(id int)');await assert.rejects(verifyDatabase(pool),/Unexpected/);await pool.query('DROP TABLE shadow.users; DROP SCHEMA shadow');
 await pool.query('ALTER TABLE anchor_portal_evidence ALTER COLUMN purpose DROP NOT NULL');await assert.rejects(verifyDatabase(pool),/mismatch/);await pool.query('ALTER TABLE anchor_portal_evidence ALTER COLUMN purpose SET NOT NULL');
 await pool.query('CREATE TABLE unexpected (id int)');await assert.rejects(verifyDatabase(pool),/Unexpected/);await pool.query('DROP TABLE unexpected');
});
test('real login, fixation rotation, concurrent sessions, logout and expiry',async()=>{
 assert.equal((await call('/api/anchor-portal/me')).status,401);
 assert.equal((await call('/__test/session/client',{},'POST',{})).status,403);
 const a=await login('client'),b=await login('operator');
 assert.equal((await call('/api/anchor-portal/me',a)).body.user.role,'facility_client');
 assert.equal((await call('/api/anchor-portal/me',b)).body.user.role,'manager');
 assert.equal((await call('/logout',a,'POST',{})).status,200);
 assert.equal((await call('/api/anchor-portal/me',a)).status,401);
 assert.equal((await call('/api/anchor-portal/me',b)).status,200);
 await pool.query('UPDATE users SET active=FALSE WHERE id=$1',[fixture.users.operator.id]);assert.equal((await call('/api/anchor-portal/me',b)).status,401);await pool.query('UPDATE users SET active=TRUE WHERE id=$1',[fixture.users.operator.id]);
 await pool.query("UPDATE portal_sessions SET sess=jsonb_set(sess::jsonb,'{expiresAt}','0') WHERE (sess->>'userId')::int=$1",[fixture.users.operator.id]);
 assert.equal((await call('/api/anchor-portal/me',b)).status,401);
});
test('CSRF, origin, host, role and location gates',async()=>{
 const a=await login('client');
 assert.equal((await call('/__test/session/client',a,'POST',{})).status,404);
 assert.equal((await call('/api/anchor-portal/issues',a,'POST',{}, {'X-CSRF-Token':''})).status,403);
 assert.equal((await call('/api/anchor-portal/issues',a,'POST',{}, {Origin:'https://evil.invalid'})).status,403);
 assert.equal(await new Promise(resolve=>require('node:http').get(base+'/api/anchor-portal/me',{headers:{host:'evil.invalid'}},r=>{r.resume();resolve(r.statusCode);})),400);
 assert.equal((await call('/api/anchor-portal/visits',a,'POST',{location_id:fixture.location.id})).status,403);
 for(const key of ['unassigned','foreignClient','foreignOperator']){
  const user=await login(key);
  assert.equal((await call(`/api/anchor-portal/locations/${fixture.location.id}/scope`,user)).status,403);
 }
});
test('authenticated report/photo/problem/resolution persists across client sessions',async()=>{
 const client=await login('client'),operator=await login('operator');const api='/api/anchor-portal';
 const report=await call(api+'/visits',operator,'POST',{location_id:fixture.location.id,service_date:'2026-10-14',visit_notes:'Synthetic walkthrough',areas:[{title:'Synthetic room',outcome:'completed'}],photos:[illustrativePhoto(0)]});
 assert.equal(report.status,201,JSON.stringify(report.body));
 const view=await call(api+'/visits/'+report.body.visit.id,client);assert.equal(view.body.visit.evidence.length,1);
 const issue=await call(api+'/issues',client,'POST',{location_id:fixture.location.id,description:'Synthetic concern',photo_data_url:illustrativePhoto(1).data_url});assert.equal(issue.status,201,JSON.stringify(issue.body));
 const resolved=await call(api+'/issues/'+issue.body.issue.id,operator,'PATCH',{status:'resolved',resolution:'Synthetic resolution',resolution_photo_data_url:illustrativePhoto(2).data_url});assert.equal(resolved.status,200,JSON.stringify(resolved.body));
 const reload=await call(api+'/issues?location_id='+fixture.location.id,await login('client'));const found=reload.body.issues.find(i=>i.id===issue.body.issue.id);assert.equal(found.status,'resolved');assert.ok(found.evidence.some(p=>p.purpose==='resolution'));
});

test('hosted proxy configuration allows legitimate writes, secure cookies, rejects missing origin',async()=>{
 const origin='https://portal-demo.example.invalid';
 const cfg=config({PORTAL_MODE:'hosted',PORT:'443',PORTAL_ORIGIN:origin,PORTAL_DATABASE_URL:'postgresql://demo:placeholder@database.example.invalid/anchor_portal_synthetic',PORTAL_SESSION_SECRET:crypto.randomBytes(32).toString('hex'),PORTAL_SYNTHETIC_MARKER:MARKER});
 const runtime=await createApp(cfg,pool);
 const listener=await new Promise(resolve=>{const s=runtime.app.listen(0,'127.0.0.1',()=>resolve(s));});
 const send=(path,actor={},body,originHeader=origin)=>new Promise((resolve,reject)=>{
  const req=require('node:http').request({hostname:'127.0.0.1',port:listener.address().port,path,method:body?'POST':'GET',headers:{Host:new URL(origin).host,'X-Forwarded-Proto':'https',Cookie:actor.cookie||'','X-CSRF-Token':actor.csrf||'','Content-Type':'application/json',...(originHeader?{Origin:originHeader}:{})}},res=>{let text='';res.on('data',c=>text+=c);res.on('end',()=>resolve({status:res.statusCode,body:JSON.parse(text),cookie:res.headers['set-cookie']?.[0].split(';')[0],rawCookie:res.headers['set-cookie']?.[0]}));});req.on('error',reject);req.end(body?JSON.stringify(body):undefined);
 });
 try{
  const pre=await send('/auth/csrf');assert.match(pre.rawCookie,/Secure/);assert.match(pre.rawCookie,/HttpOnly/);
  const actor={cookie:pre.cookie,csrf:pre.body.csrf};
  const auth=await send('/login',actor,{email:'client@exeter-fixture.invalid',password});assert.equal(auth.status,200);
  const fresh=await send('/auth/csrf',{cookie:auth.cookie});const user={cookie:auth.cookie,csrf:fresh.body.csrf};
  const payload={location_id:fixture.location.id,description:'Synthetic hosted proxy issue'};
  assert.equal((await send('/api/anchor-portal/issues',user,payload)).status,201);
  assert.equal((await send('/api/anchor-portal/issues',user,payload,null)).status,403);
  assert.equal((await send('/api/anchor-portal/issues',user,payload,'https://wrong.invalid')).status,403);
 }finally{runtime.store.close();await new Promise(r=>listener.close(r));}
});
