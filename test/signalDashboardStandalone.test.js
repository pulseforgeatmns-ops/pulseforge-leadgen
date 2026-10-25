'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),session=require('express-session'),bcrypt=require('bcryptjs');
const {createSignalApp,resolveOwner}=require('../standalone/signal-dashboard/app');
assert.doesNotThrow(()=>require('../standalone/signal-dashboard/server'));
const {migrate}=require('../standalone/signal-dashboard/migrate');
const {CA,W}=require('./helpers/signalDashboardFixtures');
test('standalone requires exact existing owner identity and never provisions accounts',async()=>{
 const pool={query:async()=>({rows:[{id:7,email:'owner@example.test',role:'admin',client_id:null,active:true}]})};
 assert.equal((await resolveOwner(pool,{SIGNAL_DASHBOARD_OWNER_EMAIL:'owner@example.test'})).id,7);
 await assert.rejects(()=>resolveOwner(pool,{SIGNAL_DASHBOARD_OWNER_EMAIL:'wrong@example.test'}),/unresolved/);
 await assert.rejects(()=>resolveOwner(pool,{}),/identity_required/);
});
test('explicit migration approval is required before any schema access',async()=>{
 let calls=0;await assert.rejects(()=>migrate({connect:async()=>{calls++;}},{SIGNAL_DASHBOARD_SCHEMA_APPROVAL:''}),/explicit/);assert.equal(calls,0);
});
test('standalone signs in only the existing owner, regenerates session, enforces CSRF and revokes password changes',async t=>{
 let owner={id:7,email:'owner@example.test',name:'Owner',role:'admin',client_id:null,active:true,password_change_required:false,password_hash:await bcrypt.hash('test-password',4)},attempts=0;
 const pool={query:async(sql,args)=>{
  assert.doesNotMatch(sql,/UPDATE users|INSERT INTO users|ALTER TABLE users/);
  if(sql.includes('signal_dashboard_auth_attempts'))return {rows:[{attempts:++attempts}]};
  return {rows:args?.[0]==7&&(!args[1]||args[1]===owner.email)?[{...owner}]:[]};}};
 const app=createSignalApp({pool,config:{enabled:true,tenantId:'1',ownerUserId:'7',wallet:W},sessionSecret:'s'.repeat(48),secureCookies:false,sessionStore:new session.MemoryStore(),service:{positions:async()=>({positions:[],readOnly:true})}});
 const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));t.after(()=>new Promise(r=>server.close(r)));const base=`http://127.0.0.1:${server.address().port}`;
 assert.equal((await fetch(base+'/health')).status,200);assert.equal((await fetch(base+'/api/v1/signal/positions')).status,401);
 const post={method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:owner.email,password:'test-password'}),redirect:'manual'};
 assert.equal((await fetch(base+'/login',post)).status,403);
 assert.equal((await fetch(base+'/login',{...post,headers:{...post.headers,Origin:'https://foreign.test'}})).status,403);
 const login=await fetch(base+'/login',{...post,headers:{...post.headers,Origin:base}});assert.equal(login.status,302);const cookie=login.headers.get('set-cookie').split(';')[0];assert.match(login.headers.get('set-cookie'),/HttpOnly/);assert.match(login.headers.get('set-cookie'),/SameSite=Strict/);
 assert.equal((await fetch(base+'/api/v1/signal/positions',{headers:{Cookie:cookie}})).status,200);
 owner.password_hash=await bcrypt.hash('changed-password',4);assert.equal((await fetch(base+'/api/v1/signal/positions',{headers:{Cookie:cookie}})).status,403);
 attempts=10;assert.equal((await fetch(base+'/login',{...post,headers:{...post.headers,Origin:base}})).status,429);
});
