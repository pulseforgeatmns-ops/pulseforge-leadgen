'use strict';
const {test,before,after,mock}=require('node:test');
const assert=require('node:assert/strict');
const crypto=require('node:crypto');
const {Pool}=require('pg');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {initializeSchema,provisionDemo}=require('../standalone/anchor-portal/initialize');
const {config,MARKER}=require('../standalone/anchor-portal/config');
const {PINS,ORIGIN,EXPIRES_AT,publicState}=require('../standalone/anchor-portal/public-demo');
const {createApp}=require('../standalone/anchor-portal/runtime');
const {illustrativePhoto}=require('../standalone/anchor-portal/fixtures');
const env={PORTAL_MODE:'hosted',PORT:'443',PORTAL_ORIGIN:ORIGIN,PORTAL_DATABASE_URL:'postgresql://demo:placeholder@database.example.invalid/anchor_portal_synthetic',PORTAL_SYNTHETIC_MARKER:MARKER,PORTAL_SESSION_SECRET:'a'.repeat(64),PORTAL_PUBLIC_DEMO:'enabled',...Object.fromEntries(Object.entries(PINS).map(([key,value])=>['PORTAL_PUBLIC_DEMO_'+key,value]))};
let pg,pool,fixture,listener,runtime;
before(async()=>{
 mock.timers.enable({apis:['Date'],now:new Date('2026-10-16T12:00:00.000Z')});
 pg=await startDisposablePostgres('portal-public-');const admin=new Pool({connectionString:pg.connectionString});
 await admin.query('CREATE DATABASE anchor_portal_synthetic');await admin.end();pool=new Pool({connectionString:pg.connectionString.replace(/\/postgres$/,'/anchor_portal_synthetic')});
 // Freeze SQL time as well as JS time so this fixed-expiry deployment test remains repeatable.
 const query=pool.query.bind(pool);pool.query=(sql,...args)=>query(typeof sql==='string'?sql.replace(/NOW\(\)/gi,"TIMESTAMPTZ '2026-10-16T12:00:00.000Z'"):sql,...args);
 await initializeSchema(pool,MARKER);const hash=await require('bcryptjs').hash(crypto.randomBytes(24).toString('hex'),12);
 fixture=await provisionDemo(pool,MARKER,{client:hash,operator:hash},new Date(EXPIRES_AT));
 runtime=await createApp(config(env),pool);listener=await new Promise(resolve=>{const s=runtime.app.listen(0,'127.0.0.1',()=>resolve(s));});
});
after(async()=>{runtime?.store.close();if(listener)await new Promise(r=>listener.close(r));if(pool)await pool.end();if(pg)await pg.stop();mock.timers.reset();});
function call(path,actor={},method='GET',body,headers={}){
 return new Promise((resolve,reject)=>{const req=require('node:http').request({hostname:'127.0.0.1',port:listener.address().port,path,method,headers:{Host:new URL(ORIGIN).host,'X-Forwarded-Proto':'https',Origin:ORIGIN,Cookie:actor.cookie||'','X-CSRF-Token':actor.csrf||'','Content-Type':'application/json',...headers}},res=>{let text='';res.on('data',c=>text+=c);res.on('end',()=>{let value;try{value=JSON.parse(text);}catch{value=text;}resolve({status:res.statusCode,body:value,cookie:res.headers['set-cookie']?.[0].split(';')[0],rawCookie:res.headers['set-cookie']?.[0]});});});req.on('error',reject);req.end(body?JSON.stringify(body):undefined);});
}
async function entry(){const result=await call('/');assert.equal(result.status,302);assert.match(result.rawCookie,/Secure/);const actor={cookie:result.cookie};actor.csrf=(await call('/auth/csrf',actor)).body.csrf;return actor;}
async function switchRole(actor,role){const result=await call('/auth/demo-role',actor,'POST',{role});assert.equal(result.status,200,JSON.stringify(result.body));assert.notEqual(result.cookie,actor.cookie);assert.equal((await call('/api/anchor-portal/me',actor)).status,401);const next={cookie:result.cookie};next.csrf=(await call('/auth/csrf',next)).body.csrf;assert.notEqual(next.csrf,actor.csrf);return next;}
test('strict opt-in, exact deployment pins, canonical origin, DB and marker',()=>{
 assert.equal(config(env).publicDemo,true);
 for(const key of Object.keys(PINS))for(const value of [undefined,'wrong'])assert.throws(()=>config({...env,['PORTAL_PUBLIC_DEMO_'+key]:value}));
 for(const changes of [{PORTAL_PUBLIC_DEMO:'true'},{PORTAL_PUBLIC_DEMO:'disabled'},{PORTAL_MODE:'local'},{PORTAL_ORIGIN:'https://different.invalid'},{PORTAL_SYNTHETIC_MARKER:'wrong'},{PORTAL_DATABASE_URL:'postgresql://demo:placeholder@database.example.invalid/production'},{DATABASE_URL:'anything'}])assert.throws(()=>config({...env,...changes}));
 const ordinary={...env};for(const key of Object.keys(ordinary))if(key.startsWith('PORTAL_PUBLIC_DEMO'))delete ordinary[key];assert.equal(config(ordinary).publicDemo,false);
 const now=Date.now;Date.now=()=>EXPIRES_AT;try{assert.throws(()=>config(env));}finally{Date.now=now;}
});
test('direct DB-backed entry, session reuse and strict role/CSRF switching',async()=>{
 assert.equal((await call('/auth/csrf')).status,401);assert.equal((await call('/api/anchor-portal/me')).status,401);
 const actor=await entry();const me=await call('/api/anchor-portal/me',actor);assert.equal(me.body.user.id,1);
 assert.equal((await call('/logout',actor)).status,302);
 const count=Number((await pool.query('SELECT count(*) FROM portal_sessions')).rows[0].count);
 assert.equal((await call('/portal/anchor',actor)).status,200);assert.equal(Number((await pool.query('SELECT count(*) FROM portal_sessions')).rows[0].count),count);
 assert.equal((await call('/auth/demo-role',actor,'POST',{role:'operator'},{'X-CSRF-Token':''})).status,403);
 assert.equal((await call('/auth/demo-role',actor,'POST',{role:'operator'},{Origin:'https://evil.invalid'})).status,403);
 for(const body of [{role:'manager'},{role:'__proto__'},{role:2},{role:['operator']},{role:'operator',id:99}])assert.equal((await call('/auth/demo-role',actor,'POST',body)).status,400);
 assert.equal((await call('/login',actor,'POST',{email:'operator@exeter-fixture.invalid',password:'x'})).status,404);
 const operator=await switchRole(actor,'operator');assert.equal((await call('/api/anchor-portal/me',operator)).body.user.id,2);
 await switchRole(operator,'client');
});
test('real report/photo/issue creation and resolution persist across public sessions',async()=>{
 let client=await entry();const operator=await switchRole(client,'operator');const api='/api/anchor-portal';
 const report=await call(api+'/visits',operator,'POST',{location_id:fixture.location.id,service_date:'2026-10-16',visit_notes:'Synthetic public walkthrough',areas:[{title:'Synthetic room',outcome:'completed'}],photos:[illustrativePhoto(0)]});assert.equal(report.status,201,JSON.stringify(report.body));
 client=await switchRole(operator,'client');assert.equal((await call(api+'/visits/'+report.body.visit.id,client)).body.visit.evidence.length,1);
 assert.equal((await call(api+'/visits',client,'POST',{location_id:fixture.location.id})).status,403);
 const issue=await call(api+'/issues',client,'POST',{location_id:fixture.location.id,description:'Synthetic public concern',photo_data_url:illustrativePhoto(1).data_url});assert.equal(issue.status,201);
 const manager=await switchRole(client,'operator');assert.equal((await call(api+'/issues/'+issue.body.issue.id,manager,'PATCH',{status:'resolved',resolution:'Synthetic public resolution',resolution_photo_data_url:illustrativePhoto(2).data_url})).status,200);
 client=await switchRole(manager,'client');const found=(await call(api+'/issues?location_id='+fixture.location.id,client)).body.issues.find(i=>i.id===issue.body.issue.id);assert.equal(found.status,'resolved');assert.ok(found.evidence.some(e=>e.purpose==='resolution'));
});
test('identity drift, marker drift, account expiry and stale sessions fail closed',async()=>{
 const actor=await entry();
 mock.timers.setTime(EXPIRES_AT);assert.equal((await call('/api/anchor-portal/me',actor)).status,410);mock.timers.setTime(Date.parse('2026-10-16T12:00:00.000Z'));
 for(const [field,bad] of [['email','other@exeter-fixture.invalid'],['role','manager'],['client_id',11],['active',false],['valid_until',new Date(EXPIRES_AT+1000)]]){
   const old=(await pool.query('SELECT '+field+' FROM users WHERE id=1')).rows[0][field];await pool.query('UPDATE users SET '+field+'=$1 WHERE id=1',[bad]);
   await assert.rejects(publicState(pool));assert.equal((await call('/api/anchor-portal/me',actor)).status,500);await pool.query('UPDATE users SET '+field+'=$1 WHERE id=1',[old]);
 }
 for(const [field,bad] of [['marker','wrong'],['expires_at',new Date(0)],['expires_at',new Date(EXPIRES_AT+1000)]]){
   const old=(await pool.query('SELECT '+field+' FROM portal_demo_marker')).rows[0][field];await pool.query('UPDATE portal_demo_marker SET '+field+'=$1',[bad]);await assert.rejects(publicState(pool));await pool.query('UPDATE portal_demo_marker SET '+field+'=$1',[old]);
 }
 const location=fixture.location.id;
 for(const [field,bad] of [['operator_client_id',11],['is_demo',false],['slug','other']]){
   const old=(await pool.query('SELECT '+field+' FROM anchor_portal_locations WHERE id=$1',[location])).rows[0][field];await pool.query('UPDATE anchor_portal_locations SET '+field+'=$1 WHERE id=$2',[bad,location]);await assert.rejects(publicState(pool));await pool.query('UPDATE anchor_portal_locations SET '+field+'=$1 WHERE id=$2',[old,location]);
 }
 // Public-mode credentials cannot survive reverting to the ordinary auth mode.
 const plain={...config(env),publicDemo:false};const oldRuntime=runtime,oldListener=listener;
 const ordinary=await createApp(plain,pool);const normal=await new Promise(resolve=>{const s=ordinary.app.listen(0,'127.0.0.1',()=>resolve(s));});
 listener=normal;try{assert.equal((await call('/api/anchor-portal/me',actor)).status,401);}finally{listener=oldListener;runtime=oldRuntime;ordinary.store.close();await new Promise(r=>normal.close(r));}
 const altered=await entry();
 await pool.query("UPDATE portal_sessions SET sess=jsonb_set(sess::jsonb,'{userId}','999') WHERE sess->>'publicDemo'='client'");assert.equal((await call('/api/anchor-portal/me',altered)).status,401);
 const fresh=await entry();await pool.query("UPDATE portal_sessions SET sess=jsonb_set(sess::jsonb,'{expiresAt}','0')");assert.equal((await call('/api/anchor-portal/me',fresh)).status,401);
});
test('cookie-less session issuance is bounded and rejects without creating rows',async()=>{
 let limited=false;for(let i=0;i<30;i++){const result=await call('/');if(result.status===429){limited=true;break;}assert.equal(result.status,302);}assert.equal(limited,true);
 const before=Number((await pool.query('SELECT count(*) FROM portal_sessions')).rows[0].count);assert.equal((await call('/')).status,429);assert.equal((await call('/auth/csrf')).status,401);assert.equal(Number((await pool.query('SELECT count(*) FROM portal_sessions')).rows[0].count),before);
});
test('write and global issuance ceilings cannot be evaded by changing claimed source',async()=>{
 // Use one independently obtained valid session; previous test exhausted the original source.
 const source={'X-Forwarded-For':'192.0.2.1'};
 const entryResult=await call('/',{},'GET',undefined,source);assert.equal(entryResult.status,302);
 const actor={cookie:entryResult.cookie};actor.csrf=(await call('/auth/csrf',actor)).body.csrf;
 const count=Number((await pool.query('SELECT count(*) FROM portal_sessions')).rows[0].count);
 for(let i=0;i<60;i++)assert.equal((await call('/auth/demo-role',actor,'POST',{role:'invalid'},source)).status,400);
 assert.equal((await call('/auth/demo-role',actor,'POST',{role:'operator'},source)).status,429);
 assert.equal(Number((await pool.query('SELECT count(*) FROM portal_sessions')).rows[0].count),count);
 assert.equal((await call('/api/anchor-portal/me',actor)).body.user.id,1);
 let limited=false;
 for(let i=2;i<140;i++){
   const result=await call('/',{},'GET',undefined,{'X-Forwarded-For':`192.0.2.${i}`});
   if(result.status===429){limited=true;break;}assert.equal(result.status,302);
 }
 assert.equal(limited,true);
 const finalCount=Number((await pool.query('SELECT count(*) FROM portal_sessions')).rows[0].count);
 assert.equal((await call('/',{},'GET',undefined,{'X-Forwarded-For':'198.51.100.1'})).status,429);
 assert.equal(Number((await pool.query('SELECT count(*) FROM portal_sessions')).rows[0].count),finalCount);
 assert.equal((await call('/portal/anchor',actor)).status,200);
 // Distributed writes hit a shared burst ceiling, then the lifetime ceiling.
 let burst=false;for(let i=1;i<=130;i++){
   const result=await call('/auth/demo-role',actor,'POST',{role:'invalid'},{'X-Forwarded-For':`203.0.113.${i}`});
   if(result.status===429){assert.equal(result.body.error,'demo_write_limit');burst=true;break;}assert.equal(result.status,400);
 }
 assert.equal(burst,true);mock.timers.setTime(Date.now()+61000);
 let lifetime=false;for(let i=1;i<=130;i++){
   const result=await call('/auth/demo-role',actor,'POST',{role:'invalid'},{'X-Forwarded-For':`203.0.113.${i}`});
   if(result.status===429){assert.equal(result.body.error,'demo_write_limit');lifetime=true;break;}assert.equal(result.status,400);
 }
 assert.equal(lifetime,true);mock.timers.setTime(Date.now()+61000);
 assert.equal((await call('/auth/demo-role',actor,'POST',{role:'operator'})).status,429);
 assert.equal((await call('/api/anchor-portal/me',actor)).status,200);
 assert.equal(Number((await pool.query('SELECT count(*) FROM portal_sessions')).rows[0].count),finalCount);
});
