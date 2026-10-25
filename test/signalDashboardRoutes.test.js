'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),express=require('express');
const {createDashboardRouter,authorize}=require('../routes/signalDashboard');
const {config}=require('../services/signalDashboard/config');
const {CA,W}=require('./helpers/signalDashboardFixtures');
const c={enabled:true,tenantId:'1',ownerUserId:'7',wallet:W};
const session={user:{id:7,role:'admin',client_id:1}};
test('configuration requires explicit public wallet and tenant/owner; default off has no side effects',()=>{
 assert.deepEqual(config({}),{enabled:false});assert.throws(()=>config({SIGNAL_DASHBOARD_ENABLED:'1'}));
 assert.equal(config({SIGNAL_DASHBOARD_ENABLED:'1',SIGNAL_DASHBOARD_TENANT_ID:'1',SIGNAL_DASHBOARD_OWNER_USER_ID:'7',SIGNAL_DASHBOARD_PUBLIC_WALLET:W}).wallet,W);
});
test('verified owner session scope rejects cross-tenant, impersonation and overrides',()=>{
 assert.equal(authorize({session,query:{}},c),true);
 for(const req of [{session:{user:{...session.user,id:8}}},{session:{user:{...session.user,client_id:2}}},{session:{...session,impersonation:{active:true}}},{session,query:{wallet:W}},{session,query:{tenant_id:2}},{session:null}])assert.equal(authorize(req,c),false);
});
test('authenticated HTTP API, polling, invalid-address guards, CSRF, notes-only writes and no transaction execution routes',async t=>{
 const app=express();app.use(express.json());let active=session,disabled=false,reads=0,notes=0;
 app.use((req,_res,next)=>{req.session=active;next();});
 app.use(createDashboardRouter({Router:express.Router,requireAuth:(req,res,next)=>req.session?next():res.sendStatus(401),requireRole:()=>((_q,_s,next)=>next()),getConfig:()=>({...c,enabled:!disabled}),
  getService:()=>{reads++;return {positions:async()=>({positions:[],readOnly:true}),detail:async()=>({tokenAddress:CA,events:[],risk:{state:'UNKNOWN'},asOf:'now',pollAfterMs:10000}),candles:async()=>({candles:[]}),transactions:async()=>[],annotate:async()=>{notes++;return {category:'OPERATOR_NOTE'};}};}}));
 const server=app.listen(0,'127.0.0.1');t.after(()=>new Promise(r=>server.close(r)));await new Promise(r=>server.once('listening',r));const base=`http://127.0.0.1:${server.address().port}`;
 let r=await fetch(base+'/api/v1/signal/positions');assert.equal(r.status,200);assert.equal(r.headers.get('cache-control'),'no-store');
 r=await fetch(base+'/api/v1/signal/stream/'+CA);assert.equal((await r.json()).transport,'bounded-polling');
 const before=reads;assert.equal((await fetch(base+'/api/v1/signal/positions/invalid')).status,400);assert.equal(reads,before);
 assert.equal((await fetch(base+`/api/v1/signal/positions/${CA}/candles?interval=1s`)).status,400);
 const post={method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:'Recovered principal'})};
 assert.equal((await fetch(base+`/api/v1/signal/positions/${CA}/annotations`,post)).status,403);
 assert.equal((await fetch(base+`/api/v1/signal/positions/${CA}/annotations`,{...post,headers:{...post.headers,Origin:'https://attacker.example'}})).status,403);
 assert.equal((await fetch(base+`/api/v1/signal/positions/${CA}/annotations`,{...post,headers:{...post.headers,Origin:base}})).status,201);assert.equal(notes,1);
 for(const suffix of ['transactions','buy','sell','sign'])assert.equal((await fetch(base+`/api/v1/signal/positions/${CA}/${suffix}`,post)).status,404);
 active={user:{...session.user,id:8}};assert.equal((await fetch(base+'/api/v1/signal/positions')).status,403);
 active=null;assert.equal((await fetch(base+'/api/v1/signal/positions')).status,401);
 active=session;disabled=true;assert.equal((await fetch(base+'/api/v1/signal/positions')).status,503);
});
test('dashboard modules and disabled worker startup load without DB/provider calls or signing libraries',async()=>{
 for(const file of ['accounting','analytics','config','decimal','events','market','quality','quotes','researchBridge','runtime','service','store','wallet','worker'])assert.doesNotThrow(()=>require('../services/signalDashboard/'+file));
 assert.equal(await require('../scripts/signalDashboardWorker').main({}),null);assert.doesNotThrow(()=>require('../routes/signalV1'));
});
