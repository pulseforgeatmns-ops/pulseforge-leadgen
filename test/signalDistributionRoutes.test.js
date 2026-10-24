'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const {requestScope,createDistributionRouter}=require('../routes/signalDistribution');
const session={user:{id:1,role:'admin',client_id:null},active_client_id:1};
const req=(extra={})=>({session,query:{},params:{monitorId:'m'},...extra});
function harness(options={}) {
 const routes=[];const calls=[];
 const store={getObservations:async s=>{calls.push(['observations',s]);return[];},listAdmissions:async s=>{calls.push(['admissions',s]);return[];},listMonitors:async s=>{calls.push(['list',s]);return[];},getMonitor:async s=>{calls.push(['monitor',s]);return{id:'m'};},getTransitions:async s=>{calls.push(['transitions',s]);return[];}};
 createDistributionRouter({Router:()=>({get:(path,...handlers)=>routes.push({path,handlers})}),requireAuth:(_q,_s,n)=>n(),requireRole:()=>((_q,_s,n)=>n()),getStore:async()=>store,getDeliveryStore:async()=>({getDeliveries:async s=>{calls.push(['delivery',s]);return[];}}),enabled:()=>true,...options});
 const res={code:200,set:()=>res,status:n=>{res.code=n;return res;},json:v=>{res.body=v;return res;}};
 return{routes,calls,res};
}
test('scope requires verified privileged session and never accepts tenant overrides',()=>{
 assert.deepEqual(requestScope(req()),{tenantId:'1',namespace:'operational'});
 assert.throws(()=>requestScope(req({query:{tenantId:'2'}})));
 assert.throws(()=>requestScope(req({session:{...session,impersonation:{active:true}}})));
 assert.throws(()=>requestScope(req({session:{user:{role:'client',client_id:1}}})));
 assert.throws(()=>requestScope(req({session:{user:{role:'admin'}}})));
 assert.throws(()=>requestScope(req({query:{namespace:'EMPIRICAL'}})));
});
test('list has auth and role guards; read model gets session scope',async()=>{
 const h=harness();assert.equal(h.routes[0].handlers.length,3);
 await h.routes[0].handlers[2](req({query:{namespace:'test'}}),h.res);
 assert.equal(h.res.code,200);assert.deepEqual(h.calls,[['list',{tenantId:'1',namespace:'test'}],['admissions',{tenantId:'1',namespace:'test'}]]);assert.deepEqual(h.res.body.admissions,[]);
});
test('disabled routes never obtain stores',async()=>{
 let obtained=0;const h=harness({enabled:()=>false,getStore:()=>{obtained++;}});
 await h.routes[0].handlers[2](req(),h.res);assert.equal(h.res.code,503);assert.equal(obtained,0);
});
test('detail passes same tenant and namespace to every read',async()=>{
 const h=harness();await h.routes[1].handlers[2](req(),h.res);assert.equal(h.res.code,200);
 for(const [,scope]of h.calls){assert.equal(scope.tenantId,'1');assert.equal(scope.namespace,'operational');}
 assert.deepEqual(h.calls.map(c=>c[0]),['monitor','transitions','observations','delivery']);
});
test('errors are sanitized and invalid monitor denied before reads',async()=>{
 const h=harness();await h.routes[1].handlers[2](req({params:{monitorId:'../../x'}}),h.res);assert.equal(h.res.code,400);assert.equal(h.calls.length,0);
 const j=harness({getStore:()=>{throw new Error('postgres credentials secret');}});await j.routes[0].handlers[2](req(),j.res);assert.equal(j.res.code,503);assert.doesNotMatch(JSON.stringify(j.res.body),/secret|postgres/);
});

test('mobile page is fixed-path and privileged-session gated',async()=>{
 const h=harness();const page=h.routes.find(r=>r.path==='/signal-distribution');assert.equal(page.handlers.length,3);
 h.res.sendFile=p=>{h.res.file=p;return h.res;};await page.handlers[2](req(),h.res);assert.match(h.res.file,/public\/signal-distribution\.html$/);
 const denied=harness();denied.res.sendFile=()=>{throw Error('must not send');};await denied.routes.find(r=>r.path==='/signal-distribution').handlers[2](req({session:null}),denied.res);assert.equal(denied.res.code,403);
});
