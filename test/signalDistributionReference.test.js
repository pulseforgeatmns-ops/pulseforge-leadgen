'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {createDistributionReferenceRouter}=require('../routes/signalDistributionReference');
function harness() {
  let handler,writes=0;const guards=[];
  createDistributionReferenceRouter({Router:()=>({post:(_path,...h)=>{handler=h[2];guards.push(...h.slice(0,2));}}),requireAuth(){},requireRole:()=>()=>{},getStore:async()=>({getMonitor:async s=>s.tenantId==='1'?{id:'m'}:null,setManualReference:async()=>{writes++;}}),enabled:()=>true,appUrl:()=> 'https://example.com'});
  const req={session:{user:{id:1,role:'admin',client_id:1}},query:{},params:{monitorId:'m'},body:{priceUsd:1.5,recordedAt:'2026-10-14T12:00:00Z',source:'EXPLICIT_MANUAL_OPERATOR',confirm:true},get:()=> 'https://example.com',is:()=>true};
  const res={code:200,set(){return this;},status(c){this.code=c;return this;},json(body){this.body=body;return this;}};
  return {req,res,handler,guards,writes:()=>writes};
}
test('manual reference is separate explicit owner-scoped authenticated action',async()=>{
  const h=harness();assert.equal(h.guards.length,2);await h.handler(h.req,h.res);assert.equal(h.res.code,201);assert.equal(h.writes(),1);assert.equal(h.res.body.noTradeExecuted,true);
});
test('cross-origin, missing Origin, wrong tenant and no confirmation cannot write',async()=>{
  for(const change of [h=>h.req.get=()=>undefined,h=>h.req.get=()=> 'https://attacker.test',h=>h.req.session.user.client_id=2,h=>h.req.body.confirm=false,h=>h.req.query.tenantId='2',h=>h.req.is=()=>false]) {
    const h=harness();change(h);await h.handler(h.req,h.res);assert.notEqual(h.res.code,201);assert.equal(h.writes(),0);
  }
});
test('zero/string price and unapproved provenance are denied',async()=>{
  for(const change of [h=>h.req.body.priceUsd=0,h=>h.req.body.priceUsd='1',h=>h.req.body.source='CALL_TIME_ASSUMED',h=>h.req.body.walletKey='secret']) {
    const h=harness();change(h);await h.handler(h.req,h.res);assert.equal(h.res.code,400);assert.equal(h.writes(),0);
  }
});
