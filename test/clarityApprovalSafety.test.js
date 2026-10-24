'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const {createExecutionRequest,EXECUTION_INTENTS}=require('../packages/acquisition-mission/ExecutionRequest');
const {routeExecutionRequest}=require('../packages/acquisition-mission/ExecutionRouter');
const {scope,checkCsrf}=require('../routes/clarity');
const {runClarityJobs}=require('../services/clarityJobs');
function request(overrides={}) {return createExecutionRequest({source:'approval_button',intent:EXECUTION_INTENTS.REVIEW_CLARITY_RECOMMENDATION,
 runtimeOwner:'clarity_review',executionMode:'manual',operatorId:1,permissions:{canExecute:true,role:'admin'},payload:{client_id:10,decision:'approve'},...overrides});}
test('canonical router records review only; custom execution handlers cannot bypass fixed handler',async()=>{
 let calls=0;
 const result=await routeExecutionRequest(request(),{tenantId:10,env:{CLARITY_ENABLED:'true'},
 handlers:{[EXECUTION_INTENTS.REVIEW_CLARITY_RECOMMENDATION](){assert.fail('no arbitrary handler');}},
 clarityStore:{getIntegration:async()=>({enabled:true,consent_status:'authorized'}),decide:async args=>{calls++;assert.equal(args.clientId,10);return {decision:{website_changes_authorized:false}};}}});
 assert.equal(calls,1);assert.equal(result.routed,true);assert.equal(result.runtimeOwner,'clarity_review');assert.equal(result.executionResult.decision.website_changes_authorized,false);
});
test('review intent rejects autonomous, no actor, missing policy, tenant mismatch and dark state',async()=>{
 const context={tenantId:10,env:{CLARITY_ENABLED:'true'},clarityStore:{getIntegration(){assert.fail('must fail before read');}}};
 for(const override of [{executionMode:'autonomous'},{operatorId:null},{executionMode:'read_only'},{runtimeOwner:'amo'},{missionId:'mission_fake'},{permissions:{canExecute:true,role:'viewer'}},{payload:{client_id:11}}]) {
 await assert.rejects(routeExecutionRequest(request(override),context)); }
 await assert.rejects(routeExecutionRequest(request(),{...context,env:{}}),/disabled/);
});
test('HTTP scope fails closed missing tenant, cross tenant, impersonation and unprivileged decision',()=>{
 const admin={id:1,role:'admin',active:true};
 assert.throws(()=>scope({session:{user:admin}}),/tenant/);
 assert.throws(()=>scope({session:{user:{id:2,role:'client',client_id:11}},query:{client_id:10}}),/mismatch/);
 assert.throws(()=>scope({session:{user:admin,active_client_id:10,impersonation:{active:true,effectiveUser:{id:2,role:'client',client_id:10}}}},true),/forbidden/);
 assert.throws(()=>scope({session:{user:{id:2,role:'viewer'},active_client_id:10}},true),/forbidden/);
 assert.equal(scope({session:{user:admin,active_client_id:10}},true).canDecide,true);
});
test('CSRF is session bound and origin is checked',()=>{
 const req={session:{clarity_csrf:'a'.repeat(64)},protocol:'https',get:name=>({'X-CSRF-Token':'a'.repeat(64),host:'example.test',origin:'https://example.test'}[name])};
 assert.doesNotThrow(()=>checkCsrf(req,{APP_URL:"https://example.test"}));
 assert.throws(()=>checkCsrf({...req,session:{clarity_csrf:'b'.repeat(64)}}),/csrf/);
 assert.throws(()=>checkCsrf({...req,get:name=>name==='origin'?'https://evil.test':req.get(name)},{APP_URL:'https://example.test'}),/origin/);
});
test('scheduled job is dark before acquiring store or network',async()=>{
 assert.deepEqual(await runClarityJobs({env:{},pool:{query(){assert.fail();}}}),{skipped:true,reason:'disabled'});
});

test('Clarity evidence reads revalidate current database identity including foreign managers/viewers',async()=>{
 const {authorizeClarity}=require('../services/clarityAuthorization');
 const req={session:{user:{id:1,role:'admin',active:true},active_client_id:10},query:{client_id:10}};
 for(const actor of [{id:1,role:'admin',active:false},{id:1,role:'manager',active:true,client_id:11},{id:1,role:'viewer',active:true,client_id:11},{id:1,role:'viewer',active:true,client_id:null}]) {
   await assert.rejects(authorizeClarity(req,{pool:{query:async()=>({rows:[actor]})}}));
 }
 const actor={id:1,role:'viewer',active:true,client_id:10};
 assert.equal((await authorizeClarity(req,{pool:{query:async()=>({rows:[actor]})}})).canDecide,false);
 await assert.rejects(authorizeClarity(req,{pool:{query:async()=>({rows:[actor]})},write:true}),/forbidden/);
 const {buildLivingCommandBrief}=require('../services/livingCommandBrief');
 const brief=await buildLivingCommandBrief({clientId:10,pool:{query:async()=>({rows:[{}]})}});
 assert.equal(brief.clarity.available,false,'default service call never reveals Clarity evidence without authorized request scope');
});

test('trusted public HTTPS origin works behind HTTP proxy; forwarded headers never grant trust',()=>{
 const req={session:{clarity_csrf:'a'.repeat(64)},protocol:'http',get:name=>({'X-CSRF-Token':'a'.repeat(64),host:'internal:8080',origin:'https://public.test','X-Forwarded-Proto':'https'}[name])};
 assert.doesNotThrow(()=>checkCsrf(req,{APP_URL:'https://public.test'}));
 assert.throws(()=>checkCsrf(req,{}),/origin/);
 assert.throws(()=>checkCsrf(req,{APP_URL:'https://other.test'}),/origin/);
});
