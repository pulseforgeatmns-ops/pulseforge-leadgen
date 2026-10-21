'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { parseReviewNavigation: parse, resolveReviewNavigation: resolve } = require('../packages/max/workspace/ReviewNavigation');
const { createReviewNavigationRouter } = require('../routes/reviewNavigation');
const { create } = require('../public/shared/review-navigation');

// Versioned diagnostic corpus, fixed before implementation scoring. Families are
// distinct supported workflows, not a production-accuracy or sustained proof claim.
const cases = [
 ['Open content review','content_review'], ['Can you open content review?','content_review'],
 ['I would like to review the content.','content_review'], ['Please show me pending posts','content_review'],
 ['What drafts are awaiting approval?','content_review'], ['Does Paige have pending posts for me to approve?','content_review'],
 ['Preview our social posts','content_review'], ['Let me see the drafts','content_review'],
 ['Review Tony’s accounts','account_review','Tony'], ['Can you give me a review of Tony\'s accounts?','account_review','Tony'],
 ['Show Tony\'s accounts please','account_review','Tony'], ['Review Tony\'s assigned accounts','account_review','Tony'],
 ['Open the accounts assigned to Tony','account_review','Tony'], ['Check accounts for Rory Matthews','account_review','Rory Matthews'],
 ['Could you show me Mike\'s accounts?','account_review','Mike'], ['Review accounts','account_review',null],
 ['Review content then publish it','blocked'], ['Show pending posts and schedule them','blocked'],
 ['Open Tony\'s accounts and send messages','blocked'], ['Don\'t open content review','blocked'],
 ['Stop showing the accounts','blocked'], ['Review content and approve everything','blocked'],
 ['Generate new posts for review','blocked'], ['Review and delete Tony\'s accounts','blocked'],
 ['Approve all pending posts',null], ['Reject the draft',null], ['Continue',null], ['Write content for Facebook',null],
 ['Review the acquisition mission',null], ['Give me Penny\'s current Google Ads report',null],
 ['Review outreach email drafts in this mission',null], ['Find saved previews',null],
 ['What are the accounts payable totals?',null], ['Tell me how to create content',null],
];
for (const [question, kind, ownerName] of cases) test(`routing corpus: ${question}`, () => {
 const result = parse(question); assert.equal(result?.kind || null, kind);
 if (ownerName !== undefined) assert.equal(result.ownerName, ownerName);
});
const roster = [{ id: 24, name: 'Tony' }, { id: 20, name: 'Mike' }];
test('owner resolution is scoped, unique and never guessed from conversation', () => {
 assert.equal(resolve(parse('Review Tony\'s accounts'), { tenantId: 10, aos: roster }).uiAction.aoId,24);
 assert.equal(resolve(parse('Review Tony\'s accounts'), { tenantId: 11, aos: [{id:3,name:'Rory'}] }).uiAction,null);
 assert.equal(resolve(parse('Review Tony\'s accounts'), { tenantId: 10, aos: [...roster,{id:25,name:'Tony Smith'}] }).metadata.navigationStatus,'clarification_required');
 assert.equal(resolve(parse('Review Tony\'s accounts'), { tenantId: 10 }).metadata.navigationStatus,'unavailable');
 assert.equal(resolve(parse('Review accounts'), { tenantId: 10, aos: roster }).uiAction,null);
});
function fixture() {
 const users = [{ id:1, role:'admin', client_id:10, active:true },{ id:2, role:'manager',client_id:10,active:true },{id:24,role:'ao',client_id:10,active:true,name:'Tony'},{id:25,role:'ao',client_id:11,active:true,name:'Rory'}];
 let unavailable = false;
 const db = { query: async (sql, values) => {
  assert.doesNotMatch(sql,/INSERT|UPDATE|DELETE|CREATE|ALTER/);
  if(unavailable) throw new Error('private database information');
  if(sql.includes('ANY')) return {rows:users.filter(x=>values[0].includes(x.id))};
  if(sql.startsWith('SELECT id, name')) return {rows:users.filter(x=>x.client_id===values[0] && x.role==='ao' && x.active!==false && (values[1]==null || x.id===values[1]))};
  return {rows:users.filter(x=>x.id===Number(values[0]) && x.client_id===values[1] && x.active!==false && x.role==='ao')};
 }};
 const router = createReviewNavigationRouter({db});
 function request(body, session = {user:{id:1,role:'admin',client_id:10},active_client_id:10}) {
  return new Promise((done,reject)=>{
   const path='/api/v1/max/workspace/review-navigation';
   const req={method:'POST',url:path,originalUrl:path,path,body,session,query:{},get:()=> 'application/json'};
   const res={statusCode:200,set(){return this;},status(code){this.statusCode=code;return this;},json(body){done({status:this.statusCode,body});}};
   router.handle(req,res,e=>reject(e||new Error('unhandled')));
  });
 }
 return {request,users,fail:()=>{unavailable=true;}};
}
test('HTTP button/text paths share tenant-revalidated decision and never action work',async()=>{
 const {request}=fixture();
 for(const body of [{question:'Can you review Tony\'s accounts?'},{reviewNavigation:{surface:'account_review',aoId:24}}]) {
  const r=await request(body); assert.equal(r.status,200); assert.equal(r.body.uiAction.aoId,24); assert.equal(r.body.uiAction.tenantId,'10'); assert.equal(r.body.metadata.navigationStatus,'requested'); assert.doesNotMatch(r.body.prose,/completed|opened|finished/);
 }
 const c=await request({reviewNavigation:{surface:'content_review'}});assert.equal(c.body.uiAction.type,'open_content_review');
 const mixed=await request({question:'send all posts',reviewNavigation:{surface:'content_review'}});assert.equal(mixed.body.uiAction,null);
});
test('HTTP revoked, stale tenant, wrong owner, role and unavailable scope fail closed',async()=>{
 const f=fixture();
 assert.equal((await f.request({question:'Review Tony\'s accounts'},{})).status,401);
 assert.equal((await f.request({reviewNavigation:{surface:'account_review',aoId:25}})).status,403);
 assert.equal((await f.request({question:'Review Tony\'s accounts',tenantId:11})).status,403);
 assert.equal((await f.request({question:'Review content'},{user:{id:24,role:'ao',client_id:10},active_client_id:10})).status,403);
 f.users[0].active=false;assert.equal((await f.request({question:'Review content'})).status,403);
 f.users[0].active=true;f.fail();const r=await f.request({question:'Review content'});assert.equal(r.status,503);assert.doesNotMatch(JSON.stringify(r),/private database/);
});
test('browser decision consumer drops dismissed, newer and changed-tenant responses',async()=>{
 let tenant=10, pending=[], shown=[], notices=[], receipts=[];
 const client=create({tenant:()=>tenant,request:body=>new Promise(resolve=>pending.push({resolve,body})),render:a=>{shown.push(a);return true;},notify:t=>notices.push(t),rendered:a=>receipts.push(a)});
 const action={type:'open_content_review',tenantId:'10'};
 const first=client.open({surface:'content_review'});client.invalidate();pending.shift().resolve({uiAction:action});assert.equal(await first,false);
 const second=client.open({surface:'content_review'});tenant=11;pending.shift().resolve({uiAction:action});assert.equal(await second,false);
 tenant=10;const older=client.open({surface:'content_review'});const newer=client.open({surface:'content_review'});pending.shift().resolve({uiAction:action});assert.equal(await older,false);pending.shift().resolve({uiAction:action});assert.equal(await newer,true);
 assert.equal(shown.length,1);assert.equal(receipts.length,1);
});
test('renderer absence and API failure cannot acknowledge rendered success, retry can recover',async()=>{
 let fail=true,renderReady=false,receipt=0;
 const client=create({tenant:()=>10,request:async()=>{if(fail)throw new Error('failure');return {uiAction:{type:'open_content_review',tenantId:'10'}};},render:()=>renderReady,rendered:()=>receipt++});
 assert.equal(await client.open({surface:'content_review'}),false);fail=false;
 assert.equal(await client.open({surface:'content_review'}),false);renderReady=true;
 assert.equal(await client.open({surface:'content_review'}),true);assert.equal(receipt,1);
});

// Independent review counterexamples, reported separately from the fixed corpus.
test('held-out review paraphrases and own accounts use canonical authority', () => {
 for (const question of ['Can I see Tony’s accounts?', 'Let me review Tony’s account list']) assert.equal(parse(question).ownerName, 'Tony');
 for (const question of ['Please bring up Content Review', 'Show me the content that needs review']) assert.equal(parse(question).kind, 'content_review');
 const own = resolve(parse('Could you please show my accounts?'), {tenantId:10,aos:roster,defaultAoId:24});
 assert.equal(own.uiAction.aoId,24);
 assert.equal(parse('Show me Tony’s accounts; approve all changes').kind,'blocked');
});
test('missing bootstrap never silently consumes ordinary text; authenticated scope enables AO review', async () => {
 const ordinary = create({tenant:()=>undefined,request:async body=>{assert.equal(body.tenantId,undefined);return {handled:false};},render:()=>true});
 assert.equal(await ordinary.routeText('How is my day looking?'),false);
 let renders=0;
 const ao = create({tenant:()=>undefined,scope:async()=>10,request:async body=>{assert.equal(body.tenantId,'10');return {handled:true,uiAction:{type:'open_account_review',tenantId:'10',aoId:24}};},render:()=>{renders++;return true;}});
 assert.equal(await ao.routeText('Show my accounts'),true); assert.equal(renders,1);
});
test('explicit initial hash review awaits scope; dismissal during bootstrap prevents navigation', async () => {
 let finish, requests=0, renders=0; const notices=[];
 const controller=create({tenant:()=>undefined,scope:()=>new Promise(resolve=>finish=resolve),notify:x=>notices.push(x),request:async()=>{requests++;return {uiAction:{type:'open_content_review',tenantId:'10'}};},render:()=>{renders++;return true;}});
 const first=controller.open({surface:'content_review'});controller.invalidate();finish(10);assert.equal(await first,false);assert.equal(requests,0);assert.deepEqual(notices,['Checking review scope…']);
 const second=controller.open({surface:'content_review'});finish(10);assert.equal(await second,true);assert.equal(renders,1);
});
test('bootstrap rejection is visible, retains blocked text and permits a verified retry', async () => {
 let fail=true, shown=0; const notices=[];
 const controller=create({tenant:()=>undefined,scope:async()=>{if(fail)throw new Error('scope failure');return 10;},request:async()=>({uiAction:{type:'open_content_review',tenantId:'10'}}),render:()=>{shown++;return true;},notify:x=>notices.push(x)});
 assert.equal(await controller.routeText('Review content'),'blocked');assert.match(notices.at(-1),/could not be checked/);
 assert.equal(await controller.open({surface:'content_review'}),false);assert.match(notices.at(-1),/unavailable/);assert.equal(shown,0);
 fail=false;assert.equal(await controller.open({surface:'content_review'}),true);assert.equal(shown,1);
});
test('actual review renderers reject data from a different tenant without shell globals', async () => {
 const fs=require('node:fs'),vm=require('node:vm');
 const account=fs.readFileSync('public/shared/account-review.js','utf8');
 const scoped=account.slice(account.indexOf('  function scoped(data)'),account.indexOf('\n',account.indexOf('  function scoped(data)')));
 const accountContext={owner:{aoId:24,tenant:'10'},activeTenant:()=>undefined};vm.createContext(accountContext);vm.runInContext(scoped,accountContext);
 assert.equal(accountContext.scoped({ao_id:24,tenant_id:10}),true);
 assert.equal(accountContext.scoped({ao_id:24,tenant_id:11}),false);
 const content=fs.readFileSync('public/shared/content-review.js','utf8');
 const load=content.slice(content.indexOf('  async function load()'),content.indexOf('  function open(',content.indexOf('  async function load()')));
 const notices=[];let retries=0;
 const context={generation:0,preview:null,model:null,expectedTenant:'10',dialog:{hidden:false},activeClient:()=>undefined,
  $:()=>({replaceChildren(){}}),node:()=>({}),notice:x=>notices.push(x),button:()=>retries++,
  request:async path=>{assert.match(path,/client_id=10/);return {clientId:11,items:[],pendingCount:0};}};
 vm.createContext(context);vm.runInContext(load,context);await context.load();
 assert.equal(context.model,null);assert.match(notices.at(-1),/could not be loaded/);assert.equal(retries,1);
});
test('legacy ask navigation failures preserve denial status without leaking diagnostics', async () => {
 const fs=require('node:fs'),vm=require('node:vm');
 const source=fs.readFileSync('routes/maxChat.js','utf8');
 const start=source.indexOf('    let reviewNavigation;');
 const code=source.slice(start,source.indexOf('    if (isApiUsageQuestion',start));
 for(const status of [403,undefined]) {
  const response={statusCode:200,status(value){this.statusCode=value;return this;},json(value){this.body=value;return this;}};
  const context={req:{},res:response,reviewNavigationForRequest:async()=>{throw Object.assign(new Error('private database credential detail'),{statusCode:status});}};
  vm.createContext(context);await vm.runInContext(`(async()=>{${code}})()`,context);
  assert.equal(response.statusCode,status || 503);assert.doesNotMatch(JSON.stringify(response.body),/private database|credential/);
 }
});

test('secondary approval imperatives fail closed across punctuation and conjunctions', () => {
 for(const question of ["Show me Tony’s accounts ; approve all changes", "Show me content review. Approve everything", "Open Content Review, approve everything", "Show Tony’s accounts and also approve changes"]) assert.equal(parse(question).kind,'blocked',question);
 for(const question of ['Show drafts awaiting approval','Does Paige have pending posts for me to approve?']) assert.equal(parse(question).kind,'content_review');
});
test('navigation ownership excludes informational and mission artifact requests before safety parsing', () => {
 for(const question of [
  'What are the approval requirements for content?', 'How do I review pending content?',
  'Explain why these drafts are awaiting approval', 'Review our content approval policy',
  'Summarize pending social content without opening a review',
  'Continue the Campaign 001 preparation-only canary package. Do not launch, execute, approve, or mail anything. Create provisional review drafts using only known facts.',
  'Create a preparation-only packet review checklist for PM-001. Include packet contents and personalized letter draft. Do not create a mission. Do not launch or execute.',
  'Proceed with the recommended next preparation-only work order: PM-001 packet-content review. Do not launch, execute, approve, print, or mail.',
  'Summarize the Campaign 001 preparation-only canary status. Desk note: review packet contents. Do not create a mission.'
 ]) assert.equal(parse(question),null,question);
 assert.equal(parse('Show Tony’s accounts, but do not approve anything').kind,'account_review');
});

test('owned review cannot discard generation, assignment or explicit cancellation',()=>{
 for(const question of ['Review content and generate posts','Open content review then create new posts','Show Tony’s accounts and assign them to Mike','Show drafts awaiting approval and generate new posts','Open Content Review. Do not open the review.']) assert.equal(parse(question).kind,'blocked',question);
});
