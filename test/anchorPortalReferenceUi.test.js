'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const source=fs.readFileSync(require('node:path').join(__dirname,'../public/anchor-portal.js'),'utf8');
function setup(responses={}) {
 const elements={};
 for(const id of ['view','flash','tabs','workspaceActions','building','demoBanner','pageTitle','pageSubtitle','issuePhoto','issuePhotoStatus','issuePhotoPreview','cancelForm','portalForm']) elements[id]={innerHTML:'',textContent:'',querySelectorAll:()=>[],classList:{toggle(){}}};
 const ctx=vm.createContext({document:{getElementById:id=>elements[id]},console,crypto:require('node:crypto'),window:{},fetch:async path=>{const result=responses[path.replace('/api/anchor-portal','')];if(result instanceof Error)throw result;return {ok:true,status:200,json:async()=>result||{}};}});
 const run=code=>vm.runInContext(code,ctx);
 run(source.replace(/boot\(\)\.catch\(e => loadError\(state.generation,e\)\);\s*$/,''));
 run("state.me={user:{name:'Fixture User',email:'user@example.test',role:'facility_client'},capabilities:{facility_client:true},locations:[{id:1,name:'Test <Facility>',address:'<script>bad</script>'}]};state.locationId=1;state.generation=1");
 return {elements,run,ctx};
}
test('dashboard matches reference regions with real data and escaped facility metadata',async()=>{
 const {elements,run}=setup({'/home?location_id=1':{last_service:'2026-10-15T14:00:00Z',open_issues_count:0,recent_visits:[{id:1,status:'completed',completed_by_name:'<Cleaner>',completed_at:'2026-10-15'}]},'/issues?location_id=1':{issues:[]},'/locations/1/scope':{scope:[{title:'Lobby',items:[{label:'Floor'}]}]}});
 await run("renderHome(1,'1')"); const html=elements.view.innerHTML;
 for(const phrase of ['summary-grid','Recent Visits','Service Scope','Recent Quality & Issue Resolution','All clear','Lobby','&lt;Cleaner&gt;','&lt;script&gt;bad'])assert.ok(html.includes(phrase),phrase);
 assert.ok(!html.includes('<script>bad'));assert.ok(!html.includes('100%'));assert.ok(!html.includes('Taylor'));assert.ok(html.includes('Facility photo not provided'));
});
test('unknown data is not fabricated and optional scope failure preserves available visits',async()=>{
 const {elements,run}=setup({'/home?location_id=1':{},'/issues?location_id=1':{issues:[]},'/locations/1/scope':new Error('offline')});
 await run("renderHome(1,'1')"); const html=elements.view.innerHTML;
 assert.match(html,/Not available/);assert.match(html,/Service scope is temporarily unavailable/);assert.match(html,/No service reports recorded/);assert.ok(!html.includes('All clear'));assert.match(html,/Try loading scope/);
});
test('Account navigation is real, busy guard prevents navigation and client cannot author',()=>{
 const {elements,run}=setup();run('renderTabs();renderAccount(1)');
 assert.match(elements.tabs.innerHTML,/data-tab="account"/);assert.match(elements.view.innerHTML,/user@example.test/);assert.match(elements.view.innerHTML,/Test &lt;Facility&gt;/);
 assert.equal(elements.workspaceActions.innerHTML,'');run("state.busy=true;goto('issues');renderTabs()");assert.equal(run('state.tab'),'home');assert.match(elements.tabs.innerHTML,/disabled/);
});
test('late dashboard requests cannot replace a newer tab',async()=>{
 const {elements,run}=setup({'/home?location_id=1':{},'/issues?location_id=1':{issues:[]},'/locations/1/scope':{scope:[]}});
 run('state.generation=2');elements.view.innerHTML='Newer account view';await run("renderHome(1,'1')");assert.equal(elements.view.innerHTML,'Newer account view');
});

test('issue screen composes working form hooks beside history without fabricated performance claims',async()=>{
 const {elements,run}=setup({'/issues?location_id=1&offset=0&status=active':{issues:[{id:8,status:'open',created_at:'2026-10-15',description:'<img onerror=x>',evidence:[]}]},'/home?location_id=1':{open_issues_count:1,recent_quality:{issues_reported_90d:3,issues_resolved_90d:2}}});
 run("state.tab='issues'");await run("renderIssues(1,'1')");const html=elements.view.innerHTML;
 assert.match(html,/issue-composition/);assert.match(html,/Issue History & Resolution/);assert.match(html,/id="portalForm"/);assert.match(html,/id="issuePhoto"/);assert.match(html,/&lt;img onerror=x&gt;/);assert.match(html,/Issues Resolved/);assert.ok(!html.includes('100%'));assert.ok(!html.includes('2 hours'));
 assert.equal(typeof elements.portalForm.onsubmit,'function');assert.equal(typeof elements.cancelForm.onclick,'function');
});
test('CSRF request header and same-origin credentials remain intact',async()=>{
 const {ctx,run}=setup();let captured;ctx.window.PORTAL_CSRF='synthetic-token';ctx.fetch=async (url,options)=>{captured={url,options};return {status:200,ok:true,json:async()=>({ok:true})};};
 await run("api('/issues',{method:'POST',body:{description:'fixture'}})");assert.equal(captured.options.headers['X-CSRF-Token'],'synthetic-token');assert.equal(captured.options.credentials,'same-origin');assert.equal(captured.options.body,'{"description":"fixture"}');
});
