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
 assert.match(elements.workspaceActions.innerHTML,/Request a visit/);assert.doesNotMatch(elements.workspaceActions.innerHTML,/New Service Report/);run("state.busy=true;goto('issues');renderTabs()");assert.equal(run('state.tab'),'home');assert.match(elements.tabs.innerHTML,/disabled/);
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
test('public demo switch keeps contrasting colors while roles load and switch',async()=>{
 const script=fs.readFileSync(require('node:path').join(__dirname,'../standalone/anchor-portal/demo-ui.js'),'utf8');
 for(const role of ['facility_client','manager']){
  const button={style:{},dataset:{},textContent:'',addEventListener(){}};
  vm.runInNewContext(script,{document:{getElementById:id=>{assert.equal(id,'demoRole');return button;}},fetch:async()=>({ok:true,json:async()=>({user:{role}})})});
  assert.equal(button.style.color,'#102e49');assert.equal(button.style.background,'#ffffff');assert.equal(button.disabled,true);
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(button.textContent,role==='manager'?'Demo client':'Demo operator');assert.equal(button.disabled,false);
  assert.equal(button.style.color,'#102e49');assert.equal(button.style.background,'#ffffff');
 }
});

test('summary cards expose keyboard-native navigation to real visit IDs and issues',async()=>{
 const {elements,run}=setup({'/home?location_id=1':{last_service:'2026-10-15T14:00:00Z',last_service_visit_id:41,next_service:'2026-10-17T14:00:00Z',next_service_visit_id:42,next_service_status:'scheduled',open_issues_count:1},'/issues?location_id=1':{issues:[]},'/locations/1/scope':{scope:[]}});
 await run("renderHome(1,'1')");
 const html=elements.view.innerHTML;
 assert.equal((html.match(/class="panel summary-card summary-action"/g)||[]).length,4);
 assert.equal((html.match(/data-visit="41"/g)||[]).length,2);
 assert.match(html,/data-visit="42"/); assert.match(html,/aria-label="Open Issues: 1\. View open issues" data-go="issues"/);
 assert.ok(!html.includes('role="button"'));
});
test('unscheduled next service is honest and cannot claim a saved booking',async()=>{
 const {elements,run}=setup({'/home?location_id=1':{}});
 await run("renderNextService(1,'1')");
 assert.match(elements.view.innerHTML,/No upcoming service visit is scheduled/);
 assert.match(elements.view.innerHTML,/does not schedule or confirm a visit/);
 assert.match(elements.view.innerHTML,/data-go="history"/);assert.match(elements.view.innerHTML,/data-go="requests"/);
});
test('late next-service response cannot replace a newer view',async()=>{
 const {elements,run}=setup({'/home?location_id=1':{next_service_visit_id:42}});
 run('state.generation=2');elements.view.innerHTML='Current view';await run("renderNextService(1,'1')");assert.equal(elements.view.innerHTML,'Current view');
});
test('visit requests render escaped preferences, explicit non-booking notice and role-specific controls',async()=>{
 const {elements,run}=setup({'/service-requests?location_id=1':{requests:[{id:5,requester_id:1,status:'requested',preferred_date:'2026-10-20',preferred_window:'<Morning>',notes:'<script>bad</script>',created_at:'2026-10-16T12:00:00Z'}]}});
 run('state.me.user.id=1');await run("renderRequests(1,'1')");assert.match(elements.view.innerHTML,/does not book or confirm a visit or send notifications/);assert.match(elements.view.innerHTML,/Cancel request/);assert.doesNotMatch(elements.view.innerHTML,/Acknowledge request/);assert.match(elements.view.innerHTML,/&lt;script&gt;bad/);
 run('state.me.capabilities.operator=true');await run("renderRequests(1,'1')");assert.match(elements.view.innerHTML,/Acknowledge request/);assert.match(elements.view.innerHTML,/Decline request/);
});
