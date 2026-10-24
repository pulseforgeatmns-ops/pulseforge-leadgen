'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { buildRecommendations, summarizeCrossEvidence } = require('../packages/max/clarity/recommendations');
const { readClarityBrief, prepareRecommendations } = require('../services/clarityRecommendations');
const report = { id: 'a', client_id: 10, window_start:'2026-10-11',window_end:'2026-10-17',synthetic:true,
 metrics:{ sessions:232,pages_per_session:1.22,scroll_depth:8.65,session_duration:51.5,dead_clicks:3.88,quick_backs:0.43,rage_clicks:0,js_errors:0 },
 deltas_pct:{sessions:180,pages_per_session:11.7,scroll_depth:-68,session_duration:-34.1},
 provenance:{gmail_message_id:'synthetic-976',parser_version:'test'} };
test('sample yields max three unique, evidenced hypotheses and never deployment authority',() => {
 const recommendations=buildRecommendations({report});
 assert.equal(recommendations.length,3);assert.equal(new Set(recommendations.map(r=>r.key)).size,3);
 for(const r of recommendations){ assert.equal(r.confidence,'moderate');assert.ok(r.evidence.length);assert.ok(r.validation_steps.length);assert.match(r.approval_scope,/No website changes/);assert.match(r.limitation,/not causation/); }
 assert.match(recommendations[0].summary,/Mobile friction is a hypothesis/);
 assert.equal(recommendations[2].evidence.find(e=>e.label==='Google Ads clicks').value,'Unavailable');
});
test('missing remains unknown, zero remains zero, no adjacent history fabricated',() => {
 const minimal={...report,metrics:{sessions:null,pages_per_session:null,js_errors:0},deltas_pct:{}};
 const r=buildRecommendations({report:minimal});assert.equal(r.length,1);assert.equal(r[0].confidence,'low');
 assert.equal(r[0].evidence.find(e=>e.label==='Pages per session').value,'Unavailable');
 const previous={...report,id:'prev',window_start:'2026-09-01',window_end:'2026-09-07',metrics:{scroll_depth:99}};
 assert.equal(buildRecommendations({report,history:[previous]})[0].evidence.find(e=>e.label==='Previous observed scroll depth').value,'Unavailable');
 const adjacent={...previous,window_start:'2026-10-04',window_end:'2026-10-10'};
 assert.equal(buildRecommendations({report,history:[adjacent]})[0].evidence.find(e=>e.label==='Previous observed scroll depth').value,'99%');
});
test('cross-source evidence requires matching tenant/window and preserves missing conversion data',() => {
 const window={start:report.window_start,end:report.window_end};
 const googleAds={client_id:10,live:{available:true,window,campaigns:[{clicks:20,spend:32,conversions:null}]}};
 const firstParty={client_id:10,availability:'AVAILABLE',observationWindow:window,observedCount:0};
 let e=summarizeCrossEvidence({clientId:10,report,googleAds,firstParty});
 assert.equal(e.google_ads.clicks,20);assert.equal(e.google_ads.platform_conversions,null);assert.equal(e.first_party.attributed_walkthrough_leads,0);
 e=summarizeCrossEvidence({clientId:10,report,googleAds:{...googleAds,client_id:11},firstParty:{...firstParty,observationWindow:{...window,end:'2026-10-18'}}});
 assert.equal(e.google_ads.available,false);assert.equal(e.first_party.available,false);assert.equal(e.first_party.attributed_walkthrough_leads,null);
});
test('default-off brief performs no database calls; available brief is bounded and stale labelled',async()=>{
 const store={getIntegration(){assert.fail('dark must not query');}};
 assert.deepEqual(await readClarityBrief({clientId:10,store,env:{}}),{available:false,reason:'disabled'});
 const result=await readClarityBrief({clientId:10,env:{CLARITY_ENABLED:'true'},now:new Date('2026-11-20'),store:{
 getIntegration:async()=>({enabled:true,consent_status:'authorized'}),listSnapshots:async()=>[report],listRecommendations:async()=>[]}});
 assert.equal(result.available,true);assert.equal(result.stale,true);
});
test('recommendation replay avoids re-reading paid providers or changing reviewed content',async()=>{
 const existing=[{id:'existing',status:'approved'}];
 assert.equal(await prepareRecommendations({clientId:10,snapshot:report,store:{listRecommendations:async()=>existing},loadAds(){assert.fail();}}),existing);
 await assert.rejects(prepareRecommendations({clientId:11,snapshot:report}),/tenant_mismatch/);
});
test('hanging cross-evidence is cancelled and becomes unavailable without late persistence',async()=>{
 const {collectCrossEvidence}=require('../services/clarityRecommendations');let aborted=false;
 const started=Date.now();
 const result=await collectCrossEvidence({clientId:10,snapshot:report,remainingMs:20,pool:{query(){assert.fail();}},
 loadAds:({signal})=>new Promise((resolve,reject)=>signal.addEventListener('abort',()=>{aborted=true;reject(new Error('cancelled'));})),
 loadLeads:async()=>({availability:'UNAVAILABLE'})});
 assert.ok(Date.now()-started<500);assert.equal(aborted,true);assert.equal(result.google_ads.available,false);
});
test('cross-evidence transport bounds requests/redirects and refuses database mutation',async()=>{
 const {boundedEvidenceTransports,loadClarityGoogleAds}=require('../services/clarityRecommendations');
 const c=new AbortController();let calls=0;
 const transports=boundedEvidenceTransports({signal:c.signal,deadline:Date.now()+1000,pool:{query:async q=>{assert.ok(q.query_timeout<=1000);return {rows:[]};}},
 httpImpl:{post:async(url,data,options)=>{calls++;assert.equal(options.signal,c.signal);assert.equal(options.maxRedirects,0);assert.ok(options.timeout<=1000);return {data:{}};}}});
 await assert.rejects(transports.pool.query('DELETE FROM users'),/readonly/);
 await assert.rejects(transports.http.post('https://evil.test',{}),/destination/);
 for(let i=0;i<15;i++)await transports.http.post('https://googleads.googleapis.com/test',{});
 await assert.rejects(transports.http.post('https://googleads.googleapis.com/test',{}),/budget/);
 assert.equal(calls,15);
 assert.equal((await loadClarityGoogleAds({tenantId:10,pool:transports.pool,http:transports.http,window:{}})).live.available,false);
 c.abort();await assert.rejects(transports.pool.query('SELECT 1'),/readonly/);
});
