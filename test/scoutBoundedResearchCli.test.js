'use strict';
require('./helpers/denyExternalProviders.cjs');
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {spawn}=require('node:child_process');
const {startResearchHarness}=require('./helpers/scoutBoundedResearchFixture');
const {validateManifest}=require('../services/scoutBoundedAcquisition');
const root=path.resolve(__dirname,'..'),cli=path.join(root,'scripts/scout-bounded/run.cjs');
const KEY='synthetic-cli-google-secret-never-print',PRIVATE='synthetic-private-transport-failure';

// Injection is exclusively a generated test --require preloader. Production CLI
// has no fetch override, test environment branch, endpoint or alternate runner.
function preloaderSource(){return `
const fs=require('node:fs');
require(${JSON.stringify(path.join(__dirname,'helpers/denyExternalProviders.cjs'))});
const pg=require(${JSON.stringify(require.resolve('pg'))}),OriginalPool=pg.Pool;pg.Pool=class extends OriginalPool{constructor(options){super(options);this.cliOwned=options.max===6;}async end(){if(!this.cliOwned)throw Error('synthetic shared pool must not close');const result=await super.end();process.stderr.write('SYNTHETIC_OWNED_POOL_CLOSED\\n');return result;}};
const originalQuery=OriginalPool.prototype.query;OriginalPool.prototype.query=function(sql,...args){if(process.env.SYNTHETIC_CLI_MODE==='attribution'&&/INSERT INTO places_api_requests/.test(typeof sql==='string'?sql:sql.text)){process.stderr.write('SYNTHETIC_ATTRIBUTION_FAILURE\\n');return Promise.reject(Error(${JSON.stringify(PRIVATE)}+' '+process.env.GOOGLE_PLACES_KEY+' https://synthetic-private.invalid'));}return originalQuery.call(this,sql,...args);};
const modulePath=${JSON.stringify(path.join(root,'scripts/scout-bounded/transport.cjs'))};
const real=require(modulePath).installTransport;
require.cache[require.resolve(modulePath)].exports={installTransport(options){
 real(options);const savedConsole={...console};try{for(const name of ['log','info','warn','error','debug'])console[name]=()=>{};require(${JSON.stringify(path.join(__dirname,'helpers/denyAcquisitionHydration.cjs'))});}finally{for(const name of ['log','info','warn','error','debug'])console[name]=savedConsole[name];}process.stderr.write('SYNTHETIC_CAPTIVE_TRANSPORT_INSTALLED\\n');
 return async(input,options)=>{const u=new URL(input);if(options.redirect!=='error')throw Error('fixture_redirect');
 process.stderr.write('SYNTHETIC_PROVIDER_ATTEMPT\\n');
 if(process.env.SYNTHETIC_CLI_MODE==='hold')return new Promise(()=>{});
 if(process.env.SYNTHETIC_CLI_MODE==='failure')throw Error(${JSON.stringify(PRIVATE)}+' '+process.env.GOOGLE_PLACES_KEY);
 const data=u.pathname.includes('textsearch')?{status:'OK',results:[{place_id:process.env.SYNTHETIC_CLI_ID,name:'Synthetic Property Management'}],next_page_token:'synthetic-private-page-token-never-print'}:
 {status:'OK',result:{place_id:u.searchParams.get('place_id'),name:'Synthetic Property Management',website:'https://'+process.env.SYNTHETIC_CLI_ID+'.example',formatted_address:'Concord, NH',types:['real_estate_agency']}};
 return{ok:true,status:200,redirected:false,json:async()=>data};};}};
if(process.env.SYNTHETIC_CLI_MODE==='drop-output'){
 const write=process.stdout.write.bind(process.stdout);
 process.stdout.write=function(chunk,...args){if(String(chunk).startsWith('{')){process.stderr.write('SYNTHETIC_OUTPUT_LOST\\n');process.exit(73);}return write(chunk,...args);};
}
`;}
function launch(args,{env={},input,preloader,onStderr,closeInput=true}={}){
 const child=spawn(process.execPath,[...(preloader?['--require',preloader]:['--require',path.join(__dirname,'helpers/denyExternalProviders.cjs')]),cli,...args],
  {cwd:root,env:{...process.env,...env},stdio:['pipe','pipe','pipe']});
 let stdout='',stderr='';const result=new Promise((resolve,reject)=>{const timer=setTimeout(()=>{child.kill('SIGKILL');reject(Error('synthetic CLI timed out'));},30000);
 child.stdout.on('data',b=>stdout+=b.toString());child.stderr.on('data',b=>{stderr+=b.toString();onStderr?.(stderr,child);});child.on('error',reject);
 child.on('close',(code,signal)=>{clearTimeout(timer);resolve({code,signal,stdout,stderr});});});
 child.stdin.on('error',()=>{});if(input!==undefined)child.stdin.write(typeof input==='string'?input:JSON.stringify(input));if(closeInput)child.stdin.end();
 return{child,result};
}
function safe(result){for(const value of [KEY,PRIVATE,'synthetic-private-page-token-never-print','execution_token','next_page_token'])assert.ok(!(result.stdout+result.stderr).includes(value),'no private value in process output');}
function json(result){assert.equal(result.code,0,result.stderr);safe(result);const lines=result.stdout.trim().split('\n');assert.equal(lines.length,1,'one bounded machine-readable stdout line');const value=JSON.parse(lines[0]);return value;}
function stopped(result){assert.equal(result.code,1);assert.match(result.stderr,/Bounded Scout stopped/);safe(result);}

// Parser failures must occur before any DB/provider access, including stdin that
// is incomplete or oversized. Nothing can be installed merely by pasting input.
test('bounded CLI validates explicit commands, bounded stdin/file and legacy activation before network',async t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'scout-cli-input-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
 const env={DATABASE_URL:'postgresql://127.0.0.1:1/never-connect',DATABASE_SSL:'false',GOOGLE_PLACES_KEY:KEY,SCOUT_BOUNDED_APPROVAL_REF:'',ANCHOR_RESEARCH_PROVIDERS_PAUSED:'true'};
 for(const[args,input]of[[[],undefined],[['--preview','-','--confirm','a'.repeat(64)],'{}'],[['--install-approved','-'],'{"version":2}'],[['--execute-approved','-'],'{"version":1,"approvalRef":"not-activated"}'],[['--preview','-'],'{'],[['--preview','-'],'x'.repeat(128*1024+1)],[['--execute-approved','-','--confirm','a'.repeat(64)+'\n'],'{}']]){
  const r=await launch(args,{env,input}).result;stopped(r);assert.ok(!r.stderr.includes('Unexpected external transport'));
 }
 const large=path.join(dir,'large.json');fs.writeFileSync(large,'x'.repeat(128*1024+1));stopped(await launch(['--preview',large],{env}).result);
 const waiting=launch(['--install-approved','-','--confirm','a'.repeat(64)],{env,input:'{"version":2',closeInput:false});
 await new Promise(resolve=>setTimeout(resolve,100));waiting.child.kill('SIGTERM');const interrupted=await waiting.result;assert.equal(interrupted.signal,'SIGTERM');assert.equal(interrupted.stdout,'');
});

test('native standalone research lifecycle survives interruption, repetition and response loss without replay or authority widening',{timeout:180000},async t=>{
 const h=await startResearchHarness(),dir=fs.mkdtempSync(path.join(os.tmpdir(),'scout-cli-native-')),preloader=path.join(dir,'synthetic-preloader.cjs');
 fs.writeFileSync(preloader,preloaderSource());t.after(async()=>{await h.close();fs.rmSync(dir,{recursive:true,force:true});});
 const env={...h.env,GOOGLE_PLACES_KEY:KEY,SCOUT_BOUNDED_APPROVAL_REF:'',ANCHOR_EMAIL_SCOPED_RESUME_ID:'',ANCHOR_EMAIL_RECOVERY_MANIFEST_ID:''};
 const protectedBefore=await h.historySnapshot();
 function run(mode,manifest,{confirm=validateManifest(manifest).manifestHash,key=true,scenario='normal',file=false,...extra}={}){
  const args=[mode,file?path.join(dir,manifest.approvalRef+'.json'):'-'];if(file)fs.writeFileSync(args[1],JSON.stringify(manifest));
  if(['--install-approved','--execute-approved','--revoke-approved'].includes(mode))args.push('--confirm',confirm);
  return launch(args,{preloader,env:{...env,GOOGLE_PLACES_KEY:key?KEY:'',SYNTHETIC_CLI_MODE:scenario,SYNTHETIC_CLI_ID:manifest.approvalRef},input:file?undefined:manifest,...extra});
 }
 async function closeAuthority(manifest){const r=json(await run('--revoke-approved',manifest,{key:false}).result);assert.equal(r.status,'revoked');}
 async function durable(){return(await h.pool.query("SELECT (SELECT count(*)::int FROM scout_bounded_research_grants) AS grants,(SELECT count(*)::int FROM scout_bounded_acquisition_runs) AS runs,(SELECT count(*)::int FROM scout_bounded_acquisition_attempts) AS attempts")).rows[0];}
 await t.test('stdin preview then file install, explicit execute, fresh receipt and revoke under still-closed global flags',async()=>{
  const {manifest}=await h.newManifest(),before=await durable();
  const previewResult=await run('--preview',manifest,{key:false}).result,preview=json(previewResult);assert.equal(preview.previewOnly,true);assert.equal(preview.status,'not_installed');assert.deepEqual(preview.manifest,manifest);assert.deepEqual(await durable(),before);assert.ok(!previewResult.stderr.includes('SYNTHETIC_PROVIDER_ATTEMPT'));assert.equal((previewResult.stderr.match(/SYNTHETIC_OWNED_POOL_CLOSED/g)||[]).length,1);
  const installed=json(await run('--install-approved',manifest,{file:true,key:false}).result);assert.equal(installed.installed,true);
  const repeated=json(await run('--install-approved',manifest,{key:false}).result);assert.equal(repeated.idempotent,true);
  const execResult=await run('--execute-approved',manifest).result,executed=json(execResult);assert.equal(executed.status,'completed');assert.equal(executed.cleanVerifiedContacts,0);assert.equal(executed.queuedForEnrichment,1);assert.equal((execResult.stderr.match(/SYNTHETIC_PROVIDER_ATTEMPT/g)||[]).length,2);
  const receiptResult=await run('--receipt',manifest,{key:false,file:true}).result,receipt=json(receiptResult);assert.equal(receipt.outcome,'completed');assert.deepEqual(receipt.manifest,manifest);assert.equal(receipt.modeledReservedUsdMicros,65000);assert.ok(!receiptResult.stderr.includes('SYNTHETIC_PROVIDER_ATTEMPT'));
  const beforeRepeat=await durable();stopped(await run('--execute-approved',manifest).result);assert.deepEqual(await durable(),beforeRepeat);
  await closeAuthority(manifest);const revoked=json(await run('--receipt',manifest,{key:false}).result);assert.equal(revoked.status,'revoked');assert.equal(revoked.outcome,'completed');assert.equal(revoked.modeledReservedUsdMicros,65000);
 });
 await t.test('confirmation mismatch and missing provider key cause no install or consumption',async()=>{
  const {manifest}=await h.newManifest(),before=await durable();
  for(const mode of ['--install-approved','--execute-approved','--revoke-approved'])stopped(await run(mode,manifest,{confirm:'0'.repeat(64)}).result);
  stopped(await run('--execute-approved',manifest,{key:false}).result);assert.deepEqual(await durable(),before);
 });
 await t.test('concurrent CLI executions share the same single durable run',async()=>{
  const {manifest}=await h.newManifest();json(await run('--install-approved',manifest,{key:false}).result);
  const outputs=await Promise.all([run('--execute-approved',manifest).result,run('--execute-approved',manifest).result]);assert.equal(outputs.filter(r=>r.code===0).length,1);assert.equal(outputs.filter(r=>r.code===1).length,1);outputs.forEach(safe);
  assert.equal(outputs.reduce((n,r)=>n+(r.stderr.match(/SYNTHETIC_PROVIDER_ATTEMPT/g)||[]).length,0),2);const receipt=json(await run('--receipt',manifest,{key:false}).result);assert.equal(receipt.modeledReservedUsdMicros,65000);await closeAuthority(manifest);
 });
 await t.test('missing schema exposes only actionable fixed reason, not SQL or database details',async()=>{
  const {manifest}=await h.newManifest();await h.pool.query('ALTER TABLE scout_bounded_research_grants RENAME TO hidden_cli_research_grants');
  try{const result=await run('--receipt',manifest,{key:false}).result;stopped(result);assert.match(result.stderr,/Reason: required_schema_unavailable/);assert.ok(!result.stderr.includes('hidden_cli'));assert.ok(!result.stderr.includes('SELECT'));}
  finally{await h.pool.query('ALTER TABLE hidden_cli_research_grants RENAME TO scout_bounded_research_grants');}
 });
 await t.test('credential-valued business input fails safe output before any preview write',async()=>{
  const {manifest}=await h.newManifest();manifest.approvalRef=KEY;manifest.grant.approvalRef=KEY;const before=await durable();
  const result=await run('--preview',manifest).result;stopped(result);assert.match(result.stderr,/Reason: bounded_output_not_safe/);assert.deepEqual(await durable(),before);
 });
 await t.test('lost install output recovers by exact receipt, without implicit execute',async()=>{
  const {manifest}=await h.newManifest(),lost=await run('--install-approved',manifest,{key:false,scenario:'drop-output'}).result;assert.equal(lost.code,73);safe(lost);
  const receipt=json(await run('--receipt',manifest,{key:false}).result);assert.equal(receipt.status,'installed');assert.equal(receipt.outcome,'not_started');assert.equal(receipt.modeledReservedUsdMicros,0);await closeAuthority(manifest);
 });
 await t.test('lost completed execution output is resolved by receipt, never another provider attempt',async()=>{
  const {manifest}=await h.newManifest();json(await run('--install-approved',manifest,{key:false}).result);
  const lost=await run('--execute-approved',manifest,{scenario:'drop-output'}).result;assert.equal(lost.code,73);safe(lost);
  const receipt=json(await run('--receipt',manifest,{key:false}).result);assert.equal(receipt.outcome,'completed');assert.equal(receipt.modeledReservedUsdMicros,65000);
  const before=await durable(),again=await run('--execute-approved',manifest).result;stopped(again);assert.ok(!again.stderr.includes('SYNTHETIC_PROVIDER_ATTEMPT'));assert.deepEqual(await durable(),before);await closeAuthority(manifest);
 });
 await t.test('process interrupted after durable transport reservation remains consumed through receipt and revoke',async()=>{
  const {manifest}=await h.newManifest();json(await run('--install-approved',manifest,{key:false}).result);
  let killed=false;const pending=run('--execute-approved',manifest,{scenario:'hold',onStderr:(text,child)=>{if(!killed&&text.includes('SYNTHETIC_PROVIDER_ATTEMPT')){killed=true;child.kill('SIGKILL');}}});
  const lost=await pending.result;assert.equal(lost.signal,'SIGKILL');assert.equal(killed,true);safe(lost);
  const receipt=json(await run('--receipt',manifest,{key:false}).result);assert.equal(receipt.outcome,'consumed_outcome_not_confirmed');assert.equal(receipt.modeledReservedUsdMicros,40000);
  const before=await durable(),again=await run('--execute-approved',manifest).result;stopped(again);assert.ok(!again.stderr.includes('SYNTHETIC_PROVIDER_ATTEMPT'));assert.deepEqual(await durable(),before);
  await closeAuthority(manifest);const revoked=json(await run('--receipt',manifest,{key:false}).result);assert.equal(revoked.status,'revoked');assert.equal(revoked.modeledReservedUsdMicros,40000);assert.equal(revoked.outcome,'consumed_outcome_not_confirmed');
 });
 await t.test('provider failure emits only bounded error text and receipt preserves uncertain spend',async()=>{
  const {manifest}=await h.newManifest();json(await run('--install-approved',manifest,{key:false}).result);
  stopped(await run('--execute-approved',manifest,{scenario:'failure'}).result);
  const receipt=json(await run('--receipt',manifest,{key:false}).result);assert.equal(receipt.outcome,'consumed_outcome_not_confirmed');assert.equal(receipt.modeledReservedUsdMicros,40000);await closeAuthority(manifest);
 });
 await t.test('raw attribution logger failure is suppressed while receipt keeps durable uncertainty',async()=>{
  const {manifest}=await h.newManifest();json(await run('--install-approved',manifest,{key:false}).result);
  const result=await run('--execute-approved',manifest,{scenario:'attribution'}).result;stopped(result);assert.ok(result.stderr.includes('SYNTHETIC_ATTRIBUTION_FAILURE'));assert.ok(!result.stderr.includes('https://synthetic-private.invalid'));assert.ok(!result.stderr.includes('[placesApi]'));
  const receipt=json(await run('--receipt',manifest,{key:false}).result);assert.equal(receipt.outcome,'consumed_outcome_not_confirmed');assert.ok(receipt.modeledReservedUsdMicros>=40000);await closeAuthority(manifest);
 });
 await t.test('lost revoke output and repeated revoke retain tombstone without refund or replay',async()=>{
  const {manifest}=await h.newManifest();json(await run('--install-approved',manifest,{key:false}).result);
  const lost=await run('--revoke-approved',manifest,{key:false,scenario:'drop-output'}).result;assert.equal(lost.code,73);safe(lost);
  assert.equal(json(await run('--receipt',manifest,{key:false}).result).status,'revoked');assert.equal(json(await run('--revoke-approved',manifest,{key:false}).result).idempotent,true);
  stopped(await run('--install-approved',manifest,{key:false}).result);stopped(await run('--execute-approved',manifest).result);
 });
 await t.test('receipt and revoke remain DB-only after revision drift without Google credentials',async()=>{
  const {manifest}=await h.newManifest();json(await run('--install-approved',manifest,{key:false}).result);
  const changed={...env,GOOGLE_PLACES_KEY:'',RAILWAY_GIT_COMMIT_SHA:'b'.repeat(40),ANCHOR_RESEARCH_PROVIDERS_PAUSED:'true'};
  const args=['--receipt','-'];const receipt=json(await launch(args,{preloader,env:changed,input:manifest}).result);assert.equal(receipt.status,'installed');
  const revoked=json(await launch(['--revoke-approved','-','--confirm',validateManifest(manifest).manifestHash],{preloader,env:changed,input:manifest}).result);assert.equal(revoked.status,'revoked');
 });
 await t.test('original v1 invocation remains separately activated and uses no v2 confirmation or authority',async()=>{
  const f=await h.newManifest(),manifest=structuredClone(f.manifest);manifest.version=1;delete manifest.researchAuthority;const before=(await h.pool.query('SELECT count(*)::int AS n FROM scout_bounded_research_grants')).rows[0].n;
  await h.pool.query('ALTER TABLE anchor_email_recovery_control RENAME TO hidden_cli_legacy_control');
  try{const result=await launch(['--execute-approved','-'],{preloader,input:manifest,env:{...env,SCOUT_BOUNDED_APPROVAL_REF:manifest.approvalRef,ANCHOR_RESEARCH_PROVIDERS_PAUSED:'false',ANCHOR_EMAIL_RECOVERY_MAINTENANCE:'false',SYNTHETIC_CLI_MODE:'normal',SYNTHETIC_CLI_ID:manifest.approvalRef}}).result;
   assert.equal(json(result).status,'completed');assert.equal((result.stderr.match(/SYNTHETIC_PROVIDER_ATTEMPT/g)||[]).length,2);
  }finally{await h.pool.query('ALTER TABLE hidden_cli_legacy_control RENAME TO anchor_email_recovery_control');}
  assert.equal((await h.pool.query('SELECT count(*)::int AS n FROM scout_bounded_research_grants')).rows[0].n,before);
 });
 assert.deepEqual(await h.historySnapshot(),protectedBefore,'historical consumption, quarantine, preparation, sends and mission data untouched');
 assert.equal(process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED,'true');assert.equal(process.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE,'true');assert.equal(require('./helpers/denyExternalProviders.cjs').attempts(),0);
});
