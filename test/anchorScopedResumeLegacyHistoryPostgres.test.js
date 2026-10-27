'use strict';
// Native synthetic regression only. No production data, provider calls, legacy authorization, history repair or retry.
const test=require('node:test'),assert=require('node:assert/strict'),path=require('node:path'),crypto=require('node:crypto');
const root=path.resolve(__dirname,'..');
const {startHarness}=require(root+'/test/helpers/anchorPreparationApprovalBrowserHarness');
const approval=require(root+'/services/anchorCrmPreparationApproval'),action=require(root+'/services/anchorCrmPreparationAction'),runtime=require(root+'/services/acquisitionMissionRuntime');
const {advanceExecutionAfterApproval}=require(root+'/packages/max/workspace/AmoOperatorApproval');
const {computePreparedArtifactBinding,computePreparedArtifactRevision,findValidExecutionApproval}=require(root+'/packages/acquisition-mission/ExecutionApproval');
const HISTORY='synthetic-legacy-cadence-approved-history';
async function authorize(h){const revision=process.env.RAILWAY_GIT_COMMIT_SHA,actor={id:'3',role:'admin',sessionId:h.f.sessionId},intent=h.intent();
 const preview=await approval.previewPreparationApproval(h.pool,{revision,intent,review:{action:'initialize_null_review_false',approvalReference:'synthetic-legacy-hash-causal-review',prospectIds:intent.admissions.map(a=>a.prospectId)}},actor);
 assert.equal(preview.installable,true);await approval.installPreparationApproval(h.pool,{revision,proposal:preview.proposal,manifestHash:preview.manifestHash,reviewInitialization:preview.reviewInitialization,reviewConfirmationHash:preview.reviewConfirmationHash},actor);
 return{actor,preview,input:{revision,manifestHash:preview.manifestHash,sourceMissionHash:preview.sourceMissionHash},receipt:{revision,receipt:{manifestId:preview.proposal.id,manifestHash:preview.manifestHash}}};}
async function rows(pool,id){return{mission:(await pool.query('SELECT * FROM acquisition_missions WHERE id=$1',[id])).rows[0],contributions:(await pool.query('SELECT * FROM acquisition_mission_contributions WHERE mission_id=$1 ORDER BY at,id',[id])).rows};}
async function seed(pool,history){await pool.query('INSERT INTO acquisition_missions SELECT * FROM jsonb_populate_record(NULL::acquisition_missions,$1)',[history.mission]);for(const c of history.contributions)await pool.query('INSERT INTO acquisition_mission_contributions SELECT * FROM jsonb_populate_record(NULL::acquisition_mission_contributions,$1)',[c]);}
async function childCounts(pool){const result={};for(const table of ['acquisition_missions','acquisition_mission_contributions','acquisition_mission_events','acquisition_mission_execution_audit'])result[table]=(await pool.query(`SELECT count(*)::int AS n FROM ${table} WHERE ${table==='acquisition_missions'?'id':'mission_id'} LIKE 'mission_crm_%'`)).rows[0].n;return result;}
function legacyRevision(missionId,contributions){const binding=computePreparedArtifactBinding(missionId,contributions);delete binding.outreachSequenceSteps;return crypto.createHash('sha256').update(JSON.stringify(binding)).digest('hex').slice(0,16);}

function executionApproval(history){return history.contributions.find(c=>c.specialist==='operator'&&c.payload.payload?.action==='execution_approved');}
function withLegacyHash(history){const saved=structuredClone(history);executionApproval(saved).payload.payload.preparedArtifactRevision=legacyRevision(saved.mission.id,saved.contributions.map(c=>c.payload));return saved;}
async function freshHydrate(h,options={}){const rt=runtime.createAcquisitionMissionRuntime({pool:h.pool,persist:true});await rt.hydrate('10',{pool:h.pool,persist:true,preinitializedSchema:true,...options});return rt;}

test('Anchor preparation isolates only proven unrelated legacy cadence approval history, without accepting it', {timeout:240000},async t=>{
 const h=await startHarness();t.after(()=>h.close());let history,currentRevision;const diagnostics=[];const original=console.error;
 t.mock.method(console,'error',(...args)=>{if(args[0]==='[anchor-crm-preparation]')diagnostics.push(JSON.parse(args[1]));else original(...args);});
 async function reset(saved){await h.reset();runtime.resetAcquisitionMissionRuntime();diagnostics.length=0;if(saved)await seed(h.pool,saved);}
 async function assertBlocked(saved,{errorCode='MISSION_STATE_INCONSISTENT'}={}){await reset(saved);const before=await rows(h.pool,HISTORY),auth=await authorize(h),protectedBefore=await h.protectedState();
   await assert.rejects(action.prepareDrafts(h.pool,auth.input,auth.actor),{code:'anchor_crm_action_rejected',status:'blocked_review_required',writesMayHaveOccurred:true,retryAllowed:false});
   assert.equal(diagnostics.length,1);assert.equal(diagnostics[0].phase,'canonical_hydration');assert.equal(diagnostics[0].claimOutcome,'committed');assert.equal(diagnostics[0].errorCode,errorCode);assert.ok(!JSON.stringify(diagnostics[0]).includes(HISTORY),'diagnostic does not expose historical identity');
   assert.ok(Object.values(await childCounts(h.pool)).every(n=>n===0));
   assert.equal((await h.pool.query('SELECT count(*)::int AS n FROM anchor_scoped_resume_consumption')).rows[0].n,1);
   const snapshot=await h.snapshot(),receipt=await approval.previewPreparationApproval(h.pool,auth.receipt,auth.actor);assert.deepEqual(await h.snapshot(),snapshot);assert.equal(receipt.preparationOutcome.status,'consumed_outcome_unknown');assert.equal(receipt.preparationOutcome.retryAllowed,false);
   assert.deepEqual(await rows(h.pool,HISTORY),before);assert.deepEqual(await h.protectedState(),protectedBefore);assert.equal(h.deny.attempts(),0);
 }
 await t.test('generate actual native persisted no-send approval fixture',async()=>{
  const auth=await authorize(h),prepared=await action.prepareDrafts(h.pool,auth.input,auth.actor);assert.equal(prepared.status,'ready_for_review');
  const generated=await rows(h.pool,prepared.missionId),renamed=JSON.parse(JSON.stringify(generated).replaceAll(prepared.missionId,HISTORY));
  await reset(renamed);const rt=runtime.getAcquisitionMissionRuntime({pool:h.pool,persist:true});await rt.hydrate('10',{pool:h.pool,persist:true,preinitializedSchema:true});const engine=rt.engine();
  const approved=await advanceExecutionAfterApproval({engine,mission:engine.get(HISTORY,'10'),tenantId:'10',operatorId:'synthetic-local-only',question:'Synthetic fixture approval only; never send',pool:h.pool,persist:true,preinitializedSchema:true});
  assert.equal(approved.alreadyExecuted,false);history=await rows(h.pool,HISTORY);assert.equal(history.mission.stage,'ready');assert.equal(history.mission.payload.pendingOperatorDecision,null);
  const contributions=history.contributions.map(c=>c.payload);currentRevision=computePreparedArtifactRevision(HISTORY,contributions);assert.notEqual(currentRevision,legacyRevision(HISTORY,contributions));assert.ok(findValidExecutionApproval(contributions,HISTORY));assert.equal(h.deny.attempts(),0);
 });
 for(const mode of ['current_hash_control','proven_legacy_hash'])await t.test(mode+' permits independent guarded READY without changing historical authority',async()=>{
  const saved=mode==='proven_legacy_hash'?withLegacyHash(history):structuredClone(history);await reset(saved);const before=await rows(h.pool,HISTORY),auth=await authorize(h),protectedBefore=await h.protectedState();
  const prepared=await action.prepareDrafts(h.pool,auth.input,auth.actor);assert.equal(prepared.status,'ready_for_review');assert.equal(prepared.sent,0);assert.equal(prepared.providerCalls,0);assert.equal((await childCounts(h.pool)).acquisition_missions,1);
  const snapshot=await h.snapshot(),receipt=await approval.previewPreparationApproval(h.pool,auth.receipt,auth.actor);assert.deepEqual(await h.snapshot(),snapshot);assert.equal(receipt.preparationOutcome.status,'ready_for_review');
  assert.deepEqual(await rows(h.pool,HISTORY),before);assert.deepEqual(await h.protectedState(),protectedBefore);assert.equal(diagnostics.length,0);assert.equal(h.deny.attempts(),0);
  const currentApproved=findValidExecutionApproval(before.contributions.map(c=>c.payload),HISTORY);assert.equal(Boolean(currentApproved),mode==='current_hash_control');
  if(mode==='proven_legacy_hash')assert.equal(runtime.current().engine().get(HISTORY,'10'),null,'legacy mission is not executable in the current singleton');
 });
 await t.test('default strict hydration still rejects proven legacy approval',async()=>{const saved=withLegacyHash(history);await reset(saved);const before=await h.snapshot();await assert.rejects(freshHydrate(h),{code:'MISSION_STATE_INCONSISTENT'});assert.deepEqual(await h.snapshot(),before);});
 const blockedCases=[
  ['duplicate active random-hash approval',s=>{const other=structuredClone(executionApproval(s));other.id+='-duplicate';other.payload.id=other.id;other.payload.payload.preparedArtifactRevision='0000000000000000';s.contributions.push(other);}],
  ['random revision',s=>{executionApproval(s).payload.payload.preparedArtifactRevision='0000000000000000';}],
  ['malformed revision',s=>{executionApproval(s).payload.payload.preparedArtifactRevision='legacy-not-a-hash';}],
  ['object revision',s=>{executionApproval(s).payload.payload.preparedArtifactRevision={private:'synthetic-canary'};}],
  ['invalidated approval',s=>{executionApproval(s).payload.payload.invalidated=true;}],
  ['superseded approval',s=>{executionApproval(s).payload.payload.superseded=true;}],
  ['nonconsumed approval',s=>{executionApproval(s).payload.payload.consumed=false;}],
  ['mismatched bound artifact ID',s=>{executionApproval(s).payload.payload.paigeContributionId='synthetic-wrong-artifact';}],
  ['mismatched queue count',s=>{executionApproval(s).payload.payload.queueCount=999;}],
  ['wrong approval stage',s=>{executionApproval(s).payload.payload.stage='execute';}],
  ['wrong approval mission',s=>{executionApproval(s).payload.payload.missionId='synthetic-wrong-mission';}],
  ['not approved',s=>{executionApproval(s).payload.payload.approved=false;}],
  ['wrong pending stage',s=>{s.mission.payload.pendingOperatorDecision={kind:'execution_approval',stage:'discover'};}],
 ];
 for(const[name,mutate]of blockedCases)await t.test(name+' is not omitted and remains blocked',async()=>{const saved=withLegacyHash(history);mutate(saved);await assertBlocked(saved);});

 await t.test('inactive prior approval is not current authority and does not block exact legacy omission',async()=>{
  const saved=withLegacyHash(history),prior=structuredClone(executionApproval(saved));prior.id+='-inactive';prior.payload.id=prior.id;prior.payload.payload.invalidated=true;prior.payload.payload.preparedArtifactRevision='0000000000000000';saved.contributions.push(prior);
  await reset(saved);const before=await rows(h.pool,HISTORY),auth=await authorize(h);const prepared=await action.prepareDrafts(h.pool,auth.input,auth.actor);assert.equal(prepared.status,'ready_for_review');assert.deepEqual(await rows(h.pool,HISTORY),before);assert.equal(findValidExecutionApproval(before.contributions.map(c=>c.payload),HISTORY),null);assert.equal(runtime.current().engine().get(HISTORY,'10'),null);assert.equal(h.deny.attempts(),0);
 });
 await t.test('malformed queue preserves native failure instead of becoming legacy omission',async()=>{
  const saved=withLegacyHash(history);require('../packages/acquisition-mission/ContributionSupersession').unwrapSpecialistPayload(saved.contributions.find(c=>c.specialist==='emmett').payload).queue.items=[null];await assertBlocked(saved,{errorCode:'unknown'});
 });
 await t.test('cached formerly-valid historical mission fails closed after durable legacy revision drift',async()=>{
  await reset(history);const rt=runtime.getAcquisitionMissionRuntime({pool:h.pool,persist:true});await rt.hydrate('10',{pool:h.pool,persist:true,preinitializedSchema:true});assert.ok(rt.engine().get(HISTORY,'10'));
  const old=legacyRevision(HISTORY,history.contributions.map(c=>c.payload));await h.pool.query("UPDATE acquisition_mission_contributions SET payload=jsonb_set(payload,'{payload,preparedArtifactRevision}',to_jsonb($2::text)) WHERE id=$1",[executionApproval(history).id,old]);
  const before=await rows(h.pool,HISTORY),auth=await authorize(h);await assert.rejects(action.prepareDrafts(h.pool,auth.input,auth.actor),{code:'anchor_crm_action_rejected',status:'blocked_review_required',retryAllowed:false});
  assert.ok(Object.values(await childCounts(h.pool)).every(n=>n===0));assert.deepEqual(await rows(h.pool,HISTORY),before);assert.equal((await h.pool.query('SELECT count(*)::int AS n FROM anchor_scoped_resume_consumption')).rows[0].n,1);assert.equal(h.deny.attempts(),0);
 });
 await t.test('source or required child is never omitted by the internal hydrate mode',async()=>{
  const legacy=withLegacyHash(history);await reset(legacy);const sourceContext={sourceMissionId:HISTORY,childMissionId:'mission_crm_'+'a'.repeat(32)};
  const before=await h.snapshot();await assert.rejects(freshHydrate(h,{anchorCrmPreparation:sourceContext}),{code:'MISSION_STATE_INCONSISTENT'});assert.deepEqual(await h.snapshot(),before);
  const childId='mission_crm_'+'b'.repeat(32),required=JSON.parse(JSON.stringify(legacy).replaceAll(HISTORY,childId));executionApproval(required).payload.payload.preparedArtifactRevision=legacyRevision(childId,required.contributions.map(c=>c.payload));
  await reset(required);const requiredBefore=await rows(h.pool,childId);await assert.rejects(freshHydrate(h,{anchorCrmPreparation:{sourceMissionId:h.f.program.source_mission_id,childMissionId:childId}}),{code:'MISSION_STATE_INCONSISTENT'});assert.deepEqual(await rows(h.pool,childId),requiredBefore);
 });
 await t.test('invalid internal mode does not enable omission',async()=>{
  await reset(withLegacyHash(history));for(const context of [true,{},null,{sourceMissionId:HISTORY,childMissionId:HISTORY},{sourceMissionId:'unrelated',childMissionId:'not-a-child'}]){
   await assert.rejects(freshHydrate(h,{anchorCrmPreparation:context}),{code:'ANCHOR_CRM_PREPARATION_HYDRATION_CONTEXT_INVALID'});
  }
 });
 await t.test('detector rejects near misses while preserving ordinary canonical consistency',async()=>{
  const {mayOmitLegacyPreparedHistory,validAnchorPreparationContext}=require('../services/anchorCrmPreparationLegacyHistory');
  const {assertMissionStateConsistent}=require('../packages/acquisition-mission/PendingOperatorDecision');
  const {unwrapSpecialistPayload}=require('../packages/acquisition-mission/ContributionSupersession');
  const legacy=withLegacyHash(history),context={sourceMissionId:'synthetic-unrelated-source',childMissionId:'mission_crm_'+'c'.repeat(32)};
  assert.equal(validAnchorPreparationContext(context,'10',true),true);assert.equal(validAnchorPreparationContext(context,'11',true),false);assert.equal(validAnchorPreparationContext(context,'10',false),false);
  const snapshot=s=>({mission:s.mission.payload,contributions:s.contributions.map(c=>c.payload)});
  let originalError;try{assertMissionStateConsistent(snapshot(legacy));}catch(error){originalError=error;}assert.equal(originalError?.code,'MISSION_STATE_INCONSISTENT');
  assert.equal(mayOmitLegacyPreparedHistory({...snapshot(legacy),error:originalError,context,cachedMission:null}),true);
  const variants=[
   ['valid pending decision',s=>{s.mission.payload.pendingOperatorDecision={kind:'execution_approval',stage:'ready'};}],
   ['cancelled',s=>{s.mission.payload.planCancelled=true;}],
   ['plan not locked',s=>{s.mission.payload.structuredMission.immutable=false;s.mission.payload.structuredMissionApproved=false;}],
   ['wrong stage',s=>{s.mission.stage='prepare';s.mission.payload.stage='prepare';}],
   ['missing Scout',s=>{s.contributions=s.contributions.filter(c=>c.specialist!=='scout');}],
   ['missing Max',s=>{s.contributions=s.contributions.filter(c=>!(c.specialist==='max'&&c.kind==='prioritization'));}],
   ['missing Paige',s=>{s.contributions=s.contributions.filter(c=>c.specialist!=='paige');}],
   ['missing Emmett',s=>{s.contributions=s.contributions.filter(c=>c.specialist!=='emmett');}],
   ['incoherent Scout',s=>{const b=unwrapSpecialistPayload(s.contributions.find(c=>c.specialist==='scout').payload);b.qualifiedCount=0;b.candidateUniverseCount=0;b.rankedProspectCount=0;}],
   ['rolled-back approval',s=>{executionApproval(s).payload.payload.rolledBack=true;}],
   ['outer superseded approval',s=>{executionApproval(s).payload.superseded=true;}],
   ['wrong tenant',s=>{s.mission.payload.tenantId='11';}],
  ];
  for(const[name,mutate]of variants){const s=structuredClone(legacy);mutate(s);
   // Recompute legacy hash after artifact removal to test semantic guards,
   // rather than merely a mismatch with the old artifact identifiers.
   executionApproval(s).payload.payload.preparedArtifactRevision=legacyRevision(HISTORY,s.contributions.map(c=>c.payload));
   assert.equal(mayOmitLegacyPreparedHistory({...snapshot(s),error:originalError,context,cachedMission:null}),false,name);
   if(name==='wrong tenant')continue;
   let expectedError;try{assertMissionStateConsistent(snapshot(s));}catch(e){expectedError=e;}
   await reset(s);const before=await h.snapshot();
   if(expectedError)await assert.rejects(freshHydrate(h,{anchorCrmPreparation:context}),{code:expectedError.code});
   else{const rt=await freshHydrate(h,{anchorCrmPreparation:context});assert.ok(rt.engine().get(HISTORY,'10'),name+' remains ordinary loaded history, not omitted');}
   assert.deepEqual(await h.snapshot(),before,name+' changes no durable history');
  }
  assert.equal(mayOmitLegacyPreparedHistory({...snapshot(legacy),error:originalError,context,cachedMission:legacy.mission.payload}),false);
  assert.equal(mayOmitLegacyPreparedHistory({...snapshot(legacy),error:Object.assign(Error('different invariant'),{code:'MISSION_STATE_INCONSISTENT'}),context,cachedMission:null}),false);
  assert.equal(mayOmitLegacyPreparedHistory({...snapshot(legacy),error:originalError,context:{...context,sourceMissionId:HISTORY},cachedMission:null}),false);
  assert.equal(h.deny.attempts(),0);
 });

});
