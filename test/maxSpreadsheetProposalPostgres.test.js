'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {randomUUID}=require('node:crypto');
const {Pool}=require('pg');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {PostgresSpreadsheetProposalStore}=require('../packages/max/stateIngestion/spreadsheetProposalStore');
const fixtureSchema=fs.readFileSync(path.join(__dirname,'fixtures/maxSpreadsheetBaseSchema.sql'),'utf8');
function operation(id,accountId,type,after,extra={}) {return {id,type,target:{accountId},before:null,after,outreachReviewRequired:type==='SET_ACCOUNT_FIELD' && ['email','phone'].includes(extra.field),evidence:[{sheet:'Sheet1',row:3,cell:'L3',rawValue:'Evidence'}],blocked:false,...extra};}
async function setup(t) {
 const instance=await startDisposablePostgres('spreadsheet-gate-');
 const db=new Pool({connectionString:instance.connectionString});
 t.after(async()=>{await db.end();await instance.stop();});
 await db.query(fixtureSchema);
 await db.query(fs.readFileSync(path.join(__dirname,'../migrations/2026-10-07-max-spreadsheet-reliability.sql'),'utf8'));
 await db.query("INSERT INTO clients VALUES(1),(2); INSERT INTO users(id,client_id,name,email,role) VALUES(10,1,'AO',null,'ao'),(11,1,'Jake',null,'admin'),(12,1,'Other AO',null,'ao'),(20,2,'Other tenant',null,'ao')");
 const accountId=randomUUID(), otherId=randomUUID(), tenantId=randomUUID();
 for(const [id,clientId,aoId] of [[accountId,1,10],[otherId,1,12],[tenantId,2,20]]) {
   const company=await db.query("INSERT INTO companies(client_id,name) VALUES($1,'Same name') RETURNING id",[clientId]);
   await db.query("INSERT INTO prospects(id,client_id,company_id,assigned_ao_id,first_name,phone,ao_last_touch_at) VALUES($1,$2,$3,$4,'Robert','603-111-2222','2026-09-23Z')",[id,clientId,company.rows[0].id,aoId]);
 }
 const store=new PostgresSpreadsheetProposalStore(db,{clientId:1,aoId:10,approverUserId:11});
 const scope={actorId:11,conversationId:'conversation',sourceHash:'a'.repeat(64)};
 const create=async operations=>store.createProposal({...scope,plan:{operations},baseline:await store.snapshotContext()});
 const commit=(proposal,extra={})=>store.commitProposal({...scope,proposalId:proposal.id,expectedDigest:proposal.digest,selectedOperationIds:proposal.plan.operations.map(o=>o.id),approvedBy:11,idempotencyKey:randomUUID(),...extra});
 return {db,store,accountId,otherId,tenantId,create,commit,scope};
}
test('PostgreSQL proposal atomic exact writes, history, tenant/AO isolation, replay and stale approval', {timeout:60000},async t=>{
 const {db,store,accountId,otherId,tenantId,create,commit,scope}=await setup(t);
 const snapshot=await store.snapshotContext();
 assert.deepEqual(snapshot.prospects.map(p=>p.id),[accountId]); assert.equal(snapshot.contacts[0].name,'Robert'); assert.equal(snapshot.users.length,1);
 const ops=[operation('email',accountId,'SET_ACCOUNT_FIELD','robert@example.test',{field:'email'}),operation('status',accountId,'SET_ACCOUNT_FIELD','application_in_progress',{field:'status'}),operation('note',accountId,'ADD_NOTE',{text:'Spoke to Robert. Added information.',category:'source'}),operation('call',accountId,'ADD_ACTIVITY',{kind:'call',occurredOn:'2026-09-23',details:'First call'}),operation('contact',accountId,'ADD_CONTACT',{name:'Mike'}),operation('task',accountId,'ADD_TASK',{kind:'email',description:'Send information',dueDate:null}),operation('suppression',accountId,'SUPPRESS_CALL',{channel:'call',reason:'Remove from call list'})];
 const proposal=await create(ops);
 assert.equal((await db.query('SELECT count(*) FROM ao_prospect_activity')).rows[0].count,'0');
 await assert.rejects(commit(proposal,{approvedBy:10}),/JAKE_APPROVAL_REQUIRED/);
 await assert.rejects(commit(proposal,{expectedDigest:'tampered'}),/PROPOSAL_DIGEST_MISMATCH/);
 await assert.rejects(commit(proposal,{actorId:10}),/PROPOSAL_NOT_FOUND/);
 await assert.rejects(commit(proposal,{conversationId:'other'}),/PROPOSAL_NOT_FOUND/);
 await assert.rejects(commit(proposal,{selectedOperationIds:['missing']}),/UNAPPROVABLE_OPERATION/);
 await assert.rejects(db.query("UPDATE max_spreadsheet_proposals SET plan='{}' WHERE id=$1",[proposal.id]),/immutable/);
 const [a,b]=await Promise.all([commit(proposal),commit(proposal)]);
 assert.equal(a.results.length,7); assert.equal(b.results.length,7); assert.ok(a.replayed || b.replayed);
 const account=(await db.query('SELECT * FROM prospects WHERE id=$1',[accountId])).rows[0];
 assert.equal(account.email,'robert@example.test'); assert.equal(account.ao_outreach_review_required,true); assert.equal(account.ao_current_status,'application_in_progress');assert.equal(account.ao_call_suppressed,true);assert.equal(account.ao_last_touch_at.toISOString(),'2026-09-23T00:00:00.000Z');
 assert.equal((await db.query('SELECT count(*) FROM ao_prospect_activity')).rows[0].count,'2');
 const repeat=await create(ops); const replay=await commit(repeat); assert.ok(replay.results.every(r=>r.status==='already_applied'));
 const stale=await create([operation('new',accountId,'ADD_NOTE',{text:'next'})]);
 await db.query("UPDATE prospects SET phone='changed externally' WHERE id=$1",[accountId]);
 await assert.rejects(commit(stale),/STALE_PROPOSAL/);
 for(const id of [otherId,tenantId]) {
  const wrong=await create([operation('wrong',id,'ADD_NOTE',{text:'wrong scope'})]); await assert.rejects(commit(wrong),/TARGET_OUTSIDE_APPROVED_SCOPE/);
 }
 const copy=await store.getProposal({proposalId:proposal.id,...scope}); copy.plan.operations[0].after='tampered';
 assert.equal((await store.getProposal({proposalId:proposal.id,...scope})).plan.operations[0].after,'robert@example.test');
});
test('PostgreSQL rollback after actual first write, subset approval, strict unsupported values and readback failure',{timeout:60000},async t=>{
 const {db,store,accountId,create,commit}=await setup(t);
 const valid=operation('first',accountId,'ADD_NOTE',{text:'must rollback'});
 const invalid=operation('second',accountId,'SET_ACCOUNT_FIELD','bad',{field:'unimplemented'});
 const proposal=await create([valid,invalid]);
 await assert.rejects(commit(proposal),/UNSUPPORTED_OR_CLEARING_FIELD/);
 assert.equal((await db.query('SELECT count(*) FROM ao_prospect_activity')).rows[0].count,'0');
 assert.equal((await db.query('SELECT count(*) FROM max_spreadsheet_effects')).rows[0].count,'0');
 assert.equal((await db.query('SELECT status FROM max_spreadsheet_proposals WHERE id=$1',[proposal.id])).rows[0].status,'pending');
 const receipt=await commit(proposal,{selectedOperationIds:['first']}); assert.equal(receipt.results.length,1);
 await assert.rejects(commit(proposal,{selectedOperationIds:['second']}),/APPROVAL_ALREADY_CONSUMED/);
 const blocked=await create([operation('blocked',accountId,'ADD_NOTE',{text:'blocked'},{blocked:true})]);await assert.rejects(commit(blocked),/UNAPPROVABLE_OPERATION/);
 const required=await create([operation('dep',accountId,'ADD_NOTE',{text:'dep'},{dependsOn:['other']})]); await assert.rejects(commit(required),/MISSING_OPERATION_DEPENDENCY/);
 const readback=await create([operation('readback',accountId,'SET_ACCOUNT_FIELD','changed@example.test',{field:'email'})]);
 await db.query(`CREATE FUNCTION discard_email() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.email:=OLD.email; RETURN NEW; END $$; CREATE TRIGGER discard_email BEFORE UPDATE ON prospects FOR EACH ROW EXECUTE FUNCTION discard_email();`);
 await assert.rejects(commit(readback),/COMMIT_VERIFICATION_FAILED/);
 assert.equal((await db.query('SELECT email FROM prospects WHERE id=$1',[accountId])).rows[0].email,null);assert.equal((await db.query('SELECT ao_outreach_review_required FROM prospects WHERE id=$1',[accountId])).rows[0].ao_outreach_review_required,false);
});
test('PostgreSQL confirmed new account dependencies, reviewed amendment, author approval and durable idempotency',{timeout:60000},async t=>{
 const {db,store,accountId,create,commit,scope}=await setup(t);
 const newId=randomUUID();
 const creation=operation('create',newId,'CREATE_ACCOUNT',{name:'Verified new account',identityConfirmed:true,outreachReviewRequired:true,identityEvidence:'Jake verified source address'});
 const email=operation('email',newId,'SET_ACCOUNT_FIELD','new@example.test',{field:'email',dependsOn:['create']});
 const note=operation('note',newId,'ADD_NOTE',{text:'First verified account note'},{dependsOn:['create']});
 const proposal=await create([note,email,creation]);
 const receipt=await commit(proposal,{selectedOperationIds:['note','email','create'],idempotencyKey:'unique-key'});
 assert.equal(receipt.results[0].operationId,'create');
 assert.equal((await db.query('SELECT email FROM prospects WHERE id=$1',[newId])).rows[0].email,'new@example.test');assert.equal((await db.query('SELECT ao_outreach_review_required FROM prospects WHERE id=$1',[newId])).rows[0].ao_outreach_review_required,true);
 const another=await create([operation('another',accountId,'ADD_NOTE',{text:'second proposal'})]);
 await assert.rejects(commit(another,{idempotencyKey:'unique-key'}),/IDEMPOTENCY_KEY_REUSED/);
 const amendment=await create([operation('amend',accountId,'SET_ACCOUNT_FIELD','603-333-4444',{field:'phone',before:'603-111-2222',amendmentReviewed:true})]);
 await commit(amendment);
 assert.equal((await db.query('SELECT phone FROM prospects WHERE id=$1',[accountId])).rows[0].phone,'603-333-4444');
 const aoPreview=await store.createProposal({...scope,actorId:10,plan:{operations:[operation('ao',accountId,'ADD_NOTE',{text:'AO proposes; Jake approves'})]},baseline:await store.snapshotContext()});
 await assert.rejects(store.getProposal({proposalId:aoPreview.id,actorId:11,conversationId:scope.conversationId}),/PROPOSAL_NOT_FOUND/);
 const read=await store.getProposal({proposalId:aoPreview.id,actorId:11,requestedBy:11,approverRead:true}); assert.equal(read.actorId,10);
 await commit(aoPreview,{actorId:10});
 assert.ok((await store.listProposals({actorId:11,approverRead:true})).some(p=>p.id===aoPreview.id));
 await assert.rejects(store.listProposals({actorId:10,approverRead:true}),/JAKE_APPROVAL_REQUIRED/);
 const revoke=await create([operation('revoke',accountId,'ADD_NOTE',{text:'must not persist'})]);
 await db.query('UPDATE users SET active=false WHERE id=11');
 await assert.rejects(commit(revoke),/APPROVAL_AUTHORIZATION_CHANGED/);
});
test('PostgreSQL canonical snapshot and invalidated persisted effects fail closed',{timeout:60000},async t=>{
 const {db,store,accountId,create,commit}=await setup(t);
 await db.query("INSERT INTO prospect_notes(client_id,prospect_id,text) VALUES(1,$1,'Existing canonical note')",[accountId]);
 await db.query("INSERT INTO prospect_lifecycle_events(client_id,prospect_id,reason) VALUES(1,$1,'Existing reason')",[accountId]);
 await db.query("INSERT INTO touchpoints(client_id,prospect_id,content_summary) VALUES(1,$1,'Historical event')",[accountId]);
 let snapshot=await store.snapshotContext();
 assert.ok(snapshot.activities.some(a=>a.details==='Existing canonical note'));assert.ok(snapshot.activities.some(a=>a.details==='Existing reason'));assert.ok(snapshot.activities.some(a=>a.details==='Historical event'));
 const op=operation('note',accountId,'ADD_NOTE',{text:'durable note'});
 const first=await create([op]); const saved=await commit(first);
 snapshot=await store.snapshotContext(); assert.equal(snapshot.effects[0].verifiedPresent,true);
 await db.query('DELETE FROM ao_prospect_activity WHERE id=$1',[saved.results[0].observed.id]);
 snapshot=await store.snapshotContext(); assert.equal(snapshot.effects[0].verifiedPresent,false);
 const changed=await create([op]); await assert.rejects(commit(changed),/PERSISTED_EFFECT_CHANGED/);
 const baseline=await create([operation('fresh',accountId,'ADD_NOTE',{text:'fresh note'})]);
 await db.query("INSERT INTO prospect_notes(client_id,prospect_id,text) VALUES(1,$1,'Manual change after approval')",[accountId]);
 await assert.rejects(commit(baseline),/STALE_PROPOSAL/);
});
test('PostgreSQL superseded plans, selected suppression and cyclic dependencies fail closed',{timeout:60000},async t=>{
 const {db,store,accountId,create,commit,scope}=await setup(t);
 const first=await create([operation('old',accountId,'ADD_NOTE',{text:'Old proposal'})]);
 const replacement=await store.createProposal({...scope,plan:{supersedesProposalId:first.id,operations:[operation('new',accountId,'ADD_NOTE',{text:'Resolved proposal'})]},baseline:await store.snapshotContext()});
 await assert.rejects(commit(first),/PROPOSAL_NOT_PENDING/);
 await commit(replacement);
 await assert.rejects(store.createProposal({...scope,plan:{supersedesProposalId:replacement.id,operations:[]},baseline:await store.snapshotContext()}),/PROPOSAL_CANNOT_BE_SUPERSEDED/);
 await assert.rejects(db.query("UPDATE max_spreadsheet_proposals SET receipt='{}' WHERE id=$1",[replacement.id]),/Finalized/);
 const suppressed=await create([operation('call-task',accountId,'ADD_TASK',{kind:'call',description:'Call tomorrow',dueDate:null}),operation('suppress',accountId,'SUPPRESS_CALL',{channel:'call',reason:'Remove from calls'})]);
 await assert.rejects(commit(suppressed),/CALL_SUPPRESSED/);
 const cyclic=await create([operation('one',accountId,'ADD_NOTE',{text:'One'},{dependsOn:['two']}),operation('two',accountId,'ADD_NOTE',{text:'Two'},{dependsOn:['one']})]);
 await assert.rejects(commit(cyclic),/CYCLIC_OPERATION_DEPENDENCY/);
});
test('PostgreSQL verifies actual task owner, target and deadline and preserves explicit due date',{timeout:60000},async t=>{
 const {db,accountId,create,commit}=await setup(t);
 const scheduled=await create([operation('dated',accountId,'ADD_TASK',{kind:'research',description:'Explicitly due research',dueDate:'2026-10-20'})]);
 const receipt=await commit(scheduled);assert.equal(receipt.results[0].observed.verified_deadline,'2026-10-20');
 const corrupt=await create([operation('corrupt',accountId,'ADD_TASK',{kind:'research',description:'Must rollback wrong owner',dueDate:null})]);
 await db.query(`CREATE FUNCTION misroute_task() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.assigned_ao_id:=12; RETURN NEW; END $$; CREATE TRIGGER misroute_task BEFORE INSERT ON ao_prospect_tasks FOR EACH ROW EXECUTE FUNCTION misroute_task();`);
 await assert.rejects(commit(corrupt),/COMMIT_VERIFICATION_FAILED/);
 assert.equal((await db.query('SELECT count(*) FROM ao_prospect_tasks')).rows[0].count,'1');
});
test('PostgreSQL linked field contacts, tasks and company address participate in comparison',{timeout:60000},async t=>{
 const {db,store,accountId,create,commit}=await setup(t);
 await db.query('ALTER TABLE companies ADD COLUMN location text,ADD COLUMN website text');
 await db.query("UPDATE companies SET location='Verified source address',website='https://example.test' WHERE id=(SELECT company_id FROM prospects WHERE id=$1)",[accountId]);
 const lead=(await db.query("INSERT INTO ao_leads(client_id,ao_owner_id,crm_prospect_id,original_visit_note) VALUES(1,10,$1,'Field visit history') RETURNING id",[accountId])).rows[0];
 await db.query("INSERT INTO ao_contacts(lead_id,contact_name) VALUES($1,'William Gagnon')",[lead.id]);
 await db.query("INSERT INTO ao_follow_up_tasks(lead_id,ao_owner_id,next_action) VALUES($1,10,'Existing field task')",[lead.id]);
 const snapshot=await store.snapshotContext();
 assert.equal(snapshot.prospects[0].address,'Verified source address');assert.equal(snapshot.prospects[0].website,'https://example.test');
 assert.ok(snapshot.contacts.some(c=>c.name==='William Gagnon' && c.prospect_id===accountId));assert.ok(snapshot.tasks.some(task=>task.description==='Existing field task'));assert.ok(snapshot.activities.some(a=>a.text==='Field visit history'));
 const amendment=await create([operation('address',accountId,'SET_ACCOUNT_FIELD','Reviewed new address',{field:'address',before:'Verified source address',amendmentReviewed:true})]);
 await commit(amendment);assert.equal((await store.snapshotContext()).prospects[0].address,'Reviewed new address');
 assert.equal((await db.query('SELECT location FROM companies WHERE id=(SELECT company_id FROM prospects WHERE id=$1)',[accountId])).rows[0].location,'Verified source address');
});
test('PostgreSQL admission hold must be explicitly reviewed and is verified atomically',{timeout:60000},async t=>{
 const {db,accountId,create,commit}=await setup(t);
 const unreviewed=await create([operation('missing-hold',accountId,'SET_ACCOUNT_FIELD','new@example.test',{field:'email',outreachReviewRequired:false})]);
 await assert.rejects(commit(unreviewed),/OUTREACH_ADMISSION_HOLD_NOT_REVIEWED/);
 assert.equal((await db.query('SELECT ao_outreach_review_required FROM prospects WHERE id=$1',[accountId])).rows[0].ao_outreach_review_required,false);
 const reviewed=await create([operation('hold',accountId,'SET_ACCOUNT_FIELD','new@example.test',{field:'email',outreachReviewRequired:true})]);
 await db.query(`CREATE FUNCTION discard_admission_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.ao_outreach_review_required:=false; RETURN NEW; END $$; CREATE TRIGGER discard_admission_hold BEFORE UPDATE ON prospects FOR EACH ROW EXECUTE FUNCTION discard_admission_hold();`);
 await assert.rejects(commit(reviewed),/COMMIT_VERIFICATION_FAILED/);
 const account=(await db.query('SELECT email,ao_outreach_review_required FROM prospects WHERE id=$1',[accountId])).rows[0];
 assert.equal(account.email,null);assert.equal(account.ao_outreach_review_required,false);
});
test('PostgreSQL replay checks canonical scalar integrity for every effect and permits completed tasks',{timeout:60000},async t=>{
 const {db,store,accountId,create,commit}=await setup(t);
 const other=randomUUID();
 await db.query('INSERT INTO prospects(id,client_id,company_id,assigned_ao_id) SELECT $1,client_id,company_id,assigned_ao_id FROM prospects WHERE id=$2',[other,accountId]);
 const cases=[
  {op:operation('contact-scalar',accountId,'ADD_CONTACT',{name:'Confirmed Contact'}),table:'max_spreadsheet_contacts',changes:[['prospect_id',other]]},
  {op:operation('task-scalar',accountId,'ADD_TASK',{kind:'research',description:'Canonical scalar task',dueDate:null}),table:'ao_prospect_tasks',changes:[['assigned_ao_id',12],['deadline','2026-11-01'],['prospect_id',other],['motion','EMAIL_LED'],['assignment_category','WARM_SIGNAL']]},
  {op:operation('activity-scalar',accountId,'ADD_ACTIVITY',{kind:'call',occurredOn:'2026-09-23',details:'Scalar activity'}),table:'ao_prospect_activity',changes:[['activity_type','visit'],['ao_id',12],['prospect_id',other]]},
  {op:operation('note-scalar',accountId,'ADD_NOTE',{text:'Scalar note'}),table:'ao_prospect_activity',changes:[['activity_type','call'],['ao_id',12]]},
  {op:operation('provider-scalar',accountId,'ADD_PROVIDER_RELATIONSHIP',{providerId:other,verified:true,sourceAssertion:'Verified provider'}),table:'max_spreadsheet_relationships',changes:[['provider_id',accountId]]},
 ];
 for(const item of cases) {
  const saved=await commit(await create([item.op]));const observed=saved.results[0].observed;
  for(const [column,value] of item.changes) {
   await db.query(`UPDATE ${item.table} SET ${column}=$1 WHERE id=$2`,[value,observed.id]);
   const snapshot=await store.snapshotContext();
   const effect=snapshot.effects.find(e=>e.observed.id===observed.id);assert.equal(effect.verifiedPresent,false,`${item.op.type}.${column} must invalidate replay`);
   const proposal=await create([item.op]);const before=(await db.query('SELECT count(*) FROM max_spreadsheet_effects')).rows[0].count;
   await assert.rejects(commit(proposal),/PERSISTED_EFFECT_CHANGED/);
   assert.equal((await db.query('SELECT count(*) FROM max_spreadsheet_effects')).rows[0].count,before);
   await db.query(`UPDATE ${item.table} SET ${column}=$1 WHERE id=$2`,[observed[column] ?? null,observed.id]);
  }
  if(item.op.type==='ADD_TASK') {
   await db.query("UPDATE ao_prospect_tasks SET status='completed' WHERE id=$1",[observed.id]);
   assert.equal((await store.snapshotContext()).effects.find(e=>e.observed.id===observed.id).verifiedPresent,true);
   assert.equal((await commit(await create([item.op]))).results[0].status,'already_applied');
  }
 }
 const contact=(await db.query('SELECT id FROM max_spreadsheet_contacts LIMIT 1')).rows[0];
 const suppression=operation('suppression-scalar',accountId,'SUPPRESS_CALL',{channel:'call',reason:'Account opt out'});
 const saved=await commit(await create([suppression]));
 await db.query('UPDATE max_spreadsheet_suppressions SET contact_id=$1 WHERE id=$2',[contact.id,saved.results[0].observed.id]);
 assert.equal((await store.snapshotContext()).effects.find(e=>e.observed.id===saved.results[0].observed.id).verifiedPresent,false);
 await assert.rejects(commit(await create([suppression])),/PERSISTED_EFFECT_CHANGED/);
});
test('PostgreSQL account creation replay verifies identity after approved dependent contact updates',{timeout:60000},async t=>{
 const {store,create,commit}=await setup(t);const id=randomUUID();
 const createOp=operation('create-replay',id,'CREATE_ACCOUNT',{name:'Replay creation',identityConfirmed:true,identityEvidence:'Verified new account',outreachReviewRequired:true});
 const email=operation('email-replay',id,'SET_ACCOUNT_FIELD','created@example.test',{field:'email',dependsOn:['create-replay']});
 const ops=[createOp,email];await commit(await create(ops));
 const snapshot=await store.snapshotContext();assert.ok(snapshot.effects.every(e=>e.verifiedPresent));
 const replay=await commit(await create(ops));assert.ok(replay.results.every(r=>r.status==='already_applied'));
});
