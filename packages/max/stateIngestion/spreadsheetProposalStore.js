'use strict';
const crypto = require('crypto');
const { PostgresStateStore } = require('./store/postgresStore');
const TABLES = ['max_spreadsheet_contacts','max_spreadsheet_suppressions','max_spreadsheet_relationships'];
const TYPES = new Set(['SET_ACCOUNT_FIELD','ADD_NOTE','ADD_ACTIVITY','ADD_TASK','SUPPRESS_CALL','ADD_CONTACT','CREATE_ACCOUNT','ADD_PROVIDER_RELATIONSHIP']);
function canonical(value) {
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(k => [k, canonical(value[k])]));
  return value;
}
function digest(value) { return crypto.createHash('sha256').update(JSON.stringify(canonical(value === undefined ? null : value))).digest('hex'); }
function snapshotDigest(snapshot) { const { users, ...business } = snapshot; return digest(business); }
function fail(code) { const error = new Error(code); error.code = code; throw error; }
function businessValue(value) {
  if(typeof value==='string') return value.normalize('NFKC').replace(/\s+/g,' ').trim();
  if(Array.isArray(value)) return value.map(businessValue);
  if(value && typeof value==='object') return Object.fromEntries(Object.entries(value).map(([k,v])=>[k,businessValue(v)]));
  return value;
}
function semanticKey(op) { return digest(businessValue({ type:op.type, target:op.target, field:op.field || null, after:op.after })); }
const EFFECT_COLLECTIONS={ADD_CONTACT:'contacts',ADD_TASK:'tasks',SUPPRESS_CALL:'suppressions',ADD_PROVIDER_RELATIONSHIP:'relationships',ADD_NOTE:'activities',ADD_ACTIVITY:'activities'};
const EFFECT_COLUMNS={
  ADD_CONTACT:['client_id','prospect_id'],
  ADD_TASK:['client_id','prospect_id','assigned_ao_id','assignment_category','motion','priority','first_action','required_debrief','required_log_fields'],
  SUPPRESS_CALL:['client_id','prospect_id','channel','contact_id'],
  ADD_PROVIDER_RELATIONSHIP:['client_id','prospect_id','provider_id'],
  ADD_NOTE:['tenant_id','prospect_id','ao_id','activity_type','notes','outcome','previous_status','new_status','previous_next_action','new_next_action','next_action_date'],
};
EFFECT_COLUMNS.ADD_ACTIVITY=EFFECT_COLUMNS.ADD_NOTE;
function sameColumns(current,observed,columns) { return columns.every(key=>digest(current[key])===digest(observed[key])); }
function deadline(row) {
  if(row.verified_deadline!==undefined) return row.verified_deadline;
  if(row.deadline instanceof Date) return `${row.deadline.getFullYear()}-${String(row.deadline.getMonth()+1).padStart(2,'0')}-${String(row.deadline.getDate()).padStart(2,'0')}`;
  return row.deadline ? String(row.deadline).slice(0,10) : null;
}
function effectStillPresent(effect,snapshot) {
  const op=effect.operation, observed=effect.observed;
  if(!op || !observed) return false;
  const account=snapshot.prospects.find(p=>String(p.id)===String(op.target?.accountId));
  if(!account) return false;
  const fields={email:'email',phone:'phone',status:'ao_current_status',ao_current_status:'ao_current_status',website:'website',address:'ao_source_address'};
  if(op.type==='SET_ACCOUNT_FIELD') return digest(account[fields[op.field]])===digest(op.after) && (!op.outreachReviewRequired || account.ao_outreach_review_required===true);
  // Creation proves durable entity identity. Later approved operations can fill
  // contacts or change status without undoing the creation business effect.
  if(op.type==='CREATE_ACCOUNT') return sameColumns(account,observed,['id','client_id','company_id','assigned_ao_id','source']);
  const collection=EFFECT_COLLECTIONS[op.type];
  const current=(snapshot[collection] || []).find(r=>String(r.id)===String(observed.id));
  if(!current || String(current.prospect_id)!==String(op.target.accountId) || !EFFECT_COLUMNS[op.type]) return false;
  if(op.type==='SUPPRESS_CALL' && !account.ao_call_suppressed) return false;
  if(op.type==='ADD_TASK' && (!['open','in_progress','completed'].includes(current.status) || deadline(current)!==deadline(observed))) return false;
  const field=['ADD_NOTE','ADD_ACTIVITY'].includes(op.type)?'metadata':op.type==='ADD_TASK'?'routing_snapshot':'data';
  return digest(current[field])===digest(observed[field]) && sameColumns(current,observed,EFFECT_COLUMNS[op.type]);
}
function publicProposal(row) { return row && { id:row.id, digest:row.digest, sourceHash:row.source_hash, actorId:row.actor_id, aoId:row.ao_id, conversationId:row.conversation_id, plan:row.plan, status:row.status, receipt:row.receipt, createdAt:row.created_at }; }
function timeoutError() { const error=new Error('SPREADSHEET_DB_TIMEOUT');error.code='SPREADSHEET_DB_TIMEOUT';return error; }
async function deadlinePromise(promise,milliseconds) {
  let timer;
  try {return await Promise.race([promise,new Promise((resolve,reject)=>{timer=setTimeout(()=>reject(timeoutError()),milliseconds);})]);}
  finally {clearTimeout(timer);}
}
async function boundedQuery(db,text,values,timeoutMs) {
  return deadlinePromise(db.query({text,values,query_timeout:timeoutMs}),timeoutMs);
}
async function boundedConnection(pool,{queryTimeoutMs,rollbackTimeoutMs,connectTimeoutMs}) {
  let expired=false;
  const pending=pool.connect();
  // A connection delivered after the application deadline must never leak.
  pending.then(connection=>{if(expired) connection.release(true);},()=>{});
  let raw;
  try {raw=await deadlinePromise(pending,connectTimeoutMs);} catch(error) {expired=true;throw error;}
  let uncertain=false;
  return {
    async query(text,values) {
      try {return await boundedQuery(raw,text,values,text==='ROLLBACK'?rollbackTimeoutMs:queryTimeoutMs);}
      catch(error) {if(error.code==='SPREADSHEET_DB_TIMEOUT' || /timeout|connection terminated/i.test(error.message)) uncertain=true;throw error;}
    },
    release(discard) {raw.release(Boolean(discard || uncertain));},
  };
}
class PostgresSpreadsheetProposalStore {
  constructor(db,{clientId,aoId,approverUserId,queryTimeoutMs=16000,rollbackTimeoutMs=2000,connectTimeoutMs=5000}={}) {
    if (!clientId || !aoId) fail('SPREADSHEET_SCOPE_REQUIRED');
    this.timeouts={queryTimeoutMs,rollbackTimeoutMs,connectTimeoutMs};
    this.readDb={query:(text,values)=>boundedQuery(db,text,values,queryTimeoutMs)};
    this.db=db; this.clientId=clientId; this.aoId=aoId; this.approverUserId=approverUserId;
  }
  async snapshotContext({ aoId=this.aoId, db=this.readDb }={}) {
    if(String(aoId)!==String(this.aoId)) fail('SPREADSHEET_SCOPE_MISMATCH');
    const snapshot=await new PostgresStateStore(db,{clientId:this.clientId}).snapshotContext({aoId});
    for(const table of TABLES) {
      const result=await db.query(`SELECT r.*${table==='ao_prospect_tasks' ? ',r.deadline::text AS verified_deadline' : ''} FROM ${table} r JOIN prospects p ON p.id=r.prospect_id AND p.client_id=r.client_id WHERE r.client_id=$1 AND p.assigned_ao_id=$2 ORDER BY r.id`,[this.clientId,aoId]);
      const key=table.replace('max_spreadsheet_','');
      snapshot[key]=(snapshot[key] || []).concat(result.rows.map(r=>({...r.data,...r})));
    }
    const fieldLeads=await db.query('SELECT l.* FROM ao_leads l JOIN prospects p ON p.id=l.crm_prospect_id AND p.client_id=l.client_id WHERE l.client_id=$1 AND l.ao_owner_id=$2 AND p.assigned_ao_id=$2 ORDER BY l.id',[this.clientId,aoId]);
    snapshot.fieldLeads=fieldLeads.rows;
    snapshot.activities.push(...fieldLeads.rows.filter(l=>l.original_visit_note).map(l=>({id:`field:${l.id}`,prospect_id:l.crm_prospect_id,text:l.original_visit_note,kind:'field_visit_note'})));
    for(const table of ['ao_contacts','ao_follow_up_tasks']) {
      const result=await db.query(`SELECT r.*,l.crm_prospect_id AS prospect_id,l.client_id FROM ${table} r JOIN ao_leads l ON l.id=r.lead_id JOIN prospects p ON p.id=l.crm_prospect_id AND p.client_id=l.client_id WHERE l.client_id=$1 AND l.ao_owner_id=$2 AND p.assigned_ao_id=$2 ORDER BY r.id`,[this.clientId,aoId]);
      if(table==='ao_contacts') snapshot.contacts.push(...result.rows.map(r=>({...r,name:r.contact_name,title:r.contact_title})));
      else snapshot.tasks=(snapshot.tasks || []).concat(result.rows.map(r=>({...r,description:r.next_action,dueDate:r.due_date})));
    }
    for(const table of ['touchpoints','ao_prospect_tasks','max_ao_follow_up_tasks','prospect_notes','prospect_lifecycle_events']) {
      const result=await db.query(`SELECT r.*${table==='ao_prospect_tasks' ? ',r.deadline::text AS verified_deadline' : ''} FROM ${table} r JOIN prospects p ON p.id::text=r.prospect_id::text AND p.client_id=r.client_id WHERE r.client_id=$1 AND p.assigned_ao_id=$2 ORDER BY r.id`,[this.clientId,aoId]);
      if(table==='touchpoints') snapshot.activities.push(...result.rows.map(r=>({...r,kind:r.channel,details:r.content_summary,occurredOn:r.created_at ? new Date(r.created_at).toISOString().slice(0,10) : null})));
      else if(table==='prospect_notes' || table==='prospect_lifecycle_events') snapshot.activities.push(...result.rows.map(r=>({...r,details:r.text || r.reason,kind:table==='prospect_notes'?'note':'lifecycle'})));
      else snapshot.tasks=(snapshot.tasks || []).concat(result.rows.map(r=>({...r,description:r.first_action || r.prompt,dueDate:r.deadline || null})));
    }
    snapshot.activities.push(...snapshot.prospects.filter(p=>p.notes).map(p=>({id:`legacy:${p.id}`,prospect_id:p.id,text:p.notes,kind:'legacy_note'})));
    const suppressions=await db.query(`SELECT r.* FROM tenant_outreach_suppressions r WHERE r.tenant_id=$1::text AND EXISTS(SELECT 1 FROM prospects p WHERE p.client_id=$1::integer AND p.assigned_ao_id=$2 AND (lower(p.email)=lower(r.email) OR p.id::text=r.contact_ref)) ORDER BY r.id`,[this.clientId,aoId]);
    snapshot.suppressions.push(...suppressions.rows);
    const effects=await db.query(`SELECT e.semantic_key,e.operation,e.observed FROM max_spreadsheet_effects e JOIN max_spreadsheet_proposals r ON r.id=e.proposal_id WHERE e.client_id=$1 AND r.ao_id=$2 ORDER BY e.semantic_key`,[this.clientId,aoId]);
    snapshot.effects=effects.rows.map(effect=>({...effect,verifiedPresent:effectStillPresent(effect,snapshot)}));
    snapshot.activities=snapshot.activities.map(a=>({...a.metadata,...a,details:a.metadata?.details || a.details || a.notes,occurredOn:a.metadata?.occurredOn || a.occurredOn || null}));
    return snapshot;
  }
  async createProposal({actorId,conversationId,sourceHash,plan,baseline}) {
    if(!actorId || !conversationId || !/^[a-f0-9]{64}$/.test(sourceHash || '')) fail('INVALID_PROPOSAL_SCOPE');
    const frozen=JSON.parse(JSON.stringify(plan));
    if(!Array.isArray(frozen.operations) || new Set(frozen.operations.map(o=>o.id)).size!==frozen.operations.length) fail('INVALID_OPERATIONS');
    for(const op of frozen.operations) {
      if(!op.id || !TYPES.has(op.type) || !Array.isArray(op.evidence) || !op.evidence.length) fail('INVALID_OPERATION');
      op.semanticKey=semanticKey(op);
    }
    const baselineHash=snapshotDigest(baseline || await this.snapshotContext());
    frozen.baselineHash=baselineHash;
    const id=crypto.randomUUID();
    const proposalDigest=digest({clientId:this.clientId,aoId:this.aoId,actorId,conversationId,sourceHash,plan:frozen,baselineHash});
    const db=await boundedConnection(this.db,this.timeouts);
    let discardConnection=false, commitAttempted=false;
    try {
      await db.query('BEGIN');
      await db.query("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'; SET LOCAL idle_in_transaction_session_timeout='20s'");
      if(frozen.supersedesProposalId) {
        const previous=await db.query("SELECT id FROM max_spreadsheet_proposals WHERE id=$1 AND client_id=$2 AND ao_id=$3 AND actor_id=$4 AND source_hash=$5 AND conversation_id=$6 AND status='pending' FOR UPDATE",[frozen.supersedesProposalId,this.clientId,this.aoId,actorId,sourceHash,conversationId]);
        if(!previous.rows[0]) fail('PROPOSAL_CANNOT_BE_SUPERSEDED');
        await db.query("UPDATE max_spreadsheet_proposals SET status='superseded' WHERE id=$1",[frozen.supersedesProposalId]);
      }
      const {rows}=await db.query(`INSERT INTO max_spreadsheet_proposals(id,client_id,actor_id,ao_id,conversation_id,source_hash,digest,baseline_hash,plan) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb) RETURNING *`,[id,this.clientId,actorId,this.aoId,conversationId,sourceHash,proposalDigest,baselineHash,JSON.stringify(frozen)]);
      commitAttempted=true;await db.query('COMMIT');commitAttempted=false;return publicProposal(rows[0]);
    } catch(error) {
      discardConnection=commitAttempted;
      if(commitAttempted) error.commitOutcomeUnknown=true;
      try {await db.query('ROLLBACK');} catch(rollbackError) {discardConnection=true;error.rollbackFailed=true;}
      throw error;
    } finally {db.release(discardConnection);}
  }
  async authorizedApproverRead(requestedBy) {
    if(!this.approverUserId || String(requestedBy)!==String(this.approverUserId)) fail('JAKE_APPROVAL_REQUIRED');
    const {rows}=await this.readDb.query("SELECT id FROM users WHERE id=$1 AND role='admin' AND active=true",[requestedBy]);
    if(!rows.length) fail('JAKE_APPROVAL_REQUIRED');
  }
  async listProposals({actorId,approverRead=false,requestedBy=actorId}={}) {
    if(approverRead) await this.authorizedApproverRead(requestedBy);
    const {rows}=await this.readDb.query(`SELECT * FROM max_spreadsheet_proposals WHERE client_id=$1 AND ao_id=$2 AND ($3::boolean OR actor_id=$4) ORDER BY created_at DESC LIMIT 100`,[this.clientId,this.aoId,approverRead,actorId]);
    return rows.map(publicProposal);
  }
  async getProposal({proposalId,actorId,conversationId,requestedBy=actorId,approverRead=false}) {
    if(approverRead) await this.authorizedApproverRead(requestedBy);
    const {rows}=await this.readDb.query(`SELECT * FROM max_spreadsheet_proposals WHERE id=$1 AND client_id=$2 AND ao_id=$3 AND ($4::boolean OR (actor_id=$5 AND conversation_id=$6))`,[proposalId,this.clientId,this.aoId,approverRead,actorId,conversationId]);
    if(!rows[0]) fail('PROPOSAL_NOT_FOUND');
    return publicProposal(rows[0]);
  }
  async commitProposal({proposalId,actorId,conversationId,sourceHash,selectedOperationIds,approvedBy,expectedDigest,idempotencyKey}) {
    if(!this.approverUserId || String(approvedBy)!==String(this.approverUserId)) fail('JAKE_APPROVAL_REQUIRED');
    if(!expectedDigest || !idempotencyKey || !Array.isArray(selectedOperationIds) || !selectedOperationIds.length || new Set(selectedOperationIds).size!==selectedOperationIds.length) fail('EXACT_OPERATION_SELECTION_REQUIRED');
    const connection=await boundedConnection(this.db,this.timeouts);
    let discardConnection=false, commitAttempted=false;
    try {
      await connection.query('BEGIN');
      await connection.query("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'; SET LOCAL idle_in_transaction_session_timeout='20s'");
      const lockPlan=await connection.query('SELECT plan FROM max_spreadsheet_proposals WHERE id=$1 AND client_id=$2 AND ao_id=$3',[proposalId,this.clientId,this.aoId]);
      const callTargets=[...new Set((lockPlan.rows[0]?.plan.operations || []).filter(o=>(o.type==='SUPPRESS_CALL' || (o.type==='SET_ACCOUNT_FIELD' && o.outreachReviewRequired===true)) && selectedOperationIds.includes(o.id)).map(o=>o.target?.accountId))].filter(Boolean).sort();
      for(const id of callTargets) await connection.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))',[`max:call:${this.clientId}:${id}`]);
      // Writers outside this importer also acquire row-exclusive table locks. This protects
      // the snapshot check against all concurrent CRM writes, not only cooperative imports.
      await connection.query(`LOCK TABLE users, prospects, companies, ao_leads, ao_contacts, ao_follow_up_tasks, ao_prospect_activity, touchpoints, ao_prospect_tasks, max_ao_follow_up_tasks, tenant_outreach_suppressions, prospect_notes, prospect_lifecycle_events, ${TABLES.join(', ')} IN SHARE ROW EXCLUSIVE MODE`);
      const authorized=await connection.query(`SELECT id,client_id,role,active FROM users WHERE id=ANY($1::integer[])`,[[Number(actorId),Number(approvedBy),Number(this.aoId)]]);
      const user=id=>authorized.rows.find(u=>String(u.id)===String(id));
      const approver=user(approvedBy), actor=user(actorId), ao=user(this.aoId);
      if(!approver?.active || approver.role!=='admin' || !actor?.active || !(actor.role==='admin' || (actor.role==='ao' && String(actor.client_id)===String(this.clientId) && String(actor.id)===String(this.aoId))) || !ao?.active || ao.role!=='ao' || String(ao.client_id)!==String(this.clientId)) fail('APPROVAL_AUTHORIZATION_CHANGED');
      const {rows}=await connection.query(`SELECT * FROM max_spreadsheet_proposals WHERE id=$1 AND client_id=$2 AND actor_id=$3 AND ao_id=$4 AND conversation_id=$5 FOR UPDATE`,[proposalId,this.clientId,actorId,this.aoId,conversationId]);
      const proposal=rows[0];
      if(!proposal) fail('PROPOSAL_NOT_FOUND');
      if(proposal.digest!==expectedDigest || proposal.source_hash!==sourceHash) fail('PROPOSAL_DIGEST_MISMATCH');
      const selection=[...selectedOperationIds].sort();
      const requestDigest=digest({proposalId,expectedDigest,selection,actorId,conversationId,approvedBy,sourceHash});
      await connection.query('INSERT INTO max_spreadsheet_commit_requests(client_id,idempotency_key,request_digest,proposal_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING',[this.clientId,idempotencyKey,requestDigest,proposalId]);
      const request=await connection.query('SELECT request_digest FROM max_spreadsheet_commit_requests WHERE client_id=$1 AND idempotency_key=$2',[this.clientId,idempotencyKey]);
      if(request.rows[0]?.request_digest!==requestDigest) fail('IDEMPOTENCY_KEY_REUSED');
      if(proposal.status==='committed') {
        if(digest(selection)!==digest(proposal.receipt.selectedOperationIds)) fail('APPROVAL_ALREADY_CONSUMED');
        commitAttempted=true;await connection.query('COMMIT');commitAttempted=false; return {...proposal.receipt,replayed:true};
      }
      if(proposal.status!=='pending') fail('PROPOSAL_NOT_PENDING');
      let selected=selectedOperationIds.map(id=>proposal.plan.operations.find(o=>o.id===id));
      if(selected.some(o=>!o || o.blocked)) fail('UNAPPROVABLE_OPERATION');
      const suppressing=new Set(selected.filter(o=>o.type==='SUPPRESS_CALL').map(o=>o.target?.accountId));
      if(selected.some(o=>o.type==='ADD_TASK' && ['call','phone','phone_call'].includes(o.after?.kind) && suppressing.has(o.target?.accountId))) fail('CALL_SUPPRESSED');
      if(selected.some(o=>(o.dependsOn || []).some(id=>!selectedOperationIds.includes(id)))) fail('MISSING_OPERATION_DEPENDENCY');
      const ordered=[], visiting=new Set(),visited=new Set();
      const visit=operation=>{
        if(visiting.has(operation.id)) fail('CYCLIC_OPERATION_DEPENDENCY');
        if(visited.has(operation.id)) return;
        visiting.add(operation.id);
        for(const id of operation.dependsOn || []) visit(selected.find(o=>o.id===id));
        visiting.delete(operation.id);visited.add(operation.id);ordered.push(operation);
      };
      for(const operation of proposal.plan.operations.filter(o=>selectedOperationIds.includes(o.id))) visit(operation);
      selected=ordered;
      if(snapshotDigest(await this.snapshotContext({db:connection}))!==proposal.baseline_hash) fail('STALE_PROPOSAL');
      const results=[];
      for(const operation of selected) {
        const key=semanticKey(operation);
        const existing=await connection.query('SELECT observed FROM max_spreadsheet_effects WHERE client_id=$1 AND semantic_key=$2',[this.clientId,key]);
        if(existing.rows[0]) { await this.verifyExistingEffect(connection,operation,existing.rows[0].observed); results.push({operationId:operation.id,status:'already_applied',observed:existing.rows[0].observed}); continue; }
        const observed=await this.applyOperation(connection,operation);
        await connection.query('INSERT INTO max_spreadsheet_effects(client_id,semantic_key,proposal_id,operation,observed) VALUES($1,$2,$3,$4::jsonb,$5::jsonb)',[this.clientId,key,proposalId,JSON.stringify(operation),JSON.stringify(observed)]);
        results.push({operationId:operation.id,status:'verified',observed});
      }
      const receipt={proposalId,digest:expectedDigest,selectedOperationIds:selection,idempotencyKey,results,committed:true,approvedBy};
      await connection.query(`UPDATE max_spreadsheet_proposals SET status='committed',receipt=$2::jsonb,approved_by=$3,approved_at=now() WHERE id=$1`,[proposalId,JSON.stringify(receipt),approvedBy]);
      commitAttempted=true;await connection.query('COMMIT');commitAttempted=false; return receipt;
    } catch(error) {
      discardConnection=commitAttempted;
      if(commitAttempted) error.commitOutcomeUnknown=true;
      try {await connection.query('ROLLBACK');} catch(rollbackError) {discardConnection=true;error.rollbackFailed=true;}
      throw error;
    } finally {connection.release(discardConnection);}
  }
  async verifyExistingEffect(db,op,observed) {
    const target=await db.query('SELECT * FROM prospects WHERE id=$1 AND client_id=$2 AND assigned_ao_id=$3',[op.target?.accountId,this.clientId,this.aoId]);
    const snapshot={prospects:target.rows};
    if(!['CREATE_ACCOUNT','SET_ACCOUNT_FIELD'].includes(op.type)) {
      const relations={ADD_CONTACT:'max_spreadsheet_contacts',ADD_TASK:'ao_prospect_tasks',SUPPRESS_CALL:'max_spreadsheet_suppressions',ADD_PROVIDER_RELATIONSHIP:'max_spreadsheet_relationships',ADD_NOTE:'ao_prospect_activity',ADD_ACTIVITY:'ao_prospect_activity'};
      const table=relations[op.type];
      if(!table || !observed.id) fail('PERSISTED_EFFECT_CHANGED');
      const tenant=table==='ao_prospect_activity'?'tenant_id':'client_id';
      const read=await db.query(`SELECT *${op.type==='ADD_TASK' ? ',deadline::text AS verified_deadline' : ''} FROM ${table} WHERE id=$1 AND ${tenant}=$2 AND prospect_id=$3`,[observed.id,this.clientId,op.target.accountId]);
      snapshot[EFFECT_COLLECTIONS[op.type]]=read.rows;
    }
    if(!effectStillPresent({operation:op,observed},snapshot)) fail('PERSISTED_EFFECT_CHANGED');
  }
  async applyOperation(db,op) {
    if(!TYPES.has(op.type)) fail('UNSUPPORTED_OPERATION');
    const accountId=op.target?.accountId;
    if(op.type==='CREATE_ACCOUNT') {
      if(!accountId || !op.after?.name || op.blocked || !op.after.identityConfirmed) fail('UNRESOLVED_ACCOUNT_IDENTITY');
      if(op.after.outreachReviewRequired!==true) fail('OUTREACH_ADMISSION_HOLD_NOT_REVIEWED');
      const company=await db.query('INSERT INTO companies(name,client_id) VALUES($1,$2) RETURNING id',[op.after.name,this.clientId]);
      await db.query(`INSERT INTO prospects(id,client_id,company_id,assigned_ao_id,email,phone,website,ao_source_address,source,status,ao_outreach_review_required) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'spreadsheet_approved','cold',true)`,[accountId,this.clientId,company.rows[0].id,this.aoId,op.after.email||null,op.after.phone||null,op.after.website||null,op.after.address||null]);
      const read=await db.query('SELECT p.*,c.name AS company_name FROM prospects p JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id WHERE p.id=$1 AND p.client_id=$2',[accountId,this.clientId]);
      const value=read.rows[0];
      if(!value || value.ao_outreach_review_required!==true || value.company_name!==op.after.name || value.company_id!==company.rows[0].id || String(value.assigned_ao_id)!==String(this.aoId) || value.email!==(op.after.email||null) || value.phone!==(op.after.phone||null) || value.website!==(op.after.website||null) || value.ao_source_address!==(op.after.address||null)) fail('COMMIT_VERIFICATION_FAILED');
      return value;
    }
    const {rows}=await db.query('SELECT * FROM prospects WHERE id=$1 AND client_id=$2 AND assigned_ao_id=$3 FOR UPDATE',[accountId,this.clientId,this.aoId]);
    if(!rows[0]) fail('TARGET_OUTSIDE_APPROVED_SCOPE');
    const account=rows[0]; const after=op.after;
    const data={...after,evidence:op.evidence};
    const insertRelation=async (table, extraColumns='', extraValues=[],extraExpressions='')=>{
      const id=crypto.randomUUID();
      await db.query(`INSERT INTO ${table}(id,client_id,prospect_id,data${extraColumns}) VALUES($1,$2,$3,$4::jsonb${extraExpressions})`,[id,this.clientId,accountId,JSON.stringify(data),...extraValues]);
      const read=await db.query(`SELECT * FROM ${table} WHERE id=$1 AND client_id=$2`,[id,this.clientId]);
      if(!read.rows[0] || String(read.rows[0].prospect_id)!==String(accountId) || digest(read.rows[0].data)!==digest(data) || (extraColumns===',provider_id' && String(read.rows[0].provider_id)!==String(extraValues[0])) || (extraColumns===',channel' && read.rows[0].channel!==extraValues[0])) fail('COMMIT_VERIFICATION_FAILED');
      return read.rows[0];
    };
    switch(op.type) {
      case 'SET_ACCOUNT_FIELD': {
        const fields={email:'email',phone:'phone',status:'ao_current_status',ao_current_status:'ao_current_status',website:'website',address:'ao_source_address'};
        const field=fields[op.field];
        if(!field || after===null || after===undefined || after==='') fail('UNSUPPORTED_OR_CLEARING_FIELD');
        let existingValue=account[field] ?? null;
        if(!existingValue && ['website','ao_source_address'].includes(field)) {
          const company=await db.query('SELECT * FROM companies WHERE id=$1 AND client_id=$2',[account.company_id,this.clientId]);
          existingValue=field==='website' ? company.rows[0]?.website || null : account.address || account.location || company.rows[0]?.address || company.rows[0]?.location || null;
        }
        if(digest(existingValue)!==digest(op.before ?? null)) fail('FIELD_PRECONDITION_FAILED');
        if(['email','phone','website','ao_source_address'].includes(field) && existingValue && existingValue!==after && op.amendmentReviewed!==true) fail('CONTACT_METHOD_AMENDMENT_REQUIRED');
        if(field==='ao_current_status' && after!=='application_in_progress') fail('UNSUPPORTED_STATUS');
        const requiresAdmission=['email','phone'].includes(field) && existingValue!==after;
        if(requiresAdmission && op.outreachReviewRequired!==true) fail('OUTREACH_ADMISSION_HOLD_NOT_REVIEWED');
        await db.query(`UPDATE prospects SET ${field}=$1${requiresAdmission ? ',ao_outreach_review_required=true' : ''} WHERE id=$2 AND client_id=$3 AND assigned_ao_id=$4`,[after,accountId,this.clientId,this.aoId]);
        const read=await db.query(`SELECT ${field},ao_outreach_review_required FROM prospects WHERE id=$1 AND client_id=$2`,[accountId,this.clientId]);
        if(read.rows[0]?.[field]!==after || (requiresAdmission && read.rows[0]?.ao_outreach_review_required!==true)) fail('COMMIT_VERIFICATION_FAILED');
        return {field,value:read.rows[0][field],outreachReviewRequired:read.rows[0].ao_outreach_review_required};
      }
      case 'ADD_CONTACT':
        if(!after?.name) fail('CONTACT_NAME_REQUIRED');
        return insertRelation('max_spreadsheet_contacts');
      case 'ADD_TASK': {
        if(!after?.description || (after.dueDate && !/^\d{4}-\d{2}-\d{2}$/.test(after.dueDate))) fail('INVALID_TASK');
        if((account.ao_call_suppressed || account.do_not_contact) && ['call','phone','phone_call'].includes(after.kind)) fail('CALL_SUPPRESSED');
        const id=crypto.randomUUID();
        await db.query(`INSERT INTO ao_prospect_tasks(id,client_id,prospect_id,assigned_ao_id,assignment_category,motion,first_action,deadline,routing_snapshot) VALUES($1,$2,$3,$4,'FOLLOW_UP_REQUIRED','AO_LED',$5,$6::date,$7::jsonb)`,[id,this.clientId,accountId,this.aoId,after.description,after.dueDate || null,JSON.stringify(data)]);
        const read=await db.query('SELECT *,deadline::text AS verified_deadline FROM ao_prospect_tasks WHERE id=$1 AND client_id=$2',[id,this.clientId]);
        if(read.rows[0]?.first_action!==after.description || String(read.rows[0]?.prospect_id)!==String(accountId) || String(read.rows[0]?.assigned_ao_id)!==String(this.aoId) || read.rows[0]?.status!=='open' || String(read.rows[0]?.verified_deadline || '')!==String(after.dueDate || '') || digest(read.rows[0]?.routing_snapshot)!==digest(data)) fail('COMMIT_VERIFICATION_FAILED');
        return read.rows[0];
      }
      case 'SUPPRESS_CALL': {
        if(after?.channel!=='call' || !after.reason || op.target.contactId) fail('INVALID_SUPPRESSION_SCOPE');
        await db.query('UPDATE prospects SET ao_call_suppressed=true WHERE id=$1 AND client_id=$2',[accountId,this.clientId]);
        const read=await db.query('SELECT ao_call_suppressed FROM prospects WHERE id=$1 AND client_id=$2',[accountId,this.clientId]);
        if(read.rows[0]?.ao_call_suppressed!==true) fail('COMMIT_VERIFICATION_FAILED');
        return insertRelation('max_spreadsheet_suppressions',',channel',['call'],',$5');
      }
      case 'ADD_PROVIDER_RELATIONSHIP': {
        if(!after?.providerId || after.verified!==true) fail('UNVERIFIED_PROVIDER');
        const provider=await db.query('SELECT id FROM prospects WHERE id=$1 AND client_id=$2 AND assigned_ao_id=$3',[after.providerId,this.clientId,this.aoId]);
        if(!provider.rows.length) fail('TARGET_OUTSIDE_APPROVED_SCOPE');
        return insertRelation('max_spreadsheet_relationships',',provider_id',[after.providerId],',$5');
      }
      case 'ADD_NOTE': case 'ADD_ACTIVITY': {
        const kind=op.type==='ADD_NOTE'?'note':({phone_call:'call',in_person_visit:'visit',source_observation:'note'}[after?.kind] || after?.kind);
        const notes=op.type==='ADD_NOTE'?after?.text:after?.details;
        if(!['note','call','visit','email'].includes(kind) || !notes) fail('INVALID_HISTORICAL_ACTIVITY');
        if(op.type==='ADD_ACTIVITY' && !/^\d{4}-\d{2}-\d{2}$/.test(after.occurredOn || '')) fail('HISTORICAL_DATE_REQUIRED');
        const id=crypto.randomUUID();
        await db.query('INSERT INTO ao_prospect_activity(id,prospect_id,tenant_id,ao_id,activity_type,notes,metadata) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)',[id,accountId,this.clientId,this.aoId,kind,notes,JSON.stringify(data)]);
        const read=await db.query('SELECT * FROM ao_prospect_activity WHERE id=$1 AND tenant_id=$2',[id,this.clientId]);
        if(read.rows[0]?.notes!==notes || read.rows[0]?.activity_type!==kind || String(read.rows[0]?.prospect_id)!==String(accountId) || String(read.rows[0]?.ao_id)!==String(this.aoId) || digest(read.rows[0]?.metadata)!==digest(data)) fail('COMMIT_VERIFICATION_FAILED');
        return read.rows[0];
      }
      default: fail('UNSUPPORTED_OPERATION');
    }
  }
}
module.exports={PostgresSpreadsheetProposalStore,snapshotDigest,canonical,digest,semanticKey,effectStillPresent};
