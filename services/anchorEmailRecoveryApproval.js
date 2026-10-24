'use strict';
// Explicit approval-only bridge. It never activates a manifest or calls a send
// method. Canonical approval uses the existing exclusive mission-stage writer.
const { hash, clock } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { validateManifest, validateLivePolicy, RECOVERY_LOCK_KEY } = require('./anchorEmailRecovery');
const { requireQuarantine, assertNotQuarantined, listUnresolved, classifyBlockingRows } = require('./anchorEmailQuarantine');
const { GovernedOutboundStore } = require('./governedOutboundStore');
const { withTenantOutreachExecutorLock } = require('./tenantOutreachExecutorLock');
function fail(code) { throw Object.assign(new Error(code), { code, retryAllowed: false }); }
function time(options) { return options.now ? new Date(typeof options.now === 'function' ? options.now() : options.now) : new Date(); }
// Bound acquisition and every query, including the existing session-lock helpers.
// A late connection is discarded and cannot leak into a later approval attempt.
function boundedPool(pool) {
  const query = (db, text, values = []) => db.query(typeof text === 'string'
    ? { text, values, query_timeout: 6000 } : { ...text, query_timeout: 6000 });
  return { query: (text, values) => query(pool, text, values), async connect() {
    let expired = false, timer;
    const pending = pool.connect().then(client => {
      if (expired) { client.release(true); fail('anchor_recovery_approval_connection_timeout'); }
      return { query: (text, values) => query(client, text, values), release: client.release.bind(client) };
    });
    try { return await Promise.race([pending, new Promise((_, reject) => {
      timer = setTimeout(() => { expired = true; reject(Object.assign(new Error('anchor_recovery_approval_connection_timeout'), { code: 'anchor_recovery_approval_connection_timeout' })); }, 6000);
    })]); } finally { clearTimeout(timer); }
  } };
}

async function bindRecoveryApproval(pool, input, actor, adapters, options = {}) {
  if (!input || Object.keys(input).some(k => !['manifest','operatorId'].includes(k))
    || !actor?.id || !['admin','manager'].includes(actor.role) || String(actor.id) !== input.operatorId
    || input.manifest?.admissions?.length !== 1 || input.manifest.admissions[0].envelopeApprovalId !== null) fail('anchor_recovery_explicit_approval_required');
  const manifest = validateManifest({ ...input.manifest, admissions: [{ ...input.manifest.admissions[0], envelopeApprovalId: 'approval-to-be-bound' }] }, { now: time(options) });
  const admission = manifest.admissions[0];
  const env = options.env || process.env;
  const armed = () => env.ANCHOR_EMAIL_RECOVERY_APPROVAL_ID === manifest.id
    && env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE === 'true' && env.ANCHOR_GOVERNED_OUTBOUND_ENABLED === 'false'
    && [undefined,''].includes(env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID);
  if (!armed()) fail('anchor_recovery_approval_not_activated');
  pool = boundedPool(pool);
  const store = new GovernedOutboundStore(pool,'10');
  return store.lock(() => withTenantOutreachExecutorLock(pool, async () => {
    const db = await pool.connect(); let begun=false,discard=false;
    try {
      await db.query('BEGIN');begun=true;
      await db.query("SET LOCAL lock_timeout='5s'");await db.query("SET LOCAL statement_timeout='5s'");
      await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',[RECOVERY_LOCK_KEY]);
      const control=(await db.query('SELECT * FROM anchor_email_recovery_control WHERE singleton=true FOR UPDATE')).rows;
      if(control.length!==1 || control[0].maintenance_armed!==true || control[0].manifest!==null)fail('anchor_recovery_maintenance_required');
      const program=(await db.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2 FOR SHARE',[manifest.programId,'10'])).rows[0];
      if(!program || String(program.authorized_by)!==input.operatorId)fail('anchor_recovery_operator_mismatch');
      validateLivePolicy(program,manifest,time(options));
      const operator=(await db.query('SELECT id,client_id,role,active FROM users WHERE id::text=$1 AND client_id=10 FOR SHARE',[input.operatorId])).rows[0];
      if(!operator || operator.active!==true || !['admin','manager'].includes(operator.role) || operator.role!==actor.role)fail('anchor_recovery_operator_invalid');
      await requireQuarantine(db,manifest.quarantineDecisionHash);
      const unresolved=await listUnresolved(db,'10');
      const quarantines=(await db.query('SELECT * FROM anchor_email_quarantines WHERE tenant_id=$1',['10'])).rows;
      if(classifyBlockingRows(unresolved,quarantines).blockingCount!==0)fail('anchor_recovery_unresolved_attempts');
      const envelope=(await db.query('SELECT * FROM acquisition_outbound_envelopes WHERE id=$1 AND tenant_id=$2 FOR UPDATE',[admission.envelopeId,'10'])).rows[0];
      const item=(await db.query('SELECT * FROM acquisition_outbound_items WHERE id=$1 AND tenant_id=$2 FOR UPDATE',[admission.itemId,'10'])).rows[0];
      const day=envelope?.local_day instanceof Date?envelope.local_day.toISOString().slice(0,10):envelope?.local_day;
      if(!envelope || !item || envelope.program_id!==program.id || day!==clock(time(options)).day
        || !['frozen','authorized'].includes(envelope.status) || envelope.revision!==admission.revision
        || envelope.manifest_hash!==admission.manifestHash || hash(envelope.manifest)!==admission.manifestHash
        || envelope.manifest.length!==1 || item.envelope_id!==envelope.id || item.status!=='pending'
        || item.attempted_at!==null || item.provider_message_id!==null
        || item.candidate_id!==admission.candidateId || item.prospect_id!==admission.prospectId
        || item.company_id!==admission.companyId || item.email!==admission.recipientEmail
        || hash(item.snapshot)!==hash(envelope.manifest[0]) || item.snapshot.message?.subject!==admission.subject
        || item.snapshot.message?.body!==admission.body || item.snapshot.sender?.senderEmail!==manifest.senderEmail)fail('anchor_recovery_approval_binding_changed');
      const prospect=(await db.query('SELECT id,company_id,email,do_not_contact,is_synthetic,ao_outreach_review_required FROM prospects WHERE id::text=$1 AND client_id=10 FOR SHARE',[admission.prospectId])).rows[0];
      if(!prospect || prospect.company_id!==admission.companyId || String(prospect.email).trim().toLowerCase()!==admission.recipientEmail
        || prospect.do_not_contact!==false || prospect.is_synthetic!==false || prospect.ao_outreach_review_required!==false)fail('anchor_recovery_approval_contact_held');
      await assertNotQuarantined(db,{tenantId:'10',prospectId:admission.prospectId,companyId:admission.companyId,email:admission.recipientEmail,itemId:item.id,lockCompanyRows:true});
      if((await db.query('SELECT 1 FROM anchor_email_recovery_reservations WHERE item_id=$1',[item.id])).rows.length)fail('anchor_recovery_admission_consumed');
      const snapshot=await adapters.loadMission(envelope.mission_id,{preinitializedSchema:true,persistencePool:db});
      const prepared=await adapters.prepared(snapshot,program,{preinitializedSchema:true});
      const selected=prepared.candidates?.find(c=>String(c.candidateId)===item.candidate_id);
      if(prepared.revision!==envelope.revision || !selected || hash(selected.message)!==hash(item.snapshot.message)
        || prepared.sender?.senderEmail!==item.snapshot.sender.senderEmail || prepared.sender?.senderName!==item.snapshot.sender.senderName
        || prepared.sender?.sendingDomain!==item.snapshot.sender.sendingDomain)fail('anchor_recovery_artifacts_changed');
      const binding={id:envelope.id,programId:program.id,policyHash:program.policy_hash,manifestHash:envelope.manifest_hash,localDay:day,candidateIds:[item.candidate_id]};
      // Fixed canonical APPROVE_EXECUTION adapter only, never execute/send.
      const canonicalClient=await pool.connect(); let approval;
      try {
        const canonicalPool={query:(text,values=[])=>canonicalClient.query({text,values,query_timeout:6000})};
        await canonicalPool.query("SET lock_timeout='5s'");await canonicalPool.query("SET statement_timeout='5s'");
        approval=await adapters.approve(snapshot,program,binding,{preinitializedSchema:true,persistencePool:canonicalPool});
      } finally { canonicalClient.release(true); }
      if(!approval?.id || approval.payload?.approved!==true || approval.payload?.preparedArtifactRevision!==envelope.revision
        || String(approval.payload?.approvedBy)!==input.operatorId || hash(approval.payload?.dailyEnvelope)!==hash(binding))fail('anchor_recovery_canonical_approval_mismatch');
      const persisted=(await db.query('SELECT payload FROM acquisition_mission_contributions WHERE id=$1 AND tenant_id=$2 AND mission_id=$3',[approval.id,'10',envelope.mission_id])).rows[0]?.payload;
      if(!persisted || hash(persisted)!==hash(approval))fail('anchor_recovery_approval_not_durable');
      if(!armed())fail('anchor_recovery_approval_not_activated');
      validateManifest(manifest,{now:time(options)});validateLivePolicy(program,manifest,time(options));
      if(envelope.approval_id && envelope.approval_id!==approval.id)fail('anchor_recovery_approval_binding_changed');
      const updated=await db.query("UPDATE acquisition_outbound_envelopes SET status='authorized',approval_id=$2 WHERE id=$1 AND tenant_id='10' AND manifest_hash=$3 AND revision=$4 AND status IN ('frozen','authorized') AND (approval_id IS NULL OR approval_id=$2) RETURNING id",[envelope.id,approval.id,envelope.manifest_hash,envelope.revision]);
      if(updated.rows.length!==1)fail('anchor_recovery_approval_binding_changed');
      await store.event('anchor_recovery_execution_approval_bound',[manifest.id,envelope.id,approval.id],{programId:program.id,envelopeId:envelope.id,itemId:item.id,
        approvalReference:manifest.approvalReference,operatorId:input.operatorId,canonicalApprovalId:approval.id,providerAuthorized:false},db);
      await db.query('COMMIT');begun=false;
      return {bound:true,providerAuthorized:false,activationInstalled:false,canonicalApprovalId:approval.id,
        proposedManifest:{...input.manifest,admissions:[{...input.manifest.admissions[0],envelopeApprovalId:approval.id}]}};
    }catch(error){discard=true;throw error;}finally{if(begun){try{await db.query('ROLLBACK');}catch{discard=true;}}db.release(discard);}
  }));
}
module.exports={bindRecoveryApproval};
