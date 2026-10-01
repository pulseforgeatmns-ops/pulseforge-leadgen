'use strict';

// Durable bridge: every mailbox send remains bound to its reviewed program,
// frozen item, canonical Paige revision, contact evidence and live capacity.
const { hash, fail, clock, windowReason, missionScope, candidateReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { GovernedOutboundStore } = require('./governedOutboundStore');
const { governedOutboundEnabledForTenant } = require('./governedOutboundTenant');

async function validateGovernedSchedule(schedule, opts = {}) {
  const binding = schedule.authorizationSnapshot?.governed;
  if (!binding) fail('governed_schedule_binding_required');
  const pool = opts.pool || require('../db');
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());
  if (!governedOutboundEnabledForTenant(schedule.tenantId)) fail('environment_kill_switch');
  if (!['1', 'true', 'yes', 'on'].includes(String(process.env.TENANT_EMMETT_CAPACITY_ENABLED ?? 'true').toLowerCase()) || opts.emmettCapacityEnabled === false) fail('emmett_capacity_required');
  const store = opts.governedStore || new GovernedOutboundStore(pool, schedule.tenantId);
  const program = await store.program();
  if (!program || program.mode !== 'active' || program.id !== binding.programId
    || program.policy_hash !== binding.policyHash || hash(program.policy) !== binding.policyHash) fail('governed_grant_changed');
  const reason = windowReason(program.policy, now, true);
  if (reason) fail(reason);
  if (schedule.sendingIdentityId !== program.policy.sendingIdentityId
    || binding.mailboxIntegrationId !== program.policy.inboxIntegrationId) fail('governed_identity_changed');
  const envelope = await store.envelope(clock(now).day);
  const item = envelope && (await store.items(envelope.id)).find(x => x.id === binding.itemId);
  if (!envelope || envelope.id !== binding.envelopeId || envelope.program_id !== program.id
    || envelope.status !== 'authorized' || envelope.approval_id !== binding.approvalId
    || hash(envelope.manifest) !== binding.manifestHash || envelope.revision !== binding.revision
    || !item || item.status !== 'attempted' || !item.attempted_at) fail('governed_item_not_authorized');
  const frozen = envelope.manifest.find(x => x.candidateId === item.candidate_id);
  const snapshot = schedule.authorizationSnapshot;
  if (!frozen || hash(frozen) !== hash(item.snapshot) || schedule.prospectId !== item.prospect_id
    || schedule.missionId !== envelope.mission_id || schedule.recipientEmail !== item.email
    || snapshot.subject !== frozen.message.subject || snapshot.body !== frozen.message.body
    || snapshot.recipientEmail !== item.email || frozen.sender.senderEmail !== program.policy.senderEmail
    || schedule.outreachAssetId !== binding.outreachAssetId) fail('governed_schedule_payload_changed');
  const asset = opts.outreachAsset || (await pool.query(
    `SELECT id,version,content,lifecycle_state FROM acquisition_knowledge_objects WHERE tenant_id=$1 AND id=$2 AND object_type='outreach_asset'`,
    [String(schedule.tenantId), schedule.outreachAssetId])).rows[0];
  if (!asset || ['RETIRED','ARCHIVED'].includes(asset.lifecycle_state)
    || String(asset.version) !== String(schedule.outreachAssetVersion)
    || asset.content.subject !== snapshot.subject || asset.content.body !== snapshot.body
    || asset.content.prospectId !== schedule.prospectId
    || asset.content.preparedArtifactRevision !== binding.revision) fail('governed_outreach_asset_changed');
  const counts = await store.counts(program, clock(now).day);
  if (counts.today > program.policy.dailyCap || counts.total > program.policy.totalCap || counts.uncertain > 1) fail('governed_budget_changed');
  const adapters = opts.governedAdapters || require('./governedOutboundAdapters').adapters(pool, { tenantId: schedule.tenantId });
  const source = await adapters.loadMission(program.source_mission_id);
  if (!source?.mission || source.mission.planCancelled || hash(missionScope(source.mission)) !== program.scope_hash) fail('source_scope_changed');
  const current = await adapters.loadMission(envelope.mission_id);
  const prepared = await adapters.prepared(current, program);
  const selected = prepared.candidates.find(x => x.candidateId === item.candidate_id);
  if (prepared.revision !== binding.revision || !selected || hash(selected.message) !== hash(frozen.message)) fail('artifacts_changed');
  const crm = await adapters.contact(item.candidate_id);
  if (String(crm?.prospect_id || crm?.id) !== schedule.prospectId || String(crm?.company_id) !== item.company_id) fail('crm_binding_changed');
  const ineligible = candidateReason(selected.item, crm, selected.message, program.policy);
  if (ineligible) fail(ineligible);
  const suppressed = await store.suppression(item.snapshot, schedule.missionId, item.id);
  if (suppressed) fail(suppressed);
  await adapters.liveGate(program, item, prepared, now);
  return { program, envelope, item };
}

async function createGovernedOutreachAsset(program, envelope, item, pool) {
  const evidence = await require('./governedOutreachAkEvidence').resolveGovernedOutreachAssetEvidence(pool, { envelope, item });
  const id = `ak_governed_${hash([program.tenant_id, envelope.mission_id, envelope.revision, item.candidate_id]).slice(0, 28)}`;
  const saved = await require('./acquisitionKnowledge').createKnowledge({
    id, tenantId: String(program.tenant_id), clientId: Number(program.tenant_id),
    missionId: envelope.mission_id, objectType: 'outreach_asset', title: `Paige mission outreach ${item.candidate_id}`,
    content: { subject: item.snapshot.message.subject, body: item.snapshot.message.body,
      prospectId: item.prospect_id, candidateId: item.candidate_id, preparedArtifactRevision: envelope.revision },
    provenance: { source: 'canonical_paige_prepared_artifact', missionId: envelope.mission_id,
      preparedArtifactRevision: envelope.revision, executionApprovalId: envelope.approval_id, governedProgramId: program.id },
    evidence,
    epistemicState: 'OBSERVED', validationState: 'UNVALIDATED', lifecycleState: 'HYPOTHESIS',
    tags: ['governed_outbound', 'paige'],
  }, { pool, actor: { role: 'paige', id: 'canonical-paige' } });
  return { id: saved.id || saved.object?.id || id, version: String(saved.version || saved.object?.version || 1) };
}

async function finishGovernedSchedule(schedule, result, opts = {}) {
  const binding = schedule.authorizationSnapshot?.governed;
  if (!binding) return;
  const pool = opts.pool || require('../db');
  const store = new GovernedOutboundStore(pool, schedule.tenantId);
  const item = (await store.items(binding.envelopeId)).find(x => x.id === binding.itemId);
  if (!item) fail('governed_item_missing');
  const sent = ['sent', 'recovered_sent'].includes(result.result);
  const providerMessageId = result.message?.providerMessageId || result.message?.rfcMessageId || null;
  await store.finish(item, sent ? 'sent' : result.result === 'skipped' ? 'suppressed' : 'uncertain',
    sent ? null : result.reason || result.error?.code || 'scheduler_failed', providerMessageId);
  await store.event('durable_schedule_result', [schedule.id, result.result], {
    programId: binding.programId, envelopeId: binding.envelopeId, itemId: binding.itemId,
    missionId: schedule.missionId, scheduleId: schedule.id, outreachAssetId: schedule.outreachAssetId,
    sendingIdentityId: schedule.sendingIdentityId, result: result.result,
    outboundMessageId: result.message?.id || result.outboundMessageId || null,
    rfcMessageId: result.message?.rfcMessageId || null, threadId: result.message?.threadId || null, providerMessageId,
  });
}
module.exports = { validateGovernedSchedule, createGovernedOutreachAsset, finishGovernedSchedule };
