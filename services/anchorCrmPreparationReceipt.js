'use strict';
// Pure projection of one bounded PostgreSQL receipt snapshot. No pool, runtime,
// schema initializer, provider, mutation, retry or authorization capability.
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const approval = require('../packages/acquisition-mission/ExecutionApproval');
const { unwrapSpecialistPayload } = require('../packages/acquisition-mission/ContributionSupersession');
const { extractCapacitySenderIdentity } = require('../utils/canonicalSenderIdentity');

// Keep grant, consumption, child, contributions and completion evidence in the
// SAME statement snapshot. N+1 limits detect overflow instead of hiding it.
const snapshotSql = `(SELECT coalesce(jsonb_agg(jsonb_build_object(
  'claim',to_jsonb(x),
  'mission',(SELECT to_jsonb(m) FROM acquisition_missions m
    WHERE m.tenant_id=g.tenant_id AND m.id='mission_crm_'||left(x.operation_id,32)),
  'contributions',(SELECT coalesce(jsonb_agg(to_jsonb(z)), '[]'::jsonb) FROM
    (SELECT id,tenant_id,mission_id,specialist,kind,payload FROM acquisition_mission_contributions
      WHERE tenant_id=g.tenant_id AND mission_id='mission_crm_'||left(x.operation_id,32)
      ORDER BY at,id LIMIT 101) z),
  'readyEvents',(SELECT coalesce(jsonb_agg(to_jsonb(z)), '[]'::jsonb) FROM
    (SELECT id,tenant_id,program_id,event_type,payload FROM acquisition_outbound_events
      WHERE tenant_id=g.tenant_id AND event_type='scoped_crm_preparation_ready'
        AND payload->>'operationId'=x.operation_id ORDER BY id LIMIT 2) z),
  'envelopes',(SELECT count(*) FROM (SELECT 1 FROM acquisition_outbound_envelopes
    WHERE tenant_id=g.tenant_id AND mission_id='mission_crm_'||left(x.operation_id,32) LIMIT 1) z),
  'executions',(SELECT count(*) FROM (SELECT 1 FROM acquisition_mission_outbound_executions
    WHERE tenant_id=g.tenant_id AND mission_id='mission_crm_'||left(x.operation_id,32) LIMIT 1) z)
)), '[]'::jsonb) FROM (SELECT operation_id,tenant_id,program_id,manifest_id,capability,operation,item_count,identities,approval_ref
  FROM anchor_scoped_resume_consumption WHERE tenant_id=g.tenant_id AND manifest_id=g.manifest_id
    AND capability='preparation' ORDER BY operation_id LIMIT 2) x) AS preparation_snapshot`;

function expectedPreparation(manifest) {
  const admissions = [...manifest.admissions].sort((a, b) => a.prospectId.localeCompare(b.prospectId));
  const operationId = hash(['anchor-crm-preparation-v1', manifest.programId, manifest.sourceMissionId,
    manifest.policyHash, manifest.scopeHash,
    admissions.map(a => [a.prospectId, a.companyId, a.recipientEmail, a.identityFingerprint])]);
  return { admissions, operationId, missionId: `mission_crm_${operationId.slice(0, 32)}` };
}
function preparationOutcome(grant) {
  const base = { status: grant.total_operations ? 'consumed_outcome_unknown' : 'not_recorded',
    evidenceScope: 'current_persisted_snapshot', retryAllowed: false, providerReadiness: 'not_checked',
    sendOutcome: 'not_checked', drafts: [] };
  try {
    const manifest = grant.manifest, expected = expectedPreparation(manifest), rows = grant.preparation_snapshot;
    if (!Array.isArray(rows) || rows.length !== 1 || grant.total_operations !== 1
      || grant.total_items !== expected.admissions.length) return base;
    const { claim, mission: row, contributions: stored, readyEvents, envelopes, executions } = rows[0];
    if (!claim || claim.tenant_id !== '10' || claim.program_id !== manifest.programId || claim.manifest_id !== manifest.id
      || claim.capability !== 'preparation' || claim.operation !== 'prepare_existing_crm'
      || claim.operation_id !== expected.operationId || claim.item_count !== expected.admissions.length
      || claim.approval_ref !== manifest.scopes.preparation.approvalReference
      || !Array.isArray(claim.identities)
      || hash([...claim.identities].sort((a, b) => a.prospectId.localeCompare(b.prospectId))) !== hash(expected.admissions)) return base;
    const unknown = { ...base, operationId: expected.operationId, missionId: expected.missionId };
    const mission = row?.payload, binding = mission?.crmPreparation;
    if (!row || row.id !== expected.missionId || row.tenant_id !== '10' || mission?.id !== expected.missionId
      || row.stage !== 'ready' || row.status !== mission.status || String(row.client_id) !== '10'
      || String(row.created_by) !== manifest.operatorId || row.orchestration_mission_id !== manifest.sourceMissionId
      || String(mission.tenantId) !== '10' || String(mission.clientId) !== '10'
      || String(mission.createdBy) !== manifest.operatorId || mission.stage !== 'ready'
      || mission.pendingOperatorDecision?.kind !== 'execution_approval' || mission.planCancelled
      || /cancel|failed/i.test(String(mission.status)) || mission.orchestrationMissionId !== manifest.sourceMissionId
      || binding?.operationId !== expected.operationId || binding.manifestHash !== grant.manifest_hash
      || binding.sourceMissionHash !== manifest.preparationAuthority.sourceMissionHash
      || binding.preparationOnly !== true || binding.providerCalls !== 0 || binding.providerReadiness !== 'not_checked'
      || hash(binding.admissions) !== hash(expected.admissions) || envelopes !== 0 || executions !== 0
      || !Array.isArray(stored) || !stored.length || stored.length > 100
      || stored.some(c => c.tenant_id !== '10' || c.mission_id !== expected.missionId || c.id !== c.payload?.id
        || c.payload?.missionId !== expected.missionId || c.specialist !== c.payload?.specialist || c.kind !== c.payload?.kind)
      || !Array.isArray(readyEvents) || readyEvents.length !== 1) return unknown;
    const event = readyEvents[0], evidence = event.payload;
    if (event.id !== hash(['scoped_crm_preparation_ready', expected.operationId]) || event.tenant_id !== '10'
      || event.program_id !== manifest.programId || event.event_type !== 'scoped_crm_preparation_ready'
      || evidence?.programId !== manifest.programId || evidence.operationId !== expected.operationId
      || evidence.missionId !== expected.missionId || evidence.selectedDraftCount !== expected.admissions.length
      || evidence.sourceMissionHash !== manifest.preparationAuthority.sourceMissionHash
      || evidence.providerCalls !== 0 || evidence.providerReadiness !== 'not_checked') return unknown;
    const contributions = stored.map(c => c.payload);
    // Pure canonical consistency check; this does not issue execution approval.
    if (approval.findExecutionApprovals(contributions).length || !approval.canAdvertiseExecutionApproval(mission, contributions)) return unknown;
    const capacity = approval.findEmmettCapacity(contributions, mission);
    const payload = unwrapSpecialistPayload(capacity), items = payload?.queue?.items;
    const sender = extractCapacitySenderIdentity(payload);
    const variants = unwrapSpecialistPayload(approval.findPaigeVariants(contributions, mission))?.variants;
    const review = mission.pendingOperatorDecision.executionReview;
    const preparedArtifactRevision = approval.computePreparedArtifactRevision(expected.missionId, contributions);
    if (!Array.isArray(items) || items.length !== expected.admissions.length || payload.governor?.halt
      || !['proceed', 'slow'].includes(payload.governor?.outcome)
      || !Array.isArray(variants) || variants.length !== expected.admissions.length
      || sender.senderEmail !== manifest.senderEmail || payload.senderIdentity?.senderName !== manifest.senderName
      || sender.sendingDomain !== manifest.sendingDomain || sender.inboxId !== manifest.senderEmail
      || review?.missionId !== expected.missionId || review.preparedArtifactRevision !== preparedArtifactRevision
      || hash(review.artifactBinding) !== hash(approval.computePreparedArtifactBinding(expected.missionId, contributions))
      || hash(review.infrastructure?.queue) !== hash(items)
      || !approval.validateProspectMessageBindings(payload).valid) return unknown;
    const drafts = [];
    for (const admitted of expected.admissions) {
      const matches = items.filter(i => String(i.companyId) === admitted.companyId && i.email === admitted.recipientEmail
        && String(i.candidateId || i.id) === admitted.companyId && String(i.prospectId) === admitted.companyId
        && String(i.paige?.candidateId) === admitted.companyId);
      const message = matches[0]?.paige;
      if (matches.length !== 1 || message?.ready !== true || typeof message.subject !== 'string' || !message.subject.trim()
        || message.subject.length > 2000 || typeof message.body !== 'string' || !message.body.trim() || message.body.length > 50000)
        return unknown;
      const copies = variants.filter(v => String(v.candidateId) === admitted.companyId && String(v.companyId) === admitted.companyId
        && v.variantId === message.variantId && v.subject === message.subject && v.body === message.body);
      if (copies.length !== 1) return unknown;
      drafts.push({ prospectId: admitted.prospectId, companyId: admitted.companyId, recipientEmail: admitted.recipientEmail,
        subject: message.subject, body: message.body });
    }
    return { ...base, status: 'ready_for_review', operationId: expected.operationId, missionId: expected.missionId,
      preparedArtifactRevision,
      draftContentHash: hash(drafts), candidateCount: drafts.length, executionApproved: false, envelopeAuthorized: false, drafts };
  } catch { return base; } // Malformed/partial evidence never promotes a consumed claim to success.
}
module.exports = { snapshotSql, expectedPreparation, preparationOutcome };
