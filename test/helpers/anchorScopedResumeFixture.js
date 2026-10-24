'use strict';
const { recoveryFixture, clone } = require('./anchorRecoveryManifestFixture');
const { hash, missionScope } = require('../../packages/acquisition-mission/DailyOutboundPolicy');

function scopedResumeFixture() {
  const f = recoveryFixture();
  f.sourceMission = { id: f.program.source_mission_id, tenantId: '10', objective: 'Synthetic local outreach',
    targetSegment: 'Synthetic firms', structuredMission: { immutable: true }, constraints: {}, status: 'ready' };
  f.program.scope_hash = hash(missionScope(f.sourceMission));
  f.prospect.company_name = f.companies[0].name;
  f.prospect.source = 'synthetic-local-fixture';
  f.prospect.source_type = null;
  f.prospect.source_ref = null;
  f.prospect.import_metadata = null;
  const scoped = require('../../services/anchorScopedResume');
  const admission = { prospectId: f.item.prospect_id, companyId: f.item.company_id,
    recipientEmail: f.item.email, companyNameKey: 'freshsyntheticenterprise',
    companyDomain: 'synthetic-recovery.test', identityFingerprint: scoped.identityFingerprint(f.prospect, f.companies[0]),
    approvalReference: 'local-synthetic-identity-review' };
  f.manifest = { version: 1, kind: 'scoped_resume', id: 'local-scoped-resume',
    approvalReference: 'local-synthetic-resume-approval', operatorId: 'local-operator', operatorRole: 'admin',
    tenantId: '10', programId: f.program.id, policyHash: f.program.policy_hash,
    sourceMissionId: f.program.source_mission_id, scopeHash: f.program.scope_hash,
    validFrom: '2026-10-19T15:30:00.000Z', expiresAt: '2026-10-19T20:30:00.000Z',
    quarantineDecisionHash: f.quarantine.decision_hash, senderEmail: f.client.sender_email,
    senderName: f.client.sender_name, sendingDomain: f.client.sending_domain,
    inboxIntegrationId: f.integration.id, reviewSchemaPolicy: 'nullable_no_default_preserve_existing',
    admissionPolicy: 'explicit_reviewed_identity_batch', admissions: [admission],
    scopes: { research: { enabled: false }, preparation: { enabled: false }, email: { enabled: true,
      approvalReference: 'local-synthetic-email-approval', dailyCap: 2, totalCap: 3,
      channel: 'email', provider: 'brevo', transport: 'governed_brevo', maxSequenceStep: 1,
      approvalMode: 'separately_reviewed_envelopes' } },
    sms: 'closed', calls: 'existing_policy', manualCrmEmail: 'closed', nonCrmVerification: 'closed', otherPrograms: 'legacy_unchanged' };
  f.control.manifest = f.manifest;
  f.env = { ANCHOR_EMAIL_SCOPED_RESUME_ID: f.manifest.id };
  f.reservations.forEach(r => { r.manifest_id = f.manifest.id; });
  f.input.command.actualPayload = { sender: clone(f.input.command.sender), to: [{ email: f.item.email, name: '' }],
    subject: f.input.command.subject, textContent: f.input.command.body,
    htmlContent: scoped.brevoHtml(f.input.command.body), headers: { 'Idempotency-Key': f.input.command.idempotencyKey } };
  const baseQuery = f.db.query;
  f.db.query = async function(query, values = []) {
    const sql = typeof query === 'string' ? query : query.text;
    if (/SELECT id FROM prospects p WHERE client_id/.test(sql)) return { rows: f.prospect.ao_outreach_review_required === true ? [{ id: f.prospect.id }] : [] };
    if (/SELECT state FROM acquisition_outbound_lifecycle/.test(sql)) return { rows: clone(f.lifecycleHits || []) };
    if (/SELECT id FROM acquisition_outbound_items WHERE tenant_id/.test(sql)) return { rows: clone(f.priorItemHits || []) };
    if (/SELECT id FROM acquisition_mission_outbound_executions/.test(sql)) return { rows: clone(f.canonicalHits || []) };
    if (/SELECT p.id FROM prospects p WHERE/.test(sql)) return { rows: clone(f.historyHits || []) };
    if (/SELECT l.id FROM ao_leads l JOIN/.test(sql)) return { rows: clone(f.aoHits || []) };
    if (/SELECT p.id,p.company_id,p.email,p.assigned_ao_id/.test(sql)) return { rows: clone(f.ownershipHits || []) };
    if (/SELECT l.id,l.business_name/.test(sql)) return { rows: clone(f.aoAliasHits || []) };
    if (/FROM users/.test(sql)) return { rows: f.operator === null ? [] : [{ id: f.manifest.operatorId, client_id: 10, role: f.manifest.operatorRole, active: true, ...f.operator }] };
    if (/AS prior_item_id/.test(sql)) return { rows: clone(f.priorAttempts || []) };
    if (/FROM acquisition_missions/.test(sql)) return { rows: [{ id: f.sourceMission.id, tenant_id: '10', payload: clone(f.sourceMission) }] };
    if (/FROM anchor_scoped_resume_grants/.test(sql)) return { rows: f.installation === null ? [] : [{ manifest_id: f.manifest.id, manifest_hash: hash(f.manifest), revoked_at: null, ...f.installation }] };
    return baseQuery(query, values);
  };
  return f;
}
module.exports = { scopedResumeFixture, clone };
