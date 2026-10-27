'use strict';

// Synthetic local fixture only. No live identities, permissions or providers.
const { hash } = require('../../packages/acquisition-mission/DailyOutboundPolicy');
const { createHash } = require('node:crypto');
const quarantine = require('../../services/anchorEmailQuarantine');
const clone = value => JSON.parse(JSON.stringify(value));

function recoveryFixture() {
  const now = new Date('2026-10-19T16:00:00.000Z');
  const policy = { tenantId: '10', sourceMissionId: 'local-source-mission',
    senderEmail: 'sender@synthetic-recovery.test', inboxIntegrationId: 'local-inbox', aoOwnerIds: [1],
    startsAt: '2026-10-19T00:00:00.000Z', expiresAt: '2026-10-20T00:00:00.000Z',
    dailyCap: 5, totalCap: 5, spacingMinutes: 60, timeZone: 'America/New_York',
    weekdays: [1, 2, 3, 4, 5], startHour: 9, endHour: 17,
    maxSequenceStep: 1, enrichmentLimit: 15, preparationAttemptsPerDay: 3 };
  const prospectId = '10000000-0000-4000-8000-000000000002';
  const companyId = '20000000-0000-4000-8000-000000000002';
  const snapshot = { candidateId: 'local-fresh-candidate', prospectId, companyId, email: 'fresh@synthetic-recovery.test',
    message: { subject: 'Synthetic approved subject', body: 'Synthetic approved body.\nNo live delivery.', candidateId: 'local-fresh-candidate' },
    sender: { senderEmail: policy.senderEmail, senderName: 'Synthetic Sender', sendingDomain: 'synthetic-recovery.test' }, revision: 'local-revision-1' };
  const program = { id: 'local-program', tenant_id: '10', mode: 'active', source_mission_id: policy.sourceMissionId,
    policy, policy_hash: hash(policy), authorized_by: 'local-operator', authorized_at: now.toISOString() };
  const envelope = { id: 'local-envelope', program_id: program.id, tenant_id: '10', local_day: '2026-10-19',
    mission_id: 'local-daily-mission', revision: snapshot.revision, manifest: [snapshot], manifest_hash: hash([snapshot]),
    approval_id: 'local-envelope-approval', status: 'authorized' };
  const item = { id: 'local-fresh-item', tenant_id: '10', envelope_id: envelope.id, candidate_id: snapshot.candidateId,
    prospect_id: prospectId, company_id: companyId, email: snapshot.email, snapshot,
    status: 'attempted', attempted_at: '2026-10-19T16:00:00.123456Z', attempt_token: '2026-10-19 16:00:00.123456+00',
    provider_message_id: null, reason: null };
  const oldDecision = { tenantId: '10', itemId: 'local-held-item', envelopeId: 'local-held-envelope', programId: 'local-held-program',
    missionId: 'local-held-mission', canonicalId: 'local-held-canonical', candidateId: 'local-held-candidate', expectedRevision: 'local-held-revision',
    attemptToken: '2026-10-16 17:08:04.859123+00', recipientEmail: 'held@held-synthetic-recovery.test', companyNameKey: 'heldsyntheticenterprise', companyDomain: 'held-synthetic-recovery.test',
    prospectId: '10000000-0000-4000-8000-000000000001', companyId: '20000000-0000-4000-8000-000000000001',
    expectedStatus: 'uncertain', expectedReason: 'provider_or_persistence_error', approvalRef: 'local-synthetic-quarantine-approval' };
  const decisionHash = quarantine.decisionHash(oldDecision);
  const q = { id: `quarantine_${decisionHash}`, tenant_id: oldDecision.tenantId, item_id: oldDecision.itemId,
    envelope_id: oldDecision.envelopeId, program_id: oldDecision.programId, mission_id: oldDecision.missionId,
    company_name_key: oldDecision.companyNameKey, company_domain: oldDecision.companyDomain,
    canonical_id: oldDecision.canonicalId, candidate_id: oldDecision.candidateId, expected_revision: oldDecision.expectedRevision,
    attempt_token: oldDecision.attemptToken, recipient_email: oldDecision.recipientEmail,
    prospect_id: oldDecision.prospectId, company_id: oldDecision.companyId, expected_status: oldDecision.expectedStatus,
    expected_reason: oldDecision.expectedReason, approval_ref: oldDecision.approvalRef, decision_hash: decisionHash, created_at: now.toISOString() };
  const admission = { itemId: item.id, envelopeId: envelope.id, prospectId, companyId, candidateId: item.candidate_id,
    recipientEmail: item.email, revision: envelope.revision, envelopeApprovalId: envelope.approval_id,
    manifestHash: envelope.manifest_hash, subject: snapshot.message.subject, body: snapshot.message.body };
  const manifest = { version: 1, id: 'local-one-send-manifest', approvalReference: 'local-synthetic-send-approval',
    tenantId: '10', channel: 'email', provider: 'brevo', transport: 'governed_brevo', programId: program.id,
    policyHash: program.policy_hash, senderEmail: policy.senderEmail, inboxIntegrationId: policy.inboxIntegrationId,
    validFrom: '2026-10-19T15:30:00.000Z', expiresAt: '2026-10-19T16:30:00.000Z',
    maxFreshSends: 1, quarantineDecisionHash: decisionHash, admissions: [admission] };
  const approval = { id: envelope.approval_id, tenant_id: '10', mission_id: envelope.mission_id,
    specialist: 'operator', kind: 'approval', payload: { id: envelope.approval_id, missionId: envelope.mission_id,
      specialist: 'operator', kind: 'approval', payload: { approved: true, decisionKind: 'execution_approval',
        preparedArtifactRevision: envelope.revision,
        dailyEnvelope: { id: envelope.id, programId: program.id, policyHash: program.policy_hash,
          manifestHash: envelope.manifest_hash, localDay: envelope.local_day, candidateIds: [item.candidate_id] } } } };
  const prospect = { id: prospectId, client_id: 10, company_id: companyId, email: item.email,
    do_not_contact: false, is_synthetic: false, ao_outreach_review_required: false,
    email_verified: true, email_status: 'valid', enrichment_provenance: { email: { source: 'website_email' } },
    acquisition_metadata: {}, contact_classification: null, company_domain: 'synthetic-recovery.test', company_website: 'https://synthetic-recovery.test' };
  const client = { id: 10, active: true, autosend_enabled: false, sender_email: policy.senderEmail,
    sender_name: snapshot.sender.senderName, sending_domain: snapshot.sender.sendingDomain };
  const integration = { id: policy.inboxIntegrationId, tenant_id: '10', status: 'active', mailbox_address: policy.senderEmail };
  const reservation = { manifest_id: manifest.id, item_id: item.id, attempt_token: item.attempt_token,
    prospect_id: prospectId, company_id: companyId, recipient_email: item.email, used_at: now.toISOString(), dispatch_started_at: null };
  const oldUnresolved = { ...q, company_name: 'Held Synthetic Enterprise', company_domain_raw: q.company_domain, company_website: `https://${q.company_domain}`, status: q.expected_status, reason: q.expected_reason };
  const ownUnresolved = { company_name: 'Fresh Synthetic Enterprise', company_domain_raw: prospect.company_domain, company_website: prospect.company_website, tenant_id: '10', item_id: item.id, envelope_id: envelope.id, program_id: program.id,
    mission_id: envelope.mission_id, canonical_id: 'local-fresh-canonical', canonical_count: 1,
    candidate_id: item.candidate_id, expected_revision: envelope.revision, attempt_token: item.attempt_token,
    prospect_id: prospectId, company_id: companyId, recipient_email: item.email, status: 'attempted', reason: null };
  const companies = [{ id: companyId, client_id: 10, name: 'Fresh Synthetic Enterprise', domain: 'synthetic-recovery.test', website: 'https://synthetic-recovery.test' },
    { id: oldDecision.companyId, client_id: 10, name: 'Held Synthetic Enterprise', domain: oldDecision.companyDomain, website: `https://${oldDecision.companyDomain}` }];
  const f = { now, env: { ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: manifest.id }, manifest, program, envelope, item,
    approval, prospect, client, integration, oldDecision, companies, quarantine: q,
    catalog: { control_table: 'anchor_email_recovery_control' },
    control: { singleton: true, maintenance_armed: true, manifest }, reservations: [reservation],
    unresolved: [oldUnresolved, ownUnresolved], calls: [], quarantineHits: [],
    input: { channel: 'email', identities: [{ prospectId, clientId: 10 }],
      context: { programId: program.id, envelopeId: envelope.id, itemId: item.id, provider: 'brevo', transport: 'governed_brevo' },
      command: { toEmail: item.email, subject: snapshot.message.subject, body: snapshot.message.body,
        sender: { email: snapshot.sender.senderEmail, name: snapshot.sender.senderName },
        idempotencyKey: `exec_${createHash('sha256').update([envelope.mission_id,item.candidate_id,envelope.revision].join(':')).digest('hex').slice(0,32)}` } } };
  // Synthetic exact request for the installed provider-boundary tests. The
  // original frozen sibling fixture is intentionally unchanged.
  f.input.command.actualPayload = { sender: clone(f.input.command.sender), to: [{ email: item.email, name: '' }],
    subject: snapshot.message.subject, textContent: snapshot.message.body,
    htmlContent: '<html><body style="font-family:Georgia,serif;font-size:16px;line-height:1.6;color:#1a1a1a;max-width:560px;margin:0 auto;padding:20px;">'
      + snapshot.message.body.replace(/\n/g, '<br>') + '</body></html>',
    headers: { 'Idempotency-Key': f.input.command.idempotencyKey } };
  f.db = { async query(query, values = []) {
    const sql = typeof query === 'string' ? query : query.text;
    values = typeof query === 'string' ? values : query.values;
    f.calls.push({ sql, values });
    if (f.fail && f.fail.test(sql)) throw Object.assign(new Error('synthetic database failure'), { code: '42703' });
    let rows;
    if (/pg_advisory/.test(sql)) rows = [];
    else if (/to_regclass/.test(sql)) rows = f.catalog ? [f.catalog] : [];
    else if (/FROM anchor_email_recovery_control/.test(sql)) rows = f.control ? [f.control] : [];
    else if (/FROM anchor_email_quarantines/.test(sql)) rows = /decision_hash=/.test(sql) ? (f.quarantine ? [f.quarantine] : [])
      : /ORDER BY item_id/.test(sql) ? [...(f.quarantine ? [f.quarantine] : []), ...f.quarantineHits] : (f.quarantine ? [f.quarantine] : []);
    else if (/i.id AS item_id/.test(sql)) rows = f.unresolved;
    else if (/FROM acquisition_outbound_programs/.test(sql)) rows = f.program ? [f.program] : [];
    else if (/FROM acquisition_outbound_envelopes/.test(sql)) rows = f.envelope ? [f.envelope] : [];
    else if (/FROM acquisition_mission_contributions/.test(sql)) rows = f.approval ? [f.approval] : [];
    else if (/FROM acquisition_outbound_items/.test(sql)) rows = f.item ? [f.item] : [];
    else if (/^SELECT id, client_id FROM companies WHERE/.test(sql)) rows = f.companies.filter(c => String(c.id)===String(values[0]) && String(c.client_id)===String(values[1]));
    else if (/FROM prospects p/.test(sql)) rows = f.prospect ? [f.prospect] : [];
    else if (/FROM companies c/.test(sql)) rows = f.companies;
    else if (/FROM clients/.test(sql)) rows = f.client ? [f.client] : [];
    else if (/FROM tenant_mailbox_integrations/.test(sql)) rows = f.integration ? [f.integration] : [];
    else if (/FROM anchor_email_recovery_reservations/.test(sql)) rows = f.reservations;
    else throw new Error(`Unexpected synthetic query: ${sql}`);
    return { rows: clone(rows) };
  } };
  return f;
}
module.exports = { recoveryFixture, clone };
