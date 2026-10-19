'use strict';

// SPEC-261 PREPARE_ONLY: inspect existing frozen work only. No adapter, search,
// enrichment, LLM, provider, pool, environment parser, DDL or write dependency.
const { hash, clock } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { governedContactReason } = require('../utils/governedContactEligibility');
const { validateCanonicalSenderConfig } = require('../utils/canonicalSenderIdentity');
const quarantine = require('./anchorEmailQuarantine');
const CONFIG_KEYS = Object.freeze(['mode','tenantId','programId','policyHash','approvalReference','validFrom','expiresAt','maxPreviewItems','quarantineDecisionHash']);
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const isText = value => typeof value === 'string' && value.length > 0 && value.length <= 1024 && value === value.trim() && !/[\x00-\x1f\x7f]/.test(value);
function stop(code) { throw Object.assign(new Error(code), { code }); }
function isoTime(value) { const time = typeof value === 'string' ? Date.parse(value) : NaN; return Number.isFinite(time) && new Date(time).toISOString() === value ? time : NaN; }
function validatePreviewConfig(config, { now = new Date() } = {}) {
  if (!isObject(config) || Object.keys(config).length !== CONFIG_KEYS.length || CONFIG_KEYS.some(key => !Object.hasOwn(config,key))) stop('prepare_only_configuration_required');
  if (config.mode !== 'PREPARE_ONLY' || config.tenantId !== '10' || config.maxPreviewItems !== 1
    || !['programId','approvalReference'].every(key => isText(config[key]) && !/^(?:todo|tbd|placeholder|<.*>)$/i.test(config[key]))
    || !['policyHash','quarantineDecisionHash'].every(key => typeof config[key] === 'string' && /^[a-f0-9]{64}$/.test(config[key]))) stop('prepare_only_scope_invalid');
  const from = isoTime(config.validFrom), until = isoTime(config.expiresAt), at = +now;
  if (!Number.isFinite(from) || !Number.isFinite(until) || !Number.isFinite(at)
    || until <= from || until - from > 86400000 || at < from || at >= until) stop('prepare_only_window_invalid');
  return Object.freeze({ ...config });
}
async function one(db, sql, values, code) { const { rows } = await db.query(sql, values); if (rows.length !== 1) stop(code); return rows[0]; }
function assert(condition, code) { if (!condition) stop(code); }
function exactEmail(value) { return typeof value === 'string' && /^[a-z0-9.!#$%&'*+\-/=?^_`{|}~]+@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/.test(value) && !value.includes('..') && !value.includes('.@') && !value.startsWith('.'); }
async function prepareOnlyPreview(db, input, { now = new Date() } = {}) {
  const result = { status: 'blocked', mode: 'PREPARE_ONLY', readOnly: true, providerCalls: 0, dispatchAllowed: false,
    approvalRequired: true, outcome: 'UNKNOWN', retryAllowed: false, rawUnknownCount: null, blockingUnknownCount: null,
    preview: [], proposedManifest: null };
  try {
    const config = validatePreviewConfig(input, { now });
    const control = await one(db, 'SELECT singleton,maintenance_armed,manifest FROM anchor_email_recovery_control', [], 'prepare_only_maintenance_required');
    assert(control.singleton === true && control.maintenance_armed === true && control.manifest === null, 'prepare_only_maintenance_required');
    await quarantine.requireQuarantine(db, config.quarantineDecisionHash);
    const unknown = await quarantine.listUnresolved(db, '10');
    const quarantines = (await db.query('SELECT * FROM anchor_email_quarantines WHERE tenant_id=$1', ['10'])).rows;
    const counts = quarantine.classifyBlockingRows(unknown, quarantines);
    result.rawUnknownCount = counts.rawCount; result.blockingUnknownCount = counts.blockingCount;
    result.quarantinedUnknownCount = counts.quarantinedCount;
    assert(counts.blockingCount === 0, 'prepare_only_unresolved_attempts');
    const program = await one(db, 'SELECT * FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2', [config.programId,'10'], 'prepare_only_program_missing');
    assert(program.id === config.programId && program.tenant_id === '10' && program.mode === 'active'
      && isObject(program.policy) && program.policy_hash === config.policyHash && hash(program.policy) === config.policyHash, 'prepare_only_policy_changed');
    const envelope = await one(db, `SELECT *,local_day::text AS local_day FROM acquisition_outbound_envelopes
      WHERE tenant_id=$1 AND program_id=$2 AND local_day=$3::date AND status='authorized' ORDER BY id LIMIT 2`,
    ['10',program.id,clock(now).day], 'current_prepared_item_required');
    assert(envelope.tenant_id === '10' && envelope.program_id === program.id && envelope.status === 'authorized'
      && envelope.local_day === clock(now).day && isText(envelope.mission_id) && isText(envelope.revision) && isText(envelope.approval_id)
      && Array.isArray(envelope.manifest) && envelope.manifest.length > 0 && hash(envelope.manifest) === envelope.manifest_hash, 'prepare_only_envelope_changed');
    const item = await one(db, `SELECT i.*,((i.attempted_at AT TIME ZONE 'UTC')::text||'+00') AS attempt_token FROM acquisition_outbound_items i
      WHERE i.tenant_id=$1 AND i.envelope_id=$2 AND i.status='pending' AND i.attempted_at IS NULL
      AND i.provider_message_id IS NULL ORDER BY i.id LIMIT 1`, ['10',envelope.id], 'current_prepared_item_required');
    assert(item.tenant_id === '10' && item.envelope_id === envelope.id && item.status === 'pending'
      && item.attempt_token === null && item.attempted_at === null && item.provider_message_id === null
      && ['id','candidate_id','prospect_id','company_id'].every(key => isText(item[key])) && exactEmail(item.email), 'prepare_only_item_changed');
    const snapshot = item.snapshot;
    const frozen = envelope.manifest.filter(row => row?.candidateId === item.candidate_id);
    assert(isObject(snapshot) && frozen.length === 1 && hash(snapshot) === hash(frozen[0])
      && snapshot.candidateId === item.candidate_id && snapshot.prospectId === item.prospect_id && snapshot.companyId === item.company_id
      && snapshot.email === item.email && snapshot.revision === envelope.revision
      && isObject(snapshot.message) && isText(snapshot.message.subject) && typeof snapshot.message.body === 'string'
      && snapshot.message.body.trim().length > 0 && snapshot.message.body.length <= 100000
      && isObject(snapshot.sender) && exactEmail(snapshot.sender.senderEmail) && isText(snapshot.sender.senderName), 'prepare_only_snapshot_changed');
    const approval = await one(db, `SELECT id,tenant_id,mission_id,specialist,kind,payload FROM acquisition_mission_contributions
      WHERE id=$1 AND tenant_id=$2 AND mission_id=$3`, [envelope.approval_id,'10',envelope.mission_id], 'prepare_only_envelope_approval_required');
    const contribution = approval.payload;
    const expectedApproval = { id: envelope.id, programId: program.id, policyHash: program.policy_hash,
      manifestHash: envelope.manifest_hash, localDay: envelope.local_day, candidateIds: envelope.manifest.map(row => row.candidateId) };
    assert(approval.id === envelope.approval_id && approval.tenant_id === '10' && approval.mission_id === envelope.mission_id
      && approval.specialist === 'operator' && approval.kind === 'approval' && contribution?.specialist === 'operator'
      && contribution.kind === 'approval' && contribution.id === approval.id && contribution.missionId === envelope.mission_id
      && contribution.payload?.approved === true && contribution.payload.decisionKind === 'execution_approval'
      && contribution.payload.invalidated !== true && contribution.payload.superseded !== true
      && contribution.payload.preparedArtifactRevision === envelope.revision && isObject(contribution.payload.dailyEnvelope)
      && hash(contribution.payload.dailyEnvelope) === hash(expectedApproval), 'prepare_only_envelope_approval_changed');
    const prospect = await one(db, `SELECT p.id,p.client_id,p.company_id,p.email,p.do_not_contact,p.is_synthetic,p.ao_outreach_review_required,
      to_jsonb(p)->'email_verified' AS email_verified,to_jsonb(p)->>'email_status' AS email_status,
      to_jsonb(p)->'enrichment_provenance' AS enrichment_provenance,to_jsonb(p)->'acquisition_metadata' AS acquisition_metadata,
      to_jsonb(p)->>'contact_classification' AS contact_classification,
      to_jsonb(c)->>'domain' AS company_domain,to_jsonb(c)->>'website' AS company_website
      FROM prospects p JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
      WHERE p.id::text=$1 AND p.client_id=$2 AND c.id::text=$3`, [item.prospect_id,10,item.company_id], 'prepare_only_current_contact_required');
    assert(String(prospect.id) === item.prospect_id && String(prospect.client_id) === '10'
      && String(prospect.company_id) === item.company_id && prospect.email === item.email, 'prepare_only_contact_changed');
    assert(prospect.ao_outreach_review_required === false, 'outreach_review_required');
    assert(prospect.do_not_contact === false && prospect.is_synthetic === false, 'prepare_only_contact_prohibited');
    const reason = governedContactReason(prospect, program.policy);
    assert(!reason, reason || 'prepare_only_contact_prohibited');
    await quarantine.assertNotQuarantined(db, { tenantId: '10', itemId: item.id, prospectId: item.prospect_id, companyId: item.company_id, email: item.email });
    const client = await one(db, 'SELECT id,active,autosend_enabled,sender_email,sender_name,sending_domain FROM clients WHERE id=$1', [10], 'prepare_only_sender_missing');
    assert(String(client.id) === '10' && client.active === true && client.autosend_enabled === false
      && client.sender_email === program.policy.senderEmail && client.sender_email === snapshot.sender.senderEmail
      && client.sender_name === snapshot.sender.senderName && client.sending_domain === snapshot.sender.sendingDomain
      && validateCanonicalSenderConfig({ tenantId:'10',clientId:10,senderEmail:client.sender_email,senderName:client.sender_name,sendingDomain:client.sending_domain }).ok, 'prepare_only_sender_changed');
    const inbox = await one(db, 'SELECT id,tenant_id,status,mailbox_address FROM tenant_mailbox_integrations WHERE id=$1 AND tenant_id=$2', [program.policy.inboxIntegrationId,'10'], 'prepare_only_inbox_missing');
    assert(inbox.id === program.policy.inboxIntegrationId && inbox.tenant_id === '10' && inbox.status === 'active'
      && inbox.mailbox_address === client.sender_email, 'prepare_only_inbox_changed');
    const used = (await db.query('SELECT item_id FROM anchor_email_recovery_reservations WHERE item_id=$1', [item.id])).rows;
    assert(used.length === 0, 'prepare_only_item_already_reserved');
    const admission = { itemId:item.id,envelopeId:envelope.id,prospectId:item.prospect_id,companyId:item.company_id,candidateId:item.candidate_id,
      recipientEmail:item.email,revision:envelope.revision,envelopeApprovalId:envelope.approval_id,manifestHash:envelope.manifest_hash,
      subject:snapshot.message.subject,body:snapshot.message.body };
    const proposedManifest = { version:1,id:null,approvalReference:null,tenantId:'10',channel:'email',provider:'brevo',transport:'governed_brevo',
      programId:program.id,policyHash:program.policy_hash,senderEmail:client.sender_email,inboxIntegrationId:inbox.id,
      validFrom:config.validFrom,expiresAt:config.expiresAt,maxFreshSends:1,quarantineDecisionHash:config.quarantineDecisionHash,admissions:[admission] };
    // Pure policy validation is shared with the real guard. The proposal remains
    // intentionally invalid as an activation manifest because approval/id are null.
    require('./anchorEmailRecovery').validateLivePolicy(program, proposedManifest, now);
    result.status = 'preview'; result.preview = [{ ...admission, senderEmail:client.sender_email,senderName:client.sender_name }];
    result.proposedManifest = proposedManifest; result.previewApprovalReference = config.approvalReference;
    result.nextStep = 'Obtain explicit send approval for this exact current recipient, copy, sender, program and cap; then revalidate live bindings before separate activation.';
    return result;
  } catch (error) {
    result.reason = error.code === '42703' ? 'prepare_only_required_schema_missing' : error.code && /^[a-z0-9_]{3,100}$/.test(error.code) ? error.code : 'prepare_only_read_incomplete';
    if (result.reason === 'current_prepared_item_required') result.nextStep = 'A separately approved bounded research/preparation step is required to create a current prepared item. This preview creates no work or send authority.';
    return result;
  }
}
module.exports = { CONFIG_KEYS, validatePreviewConfig, prepareOnlyPreview };
