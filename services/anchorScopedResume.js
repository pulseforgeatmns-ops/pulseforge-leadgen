'use strict';

// SPEC-262. Validation and locked read-only authority helpers only. No startup
// DDL, environment-only approval, provider imports, defaults or review release.
const { hash, missionScope, clock } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { validateCanonicalSenderConfig } = require('../utils/canonicalSenderIdentity');
const quarantine = require('./anchorEmailQuarantine');
const TOP_KEYS = ['version', 'kind', 'id', 'approvalReference', 'operatorId', 'operatorRole', 'tenantId',
  'programId', 'policyHash', 'sourceMissionId', 'scopeHash', 'validFrom', 'expiresAt', 'quarantineDecisionHash',
  'senderEmail', 'senderName', 'sendingDomain', 'inboxIntegrationId', 'reviewSchemaPolicy', 'admissionPolicy',
  'admissions', 'scopes', 'sms', 'calls', 'manualCrmEmail', 'nonCrmVerification', 'otherPrograms'];
const IDENTITY_KEYS = ['prospectId', 'companyId', 'recipientEmail', 'companyNameKey', 'companyDomain', 'identityFingerprint', 'approvalReference'];
const READ_ONLY = Symbol('read-only scoped inspection');
const OPERATIONS = Object.freeze({ research: ['research_existing_source'],
  preparation: ['reuse_existing_artifacts', 'generate_deterministic_existing_template', 'freeze', 'refill', 'approve_canonical_envelope'], email: ['dispatch'] });
function fail(code, reason = code) { throw Object.assign(new Error(reason), { code, reason, status: 409, providerBoundaryCrossed: false }); }
const object = v => v !== null && typeof v === 'object' && !Array.isArray(v);
const text = (v, n = 512) => typeof v === 'string' && v.length > 0 && v.length <= n && v === v.trim() && !/[\x00-\x1f\x7f]/.test(v);
const digest = v => typeof v === 'string' && /^[a-f0-9]{64}$/.test(v);
const exact = (v, keys) => object(v) && Object.keys(v).length === keys.length && keys.every(k => Object.hasOwn(v, k));
function email(v) {
  if (typeof v !== 'string' || v.length > 254) return null;
  const normalized = v.trim().toLowerCase();
  return /^[a-z0-9.!#$%&'*+\-/=?^_`{|}~]+@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/.test(normalized)
    && !normalized.startsWith('.') && !normalized.includes('..') && !normalized.includes('.@') ? normalized : null;
}
function timestamp(v) {
  if (typeof v !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/.test(v)) return NaN;
  const [y, m, d] = v.slice(0, 10).split('-').map(Number), [h, min, sec] = v.slice(11, 19).split(':').map(Number);
  if (m < 1 || m > 12 || d < 1 || d > new Date(Date.UTC(y, m, 0)).getUTCDate() || h > 23 || min > 59 || sec > 59) return NaN;
  return Date.parse(v);
}
function deepFreeze(value) { if (value && typeof value === 'object') { Object.values(value).forEach(deepFreeze); Object.freeze(value); } return value; }
function cap(v, max) { return Number.isSafeInteger(v) && v >= 1 && v <= max; }
function validateScopedResumeManifest(raw, { now = new Date() } = {}) {
  const invalid = why => fail('anchor_scoped_resume_invalid_manifest', why);
  if (!exact(raw, TOP_KEYS)) invalid('resume_manifest_fields_required');
  if (raw.version !== 1 || raw.kind !== 'scoped_resume' || raw.tenantId !== '10'
    || !['admin', 'manager'].includes(raw.operatorRole) || raw.reviewSchemaPolicy !== 'nullable_no_default_preserve_existing'
    || raw.admissionPolicy !== 'explicit_reviewed_identity_batch' || raw.calls !== 'existing_policy'
    || raw.otherPrograms !== 'legacy_unchanged'
    || ['sms', 'manualCrmEmail', 'nonCrmVerification'].some(k => raw[k] !== 'closed')) invalid('resume_manifest_scope_invalid');
  for (const k of ['id', 'approvalReference', 'operatorId', 'programId', 'sourceMissionId', 'inboxIntegrationId', 'senderName']) if (!text(raw[k])) invalid(`resume_${k}_invalid`);
  for (const k of ['policyHash', 'scopeHash', 'quarantineDecisionHash']) if (!digest(raw[k])) invalid(`resume_${k}_invalid`);
  if (!email(raw.senderEmail) || email(raw.senderEmail) !== raw.senderEmail || !text(raw.sendingDomain)
    || raw.sendingDomain !== raw.senderEmail.split('@')[1]
    || !validateCanonicalSenderConfig({ tenantId: '10', clientId: 10, senderEmail: raw.senderEmail,
      senderName: raw.senderName, sendingDomain: raw.sendingDomain }).ok) invalid('resume_sender_invalid');
  const from = timestamp(raw.validFrom), until = timestamp(raw.expiresAt), at = +now;
  if (!Number.isFinite(at) || !Number.isFinite(from) || !Number.isFinite(until) || until <= from
    || until - from > 30 * 86400000 || at < from || at >= until) invalid('resume_window_invalid');
  if (!Array.isArray(raw.admissions) || !raw.admissions.length || raw.admissions.length > 100) invalid('resume_admissions_invalid');
  const prospectIds = new Set(), recipients = new Set();
  for (const a of raw.admissions) {
    if (!exact(a, IDENTITY_KEYS) || IDENTITY_KEYS.some(k => !text(a[k])) || !digest(a.identityFingerprint)
      || !email(a.recipientEmail) || email(a.recipientEmail) !== a.recipientEmail) invalid('resume_identity_invalid');
    let identity;
    try { identity = quarantine.companyIdentity({ name: a.companyNameKey, domain: a.companyDomain }); } catch { invalid('resume_company_identity_invalid'); }
    if (identity.companyNameKey !== a.companyNameKey || identity.companyDomain !== a.companyDomain) invalid('resume_company_identity_invalid');
    if (prospectIds.has(a.prospectId) || recipients.has(a.recipientEmail)) invalid('resume_duplicate_identity');
    prospectIds.add(a.prospectId); recipients.add(a.recipientEmail);
  }
  if (!exact(raw.scopes, ['research', 'preparation', 'email'])) invalid('resume_scopes_required');
  for (const name of ['research', 'preparation', 'email']) {
    const s = raw.scopes[name];
    if (exact(s, ['enabled']) && s.enabled === false) continue;
    if (name === 'research') invalid('resume_paid_research_unsupported');
    const keys = ['enabled', 'approvalReference', 'dailyCap', 'totalCap', ...(name === 'research' ? ['operations', 'providerBudgetHash']
      : name === 'preparation' ? ['dailyItemCap', 'totalItemCap', 'operations']
        : ['channel', 'provider', 'transport', 'maxSequenceStep', 'approvalMode'])];
    if (!exact(s, keys) || s.enabled !== true || !text(s.approvalReference)
      || !cap(s.dailyCap, name === 'email' ? 50 : name === 'preparation' ? 3 : 20)
      || !cap(s.totalCap, 100) || s.totalCap < s.dailyCap) invalid(`resume_${name}_bounds_invalid`);
    if (name !== 'email' && (!Array.isArray(s.operations) || !s.operations.length || new Set(s.operations).size !== s.operations.length
      || s.operations.some(op => !OPERATIONS[name].includes(op)))) invalid(`resume_${name}_operations_invalid`);
    if (name === 'research' && !digest(s.providerBudgetHash)) invalid('resume_provider_budget_required');
    if (name === 'preparation' && (!cap(s.dailyItemCap, 50) || !cap(s.totalItemCap, 100) || s.totalItemCap < s.dailyItemCap)) invalid('resume_preparation_items_invalid');
    if (name === 'email' && (s.channel !== 'email' || s.provider !== 'brevo' || s.transport !== 'governed_brevo'
      || s.maxSequenceStep !== 1 || !['separately_reviewed_envelopes', 'delegated_existing_program'].includes(s.approvalMode))) invalid('resume_email_route_invalid');
  }
  if (raw.scopes.preparation.enabled && raw.scopes.preparation.operations.includes('approve_canonical_envelope')
    && (!raw.scopes.email.enabled || raw.scopes.email.approvalMode !== 'delegated_existing_program')) invalid('resume_canonical_delegation_required');
  return deepFreeze(JSON.parse(JSON.stringify(raw)));
}

// Review-state deliberately excluded: the separately approved NULL -> false CAS
// must preserve identity. Current source/import/provenance and company snapshot
// are all included, so a relink or enrichment cannot inherit old admission.
function identityFingerprint(prospect, company) {
  if (!object(prospect) || !object(company)) fail('anchor_scoped_resume_identity_required');
  const identity = quarantine.companyIdentity(company);
  const value = { tenantId: String(prospect.client_id), prospectId: String(prospect.id), companyId: String(prospect.company_id),
    recipientEmail: email(prospect.email), company: { id: String(company.id), tenantId: String(company.client_id),
      name: company.name, domain: company.domain ?? null, website: company.website ?? null,
      acquisitionMetadata: company.acquisition_metadata ?? null, ...identity },
    emailVerified: prospect.email_verified ?? null, emailStatus: prospect.email_status ?? null,
    enrichmentProvenance: prospect.enrichment_provenance ?? null, acquisitionMetadata: prospect.acquisition_metadata ?? null,
    contactClassification: prospect.contact_classification ?? null, source: prospect.source ?? null,
    sourceType: prospect.source_type ?? null, sourceRef: prospect.source_ref ?? null, importMetadata: prospect.import_metadata ?? null,
    acquisitionSource: prospect.acquisition_source ?? null, acquisitionKnowledgeObjectId: prospect.acquisition_knowledge_object_id ?? null,
    acquisitionProjectionId: prospect.acquisition_projection_id ?? null,
    assignedAoId: prospect.assigned_ao_id == null ? null : String(prospect.assigned_ao_id),
    closerId: prospect.closer_id == null ? null : String(prospect.closer_id),
    lastContactedAt: prospect.last_contacted_at == null ? null : new Date(prospect.last_contacted_at).toISOString(),
    lastReplyAt: prospect.last_reply_at == null ? null : new Date(prospect.last_reply_at).toISOString(),
    operationalStatus: prospect.operational_status ?? null, setterStatus: prospect.setter_status ?? null, closerStatus: prospect.closer_status ?? null };
  if (!value.recipientEmail || value.tenantId !== '10' || value.company.tenantId !== '10'
    || value.companyId !== value.company.id) fail('anchor_scoped_resume_identity_invalid');
  return hash(value);
}
async function one(db, sql, values, code) { const result = await db.query(sql, values); if (result.rows?.length !== 1) fail(code); return result.rows[0]; }
async function readScopedIdentity(db, input, { lock = true } = {}) {
  const { tenantId, prospectId, companyId, recipientEmail } = input || {};
  if (tenantId !== '10' || !text(prospectId) || !text(companyId) || email(recipientEmail) !== recipientEmail) fail('anchor_scoped_resume_identity_required');
  if (lock) for (const key of quarantine.identityLockKeys({ tenantId, prospectId, companyId, recipientEmail })) await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [key]);
  const prospect = await one(db, `SELECT p.id,p.client_id,p.company_id,p.email,p.do_not_contact,p.is_synthetic,p.ao_outreach_review_required,
    to_jsonb(p)->'email_verified' AS email_verified,to_jsonb(p)->>'email_status' AS email_status,
    to_jsonb(p)->'enrichment_provenance' AS enrichment_provenance,to_jsonb(p)->'acquisition_metadata' AS acquisition_metadata,
    to_jsonb(p)->>'contact_classification' AS contact_classification,to_jsonb(p)->>'source' AS source,
    to_jsonb(p)->>'source_type' AS source_type,to_jsonb(p)->>'source_ref' AS source_ref,to_jsonb(p)->'import_metadata' AS import_metadata,
    to_jsonb(p)->>'acquisition_source' AS acquisition_source,to_jsonb(p)->>'acquisition_knowledge_object_id' AS acquisition_knowledge_object_id,
    to_jsonb(p)->>'acquisition_projection_id' AS acquisition_projection_id,to_jsonb(c)->'acquisition_metadata' AS company_acquisition_metadata,
    to_jsonb(p)->>'assigned_ao_id' AS assigned_ao_id,to_jsonb(p)->>'closer_id' AS closer_id,
    to_jsonb(p)->>'last_contacted_at' AS last_contacted_at,to_jsonb(p)->>'last_reply_at' AS last_reply_at,
    to_jsonb(p)->>'operational_status' AS operational_status,to_jsonb(p)->>'setter_status' AS setter_status,to_jsonb(p)->>'closer_status' AS closer_status,
    c.name AS company_name,to_jsonb(c)->>'domain' AS company_domain,to_jsonb(c)->>'website' AS company_website
    FROM prospects p JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.id::text=$1 AND p.client_id=$2 AND c.id::text=$3${lock ? ' FOR SHARE OF p,c' : ''}`,
  [prospectId, 10, companyId], 'anchor_scoped_resume_identity_missing');
  if (String(prospect.id) !== prospectId || String(prospect.client_id) !== tenantId || String(prospect.company_id) !== companyId
    || email(prospect.email) !== recipientEmail) fail('anchor_scoped_resume_identity_changed');
  const company = { id: companyId, client_id: 10, name: prospect.company_name, domain: prospect.company_domain, website: prospect.company_website, acquisition_metadata: prospect.company_acquisition_metadata ?? null };
  return { prospect, company, ...quarantine.companyIdentity(company), identityFingerprint: identityFingerprint(prospect, company) };
}
function assertIdentityReview(bound, admission) {
  if (bound.prospect.ao_outreach_review_required !== false) fail('outreach_review_required');
  if (bound.prospect.do_not_contact !== false || bound.prospect.is_synthetic !== false) fail('anchor_recovery_contact_prohibited');
  for (const key of ['companyNameKey', 'companyDomain', 'identityFingerprint']) if (bound[key] !== admission[key]) fail('anchor_scoped_resume_identity_changed');
}
async function assertScopedResumeCapability(db, { program, capability, operation } = {}, options = {}) {
  if (program?.tenant_id !== '10') fail('anchor_scoped_resume_scope_invalid');
  if (!OPERATIONS[capability]?.includes(operation)) fail('anchor_scoped_resume_operation_invalid');
  const locking = options[READ_ONLY] !== true;
  if (locking) await db.query('SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))', ['anchor-email-recovery:10']);
  const state = await require('./anchorEmailRecovery').loadRecoveryState(db, { ...options, lockControl: locking });
  if (state.mode !== 'scoped_resume') fail('anchor_scoped_resume_closed', state.reason);
  const grant = state.manifest, scope = grant.scopes[capability];
  if (scope.enabled !== true || (capability !== 'email' && !scope.operations.includes(operation))) fail('anchor_scoped_resume_capability_closed');
  if (!program || program.id !== grant.programId || program.tenant_id !== '10' || program.policy_hash !== grant.policyHash) fail('anchor_scoped_resume_program_changed');
  await require('./anchorScopedResumeControl').assertInstalledScopedResume(db, grant, { ...options, lock: locking });
  const live = await one(db, `SELECT * FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2${locking ? ' FOR SHARE' : ''}`, [grant.programId, '10'], 'anchor_scoped_resume_program_missing');
  require('./anchorEmailRecovery').validateLivePolicy(live, grant, options.now || new Date());
  if (live.source_mission_id !== grant.sourceMissionId || live.scope_hash !== grant.scopeHash) fail('anchor_scoped_resume_source_changed');
  const source = await one(db, `SELECT id,tenant_id,payload FROM acquisition_missions WHERE id=$1 AND tenant_id=$2${locking ? ' FOR SHARE' : ''}`, [grant.sourceMissionId, '10'], 'anchor_scoped_resume_source_missing');
  if (source.id !== grant.sourceMissionId || source.tenant_id !== '10' || source.payload?.id !== source.id
    || String(source.payload?.tenantId) !== '10' || source.payload?.structuredMission?.immutable !== true
    || source.payload.planCancelled || /cancel/i.test(String(source.payload.status)) || hash(missionScope(source.payload)) !== grant.scopeHash) fail('anchor_scoped_resume_source_changed');
  await quarantine.requireQuarantine(db, grant.quarantineDecisionHash);
  return { legacy: false, manifest: grant, resumeGrant: grant, program: live, capability, operation, scope, manifestHash: hash(grant) };
}
async function inspectScopedResumeCapabilities(db, { program } = {}, options = {}) {
  const results = {};
  for (const capability of ['research', 'preparation', 'email']) {
    const operation = capability === 'research' ? 'research_existing_source' : capability === 'preparation' ? 'reuse_existing_artifacts' : 'dispatch';
    try { const grant = await assertScopedResumeCapability(db, { program, capability, operation }, { ...options, [READ_ONLY]: true }); results[capability] = { enabled: true, scope: grant.scope, manifestHash: grant.manifestHash }; }
    catch (e) { results[capability] = { enabled: false, reason: e.code || 'anchor_scoped_resume_unavailable' }; }
  }
  return results;
}
async function assertScopedFirstTouch(db, admission, itemId) {
  // Across grants, envelopes and days: a new item ID cannot make a previously
  // attempted recipient/contact/company a first touch again. Reservations are
  // permanent even when a proven pre-provider cleanup clears attempted_at.
  const prior = await db.query(`SELECT r.item_id AS prior_item_id FROM anchor_email_recovery_reservations r
    WHERE r.item_id<>$1 AND (r.prospect_id=$2 OR r.company_id=$3 OR r.recipient_email=$4)
    UNION SELECT i.id AS prior_item_id FROM acquisition_outbound_items i
    WHERE i.tenant_id='10' AND i.id<>$1 AND (i.prospect_id=$2 OR i.company_id=$3 OR lower(btrim(i.email))=$4)
      AND (i.attempted_at IS NOT NULL OR i.provider_message_id IS NOT NULL OR i.status IN ('attempted','uncertain','sent'))`,
  [itemId, admission.prospectId, admission.companyId, admission.recipientEmail]);
  if (!Array.isArray(prior.rows) || prior.rows.length !== 0) fail('anchor_scoped_resume_first_touch_consumed');
}
async function buildScopedBinding(db, state, context, options = {}) {
  if (!object(context) || !['programId', 'envelopeId', 'itemId'].every(k => text(context[k]))) fail('anchor_recovery_context_required');
  const grant = state.manifest;
  const authority = await assertScopedResumeCapability(db, { program: { id: context.programId, tenant_id: '10', policy_hash: grant.policyHash }, capability: 'email', operation: 'dispatch' }, options);
  const envelope = await one(db, 'SELECT * FROM acquisition_outbound_envelopes WHERE id=$1 AND tenant_id=$2 AND program_id=$3', [context.envelopeId, '10', grant.programId], 'anchor_recovery_envelope_missing');
  const item = await one(db, `SELECT i.*,((i.attempted_at AT TIME ZONE 'UTC')::text || '+00') AS attempt_token FROM acquisition_outbound_items i WHERE i.id=$1 AND i.tenant_id=$2 AND i.envelope_id=$3`, [context.itemId, '10', envelope.id], 'anchor_recovery_item_missing');
  const approvedIdentity = grant.admissions.find(a => a.prospectId === item.prospect_id && a.companyId === item.company_id && a.recipientEmail === item.email);
  if (!approvedIdentity) fail('anchor_scoped_resume_admission_missing');
  const identity = await readScopedIdentity(db, { tenantId: '10', ...approvedIdentity });
  assertIdentityReview(identity, approvedIdentity);
  const admission = { itemId: item.id, envelopeId: envelope.id, prospectId: item.prospect_id, companyId: item.company_id,
    candidateId: item.candidate_id, recipientEmail: item.email, revision: envelope.revision, envelopeApprovalId: envelope.approval_id,
    manifestHash: envelope.manifest_hash, subject: item.snapshot?.message?.subject, body: item.snapshot?.message?.body };
  const runtimeManifest = { version: 1, id: grant.id, approvalReference: grant.approvalReference, tenantId: '10', channel: 'email',
    provider: 'brevo', transport: 'governed_brevo', programId: grant.programId, policyHash: grant.policyHash,
    senderEmail: grant.senderEmail, inboxIntegrationId: grant.inboxIntegrationId, validFrom: grant.validFrom, expiresAt: grant.expiresAt,
    maxFreshSends: 1, quarantineDecisionHash: grant.quarantineDecisionHash, admissions: [admission] };
  const bound = await require('./anchorEmailRecovery').readBindings(db, { manifest: runtimeManifest }, context, options);
  await assertScopedFirstTouch(db, admission, item.id);
  if (item.snapshot?.sender?.senderName !== grant.senderName || item.snapshot?.sender?.sendingDomain !== grant.sendingDomain) fail('anchor_scoped_resume_sender_changed');
  return { ...bound, ...authority, admission, approvedIdentity, envelope: bound.envelope, item: bound.item };
}
function brevoHtml(body) { return '<html><body style="font-family:Georgia,serif;font-size:16px;line-height:1.6;color:#1a1a1a;max-width:560px;margin:0 auto;padding:20px;">' + body.replace(/\n/g, '<br>') + '</body></html>'; }
function actualRecipients(command) {
  const p = command?.actualPayload;
  if (!object(p)) fail('anchor_recovery_actual_payload_required');
  if (command.transport === 'smtp') {
    if (Object.keys(p).some(k => !['from', 'replyTo', 'to', 'subject', 'text', 'html', 'headers'].includes(k))
      || !Array.isArray(p.to) || !p.to.length || !text(p.from) || !text(p.replyTo)
      || typeof p.subject !== 'string' || typeof p.text !== 'string'
      || (p.html != null && typeof p.html !== 'string') || !object(p.headers)
      || Object.keys(p.headers).some(k => !['Message-ID', 'In-Reply-To', 'References'].includes(k))) fail('anchor_recovery_payload_invalid');
    const recipients = p.to.map(r => typeof r === 'string' ? email(r) : exact(r, ['address', 'name']) ? email(r.address) : null);
    if (recipients.some(r => !r)) fail('anchor_recovery_payload_invalid');
    return recipients;
  }
  if (Object.keys(p).some(k => !['sender', 'to', 'subject', 'htmlContent', 'textContent', 'tags', 'headers'].includes(k))
    || !exact(p.sender, ['name', 'email']) || !email(p.sender.email) || !text(p.sender.name)
    || !Array.isArray(p.to) || p.to.length !== 1 || !exact(p.to[0], ['email', 'name']) || !email(p.to[0].email)
    || typeof p.to[0].name !== 'string' || typeof p.subject !== 'string' || typeof p.textContent !== 'string'
    || p.htmlContent !== brevoHtml(p.textContent) || (p.tags != null && (!Array.isArray(p.tags) || p.tags.some(t => !text(t))))
    || (p.headers != null && (!exact(p.headers, ['Idempotency-Key']) || !text(p.headers['Idempotency-Key'])))) fail('anchor_recovery_payload_invalid');
  return [email(p.to[0].email)];
}
function assertActualPayload(command) {
  const recipients = actualRecipients(command), p = command.actualPayload;
  if (command.transport === 'smtp') return recipients;
  if (recipients[0] !== email(command.toEmail) || p.subject !== command.subject || p.textContent !== command.body
    || email(p.sender.email) !== email(command.sender?.email) || p.sender.name !== command.sender?.name
    || p.headers?.['Idempotency-Key'] !== command.idempotencyKey) fail('anchor_recovery_payload_changed');
  return recipients;
}
async function assertCrmEmailQuarantine(db, { identities, command, actualPayload, tenantId, itemId } = {}) {
  if (!command) command = { actualPayload };
  const recipients = assertActualPayload(command);
  if (!Array.isArray(identities) || !identities.length || identities.length !== recipients.length
    || new Set(recipients).size !== recipients.length) fail('anchor_recovery_identity_mapping_invalid');
  const used = new Set();
  for (const identity of identities) {
    const tenant = String(identity?.clientId ?? tenantId ?? '');
    if (!text(identity?.prospectId) || tenant !== '10') fail('anchor_recovery_identity_mapping_invalid');
    const located = await one(db, `SELECT p.id,p.client_id,p.company_id,p.email,p.do_not_contact,p.is_synthetic,p.ao_outreach_review_required
      FROM prospects p WHERE p.id::text=$1 AND p.client_id=$2`, [identity.prospectId, Number(tenant)], 'anchor_recovery_identity_mapping_invalid');
    const locatedRecipient = email(located.email);
    if (!locatedRecipient || !text(String(located.company_id || ''))) fail('anchor_recovery_identity_mapping_invalid');
    for (const key of quarantine.identityLockKeys({ tenantId: tenant, prospectId: identity.prospectId,
      companyId: String(located.company_id), recipientEmail: locatedRecipient })) await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [key]);
    const prospect = await one(db, `SELECT p.id,p.client_id,p.company_id,p.email,p.do_not_contact,p.is_synthetic,p.ao_outreach_review_required
      FROM prospects p WHERE p.id::text=$1 AND p.client_id=$2 FOR SHARE`, [identity.prospectId, Number(tenant)], 'anchor_recovery_identity_mapping_invalid');
    if (String(prospect.company_id) !== String(located.company_id) || email(prospect.email) !== locatedRecipient) fail('anchor_recovery_identity_mapping_invalid');
    const recipient = email(prospect.email);
    if (String(prospect.id) !== identity.prospectId || String(prospect.client_id) !== tenant || !text(String(prospect.company_id || ''))
      || !recipient || !recipients.includes(recipient) || used.has(recipient)) fail('anchor_recovery_payload_changed');
    used.add(recipient);
    if (prospect.ao_outreach_review_required !== false) fail('outreach_review_required');
    if (prospect.do_not_contact !== false || prospect.is_synthetic !== false) fail('anchor_recovery_contact_prohibited');
    await quarantine.assertNotQuarantined(db, { tenantId: tenant, prospectId: identity.prospectId,
      companyId: String(prospect.company_id), email: recipient, itemId, lockCompanyRows: true });
  }
  return { recipients };
}
module.exports = { TOP_KEYS, IDENTITY_KEYS, OPERATIONS, validateScopedResumeManifest,
  assertScopedResumeCapability, inspectScopedResumeCapabilities, buildScopedBinding, readScopedIdentity,
  assertScopedFirstTouch, identityFingerprint, sourceFingerprint: identityFingerprint, assertIdentityReview, assertCrmEmailQuarantine,
  assertActualPayload, actualRecipients, brevoHtml, deepFreeze, timestamp };
