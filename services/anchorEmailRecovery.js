'use strict';

// SPEC-261: no startup DDL, policy backfill, environment-only grant, or provider
// dependencies. Callers own the transaction and keep its locks through handoff.
const { hash, clock, windowReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { createHash } = require('node:crypto');
const { validateCanonicalSenderConfig } = require('../utils/canonicalSenderIdentity');
const { governedContactReason } = require('../utils/governedContactEligibility');

const RECOVERY_LOCK_KEY = 'anchor-email-recovery:10';
const MAX_MANIFEST_WINDOW_MS = 24 * 60 * 60 * 1000;
const MANIFEST_KEYS = ['version', 'id', 'approvalReference', 'tenantId', 'channel', 'programId', 'policyHash',
  'senderEmail', 'provider', 'transport', 'inboxIntegrationId', 'validFrom', 'expiresAt', 'maxFreshSends',
  'quarantineDecisionHash', 'admissions'];
const ADMISSION_KEYS = ['itemId', 'envelopeId', 'prospectId', 'companyId', 'candidateId', 'recipientEmail',
  'revision', 'envelopeApprovalId', 'manifestHash', 'subject', 'body'];
const POLICY_KEYS = ['tenantId', 'sourceMissionId', 'senderEmail', 'inboxIntegrationId', 'aoOwnerIds', 'startsAt',
  'expiresAt', 'dailyCap', 'totalCap', 'spacingMinutes', 'timeZone', 'weekdays', 'startHour', 'endHour',
  'maxSequenceStep', 'enrichmentLimit', 'preparationAttemptsPerDay',
  'allowedContactClassifications', 'operatorDelegatedMaximumDailyCapacity', 'preparationOnly'];

function reject(code, reason) {
  throw Object.assign(new Error(reason || code), { code, reason: reason || code, status: 409, providerBoundaryCrossed: false });
}
function object(value) { return value !== null && typeof value === 'object' && !Array.isArray(value); }
function text(value, max = 512) {
  return typeof value === 'string' && value.length > 0 && value.length <= max
    && value === value.trim() && !/[\u0000-\u001f\u007f]/.test(value);
}
function email(value) {
  if (typeof value !== 'string' || value.length > 254) return null;
  const normalized = value.trim().toLowerCase();
  return /^[a-z0-9.!#$%&'*+\-/=?^_`{|}~]+@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/.test(normalized)
    && !normalized.startsWith('.') && !normalized.includes('..') && !normalized.includes('.@') ? normalized : null;
}
function sha256(value) { return typeof value === 'string' && /^[a-f0-9]{64}$/.test(value); }
function exactKeys(value, keys) {
  return object(value) && Object.keys(value).length === keys.length
    && keys.every(key => Object.hasOwn(value, key));
}
function timestamp(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) return NaN;
  // Date.parse alone accepts normalized impossible dates such as February 30.
  const [year, month, day] = value.slice(0, 10).split('-').map(Number);
  if (month < 1 || month > 12 || day < 1 || day > new Date(Date.UTC(year, month, 0)).getUTCDate()) return NaN;
  const [hour, minute, second] = value.slice(11, 19).split(':').map(Number);
  if (hour > 23 || minute > 59 || second > 59) return NaN;
  return Date.parse(value);
}
function deepFreeze(value) {
  if (value && typeof value === 'object') { Object.values(value).forEach(deepFreeze); Object.freeze(value); }
  return value;
}
function validateManifest(raw, { now = new Date() } = {}) {
  const invalid = reason => reject('anchor_recovery_invalid_manifest', reason);
  if (!exactKeys(raw, MANIFEST_KEYS)) invalid('manifest_fields_required');
  if (raw.version !== 1 || raw.tenantId !== '10' || raw.channel !== 'email' || raw.provider !== 'brevo' || raw.transport !== 'governed_brevo' || raw.maxFreshSends !== 1) invalid('manifest_scope_invalid');
  for (const key of ['id', 'approvalReference', 'programId', 'inboxIntegrationId']) {
    if (!text(raw[key])) invalid(`manifest_${key}_invalid`);
  }
  if (!sha256(raw.policyHash) || !sha256(raw.quarantineDecisionHash)) invalid('manifest_hash_invalid');
  if (!email(raw.senderEmail) || email(raw.senderEmail) !== raw.senderEmail) invalid('manifest_sender_invalid');
  const from = timestamp(raw.validFrom), until = timestamp(raw.expiresAt), at = +now;
  if (!Number.isFinite(at) || !Number.isFinite(from) || !Number.isFinite(until) || until <= from
    || until - from > MAX_MANIFEST_WINDOW_MS || at < from || at >= until) invalid('manifest_window_invalid');
  if (!Array.isArray(raw.admissions) || raw.admissions.length !== 1) invalid('manifest_admission_limit');
  for (const row of raw.admissions) {
    if (!exactKeys(row, ADMISSION_KEYS)) invalid('admission_fields_required');
    for (const key of ['itemId', 'envelopeId', 'prospectId', 'companyId', 'candidateId', 'revision', 'envelopeApprovalId']) {
      if (!text(row[key])) invalid(`admission_${key}_invalid`);
    }
    if (!email(row.recipientEmail) || email(row.recipientEmail) !== row.recipientEmail) invalid('admission_recipient_invalid');
    if (!sha256(row.manifestHash)) invalid('admission_manifest_hash_invalid');
    if (!text(row.subject, 998) || typeof row.body !== 'string' || !row.body.trim() || row.body.length > 100000
      || /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(row.body)) invalid('admission_copy_invalid');
  }
  return deepFreeze(JSON.parse(JSON.stringify(raw)));
}

async function readState(db, { env = process.env, now = new Date(), lockControl = false } = {}) {
  try {
    const catalog = await db.query("SELECT to_regclass('public.anchor_email_recovery_control') AS control_table");
    if (catalog.rows?.length !== 1 || !Object.hasOwn(catalog.rows[0], 'control_table')) {
      return { mode: 'maintenance', reason: 'recovery_catalog_unavailable' };
    }
    if (catalog.rows[0].control_table === null) {
      const disabled = [undefined, '', 'false'].includes(env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE);
      const noManifest = [undefined, ''].includes(env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID)
        && [undefined, ''].includes(env.ANCHOR_EMAIL_SCOPED_RESUME_ID);
      return disabled && noManifest ? { mode: 'legacy' }
        : { mode: 'maintenance', reason: 'recovery_schema_missing' };
    }
    if (!catalog.rows[0].control_table) return { mode: 'maintenance', reason: 'recovery_catalog_invalid' };
    const result = await db.query(`SELECT singleton, maintenance_armed, manifest FROM anchor_email_recovery_control${lockControl ? ' FOR SHARE' : ''}`);
    const row = result.rows?.[0];
    if (result.rows?.length !== 1 || row?.singleton !== true || row?.maintenance_armed !== true) {
      return { mode: 'maintenance', reason: 'recovery_control_invalid' };
    }
    if (![undefined, '', 'false'].includes(env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE)) {
      return { mode: 'maintenance', reason: 'recovery_kill_switch' };
    }
    if (row.manifest == null) return { mode: 'maintenance', reason: 'recovery_manifest_missing' };
    if (row.manifest?.kind === 'scoped_resume') {
      const manifest = require('./anchorScopedResume').validateScopedResumeManifest(row.manifest, { now });
      if (env.ANCHOR_EMAIL_SCOPED_RESUME_ID !== manifest.id
        || ![undefined, ''].includes(env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID)) {
        return { mode: 'maintenance', reason: 'recovery_resume_not_activated' };
      }
      await require('./anchorScopedResumeControl').assertInstalledScopedResume(db, manifest, { now, lock: lockControl });
      return { mode: 'scoped_resume', manifest };
    }
    if (![undefined, ''].includes(env.ANCHOR_EMAIL_SCOPED_RESUME_ID)) {
      return { mode: 'maintenance', reason: 'recovery_activation_conflict' };
    }
    const manifest = validateManifest(row.manifest, { now });
    if (env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID !== manifest.id) {
      return { mode: 'maintenance', reason: 'recovery_manifest_not_activated' };
    }
    return { mode: 'active', manifest };
  } catch (error) {
    // A catalog/permission/schema/transport error never silently restores legacy.
    return { mode: 'maintenance', reason: ['anchor_recovery_invalid_manifest', 'anchor_scoped_resume_invalid_manifest'].includes(error.code)
      ? error.reason : 'recovery_state_unavailable' };
  }
}
async function loadRecoveryState(db, options = {}) { return readState(db, options); }
async function lockedState(db, options) {
  await db.query('SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))', [RECOVERY_LOCK_KEY]);
  const state = await readState(db, { ...options, lockControl: true });
  if (state.mode === 'maintenance') reject('anchor_recovery_maintenance', state.reason);
  return state;
}
function requireEqual(actual, expected, reason) {
  if (actual !== expected) reject('anchor_recovery_binding_changed', reason);
}
async function one(db, sql, values, reason) {
  const result = await db.query(sql, values);
  if (result.rows?.length !== 1) reject('anchor_recovery_binding_changed', reason);
  return result.rows[0];
}
function validateLivePolicy(program, manifest, now) {
  const p = program.policy;
  if (!object(p) || Object.keys(p).some(key => !POLICY_KEYS.includes(key))) reject('anchor_recovery_policy_invalid');
  for (const [actual, expected] of [[program.tenant_id, '10'], [program.mode, 'active'], [program.id, manifest.programId],
    [program.policy_hash, manifest.policyHash], [hash(p), manifest.policyHash], [p.tenantId, '10'],
    [p.senderEmail, manifest.senderEmail],
    [p.inboxIntegrationId, manifest.inboxIntegrationId], [p.sourceMissionId, program.source_mission_id]]) {
    requireEqual(actual, expected, 'recovery_program_changed');
  }
  if (!text(program.authorized_by) || !text(p.sourceMissionId) || p.preparationOnly === true
    || (Object.hasOwn(p, 'preparationOnly') && p.preparationOnly !== false)
    || p.maxSequenceStep !== 1 || p.enrichmentLimit !== 15 || p.preparationAttemptsPerDay !== 3
    || p.timeZone !== 'America/New_York' || !Array.isArray(p.weekdays) || hash(p.weekdays) !== hash([1, 2, 3, 4, 5])
    || p.startHour !== 9 || p.endHour !== 17
    || !Array.isArray(p.aoOwnerIds) || !p.aoOwnerIds.length || p.aoOwnerIds.length > 10
    || new Set(p.aoOwnerIds).size !== p.aoOwnerIds.length || p.aoOwnerIds.some(id => !Number.isSafeInteger(id) || id < 1)
    || !Number.isSafeInteger(p.dailyCap) || p.dailyCap < 1 || p.dailyCap > 50
    || !Number.isSafeInteger(p.totalCap) || p.totalCap < p.dailyCap || p.totalCap > 100
    || !Number.isSafeInteger(p.spacingMinutes) || p.spacingMinutes < 60 || p.spacingMinutes > 240) {
    reject('anchor_recovery_policy_invalid');
  }
  if (Object.hasOwn(p, 'operatorDelegatedMaximumDailyCapacity')
    && (!Number.isSafeInteger(p.operatorDelegatedMaximumDailyCapacity) || p.operatorDelegatedMaximumDailyCapacity < 1
      || p.operatorDelegatedMaximumDailyCapacity > 50 || p.operatorDelegatedMaximumDailyCapacity > p.totalCap)) reject('anchor_recovery_policy_invalid');
  if (Object.hasOwn(p, 'allowedContactClassifications') && (!Array.isArray(p.allowedContactClassifications)
    || !p.allowedContactClassifications.length || p.allowedContactClassifications.some(x => !['VERIFIED_FOUNDER_EMAIL', 'VERIFIED_ROLE_EMAIL'].includes(x)))) reject('anchor_recovery_policy_invalid');
  const from = timestamp(p.startsAt), until = timestamp(p.expiresAt);
  if (!Number.isFinite(from) || !Number.isFinite(until) || until <= from || until - from > 30 * 86400000
    || timestamp(manifest.validFrom) < from || timestamp(manifest.expiresAt) > until || windowReason(p, now, true)) {
    reject('anchor_recovery_policy_invalid');
  }
}

async function readBindings(db, state, context, { now = new Date(), stage = 'leaf' } = {}) {
  const manifest = state.manifest;
  if (!object(context) || !['programId', 'envelopeId', 'itemId'].every(k => text(context[k]))) reject('anchor_recovery_context_required');
  const admission = manifest.admissions.find(a => a.itemId === context.itemId && a.envelopeId === context.envelopeId);
  if (!admission || context.programId !== manifest.programId) reject('anchor_recovery_admission_missing');
  const quarantine = require('./anchorEmailQuarantine');
  await quarantine.requireQuarantine(db, manifest.quarantineDecisionHash);

  // This order is shared with quarantine application. Never take scheduler or
  // tenant executor locks here: those are already held outside the leaf.
  const keys = [`max:call:10:${admission.prospectId}`, `anchor-email-recovery:recipient:10:${admission.recipientEmail}`,
    `anchor-email-recovery:company:10:${admission.companyId}`].sort();
  for (const key of keys) await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [key]);
  const program = await one(db, 'SELECT * FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2 FOR SHARE',
    [context.programId, '10'], 'recovery_program_missing');
  validateLivePolicy(program, manifest, now);
  const envelope = await one(db, 'SELECT * FROM acquisition_outbound_envelopes WHERE id=$1 AND tenant_id=$2 AND program_id=$3 FOR SHARE',
    [context.envelopeId, '10', program.id], 'recovery_envelope_missing');
  for (const [actual, expected] of [[envelope.id, admission.envelopeId], [envelope.tenant_id, '10'],
    [envelope.program_id, program.id], [envelope.status, 'authorized'], [envelope.revision, admission.revision],
    [envelope.approval_id, admission.envelopeApprovalId], [envelope.manifest_hash, admission.manifestHash]]) {
    requireEqual(actual, expected, 'recovery_envelope_changed');
  }
  const localDay = envelope.local_day instanceof Date ? envelope.local_day.toISOString().slice(0, 10) : envelope.local_day;
  if (!text(envelope.mission_id) || localDay !== clock(now).day || !Array.isArray(envelope.manifest)
    || hash(envelope.manifest) !== admission.manifestHash) reject('anchor_recovery_envelope_invalid');
  const approval = await one(db, `SELECT id, tenant_id, mission_id, specialist, kind, payload
    FROM acquisition_mission_contributions WHERE id=$1 AND tenant_id=$2 AND mission_id=$3 FOR SHARE`,
  [admission.envelopeApprovalId, '10', envelope.mission_id], 'recovery_approval_missing');
  const contribution = approval.payload;
  const expectedApproval = { id: envelope.id, programId: program.id, policyHash: program.policy_hash,
    manifestHash: envelope.manifest_hash, localDay, candidateIds: envelope.manifest.map(row => row.candidateId) };
  if (approval.id !== admission.envelopeApprovalId || approval.tenant_id !== '10' || approval.mission_id !== envelope.mission_id
    || approval.specialist !== 'operator' || approval.kind !== 'approval' || !object(contribution) || contribution.specialist !== 'operator' || contribution.kind !== 'approval'
    || !object(contribution.payload) || contribution.payload.invalidated === true || contribution.payload.superseded === true
    || contribution.payload.decisionKind !== 'execution_approval'
    || contribution.id !== approval.id || contribution.missionId !== envelope.mission_id
    || contribution.payload.approved !== true || contribution.payload.preparedArtifactRevision !== admission.revision
    || !object(contribution.payload.dailyEnvelope) || hash(contribution.payload.dailyEnvelope) !== hash(expectedApproval)) {
    reject('anchor_recovery_approval_changed');
  }
  const item = await one(db, `SELECT i.*, ((i.attempted_at AT TIME ZONE 'UTC')::text || '+00') AS attempt_token FROM acquisition_outbound_items i
    WHERE i.id=$1 AND i.tenant_id=$2 AND i.envelope_id=$3 FOR SHARE`, [context.itemId, '10', envelope.id], 'recovery_item_missing');
  for (const [actual, expected] of [[item.id, admission.itemId], [item.tenant_id, '10'], [item.envelope_id, envelope.id],
    [item.prospect_id, admission.prospectId], [item.company_id, admission.companyId], [item.candidate_id, admission.candidateId],
    [item.email, admission.recipientEmail], [item.status, stage === 'leaf' ? 'attempted' : 'pending']]) {
    requireEqual(actual, expected, 'recovery_item_changed');
  }
  if (item.provider_message_id != null || (stage === 'leaf' ? !text(item.attempt_token) : item.attempt_token != null)) reject('anchor_recovery_attempt_invalid');
  const snapshot = item.snapshot;
  const frozen = envelope.manifest.filter(entry => entry?.candidateId === admission.candidateId);
  if (!object(snapshot) || frozen.length !== 1 || hash(snapshot) !== hash(frozen[0])) reject('anchor_recovery_snapshot_changed');
  for (const [actual, expected] of [[snapshot.candidateId, admission.candidateId], [snapshot.prospectId, admission.prospectId],
    [snapshot.companyId, admission.companyId], [snapshot.email, admission.recipientEmail], [snapshot.revision, admission.revision],
    [snapshot.message?.subject, admission.subject], [snapshot.message?.body, admission.body],
    [snapshot.sender?.senderEmail, manifest.senderEmail]]) requireEqual(actual, expected, 'recovery_snapshot_changed');
  if (!text(snapshot.sender?.senderName)) reject('anchor_recovery_sender_invalid');

  // The review column must exist and be an actual false boolean. Do not use
  // COALESCE/to_jsonb to silently turn a missing review schema into permission.
  // The call-only flag is deliberately absent from this scoped email SELECT.
  const prospect = await one(db, `SELECT p.id, p.client_id, p.company_id, p.email,
    p.do_not_contact, p.is_synthetic, p.ao_outreach_review_required,
    to_jsonb(p)->'email_verified' AS email_verified, to_jsonb(p)->>'email_status' AS email_status,
    to_jsonb(p)->'enrichment_provenance' AS enrichment_provenance,
    to_jsonb(p)->'acquisition_metadata' AS acquisition_metadata,
    to_jsonb(p)->>'contact_classification' AS contact_classification,
    to_jsonb(c)->>'domain' AS company_domain, to_jsonb(c)->>'website' AS company_website
    FROM prospects p JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.id::text=$1 AND p.client_id=$2 AND c.id::text=$3 FOR SHARE OF p,c`,
  [admission.prospectId, 10, admission.companyId], 'recovery_prospect_missing');
  if (String(prospect.id) !== admission.prospectId || String(prospect.client_id) !== '10'
    || String(prospect.company_id) !== admission.companyId || email(prospect.email) !== admission.recipientEmail) reject('anchor_recovery_crm_changed');
  if (prospect.ao_outreach_review_required !== false) reject('outreach_review_required');
  if (prospect.do_not_contact !== false || prospect.is_synthetic !== false) reject('anchor_recovery_contact_prohibited');
  const contactReason = governedContactReason(prospect, program.policy);
  if (contactReason) reject('anchor_recovery_contact_prohibited', contactReason);
  await quarantine.assertNotQuarantined(db, { tenantId: '10', prospectId: admission.prospectId,
    companyId: admission.companyId, email: admission.recipientEmail, itemId: item.id, lockCompanyRows: true });

  // The legacy readiness checks remain outside this leaf. These immutable
  // bindings additionally prevent an otherwise-live mailbox/identity swap.
  const client = await one(db, 'SELECT id, active, autosend_enabled, sender_email, sender_name, sending_domain FROM clients WHERE id=$1 FOR SHARE', [10], 'recovery_client_missing');
  if (String(client.id) !== '10' || client.active !== true || client.autosend_enabled !== false
    || email(client.sender_email) !== manifest.senderEmail || client.sender_name !== snapshot.sender.senderName) reject('anchor_recovery_sender_changed');
  if (!validateCanonicalSenderConfig({ tenantId: '10', clientId: 10, senderEmail: client.sender_email,
    senderName: client.sender_name, sendingDomain: client.sending_domain }).ok
    || snapshot.sender.sendingDomain !== client.sending_domain) reject('anchor_recovery_sender_changed');
  const integration = await one(db, `SELECT id, tenant_id, status, mailbox_address FROM tenant_mailbox_integrations
    WHERE id=$1 AND tenant_id=$2 FOR SHARE`, [manifest.inboxIntegrationId, '10'], 'recovery_inbox_missing');
  if (integration.id !== manifest.inboxIntegrationId || integration.tenant_id !== '10' || integration.status !== 'active'
    || email(integration.mailbox_address) !== manifest.senderEmail) reject('anchor_recovery_sender_changed');
  return { legacy: false, manifest, admission, prospect, program, envelope, item };
}

async function assertRecoveryAdmission(db, { program, item, envelope, stage = 'claim' } = {}, options = {}) {
  const state = await lockedState(db, options);
  if (state.mode === 'legacy') return { legacy: true };
  if (!['claim', 'preparation'].includes(stage)) reject('anchor_recovery_stage_invalid');
  if (program?.tenant_id !== '10' || item?.tenant_id !== '10') reject('anchor_recovery_scope_invalid');
  const reader = state.mode === 'scoped_resume' ? require('./anchorScopedResume').buildScopedBinding : readBindings;
  const bound = await reader(db, state, { programId: program.id, envelopeId: envelope?.id || item.envelope_id, itemId: item.id }, { ...options, stage });
  const reservations = bound.resumeGrant
    ? await db.query('SELECT * FROM anchor_email_recovery_reservations WHERE item_id=$1', [bound.item.id])
    : await db.query('SELECT * FROM anchor_email_recovery_reservations WHERE manifest_id=$1 OR item_id=$2', [bound.manifest.id, bound.item.id]);
  if (reservations.rows.length !== 0) reject('anchor_recovery_admission_consumed');
  return bound;
}
async function assertRecoveryLeaf(db, { channel, identities, context, command } = {}, options = {}) {
  const state = await lockedState(db, options);
  if (state.mode === 'legacy') return { legacy: true };
  if (channel !== 'email' || !Array.isArray(identities) || identities.length !== 1
    || ![10, '10'].includes(identities[0]?.clientId) || !text(identities[0]?.prospectId)) reject('anchor_recovery_scope_invalid');
  if (!object(command) || !email(command.toEmail) || !object(command.sender)
    || ['to', 'toEmails', 'cc', 'bcc', 'recipients'].some(key => command[key] != null)) reject('anchor_recovery_payload_invalid');
  if (command.credentialOverride != null && command.credentialOverride !== false) reject('anchor_recovery_credential_override');
  if (context?.provider !== 'brevo' || context?.transport !== 'governed_brevo') reject('anchor_recovery_transport_invalid');
  const reader = state.mode === 'scoped_resume' ? require('./anchorScopedResume').buildScopedBinding : readBindings;
  if (state.mode === 'scoped_resume') require('./anchorScopedResume').assertActualPayload(command);
  const bound = await reader(db, state, context, { ...options, stage: 'leaf' });
  const { manifest, admission, item, envelope } = bound;
  if (identities[0].prospectId !== admission.prospectId || email(command.toEmail) !== admission.recipientEmail
    || command.subject !== admission.subject || command.body !== admission.body
    || email(command.sender.email) !== manifest.senderEmail || command.sender.name !== item.snapshot.sender.senderName) reject('anchor_recovery_payload_changed');
  // Identical to canonical OutboundExecution. Keep this tiny derivation local:
  // importing that runtime bootstraps unrelated provider/database modules.
  const expectedKey = `exec_${createHash('sha256').update([envelope.mission_id, item.candidate_id, envelope.revision].join(':')).digest('hex').slice(0, 32)}`;
  if (command.idempotencyKey !== expectedKey) reject('anchor_recovery_idempotency_changed');
  const reservations = bound.resumeGrant
    ? await db.query('SELECT * FROM anchor_email_recovery_reservations WHERE item_id=$1', [item.id])
    : await db.query('SELECT * FROM anchor_email_recovery_reservations WHERE manifest_id=$1 OR item_id=$2', [manifest.id, item.id]);
  const reservation = reservations.rows?.[0];
  if (reservations.rows?.length !== 1 || reservation.manifest_id !== manifest.id || reservation.item_id !== item.id
    || reservation.attempt_token !== item.attempt_token || reservation.prospect_id !== admission.prospectId
    || reservation.company_id !== admission.companyId || reservation.recipient_email !== admission.recipientEmail
    || !reservation.used_at || reservation.dispatch_started_at !== null) reject('anchor_recovery_reservation_invalid');
  const quarantine = require('./anchorEmailQuarantine');
  const unresolved = await quarantine.listUnresolved(db, '10');
  const quarantines = (await db.query('SELECT * FROM anchor_email_quarantines WHERE tenant_id=$1', ['10'])).rows;
  const blockers = quarantine.classifyBlockingRows(unresolved, quarantines, { ownAttempt: {
    tenantId: '10', programId: bound.program.id, envelopeId: envelope.id, itemId: item.id, attemptToken: item.attempt_token,
  } });
  if (blockers.blockingCount !== 0 || blockers.ownAttemptCount !== 1) reject('anchor_recovery_unresolved_attempts');
  if (bound.resumeGrant) {
    // Reuse the current governed predicate after claim, under this transaction's
    // identity and prospect/company locks. This catches committed late AO,
    // reply, lifecycle and human-ownership changes without inventing policy.
    // Existing observation triggers do not all take these advisory locks: this
    // snapshot check does not serialize every concurrent absent-row insertion.
    const { GovernedOutboundStore } = require('./governedOutboundStore');
    const suppression = await new GovernedOutboundStore({ query: (sql, values) => db.query(sql, values) }, '10')
      .suppression(item.snapshot, envelope.mission_id, item.id);
    if (suppression) reject('anchor_scoped_resume_live_suppression', suppression);
  }
  // Locks may have waited across the manifest/program deadline. Never dispatch
  // on the time sampled before acquiring those locks.
  const finalNow = options.now || new Date();
  const scoped = Boolean(bound.resumeGrant);
  (scoped ? require('./anchorScopedResume').validateScopedResumeManifest : validateManifest)(manifest, { now: finalNow });
  validateLivePolicy(bound.program, manifest, finalNow);
  const env = options.env || process.env;
  if (env[scoped ? 'ANCHOR_EMAIL_SCOPED_RESUME_ID' : 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID'] !== manifest.id
    || ![undefined, '', 'false'].includes(env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE)) reject('anchor_recovery_maintenance', 'recovery_manifest_not_activated');
  if (scoped && clock(finalNow).day !== clock(new Date(reservation.used_at)).day) reject('anchor_recovery_dispatch_day_changed');
  return { legacy: false, prospect: bound.prospect, program: bound.program, manifest, item,
    ...(scoped ? { resumeGrant: bound.resumeGrant, envelope, reservation } : {}), attemptToken: item.attempt_token };
}

module.exports = { RECOVERY_LOCK_KEY, MAX_MANIFEST_WINDOW_MS, validateManifest, validateLivePolicy, loadRecoveryState,
  assertRecoveryAdmission, assertRecoveryLeaf, readBindings };
