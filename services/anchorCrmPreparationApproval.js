'use strict';
// Same-origin adapters only. No provider, draft runner, settings or direct review writes.
// Explicit exact review confirmation delegates to the existing guarded installer.
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const scope = require('./anchorScopedResume');
const control = require('./anchorScopedResumeControl');
const authority = require('./anchorCrmPreparationAuthority');
const identity = require('./anchorCrmPreparationIdentity');
const { clientBinding } = require('./identityTenantSelection');
const text = v => typeof v === 'string' && v.length > 0 && v.length <= 512 && v.trim() === v && !/[\x00-\x1f\x7f]/.test(v);
const exact = (v, keys) => v && typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === keys.length && keys.every(k => Object.hasOwn(v, k));
const digest = v => typeof v === 'string' && /^[a-f0-9]{64}$/.test(v);
function fail(code = 'anchor_crm_approval_rejected') { throw Object.assign(new Error(code), { code, retryAllowed: false }); }
function revision(input, actor, env) {
  if (!/^[a-f0-9]{40}$/.test(input?.revision || '') || input.revision !== env.RAILWAY_GIT_COMMIT_SHA
    || !actor || actor.role !== 'admin' || !/^[1-9][0-9]*$/.test(actor.id || '')) fail();
}
const at = options => new Date(typeof options.now === 'function' ? options.now() : options.now || new Date());
async function read(pool, work) {
  let timer, expired = false, client, begun = false;
  const pending = pool.connect().then(c => { if (expired) { c.release(true); fail(); } return c; });
  try {
    client = await Promise.race([pending, new Promise((_, reject) => { timer = setTimeout(() => { expired = true; reject(new Error('connection_timeout')); }, 6000); })]);
    clearTimeout(timer);
    const db = { query: (text, values = []) => client.query({ text, values, query_timeout: 6000 }) };
    await db.query('BEGIN READ ONLY'); begun = true;
    await db.query("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='5s'");
    const result = await work(db);
    await db.query('COMMIT'); begun = false; return result;
  } finally { clearTimeout(timer); if (client) { if (begun) await client.query('ROLLBACK').catch(() => {}); client.release(true); } }
}
async function actorBinding(db, actor) {
  const users = (await db.query('SELECT client_id FROM users WHERE id::text=$1', [actor.id])).rows;
  if (users.length !== 1) fail();
  let account;
  try { account = clientBinding(users[0].client_id); } catch { fail(); }
  const binding = { operatorId: actor.id, operatorRole: 'admin', preparationAuthority: {
    approverAccountClientId: account, approverSessionHash: identity.sessionBindingHash(actor.sessionId) } };
  await identity.assertPreparationApprover(db, binding, { lock: false, actor });
  return binding;
}
function publicProposal(manifest) {
  const p = structuredClone(manifest); delete p.operatorId; delete p.operatorRole;
  delete p.preparationAuthority.approverAccountClientId; delete p.preparationAuthority.approverSessionHash;
  return p;
}
function bindProposal(proposal, binding, options) {
  if (!proposal || Object.hasOwn(proposal, 'operatorId') || Object.hasOwn(proposal, 'operatorRole')
    || !proposal.preparationAuthority || Object.hasOwn(proposal.preparationAuthority, 'approverAccountClientId')
    || Object.hasOwn(proposal.preparationAuthority, 'approverSessionHash')) fail();
  const manifest = { ...proposal, operatorId: binding.operatorId, operatorRole: 'admin',
    preparationAuthority: { ...proposal.preparationAuthority, ...binding.preparationAuthority } };
  if (!authority.isPreparationAuthority(manifest)) fail();
  return scope.validateScopedResumeManifest(manifest, { now: at(options) });
}
function validateIntent(i) {
  if (!exact(i, ['id','programId','sourceMissionId','validFrom','expiresAt','approvalReference','preparationApprovalReference','dailyOperationCap','dailyItemCap','admissions'])
    || ['id','programId','sourceMissionId','validFrom','expiresAt','approvalReference','preparationApprovalReference'].some(k => !text(i[k]))
    || !Number.isSafeInteger(i.dailyOperationCap) || !Number.isSafeInteger(i.dailyItemCap)
    || !Array.isArray(i.admissions) || !i.admissions.length || i.admissions.length > 5
    || i.admissions.some(a => !exact(a, ['prospectId','approvalReference']) || !text(a.prospectId) || !text(a.approvalReference))
    || new Set(i.admissions.map(a => a.prospectId)).size !== i.admissions.length) fail();
}
function validateReviewSelection(review, intent) {
  if (!exact(review, ['action','approvalReference','prospectIds']) || review.action !== 'initialize_null_review_false'
    || !text(review.approvalReference) || !Array.isArray(review.prospectIds) || !review.prospectIds.length
    || review.prospectIds.length > intent.admissions.length || new Set(review.prospectIds).size !== review.prospectIds.length
    || review.prospectIds.some(id => !text(id) || !intent.admissions.some(a => a.prospectId === id))) fail();
}
function reviewPlan(review, manifest) {
  if (review === undefined) return undefined;
  return { action: review.action, approvalReference: review.approvalReference,
    identities: [...review.prospectIds].sort((a, b) => a.localeCompare(b)).map(id => {
      const a = manifest.admissions.find(a => a.prospectId === id);
      return { prospectId: a.prospectId, companyId: a.companyId, recipientEmail: a.recipientEmail, identityFingerprint: a.identityFingerprint };
    }) };
}
const reviewAuditHash = (manifestHash, reviewInitializations) => hash({ kind: 'anchor_preparation_review_audit_v1', manifestHash, reviewInitializations });
const confirmationHash = (revision, manifestHash, reviewInitializations) => hash({ kind: 'anchor_preparation_review_confirmation_v1', revision, manifestHash, reviewInitializations });
function storedReviewAudit(grant) {
  const rows = grant.review_initializations;
  if (!Array.isArray(rows)) fail();
  if (!rows.length) return [];
  if (rows.some(r => !exact(r, ['prospectId','companyId','recipientEmail','identityFingerprint','approvalReference','beforeReview','afterReview'])
    || r.approvalReference !== rows[0].approvalReference || r.beforeReview !== null || r.afterReview !== false)) fail();
  const normalized = control.initializationPlan({ action: 'initialize_null_review_false', approvalReference: rows[0].approvalReference,
    identities: rows.map(({ prospectId, companyId, recipientEmail, identityFingerprint }) => ({ prospectId, companyId, recipientEmail, identityFingerprint })) }, grant.manifest);
  if (hash(rows) !== hash(normalized)) fail();
  return normalized;
}
async function deriveProposal(db, i, binding, options) {
  const programs = (await db.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2', [i.programId, '10'])).rows;
  const sources = (await db.query('SELECT payload FROM acquisition_missions WHERE id=$1 AND tenant_id=$2', [i.sourceMissionId, '10'])).rows;
  const clients = (await db.query('SELECT sender_email,sender_name,sending_domain FROM clients WHERE id=10')).rows;
  const quarantines = (await db.query('SELECT decision_hash FROM anchor_email_quarantines WHERE tenant_id=$1', ['10'])).rows;
  if (programs.length !== 1 || sources.length !== 1 || clients.length !== 1 || quarantines.length !== 1) fail();
  const program = programs[0], source = sources[0].payload, client = clients[0];
  const totals = (await db.query(`SELECT count(*)::int AS total_operations,coalesce(sum(item_count),0)::int AS total_items
    FROM anchor_scoped_resume_consumption WHERE tenant_id='10' AND program_id=$1 AND capability='preparation'`, [i.programId])).rows[0];
  const admissions = [];
  for (const admission of i.admissions) {
    const rows = (await db.query('SELECT company_id,email FROM prospects WHERE id::text=$1 AND client_id=10', [admission.prospectId])).rows;
    if (rows.length !== 1 || rows[0].company_id == null) fail();
    const input = { tenantId: '10', prospectId: admission.prospectId, companyId: String(rows[0].company_id), recipientEmail: rows[0].email };
    const bound = await scope.readScopedIdentity(db, input, { lock: false });
    admissions.push({ prospectId: input.prospectId, companyId: input.companyId, recipientEmail: input.recipientEmail,
      companyNameKey: bound.companyNameKey, companyDomain: bound.companyDomain, identityFingerprint: bound.identityFingerprint, approvalReference: admission.approvalReference });
  }
  return bindProposal({ version: 2, kind: 'scoped_resume', id: i.id, tenantId: '10', approvalReference: i.approvalReference,
    programId: i.programId, sourceMissionId: i.sourceMissionId, policyHash: program.policy_hash, scopeHash: hash(missionScope(source)),
    validFrom: i.validFrom, expiresAt: i.expiresAt, quarantineDecisionHash: quarantines[0].decision_hash,
    senderEmail: client.sender_email, senderName: client.sender_name, sendingDomain: client.sending_domain, inboxIntegrationId: program.policy?.inboxIntegrationId,
    reviewSchemaPolicy: 'nullable_no_default_preserve_existing', admissionPolicy: 'explicit_reviewed_identity_batch', admissions,
    scopes: { research: { enabled: false }, email: { enabled: false }, preparation: { enabled: true, approvalReference: i.preparationApprovalReference,
      dailyCap: i.dailyOperationCap, totalCap: totals.total_operations + 1, dailyItemCap: i.dailyItemCap, totalItemCap: totals.total_items + admissions.length,
      operations: ['prepare_existing_crm'] } }, sms: 'closed', calls: 'existing_policy', manualCrmEmail: 'closed', nonCrmVerification: 'closed', otherPrograms: 'legacy_unchanged',
    preparationAuthority: { kind: 'existing_crm_continuation', programBindingHash: authority.programBindingHash(program), sourceMissionHash: hash(source),
      originalAuthorizerId: String(program.authorized_by), consumedOperations: totals.total_operations, consumedItems: totals.total_items } }, binding, options);
}
async function readPreparationReceipt(pool, input, actor, options) {
  if (!exact(input.receipt, ['manifestId','manifestHash']) || !text(input.receipt.manifestId) || !digest(input.receipt.manifestHash)) fail();
  return read(pool, async db => {
    await actorBinding(db, actor);
    // One statement gives grant/control/ledger a single PostgreSQL snapshot.
    const rows = (await db.query(`SELECT g.manifest,g.manifest_hash,g.operator_id,g.operator_role,g.revoked_at,g.review_initializations,
      c.manifest AS current_control_manifest,c.maintenance_armed,
      (SELECT count(*)::int FROM anchor_scoped_resume_consumption x WHERE x.tenant_id='10' AND x.manifest_id=g.manifest_id AND x.capability='preparation') AS total_operations,
      (SELECT coalesce(sum(item_count),0)::int FROM anchor_scoped_resume_consumption x WHERE x.tenant_id='10' AND x.manifest_id=g.manifest_id AND x.capability='preparation') AS total_items
      FROM anchor_scoped_resume_grants g LEFT JOIN anchor_email_recovery_control c ON c.singleton=true
      WHERE g.manifest_id=$1 AND g.tenant_id=$2`, [input.receipt.manifestId, '10'])).rows;
    if (!rows.length) return { status: 'not_installed', recorded: false, installed: false, manifestId: input.receipt.manifestId,
      manifestHash: input.receipt.manifestHash, reviewInitializations: [], reviewInitializationCount: 0,
      reviewInitializationHash: null, reviewAuditScope: 'no_recorded_grant', currentReviewState: 'not_checked',
      effectsScope: 'this_request_only', historicalPreparationOutcome: 'not_checked', preparationInvoked: false, sent: 0, providerCalls: 0, retryAllowed: false };
    const grant = rows[0], manifest = grant.manifest;
    if (rows.length !== 1 || !authority.isPreparationAuthority(manifest) || manifest.tenantId !== '10'
      || manifest.id !== input.receipt.manifestId || hash(manifest) !== grant.manifest_hash || grant.manifest_hash !== input.receipt.manifestHash
      || grant.operator_id !== manifest.operatorId || grant.operator_role !== manifest.operatorRole) fail();
    await identity.assertPreparationRevoker(db, manifest, actor, { lock: false });
    const reviewInitializations = storedReviewAudit(grant);
    const revoked = grant.revoked_at !== null;
    return { status: revoked ? 'revoked' : grant.total_operations ? 'consumed' : 'installed', recorded: true,
      installed: !revoked, revoked, expired: Date.parse(manifest.expiresAt) <= +at(options),
      manifestId: manifest.id, manifestHash: grant.manifest_hash, approverId: manifest.operatorId,
      currentControlMatches: hash(grant.current_control_manifest) === grant.manifest_hash, maintenanceArmed: grant.maintenance_armed === true,
      consumedOperations: grant.total_operations, consumedItems: grant.total_items,
      reviewInitializations, reviewInitializationCount: reviewInitializations.length,
      reviewInitializationHash: reviewAuditHash(grant.manifest_hash, reviewInitializations),
      reviewAuditScope: 'historical_committed_transition', currentReviewState: 'not_checked',
      effectsScope: 'this_request_only', historicalPreparationOutcome: 'not_checked', preparationAvailability: 'not_asserted', preparationInvoked: false, sent: 0, providerCalls: 0, retryAllowed: false };
  });
}
const BLOCKERS = new Set(['outreach_review_required', 'anchor_recovery_contact_prohibited', 'anchor_scoped_resume_identity_changed',
  'anchor_email_quarantined', 'anchor_quarantine_invalid_record', 'anchor_quarantine_required', 'anchor_scoped_resume_control_required',
  'anchor_scoped_resume_program_changed', 'anchor_crm_preparation_program_changed', 'anchor_scoped_resume_source_changed',
  'anchor_crm_preparation_source_changed', 'anchor_scoped_resume_sender_changed', 'anchor_scoped_resume_unresolved_attempts',
  'anchor_scoped_resume_install_conflict', 'anchor_scoped_resume_revoke_required', 'anchor_crm_preparation_baseline_changed',
  'anchor_crm_preparation_identity_consumed', 'anchor_crm_preparation_approver_invalid', 'anchor_scoped_resume_invalid_manifest',
  'anchor_scoped_resume_review_cas_failed']);
async function previewPreparationApproval(pool, input, actor, options = {}) {
  const env = options.env || process.env;
  revision(input, actor, env);
  if (exact(input, ['revision','receipt'])) return readPreparationReceipt(pool, input, actor, options);
  const reviewing = exact(input, ['revision','intent','review']);
  if (!reviewing && !exact(input, ['revision','intent'])) fail(); validateIntent(input.intent);
  if (reviewing) validateReviewSelection(input.review, input.intent);
  const manifest = await read(pool, async db => {
    const binding = await actorBinding(db, actor);
    const proposed = await deriveProposal(db, input.intent, binding, options);
    await identity.assertPreparationApprover(db, proposed, { lock: false, actor });
    return proposed;
  });
  const reviewInitialization = reviewing ? reviewPlan(input.review, manifest) : undefined;
  const reviewInitializations = control.initializationPlan(reviewInitialization, manifest);
  const blockers = [];
  let inspection;
  try { inspection = await control.previewScopedPreparation(pool, manifest, actor, { ...options, env, reviewInitialization }); }
  catch (error) { blockers.push(BLOCKERS.has(error.code) ? error.code : 'anchor_crm_approval_rejected'); }
  try { authority.assertPreparationEnvironment(manifest, { env }); } catch { blockers.push('preparation_settings_not_activated'); }
  return { status: blockers.length ? 'blocked' : inspection?.alreadyInstalled ? 'already_installed' : 'previewed', proposal: publicProposal(manifest),
    revision: input.revision, manifestHash: hash(manifest), sourceMissionHash: manifest.preparationAuthority.sourceMissionHash,
    programBindingHash: manifest.preparationAuthority.programBindingHash, identityBatchHash: hash(manifest.admissions),
    consumedOperations: manifest.preparationAuthority.consumedOperations, consumedItems: manifest.preparationAuthority.consumedItems,
    approverId: manifest.operatorId, approverAccountClientId: manifest.preparationAuthority.approverAccountClientId,
    ...(reviewing ? { reviewInitialization, reviewConfirmationHash: confirmationHash(input.revision, hash(manifest), reviewInitializations),
      reviewInitializationHash: reviewAuditHash(hash(manifest), reviewInitializations),
      reviewEffects: { action: 'initialize_null_review_false', scope: 'exact_selected_prospects_only',
        status: blockers.length ? 'blocked_not_applied' : inspection?.alreadyInstalled ? 'already_recorded' : 'proposed_not_applied',
        stateSemantics: 'expected_transition_not_current_state',
        identities: reviewInitializations.map(({ prospectId, beforeReview, afterReview }) => ({ prospectId, beforeReview, afterReview })),
        buyingIntent: 'not_asserted', canonicalEligibility: 'not_checked' } } : {}),
    blockers, installable: blockers.length === 0, installed: inspection ? inspection.alreadyInstalled === true : null,
    canonicalEligibility: 'not_checked', preparationInvoked: false, sent: 0, providerCalls: 0, retryAllowed: false };
}
async function installPreparationApproval(pool, input, actor, options = {}) {
  const env = options.env || process.env;
  const reviewing = exact(input, ['revision','proposal','manifestHash','reviewInitialization','reviewConfirmationHash']);
  if ((!reviewing && !exact(input, ['revision','proposal','manifestHash'])) || !digest(input.manifestHash)
    || (reviewing && (!digest(input.reviewConfirmationHash) || input.reviewInitialization === undefined))) fail(); revision(input, actor, env);
  const manifest = await read(pool, async db => bindProposal(input.proposal, await actorBinding(db, actor), options));
  if (hash(manifest) !== input.manifestHash) fail('anchor_crm_approval_preview_changed');
  const reviewInitializations = control.initializationPlan(input.reviewInitialization, manifest);
  if (reviewing && input.reviewConfirmationHash !== confirmationHash(input.revision, input.manifestHash, reviewInitializations)) fail('anchor_crm_approval_preview_changed');
  const result = await control.installScopedResume(pool, { manifest, approvalReference: manifest.approvalReference,
    ...(reviewing ? { reviewInitialization: input.reviewInitialization } : {}) }, actor, { ...options, env });
  return { status: result.idempotent ? 'already_installed' : 'installed', installed: true, idempotent: result.idempotent,
    freshApproval: !result.idempotent, manifestId: manifest.id, manifestHash: input.manifestHash, preparationInvoked: false,
    initialized: result.initialized, reviewInitializationCount: reviewInitializations.length,
    reviewInitializationHash: reviewAuditHash(input.manifestHash, reviewInitializations), currentReviewState: 'not_checked',
    preparationAvailability: 'not_asserted', sent: 0, providerCalls: 0, retryAllowed: false };
}
async function revokePreparationApproval(pool, input, actor, options = {}) {
  const env = options.env || process.env;
  if (!exact(input, ['revision','manifestId','manifestHash','approvalReference']) || !digest(input.manifestHash)
    || !text(input.manifestId) || !text(input.approvalReference)) fail(); revision(input, actor, env);
  const result = await control.revokeScopedResume(pool, { manifestId: input.manifestId, approvalReference: input.approvalReference }, actor,
    { ...options, env, preparationOnly: true, expectedManifestHash: input.manifestHash });
  return { status: result.idempotent ? 'already_revoked' : 'revoked', revoked: true, idempotent: result.idempotent,
    manifestId: input.manifestId, manifestHash: input.manifestHash, preparationInvoked: false, sent: 0, providerCalls: 0, retryAllowed: false };
}
module.exports = { previewPreparationApproval, installPreparationApproval, revokePreparationApproval };
