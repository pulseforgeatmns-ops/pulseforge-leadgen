'use strict';

// SPEC-262. Explicit operator APIs only. No providers, routes, startup DDL,
// source-derived admission, automatic activation or review-hold release.
const { hash, clock, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { RECOVERY_LOCK_KEY, validateLivePolicy } = require('./anchorEmailRecovery');
const quarantine = require('./anchorEmailQuarantine');
const { validateCanonicalSenderConfig } = require('../utils/canonicalSenderIdentity');
const scope = () => require('./anchorScopedResume');
const preparationAuthority = require('./anchorCrmPreparationAuthority');
const preparationIdentity = require('./anchorCrmPreparationIdentity');
const originalAuthorizer = manifest => preparationAuthority.isPreparationAuthority(manifest)
  ? manifest.preparationAuthority.originalAuthorizerId : manifest.operatorId;
function validateProgramAuthority(program, manifest, options) {
  if (preparationAuthority.isPreparationAuthority(manifest)) {
    preparationAuthority.assertPreparationEnvironment(manifest, options);
    preparationAuthority.validatePreparationProgram(program, manifest, now(options));
  } else validateLivePolicy(program, manifest, now(options));
}
async function assertPreparationIdentitiesUnconsumed(db, manifest) {
  // An explicitly regranted/subset batch cannot replay a partially prepared
  // identity. Keep the original program namespace and permanently charged rows.
  const rows = (await db.query(`SELECT identities FROM anchor_scoped_resume_consumption
    WHERE tenant_id='10' AND program_id=$1 AND capability='preparation' AND operation='prepare_existing_crm'`,
  [manifest.programId])).rows;
  const keys = ['prospectId', 'companyId', 'recipientEmail', 'companyNameKey', 'companyDomain'];
  for (const row of rows) {
    if (!Array.isArray(row.identities) || !row.identities.length
      || row.identities.some(identity => !exact(identity, scope().IDENTITY_KEYS)
        || scope().IDENTITY_KEYS.some(key => !text(identity[key])) || !/^[a-f0-9]{64}$/.test(identity.identityFingerprint)
        || identity.recipientEmail !== identity.recipientEmail.trim().toLowerCase()))
      fail('anchor_crm_preparation_consumed_identity_invalid');
    for (const identity of row.identities) {
      let company;
      try { company = quarantine.companyIdentity({ name: identity.companyNameKey, domain: identity.companyDomain }); }
      catch { fail('anchor_crm_preparation_consumed_identity_invalid'); }
      if (company.companyNameKey !== identity.companyNameKey || company.companyDomain !== identity.companyDomain)
        fail('anchor_crm_preparation_consumed_identity_invalid');
    }
    if (row.identities.some(prior => manifest.admissions.some(admission => keys.some(key => prior[key] === admission[key]))))
      fail('anchor_crm_preparation_identity_consumed');
  }
}
async function assertPreparationBaseline(db, manifest) {
  const baseline = manifest.preparationAuthority;
  const totals = await one(db, `SELECT count(*)::int AS total_operations,coalesce(sum(item_count),0)::int AS total_items
    FROM anchor_scoped_resume_consumption WHERE tenant_id='10' AND program_id=$1 AND capability='preparation'`,
  [manifest.programId], 'anchor_scoped_resume_consumption_unavailable');
  if (totals.total_operations !== baseline.consumedOperations || totals.total_items !== baseline.consumedItems)
    fail('anchor_crm_preparation_baseline_changed');
}
const text = value => typeof value === 'string' && value.length > 0 && value.length <= 512
  && value === value.trim() && !/[\x00-\x1f\x7f]/.test(value);
const exact = (value, keys) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
function fail(code) { throw Object.assign(new Error(code), { code, retryAllowed: false, providerBoundaryCrossed: false }); }
function now(options = {}) { return new Date(options.now instanceof Function ? options.now() : options.now || new Date()); }
async function one(db, sql, values, code) {
  const result = await db.query(sql, values);
  if (result.rows?.length !== 1) fail(code);
  return result.rows[0];
}
async function transaction(pool, work) {
  if (!pool || typeof pool.connect !== 'function') fail('anchor_scoped_resume_owned_transaction_required');
  let expired = false, timer, begun = false, discard = false;
  const pending = pool.connect().then(client => {
    if (expired) { client.release(true); fail('anchor_scoped_resume_connection_timeout'); }
    return client;
  });
  let client;
  try { client = await Promise.race([pending, new Promise((_, reject) => {
    timer = setTimeout(() => { expired = true; reject(Object.assign(new Error('anchor_scoped_resume_connection_timeout'), { code: 'anchor_scoped_resume_connection_timeout' })); }, 6000);
  })]); } finally { clearTimeout(timer); }
  const db = { query: (query, values = []) => client.query(typeof query === 'string'
    ? { text: query, values, query_timeout: 6000 } : { ...query, query_timeout: 6000 }) };
  try {
    await db.query('BEGIN'); begun = true;
    await db.query("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='5s'");
    await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [RECOVERY_LOCK_KEY]);
    const control = await one(db, 'SELECT * FROM anchor_email_recovery_control WHERE singleton=true FOR UPDATE', [], 'anchor_scoped_resume_control_required');
    if (control.singleton !== true || control.maintenance_armed !== true) fail('anchor_scoped_resume_control_required');
    const result = await work(db, control);
    await db.query('COMMIT'); begun = false;
    return result;
  } catch (error) { discard = true; throw error; }
  finally {
    if (begun) { try { await db.query('ROLLBACK'); } catch { discard = true; } }
    client.release(discard);
  }
}
async function currentActor(db, actor, expected, { lock = true, requireSession = false, sessionId } = {}) {
  if (preparationAuthority.isPreparationAuthority(expected)) {
    return preparationIdentity.assertPreparationApprover(db, expected, { lock, sessionId, ...(requireSession ? { actor } : {}) });
  }
  if (!actor || !text(String(actor.id || '')) || !['admin', 'manager'].includes(actor.role)
    || (expected && (String(actor.id) !== expected.operatorId || actor.role !== expected.operatorRole))) fail('anchor_scoped_resume_operator_invalid');
  const row = await one(db, `SELECT id,client_id,role,active FROM users WHERE id::text=$1 AND client_id=10${lock ? ' FOR SHARE' : ''}`,
    [String(actor.id)], 'anchor_scoped_resume_operator_invalid');
  if (String(row.id) !== String(actor.id) || String(row.client_id) !== '10' || row.active !== true || row.role !== actor.role) fail('anchor_scoped_resume_operator_invalid');
  return row;
}
async function liveBindings(db, manifest, options, previewOnly = false) {
  scope().validateScopedResumeManifest(manifest, { now: now(options) });
  await currentActor(db, { id: manifest.operatorId, role: manifest.operatorRole }, manifest, { sessionId: options.sessionId });
  const program = await one(db, 'SELECT * FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2 FOR SHARE',
    [manifest.programId, '10'], 'anchor_scoped_resume_program_missing');
  if (previewOnly && preparationAuthority.isPreparationAuthority(manifest)) preparationAuthority.validatePreparationProgram(program, manifest, now(options));
  else validateProgramAuthority(program, manifest, options);
  if (String(program.authorized_by) !== originalAuthorizer(manifest) || program.source_mission_id !== manifest.sourceMissionId
    || program.scope_hash !== manifest.scopeHash) fail('anchor_scoped_resume_program_changed');
  const source = await one(db, 'SELECT id,tenant_id,payload FROM acquisition_missions WHERE id=$1 AND tenant_id=$2 FOR SHARE',
    [manifest.sourceMissionId, '10'], 'anchor_scoped_resume_source_missing');
  if (source.id !== manifest.sourceMissionId || source.tenant_id !== '10' || source.payload?.id !== source.id
    || String(source.payload?.tenantId) !== '10' || source.payload?.structuredMission?.immutable !== true
    || source.payload.planCancelled || /cancel/i.test(String(source.payload.status))
    || hash(missionScope(source.payload)) !== manifest.scopeHash) fail('anchor_scoped_resume_source_changed');
  if (preparationAuthority.isPreparationAuthority(manifest)) preparationAuthority.assertPreparationSource(source.payload, manifest);
  const client = await one(db, 'SELECT id,active,autosend_enabled,sender_email,sender_name,sending_domain FROM clients WHERE id=$1 FOR SHARE', [10], 'anchor_scoped_resume_sender_changed');
  if (client.active !== true || client.autosend_enabled !== false || client.sender_email !== manifest.senderEmail
    || client.sender_name !== manifest.senderName || client.sending_domain !== manifest.sendingDomain
    || !validateCanonicalSenderConfig({ tenantId: '10', clientId: 10, senderEmail: client.sender_email,
      senderName: client.sender_name, sendingDomain: client.sending_domain }).ok) fail('anchor_scoped_resume_sender_changed');
  const inbox = await one(db, 'SELECT id,tenant_id,status,mailbox_address FROM tenant_mailbox_integrations WHERE id=$1 AND tenant_id=$2 FOR SHARE',
    [manifest.inboxIntegrationId, '10'], 'anchor_scoped_resume_sender_changed');
  if (inbox.status !== 'active' || inbox.mailbox_address !== manifest.senderEmail) fail('anchor_scoped_resume_sender_changed');
  await quarantine.requireQuarantine(db, manifest.quarantineDecisionHash);
  const unresolved = await quarantine.listUnresolved(db, '10');
  const quarantines = (await db.query('SELECT * FROM anchor_email_quarantines WHERE tenant_id=$1', ['10'])).rows;
  if (quarantine.classifyBlockingRows(unresolved, quarantines).blockingCount !== 0) fail('anchor_scoped_resume_unresolved_attempts');
  return program;
}
async function assertInstalledScopedResume(db, manifest, options = {}) {
  scope().validateScopedResumeManifest(manifest, { now: now(options) });
  const result = await db.query('SELECT manifest_id,manifest_hash,revoked_at,operator_id,operator_role FROM anchor_scoped_resume_grants WHERE manifest_id=$1 AND tenant_id=$2', [manifest.id, '10']);
  const installed = result.rows?.[0];
  if (result.rows?.length !== 1 || installed.manifest_id !== manifest.id || installed.manifest_hash !== hash(manifest)
    || installed.revoked_at !== null
    || (preparationAuthority.isPreparationAuthority(manifest)
      && (installed.operator_id !== manifest.operatorId || installed.operator_role !== manifest.operatorRole))) fail('anchor_scoped_resume_not_installed');
  await currentActor(db, { id: manifest.operatorId, role: manifest.operatorRole }, manifest, { lock: options.lock === true, sessionId: options.sessionId });
  const program = await one(db, `SELECT * FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2${options.lock === true ? ' FOR SHARE' : ''}`,
    [manifest.programId, '10'], 'anchor_scoped_resume_operator_invalid');
  if (String(program.authorized_by) !== originalAuthorizer(manifest)) fail('anchor_scoped_resume_operator_invalid');
  return installed;
}
async function assertScopedResumeProviderBudget() {
  // Existing Scout limits bound calls/items, not provider monetary exposure.
  // No durable provider-spend contract has been verified in this source tree.
  // A caller-supplied digest/callback/environment value cannot establish one.
  fail('anchor_scoped_resume_provider_budget_unverified');
}
function initializationPlan(input, manifest) {
  if (input === undefined) return [];
  if (!exact(input, ['action', 'approvalReference', 'identities']) || input.action !== 'initialize_null_review_false'
    || !text(input.approvalReference) || !Array.isArray(input.identities) || !input.identities.length) fail('anchor_scoped_resume_review_initialization_invalid');
  const seen = new Set();
  return [...input.identities].sort((a, b) => String(a?.prospectId).localeCompare(String(b?.prospectId))).map(identity => {
    if (!exact(identity, ['prospectId', 'companyId', 'recipientEmail', 'identityFingerprint']) || seen.has(identity.prospectId)) fail('anchor_scoped_resume_review_initialization_invalid');
    const admission = manifest.admissions.find(a => ['prospectId', 'companyId', 'recipientEmail', 'identityFingerprint'].every(k => a[k] === identity[k]));
    if (!admission) fail('anchor_scoped_resume_review_initialization_not_admitted');
    seen.add(identity.prospectId);
    return { ...identity, approvalReference: input.approvalReference, beforeReview: null, afterReview: false };
  });
}
async function reviewIdentities(db, manifest, initialization = [], { previewOnly = false } = {}) {
  const audit = [];
  // The exclusive recovery lock precedes sorted identity locks everywhere here.
  for (const admission of [...manifest.admissions].sort((a, b) => a.prospectId.localeCompare(b.prospectId))) {
    const bound = await scope().readScopedIdentity(db, { tenantId: '10', ...admission });
    const approved = initialization.find(row => row.prospectId === admission.prospectId);
    if (approved) {
      if (bound.prospect.ao_outreach_review_required !== null || bound.identityFingerprint !== approved.identityFingerprint
        || bound.companyNameKey !== admission.companyNameKey || bound.companyDomain !== admission.companyDomain
        || bound.prospect.do_not_contact !== false || bound.prospect.is_synthetic !== false) fail('anchor_scoped_resume_review_cas_failed');
      await quarantine.assertNotQuarantined(db, { tenantId: '10', prospectId: admission.prospectId,
        companyId: admission.companyId, email: admission.recipientEmail, lockCompanyRows: true });
      if (!previewOnly) {
        const changed = await db.query(`UPDATE prospects SET ao_outreach_review_required=false WHERE id::text=$1 AND client_id=10
        AND company_id::text=$2 AND email=$3 AND ao_outreach_review_required IS NULL AND do_not_contact=false AND is_synthetic=false RETURNING id`,
        [admission.prospectId, admission.companyId, admission.recipientEmail]);
        if (changed.rows.length !== 1) fail('anchor_scoped_resume_review_cas_failed');
      }
      // The dry-run changes only this transaction-local result object, never SQL.
      bound.prospect.ao_outreach_review_required = false;
      audit.push(approved);
    }
    scope().assertIdentityReview(bound, admission);
    await quarantine.assertNotQuarantined(db, { tenantId: '10', prospectId: admission.prospectId,
      companyId: admission.companyId, email: admission.recipientEmail, lockCompanyRows: true });
  }
  return audit;
}
async function installScopedResume(pool, input, actor, options = {}) {
  if (!input || Object.keys(input).some(k => !['manifest', 'approvalReference', 'reviewInitialization'].includes(k))
    || !text(input.approvalReference) || input.approvalReference !== input.manifest?.approvalReference) fail('anchor_scoped_resume_explicit_approval_required');
  const manifest = scope().validateScopedResumeManifest(input.manifest, { now: now(options) });
  if (preparationAuthority.isPreparationAuthority(manifest)) options = { ...options, sessionId: actor?.sessionId };
  const initialization = initializationPlan(input.reviewInitialization, manifest);
  if (!actor || String(actor.id) !== manifest.operatorId || actor.role !== manifest.operatorRole) fail('anchor_scoped_resume_operator_invalid');
  if ((options.env || process.env).ANCHOR_EMAIL_SCOPED_RESUME_ID !== manifest.id) fail('anchor_scoped_resume_activation_required');
  return transaction(pool, async (db, control) => {
    await currentActor(db, actor, manifest, { requireSession: true });
    const program = await liveBindings(db, manifest, options);
    if (manifest.scopes.research.enabled) await assertScopedResumeProviderBudget(db, manifest, { program, ...options });
    const previous = (await db.query('SELECT * FROM anchor_scoped_resume_grants WHERE manifest_id=$1 AND tenant_id=$2 FOR UPDATE', [manifest.id, '10'])).rows[0];
    if (previous) {
      if (previous.revoked_at !== null || previous.manifest_hash !== hash(manifest)
        || hash(previous.review_initializations) !== hash(initialization) || hash(control.manifest) !== hash(manifest)) fail('anchor_scoped_resume_install_conflict');
      await reviewIdentities(db, manifest);
      return { installed: true, idempotent: true, manifestId: manifest.id, initialized: 0, providerAuthorized: false };
    }
    if (control.manifest !== null) fail('anchor_scoped_resume_revoke_required');
    if (preparationAuthority.isPreparationAuthority(manifest)) {
      await assertPreparationBaseline(db, manifest);
      await assertPreparationIdentitiesUnconsumed(db, manifest);
    }
    const audit = await reviewIdentities(db, manifest, initialization);
    scope().validateScopedResumeManifest(manifest, { now: now(options) });
    validateProgramAuthority(program, manifest, options);
    if (preparationAuthority.isPreparationAuthority(manifest)) await preparationIdentity.assertPreparationApprover(db, manifest, { sessionId: options.sessionId });
    if ((options.env || process.env).ANCHOR_EMAIL_SCOPED_RESUME_ID !== manifest.id) fail('anchor_scoped_resume_activation_required');
    const installedGrant = await db.query(`INSERT INTO anchor_scoped_resume_grants(manifest_id,tenant_id,program_id,manifest_hash,manifest,
      approval_ref,operator_id,operator_role,review_initializations,installed_at)
      ${preparationAuthority.isPreparationAuthority(manifest)
    ? `SELECT $1,'10',$2,$3,$4,$5,$6,$7,$8,$9 WHERE EXISTS
      (SELECT 1 FROM "session" WHERE sid=$10 AND expire > clock_timestamp())`
    : "VALUES($1,'10',$2,$3,$4,$5,$6,$7,$8,$9)"}`,
    [manifest.id, manifest.programId, hash(manifest), manifest, input.approvalReference, String(actor.id), actor.role, JSON.stringify(audit), now(options),
      ...(preparationAuthority.isPreparationAuthority(manifest) ? [options.sessionId] : [])]);
    if (preparationAuthority.isPreparationAuthority(manifest) && installedGrant.rowCount !== 1) fail('anchor_crm_preparation_approver_invalid');
    const updated = await db.query('UPDATE anchor_email_recovery_control SET manifest=$1 WHERE singleton=true AND maintenance_armed=true AND manifest IS NULL RETURNING singleton', [manifest]);
    if (updated.rows.length !== 1) fail('anchor_scoped_resume_control_changed');
    return { installed: true, idempotent: false, manifestId: manifest.id, initialized: audit.length, providerAuthorized: false };
  });
}
async function revokeScopedResume(pool, input, actor, options = {}) {
  if (!exact(input, ['manifestId', 'approvalReference']) || !text(input.manifestId) || !text(input.approvalReference)) fail('anchor_scoped_resume_explicit_revocation_required');
  return transaction(pool, async (db, control) => {
    const grant = await one(db, 'SELECT * FROM anchor_scoped_resume_grants WHERE manifest_id=$1 AND tenant_id=$2 FOR UPDATE', [input.manifestId, '10'], 'anchor_scoped_resume_not_installed');
    if (options.expectedManifestHash !== undefined && grant.manifest_hash !== options.expectedManifestHash) fail('anchor_scoped_resume_control_changed');
    if (options.preparationOnly === true && !preparationAuthority.isPreparationAuthority(grant.manifest)) fail('anchor_scoped_resume_control_changed');
    if (preparationAuthority.isPreparationAuthority(grant.manifest)) {
      if (hash(grant.manifest) !== grant.manifest_hash) fail('anchor_scoped_resume_control_changed');
      await preparationIdentity.assertPreparationRevoker(db, grant.manifest, actor);
    } else await currentActor(db, actor);
    if (grant.revoked_at !== null) return { revoked: true, idempotent: true, manifestId: input.manifestId };
    if (control.manifest?.kind !== 'scoped_resume' || control.manifest.id !== input.manifestId
      || hash(control.manifest) !== grant.manifest_hash) fail('anchor_scoped_resume_control_changed');
    await db.query(`UPDATE anchor_scoped_resume_grants SET revoked_at=$2,revoke_approval_ref=$3,revoked_by=$4,revoked_role=$5
      WHERE manifest_id=$1 AND tenant_id='10' AND revoked_at IS NULL`, [input.manifestId, now(options), input.approvalReference, String(actor.id), actor.role]);
    await db.query('UPDATE anchor_email_recovery_control SET manifest=NULL WHERE singleton=true AND maintenance_armed=true');
    return { revoked: true, idempotent: false, manifestId: input.manifestId };
  });
}
async function consumeScopedOperation(pool, input, options = {}) {
  if (!input || Object.keys(input).some(k => !['manifest', 'capability', 'operation', 'operationId', 'units', 'identities', 'itemCount', 'providerBudgetHash'].includes(k))
    || !['research', 'preparation'].includes(input.capability) || !text(input.operationId)
    || (input.units !== undefined && input.units !== 1)) fail('anchor_scoped_resume_operation_invalid');
  const manifest = scope().validateScopedResumeManifest(input.manifest, { now: now(options) });
  const capability = input.capability, permission = manifest.scopes[capability];
  if (!permission.enabled || !permission.operations.includes(input.operation) || input.operation === 'reuse_existing_artifacts') fail('anchor_scoped_resume_capability_closed');
  const identities = input.identities === undefined ? manifest.admissions : input.identities;
  if (!Array.isArray(identities) || !identities.length || new Set(identities.map(a => a.prospectId)).size !== identities.length
    || identities.some(a => !manifest.admissions.some(admitted => hash(a) === hash(admitted)))) fail('anchor_scoped_resume_operation_identity_invalid');
  if (preparationAuthority.isPreparationAuthority(manifest)
    && (capability !== 'preparation' || input.operation !== 'prepare_existing_crm'
      || hash([...identities].sort((a, b) => a.prospectId.localeCompare(b.prospectId)))
        !== hash([...manifest.admissions].sort((a, b) => a.prospectId.localeCompare(b.prospectId)))))
    fail('anchor_crm_preparation_exact_batch_required');
  // Item budgets count new CRM draft identities and separately frozen/refilled entries.
  // Draft then freeze charges twice; canonical approval and read-only reuse add no items.
  const itemCount = ['prepare_existing_crm', 'freeze', 'refill'].includes(input.operation) ? identities.length : 0;
  if (input.itemCount !== undefined && input.itemCount !== itemCount) fail('anchor_scoped_resume_operation_item_count_invalid');
  return transaction(pool, async (db, control) => {
    if (hash(control.manifest) !== hash(manifest) || (options.env || process.env).ANCHOR_EMAIL_SCOPED_RESUME_ID !== manifest.id) fail('anchor_scoped_resume_closed');
    await assertInstalledScopedResume(db, manifest, { ...options, lock: true });
    const program = await liveBindings(db, manifest, options);
    if (capability === 'research') await assertScopedResumeProviderBudget(db, manifest, { program, operation: input.operation, ...options });
    await reviewIdentities(db, { ...manifest, admissions: identities });
    if ((await db.query('SELECT operation_id FROM anchor_scoped_resume_consumption WHERE operation_id=$1', [input.operationId])).rows.length) fail('anchor_scoped_resume_operation_consumed');
    if (preparationAuthority.isPreparationAuthority(manifest)) await assertPreparationIdentitiesUnconsumed(db, manifest);
    const at = now(options), localDay = clock(at).day;
    const totals = await one(db, `SELECT count(*)::int AS total_operations,
      count(*) FILTER (WHERE local_day=$3::date)::int AS daily_operations,
      coalesce(sum(item_count),0)::int AS total_items,
      coalesce(sum(item_count) FILTER (WHERE local_day=$3::date),0)::int AS daily_items
      FROM anchor_scoped_resume_consumption WHERE tenant_id='10' AND program_id=$1 AND capability=$2`,
    [manifest.programId, capability, localDay], 'anchor_scoped_resume_consumption_unavailable');
    if (preparationAuthority.isPreparationAuthority(manifest)
      && (totals.total_operations !== manifest.preparationAuthority.consumedOperations
        || totals.total_items !== manifest.preparationAuthority.consumedItems)) fail('anchor_crm_preparation_baseline_changed');
    const dailyCap = Math.min(permission.dailyCap, capability === 'preparation' ? program.policy.preparationAttemptsPerDay : program.policy.enrichmentLimit);
    if (totals.total_operations >= permission.totalCap || totals.daily_operations >= dailyCap
      || (capability === 'preparation' && (totals.total_items + itemCount > permission.totalItemCap || totals.daily_items + itemCount > permission.dailyItemCap))) fail('anchor_scoped_resume_operation_cap_reached');
    scope().validateScopedResumeManifest(manifest, { now: now(options) });
    validateProgramAuthority(program, manifest, options);
    if (preparationAuthority.isPreparationAuthority(manifest)) await preparationIdentity.assertPreparationApprover(db, manifest, { sessionId: options.sessionId });
    if ((options.env || process.env).ANCHOR_EMAIL_SCOPED_RESUME_ID !== manifest.id) fail('anchor_scoped_resume_closed');
    if (clock(now(options)).day !== localDay) fail('anchor_scoped_resume_operation_day_changed');
    const consumed = await db.query(`INSERT INTO anchor_scoped_resume_consumption(operation_id,tenant_id,program_id,manifest_id,capability,operation,
      local_day,item_count,identities,approval_ref,consumed_at)
      ${preparationAuthority.isPreparationAuthority(manifest)
    ? `SELECT $1,'10',$2,$3,$4,$5,$6,$7,$8,$9,$10 WHERE EXISTS
      (SELECT 1 FROM "session" WHERE sid=$11 AND expire > clock_timestamp())`
    : "VALUES($1,'10',$2,$3,$4,$5,$6,$7,$8,$9,$10)"}`,
    [input.operationId, manifest.programId, manifest.id, capability, input.operation, localDay, itemCount, JSON.stringify(identities), permission.approvalReference, at,
      ...(preparationAuthority.isPreparationAuthority(manifest) ? [options.sessionId] : [])]);
    if (preparationAuthority.isPreparationAuthority(manifest) && consumed.rowCount !== 1) fail('anchor_crm_preparation_approver_invalid');
    return { consumed: true, operationId: input.operationId, manifestId: manifest.id, capability, operation: input.operation, localDay, units: 1, itemCount };
  });
}
// Read-only validation: an explicit initialization plan is simulated, never written.
// It takes the same bounded locks as installation; activation is reported by the
// caller, never synthesized into environment authority here.
async function previewScopedPreparation(pool, input, actor, options = {}) {
  const manifest = scope().validateScopedResumeManifest(input, { now: now(options) });
  if (!preparationAuthority.isPreparationAuthority(manifest)) fail('anchor_scoped_resume_invalid_manifest');
  const initialization = initializationPlan(options.reviewInitialization, manifest);
  return transaction(pool, async (db, control) => {
    await currentActor(db, actor, manifest, { requireSession: true });
    await liveBindings(db, manifest, { ...options, sessionId: actor.sessionId }, true);
    const prior = (await db.query('SELECT manifest_hash,revoked_at,review_initializations FROM anchor_scoped_resume_grants WHERE manifest_id=$1 AND tenant_id=$2', [manifest.id, '10'])).rows;
    if (prior.length) {
      if (prior.length !== 1 || prior[0].revoked_at !== null || prior[0].manifest_hash !== hash(manifest)
        || hash(prior[0].review_initializations) !== hash(initialization)
        || hash(control.manifest) !== hash(manifest)) fail('anchor_scoped_resume_install_conflict');
    } else if (control.manifest !== null) fail('anchor_scoped_resume_revoke_required');
    await reviewIdentities(db, manifest, prior.length ? [] : initialization, { previewOnly: true });
    await assertPreparationBaseline(db, manifest);
    await assertPreparationIdentitiesUnconsumed(db, manifest);
    return { alreadyInstalled: prior.length === 1 };
  });
}
module.exports = { initializationPlan, previewScopedPreparation, installScopedResume, revokeScopedResume, assertInstalledScopedResume,
  consumeScopedOperation, assertScopedResumeProviderBudget };
