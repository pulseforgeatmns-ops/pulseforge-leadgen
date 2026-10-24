'use strict';

// SPEC-263. Explicit preparation-only operator entry point. No scheduled caller,
// dependency injection, provider discovery, enrichment, verification or dispatch.
const amo = require('../packages/acquisition-mission');
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const scope = require('./anchorScopedResume');
const control = require('./anchorScopedResumeControl');
const quarantine = require('./anchorEmailQuarantine');
const preparationAuthority = require('./anchorCrmPreparationAuthority');
const { GovernedOutboundStore, ownershipNameKey, ownershipDomain } = require('./governedOutboundStore');
const { governedContactReason } = require('../utils/governedContactEligibility');
const { sourceScope, missionCandidateReason } = require('./maxOutboundControlLoop');
const { getAcquisitionMissionRuntime } = require('./acquisitionMissionRuntime');
const persistence = require('./acquisitionMissionPersistence');
const { buildInboxSnapshot } = require('./emmettOutboundSnapshot');
const { resolveCanonicalSenderIdentity } = require('../utils/canonicalSenderIdentity');
const OPERATION = 'prepare_existing_crm';
const MAX_BATCH = 5;
function fail(code) { throw Object.assign(new Error(code), { code, retryAllowed: false, providerBoundaryCrossed: false }); }
function time(options) { return new Date(typeof options.now === 'function' ? options.now() : options.now || new Date()); }
function validateInput(input) {
  if (!input || Object.keys(input).sort().join(',') !== 'admissions,programId,sourceMissionHash'
    || typeof input.programId !== 'string' || !input.programId.trim()
    || !/^[a-f0-9]{64}$/.test(input.sourceMissionHash || '') || !Array.isArray(input.admissions)
    || !input.admissions.length || input.admissions.length > MAX_BATCH
    || new Set(input.admissions.map(a => a?.prospectId)).size !== input.admissions.length) fail('anchor_crm_preparation_input_invalid');
  return JSON.parse(JSON.stringify(input));
}
async function connection(pool) {
  let timer, expired = false;
  const pending = pool.connect().then(db => { if (expired) { db.release(true); fail('anchor_crm_preparation_connection_timeout'); } return db; });
  let client;
  try { client = await Promise.race([pending, new Promise((_, reject) => { timer = setTimeout(() => {
    expired = true; reject(Object.assign(new Error('anchor_crm_preparation_connection_timeout'), { code: 'anchor_crm_preparation_connection_timeout' }));
  }, 6000); })]); } finally { clearTimeout(timer); }
  return { query: (q, values = []) => client.query(typeof q === 'string' ? { text: q, values, query_timeout: 6000 } : { ...q, query_timeout: 6000 }),
    release: () => client.release(true) };
}
async function locked(pool, work) {
  const db = await connection(pool); let begun = false;
  try {
    await db.query('BEGIN'); begun = true;
    await db.query("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='5s'");
    const result = await work(db);
    await db.query('COMMIT'); begun = false;
    return result;
  } finally { if (begun) await db.query('ROLLBACK').catch(() => {}); db.release(); }
}
async function bindings(db, input, options) {
  const program = (await db.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2', [input.programId, '10'])).rows[0];
  const authorityOptions = { ...options, now: time(options) };
  const authority = await preparationAuthority.assertPreparationCapability(db, program, authorityOptions)
    || await scope.assertScopedResumeCapability(db, { program, capability: 'preparation', operation: OPERATION }, authorityOptions);
  const grant = authority.manifest;
  // Optional HTTP authority is checked under every canonical binding lock, not
  // just in route preflight. Legacy one-shot callers retain their old contract.
  if (options.actionAuthority) {
    const a = options.actionAuthority;
    if (grant.operatorId !== a.actorId || grant.operatorRole !== 'admin'
      || hash(grant) !== a.manifestHash || (options.env || process.env).RAILWAY_GIT_COMMIT_SHA !== a.revision)
      fail('anchor_crm_preparation_action_authority_changed');
  }
  if (grant.scopes.email.enabled || grant.scopes.research.enabled) fail('anchor_crm_preparation_only_required');
  const admissions = input.admissions.map(a => grant.admissions.find(approved => hash(approved) === hash(a)));
  if (admissions.some(a => !a)) fail('anchor_scoped_resume_admission_missing');
  admissions.sort((a, b) => a.prospectId.localeCompare(b.prospectId));
  if (preparationAuthority.isPreparationAuthority(grant)
    && hash(admissions) !== hash([...grant.admissions].sort((a, b) => a.prospectId.localeCompare(b.prospectId))))
    fail('anchor_crm_preparation_exact_batch_required');
  if (new Set(admissions.map(a => a.companyId)).size !== admissions.length
    || new Set(admissions.map(a => a.companyNameKey)).size !== admissions.length
    || new Set(admissions.map(a => a.companyDomain)).size !== admissions.length) fail('anchor_crm_preparation_duplicate_company');
  const source = (await db.query('SELECT payload FROM acquisition_missions WHERE id=$1 AND tenant_id=$2 FOR SHARE', [grant.sourceMissionId, '10'])).rows[0]?.payload;
  if (!source || source.stage !== 'discover' || hash(source) !== input.sourceMissionHash
    || hash(missionScope(source)) !== grant.scopeHash) fail('anchor_crm_preparation_source_changed');
  if (preparationAuthority.isPreparationAuthority(grant)) preparationAuthority.assertPreparationSource(source, grant);
  const store = new GovernedOutboundStore(db, '10'), rows = [];
  for (const admission of admissions) {
    const bound = await scope.readScopedIdentity(db, { tenantId: '10', ...admission });
    scope.assertIdentityReview(bound, admission);
    await quarantine.assertNotQuarantined(db, { tenantId: '10', prospectId: admission.prospectId,
      companyId: admission.companyId, email: admission.recipientEmail, lockCompanyRows: true });
    await scope.assertScopedFirstTouch(db, admission, '__crm_preparation_no_item__');
    const row = (await db.query(`SELECT p.*,c.name AS company_name,c.domain AS company_domain,c.website AS company_website,
      c.location AS company_location,c.industry FROM prospects p JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
      WHERE p.client_id=10 AND p.id::text=$1 AND c.id::text=$2 FOR SHARE OF p,c`, [admission.prospectId, admission.companyId])).rows[0];
    const reason = !row ? 'anchor_crm_preparation_identity_missing' : governedContactReason(row, program.policy)
      || missionCandidateReason(row, sourceScope(source), 10)
      || await store.suppression({ candidateId: admission.companyId, prospectId: admission.prospectId,
        companyId: admission.companyId, email: admission.recipientEmail });
    if (reason) fail(reason);
    rows.push(row);
  }
  // A different contact/CRM key cannot erase an earlier company-alias attempt.
  const attemptedCompanies = (await db.query(`SELECT c.id,c.name,c.domain,c.website FROM companies c WHERE c.client_id=10 AND (
    EXISTS(SELECT 1 FROM acquisition_outbound_items i WHERE i.tenant_id='10' AND i.company_id=c.id::text
      AND (i.attempted_at IS NOT NULL OR i.provider_message_id IS NOT NULL OR i.status IN ('attempted','uncertain','sent')))
    OR EXISTS(SELECT 1 FROM anchor_email_recovery_reservations r WHERE r.company_id=c.id::text)
    OR EXISTS(SELECT 1 FROM acquisition_mission_outbound_executions x LEFT JOIN prospects p ON p.client_id=10 AND p.company_id=c.id
      WHERE x.tenant_id='10' AND x.status IN ('sent','attempted','failed')
      AND (x.prospect_id=c.id::text OR x.prospect_id=p.id::text OR lower(x.payload->>'email')=lower(p.email))))`)).rows;
  for (const a of admissions) if (attemptedCompanies.some(c => ownershipNameKey(c.name) === a.companyNameKey
    || [c.domain, c.website].some(d => ownershipDomain(d) === a.companyDomain))) fail('anchor_crm_preparation_prior_company_attempt');
  return { ...authority, admissions, source, rows };
}
async function infrastructure(db, binding, options) {
  const now = time(options), { program, manifest } = binding;
  const client = (await db.query('SELECT * FROM clients WHERE id=10 FOR SHARE')).rows[0];
  const sender = await resolveCanonicalSenderIdentity({ tenantId: '10', clientId: 10, client, pool: db });
  if (!client || client.active !== true || client.autosend_enabled !== false || !sender.ok
    || sender.identity.senderEmail !== manifest.senderEmail || sender.identity.senderName !== manifest.senderName
    || sender.identity.sendingDomain !== manifest.sendingDomain) fail('anchor_crm_preparation_sender_changed');
  const inbox = (await db.query('SELECT * FROM tenant_mailbox_integrations WHERE tenant_id=$1 AND id=$2 FOR SHARE', ['10', manifest.inboxIntegrationId])).rows[0];
  if (!inbox || inbox.status !== 'active' || inbox.mailbox_address !== manifest.senderEmail) fail('anchor_crm_preparation_inbox_unavailable');
  const health = (await db.query('SELECT last_success_at FROM acquisition_outbound_inbox_health WHERE tenant_id=$1 AND integration_id=$2 FOR SHARE', ['10', manifest.inboxIntegrationId])).rows[0];
  if (!health?.last_success_at || +now - +new Date(health.last_success_at) < 0
    || +now - +new Date(health.last_success_at) >= 300000) fail('anchor_crm_preparation_telemetry_stale');
  const authentication = Object.fromEntries(['spf', 'dkim', 'dmarc'].map(key => [key, { state: 'UNKNOWN',
    provenance: { source: 'crm_only_preparation', reason: 'Provider authentication was not checked.' } }]));
  const snapshot = await buildInboxSnapshot(10, { pool: db, now, skipEnsure: true, strictReadOnly: true, authentication,
    providerCeiling: program.policy.dailyCap, blacklist: { listed: null, status: 'not_checked', sources: [],
      provenance: { source: 'crm_only_preparation', reason: 'No blacklist check was performed.' } } });
  Object.assign(snapshot, sender.identity, { inboxId: manifest.senderEmail, domain: manifest.sendingDomain,
    providerReadiness: 'not_checked', preparationOnly: true });
  const assessed = require('../packages/emmett-outbound').createOutboundEngine().assess({ tenantId: '10', snapshot, now });
  if (assessed.governor.halt || !['proceed', 'slow'].includes(assessed.governor.outcome)
    || !Number.isFinite(assessed.capacity.recommended) || assessed.capacity.recommended < binding.rows.length) fail('anchor_crm_preparation_capacity_unavailable');
  if (binding.rows.length > program.policy.dailyCap) fail('anchor_crm_preparation_capacity_unavailable');
  return snapshot;
}
function crmDiscovery(binding) {
  const fitCandidates = binding.rows.map(row => ({ id: String(row.id), prospectId: String(row.id), companyId: String(row.company_id),
    name: row.company_name, website: row.company_website || row.company_domain, location: row.company_location,
    industry: row.vertical, readinessState: 'unknown', buyerReadiness: 'unknown', signals: [],
    evidenceRefs: [{ id: `crm_${row.id}`, entityId: String(row.id), source: `crm:${row.id}`,
      label: 'Existing canonical CRM contact and scope evidence; no new provider verification or discovery performed.',
      observedAt: row.enrichment_provenance.email.resolved_at,
      provenance: { kind: 'existing_crm', email: row.enrichment_provenance.email, programId: binding.program.id } }],
    unknowns: ['Buyer intent, incumbent vendor, budget and willingness to discuss are unknown.'] }));
  return { payload: { fitCandidates, candidateUniverse: fitCandidates, qualifiedCount: fitCandidates.length,
    candidateCount: fitCandidates.length, discoveryStatus: 'complete', source: 'explicit_approved_existing_crm',
    unknowns: ['No new discovery or verification was performed; existing contact eligibility does not establish buyer readiness.'] } };
}
// Preserve native numbers/outcomes; its legacy explanation conflates absence of
// blacklist evidence with clearance. This bridge must disclose the unknown.
function reviewExplanations(value) {
  if (typeof value === 'string') return value === 'No blacklist concerns'
    ? 'Blacklist not checked; no clearance is established.'
    : value === 'Healthy. Safe. Send.' ? 'Native planning assessment permits review; provider readiness remains unchecked.' : value;
  if (Array.isArray(value)) return value.map(reviewExplanations);
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, reviewExplanations(v)]));
  return value;
}
async function prepareExistingCrm(pool, rawInput, options = {}) {
  const input = validateInput(rawInput);
  if (!pool?.connect || Object.keys(options).some(k => !['env', 'now', 'actionAuthority'].includes(k))) fail('anchor_crm_preparation_input_invalid');
  const reviewed = await locked(pool, async db => { const b = await bindings(db, input, options); await infrastructure(db, b, options); return b; });
  // Excludes grant ID/day/caller-generated IDs: changing any of those cannot
  // replay the same source+identity batch, even after a downstream failure.
  const operationId = hash(['anchor-crm-preparation-v1', input.programId, reviewed.source.id, reviewed.program.policy_hash, reviewed.program.scope_hash,
    reviewed.admissions.map(a => [a.prospectId, a.companyId, a.recipientEmail, a.identityFingerprint])]);
  await control.consumeScopedOperation(pool, { manifest: reviewed.manifest, capability: 'preparation', operation: OPERATION,
    operationId, identities: reviewed.admissions }, { ...options, now: time(options) });
  return locked(pool, async db => {
    let binding = await bindings(db, input, options);
    if (hash(binding.manifest) !== hash(reviewed.manifest)) fail('anchor_crm_preparation_grant_changed');
    let snapshot = await infrastructure(db, binding, options);
    const missionId = `mission_crm_${operationId.slice(0, 32)}`;
    if ((await db.query('SELECT id FROM acquisition_missions WHERE id=$1', [missionId])).rows.length) fail('anchor_crm_preparation_already_exists');
    const canonical = await connection(pool);
    try {
      await canonical.query("SET lock_timeout='5s'; SET statement_timeout='5s'");
      const runtime = getAcquisitionMissionRuntime({ pool, persist: true });
      await runtime.hydrate('10', { pool: canonical, persist: true, preinitializedSchema: true });
      const engine = runtime.engine();
      if (engine.get(missionId, '10')) fail('anchor_crm_preparation_already_exists');
      const mission = engine.create({ ...missionScope(binding.source), id: missionId, tenantId: '10', clientId: 10,
        title: 'Reviewed existing CRM preparation', createdBy: binding.manifest.operatorId,
        orchestrationMissionId: binding.source.id });
      mission.crmPreparation = { operationId, sourceMissionHash: input.sourceMissionHash, manifestHash: hash(binding.manifest),
        admissions: binding.admissions, providerReadiness: 'not_checked', providerCalls: 0, preparationOnly: true };
      engine.store.putMission(mission);
      const crmByProspectId = new Map();
      for (const row of binding.rows) { const crm = { ...row, prospect_id: String(row.id) };
        crmByProspectId.set(String(row.id), crm); crmByProspectId.set(String(row.company_id), crm); }
      const scoutExecutor = require('../packages/max/workspace/ScoutDiscoveryExecutor');
      const runScout = preparationAuthority.isPreparationAuthority(binding.manifest)
        ? scoutExecutor.createExistingCrmScoutRunner(crmDiscovery(binding))
        : (m, opts) => scoutExecutor.runScoutForAmoMission(m,
          { ...opts, runScout: undefined, allowFixtureFallback: false, discoverImpl: async () => crmDiscovery(binding) });
      const runPaige = async (m, opts) => {
        const executionInput = amo.buildExecutionInput({ mission: m, contributions: opts.contributions, specialist: 'paige',
          transactionId: opts.transactionId, executionContext: opts.executionContext, store: engine.store, crmByProspectId });
        const result = await require('../packages/max/workspace/PaigeVariantsExecutor').runPaigeVariants({ ...executionInput,
          crmByProspectId, approvedCopies: {}, aoSenderNameByProspectId: new Map(),
          mission: { ...m, senderName: binding.manifest.senderName } });
        if (result.status !== amo.EXECUTION_STATUSES.SUCCESS) fail('anchor_crm_preparation_copy_blocked');
        return result.contributions;
      };
      const runEmmett = async (m, opts) => {
        const result = await require('../packages/max/workspace/EmmettCapacityExecution').runEmmettForAmoMission(m,
          { ...opts, runEmmett: undefined, crmByProspectId, infrastructureSnapshot: snapshot, allowFixtureFallback: false, now: time(options) });
        result.capacityPayload = reviewExplanations(result.capacityPayload);
        result.assessed = reviewExplanations(result.assessed);
        result.capacityPayload.preparationEvidence = { providerReadiness: 'not_checked', preparationOnly: true,
          authentication: snapshot.authentication, blacklist: snapshot.blacklist };
        return result;
      };
      for (let step = 0; step < 6; step++) {
        binding = await bindings(db, input, options);
        snapshot = await infrastructure(db, binding, options);
        const current = engine.get(missionId, '10');
        if (current.stage === 'ready') break;
        const ctx = amo.specialistContext(engine.inspect(missionId, { tenantId: '10' }).contributions, { missionId });
        const intent = current.stage === 'discover' ? amo.intentFromPendingDecision(current.pendingOperatorDecision)
          : !ctx.acquisitionApproachComplete ? 'DECIDE_ACQUISITION_APPROACH' : !ctx.paigeComplete ? 'GENERATE_VARIANTS' : 'GENERATE_CAPACITY';
        if (!['APPROVE_DISCOVERY', 'APPROVE_PRIORITIZATION', 'DECIDE_ACQUISITION_APPROACH', 'GENERATE_VARIANTS', 'GENERATE_CAPACITY'].includes(intent)) fail('anchor_crm_preparation_operator_judgment_required');
        const request = amo.createExecutionRequest({ source: amo.EXECUTION_SOURCES.API, missionId, mission: current,
          stage: current.stage, operatorId: binding.manifest.operatorId, intent, permissions: { canExecute: true, role: 'operator' },
          payload: { question: 'Explicit existing CRM preparation only', approach: 'outbound' } });
        const routed = await amo.routeExecutionRequest(request, { engine, tenantId: '10', pool: canonical, persist: true,
          preinitializedSchema: true, operatorId: binding.manifest.operatorId, allowFixtureFallback: false,
          runScout, runPaige, runEmmett, infrastructureSnapshot: snapshot });
        if (routed.executionResult?.rolledBack) throw routed.executionResult.error || new Error(routed.executionResult.rollbackReason);
      }
      binding = await bindings(db, input, options);
      const result = await persistence.loadMissionSnapshot(missionId, '10', canonical, { skipEnsure: true });
      if (result.mission.stage !== 'ready' || result.mission.pendingOperatorDecision?.kind !== 'execution_approval'
        || require('../packages/acquisition-mission/ExecutionApproval').findExecutionApprovals(result.contributions).length) fail('anchor_crm_preparation_not_review_ready');
      const capacity = amo.findEmmettCapacity(result.contributions, result.mission);
      const payload = require('../packages/acquisition-mission/ContributionSupersession').unwrapSpecialistPayload(capacity);
      if (payload.queue?.items?.length !== binding.admissions.length || payload.governor?.halt
        || !['proceed', 'slow'].includes(payload.governor?.outcome)) fail('anchor_crm_preparation_capacity_unavailable');
      for (const a of binding.admissions) {
        const matches = payload.queue.items.filter(item => String(item.companyId) === a.companyId && item.email === a.recipientEmail
          && String(item.candidateId || item.id) === a.companyId && String(item.prospectId) === a.companyId
          && String(item.paige?.candidateId) === a.companyId);
        if (matches.length !== 1 || matches[0].paige?.ready !== true || !matches[0].paige?.subject || !matches[0].paige?.body) fail('anchor_crm_preparation_output_identity_changed');
      }
      await new GovernedOutboundStore(db, '10').event('scoped_crm_preparation_ready', operationId, { programId: input.programId,
        missionId, operationId, selectedDraftCount: binding.admissions.length, sourceMissionHash: input.sourceMissionHash, providerReadiness: 'not_checked', providerCalls: 0 });
      return { readyForReview: true, missionId, operationId, preparedArtifactRevision: amo.computePreparedArtifactRevision(missionId, result.contributions),
        candidateCount: binding.admissions.length, providerReadiness: 'not_checked', providerCalls: 0,
        executionApproved: false, envelopeAuthorized: false, sent: 0 };
    } finally { canonical.release(); }
  });
}
module.exports = { prepareExistingCrm };
