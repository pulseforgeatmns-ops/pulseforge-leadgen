'use strict';

// SPEC-261: explicit freeze-only authority for existing current-day artifacts.
// Deliberately do not import runtime adapters/loadMissionSnapshot/store.freeze:
// those entrypoints bootstrap schema/providers or own a different transaction.
const { createHash } = require('node:crypto');
const { hash, clock, missionScope, candidateReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { selectCanonicalContribution } = require('../packages/acquisition-mission/CanonicalContributionSelection');
const { unwrapSpecialistPayload } = require('../packages/acquisition-mission/ContributionSupersession');
const { outreachSequenceForRevisionHash } = require('../packages/acquisition-mission/PreparedOutreachSequence');
const { extractCapacitySenderIdentity, assertCapacityMatchesCanonical, validateCanonicalSenderConfig } = require('../utils/canonicalSenderIdentity');
const { validateLivePolicy, RECOVERY_LOCK_KEY } = require('./anchorEmailRecovery');
const quarantine = require('./anchorEmailQuarantine');
const { GOVERNED_OUTBOUND_SEND_LOCK_NAMESPACE } = require('./governedOutboundTenant');
const { EXECUTOR_LOCK_NAMESPACE } = require('./tenantOutreachExecutorLock');
const ACTION = 'APPROVED_PREPARE_EXISTING';
const CONFIG_KEYS = ['version', 'action', 'id', 'approvalReference', 'operatorId', 'operatorRole', 'testMode', 'tenantId', 'localDay', 'programId',
  'policyHash', 'scopeHash', 'missionId', 'missionHash', 'contributionSetHash', 'preparedRevision', 'artifacts',
  'maxItems', 'validFrom', 'expiresAt', 'quarantineDecisionHash', 'candidate'];
const CANDIDATE_KEYS = ['candidateId', 'prospectId', 'companyId', 'recipientEmail', 'subject', 'body', 'senderEmail', 'senderName', 'sendingDomain'];
const ARTIFACTS = { scout: 'discovery', max: 'prioritization', paige: 'variants', emmett: 'capacity' };
const NEEDS_RESEARCH = 'Separate bounded research or preparation approval is required; no artifact was regenerated.';
const text = value => typeof value === 'string' && value.length > 0 && value.length <= 1000 && value === value.trim() && !/[\x00-\x1f\x7f]/.test(value);
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const exact = (value, keys) => object(value) && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
function stop(code, dependency = false) { throw Object.assign(new Error(code), { code, preparationDependency: dependency }); }
function currentNow(options) { return options.now ? new Date(options.now) : new Date(); }
function iso(value) { const date = new Date(value); return Number.isFinite(+date) ? date.toISOString() : null; }
function normalizedEmail(value) {
  return text(value) && value === value.toLowerCase() && /^[^\s@<>,;]+@[^\s@<>,;]+\.[a-z]{2,}$/.test(value);
}
function validTime(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)
    && iso(value) === value;
}
function validatePrepareExistingConfig(input, { now = new Date() } = {}) {
  if (!exact(input, CONFIG_KEYS) || input.version !== 1 || input.action !== ACTION || input.tenantId !== '10'
    || !['admin', 'manager'].includes(input.operatorRole) || input.testMode !== false
    || input.maxItems !== 1 || input.localDay !== clock(now).day) stop('anchor_prepare_existing_invalid_config');
  for (const key of ['id', 'approvalReference', 'operatorId', 'programId', 'missionId', 'preparedRevision']) {
    if (!text(input[key]) || /^(?:todo|tbd|placeholder|replace[_ -]?me|<.*>)$/i.test(input[key])) stop('anchor_prepare_existing_explicit_approval_required');
  }
  for (const key of ['policyHash', 'scopeHash', 'missionHash', 'contributionSetHash', 'quarantineDecisionHash']) {
    if (!digest(input[key])) stop('anchor_prepare_existing_invalid_hash');
  }
  if (!validTime(input.validFrom) || !validTime(input.expiresAt) || !Number.isFinite(+now)
    || +now < Date.parse(input.validFrom) || +now >= Date.parse(input.expiresAt)
    || Date.parse(input.expiresAt) - Date.parse(input.validFrom) > 86400000) stop('anchor_prepare_existing_expired');
  if (!exact(input.artifacts, Object.keys(ARTIFACTS))) stop('anchor_prepare_existing_artifact_bindings_required', true);
  const ids = new Set();
  for (const artifact of Object.values(input.artifacts)) {
    if (!exact(artifact, ['id', 'hash']) || !text(artifact.id) || !digest(artifact.hash) || ids.has(artifact.id)) stop('anchor_prepare_existing_artifact_bindings_required', true);
    ids.add(artifact.id);
  }
  const c = input.candidate;
  if (!exact(c, CANDIDATE_KEYS) || !['candidateId', 'prospectId', 'companyId', 'subject', 'senderName', 'sendingDomain'].every(key => text(c[key]))
    || !normalizedEmail(c.recipientEmail) || !normalizedEmail(c.senderEmail) || typeof c.body !== 'string' || !c.body.trim()
    || c.body.length > 100000 || /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(c.body)) stop('anchor_prepare_existing_candidate_required');
  return JSON.parse(JSON.stringify(input));
}

function contributionSetHash(rows) {
  return hash(rows.map(row => ({ id: row.id, hash: hash(row.payload), at: iso(row.at) })).sort((a, b) => a.id.localeCompare(b.id)));
}
// Same binding order/algorithm as ExecutionApproval.computePreparedArtifactRevision.
// Reuse its pure selectors and sequence normalization without its runtime imports.
function preparedRevision(missionId, contributions) {
  const choose = (specialist, kind) => selectCanonicalContribution(contributions, { missionId, specialist, kind, mission: { id: missionId } });
  const max = choose('max', 'prioritization'), paige = choose('paige', 'variants'), emmett = choose('emmett', 'capacity');
  const cap = unwrapSpecialistPayload(emmett), variants = unwrapSpecialistPayload(paige);
  const items = Array.isArray(cap.queue?.items) ? cap.queue.items : [];
  const sender = extractCapacitySenderIdentity(cap);
  const binding = { missionId, maxContributionId: max?.id || null, paigeContributionId: paige?.id || null,
    emmettContributionId: emmett?.id || null, queueCount: items.length,
    queueTargetIds: items.slice(0, 25).map(item => String(item.prospectId || item.id || item.companyId || item.company || '').trim()).filter(Boolean).sort(),
    governorOutcome: cap.governor?.outcome || null, senderEmail: sender.senderEmail || null,
    sendingDomain: sender.sendingDomain || null, outreachSequenceSteps: outreachSequenceForRevisionHash(variants.outreachSequence) };
  return createHash('sha256').update(JSON.stringify(binding)).digest('hex').slice(0, 16);
}
async function single(db, sql, values, code, dependency = false) {
  const { rows } = await db.query(sql, values);
  if (rows.length !== 1) stop(code, dependency);
  return rows[0];
}
function fresh(value, now, day) { return iso(value) !== null && +new Date(value) <= +now && clock(new Date(value)).day === day; }

async function inspect(db, config, options, locked) {
  const now = currentNow(options), suffix = locked ? ' FOR SHARE' : '';
  validatePrepareExistingConfig(config, { now });
  const control = await single(db, `SELECT singleton,maintenance_armed,manifest FROM anchor_email_recovery_control${locked ? ' FOR UPDATE' : ''}`, [], 'anchor_prepare_existing_maintenance_required');
  if (control.singleton !== true || control.maintenance_armed !== true || control.manifest !== null) stop('anchor_prepare_existing_maintenance_required');
  if (locked) for (const key of quarantine.identityLockKeys({ tenantId: '10', prospectId: config.candidate.prospectId,
    companyId: config.candidate.companyId, recipientEmail: config.candidate.recipientEmail })) {
    await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [key]);
  }
  const program = await single(db, `SELECT * FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2${suffix}`,
    [config.programId, '10'], 'anchor_prepare_existing_program_missing');
  const policyManifest = { programId: config.programId, policyHash: config.policyHash, senderEmail: config.candidate.senderEmail,
    inboxIntegrationId: program.policy?.inboxIntegrationId, validFrom: config.validFrom, expiresAt: config.expiresAt };
  validateLivePolicy(program, policyManifest, now);
  if (program.scope_hash !== config.scopeHash) stop('anchor_prepare_existing_scope_changed');
  if (String(program.authorized_by) !== config.operatorId) stop('anchor_prepare_existing_operator_mismatch');
  const operator = await single(db, `SELECT id,client_id,role,active FROM users WHERE id::text=$1 AND client_id=10${suffix}`,
    [config.operatorId], 'anchor_prepare_existing_operator_invalid');
  if (String(operator.id) !== config.operatorId || operator.client_id !== 10 || operator.active !== true
    || operator.role !== config.operatorRole) stop('anchor_prepare_existing_operator_invalid');
  const existing = await db.query(`SELECT id FROM acquisition_outbound_envelopes WHERE tenant_id=$1 AND local_day=$2::date${suffix}`, ['10', config.localDay]);
  if (existing.rows.length) stop('anchor_prepare_existing_envelope_exists');
  const eventId = hash(['anchor_recovery_existing_prepared', config.id]);
  if ((await db.query('SELECT id FROM acquisition_outbound_events WHERE id=$1', [eventId])).rows.length) stop('anchor_prepare_existing_grant_consumed');

  const missionRow = await single(db, `SELECT * FROM acquisition_missions WHERE id=$1 AND tenant_id=$2${suffix}`,
    [config.missionId, '10'], 'anchor_prepare_existing_current_artifacts_missing', true);
  if (missionRow.id !== config.missionId || missionRow.tenant_id !== '10' || missionRow.client_id !== 10
    || missionRow.stage !== 'ready' || !object(missionRow.payload) || hash(missionRow.payload) !== config.missionHash
    || !fresh(missionRow.created_at, now, config.localDay) || !fresh(missionRow.updated_at, now, config.localDay)) stop('anchor_prepare_existing_current_artifacts_missing', true);
  const mission = { ...missionRow.payload, id: missionRow.id, tenantId: missionRow.tenant_id,
    clientId: missionRow.client_id, stage: missionRow.stage, objective: missionRow.objective, targetSegment: missionRow.target_segment };
  if (mission.planCancelled || missionRow.orchestration_mission_id !== program.source_mission_id
    || hash(missionScope(mission)) !== program.scope_hash) stop('anchor_prepare_existing_scope_changed');
  const rows = (await db.query(`SELECT id,mission_id,tenant_id,specialist,kind,payload,at FROM acquisition_mission_contributions
    WHERE mission_id=$1 AND tenant_id=$2 ORDER BY id LIMIT 1001${suffix}`, [mission.id, '10'])).rows;
  if (!rows.length || rows.length > 1000 || contributionSetHash(rows) !== config.contributionSetHash) stop('anchor_prepare_existing_artifacts_changed', true);
  const contributions = rows.map(row => row.payload);
  const selected = {};
  for (const [specialist, kind] of Object.entries(ARTIFACTS)) {
    const ref = config.artifacts[specialist];
    const row = rows.find(candidate => candidate.id === ref.id);
    const canonical = selectCanonicalContribution(contributions, { missionId: mission.id, specialist, kind, mission });
    const unpointed = selectCanonicalContribution(contributions, { missionId: mission.id, specialist, kind, mission: { id: mission.id } });
    if (!row || row.tenant_id !== '10' || row.mission_id !== mission.id || row.specialist !== specialist || row.kind !== kind
      || row.payload?.id !== row.id || row.payload?.missionId !== mission.id || row.payload?.specialist !== specialist || row.payload?.kind !== kind
      || row.payload?.at !== iso(row.at) || hash(row.payload) !== ref.hash || canonical?.id !== row.id || unpointed?.id !== row.id
      || !fresh(row.at, now, config.localDay)) stop('anchor_prepare_existing_current_artifacts_missing', true);
    selected[specialist] = row.payload;
  }
  if (preparedRevision(mission.id, contributions) !== config.preparedRevision) stop('anchor_prepare_existing_revision_changed', true);
  const cap = unwrapSpecialistPayload(selected.emmett), variants = unwrapSpecialistPayload(selected.paige);
  const items = cap.queue?.items;
  if (!Array.isArray(items) || !items.length || items.length > 25 || !Number.isSafeInteger(cap.capacity?.recommended) || cap.capacity.recommended < 1
    || cap.governor?.halt || !['proceed', 'slow'].includes(cap.governor?.outcome)) stop('anchor_prepare_existing_capacity_blocked', true);
  for (const item of items) {
    const candidateId = String(item.id || item.candidateId || item.prospectId || item.companyId || '').trim();
    if (!candidateId || item.paige?.candidateId !== candidateId || !['prospect', 'mission'].includes(item.paige.bindingScope || 'prospect')
      || ((item.paige.bindingScope || 'prospect') === 'prospect' && !item.paige.attributableIntelligence)) stop('anchor_prepare_existing_message_binding_changed', true);
  }
  const candidate = config.candidate;
  const matching = items.filter(item => String(item.prospectId || item.id) === candidate.candidateId);
  const boundVariants = (variants.variants || []).filter(row => row.candidateId === candidate.candidateId);
  if (matching.length !== 1 || boundVariants.length !== 1) stop('anchor_prepare_existing_candidate_artifacts_missing', true);
  const queue = matching[0], variant = boundVariants[0];
  if (variant.subject !== candidate.subject || variant.body !== candidate.body || queue.email !== candidate.recipientEmail
    || queue.paige.subject !== candidate.subject || queue.paige.body !== candidate.body) stop('anchor_prepare_existing_copy_changed');
  const message = { variantLabel: variant.label || 'Primary', subject: variant.subject, body: variant.body,
    cta: variant.cta || variants.cta || null, candidateId: variant.candidateId, companyId: variant.companyId, companyName: variant.companyName };
  const client = await single(db, `SELECT id,active,autosend_enabled,sender_email,sender_name,sending_domain FROM clients WHERE id=$1${suffix}`, [10], 'anchor_prepare_existing_sender_missing');
  const sender = { tenantId: '10', clientId: 10, senderEmail: client.sender_email, senderName: client.sender_name, sendingDomain: client.sending_domain };
  if (client.id !== 10 || client.active !== true || client.autosend_enabled !== false || !validateCanonicalSenderConfig(sender).ok
    || !assertCapacityMatchesCanonical(cap, sender).ok || sender.senderEmail !== candidate.senderEmail
    || sender.senderName !== candidate.senderName || sender.sendingDomain !== candidate.sendingDomain) stop('anchor_prepare_existing_sender_changed');
  const inbox = await single(db, `SELECT id,tenant_id,status,mailbox_address FROM tenant_mailbox_integrations WHERE id=$1 AND tenant_id=$2${suffix}`,
    [program.policy.inboxIntegrationId, '10'], 'anchor_prepare_existing_inbox_missing');
  if (inbox.status !== 'active' || inbox.mailbox_address !== candidate.senderEmail) stop('anchor_prepare_existing_sender_changed');
  const prospect = await single(db, `SELECT p.id,p.client_id,p.company_id,p.email,p.do_not_contact,p.is_synthetic,p.ao_outreach_review_required,
    to_jsonb(p)->'email_verified' AS email_verified,to_jsonb(p)->>'email_status' AS email_status,
    to_jsonb(p)->'enrichment_provenance' AS enrichment_provenance,to_jsonb(p)->'acquisition_metadata' AS acquisition_metadata,
    to_jsonb(p)->>'contact_classification' AS contact_classification,to_jsonb(p)->>'assigned_ao_id' AS assigned_ao_id,
    to_jsonb(p)->>'last_contacted_at' AS last_contacted_at,to_jsonb(p)->>'last_reply_at' AS last_reply_at,
    to_jsonb(p)->>'closer_id' AS closer_id,c.name AS company_name,
    to_jsonb(c)->>'domain' AS company_domain,to_jsonb(c)->>'website' AS company_website
    FROM prospects p JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.id::text=$1 AND p.client_id=$2 AND c.id::text=$3${locked ? ' FOR SHARE OF p,c' : ''}`,
  [candidate.prospectId, 10, candidate.companyId], 'anchor_prepare_existing_crm_missing');
  if (String(prospect.id) !== candidate.prospectId || prospect.client_id !== 10 || String(prospect.company_id) !== candidate.companyId
    || prospect.email !== candidate.recipientEmail || prospect.ao_outreach_review_required !== false
    || prospect.do_not_contact !== false || prospect.is_synthetic !== false || prospect.assigned_ao_id != null
    || prospect.last_contacted_at != null || prospect.last_reply_at != null || prospect.closer_id != null) stop('anchor_prepare_existing_contact_held');
  const reason = candidateReason(queue, prospect, message, program.policy);
  if (reason) stop(reason);
  // Existing suppression/ownership checks are SELECT-only and do not bootstrap
  // the runtime; use this transaction as their pool so the reads cannot escape.
  const { GovernedOutboundStore } = require('./governedOutboundStore');
  const suppressed = await new GovernedOutboundStore(db, '10').suppression({ candidateId: candidate.candidateId,
    prospectId: candidate.prospectId, companyId: candidate.companyId, email: candidate.recipientEmail });
  if (suppressed) stop('anchor_prepare_existing_contact_held');
  const original = await quarantine.requireQuarantine(db, config.quarantineDecisionHash);
  await quarantine.assertNotQuarantined(db, { tenantId: '10', prospectId: candidate.prospectId, companyId: candidate.companyId,
    email: candidate.recipientEmail, lockCompanyRows: locked });
  const unresolved = await quarantine.listUnresolved(db, '10');
  if (quarantine.classifyBlockingRows(unresolved, [original]).blockingCount !== 0) stop('anchor_prepare_existing_unresolved_attempts');
  validatePrepareExistingConfig(config, { now: currentNow(options) });
  validateLivePolicy(program, policyManifest, currentNow(options));
  const envelopeId = `daily_${hash(['10', config.localDay]).slice(0, 24)}`;
  const snapshot = { candidateId: candidate.candidateId, prospectId: candidate.prospectId, companyId: candidate.companyId,
    email: candidate.recipientEmail, message, sender, revision: config.preparedRevision,
    senderResolution: { usedAoSender: false, reason: 'approved_existing_canonical_sender' } };
  const manifestHash = hash([snapshot]);
  return { config, eventId, program, mission, snapshot, envelopeId, itemId: `${envelopeId}_0`, manifestHash };
}

function proposal(plan) {
  const { config, snapshot, envelopeId, itemId, manifestHash, program } = plan;
  return { version: 1, id: null, approvalReference: null, tenantId: '10', channel: 'email', provider: 'brevo', transport: 'governed_brevo',
    programId: program.id, policyHash: program.policy_hash, senderEmail: snapshot.sender.senderEmail, inboxIntegrationId: program.policy.inboxIntegrationId,
    validFrom: config.validFrom, expiresAt: config.expiresAt, maxFreshSends: 1, quarantineDecisionHash: config.quarantineDecisionHash,
    admissions: [{ itemId, envelopeId, prospectId: snapshot.prospectId, companyId: snapshot.companyId, candidateId: snapshot.candidateId,
      recipientEmail: snapshot.email, revision: snapshot.revision, envelopeApprovalId: null, manifestHash,
      subject: snapshot.message.subject, body: snapshot.message.body }] };
}
async function connection(pool) {
  let expired = false, timer;
  const pending = pool.connect().then(client => {
    if (expired) { client.release(true); stop('anchor_prepare_existing_connection_timeout'); }
    return client;
  });
  try { return await Promise.race([pending, new Promise((_, reject) => {
    timer = setTimeout(() => { expired = true; reject(Object.assign(new Error('anchor_prepare_existing_connection_timeout'), { code: 'anchor_prepare_existing_connection_timeout' })); }, 6000);
  })]); } finally { clearTimeout(timer); }
}
async function transaction(pool, write, run) {
  const client = await connection(pool);
  const db = { query: (text, values = []) => client.query({ text, values, query_timeout: 6000 }) };
  let begun = false, discard = false;
  try {
    await db.query(write ? 'BEGIN' : 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY'); begun = true;
    await db.query("SET LOCAL lock_timeout='5s'"); await db.query("SET LOCAL statement_timeout='5s'");
    const result = await run(db);
    await db.query('COMMIT'); begun = false;
    return result;
  } catch (error) { discard = true; throw error; }
  finally { if (begun) { try { await db.query('ROLLBACK'); } catch { discard = true; } } client.release(discard); }
}
async function reviewExistingPreparation(pool, raw, options = {}) {
  try {
    const config = validatePrepareExistingConfig(raw, { now: currentNow(options) });
    const plan = await transaction(pool, false, db => inspect(db, config, options, false));
    return { mode: 'PREPARE_ONLY', ready: true, frozen: false, providerAuthorized: false, envelopeId: plan.envelopeId,
      itemId: plan.itemId, proposedManifest: proposal(plan), stillRequired: ['envelope_execution_approval', 'fresh_send_manifest_approval'] };
  } catch (error) {
    return { mode: 'PREPARE_ONLY', ready: false, frozen: false, providerAuthorized: false,
      blocked: error.code || 'anchor_prepare_existing_read_failed', ...(error.preparationDependency ? { dependency: NEEDS_RESEARCH } : {}) };
  }
}
async function prepareExistingApproved(pool, raw, options = {}) {
  const config = validatePrepareExistingConfig(raw, { now: currentNow(options) });
  const env = options.env || process.env;
  const armed = () => env.ANCHOR_EMAIL_RECOVERY_PREPARE_EXISTING_ID === config.id
    && env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE === 'true' && env.ANCHOR_GOVERNED_OUTBOUND_ENABLED === 'false'
    && [undefined, ''].includes(env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID);
  if (!armed()) stop('anchor_prepare_existing_not_activated');
  return transaction(pool, true, async db => {
    for (const pair of [[GOVERNED_OUTBOUND_SEND_LOCK_NAMESPACE, 10], [EXECUTOR_LOCK_NAMESPACE, 0]]) {
      const lock = await db.query('SELECT pg_try_advisory_xact_lock($1,$2) AS locked', pair);
      if (lock.rows[0]?.locked !== true) stop('anchor_prepare_existing_overlap');
    }
    await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [RECOVERY_LOCK_KEY]);
    const plan = await inspect(db, config, options, true);
    if (!armed()) stop('anchor_prepare_existing_not_activated');
    const envelope = await db.query(`INSERT INTO acquisition_outbound_envelopes
      (id,program_id,tenant_id,local_day,mission_id,revision,manifest,manifest_hash,status)
      VALUES($1,$2,$3,$4::date,$5,$6,$7::jsonb,$8,'frozen') ON CONFLICT DO NOTHING RETURNING id`,
    [plan.envelopeId, config.programId, '10', config.localDay, config.missionId, config.preparedRevision, JSON.stringify([plan.snapshot]), plan.manifestHash]);
    if (envelope.rows.length !== 1) stop('anchor_prepare_existing_envelope_changed');
    const item = await db.query(`INSERT INTO acquisition_outbound_items
      (id,envelope_id,tenant_id,candidate_id,prospect_id,company_id,email,snapshot)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb) RETURNING id`,
    [plan.itemId, plan.envelopeId, '10', plan.snapshot.candidateId, plan.snapshot.prospectId, plan.snapshot.companyId, plan.snapshot.email, JSON.stringify(plan.snapshot)]);
    if (item.rows.length !== 1) stop('anchor_prepare_existing_item_changed');
    const event = await db.query(`INSERT INTO acquisition_outbound_events(id,tenant_id,program_id,envelope_id,item_id,event_type,payload)
      VALUES($1,$2,$3,$4,$5,'anchor_recovery_existing_prepared',$6::jsonb) ON CONFLICT DO NOTHING RETURNING id`,
    [plan.eventId, '10', config.programId, plan.envelopeId, plan.itemId, JSON.stringify({ action: ACTION, approvalReference: config.approvalReference,
      operatorId: config.operatorId, operatorRole: config.operatorRole, testMode: config.testMode, configId: config.id, configHash: hash(config), missionId: config.missionId,
      artifacts: config.artifacts, preparedRevision: config.preparedRevision, manifestHash: plan.manifestHash,
      count: 1, providerAuthorized: false, executionApprovalCreated: false })]);
    if (event.rows.length !== 1) stop('anchor_prepare_existing_grant_consumed');
    return { mode: 'PREPARE_ONLY', ready: true, frozen: true, providerAuthorized: false, envelopeId: plan.envelopeId,
      itemId: plan.itemId, proposedManifest: proposal(plan), stillRequired: ['envelope_execution_approval', 'fresh_send_manifest_approval'] };
  });
}
module.exports = { reviewExistingPreparation, prepareExistingApproved, validatePrepareExistingConfig, contributionSetHash, preparedRevision };
