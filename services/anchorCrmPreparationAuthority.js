'use strict';

// A typed continuation of an expired historical program, not a renewal or a
// send policy. Only the explicit CRM draft action reads this authority. The
// shared recovery/send reader and validateLivePolicy remain fail closed.
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const MAX_WINDOW_MS = 24 * 60 * 60 * 1000;
const POLICY_KEYS = ['tenantId', 'sourceMissionId', 'senderEmail', 'inboxIntegrationId', 'aoOwnerIds', 'startsAt',
  'expiresAt', 'dailyCap', 'totalCap', 'spacingMinutes', 'timeZone', 'weekdays', 'startHour', 'endHour',
  'maxSequenceStep', 'enrichmentLimit', 'preparationAttemptsPerDay', 'allowedContactClassifications',
  'operatorDelegatedMaximumDailyCapacity', 'preparationOnly'];
function fail(code) { throw Object.assign(new Error(code), { code, retryAllowed: false, providerBoundaryCrossed: false }); }
function isPreparationAuthority(manifest) { return manifest?.version === 2 && manifest.kind === 'scoped_resume'; }
// JSON normalization preserves timestamp values across pg Date and JSON inputs;
// every stored column, including authorization/history, participates in the pin.
function programBindingHash(program) {
  function normalized(value) {
    if (value instanceof Date) {
      if (!Number.isFinite(+value)) fail('anchor_crm_preparation_program_invalid');
      return value.toISOString();
    }
    if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (Array.isArray(value)) return value.map(normalized);
    if (value && typeof value === 'object' && [Object.prototype, null].includes(Object.getPrototypeOf(value)))
      return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, normalized(child)]));
    fail('anchor_crm_preparation_program_invalid');
  }
  if (!program || typeof program !== 'object' || Array.isArray(program)) fail('anchor_crm_preparation_program_invalid');
  return hash(normalized(program));
}
function assertPreparationEnvironment(manifest, options = {}) {
  const env = options.env || process.env;
  if (env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE !== 'true'
    || env.ANCHOR_GOVERNED_OUTBOUND_ENABLED !== 'false'
    || env.ANCHOR_RESEARCH_PROVIDERS_PAUSED !== 'true'
    || env.ANCHOR_EMAIL_SCOPED_RESUME_ID !== manifest.id
    || ![undefined, ''].includes(env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID)) fail('anchor_crm_preparation_maintenance_required');
}
function validatePreparationProgram(program, manifest, now = new Date()) {
  const p = program?.policy, a = manifest?.preparationAuthority;
  if (!isPreparationAuthority(manifest) || !a || !p || typeof p !== 'object' || Array.isArray(p)
    || Object.keys(p).some(key => !POLICY_KEYS.includes(key))
    || programBindingHash(program) !== a.programBindingHash
    || program.tenant_id !== '10' || program.id !== manifest.programId || program.mode !== 'active'
    || program.policy_hash !== manifest.policyHash || hash(p) !== manifest.policyHash
    || program.source_mission_id !== manifest.sourceMissionId || program.scope_hash !== manifest.scopeHash
    || String(program.authorized_by) !== a.originalAuthorizerId || p.tenantId !== '10'
    || p.sourceMissionId !== manifest.sourceMissionId || p.senderEmail !== manifest.senderEmail
    || p.inboxIntegrationId !== manifest.inboxIntegrationId) fail('anchor_crm_preparation_program_changed');
  // Independently validate the historical policy's structure. Do not call the
  // send validator with a fabricated clock or temporarily rewritten dates.
  if ((Object.hasOwn(p, 'preparationOnly') && p.preparationOnly !== false)
    || p.maxSequenceStep !== 1 || p.enrichmentLimit !== 15 || p.preparationAttemptsPerDay !== 3
    || p.timeZone !== 'America/New_York' || !Array.isArray(p.weekdays) || hash(p.weekdays) !== hash([1, 2, 3, 4, 5])
    || p.startHour !== 9 || p.endHour !== 17 || !Array.isArray(p.aoOwnerIds)
    || !p.aoOwnerIds.length || p.aoOwnerIds.length > 10 || new Set(p.aoOwnerIds).size !== p.aoOwnerIds.length
    || p.aoOwnerIds.some(id => !Number.isSafeInteger(id) || id < 1)
    || !Number.isSafeInteger(p.dailyCap) || p.dailyCap < 1 || p.dailyCap > 50
    || !Number.isSafeInteger(p.totalCap) || p.totalCap < p.dailyCap || p.totalCap > 100
    || !Number.isSafeInteger(p.spacingMinutes) || p.spacingMinutes < 60 || p.spacingMinutes > 240
    || (Object.hasOwn(p, 'operatorDelegatedMaximumDailyCapacity')
      && (!Number.isSafeInteger(p.operatorDelegatedMaximumDailyCapacity) || p.operatorDelegatedMaximumDailyCapacity < 1
        || p.operatorDelegatedMaximumDailyCapacity > Math.min(50, p.totalCap)))
    || (Object.hasOwn(p, 'allowedContactClassifications') && (!Array.isArray(p.allowedContactClassifications)
      || !p.allowedContactClassifications.length
      || p.allowedContactClassifications.some(x => !['VERIFIED_FOUNDER_EMAIL', 'VERIFIED_ROLE_EMAIL'].includes(x))))) fail('anchor_crm_preparation_historical_policy_invalid');
  const timestamp = require('./anchorScopedResume').timestamp;
  const from = timestamp(p.startsAt), until = timestamp(p.expiresAt);
  if (!Number.isFinite(+now) || !Number.isFinite(from) || !Number.isFinite(until) || until <= from
    || until - from > 30 * 86400000 || until > +now || timestamp(manifest.validFrom) < until)
    fail('anchor_crm_preparation_historical_window_invalid');
}
function assertPreparationSource(source, manifest) {
  if (!source || source.id !== manifest.sourceMissionId || String(source.tenantId) !== '10'
    || source.stage !== 'discover' || source.structuredMission?.immutable !== true
    || source.planCancelled || /cancel/i.test(String(source.status))
    || hash(source) !== manifest.preparationAuthority.sourceMissionHash
    || hash(missionScope(source)) !== manifest.scopeHash) fail('anchor_crm_preparation_source_changed');
}
async function readPreparationState(db, options = {}) {
  const lock = options.lockControl === true;
  const result = await db.query(`SELECT singleton,maintenance_armed,manifest FROM anchor_email_recovery_control${lock ? ' FOR SHARE' : ''}`);
  if (!isPreparationAuthority(result.rows?.[0]?.manifest)) return { mode: 'not_preparation_only' };
  if (result.rows.length !== 1 || result.rows[0].singleton !== true || result.rows[0].maintenance_armed !== true)
    fail('anchor_crm_preparation_maintenance_required');
  const manifest = require('./anchorScopedResume').validateScopedResumeManifest(result.rows[0].manifest, { now: options.now || new Date() });
  assertPreparationEnvironment(manifest, options);
  await require('./anchorScopedResumeControl').assertInstalledScopedResume(db, manifest, { ...options, lock });
  return { mode: 'preparation_only', manifest };
}
async function assertPreparationCapability(db, program, options = {}) {
  await db.query('SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))', ['anchor-email-recovery:10']);
  const state = await readPreparationState(db, { ...options, lockControl: true });
  if (state.mode !== 'preparation_only') return null;
  const manifest = state.manifest;
  if (program?.id !== manifest.programId || program.tenant_id !== '10' || program.policy_hash !== manifest.policyHash)
    fail('anchor_crm_preparation_program_changed');
  const programs = await db.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2 FOR SHARE', [manifest.programId, '10']);
  if (programs.rows.length !== 1) fail('anchor_crm_preparation_program_changed');
  const live = programs.rows[0];
  validatePreparationProgram(live, manifest, options.now || new Date());
  const sources = await db.query('SELECT id,tenant_id,payload FROM acquisition_missions WHERE id=$1 AND tenant_id=$2 FOR SHARE', [manifest.sourceMissionId, '10']);
  if (sources.rows.length !== 1 || sources.rows[0].id !== manifest.sourceMissionId || sources.rows[0].tenant_id !== '10')
    fail('anchor_crm_preparation_source_changed');
  assertPreparationSource(sources.rows[0].payload, manifest);
  await require('./anchorEmailQuarantine').requireQuarantine(db, manifest.quarantineDecisionHash);
  return { legacy: false, manifest, resumeGrant: manifest, program: live, capability: 'preparation',
    operation: 'prepare_existing_crm', scope: manifest.scopes.preparation, manifestHash: hash(manifest) };
}
module.exports = { MAX_WINDOW_MS, isPreparationAuthority, programBindingHash, assertPreparationEnvironment,
  validatePreparationProgram, assertPreparationSource, readPreparationState, assertPreparationCapability };
