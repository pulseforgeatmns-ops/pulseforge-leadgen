'use strict';
// SPEC-SCOUT-P0-003: durable research-only authority for Tenant 10 Scout
// discovery while the email recovery stays in maintenance. A grant is an
// admin-session-approved, hash-pinned record: at most 8 discovery cycles, a
// Tenant 10 Google ceiling of at most $5 per UTC day, an expiry and a revocation.
// It never grants enrichment, verification, preparation or outbound, never reads
// or changes the prepared envelope, and never reinterprets the quarantine.
// No startup DDL, environment mutation or schema creation lives here.
const { AsyncLocalStorage } = require('node:async_hooks');
const { randomUUID } = require('node:crypto');
const { hash } = require('./scoutBoundedAcquisitionBudget');
const { PLACES_UPPER_BOUND } = require('../utils/apiPricing');

const RECOVERY_LOCK_KEY = 'anchor-email-recovery:10';
const SPEND_LOCK_KEY = 'scout-research-google:10';
const MAX_CYCLES = 8;
const INITIAL_RELEASED_CYCLES = 1;
const MAX_DAILY_USD_MICROS = 5000000;
const MAX_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;
const CYCLE_LEASE_MS = 30 * 60 * 1000;
const PERMISSIONS = Object.freeze({ discovery: true, enrichment: false, verification: false, preparation: false, outbound: false });
const PROVIDERS = Object.freeze(['google_places']);
// missionId/sourceHash pin the unchanged outbound mission; discovery runs only
// under the separately approved research mission (id + exact hash).
const MANIFEST_KEYS = ['version', 'kind', 'approvalRef', 'tenantId', 'programId', 'missionId', 'programHash', 'sourceHash',
  'researchMissionId', 'researchMissionHash', 'fingerprint', 'expectedRevision', 'quarantineDecisionHash', 'recoveryControlHash', 'operator', 'authorizationEvidence',
  'maxCycles', 'dailyGoogleUsdMicros', 'notBefore', 'expiresAt', 'permissions', 'providers'];
const OPERATOR_KEYS = ['id', 'role', 'accountClientId', 'sessionHash'];
const PROGRAM_COLUMNS = 'id,tenant_id,source_mission_id,policy,policy_hash,scope_hash,authorized_by,authorized_at,mode';
const HEX64 = /^[a-f0-9]{64}$/;
const SPEND_CODE = 'research_only_google_spend_denied';

const cycleScope = new AsyncLocalStorage();
const attemptScope = new AsyncLocalStorage();

function fail(code, reason) {
  throw Object.assign(new Error(reason ? `${code}:${reason}` : code), { code, reason: reason || code, retryAllowed: false });
}
function exact(value, keys) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
}
function text(value, max = 512) {
  return typeof value === 'string' && value.length > 0 && value.length <= max && value === value.trim()
    && !/[\u0000-\u001f\u007f]/.test(value);
}
function isoTime(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value)
    && Number.isFinite(Date.parse(value)) && new Date(Date.parse(value)).toISOString().slice(0, 19) === value.slice(0, 19);
}
function schedulerEnabled(env = process.env) { return env.SCOUT_RESEARCH_ONLY_GRANTS_ENABLED === 'true'; }

function validateManifest(raw) {
  const invalid = reason => fail('research_only_manifest_invalid', reason);
  if (!exact(raw, MANIFEST_KEYS)) invalid('fields');
  if (raw.version !== 2 || raw.kind !== 'scout_research_only' || raw.tenantId !== '10') invalid('scope');
  if (!/^[a-zA-Z0-9_.:-]{1,120}$/.test(raw.approvalRef || '')) invalid('approval_ref');
  if (!text(raw.programId) || !text(raw.missionId)) invalid('program');
  if (!require('./scoutResearchMission').ID.test(raw.researchMissionId || '')) invalid('research_mission');
  if (!['programHash', 'sourceHash', 'researchMissionHash', 'fingerprint', 'quarantineDecisionHash', 'recoveryControlHash']
    .every(key => HEX64.test(raw[key] || ''))) invalid('hash');
  if (!/^[a-f0-9]{40}$/.test(raw.expectedRevision || '')) invalid('revision');
  const operator = raw.operator;
  if (!exact(operator, OPERATOR_KEYS) || !/^[1-9][0-9]{0,18}$/.test(operator.id || '') || operator.role !== 'admin'
    || ![null, 10].includes(operator.accountClientId) || !HEX64.test(operator.sessionHash || '')) invalid('operator');
  const evidence = raw.authorizationEvidence;
  if (!exact(evidence, ['kind', 'statement']) || evidence.kind !== 'admin_session_approval'
    || !text(evidence.statement, 2000)) invalid('evidence');
  if (raw.maxCycles !== MAX_CYCLES) invalid('cycles');
  if (!Number.isSafeInteger(raw.dailyGoogleUsdMicros) || raw.dailyGoogleUsdMicros < 1
    || raw.dailyGoogleUsdMicros > MAX_DAILY_USD_MICROS) invalid('budget');
  if (!isoTime(raw.notBefore) || !isoTime(raw.expiresAt) || Date.parse(raw.expiresAt) <= Date.parse(raw.notBefore)
    || Date.parse(raw.expiresAt) - Date.parse(raw.notBefore) > MAX_WINDOW_MS) invalid('window');
  if (!exact(raw.permissions, Object.keys(PERMISSIONS))
    || Object.entries(PERMISSIONS).some(([key, value]) => raw.permissions[key] !== value)) invalid('permissions');
  if (!Array.isArray(raw.providers) || JSON.stringify(raw.providers) !== JSON.stringify(PROVIDERS)) invalid('providers');
  return Object.freeze(JSON.parse(JSON.stringify(raw)));
}

// The closed state is a precondition, never something a grant lowers. Sending
// stays off, the research-provider pause stays armed (only the metered Places
// transport opens inside a claimed cycle) and durable discovery memory is on.
function assertEnvironment(manifest, env = process.env) {
  if (!schedulerEnabled(env)) fail('research_only_disabled');
  if (env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE !== 'true' || env.ANCHOR_RESEARCH_PROVIDERS_PAUSED !== 'true'
    || env.ANCHOR_GOVERNED_OUTBOUND_ENABLED !== 'false'
    || ![undefined, ''].includes(env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID)
    || ![undefined, ''].includes(env.ANCHOR_EMAIL_SCOPED_RESUME_ID)) fail('research_only_closed_environment_required');
  if (env.SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED !== 'true') fail('research_only_discovery_memory_required');
  if (manifest && env.RAILWAY_GIT_COMMIT_SHA !== manifest.expectedRevision) fail('research_only_revision_mismatch');
}

async function assertSchema(db) {
  try {
    await db.query(`SELECT grant_id,approval_ref,tenant_id,program_id,mission_id,research_mission_id,search_fingerprint,manifest,operator_id,operator_role,
      approver_session_hash,max_cycles,daily_google_usd_micros,not_before,expires_at,installed_at,revoked_at,revoke_approval_ref,revoked_by
      FROM scout_research_only_grants LIMIT 0`);
    await db.query(`SELECT grant_id,cycle_number,tenant_id,cycle_id,execution_token,state,claimed_at,lease_expires_at,finished_at,result
      FROM scout_research_only_cycles LIMIT 0`);
    await db.query(`SELECT id,tenant_id,grant_id,cycle_number,usage_day,operation,reserved_usd_micros,api_usage_id,state,http_status,created_at,finished_at
      FROM scout_research_google_spend LIMIT 0`);
    await db.query('SELECT release_id,grant_id,tenant_id,previous_allowance,released_cycles,administrator_id,approver_session_hash,approval_reference,authorization_statement,kind,created_at FROM scout_research_cycle_releases LIMIT 0');
  } catch { fail('research_only_schema_unavailable'); }
  const guards = (await db.query(`SELECT count(*)::int AS count FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='O'
    AND tgname = ANY($1::text[])`, [['scout_research_only_grants_no_delete', 'scout_research_only_grants_revoke_only',
    'scout_research_only_cycles_no_delete', 'scout_research_only_cycles_forward_only',
    'scout_research_google_spend_no_delete', 'scout_research_google_spend_settle']])).rows[0];
  const indexes = (await db.query(`SELECT count(*)::int AS count FROM pg_index i WHERE i.indisunique AND i.indisvalid AND i.indisready
    AND i.indexrelid IN (to_regclass('public.scout_research_only_one_open_tenant'), to_regclass('public.scout_research_only_one_live_cycle'))`)).rows[0];
  if (guards?.count !== 6 || indexes?.count !== 2) fail('research_only_schema_guards_missing');
  const releaseGuards = (await db.query(`SELECT count(*)::int AS count FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='O'
    AND tgname=ANY($1::text[])`, [['scout_research_releases_no_delete', 'scout_research_releases_no_update',
    'scout_research_releases_forward', 'scout_research_cycles_release_guard']])).rows[0];
  if (releaseGuards?.count !== 4) fail('research_only_schema_guards_missing');
  await require('./scoutResearchMission').assertSchema(db)
    .catch(error => fail(error.code === 'research_mission_schema_guards_missing' ? 'research_only_schema_guards_missing' : 'research_only_schema_unavailable'));
}

async function one(db, sql, values, code) {
  const { rows } = await db.query(sql, values);
  if (rows.length !== 1) fail(code);
  return rows[0];
}
async function dbNow(db) { return new Date((await db.query('SELECT clock_timestamp() AS now')).rows[0].now); }

// The operator must still be an active, verified admin bound to the same
// account. The approving session is checked at install; a scheduled cycle
// re-checks the account so a deactivated or demoted admin's grant stops.
async function assertOperatorAccount(db, operator, { lock = true } = {}) {
  const user = (await db.query(`SELECT id,role,active,client_id,email_verified,password_change_required
    FROM users WHERE id::text=$1${lock ? ' FOR SHARE' : ''}`, [operator.id])).rows[0];
  if (!user || String(user.id) !== operator.id || user.role !== 'admin' || user.active !== true
    || user.email_verified === false || user.password_change_required === true
    || (user.client_id == null ? null : Number(user.client_id)) !== operator.accountClientId) fail('research_only_operator_invalid');
  return user;
}
function approverIdentity(manifest) {
  return { operatorId: manifest.operator.id, operatorRole: 'admin',
    preparationAuthority: { approverSessionHash: manifest.operator.sessionHash, approverAccountClientId: manifest.operator.accountClientId } };
}

// Everything the grant is pinned to, read under share locks when executing.
async function liveBindings(db, manifest, { lock = true } = {}) {
  const share = lock ? ' FOR SHARE' : '';
  const control = (await db.query(`SELECT singleton,maintenance_armed,manifest FROM anchor_email_recovery_control${share}`)).rows;
  if (control.length !== 1 || control[0].singleton !== true || control[0].maintenance_armed !== true || control[0].manifest !== null
    || hash(control[0]) !== manifest.recoveryControlHash) fail('research_only_closed_control_required');
  if ((await db.query("SELECT 1 FROM anchor_scoped_resume_grants WHERE tenant_id='10' AND revoked_at IS NULL LIMIT 1")).rows.length)
    fail('research_only_other_authority_open');
  if ((await db.query("SELECT to_regclass('public.scout_bounded_research_grants') AS t")).rows[0].t
    && (await db.query("SELECT 1 FROM scout_bounded_research_grants WHERE tenant_id='10' AND revoked_at IS NULL LIMIT 1")).rows.length)
    fail('research_only_other_authority_open');
  // The permanent quarantine is a precondition. It is never resolved, classified,
  // drained or bypassed: the decision record must still exist exactly.
  if (lock) await db.query("SELECT decision_hash FROM anchor_email_quarantines WHERE tenant_id='10' AND decision_hash=$1 FOR SHARE",
    [manifest.quarantineDecisionHash]);
  await require('./anchorEmailQuarantine').requireQuarantine(db, manifest.quarantineDecisionHash);
  const program = await one(db, `SELECT ${PROGRAM_COLUMNS} FROM acquisition_outbound_programs WHERE tenant_id='10' AND id=$1${share}`,
    [manifest.programId], 'research_only_program_changed');
  const source = await one(db, `SELECT id,objective,target_segment,payload FROM acquisition_missions WHERE tenant_id='10' AND id=$1${share}`,
    [manifest.missionId], 'research_only_mission_changed');
  if (program.mode === 'revoked' || program.source_mission_id !== manifest.missionId || hash(program) !== manifest.programHash)
    fail('research_only_program_changed');
  if (hash(source) !== manifest.sourceHash || source.payload?.id !== source.id) fail('research_only_mission_changed');
  // Discovery scope is the research mission, never the outbound mission.
  const missions = require('./scoutResearchMission');
  const research = await missions.loadResearchMission(db, { id: manifest.researchMissionId, missionHash: manifest.researchMissionHash,
    program, lock }).catch(() => fail('research_only_research_mission_changed'));
  const researchSource = missions.researchSource(research);
  const { scoutSearchFingerprint } = require('./maxOutboundControlLoop');
  if (scoutSearchFingerprint(missions.researchProgram(program, research), researchSource) !== manifest.fingerprint)
    fail('research_only_research_mission_changed');
  return { program, source, research, researchSource };
}
function assertWindow(manifest, now) {
  if (+now < Date.parse(manifest.notBefore) || +now >= Date.parse(manifest.expiresAt)) fail('research_only_grant_window_inactive');
}

async function transaction(pool, work, { readOnly = false } = {}) {
  const db = await pool.connect(); let committing = false, discard = false;
  try {
    await db.query(readOnly ? 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY' : 'BEGIN');
    await db.query("SET LOCAL lock_timeout='2s'");
    await db.query("SET LOCAL statement_timeout='15s'");
    const result = await work(db);
    committing = true; await db.query('COMMIT'); committing = false;
    return result;
  } catch (failure) {
    try { await db.query('ROLLBACK'); } catch { discard = true; }
    if (committing) fail('research_only_commit_uncertain');
    throw failure;
  } finally { db.release(discard || committing); }
}
async function recoveryLock(db, exclusive = false) {
  await db.query(`SELECT pg_advisory_xact_lock${exclusive ? '' : '_shared'}(hashtextextended($1,0))`, [RECOVERY_LOCK_KEY]);
}
function storedManifest(row) {
  const manifest = validateManifest(row.manifest);
  if (hash(manifest) !== row.grant_id || row.approval_ref !== manifest.approvalRef || row.tenant_id !== '10'
    || row.program_id !== manifest.programId || row.mission_id !== manifest.missionId || row.search_fingerprint !== manifest.fingerprint
    || row.research_mission_id !== manifest.researchMissionId
    || row.operator_id !== manifest.operator.id || row.approver_session_hash !== manifest.operator.sessionHash
    || row.max_cycles !== manifest.maxCycles || row.daily_google_usd_micros !== manifest.dailyGoogleUsdMicros
    || +new Date(row.not_before) !== Date.parse(manifest.notBefore) || +new Date(row.expires_at) !== Date.parse(manifest.expiresAt))
    fail('research_only_grant_conflict');
  return manifest;
}

function summary(manifest) {
  return { grantId: hash(manifest), approvalRef: manifest.approvalRef, tenantId: '10', programId: manifest.programId,
    missionId: manifest.missionId, researchMissionId: manifest.researchMissionId, researchMissionHash: manifest.researchMissionHash,
    fingerprint: manifest.fingerprint, expectedRevision: manifest.expectedRevision,
    quarantineDecisionHash: manifest.quarantineDecisionHash, operatorId: manifest.operator.id,
    maxCycles: manifest.maxCycles, initiallyReleasedCycles: INITIAL_RELEASED_CYCLES, dailyGoogleUsdMicros: manifest.dailyGoogleUsdMicros,
    notBefore: manifest.notBefore, expiresAt: manifest.expiresAt, permissions: { ...PERMISSIONS }, providers: [...PROVIDERS],
    preparationInvoked: false, sent: 0, retryAllowed: false };
}

// Preview derives the exact manifest from live state for the signed-in admin.
// It is read-only: no grant, cycle, spend, provider call or envelope change.
async function previewResearchOnlyGrant(pool, input, actor, { env = process.env } = {}) {
  if (!exact(input, ['approvalRef', 'expiresAt', 'statement', 'researchMissionId']) || !/^[a-zA-Z0-9_.:-]{1,120}$/.test(input.approvalRef || '')
    || !isoTime(input.expiresAt) || !text(input.statement, 2000)) fail('research_only_preview_invalid');
  const identity = require('./anchorCrmPreparationIdentity');
  if (!actor || actor.role !== 'admin' || !/^[1-9][0-9]{0,18}$/.test(String(actor.id || ''))) fail('research_only_operator_invalid');
  return transaction(pool, async db => {
    await assertSchema(db);
    const account = (await db.query('SELECT client_id FROM users WHERE id::text=$1', [String(actor.id)])).rows[0];
    if (!account) fail('research_only_operator_invalid');
    const user = await assertOperatorAccount(db, { id: String(actor.id),
      accountClientId: account.client_id == null ? null : Number(account.client_id) }, { lock: false });
    const program = (await db.query(`SELECT ${PROGRAM_COLUMNS} FROM acquisition_outbound_programs
      WHERE tenant_id='10' AND mode<>'revoked' ORDER BY authorized_at DESC LIMIT 1`)).rows[0];
    if (!program) fail('research_only_program_changed');
    const source = await one(db, "SELECT id,objective,target_segment,payload FROM acquisition_missions WHERE tenant_id='10' AND id=$1",
      [program.source_mission_id], 'research_only_mission_changed');
    const control = (await db.query('SELECT singleton,maintenance_armed,manifest FROM anchor_email_recovery_control')).rows;
    const missions = require('./scoutResearchMission');
    const research = (await db.query("SELECT * FROM scout_research_missions WHERE tenant_id='10' AND id=$1", [input.researchMissionId])).rows[0];
    if (!research) fail('research_only_research_mission_changed');
    const quarantines = (await db.query("SELECT decision_hash FROM anchor_email_quarantines WHERE tenant_id='10' ORDER BY decision_hash")).rows;
    if (quarantines.length !== 1) fail('research_only_quarantine_ambiguous');
    const now = await dbNow(db);
    const notBefore = new Date(Math.floor(+now / 1000) * 1000).toISOString();
    const manifest = validateManifest({ version: 2, kind: 'scout_research_only', approvalRef: input.approvalRef, tenantId: '10',
      programId: program.id, missionId: source.id, programHash: hash(program), sourceHash: hash(source),
      researchMissionId: research.id, researchMissionHash: research.mission_hash,
      fingerprint: require('./maxOutboundControlLoop').scoutSearchFingerprint(missions.researchProgram(program, research), missions.researchSource(research)),
      expectedRevision: /^[a-f0-9]{40}$/.test(env.RAILWAY_GIT_COMMIT_SHA || '') ? env.RAILWAY_GIT_COMMIT_SHA : '0'.repeat(40),
      quarantineDecisionHash: quarantines[0].decision_hash, recoveryControlHash: hash(control[0] || null),
      operator: { id: String(user.id), role: 'admin', accountClientId: user.client_id == null ? null : Number(user.client_id),
        sessionHash: identity.sessionBindingHash(actor.sessionId) },
      authorizationEvidence: { kind: 'admin_session_approval', statement: input.statement },
      maxCycles: MAX_CYCLES, dailyGoogleUsdMicros: MAX_DAILY_USD_MICROS, notBefore, expiresAt: input.expiresAt,
      permissions: { ...PERMISSIONS }, providers: [...PROVIDERS] });
    const blockers = [];
    const check = async (fn) => { try { await fn(); } catch (error) { blockers.push(error.code || 'research_only_preview_failed'); } };
    await check(() => assertEnvironment(manifest, env));
    await check(() => liveBindings(db, manifest, { lock: false }));
    await check(() => identity.assertPreparationApprover(db, approverIdentity(manifest), { lock: false, sessionId: actor.sessionId }));
    await check(() => assertWindow(manifest, now));
    const open = (await db.query("SELECT grant_id FROM scout_research_only_grants WHERE tenant_id='10' AND revoked_at IS NULL")).rows;
    if (open.length && open[0].grant_id !== hash(manifest)) blockers.push('research_only_other_grant_open');
    return { ...summary(manifest), manifest, manifestHash: hash(manifest), status: blockers.length ? 'blocked' : 'previewed',
      blockers, installable: blockers.length === 0, previewOnly: true };
  }, { readOnly: true });
}

async function installResearchOnlyGrant(pool, input, actor, { env = process.env } = {}) {
  if (!exact(input, ['manifest', 'manifestHash']) || !HEX64.test(input.manifestHash || '')) fail('research_only_explicit_approval_required');
  const manifest = validateManifest(input.manifest);
  if (hash(manifest) !== input.manifestHash) fail('research_only_exact_confirmation_required');
  const identity = require('./anchorCrmPreparationIdentity');
  if (!actor || String(actor.id) !== manifest.operator.id || actor.role !== 'admin'
    || identity.sessionBindingHash(actor.sessionId) !== manifest.operator.sessionHash) fail('research_only_operator_invalid');
  assertEnvironment(manifest, env);
  return transaction(pool, async db => {
    await recoveryLock(db, true);
    await assertSchema(db);
    await identity.assertPreparationApprover(db, approverIdentity(manifest), { sessionId: actor.sessionId });
    await assertOperatorAccount(db, manifest.operator);
    await liveBindings(db, manifest);
    const existing = (await db.query(`SELECT * FROM scout_research_only_grants WHERE grant_id=$1 OR approval_ref=$2 FOR UPDATE`,
      [hash(manifest), manifest.approvalRef])).rows;
    if (existing.length > 1 || (existing.length === 1 && existing[0].grant_id !== hash(manifest))) fail('research_only_grant_conflict');
    if (existing[0]) {
      storedManifest(existing[0]);
      if (existing[0].revoked_at) fail('research_only_grant_revoked');
      return { ...summary(manifest), releasedCycles: await releasedAllowance(db, hash(manifest)), status: 'installed', installed: false, idempotent: true };
    }
    if ((await db.query("SELECT 1 FROM scout_research_only_grants WHERE tenant_id='10' AND revoked_at IS NULL")).rows.length)
      fail('research_only_other_grant_open');
    assertWindow(manifest, await dbNow(db));
    await db.query(`INSERT INTO scout_research_only_grants(grant_id,approval_ref,tenant_id,program_id,mission_id,research_mission_id,search_fingerprint,
      manifest,operator_id,operator_role,approver_session_hash,max_cycles,daily_google_usd_micros,not_before,expires_at)
      VALUES($1,$2,'10',$3,$4,$13,$5,$6,$7,'admin',$8,$9,$10,$11,$12)`,
    [hash(manifest), manifest.approvalRef, manifest.programId, manifest.missionId, manifest.fingerprint, JSON.stringify(manifest),
      manifest.operator.id, manifest.operator.sessionHash, manifest.maxCycles, manifest.dailyGoogleUsdMicros,
      manifest.notBefore, manifest.expiresAt, manifest.researchMissionId]);
    // Installation releases one cycle, not the eight-cycle ceiling. An older
    // grant is never backfilled on an idempotent installation retry.
    await appendRelease(db, { grantId: hash(manifest), previousAllowance: 0, newAllowance: INITIAL_RELEASED_CYCLES,
      operator: manifest.operator, approvalReference: manifest.approvalRef,
      statement: manifest.authorizationEvidence.statement, expectedRevision: manifest.expectedRevision }, 'initial_install');
    return { ...summary(manifest), releasedCycles: INITIAL_RELEASED_CYCLES, status: 'installed', installed: true, idempotent: false };
  });
}

// Revocation only closes authority, so it needs no window, revision or live
// binding: an active tenant admin or the operator CLI can always revoke.
// It takes effect before the next cycle claim and the next Google reservation.
async function revokeResearchOnlyGrant(pool, input, actor) {
  if (!exact(input, ['grantId', 'approvalReference']) || !HEX64.test(input.grantId || '')
    || !/^[a-zA-Z0-9_.:-]{1,120}$/.test(input.approvalReference || '')) fail('research_only_explicit_revocation_required');
  return transaction(pool, async db => {
    await assertSchema(db);
    let revokedBy;
    if (actor?.kind === 'operator_cli') revokedBy = 'operator_cli';
    else {
      if (!actor || actor.role !== 'admin' || !/^[1-9][0-9]{0,18}$/.test(String(actor.id || ''))) fail('research_only_operator_invalid');
      const user = (await db.query('SELECT id,role,active FROM users WHERE id::text=$1', [String(actor.id)])).rows[0];
      if (!user || user.role !== 'admin' || user.active !== true) fail('research_only_operator_invalid');
      revokedBy = `admin:${user.id}`;
    }
    const grant = await one(db, "SELECT * FROM scout_research_only_grants WHERE grant_id=$1 AND tenant_id='10' FOR UPDATE",
      [input.grantId], 'research_only_grant_not_found');
    if (grant.revoked_at) return { grantId: input.grantId, status: 'revoked', revoked: false, idempotent: true };
    await db.query(`UPDATE scout_research_only_grants SET revoked_at=clock_timestamp(),revoke_approval_ref=$2,revoked_by=$3
      WHERE grant_id=$1 AND revoked_at IS NULL`, [input.grantId, input.approvalReference, revokedBy]);
    return { grantId: input.grantId, status: 'revoked', revoked: true, idempotent: false };
  });
}

async function releasedAllowance(db, grantId) {
  return Number((await db.query('SELECT COALESCE(max(released_cycles),0)::int AS n FROM scout_research_cycle_releases WHERE grant_id=$1', [grantId])).rows[0].n);
}
async function appendRelease(db, release, kind) {
  const releaseId = hash({ auditKind: kind, ...release });
  await db.query(`INSERT INTO scout_research_cycle_releases(release_id,grant_id,tenant_id,previous_allowance,released_cycles,
    administrator_id,approver_session_hash,approval_reference,authorization_statement,kind)
    VALUES($1,$2,'10',$3,$4,$5,$6,$7,$8,$9)`, [releaseId, release.grantId, release.previousAllowance, release.newAllowance,
  release.operator.id, release.operator.sessionHash, release.approvalReference, release.statement, kind]);
  return releaseId;
}
const RELEASE_KEYS = ['version','kind','tenantId','grantId','previousAllowance','newAllowance','expectedRevision','operator','approvalReference','statement'];
function validateCycleRelease(release) {
  const op = release?.operator;
  if (!exact(release, RELEASE_KEYS) || release.version !== 1 || release.kind !== 'scout_research_cycle_release'
    || release.tenantId !== '10' || !HEX64.test(release.grantId || '') || !/^[a-f0-9]{40}$/.test(release.expectedRevision || '')
    || !Number.isSafeInteger(release.previousAllowance) || release.previousAllowance < 1
    || release.newAllowance !== release.previousAllowance + 1 || release.newAllowance > MAX_CYCLES
    || !/^[a-zA-Z0-9_.:-]{1,120}$/.test(release.approvalReference || '') || !text(release.statement, 2000)
    || !exact(op, OPERATOR_KEYS) || !/^[1-9][0-9]{0,18}$/.test(op.id || '') || op.role !== 'admin'
    || ![null, 10].includes(op.accountClientId) || !HEX64.test(op.sessionHash || '')) fail('research_only_release_invalid');
  return release;
}
async function releaseBindings(db, grantId, actor, env, lock) {
  if (!HEX64.test(grantId || '') || !actor || actor.role !== 'admin') fail('research_only_operator_invalid');
  const grant = await one(db, `SELECT * FROM scout_research_only_grants WHERE grant_id=$1 AND tenant_id='10'${lock ? ' FOR UPDATE' : ''}`,
    [grantId], 'research_only_grant_not_found');
  const manifest = storedManifest(grant);
  if (grant.revoked_at) fail('research_only_grant_revoked');
  assertEnvironment(manifest, env);
  assertWindow(manifest, await dbNow(db));
  await assertOperatorAccount(db, manifest.operator, { lock });
  await liveBindings(db, manifest, { lock });
  const account = (await db.query('SELECT client_id,email_verified FROM users WHERE id::text=$1', [String(actor.id)])).rows[0];
  if (!account || account.email_verified !== true) fail('research_only_operator_invalid');
  const operator = { id: String(actor.id), role: 'admin', accountClientId: account.client_id == null ? null : Number(account.client_id),
    sessionHash: require('./anchorCrmPreparationIdentity').sessionBindingHash(actor.sessionId) };
  await assertOperatorAccount(db, operator, { lock });
  await require('./anchorCrmPreparationIdentity').assertPreparationApprover(db, approverIdentity({ operator }), { sessionId: actor.sessionId, lock });
  return { grant, manifest, operator };
}
async function assertReleaseDue(db, grant, previousAllowance) {
  const allowance = await releasedAllowance(db, grant.grant_id);
  if (!allowance) fail('research_only_release_initial_missing');
  if (allowance >= grant.max_cycles) fail('research_only_cycles_exhausted');
  if (previousAllowance !== undefined && allowance !== previousAllowance) fail('research_only_release_allowance_changed');
  const counts = (await db.query(`SELECT count(*)::int AS consumed,
    count(*) FILTER(WHERE state='claimed' AND lease_expires_at>clock_timestamp())::int AS active
    FROM scout_research_only_cycles WHERE grant_id=$1`, [grant.grant_id])).rows[0];
  if (counts.consumed < allowance || counts.active > 0) fail('research_only_release_not_due');
  return allowance;
}
// Read-only preview, bound to the current session, exact grant and old allowance.
async function previewResearchCycleRelease(pool, input, actor, { env = process.env } = {}) {
  if (!exact(input, ['grantId','approvalReference','statement']) || !/^[a-zA-Z0-9_.:-]{1,120}$/.test(input.approvalReference || '')
    || !text(input.statement, 2000)) fail('research_only_release_invalid');
  return transaction(pool, async db => {
    await assertSchema(db);
    const { grant, manifest, operator } = await releaseBindings(db, input.grantId, actor, env, false);
    const previousAllowance = await assertReleaseDue(db, grant);
    const release = validateCycleRelease({ version: 1, kind: 'scout_research_cycle_release', tenantId: '10', grantId: input.grantId,
      previousAllowance, newAllowance: previousAllowance + 1, expectedRevision: manifest.expectedRevision, operator,
      approvalReference: input.approvalReference, statement: input.statement });
    return { ...summary(manifest), status: 'previewed', previewOnly: true, releasable: true, release, releaseHash: hash(release) };
  }, { readOnly: true });
}
async function releaseResearchCycle(pool, input, actor, { env = process.env } = {}) {
  if (!exact(input, ['release','releaseHash']) || !HEX64.test(input.releaseHash || '')) fail('research_only_release_invalid');
  const release = validateCycleRelease(input.release);
  if (hash(release) !== input.releaseHash) fail('research_only_release_exact_confirmation_required');
  const identity = require('./anchorCrmPreparationIdentity');
  if (!actor || actor.role !== 'admin' || String(actor.id) !== release.operator.id
    || identity.sessionBindingHash(actor.sessionId) !== release.operator.sessionHash) fail('research_only_operator_invalid');
  return transaction(pool, async db => {
    await recoveryLock(db, true);
    await assertSchema(db);
    const { grant, manifest, operator } = await releaseBindings(db, release.grantId, actor, env, true);
    if (manifest.expectedRevision !== release.expectedRevision || hash(operator) !== hash(release.operator)) fail('research_only_release_invalid');
    const releaseId = hash({ auditKind: 'administrative', ...release });
    const existing = (await db.query('SELECT release_id FROM scout_research_cycle_releases WHERE release_id=$1 OR approval_reference=$2',
      [releaseId, release.approvalReference])).rows;
    if (existing.length) {
      if (existing.length !== 1 || existing[0].release_id !== releaseId) fail('research_only_release_conflict');
      return { grantId: release.grantId, releaseId, status: 'released', idempotent: true, releasedCycles: await releasedAllowance(db, release.grantId) };
    }
    await assertReleaseDue(db, grant, release.previousAllowance);
    // Single-use, purpose-bound password confirmation is consumed atomically
    // with the release. A concurrent request or logout cannot resurrect it.
    if (!await require('../utils/freshConfirmation').consume(db, { sessionId: actor.sessionId,
      userId: actor.id, purpose: 'scout_research_cycle_release' })) fail('fresh_confirmation_required');
    await appendRelease(db, release, 'administrative');
    return { grantId: release.grantId, releaseId, status: 'released', idempotent: false, releasedCycles: release.newAllowance };
  });
}

async function inspectResearchOnlyGrant(pool, grantId) {
  if (grantId !== undefined && !HEX64.test(grantId || '')) fail('research_only_grant_not_found');
  return transaction(pool, async db => {
    await assertSchema(db);
    const grant = (await db.query(`SELECT * FROM scout_research_only_grants WHERE tenant_id='10'
      AND ($1::text IS NULL AND revoked_at IS NULL OR grant_id=$1) ORDER BY installed_at DESC LIMIT 1`, [grantId ?? null])).rows[0];
    const now = await dbNow(db);
    if (!grant) return { status: 'not_installed', observedAt: now.toISOString() };
    const manifest = storedManifest(grant);
    const cycles = (await db.query(`SELECT cycle_number,cycle_id,state,claimed_at,lease_expires_at,finished_at,result FROM scout_research_only_cycles
      WHERE grant_id=$1 ORDER BY cycle_number`, [grant.grant_id])).rows;
    const spend = (await db.query(`SELECT usage_day::text AS day,state,operation,count(*)::int AS attempts,sum(reserved_usd_micros)::int AS micros
      FROM scout_research_google_spend WHERE grant_id=$1 GROUP BY usage_day,state,operation ORDER BY usage_day,operation,state`, [grant.grant_id])).rows;
    const releases = (await db.query(`SELECT release_id,previous_allowance,released_cycles,administrator_id,approval_reference,
      authorization_statement,kind,created_at FROM scout_research_cycle_releases WHERE grant_id=$1 ORDER BY released_cycles`, [grant.grant_id])).rows;
    const releasedCycles = releases.at(-1)?.released_cycles || 0;
    return { ...summary(manifest), observedAt: now.toISOString(),
      status: grant.revoked_at ? 'revoked' : +now >= +new Date(grant.expires_at) ? 'expired'
        : cycles.length >= grant.max_cycles ? 'exhausted' : cycles.length >= releasedCycles ? 'awaiting_cycle_release' : 'installed',
      releasedCycles, releasedCyclesRemaining: Math.max(0, releasedCycles - cycles.length), releases,
      cycleInProgress: cycles.some(row => row.state === 'claimed' && +new Date(row.lease_expires_at) > +now),
      installedAt: grant.installed_at, revokedAt: grant.revoked_at, revokedBy: grant.revoked_by,
      cyclesConsumed: cycles.length, cyclesRemaining: Math.max(0, grant.max_cycles - cycles.length), cycles,
      // Reservations are priced at the highest public field tier before each
      // attempt and never refunded: an upper bound, not Google's invoice.
      spend, reservedUsdMicros: spend.reduce((sum, row) => sum + row.micros, 0), spendBasis: 'reserved_upper_bound',
      candidates: await candidateAccounting(db, grant, cycles) };
  }, { readOnly: true });
}

// Ticket outcomes that still need first-party business-type evidence. The
// legacy reason predates parking; both are preserved, never discarded.
const AWAITING_EVIDENCE = Object.freeze(['business_evidence_not_authorized', 'evidence_budget_deferred']);
const CANDIDATE_LIST_LIMIT = 200;

// Read-only. Retained rows are this grant's own records. The ticket ledger is
// keyed by search fingerprint, so it is reported with that scope stated: any
// other Scout pass on the same fingerprint shares it.
async function candidateAccounting(db, grant, cycles) {
  const retainedRows = (await db.query(`SELECT cycle_number,company,domain,vertical,location,reconciled_at IS NOT NULL AS reconciled
    FROM scout_research_candidates WHERE grant_id=$1 AND tenant_id='10' ORDER BY cycle_number,id LIMIT $2`,
  [grant.grant_id, CANDIDATE_LIST_LIMIT])).rows;
  const retainedTotal = (await db.query(`SELECT count(*)::int AS n, count(*) FILTER (WHERE reconciled_at IS NOT NULL)::int AS reconciled
    FROM scout_research_candidates WHERE grant_id=$1 AND tenant_id='10'`, [grant.grant_id])).rows[0];
  const perCycle = cycles.map(row => {
    const result = row.result || {};
    const recorded = key => (Number.isSafeInteger(result[key]) ? result[key] : null);
    return { cycleNumber: row.cycle_number, state: row.state, acceptedCandidates: recorded('acceptedCandidates'),
      retainedCandidates: recorded('retainedCandidates'), evaluatedCandidates: recorded('evaluatedCandidates') };
  });
  const out = { retained: { total: retainedTotal.n, reconciled: retainedTotal.reconciled, listed: retainedRows.length,
    rows: retainedRows.map(row => ({ cycleNumber: row.cycle_number, company: row.company, domain: row.domain,
      vertical: row.vertical, location: row.location, reconciled: row.reconciled })) },
  perCycle, ledger: null };
  const present = (await db.query(`SELECT to_regclass('public.scout_candidate_work') IS NOT NULL
    AND to_regclass('public.scout_candidate_memberships') IS NOT NULL AS present`)).rows[0]?.present;
  if (!present) return out;
  const scope = ['10', grant.search_fingerprint];
  const groups = (await db.query(`SELECT state,reason,count(*)::int AS count FROM scout_candidate_work
    WHERE tenant_id=$1 AND search_fingerprint=$2 GROUP BY state,reason ORDER BY state,reason`, scope)).rows;
  const byState = { pending: 0, deferred: 0, blocked: 0, handled: 0 }; const byReason = {};
  for (const row of groups) {
    byState[row.state] = (byState[row.state] || 0) + row.count;
    const key = row.reason || 'unspecified';
    byReason[key] = (byReason[key] || 0) + row.count;
  }
  const total = groups.reduce((sum, row) => sum + row.count, 0);
  const acceptedTickets = groups.filter(row => row.state === 'handled' && row.reason === 'queued').reduce((s, r) => s + r.count, 0);
  const awaiting = (await db.query(`SELECT w.state,w.reason,m.raw_candidate FROM scout_candidate_work w
    JOIN LATERAL (SELECT raw_candidate FROM scout_candidate_memberships m WHERE m.tenant_id=w.tenant_id
      AND m.search_fingerprint=w.search_fingerprint AND m.plan_version=w.plan_version AND m.ticket_version=w.ticket_version
      AND m.candidate_key=w.candidate_key ORDER BY m.created_at,m.query_key,m.ordinal LIMIT 1) m ON true
    WHERE w.tenant_id=$1 AND w.search_fingerprint=$2 AND w.reason=ANY($3::text[])
    ORDER BY w.created_at,w.candidate_key LIMIT $4`, [...scope, AWAITING_EVIDENCE, CANDIDATE_LIST_LIMIT])).rows;
  const awaitingTotal = AWAITING_EVIDENCE.reduce((sum, reason) => sum + (byReason[reason] || 0), 0);
  out.ledger = { scope: 'search_fingerprint', fingerprint: grant.search_fingerprint, total, byState, byReason,
    acceptedTickets, acceptedNotRetained: Math.max(0, acceptedTickets - retainedTotal.n),
    awaitingBusinessEvidence: { total: awaitingTotal, listed: awaiting.length, rows: awaiting.map(row => awaitingRow(row)) } };
  return out;
}

function awaitingRow({ state, reason, raw_candidate: stored }) {
  const raw = stored && typeof stored === 'object' ? stored : {};
  const textField = value => (typeof value === 'string' && value.trim() ? value.trim().slice(0, 300) : null);
  const types = Array.isArray(raw.placeTypes) && raw.placeTypes.length ? raw.placeTypes : Array.isArray(raw.types) ? raw.types : [];
  return { state, reason, name: textField(raw.companyName || raw.name || raw.company), location: textField(raw.location || raw.address),
    website: textField(raw.website || raw.url || raw.domain), discoveryQuery: textField(raw.discoveryQuery),
    placeTypes: types.filter(type => typeof type === 'string').slice(0, 12) };
}

// The scheduler claims one cycle. Consumption commits before any provider
// work and is never refunded; a crashed cycle is abandoned, not replayed.
async function claimResearchCycle(pool, { program, source, cycleId, env = process.env, leaseMs = CYCLE_LEASE_MS } = {}) {
  if (!text(cycleId, 200)) fail('research_only_cycle_id_required');
  // A shorter lease only fences a cycle sooner; it can never extend one.
  const lease = Number.isSafeInteger(leaseMs) && leaseMs > 0 ? Math.min(leaseMs, CYCLE_LEASE_MS) : CYCLE_LEASE_MS;
  return transaction(pool, async db => {
    await recoveryLock(db);
    await assertSchema(db);
    const grant = (await db.query("SELECT * FROM scout_research_only_grants WHERE tenant_id='10' AND revoked_at IS NULL FOR UPDATE")).rows[0];
    if (!grant) return null;
    const manifest = storedManifest(grant);
    assertEnvironment(manifest, env);
    if (program?.id !== manifest.programId || String(program?.tenant_id) !== '10' || source?.id !== manifest.missionId)
      fail('research_only_program_changed');
    await assertOperatorAccount(db, manifest.operator);
    const bound = await liveBindings(db, manifest);
    const now = await dbNow(db);
    assertWindow(manifest, now);
    await db.query(`UPDATE scout_research_only_cycles SET state='abandoned',finished_at=clock_timestamp(),
      result='{"reason":"lease_expired_not_refunded"}'::jsonb WHERE grant_id=$1 AND state='claimed' AND lease_expires_at<=clock_timestamp()`,
    [grant.grant_id]);
    const cycles = (await db.query('SELECT cycle_number,state FROM scout_research_only_cycles WHERE grant_id=$1', [grant.grant_id])).rows;
    if ((await db.query('SELECT 1 FROM scout_research_only_cycles WHERE cycle_id=$1', [cycleId])).rows.length)
      fail('research_only_cycle_already_claimed');
    if (cycles.some(row => row.state === 'claimed')) fail('research_only_cycle_in_progress');
    if (cycles.length >= grant.max_cycles) fail('research_only_cycles_exhausted');
    const releasedCycles = await releasedAllowance(db, grant.grant_id);
    if (cycles.length >= releasedCycles) fail('research_only_awaiting_cycle_release');
    // Do not consume a cycle that today's ceiling cannot fund a single search for.
    const spent = Number((await db.query(`SELECT COALESCE(sum(reserved_usd_micros),0)::bigint AS micros FROM scout_research_google_spend
      WHERE tenant_id='10' AND usage_day=(clock_timestamp() AT TIME ZONE 'UTC')::date`)).rows[0].micros);
    if (spent + Math.ceil(PLACES_UPPER_BOUND.text_search * 1e6) > Math.min(grant.daily_google_usd_micros, MAX_DAILY_USD_MICROS))
      fail('research_only_daily_budget_exhausted');
    const token = randomUUID(), cycleNumber = cycles.length + 1;
    await db.query(`INSERT INTO scout_research_only_cycles(grant_id,cycle_number,tenant_id,cycle_id,execution_token,lease_expires_at)
      VALUES($1,$2,'10',$3,$4,clock_timestamp()+$5::interval)`, [grant.grant_id, cycleNumber, cycleId, token, `${lease} milliseconds`]);
    const missions = require('./scoutResearchMission');
    return Object.freeze({ grantId: grant.grant_id, cycleNumber, cycleId, token, manifest, maxCycles: grant.max_cycles, releasedCycles,
      researchMissionId: bound.research.id, researchSource: bound.researchSource,
      researchProgram: missions.researchProgram(bound.program, bound.research),
      dailyCapMicros: Math.min(grant.daily_google_usd_micros, MAX_DAILY_USD_MICROS),
      leaseExpiresAt: +now + lease, expiresAt: Date.parse(manifest.expiresAt) });
  });
}
async function finishResearchCycle(pool, claim, { completed, result }) {
  const finished = await pool.query(`UPDATE scout_research_only_cycles SET state=$4,finished_at=clock_timestamp(),result=$5::jsonb
    WHERE grant_id=$1 AND cycle_number=$2 AND execution_token=$3 AND state='claimed' RETURNING cycle_number`,
  [claim.grantId, claim.cycleNumber, claim.token, completed ? 'completed' : 'stopped', JSON.stringify(result || {})]);
  if (finished.rows.length !== 1) fail('research_only_cycle_finish_unconfirmed');
}

function runInResearchScope(claim, fn, pool) {
  if (!pool?.connect) fail('research_only_scope_required');
  const current = { active: true, tenantId: '10', claim, pool };
  return cycleScope.run(current, async () => { try { return await fn(); } finally { current.active = false; } });
}
function liveScope() {
  const current = cycleScope.getStore();
  return current?.active && Date.now() < current.claim.expiresAt && Date.now() < current.claim.leaseExpiresAt ? current : null;
}
function assertResearchOnlyScope(tenantId) {
  if (String(tenantId) !== '10' || !liveScope()) fail('research_only_scope_required');
  return true;
}
// Consulted by the Tenant 10 research-provider pause. The generic research
// entry is allowed for the claimed cycle; Google Places only inside one
// metered attempt that already holds a durable Tenant 10 debit. Every other
// provider, and every direct (unmetered) Places transport, stays paused.
function allowsResearchOnlyProvider(provider, tenant) {
  const current = liveScope();
  if (!current || String(tenant) !== '10') return false;
  if (provider === 'research') return true;
  const attempt = attemptScope.getStore();
  return provider === 'google_places' && Boolean(attempt?.active && attempt.token === current.claim.token);
}

async function reserveGoogle(claim, { endpoint, tenantId }, pool) {
  if (tenantId != null && String(tenantId) !== '10') fail(SPEND_CODE, 'tenant_mismatch');
  const price = PLACES_UPPER_BOUND[endpoint];
  if (!Number.isFinite(price) || price <= 0) fail(SPEND_CODE, 'cost_unknown');
  const micros = Math.ceil(price * 1e6);
  const id = randomUUID();
  try {
    await transaction(pool, async db => {
      await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [SPEND_LOCK_KEY]);
      await recoveryLock(db);
      const grant = (await db.query("SELECT grant_id,revoked_at,expires_at,not_before,daily_google_usd_micros FROM scout_research_only_grants WHERE grant_id=$1 AND tenant_id='10' FOR SHARE",
        [claim.grantId])).rows[0];
      const cycle = (await db.query(`SELECT state,execution_token,lease_expires_at>clock_timestamp() AS leased FROM scout_research_only_cycles
        WHERE grant_id=$1 AND cycle_number=$2 FOR SHARE`, [claim.grantId, claim.cycleNumber])).rows[0];
      const now = await dbNow(db);
      if (!grant || grant.revoked_at) fail(SPEND_CODE, 'grant_revoked');
      if (+now < +new Date(grant.not_before) || +now >= +new Date(grant.expires_at)) fail(SPEND_CODE, 'grant_expired');
      if (cycle?.state !== 'claimed' || cycle.execution_token !== claim.token || cycle.leased !== true) fail(SPEND_CODE, 'cycle_fenced');
      assertEnvironment(claim.manifest);
      const cap = Math.min(grant.daily_google_usd_micros, MAX_DAILY_USD_MICROS);
      // The ceiling counts every reserved Tenant 10 Google attempt today, from
      // this ledger and from any other metered Tenant 10 Places event, at the
      // reserved upper bound. Nothing is refunded, so retries cannot exceed it.
      const day = (await db.query(`SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date::text AS day,
        COALESCE((SELECT sum(reserved_usd_micros) FROM scout_research_google_spend WHERE tenant_id='10'
          AND usage_day=(clock_timestamp() AT TIME ZONE 'UTC')::date),0)::bigint AS ledger,
        (SELECT COALESCE(sum(ceil(COALESCE(estimated_usd,reserved_usd)*1000000)),0)::bigint FROM api_usage_events e
          WHERE e.provider='google_places' AND e.tenant_id=10
            AND e.created_at >= date_trunc('day', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
            AND NOT EXISTS (SELECT 1 FROM scout_research_google_spend s WHERE s.api_usage_id=e.id)) AS other,
        (SELECT count(*) FROM api_usage_events e WHERE e.provider='google_places' AND e.tenant_id=10
            AND e.created_at >= date_trunc('day', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
            AND COALESCE(e.estimated_usd,e.reserved_usd) IS NULL)::int AS unknown`)).rows[0];
      if (day.unknown > 0) fail(SPEND_CODE, 'cost_unknown');
      if (Number(day.ledger) + Number(day.other) + micros > cap) fail(SPEND_CODE, 'daily_budget_exhausted');
      await db.query(`INSERT INTO scout_research_google_spend(id,tenant_id,grant_id,cycle_number,usage_day,operation,reserved_usd_micros)
        VALUES($1,'10',$2,$3,$4::date,$5,$6)`, [id, claim.grantId, claim.cycleNumber, day.day, endpoint, micros]);
    });
  } catch (error) {
    if (error.code === SPEND_CODE) throw error;
    // Unavailable enforcement is a refusal, never a pass-through.
    fail(SPEND_CODE, 'reservation_unavailable');
  }
  const marker = { active: true, token: claim.token, id };
  let settled = false;
  return {
    id, micros,
    run: fn => attemptScope.run(marker, fn),
    async link(apiUsageId) {
      const linked = await pool.query(`UPDATE scout_research_google_spend SET api_usage_id=$2 WHERE id=$1 AND state='reserved'
        AND api_usage_id IS NULL RETURNING id`, [id, apiUsageId]);
      if (linked.rows.length !== 1) { marker.active = false; fail(SPEND_CODE, 'shared_link_unconfirmed'); }
    },
    async settle(status, httpStatus) {
      marker.active = false;
      if (settled) return; settled = true;
      await pool.query(`UPDATE scout_research_google_spend SET state=$2,http_status=$3,finished_at=clock_timestamp()
        WHERE id=$1 AND state='reserved'`, [id, ['ok', 'error'].includes(status) ? status : 'uncertain',
        Number.isInteger(httpStatus) ? httpStatus : null]).catch(() => {});
    },
  };
}
// Null outside a claimed research-only cycle: every other caller keeps its
// existing metering unchanged. Inside one, the debit is mandatory.
function googleMeter() {
  const current = cycleScope.getStore();
  if (!current?.active) return null;
  return { reserve: input => {
    if (!liveScope()) fail(SPEND_CODE, 'cycle_scope_expired');
    return reserveGoogle(current.claim, input, current.pool);
  } };
}

module.exports = {
  MAX_CYCLES, INITIAL_RELEASED_CYCLES, MAX_DAILY_USD_MICROS, PERMISSIONS, SPEND_CODE, RECOVERY_LOCK_KEY,
  schedulerEnabled, validateManifest, assertEnvironment, assertSchema,
  previewResearchOnlyGrant, installResearchOnlyGrant, revokeResearchOnlyGrant, inspectResearchOnlyGrant,
  previewResearchCycleRelease, releaseResearchCycle, validateCycleRelease,
  claimResearchCycle, finishResearchCycle, runInResearchScope, assertResearchOnlyScope,
  allowsResearchOnlyProvider, googleMeter,
};
