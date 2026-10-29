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
  } catch { fail('research_only_schema_unavailable'); }
  const guards = (await db.query(`SELECT count(*)::int AS count FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='O'
    AND tgname = ANY($1::text[])`, [['scout_research_only_grants_no_delete', 'scout_research_only_grants_revoke_only',
    'scout_research_only_cycles_no_delete', 'scout_research_only_cycles_forward_only',
    'scout_research_google_spend_no_delete', 'scout_research_google_spend_settle']])).rows[0];
  const indexes = (await db.query(`SELECT count(*)::int AS count FROM pg_index i WHERE i.indisunique AND i.indisvalid AND i.indisready
    AND i.indexrelid IN (to_regclass('public.scout_research_only_one_open_tenant'), to_regclass('public.scout_research_only_one_live_cycle'))`)).rows[0];
  if (guards?.count !== 6 || indexes?.count !== 2) fail('research_only_schema_guards_missing');
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
    maxCycles: manifest.maxCycles, dailyGoogleUsdMicros: manifest.dailyGoogleUsdMicros,
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
      return { ...summary(manifest), status: 'installed', installed: false, idempotent: true };
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
    return { ...summary(manifest), status: 'installed', installed: true, idempotent: false };
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

async function inspectResearchOnlyGrant(pool, grantId) {
  if (grantId !== undefined && !HEX64.test(grantId || '')) fail('research_only_grant_not_found');
  return transaction(pool, async db => {
    await assertSchema(db);
    const grant = (await db.query(`SELECT * FROM scout_research_only_grants WHERE tenant_id='10'
      AND ($1::text IS NULL AND revoked_at IS NULL OR grant_id=$1) ORDER BY installed_at DESC LIMIT 1`, [grantId ?? null])).rows[0];
    const now = await dbNow(db);
    if (!grant) return { status: 'not_installed', observedAt: now.toISOString() };
    const manifest = storedManifest(grant);
    const cycles = (await db.query(`SELECT cycle_number,cycle_id,state,claimed_at,finished_at,result FROM scout_research_only_cycles
      WHERE grant_id=$1 ORDER BY cycle_number`, [grant.grant_id])).rows;
    const spend = (await db.query(`SELECT usage_day::text AS day,state,operation,count(*)::int AS attempts,sum(reserved_usd_micros)::int AS micros
      FROM scout_research_google_spend WHERE grant_id=$1 GROUP BY usage_day,state,operation ORDER BY usage_day,operation,state`, [grant.grant_id])).rows;
    return { ...summary(manifest), observedAt: now.toISOString(),
      status: grant.revoked_at ? 'revoked' : +now >= +new Date(grant.expires_at) ? 'expired'
        : cycles.length >= grant.max_cycles ? 'exhausted' : 'installed',
      installedAt: grant.installed_at, revokedAt: grant.revoked_at, revokedBy: grant.revoked_by,
      cyclesConsumed: cycles.length, cyclesRemaining: Math.max(0, grant.max_cycles - cycles.length), cycles,
      spend, reservedUsdMicros: spend.reduce((sum, row) => sum + row.micros, 0) };
  }, { readOnly: true });
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
    if (cycles.some(row => row.state === 'claimed')) fail('research_only_cycle_in_progress');
    if (cycles.length >= grant.max_cycles) fail('research_only_cycles_exhausted');
    // Do not consume a cycle that today's ceiling cannot fund a single search for.
    const spent = Number((await db.query(`SELECT COALESCE(sum(reserved_usd_micros),0)::bigint AS micros FROM scout_research_google_spend
      WHERE tenant_id='10' AND usage_day=(clock_timestamp() AT TIME ZONE 'UTC')::date`)).rows[0].micros);
    if (spent + Math.ceil(PLACES_UPPER_BOUND.text_search * 1e6) > Math.min(grant.daily_google_usd_micros, MAX_DAILY_USD_MICROS))
      fail('research_only_daily_budget_exhausted');
    const token = randomUUID(), cycleNumber = cycles.length + 1;
    await db.query(`INSERT INTO scout_research_only_cycles(grant_id,cycle_number,tenant_id,cycle_id,execution_token,lease_expires_at)
      VALUES($1,$2,'10',$3,$4,clock_timestamp()+$5::interval)`, [grant.grant_id, cycleNumber, cycleId, token, `${lease} milliseconds`]);
    const missions = require('./scoutResearchMission');
    return Object.freeze({ grantId: grant.grant_id, cycleNumber, cycleId, token, manifest, maxCycles: grant.max_cycles,
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
  MAX_CYCLES, MAX_DAILY_USD_MICROS, PERMISSIONS, SPEND_CODE, RECOVERY_LOCK_KEY,
  schedulerEnabled, validateManifest, assertEnvironment, assertSchema,
  previewResearchOnlyGrant, installResearchOnlyGrant, revokeResearchOnlyGrant, inspectResearchOnlyGrant,
  claimResearchCycle, finishResearchCycle, runInResearchScope, assertResearchOnlyScope,
  allowsResearchOnlyProvider, googleMeter,
};
