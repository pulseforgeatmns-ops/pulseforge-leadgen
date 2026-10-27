'use strict';
// Authorized Scout backlog recovery. One operator-installed, one-shot pass over
// already persisted Tenant 10 candidate-work tickets, through the existing ticket
// claim/renew/finish, ownership and queue-admission path. No discovery, query
// transfer, Places/provider call, enrichment, verification, preparation or email.
// It requires the recovery kill switch to stay armed; it never lowers it.
const { randomUUID } = require('node:crypto');
const { isProxy } = require('node:util').types;
const { createCandidateTicketStore, TICKET_VERSION } = require('./scoutCandidateTickets');
const { PLAN_VERSION } = require('./scoutQueryCheckpoints');
const { claimedCompanies, candidateTicketDependencyKey } = require('./scoutCandidateTicketDispatch');
const { hash } = require('./scoutBoundedAcquisitionBudget');
const { validateJsonDocument } = require('./scoutBoundedResearchAuthority');
const { loadRecoveryState, RECOVERY_LOCK_KEY } = require('./anchorEmailRecovery');

const TENANT = '10';
// The only fingerprint verified for this recovery (Anchor Cleaning Scout mission).
const APPROVED_FINGERPRINT = '22d50dc07f997a3a811368a7e8b1de5a3211df5bd555260c3d42c4e780a960b7';
const MAX_CANDIDATES = 20;
const MAX_WINDOW_MS = 60 * 60 * 1000;
const RUN_LOCK_KEY = 'scout-backlog-recovery:10';
const PROGRAM_AUTHORITY_COLUMNS = 'id,tenant_id,source_mission_id,policy,policy_hash,scope_hash,authorized_by,authorized_at,mode';
const MANIFEST_KEYS = ['version','kind','approvalRef','tenantId','programId','missionId','programHash','sourceHash',
  'fingerprint','planVersion','ticketVersion','maxCandidates','candidates','authority','window'];
const AUTHORITY_KEYS = ['expectedRevision','quarantineDecisionHash','recoveryControlHash',
  'discovery','places','enrichment','verification','preparation','email'];
const DENIED_RIGHTS = ['discovery','places','enrichment','verification','preparation','email'];
const STALE_CODES = new Set(['claim_stale','lease_expired','membership_changed','stale_claim']);
const HEX64 = /^[a-f0-9]{64}(?![\s\S])/;
const SCOPE_SQL = 'tenant_id=$1 AND search_fingerprint=$2 AND plan_version=$3 AND ticket_version=$4';

const fail = code => { throw Object.assign(Error(code), { code, retryAllowed:false }); };
const exactKeys = (value, keys) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const iso = value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z(?![\s\S])/.test(value)
  && Number.isFinite(Date.parse(value)) && new Date(value).toISOString().slice(0,19) === value.slice(0,19);
function stopCode(error) {
  const code = error && Object.getOwnPropertyDescriptor(error,'code')?.value;
  return typeof code === 'string' && /^[a-z][a-z0-9_]{0,119}(?![\s\S])/.test(code) ? code : 'backlog_recovery_failed';
}

function validateManifest(input, approvedFingerprint = APPROVED_FINGERPRINT) {
  if (isProxy(input)) fail('backlog_recovery_manifest_invalid');
  try { validateJsonDocument(input); } catch { fail('backlog_recovery_manifest_invalid'); }
  const m = input;
  if (!exactKeys(m, MANIFEST_KEYS) || m.version !== 1 || m.kind !== 'scout_backlog_recovery'
    || typeof m.approvalRef !== 'string' || !/^[a-zA-Z0-9_.:-]{1,120}(?![\s\S])/.test(m.approvalRef)
    || !['programId','missionId'].every(k => typeof m[k] === 'string' && m[k].length > 0 && m[k].length < 512)
    || !['programHash','sourceHash','fingerprint'].every(k => typeof m[k] === 'string' && HEX64.test(m[k]))
    || m.planVersion !== PLAN_VERSION || m.ticketVersion !== TICKET_VERSION) fail('backlog_recovery_manifest_invalid');
  if (m.tenantId !== TENANT) fail('backlog_recovery_tenant_not_authorized');
  if (m.fingerprint !== approvedFingerprint) fail('backlog_recovery_fingerprint_not_authorized');
  if (!Number.isSafeInteger(m.maxCandidates) || m.maxCandidates < 1 || m.maxCandidates > MAX_CANDIDATES
    || !Array.isArray(m.candidates) || m.candidates.length !== m.maxCandidates) fail('backlog_recovery_candidate_limit');
  let previous = '';
  for (const c of m.candidates) {
    if (!exactKeys(c, ['candidateKey','membershipRevision','attempts']) || typeof c.candidateKey !== 'string'
      || !HEX64.test(c.candidateKey) || c.candidateKey <= previous
      || typeof c.membershipRevision !== 'string' || !/^[1-9][0-9]{0,17}(?![\s\S])/.test(c.membershipRevision)
      || !Number.isSafeInteger(c.attempts) || c.attempts < 0) fail('backlog_recovery_candidates_invalid');
    previous = c.candidateKey;
  }
  const a = m.authority;
  if (!exactKeys(a, AUTHORITY_KEYS) || typeof a.expectedRevision !== 'string' || !/^[a-f0-9]{40}(?![\s\S])/.test(a.expectedRevision)
    || !['quarantineDecisionHash','recoveryControlHash'].every(k => typeof a[k] === 'string' && HEX64.test(a[k]))
    || !DENIED_RIGHTS.every(k => a[k] === false)) fail('backlog_recovery_authority_invalid');
  const w = m.window;
  if (!exactKeys(w, ['notBefore','expiresAt']) || !iso(w.notBefore) || !iso(w.expiresAt)
    || Date.parse(w.expiresAt) <= Date.parse(w.notBefore)
    || Date.parse(w.expiresAt) - Date.parse(w.notBefore) > MAX_WINDOW_MS) fail('backlog_recovery_window_invalid');
  const copy = JSON.parse(JSON.stringify(m));
  return Object.freeze({ ...copy, manifestHash:hash(copy), approvalDocument:copy });
}

function environment(manifest) {
  const env = process.env;
  // The verified state is maintenance/recovery_kill_switch. This path only runs
  // with every closed control still armed and never changes any of them.
  if (env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE !== 'true'
    || env.ANCHOR_RESEARCH_PROVIDERS_PAUSED !== 'true'
    || env.ANCHOR_GOVERNED_OUTBOUND_ENABLED !== 'false'
    || ![undefined,''].includes(env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID)
    || ![undefined,''].includes(env.ANCHOR_EMAIL_SCOPED_RESUME_ID)
    || env.RAILWAY_GIT_COMMIT_SHA !== manifest.authority.expectedRevision) fail('backlog_recovery_closed_environment_required');
}
async function assertSchema(db) {
  await db.query(`SELECT manifest_hash,approval_ref,tenant_id,search_fingerprint,max_candidates,manifest,created_at,installed_at,
    revoked_at,revoke_approval_ref FROM scout_backlog_recovery_grants LIMIT 0`);
  await db.query(`SELECT approval_ref,manifest_hash,tenant_id,search_fingerprint,dependency_key,execution_token,state,stop_reason,
    result,created_at,finished_at FROM scout_backlog_recovery_runs LIMIT 0`);
  await db.query(`SELECT approval_ref,tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key,disposition,skip_reason,
    claim_token,membership_revision,created_at FROM scout_backlog_recovery_items LIMIT 0`);
  const guards = (await db.query(`SELECT count(*)::int AS count FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='O'
    AND tgname = ANY(ARRAY['scout_backlog_recovery_grants_no_delete','scout_backlog_recovery_grants_revoke_only',
      'scout_backlog_recovery_runs_no_delete','scout_backlog_recovery_runs_finish_only','scout_backlog_recovery_items_immutable'])
    AND tgrelid = ANY(ARRAY['scout_backlog_recovery_grants'::regclass,'scout_backlog_recovery_runs'::regclass,
      'scout_backlog_recovery_items'::regclass])`)).rows[0];
  if (guards?.count !== 5) fail('backlog_recovery_schema_guards_missing');
  const indexes = (await db.query(`SELECT count(*)::int AS count FROM pg_index i
    WHERE i.indisunique AND i.indisvalid AND i.indexrelid = ANY(ARRAY[
      to_regclass('public.scout_backlog_recovery_one_open_tenant'),to_regclass('public.scout_backlog_recovery_one_inflight_tenant')])`)).rows[0];
  if (indexes?.count !== 2) fail('backlog_recovery_schema_identity_missing');
}
async function loadSource(db, manifest, lock = false) {
  const { scoutSearchFingerprint, sourceScope } = require('./maxOutboundControlLoop');
  const program = (await db.query(`SELECT ${PROGRAM_AUTHORITY_COLUMNS} FROM acquisition_outbound_programs WHERE tenant_id=$1 AND id=$2${lock ? ' FOR SHARE' : ''}`,
    [TENANT,manifest.programId])).rows[0];
  const source = (await db.query(`SELECT id,objective,target_segment,payload FROM acquisition_missions WHERE tenant_id=$1 AND id=$2${lock ? ' FOR SHARE' : ''}`,
    [TENANT,manifest.missionId])).rows[0];
  if (!program || !source || program.source_mission_id !== manifest.missionId || program.mode === 'revoked'
    || hash(program) !== manifest.programHash || hash(source) !== manifest.sourceHash) fail('backlog_recovery_source_changed');
  if (scoutSearchFingerprint(program,source) !== manifest.fingerprint) fail('backlog_recovery_fingerprint_mismatch');
  return { program, source, scope:sourceScope(source) };
}
async function readGrant(db, manifest, lock = false) {
  const rows = (await db.query(`SELECT manifest_hash,approval_ref,tenant_id,search_fingerprint,max_candidates,manifest,installed_at,revoked_at
    FROM scout_backlog_recovery_grants WHERE manifest_hash=$1 OR approval_ref=$2${lock ? ' FOR SHARE' : ''}`,
  [manifest.manifestHash,manifest.approvalRef])).rows;
  if (rows.length > 1 || rows.length === 1 && (rows[0].manifest_hash !== manifest.manifestHash
    || rows[0].approval_ref !== manifest.approvalRef || rows[0].tenant_id !== TENANT
    || rows[0].search_fingerprint !== manifest.fingerprint || hash(rows[0].manifest) !== manifest.manifestHash)) fail('backlog_recovery_grant_conflict');
  return rows[0] || null;
}
async function assertAuthority(db, manifest, { installed = true, lock = true } = {}) {
  environment(manifest);
  await assertSchema(db);
  const state = await loadRecoveryState(db, { lockControl:lock });
  if (state.mode !== 'maintenance' || state.reason !== 'recovery_kill_switch') fail('backlog_recovery_kill_switch_state_required');
  const control = (await db.query(`SELECT singleton,maintenance_armed,manifest FROM anchor_email_recovery_control${lock ? ' FOR SHARE' : ''}`)).rows;
  if (control.length !== 1 || control[0].singleton !== true || control[0].maintenance_armed !== true || control[0].manifest !== null
    || hash(control[0]) !== manifest.authority.recoveryControlHash) fail('backlog_recovery_closed_control_required');
  const open = (await db.query(`SELECT
    EXISTS(SELECT 1 FROM anchor_scoped_resume_grants WHERE tenant_id='10' AND revoked_at IS NULL) AS scoped_resume,
    EXISTS(SELECT 1 FROM scout_bounded_research_grants WHERE tenant_id='10' AND revoked_at IS NULL) AS research,
    EXISTS(SELECT 1 FROM scout_bounded_acquisition_runs WHERE tenant_id='10' AND state='claimed') AS research_run`)).rows[0];
  if (!open || open.scoped_resume !== false || open.research !== false || open.research_run !== false) fail('backlog_recovery_other_authority_open');
  if (lock) await db.query("SELECT decision_hash FROM anchor_email_quarantines WHERE tenant_id='10' AND decision_hash=$1 FOR SHARE",
    [manifest.authority.quarantineDecisionHash]);
  await require('./anchorEmailQuarantine').requireQuarantine(db, manifest.authority.quarantineDecisionHash);
  const bound = await loadSource(db, manifest, lock);
  const grant = await readGrant(db, manifest, lock);
  const other = (await db.query(`SELECT manifest_hash FROM scout_backlog_recovery_grants
    WHERE tenant_id='10' AND revoked_at IS NULL AND manifest_hash<>$1 LIMIT 1`, [manifest.manifestHash])).rows;
  if (other.length) fail('backlog_recovery_other_authority_open');
  if (grant?.revoked_at) fail('backlog_recovery_grant_revoked');
  if (installed && !grant?.installed_at) fail('backlog_recovery_grant_required');
  // Time is sampled only after every potentially waiting row lock.
  const now = (await db.query('SELECT clock_timestamp() AS now')).rows[0]?.now;
  if (!now || +new Date(now) < Date.parse(manifest.window.notBefore)
    || +new Date(now) >= Date.parse(manifest.window.expiresAt)) fail('backlog_recovery_window_inactive');
  return { ...bound, grant, now };
}

async function transaction(pool, work) {
  const db = await pool.connect(); let committing = false, discard = false;
  try {
    await db.query('BEGIN');
    await db.query("SET LOCAL lock_timeout='2s'");
    await db.query("SET LOCAL statement_timeout='30s'");
    const result = await work(db);
    committing = true; await db.query('COMMIT'); committing = false;
    return result;
  } catch (failure) {
    try { await db.query('ROLLBACK'); } catch { discard = true; }
    if (committing) fail('backlog_recovery_commit_uncertain');
    throw failure;
  } finally { db.release(discard || committing); }
}
async function readOnly(pool, work) {
  const db = await pool.connect(); let discard = false;
  try {
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    await db.query("SET LOCAL statement_timeout='5s'");
    await db.query("SET LOCAL lock_timeout='2s'");
    const result = await work(db);
    await db.query('COMMIT');
    return result;
  } catch (failure) {
    try { await db.query('ROLLBACK'); } catch { discard = true; }
    throw failure;
  } finally { db.release(discard); }
}
// Serializes recovery operations per tenant and keeps the shared recovery
// interlock held, so maintenance work cannot interleave with an invocation.
async function exclusive(pool, work) {
  return transaction(pool, async db => {
    const { rows } = await db.query('SELECT pg_try_advisory_xact_lock(hashtextextended($1,0)) AS ok', [RUN_LOCK_KEY]);
    if (rows[0]?.ok !== true) fail('backlog_recovery_in_progress');
    await db.query('SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))', [RECOVERY_LOCK_KEY]);
    return work(db);
  });
}
const sharedRecoveryLock = db => db.query('SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))', [RECOVERY_LOCK_KEY]);

function scopeValues(manifest) { return [TENANT, manifest.fingerprint, manifest.planVersion, manifest.ticketVersion]; }
// Mirrors claimDue's due predicate, narrowed to pending/deferred work that is
// exactly as approved: same membership revision (no new discovery evidence) and
// the same attempt count (nothing else has processed it since approval).
async function eligibility(db, manifest, lock) {
  const rows = (await db.query(`SELECT candidate_key,state,membership_revision::text AS membership_revision,attempts::int AS attempts,
    claim_token IS NOT NULL AND lease_until > clock_timestamp() AS leased,
    (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()) AS due,
    EXISTS(SELECT 1 FROM scout_candidate_memberships m WHERE m.tenant_id=w.tenant_id AND m.search_fingerprint=w.search_fingerprint
      AND m.plan_version=w.plan_version AND m.ticket_version=w.ticket_version AND m.candidate_key=w.candidate_key) AS has_members
    FROM scout_candidate_work w WHERE ${SCOPE_SQL} AND candidate_key=ANY($5::text[])
    ORDER BY candidate_key${lock ? ' FOR UPDATE' : ''}`, [...scopeValues(manifest), manifest.candidates.map(c => c.candidateKey)])).rows;
  const byKey = new Map(rows.map(row => [row.candidate_key, row]));
  return manifest.candidates.map(c => {
    const row = byKey.get(c.candidateKey);
    const reason = !row || !row.has_members ? 'candidate_not_persisted'
      : !['pending','deferred'].includes(row.state) ? 'state_not_claimable'
        : row.leased ? 'claimed_elsewhere'
          : row.membership_revision !== c.membershipRevision ? 'membership_revision_changed'
            : row.attempts !== c.attempts ? 'attempted_since_approval'
              : !row.due ? 'not_due' : null;
    return { candidateKey:c.candidateKey, eligible:reason === null, reason,
      observed:row ? { state:row.state, membershipRevision:row.membership_revision, attempts:row.attempts } : null };
  });
}

function rawIdentity(raw) {
  const { normalizeDomain } = require('../utils/canonicalEmailEligibility');
  return { domain:normalizeDomain(raw.domain || raw.website || raw.url) || null,
    name:String(raw.companyName || raw.name || raw.company || '').trim().toLowerCase() || null };
}
// Ticket truth comes from scout_candidate_work.last_completion_token; queue truth
// from scout_unenriched, matched with the same domain/name predicate admission uses.
async function reconcile(db, manifest, items) {
  const claimed = items.filter(item => item.disposition === 'claimed');
  if (!claimed.length) return [];
  const keys = claimed.map(item => item.candidate_key);
  const work = new Map((await db.query(`SELECT candidate_key,state,reason,last_completion_token::text AS last_completion_token,
    claim_token::text AS claim_token,lease_until > clock_timestamp() AS lease_active
    FROM scout_candidate_work WHERE ${SCOPE_SQL} AND candidate_key=ANY($5::text[])`, [...scopeValues(manifest), keys])).rows
    .map(row => [row.candidate_key, row]));
  const members = (await db.query(`SELECT candidate_key,raw_candidate FROM scout_candidate_memberships
    WHERE ${SCOPE_SQL} AND candidate_key=ANY($5::text[]) ORDER BY candidate_key,member_key`, [...scopeValues(manifest), keys])).rows;
  const result = [];
  for (const item of claimed) {
    const row = work.get(item.candidate_key), token = String(item.claim_token);
    const ticket = !row ? 'missing' : row.last_completion_token === token ? 'completed'
      : row.claim_token === token ? 'claim_unfinished' : 'claim_superseded';
    const entry = { candidateKey:item.candidate_key, ticket,
      state:ticket === 'completed' ? row.state : null, reason:ticket === 'completed' ? row.reason : null };
    if (entry.reason === 'queued') {
      const ids = members.filter(m => m.candidate_key === item.candidate_key).map(m => rawIdentity(m.raw_candidate));
      const domains = [...new Set(ids.map(id => id.domain).filter(Boolean))], names = [...new Set(ids.map(id => id.name).filter(Boolean))];
      const queue = (await db.query(`SELECT id,source,lower(domain)=ANY($1::text[]) AS domain_match FROM scout_unenriched WHERE client_id=10
        AND (lower(domain)=ANY($1::text[]) OR lower(trim(company))=ANY($2::text[])) ORDER BY id`, [domains, names])).rows;
      // A name-only match can be a pre-existing row the admission dedup kept;
      // report how the match was made rather than treating both alike.
      entry.downstream = { verified:queue.some(q => q.source === 'max_buffer_replenishment'),
        matchedBy:!queue.length ? null : queue.some(q => q.domain_match) ? 'domain' : 'name', queueRowIds:queue.map(q => String(q.id)) };
    }
    result.push(entry);
  }
  return result;
}
function summarize(reconciled) {
  const count = fn => reconciled.filter(fn).length;
  const byReason = {};
  for (const row of reconciled) if (row.reason) byReason[row.reason] = (byReason[row.reason] || 0) + 1;
  return { claimed:reconciled.length, completed:count(r => r.ticket === 'completed'),
    unfinished:count(r => r.ticket === 'claim_unfinished'), superseded:count(r => r.ticket === 'claim_superseded' || r.ticket === 'missing'),
    queued:count(r => r.reason === 'queued'), queuedVerifiedDownstream:count(r => r.downstream?.verified === true),
    queuedNotVerifiedDownstream:count(r => r.reason === 'queued' && r.downstream?.verified !== true), byReason };
}
function summary(manifest) {
  return { version:1, kind:'scout_backlog_recovery', manifestHash:manifest.manifestHash, approvalRef:manifest.approvalRef,
    tenantId:TENANT, fingerprint:manifest.fingerprint, programId:manifest.programId, missionId:manifest.missionId,
    expectedRevision:manifest.authority.expectedRevision, maxCandidates:manifest.maxCandidates,
    notBefore:manifest.window.notBefore, expiresAt:manifest.window.expiresAt,
    output:'existing_candidate_tickets_to_raw_business_queue_only', discovery:false, places:false, enrichment:false,
    verification:false, preparation:false, email:false, retryAllowed:false };
}

function createBacklogRecovery({ approvedFingerprint = APPROVED_FINGERPRINT } = {}) {
  const normalize = raw => {
    if (isProxy(raw)) fail('backlog_recovery_manifest_invalid');
    const document = raw && typeof raw === 'object' && Object.getOwnPropertyDescriptor(raw,'approvalDocument');
    if (document && !Object.hasOwn(document,'value')) fail('backlog_recovery_manifest_invalid');
    return validateManifest(document ? document.value : raw, approvedFingerprint);
  };

  async function preview(pool, raw) {
    const manifest = normalize(raw);
    return readOnly(pool, async db => {
      const { grant, now } = await assertAuthority(db, manifest, { installed:false, lock:false });
      const candidates = await eligibility(db, manifest, false);
      return { ...summary(manifest), status:grant ? 'installed' : 'not_installed', observedAt:new Date(now).toISOString(),
        eligibleCandidates:candidates.filter(c => c.eligible).length, candidates, previewOnly:true };
    });
  }
  async function install(pool, raw, confirmationHash) {
    const manifest = normalize(raw);
    if (confirmationHash !== manifest.manifestHash) fail('backlog_recovery_exact_confirmation_required');
    return exclusive(pool, async db => {
      const { grant } = await assertAuthority(db, manifest, { installed:false });
      if (grant) return { ...summary(manifest), status:'installed', installed:false, idempotent:true };
      const consumed = (await db.query('SELECT 1 FROM scout_backlog_recovery_runs WHERE approval_ref=$1 OR manifest_hash=$2',
        [manifest.approvalRef, manifest.manifestHash])).rows;
      if (consumed.length) fail('backlog_recovery_already_consumed');
      await db.query(`INSERT INTO scout_backlog_recovery_grants(manifest_hash,approval_ref,tenant_id,search_fingerprint,max_candidates,manifest,installed_at)
        VALUES($1,$2,'10',$3,$4,$5,clock_timestamp())`,
      [manifest.manifestHash, manifest.approvalRef, manifest.fingerprint, manifest.maxCandidates, JSON.stringify(manifest.approvalDocument)]);
      return { ...summary(manifest), status:'installed', installed:true, idempotent:false };
    });
  }
  async function revoke(pool, raw, confirmationHash) {
    const manifest = normalize(raw);
    if (confirmationHash !== manifest.manifestHash) fail('backlog_recovery_exact_confirmation_required');
    // Revocation does not depend on environment, source or expiry. A tombstone
    // closes an approval that was never installed so it can never be installed.
    return transaction(pool, async db => {
      await sharedRecoveryLock(db);
      await assertSchema(db);
      const grant = await readGrant(db, manifest, true);
      if (grant?.revoked_at) return { ...summary(manifest), status:'revoked', revoked:false, idempotent:true, tombstone:!grant.installed_at };
      if (!grant) await db.query(`INSERT INTO scout_backlog_recovery_grants
        (manifest_hash,approval_ref,tenant_id,search_fingerprint,max_candidates,manifest,revoked_at,revoke_approval_ref)
        VALUES($1,$2,'10',$3,$4,$5,clock_timestamp(),$6)`,
      [manifest.manifestHash, manifest.approvalRef, manifest.fingerprint, manifest.maxCandidates,
        JSON.stringify(manifest.approvalDocument), `${manifest.approvalRef}:revoke`]);
      else await db.query(`UPDATE scout_backlog_recovery_grants SET revoked_at=clock_timestamp(),revoke_approval_ref=$2
        WHERE manifest_hash=$1 AND revoked_at IS NULL`, [manifest.manifestHash, `${manifest.approvalRef}:revoke`]);
      return { ...summary(manifest), status:'revoked', revoked:true, idempotent:false, tombstone:!grant };
    });
  }
  async function inspect(pool, raw) {
    const manifest = normalize(raw);
    return readOnly(pool, async db => {
      await assertSchema(db);
      const grant = await readGrant(db, manifest, false);
      const run = (await db.query(`SELECT manifest_hash,state,stop_reason,result,created_at,finished_at
        FROM scout_backlog_recovery_runs WHERE approval_ref=$1 AND tenant_id='10'`, [manifest.approvalRef])).rows[0] || null;
      if (run && run.manifest_hash !== manifest.manifestHash) fail('backlog_recovery_receipt_conflict');
      const items = run ? (await db.query(`SELECT candidate_key,disposition,skip_reason,claim_token::text AS claim_token
        FROM scout_backlog_recovery_items WHERE approval_ref=$1 ORDER BY candidate_key`, [manifest.approvalRef])).rows : [];
      const reconciled = await reconcile(db, manifest, items);
      const now = (await db.query('SELECT clock_timestamp() AS now')).rows[0].now;
      return { ...summary(manifest), observedAt:new Date(now).toISOString(),
        status:!grant ? 'not_installed' : grant.revoked_at ? 'revoked' : +new Date(now) >= Date.parse(manifest.window.expiresAt) ? 'expired' : 'installed',
        installedAt:grant?.installed_at || null, revokedAt:grant?.revoked_at || null, tombstone:Boolean(grant && !grant.installed_at),
        run:run && { state:run.state, stopReason:run.stop_reason, createdAt:run.created_at, finishedAt:run.finished_at },
        outcome:!run ? 'not_started' : run.state,
        skipped:items.filter(i => i.disposition === 'skipped').map(i => ({ candidateKey:i.candidate_key, reason:i.skip_reason })),
        candidates:reconciled, totals:summarize(reconciled), receiptOnly:true };
    });
  }

  async function execute({ pool, manifest:raw, confirmationHash }) {
    const manifest = normalize(raw);
    if (confirmationHash !== manifest.manifestHash) fail('backlog_recovery_exact_confirmation_required');
    if (!pool?.connect || pool.options?.max < 3
      || !(pool.options?.connectionTimeoutMillis > 0 && pool.options.connectionTimeoutMillis <= 2000)
      || !['statement_timeout','query_timeout'].every(key => pool.options?.[key] > 0 && pool.options[key] <= 30000)
      || !(pool.options?.lock_timeout > 0 && pool.options.lock_timeout <= 2000)) fail('backlog_recovery_dependencies_required');
    const { GovernedOutboundStore } = require('./governedOutboundStore');
    const loop = require('./maxOutboundControlLoop');
    const tickets = createCandidateTicketStore({ pool, tenantId:TENANT, fingerprint:manifest.fingerprint, lockTimeoutMs:2000 });
    // Probe every local dependency before consuming the approval.
    await tickets.assertReady();
    const ownership = new GovernedOutboundStore(pool, TENANT);
    await ownership.candidateOwnership({ company:'', domain:'' });
    await pool.query('SELECT client_id,company,domain,website_url,vertical,location,source,enrichment_attempts,last_attempt_at,notes FROM scout_unenriched LIMIT 0');
    const executionToken = randomUUID();

    return exclusive(pool, async () => {
      // Consumption, eligibility and ticket claims commit atomically, or not at all.
      const consumed = await transaction(pool, async db => {
        await sharedRecoveryLock(db);
        const { program, source, scope } = await assertAuthority(db, manifest);
        const dependencyKey = candidateTicketDependencyKey({ tenantId:TENANT, program, source });
        const inserted = (await db.query(`INSERT INTO scout_backlog_recovery_runs
          (approval_ref,manifest_hash,tenant_id,search_fingerprint,dependency_key,execution_token)
          VALUES($1,$2,'10',$3,$4,$5) ON CONFLICT DO NOTHING RETURNING approval_ref`,
        [manifest.approvalRef, manifest.manifestHash, manifest.fingerprint, dependencyKey, executionToken])).rows;
        if (inserted.length !== 1) fail('backlog_recovery_already_consumed');
        const checks = await eligibility(db, manifest, true);
        const eligible = checks.filter(c => c.eligible).map(c => c.candidateKey);
        if (!eligible.length) fail('backlog_recovery_no_eligible_candidates');
        const claims = await tickets.claimDue({ dependencyKey, limit:eligible.length, candidateKeys:eligible, db });
        if (claims.length !== eligible.length || claims.some(c => !eligible.includes(c.candidateKey))) fail('backlog_recovery_claim_unconfirmed');
        for (const check of checks) {
          const claim = claims.find(c => c.candidateKey === check.candidateKey);
          await db.query(`INSERT INTO scout_backlog_recovery_items(approval_ref,tenant_id,search_fingerprint,plan_version,ticket_version,
            candidate_key,disposition,skip_reason,claim_token,membership_revision) VALUES($1,'10',$2,$3,$4,$5,$6,$7,$8,$9)`,
          [manifest.approvalRef, manifest.fingerprint, manifest.planVersion, manifest.ticketVersion, check.candidateKey,
            claim ? 'claimed' : 'skipped', claim ? null : check.reason, claim?.claimToken || null, claim?.membershipRevision || null]);
        }
        return { program, source, scope, dependencyKey, claims, skipped:checks.filter(c => !c.eligible) };
      });

      const tokens = new Map(consumed.claims.map(c => [c.candidateKey, c.claimToken]));
      const own = refs => {
        if (!Array.isArray(refs) || !refs.length || refs.some(ref => tokens.get(ref?.candidateKey) !== ref.claimToken)) {
          fail('backlog_recovery_claim_not_owned');
        }
      };
      async function assertStillAuthorized() {
        await transaction(pool, async db => {
          await sharedRecoveryLock(db);
          await assertAuthority(db, manifest);
          const run = (await db.query(`SELECT state,execution_token::text AS token FROM scout_backlog_recovery_runs
            WHERE approval_ref=$1 FOR SHARE`, [manifest.approvalRef])).rows[0];
          if (run?.state !== 'claimed' || run.token !== executionToken) fail('backlog_recovery_run_fenced');
        });
      }
      const pass = {
        // Authority (window, grant, kill-switch state, source pins) is rechecked
        // before each candidate; a lapse stops the run rather than skipping.
        async renew(refs) {
          own(refs);
          await assertStillAuthorized();
          try { await tickets.renew(refs); return true; }
          catch (error) { if (STALE_CODES.has(error.ticketCode)) return false; throw error; }
        },
        finish(refs, outcome, write) {
          own(refs);
          return tickets.finish(refs, { ...outcome, dependencyKey:consumed.dependencyKey }, write);
        },
      };
      let admission = null, failure = null;
      try {
        const cities = loop.resolveScoutRampAllowedCities(consumed.scope);
        admission = await loop._test.persistDiscoveredCompanies(pool, ownership, { tenantId:TENANT,
          companies:claimedCompanies(consumed.claims, TENANT),
          scoutContext:{ scope:consumed.scope, source:consumed.source, allowedCities:cities, serviceAreas:cities,
            clientId:10, authorizedTenantId:TENANT, program:consumed.program,
            investigateBusinessEvidence:false, ticketPass:pass } });
      } catch (error) { failure = error; }

      const items = consumed.claims.map(c => ({ candidate_key:c.candidateKey, disposition:'claimed', claim_token:c.claimToken }));
      let reconciled = null;
      try { reconciled = await reconcile(pool, manifest, items); } catch (error) { failure = failure || error; }
      const totals = reconciled ? summarize(reconciled) : null;
      const complete = !failure && totals && totals.completed === totals.claimed && totals.queuedNotVerifiedDownstream === 0;
      const stopReason = failure ? stopCode(failure) : complete ? null
        : totals?.queuedNotVerifiedDownstream ? 'backlog_recovery_downstream_unverified' : 'backlog_recovery_claims_not_completed';
      const result = { admitted:Number.isSafeInteger(admission?.inserted) ? admission.inserted : null, totals,
        skipped:consumed.skipped.map(c => ({ candidateKey:c.candidateKey, reason:c.reason })) };
      const finished = await pool.query(`UPDATE scout_backlog_recovery_runs SET state=$3,stop_reason=$4,result=$5::jsonb,finished_at=clock_timestamp()
        WHERE approval_ref=$1 AND execution_token=$2 AND state='claimed' RETURNING approval_ref`,
      [manifest.approvalRef, executionToken, complete ? 'completed' : 'stopped', stopReason, JSON.stringify(result)]);
      if (finished.rows.length !== 1) fail('backlog_recovery_finish_unconfirmed');
      if (failure) throw Object.assign(failure, { backlogRecoveryResult:result });
      return { ...summary(manifest), status:complete ? 'completed' : 'stopped', stopReason, ...result,
        candidates:reconciled, cleanVerifiedContacts:0, providerCalls:0 };
    });
  }

  return { validateManifest:normalize, preview, install, revoke, inspect, execute };
}

// Read-only drafting aid: binds the current source/control pins and lists the due,
// unattempted, unleased persisted tickets. It never installs or claims anything.
async function draftManifest(pool, input, { approvedFingerprint = APPROVED_FINGERPRINT } = {}) {
  const keys = ['approvalRef','programId','missionId','quarantineDecisionHash','expectedRevision','notBefore','expiresAt','maxCandidates'];
  if (!exactKeys(input, keys)) fail('backlog_recovery_draft_input_invalid');
  return readOnly(pool, async db => {
    const { scoutSearchFingerprint } = require('./maxOutboundControlLoop');
    const program = (await db.query(`SELECT ${PROGRAM_AUTHORITY_COLUMNS} FROM acquisition_outbound_programs WHERE tenant_id=$1 AND id=$2`, [TENANT,input.programId])).rows[0];
    const source = (await db.query('SELECT id,objective,target_segment,payload FROM acquisition_missions WHERE tenant_id=$1 AND id=$2', [TENANT,input.missionId])).rows[0];
    if (!program || !source) fail('backlog_recovery_source_changed');
    const fingerprint = scoutSearchFingerprint(program, source);
    if (fingerprint !== approvedFingerprint) fail('backlog_recovery_fingerprint_not_authorized');
    const control = (await db.query('SELECT singleton,maintenance_armed,manifest FROM anchor_email_recovery_control')).rows;
    if (control.length !== 1) fail('backlog_recovery_closed_control_required');
    const due = (await db.query(`SELECT candidate_key,membership_revision::text AS membership_revision,attempts::int AS attempts
      FROM scout_candidate_work w WHERE ${SCOPE_SQL} AND state IN ('pending','deferred') AND attempts=0
        AND (claim_token IS NULL OR lease_until <= clock_timestamp())
        AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())
        AND EXISTS(SELECT 1 FROM scout_candidate_memberships m WHERE m.tenant_id=w.tenant_id AND m.search_fingerprint=w.search_fingerprint
          AND m.plan_version=w.plan_version AND m.ticket_version=w.ticket_version AND m.candidate_key=w.candidate_key)
      ORDER BY next_attempt_at NULLS FIRST,created_at,candidate_key`, [TENANT, fingerprint, PLAN_VERSION, TICKET_VERSION])).rows;
    const limit = Math.min(Number(input.maxCandidates) || 0, MAX_CANDIDATES);
    const selected = due.slice(0, limit).sort((a,b) => a.candidate_key < b.candidate_key ? -1 : 1);
    const manifest = { version:1, kind:'scout_backlog_recovery', approvalRef:input.approvalRef, tenantId:TENANT,
      programId:input.programId, missionId:input.missionId, programHash:hash(program), sourceHash:hash(source), fingerprint,
      planVersion:PLAN_VERSION, ticketVersion:TICKET_VERSION, maxCandidates:selected.length,
      candidates:selected.map(row => ({ candidateKey:row.candidate_key, membershipRevision:row.membership_revision, attempts:row.attempts })),
      authority:{ expectedRevision:input.expectedRevision, quarantineDecisionHash:input.quarantineDecisionHash,
        recoveryControlHash:hash(control[0]), discovery:false, places:false, enrichment:false, verification:false, preparation:false, email:false },
      window:{ notBefore:input.notBefore, expiresAt:input.expiresAt } };
    const validated = validateManifest(manifest, approvedFingerprint);
    return { manifest, manifestHash:validated.manifestHash, dueUnattempted:due.length, selected:selected.length, draftOnly:true };
  });
}

const production = createBacklogRecovery();
module.exports = { APPROVED_FINGERPRINT, MAX_CANDIDATES, validateManifest:production.validateManifest,
  previewBacklogRecovery:production.preview, installBacklogRecovery:production.install,
  revokeBacklogRecovery:production.revoke, inspectBacklogRecovery:production.inspect,
  executeBacklogRecovery:production.execute, draftBacklogRecoveryManifest:(pool, input) => draftManifest(pool, input),
  _test:{ createBacklogRecovery, draftManifest } };
