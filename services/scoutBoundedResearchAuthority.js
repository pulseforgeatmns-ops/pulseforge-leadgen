'use strict';
// Distinct operator-installed research authority. No application bootstrap,
// provider calls, environment mutation, schema creation, or preparation rights.
const { AsyncLocalStorage } = require('node:async_hooks');
const { isProxy } = require('node:util').types;
const scope = new AsyncLocalStorage();
const RECOVERY_LOCK_KEY = 'anchor-email-recovery:10';
const HEX64 = /^[a-f0-9]{64}(?![\s\S])/;
const AUTHORITY_KEYS = ['kind','expectedRevision','quarantineDecisionHash','recoveryControlHash','provider',
  'maxSearchAttempts','maxDetailsAttempts','enrichment','verification','preparation','email'];
const fail = code => { throw Object.assign(Error(code), { code, retryAllowed:false }); };
const hash = value => require('./scoutBoundedAcquisitionBudget').hash(value);

function validateJsonDocument(input) {
  const ancestors = new Set(); let nodes = 0;
  function visit(value, depth) {
    if (++nodes > 4096 || depth > 16) fail('bounded_manifest_invalid');
    if (value === null || typeof value === 'boolean') return;
    if (typeof value === 'string') { if (value.length > 8192) fail('bounded_manifest_invalid'); return; }
    if (typeof value === 'number') { if (!Number.isFinite(value)) fail('bounded_manifest_invalid'); return; }
    if (!value || typeof value !== 'object' || isProxy(value) || ancestors.has(value)) fail('bounded_manifest_invalid');
    const array = Array.isArray(value), proto = Object.getPrototypeOf(value);
    if (array ? proto !== Array.prototype : proto !== Object.prototype && proto !== null) fail('bounded_manifest_invalid');
    const descriptors = Object.getOwnPropertyDescriptors(value), keys = Reflect.ownKeys(descriptors);
    if (keys.some(key => typeof key !== 'string')) fail('bounded_manifest_invalid');
    if (array && (value.length > 256 || keys.length !== value.length + 1
      || !keys.every(key => key === 'length' || /^(0|[1-9][0-9]*)(?![\s\S])/.test(key) && Number(key) < value.length))) fail('bounded_manifest_invalid');
    ancestors.add(value);
    for (const key of keys) {
      if (array && key === 'length') continue;
      const descriptor = descriptors[key];
      if (!Object.hasOwn(descriptor,'value') || !descriptor.enumerable) fail('bounded_manifest_invalid');
      visit(descriptor.value, depth + 1);
    }
    ancestors.delete(value);
  }
  visit(input, 0);
  if (JSON.stringify(input).length > 32768) fail('bounded_manifest_invalid');
  return input;
}
function validateResearchFields(raw) {
  validateJsonDocument(raw);
  const a = raw?.researchAuthority;
  if (raw?.version !== 2 || !a || Array.isArray(a) || Object.keys(a).length !== AUTHORITY_KEYS.length
    || AUTHORITY_KEYS.some(key => !Object.hasOwn(a,key))
    || a.kind !== 'bounded_places_research' || a.provider !== 'google_places'
    || typeof a.expectedRevision !== 'string' || !/^[a-f0-9]{40}(?![\s\S])/.test(a.expectedRevision)
    || !['quarantineDecisionHash','recoveryControlHash'].every(key => typeof a[key] === 'string' && HEX64.test(a[key]))
    || a.maxSearchAttempts !== 3 || a.maxDetailsAttempts !== 20
    || !['enrichment','verification','preparation','email'].every(key => a[key] === false)
    || !Number.isFinite(Date.parse(raw.grant?.notBefore)) || !Number.isFinite(Date.parse(raw.grant?.expiresAt))
    || Date.parse(raw.grant.expiresAt) <= Date.parse(raw.grant.notBefore)
    || Date.parse(raw.grant.expiresAt) - Date.parse(raw.grant.notBefore) > 3600000) fail('bounded_research_manifest_invalid');
  return a;
}
function normalized(input) {
  if (isProxy(input)) fail('bounded_manifest_invalid');
  const descriptor = input && Object.getOwnPropertyDescriptor(input,'approvalDocument');
  if (descriptor && !Object.hasOwn(descriptor,'value')) fail('bounded_manifest_invalid');
  const raw = descriptor ? descriptor.value : input;
  validateResearchFields(raw);
  return require('./scoutBoundedAcquisition').validateManifest(raw);
}
function original(manifest) {
  const raw = manifest.approvalDocument;
  if (!raw || hash(raw) !== manifest.manifestHash) fail('bounded_research_manifest_invalid');
  return raw;
}
function environment(manifest) {
  const env = process.env;
  if (env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE !== 'true'
    || env.ANCHOR_RESEARCH_PROVIDERS_PAUSED !== 'true'
    || env.ANCHOR_GOVERNED_OUTBOUND_ENABLED !== 'false'
    || ![undefined,''].includes(env.ANCHOR_EMAIL_RECOVERY_MANIFEST_ID)
    || ![undefined,''].includes(env.ANCHOR_EMAIL_SCOPED_RESUME_ID)
    || env.RAILWAY_GIT_COMMIT_SHA !== manifest.researchAuthority.expectedRevision) fail('bounded_research_closed_environment_required');
}
async function assertSchema(db) {
  await db.query('SELECT manifest_hash,approval_ref,tenant_id,manifest,created_at,installed_at,revoked_at,revoke_approval_ref FROM scout_bounded_research_grants LIMIT 0');
  const guards = (await db.query(`SELECT count(*)::int AS count FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='O'
    AND tgrelid='scout_bounded_research_grants'::regclass
    AND ((tgname='scout_bounded_research_no_delete' AND tgtype=42
      AND tgfoid=to_regprocedure('public.scout_bounded_research_permanent()'))
      OR (tgname='scout_bounded_research_revoke_only' AND tgtype=19
      AND tgfoid=to_regprocedure('public.scout_bounded_research_revoke_once()')))`)).rows[0];
  if (guards?.count !== 2) fail('bounded_research_schema_guards_missing');
  const openIndex = (await db.query(`SELECT i.indisunique AND i.indisvalid AND i.indisready
    AND pg_get_expr(i.indpred,i.indrelid)='(revoked_at IS NULL)'
    AND (SELECT array_agg(a.attname::text ORDER BY k.ord) FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,ord)
      JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum)=ARRAY['tenant_id']::text[] AS valid
    FROM pg_index i WHERE i.indexrelid=to_regclass('public.scout_bounded_research_one_open_tenant')
      AND i.indrelid='scout_bounded_research_grants'::regclass`)).rows;
  if (openIndex.length !== 1 || openIndex[0].valid !== true) fail('bounded_research_schema_identity_missing');
  const constraints = (await db.query(`SELECT c.contype,ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(attnum,ord)
    JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum ORDER BY k.ord) AS columns
    FROM pg_constraint c WHERE c.conrelid='scout_bounded_research_grants'::regclass AND c.convalidated`)).rows;
  if (!['manifest_hash','approval_ref'].every(column => constraints.some(row => ['p','u'].includes(row.contype)
    && row.columns.length === 1 && row.columns[0] === column))) fail('bounded_research_schema_identity_missing');
}
async function readGrant(db, manifest, lock = false) {
  const rows = (await db.query(`SELECT manifest_hash,approval_ref,tenant_id,manifest,created_at,installed_at,revoked_at,revoke_approval_ref
    FROM scout_bounded_research_grants WHERE manifest_hash=$1 OR approval_ref=$2${lock ? ' FOR SHARE' : ''}`,
  [manifest.manifestHash,manifest.approvalRef])).rows;
  if (rows.length > 1 || rows.length === 1 && (rows[0].manifest_hash !== manifest.manifestHash
    || rows[0].approval_ref !== manifest.approvalRef || rows[0].tenant_id !== '10'
    || hash(rows[0].manifest) !== manifest.manifestHash || hash(rows[0].manifest) !== hash(original(manifest)))) fail('bounded_research_grant_conflict');
  return rows[0] || null;
}
async function assertResearchAuthority(db, input, { installed = true, lock = true } = {}) {
  const manifest = normalized(input); environment(manifest);
  await assertSchema(db);
  const control = (await db.query(`SELECT singleton,maintenance_armed,manifest FROM anchor_email_recovery_control${lock ? ' FOR SHARE' : ''}`)).rows;
  if (control.length !== 1 || control[0].singleton !== true || control[0].maintenance_armed !== true || control[0].manifest !== null
    || hash(control[0]) !== manifest.researchAuthority.recoveryControlHash) fail('bounded_research_closed_control_required');
  const active = (await db.query("SELECT manifest_id FROM anchor_scoped_resume_grants WHERE tenant_id='10' AND revoked_at IS NULL LIMIT 1")).rows;
  if (active.length) fail('bounded_research_other_authority_open');
  if (lock) await db.query("SELECT decision_hash FROM anchor_email_quarantines WHERE tenant_id='10' AND decision_hash=$1 FOR SHARE",
    [manifest.researchAuthority.quarantineDecisionHash]);
  await require('./anchorEmailQuarantine').requireQuarantine(db, manifest.researchAuthority.quarantineDecisionHash);
  await require('./scoutBoundedAcquisition').loadBoundSource(db,manifest,lock);
  const grant = await readGrant(db,manifest,lock);
  const otherResearch = (await db.query("SELECT manifest_hash FROM scout_bounded_research_grants WHERE tenant_id='10' AND revoked_at IS NULL AND manifest_hash<>$1 LIMIT 1", [manifest.manifestHash])).rows;
  if (otherResearch.length) fail('bounded_research_other_authority_open');
  if (grant?.revoked_at) fail('bounded_research_grant_revoked');
  if (installed && !grant?.installed_at) fail('bounded_research_grant_required');
  // Time is sampled only after every potentially waiting row lock.
  const now = (await db.query('SELECT clock_timestamp() AS now')).rows[0]?.now;
  if (!now || +new Date(now) < Date.parse(manifest.grant.notBefore)
    || +new Date(now) >= Date.parse(manifest.grant.expiresAt)) fail('bounded_research_window_inactive');
  return { manifest, grant, now };
}
async function locked(pool, work, exclusive = false) {
  return require('./scoutBoundedAcquisitionBudget').transaction(pool, async db => {
    await db.query(`SELECT pg_advisory_xact_lock${exclusive ? '' : '_shared'}(hashtextextended($1,0))`, [RECOVERY_LOCK_KEY]);
    return work(db);
  });
}
async function runWithResearchAuthority({ pool, manifest:input, apiKey, fetchImpl }) {
  const manifest = normalized(input);
  await locked(pool, async db => { await assertResearchAuthority(db,manifest); await assertFirstPageUnclaimed(db,manifest); });
  const current = { active:true, tenantId:'10', manifestHash:manifest.manifestHash, expiresAt:Date.parse(manifest.grant.expiresAt) };
  try { return await scope.run(current, () => require('./scoutBoundedAcquisition').runBoundedAcquisitionInScope({
    pool,manifest:original(manifest),apiKey,fetchImpl })); }
  finally { current.active = false; }
}
function assertResearchExecutionScope(manifestHash) {
  const current = scope.getStore();
  if (!current?.active || current.tenantId !== '10' || current.manifestHash !== manifestHash
    || Date.now() >= current.expiresAt) fail('bounded_research_execution_scope_required');
  return true;
}
function allowsBoundedPlacesResearch(provider, tenant) {
  const current = scope.getStore();
  return Boolean(current?.active && current.tenantId === '10' && (tenant === '10' || tenant === 10)
    && ['research','google_places'].includes(provider) && Date.now() < current.expiresAt);
}
async function readOnly(pool, work) {
  const db = await pool.connect(); let discard = false, committing = false;
  try {
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    await db.query("SET LOCAL statement_timeout='5s'");
    await db.query("SET LOCAL lock_timeout='2s'");
    const snapshotAt = (await db.query('SELECT clock_timestamp() AS snapshot_at')).rows[0].snapshot_at;
    const result = await work(db);
    committing = true; await db.query('COMMIT'); committing = false;
    return { ...result, snapshotAt:new Date(snapshotAt).toISOString() };
  } catch (failure) {
    try { await db.query('ROLLBACK'); } catch { discard = true; }
    throw failure;
  } finally { db.release(discard || committing); }
}
function summary(manifest) {
  return { version:2,manifestHash:manifest.manifestHash,approvalRef:manifest.approvalRef,tenantId:'10',
    expectedRevision:manifest.researchAuthority.expectedRevision,programId:manifest.programId,missionId:manifest.missionId,
    programHash:manifest.programHash,sourceHash:manifest.sourceHash,fingerprint:manifest.fingerprint,
    quarantineDecisionHash:manifest.researchAuthority.quarantineDecisionHash,recoveryControlHash:manifest.researchAuthority.recoveryControlHash,
    coverageHash:manifest.grant.manifestHash,query:manifest.query,rootQueryKey:manifest.rootQueryKey,firstPageKey:manifest.pageKey,
    notBefore:manifest.grant.notBefore,expiresAt:manifest.grant.expiresAt,
    maxNewPages:1,maxSearchAttempts:3,maxDetailsAttempts:20,maxModeledUsdMicros:manifest.maxModeledUsdMicros,
    output:'raw_business_queue_only',enrichment:false,verification:false,preparation:false,email:false,retryAllowed:false };
}
async function assertFirstPageUnclaimed(db, manifest) {
  const state = (await db.query(`SELECT
    EXISTS(SELECT 1 FROM scout_query_checkpoints WHERE tenant_id='10' AND search_fingerprint=$1 AND query_key=$2
      AND plan_version='places-market-query-v1' AND state='handled') AS legacy_handled,
    EXISTS(SELECT 1 FROM scout_query_pages WHERE tenant_id='10' AND search_fingerprint=$1
      AND (root_query_key=$2 OR root_query_key=$3 OR query_key=$3)) AS prior_page,
    EXISTS(SELECT 1 FROM scout_query_checkpoints WHERE tenant_id='10' AND search_fingerprint=$1 AND query_key=$3) AS prior_refresh,
    EXISTS(SELECT 1 FROM scout_bounded_acquisition_runs WHERE approval_ref=$4 OR manifest_hash=$5
      OR (tenant_id='10' AND search_fingerprint=$1 AND query_key=$3)) AS prior_run`,
  [manifest.fingerprint,manifest.rootQueryKey,manifest.pageKey,manifest.approvalRef,manifest.manifestHash])).rows[0];
  if (!state || state.legacy_handled !== true || state.prior_page !== false || state.prior_refresh !== false)
    fail('bounded_fresh_first_page_required');
  if (state.prior_run !== false) fail('bounded_research_run_already_claimed');
  const v1 = (await db.query(`SELECT EXISTS(SELECT 1 FROM scout_query_coverage_epochs WHERE tenant_id='10' AND search_fingerprint=$1)
    OR EXISTS(SELECT 1 FROM scout_query_coverage_epoch_receipts WHERE tenant_id='10' AND search_fingerprint=$1) AS legacy`,[manifest.fingerprint])).rows[0];
  if (!v1 || v1.legacy !== false) fail('coverage_grant_legacy_reconciliation_required');
  const allocation = (await db.query(`SELECT allocation_hash,epoch,max_roots,max_pages_per_root FROM scout_query_coverage_allocations
    WHERE tenant_id='10' AND search_fingerprint=$1`,[manifest.fingerprint])).rows;
  if (allocation.length > 1 || allocation.length === 1 && (allocation[0].allocation_hash !== manifest.grant.allocationHash
    || allocation[0].epoch !== 1 || allocation[0].max_roots !== 54 || allocation[0].max_pages_per_root !== 3)) fail('coverage_allocation_corrupt');
  const mappings = (await db.query(`SELECT allocation_hash,original_root_key,epoch_root_key FROM scout_query_coverage_grant_roots
    WHERE tenant_id='10' AND search_fingerprint=$1 AND (original_root_key=$2 OR epoch_root_key=$3)`,
  [manifest.fingerprint,manifest.rootQueryKey,manifest.pageKey])).rows;
  if (mappings.length > 1 || mappings.length === 1 && (!allocation.length
    || mappings[0].allocation_hash !== manifest.grant.allocationHash || mappings[0].original_root_key !== manifest.rootQueryKey
    || mappings[0].epoch_root_key !== manifest.pageKey)) fail('coverage_grant_mapping_corrupt');
  const counts = (await db.query(`SELECT
    (SELECT count(*)::int FROM scout_query_coverage_grant_roots WHERE tenant_id='10' AND search_fingerprint=$1) AS roots,
    (SELECT count(*)::int FROM scout_query_coverage_grant_pages WHERE tenant_id='10' AND search_fingerprint=$1 AND grant_hash=$2) AS pages`,
  [manifest.fingerprint,manifest.grant.manifestHash])).rows[0];
  if (!counts || counts.roots > 54 || !mappings.length && counts.roots >= 54) fail('coverage_allocation_root_limit');
  if (counts.pages !== 0) fail('bounded_fresh_first_page_required');
  const grants = (await db.query(`SELECT grant_hash,allocation_hash,approval_ref,root_query_keys,not_before,expires_at,max_new_pages,revoked_at
    FROM scout_query_coverage_grants WHERE tenant_id='10' AND search_fingerprint=$1 AND (grant_hash=$2 OR approval_ref=$3)`,
  [manifest.fingerprint,manifest.grant.manifestHash,manifest.approvalRef])).rows;
  if (grants.length > 1) fail('coverage_grant_changed');
  const grant = grants[0], config = manifest.grant;
  if (grant && (grant.grant_hash !== config.manifestHash || grant.allocation_hash !== config.allocationHash
    || grant.approval_ref !== manifest.approvalRef || JSON.stringify(grant.root_query_keys) !== JSON.stringify(config.rootQueryKeys)
    || +new Date(grant.not_before) !== Date.parse(config.notBefore) || +new Date(grant.expires_at) !== Date.parse(config.expiresAt)
    || grant.max_new_pages !== 1)) fail('coverage_grant_changed');
  if (grant?.revoked_at) fail('coverage_grant_revoked');
}
async function previewResearchAuthority(pool, raw) {
  const manifest = normalized(raw);
  return readOnly(pool, async db => {
    const { grant,now } = await assertResearchAuthority(db,manifest,{installed:false,lock:false});
    await assertFirstPageUnclaimed(db,manifest);
    return { ...summary(manifest),status:grant ? 'installed' : 'not_installed',observedAt:new Date(now).toISOString(),
      installedAuthority:Boolean(grant?.installed_at),previewOnly:true };
  });
}
async function installResearchAuthority(pool, raw, confirmationHash) {
  const manifest = normalized(raw);
  if (confirmationHash !== manifest.manifestHash) fail('bounded_research_exact_confirmation_required');
  return locked(pool, async db => {
    const { grant } = await assertResearchAuthority(db,manifest,{installed:false});
    if (grant) return { ...summary(manifest),status:'installed',installed:false,idempotent:true };
    await assertFirstPageUnclaimed(db,manifest);
    const other = (await db.query("SELECT manifest_hash FROM scout_bounded_research_grants WHERE tenant_id='10' AND revoked_at IS NULL LIMIT 1")).rows;
    if (other.length) fail('bounded_research_other_authority_open');
    await db.query(`INSERT INTO scout_bounded_research_grants(manifest_hash,approval_ref,tenant_id,manifest,installed_at)
      VALUES($1,$2,'10',$3,clock_timestamp())`,[manifest.manifestHash,manifest.approvalRef,JSON.stringify(original(manifest))]);
    return { ...summary(manifest),status:'installed',installed:true,idempotent:false };
  },true);
}
async function revokeResearchAuthority(pool, raw, confirmationHash) {
  const manifest = normalized(raw);
  if (confirmationHash !== manifest.manifestHash) fail('bounded_research_exact_confirmation_required');
  // Revocation intentionally does not depend on current env/source/expiry. A
  // closed tombstone serializes an uncertain concurrent install without refund.
  return locked(pool, async db => {
    await assertSchema(db);
    const grant = await readGrant(db,manifest,true);
    if (grant?.revoked_at) return { ...summary(manifest),status:'revoked',revoked:false,idempotent:true,tombstone:!grant.installed_at };
    if (!grant) await db.query(`INSERT INTO scout_bounded_research_grants
      (manifest_hash,approval_ref,tenant_id,manifest,revoked_at,revoke_approval_ref)
      VALUES($1,$2,'10',$3,clock_timestamp(),$4)`,
    [manifest.manifestHash,manifest.approvalRef,JSON.stringify(original(manifest)),`${manifest.approvalRef}:revoke`]);
    else await db.query(`UPDATE scout_bounded_research_grants SET revoked_at=clock_timestamp(),revoke_approval_ref=$2
      WHERE manifest_hash=$1 AND revoked_at IS NULL`,[manifest.manifestHash,`${manifest.approvalRef}:revoke`]);
    return { ...summary(manifest),status:'revoked',revoked:true,idempotent:false,tombstone:!grant };
  },true);
}
async function inspectResearchAuthority(pool, raw) {
  const manifest = normalized(raw);
  return readOnly(pool,async db => {
    const grant = await readGrant(db,manifest,false);
    const run = (await db.query(`SELECT manifest_hash,state,created_at,finished_at,max_usd_micros FROM scout_bounded_acquisition_runs
      WHERE approval_ref=$1 AND tenant_id='10'`,[manifest.approvalRef])).rows[0] || null;
    if (run && run.manifest_hash !== manifest.manifestHash) fail('bounded_research_receipt_conflict');
    const attempts = run ? (await db.query(`SELECT operation,state,count(*)::int AS count,sum(reserved_usd_micros)::int AS micros,
      count(*) FILTER(WHERE response_hash IS NOT NULL)::int AS evidence_count
      FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1 GROUP BY operation,state ORDER BY operation,state`,[manifest.approvalRef])).rows : [];
    const work = run ? (await db.query(`SELECT w.state,count(DISTINCT w.candidate_key)::int AS count
      FROM scout_candidate_memberships m JOIN scout_candidate_work w USING(tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key)
      WHERE m.tenant_id='10' AND m.search_fingerprint=$1 AND m.query_key=$2
      GROUP BY w.state ORDER BY w.state`,[manifest.fingerprint,manifest.pageKey])).rows : [];
    const now = (await db.query('SELECT clock_timestamp() AS now')).rows[0].now;
    return { ...summary(manifest),observedAt:new Date(now).toISOString(),
      status:!grant ? 'not_installed' : grant.revoked_at ? 'revoked' : +new Date(now)>=Date.parse(manifest.grant.expiresAt) ? 'expired' : 'installed',
      installedAt:grant?.installed_at || null,revokedAt:grant?.revoked_at || null,tombstone:Boolean(grant && !grant.installed_at),
      run,attempts,modeledReservedUsdMicros:attempts.reduce((sum,row)=>sum+row.micros,0),
      pageWork:work,workSnapshotOnly:true,outcome:!run ? 'not_started' : run.state === 'completed' ? 'completed' : 'consumed_outcome_not_confirmed',
      cleanVerifiedContacts:0,executionAuthorizationNotRechecked:true };
  });
}
module.exports = { validateJsonDocument,validateResearchFields,assertResearchAuthority,runWithResearchAuthority,
  assertResearchExecutionScope,allowsBoundedPlacesResearch,previewResearchAuthority,installResearchAuthority,inspectResearchAuthority,revokeResearchAuthority };
