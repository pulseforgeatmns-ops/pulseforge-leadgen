'use strict';
const { randomUUID, createHash } = require('node:crypto');
const { canonicalJson } = require('./scoutCandidateTickets');
const { loadRecoveryState, RECOVERY_LOCK_KEY } = require('./anchorEmailRecovery');
const { assertResearchProviderAllowed } = require('../utils/anchorResearchProviderPause');
const { reserveApiCall, settleApiCall, assertApiUsageSchema } = require('../utils/apiSpendControl');
const { PLACES_UPPER_BOUND } = require('../utils/apiPricing');
const COST = Object.freeze({ text_search: 40000, place_details: 25000 });
const LIMIT = Object.freeze({ text_search: 3, place_details: 20 });
const error = code => Object.assign(Error(code), { code });
const hash = value => createHash('sha256').update(canonicalJson(JSON.parse(JSON.stringify(value)))).digest('hex');

async function transaction(pool, action) {
  const client = await pool.connect(); let committing = false, discard = false;
  try {
    await client.query('BEGIN');
    await client.query("SET LOCAL lock_timeout='2s'");
    await client.query("SET LOCAL statement_timeout='30s'");
    const result = await action(client);
    committing = true; await client.query('COMMIT'); committing = false;
    return result;
  } catch (failure) {
    try { await client.query('ROLLBACK'); } catch (_) { discard = true; }
    if (committing) throw error('bounded_commit_uncertain');
    throw failure;
  } finally { client.release(discard || committing); }
}
async function assertLegacy(db) {
  assertResearchProviderAllowed('google_places', '10');
  if (process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED !== 'false') throw error('bounded_activation_required');
  const state = await loadRecoveryState(db);
  if (state.mode !== 'legacy') throw error('bounded_recovery_not_legacy');
}
async function lockRecovery(db, manifest) {
  await db.query('SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))', [RECOVERY_LOCK_KEY]);
  if (manifest?.version === 2) {
    // A research grant does not reinterpret or disable the permanent recovery latch.
    // Scope entry is private to the DB-verified native runner, never a caller token.
    require('./scoutBoundedResearchAuthority').assertResearchExecutionScope(manifest.manifestHash);
    assertResearchProviderAllowed('google_places', '10');
    await require('./scoutBoundedResearchAuthority').assertResearchAuthority(db, manifest);
  } else await assertLegacy(db);
}
async function assertGrant(db, manifest) {
  // Lock first, then inspect a fresh DB clock. Time predicates in a locking
  // SELECT can be evaluated before a wait on an unchanged row.
  const { rows } = await db.query(`SELECT grant_hash FROM scout_query_coverage_grants
    WHERE tenant_id=$1 AND search_fingerprint=$2 AND grant_hash=$3 AND revoked_at IS NULL
      AND root_query_keys=ARRAY[$4]::text[] AND max_new_pages=1
      AND approval_ref=$5 AND allocation_hash=$6 AND not_before=$7::timestamptz AND expires_at=$8::timestamptz FOR SHARE`,
  [manifest.tenantId, manifest.fingerprint, manifest.grant.manifestHash, manifest.rootQueryKey,
    manifest.approvalRef,manifest.grant.allocationHash,manifest.grant.notBefore,manifest.grant.expiresAt]);
  const clock = (await db.query('SELECT clock_timestamp() AS now')).rows[0]?.now;
  if (rows.length !== 1 || !clock || +new Date(clock)<Date.parse(manifest.grant.notBefore)
    || +new Date(clock)>=Date.parse(manifest.grant.expiresAt)) throw error('bounded_grant_inactive');
}

function responseEvidence(operation, data) {
  const text = (value,max=2048) => typeof value === 'string' ? value.slice(0,max) : null;
  const business = row => ({ place_id:text(row?.place_id,512),name:text(row?.name,1024),
    formatted_address:text(row?.formatted_address),formatted_phone_number:text(row?.formatted_phone_number,100),
    website:text(row?.website),types:Array.isArray(row?.types) ? row.types.filter(x=>typeof x==='string').slice(0,30) : [],
    rating:Number.isFinite(row?.rating)?row.rating:null,business_status:text(row?.business_status,100) });
  return { status:['OK','ZERO_RESULTS','OVER_QUERY_LIMIT','UNKNOWN_ERROR','REQUEST_DENIED','INVALID_REQUEST','NOT_FOUND'].includes(data?.status) ? data.status : 'UNRECOGNIZED_STATUS',
    ...(operation==='text_search' ? { results:Array.isArray(data?.results)?data.results.slice(0,20).map(business):[],
      resultCount:Array.isArray(data?.results)?data.results.length:null,next_page_token:text(data?.next_page_token,8192) }
      : { result:data?.result?business(data.result):null }) };
}
function createBoundedBudget({ pool, manifest, assertSource }) {
  if (typeof assertSource !== 'function') throw error('bounded_source_check_required');
  const executionToken = randomUUID();
  let claimed = false, pending = null, stopped = false;
  async function ready() {
    await pool.query('SELECT approval_ref,manifest_hash,tenant_id,search_fingerprint,grant_hash,query_key,execution_token,max_usd_micros,state FROM scout_bounded_acquisition_runs LIMIT 0');
    await pool.query('SELECT id,approval_ref,operation,reserved_usd_micros,api_usage_id,state,http_status,response_evidence,response_hash,finished_at FROM scout_bounded_acquisition_attempts LIMIT 0');
    const constraints = (await pool.query(`SELECT c.contype,ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(attnum,ord)
      JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum ORDER BY k.ord) AS columns
      FROM pg_constraint c WHERE c.conrelid='scout_bounded_acquisition_runs'::regclass AND c.convalidated`)).rows;
    for (const columns of [['approval_ref'],['manifest_hash'],['tenant_id','search_fingerprint','grant_hash'],['tenant_id','search_fingerprint','query_key']]) {
      if (!constraints.some(row => ['p','u'].includes(row.contype) && JSON.stringify(row.columns)===JSON.stringify(columns))) throw error('bounded_ledger_identity_constraints_missing');
    }
    await assertApiUsageSchema(pool);
    // Every column used by the actual legacy attribution writer, without DDL.
    await pool.query(`SELECT caller,feature,mission_id,mission_stage,tenant_id,execution_id,operator_id,
      trigger_mode,endpoint,is_autocomplete,is_nearby_search,is_find_place,cost_class,http_status,google_status,latency_ms,
      hypothesis_id,hypothesis_label,evidence_requirement,investigation_task,provider_id,cache_hit,cache_miss,cache_age,
      cache_key,cache_strategy,original_query,normalized_query,businesses_returned,businesses_accepted,candidates_created,qualified_candidates
      FROM places_api_requests LIMIT 0`);
    for (const operation of Object.keys(COST)) if (!Number.isFinite(PLACES_UPPER_BOUND[operation]) || PLACES_UPPER_BOUND[operation] < 0 || PLACES_UPPER_BOUND[operation] * 1e6 > COST[operation]) throw error('bounded_pricing_changed');
  }
  async function claim() {
    await ready();
    await transaction(pool, async db => {
      await lockRecovery(db, manifest); await assertGrant(db, manifest); await assertSource(db);
      const { rows } = await db.query(`INSERT INTO scout_bounded_acquisition_runs
        (approval_ref,manifest_hash,tenant_id,search_fingerprint,grant_hash,query_key,execution_token,max_usd_micros)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING RETURNING approval_ref`,
      [manifest.approvalRef, manifest.manifestHash, manifest.tenantId, manifest.fingerprint,
        manifest.grant.manifestHash, manifest.pageKey, executionToken, manifest.maxModeledUsdMicros]);
      if (rows.length !== 1) throw error('bounded_run_already_claimed');
    });
    claimed = true;
  }
  async function reserve(metadata) {
    if (!claimed || stopped || pending || metadata.provider !== 'google_places' || !Object.hasOwn(COST, metadata.operation)
      || String(metadata.tenantId) !== manifest.tenantId || metadata.missionId !== manifest.missionId) throw error('bounded_attempt_denied');
    const id = randomUUID(), operation = metadata.operation;
    await transaction(pool, async db => {
      await lockRecovery(db, manifest); await assertGrant(db, manifest); await assertSource(db);
      const run = (await db.query(`SELECT state,execution_token FROM scout_bounded_acquisition_runs WHERE approval_ref=$1 FOR UPDATE`, [manifest.approvalRef])).rows[0];
      if (run?.state !== 'claimed' || run.execution_token !== executionToken) throw error('bounded_run_fenced');
      const rows = (await db.query('SELECT operation,reserved_usd_micros,state FROM scout_bounded_acquisition_attempts WHERE approval_ref=$1', [manifest.approvalRef])).rows;
      if (rows.some(row => row.state !== 'settled')) throw error('bounded_prior_attempt_unconfirmed');
      if (rows.filter(row => row.operation === operation).length >= LIMIT[operation]
        || rows.reduce((sum, row) => sum + row.reserved_usd_micros, 0) + COST[operation] > manifest.maxModeledUsdMicros) throw error('bounded_budget_exhausted');
      await db.query('INSERT INTO scout_bounded_acquisition_attempts(id,approval_ref,operation,reserved_usd_micros) VALUES($1,$2,$3,$4)', [id,manifest.approvalRef,operation,COST[operation]]);
    });
    pending = { id, operation, apiId: null, sent: false };
    try {
      // One shared policy event per possible HTTP attempt, never one event for
      // the whole run. Failure after either commit retains the local debit.
      pending.apiId = await reserveApiCall({ ...metadata, estimatedUsd:COST[operation]/1e6 }, { db:pool, existingSchemaOnly:true });
      const linked = await pool.query('UPDATE scout_bounded_acquisition_attempts SET api_usage_id=$2 WHERE id=$1 AND state=\'reserved\' RETURNING id', [id,pending.apiId]);
      if (linked.rows.length !== 1) throw error('bounded_api_link_unconfirmed');
      return id;
    } catch (failure) { stopped = true; throw failure; }
  }
  async function settle(id, outcome) {
    if (!pending || pending.id !== id) { stopped = true; throw error('bounded_settlement_mismatch'); }
    const attempt = pending;
    try {
      if (!attempt.apiId || !attempt.sent) throw error('bounded_transport_unconfirmed');
      await settleApiCall(attempt.apiId, { ...outcome, estimatedUsd:COST[attempt.operation]/1e6 }, { db:pool });
      const shared=(await pool.query('SELECT status,estimated_usd FROM api_usage_events WHERE id=$1',[attempt.apiId])).rows[0];
      if (!shared || shared.status!==outcome.status || Number(shared.estimated_usd)!==COST[attempt.operation]/1e6) throw error('bounded_shared_settlement_unconfirmed');
      const state = outcome.status === 'uncertain' ? 'uncertain' : 'settled';
      const result = await pool.query(`UPDATE scout_bounded_acquisition_attempts SET state=$2,http_status=$3,finished_at=clock_timestamp()
        WHERE id=$1 AND state='sent' RETURNING id`, [id,state,outcome.httpStatus || null]);
      if (result.rows.length !== 1) throw error('bounded_settlement_unconfirmed');
      if (state !== 'settled') stopped = true;
      pending = null;
    } catch (failure) { stopped = true; throw failure; }
  }
  async function transport(input, options, fetchImpl) {
    if (!pending || stopped || pending.sent) throw error('bounded_transport_without_reservation');
    const url = new URL(input);
    const operation = url.pathname === '/maps/api/place/textsearch/json' ? 'text_search'
      : url.pathname === '/maps/api/place/details/json' ? 'place_details' : null;
    const keys = operation === 'text_search' ? ['query','key'] : ['place_id','fields','key'];
    if (url.origin !== 'https://maps.googleapis.com' || url.username || url.password || url.hash || operation !== pending.operation
      || [...url.searchParams.keys()].some(key => !keys.includes(key)) || keys.some(key => url.searchParams.getAll(key).length !== 1)
      || (operation === 'text_search' && url.searchParams.get('query') !== manifest.query)
      || (operation === 'place_details' && (!allowedPlaceIds.has(url.searchParams.get('place_id'))
        || url.searchParams.get('fields') !== 'name,formatted_address,formatted_phone_number,website,place_id,types,address_component,business_status'))) throw error('bounded_transport_scope_mismatch');
    return transaction(pool, async db => {
      // Shared recovery lock covers this attempt only. Maintenance can interlock
      // between requests; no broad table lock is held over HTTP.
      await lockRecovery(db, manifest); await assertGrant(db, manifest); await assertSource(db);
      const result = await pool.query(`UPDATE scout_bounded_acquisition_attempts SET state='sent' WHERE id=$1 AND state='reserved' RETURNING id`, [pending.id]);
      if (result.rows.length !== 1) throw error('bounded_attempt_unconfirmed');
      pending.sent = true;
      await assertGrant(db,manifest);
      if (manifest.version === 2) await require('./scoutBoundedResearchAuthority').assertResearchAuthority(db,manifest);
      assertResearchProviderAllowed('google_places','10');
      // Commit durable sent marker separately before network. The outer shared
      // lock remains held until body consumption, at most 30 seconds.
      const response = await fetchImpl(input, { ...options, redirect:'error', signal:AbortSignal.timeout(30000) });
      if (response.redirected || response.status >= 300 && response.status < 400) throw error('bounded_redirect_denied');
      const data = await response.json();
      const evidence = responseEvidence(operation,data);
      const saved = await pool.query(`UPDATE scout_bounded_acquisition_attempts SET response_evidence=$2::jsonb,response_hash=$3
        WHERE id=$1 AND state='sent' AND response_evidence IS NULL RETURNING id`, [pending.id,JSON.stringify(evidence),hash(evidence)]);
      if (saved.rows.length !== 1) throw error('bounded_response_persistence_unconfirmed');
      if (operation === 'text_search' && data?.status === 'OK') {
        if (!Array.isArray(data.results) || data.results.length > 20) throw error('bounded_response_exceeds_page');
        for (const row of data.results) if (typeof row.place_id === 'string' && row.place_id) allowedPlaceIds.add(row.place_id);
      }
      return { ok:response.ok, status:response.status, json:async () => data };
    });
  }
  const allowedPlaceIds = new Set();
  async function finish(success) {
    stopped = true;
    const finished = await pool.query(`UPDATE scout_bounded_acquisition_runs SET state=$3,finished_at=clock_timestamp()
      WHERE approval_ref=$1 AND execution_token=$2 AND state='claimed' RETURNING approval_ref`, [manifest.approvalRef,executionToken,success && !pending ? 'completed' : 'stopped']);
    if (finished.rows.length !== 1) throw error('bounded_finish_unconfirmed');
  }
  return { ready, claim, reserveApiCall:reserve, settleApiCall:settle, transport, finish,
    get healthy() { return claimed && !stopped && !pending; } };
}
module.exports = { createBoundedBudget, transaction, assertLegacy, lockRecovery, assertGrant, hash, error, COST, LIMIT };
