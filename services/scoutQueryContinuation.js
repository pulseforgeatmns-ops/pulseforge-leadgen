'use strict';

const { createHash, randomUUID } = require('node:crypto');
const { PLAN_VERSION } = require('./scoutQueryCheckpoints');
const MAX_PAGES = 3;
const TOKEN_ACTIVATION_DELAY_MS = 5000;
const UNCONFIRMED_AFTER_MS = 60 * 60 * 1000;

function pageIdentity(rootKey, index) {
  if (!/^[a-f0-9]{64}$/.test(rootKey) || !Number.isInteger(index) || index < 0 || index >= MAX_PAGES) {
    throw Error('Invalid Scout page identity');
  }
  return index === 0 ? rootKey : createHash('sha256')
    .update(JSON.stringify(['places-market-page-v1', rootKey, index])).digest('hex');
}
function token(value) {
  if (value == null || value === '') return null;
  if (typeof value !== 'string' || value.length > 8192 || /[\u0000-\u001f\u007f]/.test(value)) {
    throw Error('Invalid Scout continuation token');
  }
  return value;
}
function rawResults(rows) {
  if (!Array.isArray(rows) || rows.length > 20) throw Error('Invalid Scout page result bounds');
  return rows.map(row => ({
    place_id: String(row.place_id || '').slice(0, 512),
    name: String(row.name || '').slice(0, 1024),
    formatted_address: String(row.formatted_address || '').slice(0, 2048),
    types: Array.isArray(row.types) ? row.types.filter(x => typeof x === 'string').slice(0, 30) : [],
  }));
}

// Pure selection: a handled page is not proof that the next page is exhausted.
// Legacy rows lack continuation evidence and must never be guessed or refetched.
function selectPage(rootKey, rows, legacy, now = Date.now()) {
  if (!rows.length) {
    if (legacy) return legacy.state === 'fetched'
      ? { key: rootKey, index: 0, row: legacy, legacy: true }
      : { status: 'legacy_continuation_unknown', terminal: true };
    return { key: pageIdentity(rootKey, 0), rootKey, index: 0, pageToken: null };
  }
  const sorted = [...rows].sort((a, b) => a.page_index - b.page_index);
  for (let i = 0; i < sorted.length; i++) {
    const row = sorted[i];
    if (row.root_query_key !== rootKey || row.page_index !== i || row.query_key !== pageIdentity(rootKey, i)) {
      throw Error('Corrupt Scout page chain');
    }
    if (token(row.request_token) !== (i === 0 ? null : token(sorted[i - 1].next_page_token))) throw Error('Scout page token chain mismatch');
    if (row.state === 'reserved') return {
      status: now - new Date(row.created_at).getTime() >= UNCONFIRMED_AFTER_MS
        ? 'page_fetch_outcome_unknown' : 'page_fetch_in_progress', terminal: false,
    };
    if (row.state === 'blocked') {
      if (!['continuation_token_invalid', 'page_provider_denied', 'page_provider_failure'].includes(row.blocked_reason)) throw Error('Invalid Scout page failure');
      return { status: row.blocked_reason, terminal: true };
    }
    if (row.state !== 'fetched' || !['fetched','handled'].includes(row.checkpoint_state)) throw Error('Incomplete Scout page checkpoint');
    if (row.checkpoint_state === 'fetched') {
      if (i !== sorted.length - 1) throw Error('Scout page advanced before durable handling');
      return { key: row.query_key, rootKey, index: i, row: { state: 'fetched', candidates: row.candidates } };
    }
  }
  const last = sorted.at(-1);
  const next = token(last.next_page_token);
  if (!next) return { status: 'finite_query_exhausted', terminal: true };
  if (sorted.length >= MAX_PAGES) return { status: 'provider_page_limit_reached', terminal: true };
  if (new Date(last.available_at).getTime() > now) return { status: 'page_token_not_ready', terminal: false };
  return { key: pageIdentity(rootKey, sorted.length), rootKey, index: sorted.length, pageToken: next };
}

function createBasePageCheckpointStore({ db, tenantId, fingerprint, now = () => Date.now() }) {
  if (!db?.query || !db?.connect || !tenantId || !fingerprint) throw Error('Page store requires transactional DB and scoped identity');
  const scope = [String(tenantId), String(fingerprint)];
  return {
    tenantId: scope[0],
    async assertReady() {
      await db.query(`SELECT query_key,root_query_key,page_index,state,reservation_id,request_token,
        next_page_token,raw_results,blocked_reason,available_at,created_at,fetched_at
        FROM scout_query_pages WHERE tenant_id=$1 AND search_fingerprint=$2 LIMIT 0`, scope);
    },
    async plan(rootKey) {
      const { rows } = await db.query(`SELECT p.*,c.state AS checkpoint_state,c.candidates
        FROM scout_query_pages p LEFT JOIN scout_query_checkpoints c
        ON c.tenant_id=p.tenant_id AND c.search_fingerprint=p.search_fingerprint AND c.query_key=p.query_key
        WHERE p.tenant_id=$1 AND p.search_fingerprint=$2 AND p.root_query_key=$3 ORDER BY p.page_index`, [...scope, rootKey]);
      let legacy = null;
      if (!rows.length) {
        const found = await db.query(`SELECT state,candidates FROM scout_query_checkpoints
          WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=$3`, [...scope, rootKey]);
        legacy = found.rows[0] || null;
      }
      return selectPage(rootKey, rows, legacy, now());
    },
    async reserve(plan, reservationDb = db) {
      if (plan.key !== pageIdentity(plan.rootKey, plan.index)) throw Error('Scout page reservation identity mismatch');
      const reservationId = randomUUID();
      const epochGuard = plan.coverageVersion === 2 ? `AND EXISTS (
          SELECT 1 FROM scout_query_coverage_grant_roots r
          JOIN scout_query_coverage_grants g ON g.tenant_id=r.tenant_id AND g.search_fingerprint=r.search_fingerprint
            AND g.allocation_hash=r.allocation_hash
          JOIN scout_query_checkpoints legacy ON legacy.tenant_id=r.tenant_id AND legacy.search_fingerprint=r.search_fingerprint
            AND legacy.query_key=r.original_root_key
          WHERE r.tenant_id=$1 AND r.search_fingerprint=$2 AND r.epoch_root_key=$4 AND r.original_root_key=$9
            AND r.allocation_hash=$10 AND g.grant_hash=$11 AND g.revoked_at IS NULL
            AND r.original_root_key=ANY(g.root_query_keys) AND g.not_before<=$8 AND g.expires_at>$8
            AND legacy.state='handled' AND legacy.plan_version='places-market-query-v1'
            AND NOT EXISTS(SELECT 1 FROM scout_query_pages old WHERE old.tenant_id=$1 AND old.search_fingerprint=$2 AND old.root_query_key=r.original_root_key)
            AND (SELECT count(*) FROM scout_query_coverage_grant_pages used WHERE used.tenant_id=$1 AND used.search_fingerprint=$2 AND used.grant_hash=g.grant_hash)<g.max_new_pages
        )` : plan.originalRootKey ? `AND EXISTS (
          SELECT 1 FROM scout_query_coverage_epochs e JOIN scout_query_checkpoints legacy
          ON legacy.tenant_id=e.tenant_id AND legacy.search_fingerprint=e.search_fingerprint AND legacy.query_key=e.original_root_key
          JOIN scout_query_coverage_epoch_receipts receipt ON receipt.tenant_id=e.tenant_id
          AND receipt.search_fingerprint=e.search_fingerprint AND receipt.manifest_hash=e.manifest_hash
          AND receipt.epoch=e.epoch AND e.original_root_key=ANY(receipt.root_query_keys)
          AND receipt.not_before=e.not_before AND receipt.expires_at=e.expires_at
          WHERE e.tenant_id=$1 AND e.search_fingerprint=$2 AND e.epoch_root_key=$4 AND e.original_root_key=$9
          AND e.manifest_hash=$10 AND e.epoch=1 AND e.not_before<=$8 AND e.expires_at>$8 AND legacy.state='handled' AND legacy.plan_version='places-market-query-v1'
          AND NOT EXISTS (SELECT 1 FROM scout_query_pages old WHERE old.tenant_id=$1 AND old.search_fingerprint=$2
            AND old.root_query_key=e.original_root_key))` : '';
      const { rows } = await reservationDb.query(`INSERT INTO scout_query_pages
        (tenant_id,search_fingerprint,query_key,root_query_key,page_index,state,reservation_id,request_token)
        SELECT $1,$2,$3,$4,$5,'reserved',$6,$7
        WHERE (($5=0 AND NOT EXISTS (SELECT 1 FROM scout_query_checkpoints
          WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=$4))
        OR ($5>0 AND EXISTS (SELECT 1 FROM scout_query_pages p JOIN scout_query_checkpoints c
          ON c.tenant_id=p.tenant_id AND c.search_fingerprint=p.search_fingerprint AND c.query_key=p.query_key
          WHERE p.tenant_id=$1 AND p.search_fingerprint=$2 AND p.root_query_key=$4 AND p.page_index=$5-1
          AND p.state='fetched' AND c.state='handled' AND p.next_page_token=$7 AND p.available_at<=$8)))
        ${epochGuard}
        ON CONFLICT DO NOTHING RETURNING reservation_id`,
      [...scope, plan.key, plan.rootKey, plan.index, reservationId, token(plan.pageToken), new Date(now()),
        ...(plan.coverageVersion === 2 ? [plan.originalRootKey, plan.allocationHash, plan.grantHash]
          : plan.originalRootKey ? [plan.originalRootKey, plan.manifestHash] : [])]);
      return rows[0]?.reservation_id === reservationId ? reservationId : null;
    },
    async stage(plan, candidates, metadata) {
      const next = token(metadata?.nextPageToken);
      const raw = rawResults(metadata?.rawResults);
      const client = await db.connect();
      let uncertain = false;
      try {
        await client.query('BEGIN');
        const locked = await client.query(`SELECT state,reservation_id FROM scout_query_pages
          WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=$3 FOR UPDATE`, [...scope, plan.key]);
        if (locked.rows[0]?.state !== 'reserved' || locked.rows[0]?.reservation_id !== plan.reservationId) throw Error('Scout page reservation lost');
        const staged = await client.query(`INSERT INTO scout_query_checkpoints
          (tenant_id,search_fingerprint,query_key,plan_version,state,candidates)
          VALUES($1,$2,$3,$4,'fetched',$5::jsonb) ON CONFLICT DO NOTHING RETURNING query_key`,
        [...scope, plan.key, PLAN_VERSION, JSON.stringify(candidates)]);
        if (staged.rows[0]?.query_key !== plan.key) throw Error('Scout page checkpoint conflict');
        const confirmed = await client.query(`UPDATE scout_query_pages SET state='fetched',next_page_token=$5,
          raw_results=$6::jsonb,available_at=$7,fetched_at=now()
          WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=$3 AND reservation_id=$4 AND state='reserved'
          RETURNING query_key`, [...scope, plan.key, plan.reservationId, next, JSON.stringify(raw),
          new Date(now() + TOKEN_ACTIVATION_DELAY_MS)]);
        if (confirmed.rows[0]?.query_key !== plan.key) throw Error('Scout page staging unconfirmed');
        uncertain = true;
        await client.query('COMMIT');
        uncertain = false;
        return { state: 'fetched', candidates };
      } catch (error) {
        try { await client.query('ROLLBACK'); } catch (_) { uncertain = true; }
        throw error;
      } finally { client.release(uncertain); }
    },
    async fail(plan, failure = {}, failureDb = db) {
      // A proven pre-network budget denial can release its unused reservation.
      // All other unconfirmed outcomes retain a durable stop; no auto page reset.
      if (failure.noNetwork === true) {
        await failureDb.query(`DELETE FROM scout_query_pages WHERE tenant_id=$1 AND search_fingerprint=$2
          AND query_key=$3 AND reservation_id=$4 AND state='reserved'`, [...scope, plan.key, plan.reservationId]);
        return;
      }
      const reason = failure.googleStatus === 'INVALID_REQUEST' ? 'continuation_token_invalid'
        : failure.googleStatus === 'REQUEST_DENIED' ? 'page_provider_denied' : 'page_provider_failure';
      const { rows } = await failureDb.query(`UPDATE scout_query_pages SET state='blocked',blocked_reason=$5,raw_results=$6::jsonb
        WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=$3 AND reservation_id=$4 AND state='reserved'
        RETURNING query_key`, [...scope, plan.key, plan.reservationId, reason, JSON.stringify(rawResults(failure.rawResults || []))]);
      if (rows[0]?.query_key !== plan.key) throw Error('Scout page failure unconfirmed');
    },
  };
}
function createPageCheckpointStore(options) {
  const base = createBasePageCheckpointStore(options);
  const config = require('./scoutCoverageEpoch').validateCoverageEpochConfig(options.coverageEpoch, options);
  const legacy = require('./scoutCoverageEpochStore').wrapCoverageEpochStore(base, { ...options, coverageEpoch: config?.version === 2 ? null : config });
  return require('./scoutCoverageGrantStore').wrapCoverageGrantStore(base, legacy, { ...options, coverageEpoch: config });
}
module.exports = { MAX_PAGES, TOKEN_ACTIVATION_DELAY_MS, pageIdentity, selectPage, createPageCheckpointStore };
