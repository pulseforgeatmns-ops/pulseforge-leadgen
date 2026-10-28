'use strict';

const { createHash } = require('node:crypto');
const PLAN_VERSION = 'places-market-query-v1';

// Identity preserves every geography token. Reporting normalization is unsuitable.
function queryIdentity(query, requireWebsite) {
  const text = String(query || '').toLowerCase().replace(/\s+/g, ' ').trim();
  if (!text) throw new Error('Checkpoint query is required');
  return createHash('sha256').update(JSON.stringify([
    PLAN_VERSION, 'google_places', text, Boolean(requireWebsite),
  ])).digest('hex');
}

function createCheckpointStore({ db, tenantId, fingerprint }) {
  if (!db || typeof db.query !== 'function' || !tenantId || !fingerprint) {
    throw new Error('Checkpoint store requires DB, tenant and mission fingerprint');
  }
  const scope = [String(tenantId), String(fingerprint)];
  return {
    tenantId: String(tenantId),
    fingerprint: String(fingerprint),
    async load(key) {
      const { rows } = await db.query(`SELECT query_key, state, candidates
        FROM scout_query_checkpoints WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=$3`, [...scope, key]);
      return rows[0] || null;
    },
    async stage(key, candidates) {
      // Immutable staged payload makes concurrent fetches converge on one replay unit.
      await db.query(`INSERT INTO scout_query_checkpoints
        (tenant_id, search_fingerprint, query_key, plan_version, state, candidates)
        VALUES ($1,$2,$3,$4,'fetched',$5::jsonb) ON CONFLICT DO NOTHING`,
      [...scope, key, PLAN_VERSION, JSON.stringify(candidates)]);
      return this.load(key);
    },
    async acknowledge(keys) {
      if (!keys.length) return;
      const { rows } = await db.query(`UPDATE scout_query_checkpoints SET state='handled', handled_at=COALESCE(handled_at,now()), candidates='[]'::jsonb
        WHERE tenant_id=$1 AND search_fingerprint=$2 AND query_key=ANY($3::text[])
        RETURNING query_key`, [...scope, keys]);
      if (new Set(rows.map(row => row.query_key)).size !== new Set(keys).size) {
        throw new Error('Checkpoint acknowledgement was not confirmed');
      }
    },
  };
}

// One session per acquisition run. Not telemetry: fetched payloads are durable
// replay units; only the ingestion acknowledgement advances them to handled.
function createCheckpointSession({ store, ticketStore = null, pageStore = null, tenantMemory = null }) {
  if (!store) throw new Error('Checkpoint session requires a store');
  if (tenantMemory && (!pageStore || tenantMemory.tenantId!==store.tenantId || tenantMemory.fingerprint!==store.fingerprint))
    throw new Error('Scout tenant memory requires the exact checkpoint scope and page store');
  const pending = new Set();
  const observed = new Set();
  const handled = new Set();
  const unresolved = new Set();
  const skipped = new Set();
  const fetched = new Set();
  const replayed = new Set();
  const rootPlans = new Map();
  const observedCheckpointRoots = new Set();
  const selectedPages = new Map();
  const pageStatuses = new Map();
  let newPagesReserved = 0;
  async function transfer(key, candidates) {
    if (!ticketStore) { pending.add(key); return candidates; }
    const receipt = await ticketStore.transferQuery(key);
    if (receipt?.transferred !== true || receipt.queryKey !== key || !Array.isArray(receipt.candidates)) {
      throw Error('Scout candidate responsibility transfer unconfirmed');
    }
    pending.delete(key); handled.add(key);
    return receipt.candidates;
  }
  return {
    assertTenant(tenantId) {
      for (const scopedStore of [store, pageStore, ticketStore, tenantMemory].filter(Boolean)) {
        if (scopedStore.tenantId && String(tenantId || '') !== scopedStore.tenantId) throw new Error('Checkpoint tenant mismatch');
      }
    },
    async lookup(query, requireWebsite) {
      const rootKey = queryIdentity(query, requireWebsite);
      let key = rootKey;
      let plan = null;
      const sharedStatus = tenantMemory ? await tenantMemory.checkQuery(rootKey,query) : null;
      if (sharedStatus) {
        observed.add(rootKey); pageStatuses.set(rootKey,sharedStatus);
        if (sharedStatus==='tenant_query_previously_searched') { handled.add(rootKey); skipped.add(rootKey); }
        else unresolved.add(rootKey);
        return { key:rootKey,handled:true,candidates:[],continuationStatus:sharedStatus };
      }
      if (pageStore) {
        if (!rootPlans.has(rootKey)) rootPlans.set(rootKey, await pageStore.plan(rootKey));
        plan = rootPlans.get(rootKey);
        if (tenantMemory && !plan.status && !plan.row && plan.index>0) {
          const noveltyStatus=await tenantMemory.continuationStatus(plan.rootKey);
          if (noveltyStatus) {
            observed.add(rootKey); unresolved.add(rootKey); pageStatuses.set(rootKey,noveltyStatus);
            return {key:rootKey,handled:true,candidates:[],continuationStatus:noveltyStatus};
          }
        }
        if (plan.status) {
          if (plan.status === 'legacy_continuation_unknown') observedCheckpointRoots.add(rootKey);
          observed.add(rootKey);
          pageStatuses.set(rootKey, plan.status);
          if (plan.status === 'finite_query_exhausted' || plan.status === 'provider_page_limit_reached') handled.add(rootKey);
          else unresolved.add(rootKey);
          return { key: rootKey, handled: true, candidates: [], continuationStatus: plan.status };
        }
        key = plan.key;
      }
      observed.add(key); unresolved.add(key);
      const row = await store.load(key);
      if (key === rootKey && ['fetched','handled'].includes(row?.state)) observedCheckpointRoots.add(rootKey);
      if (row?.state === 'handled') { unresolved.delete(key); handled.add(key); skipped.add(key); return { key, handled: true, candidates: [] }; }
      if (row?.state === 'fetched') {
        if (!Array.isArray(row.candidates)) throw new Error('Invalid staged checkpoint payload');
        const candidates = await transfer(key, row.candidates);
        unresolved.delete(key); replayed.add(key);
        return { key, handled: false, candidates };
      }
      if (pageStore) {
        // One page for the entire acquisition cycle, shared by every workload.
        // A denied/failed attempt consumes this local allocation as well.
        if (newPagesReserved >= 1) {
          pageStatuses.set(rootKey, 'cycle_page_budget_reached');
          return { key, handled: true, candidates: [], continuationStatus: 'cycle_page_budget_reached' };
        }
        // The tenant-wide claim is committed before the existing page claim.
        // Concurrent missions cannot each purchase the same first-page search.
        newPagesReserved += 1;
        const racedStatus = tenantMemory ? await tenantMemory.reserveQuery(rootKey,query) : null;
        if (racedStatus) {
          newPagesReserved -= 1;
          pageStatuses.set(rootKey,racedStatus);
          if (racedStatus==='tenant_query_previously_searched') { handled.add(key); unresolved.delete(key); skipped.add(key); }
          return {key,handled:true,candidates:[],continuationStatus:racedStatus};
        }
        const reservationId = await pageStore.reserve(plan);
        if (!reservationId) {
          newPagesReserved -= 1;
          pageStatuses.set(rootKey, 'page_fetch_in_progress');
          return { key, handled: true, candidates: [], continuationStatus: 'page_fetch_in_progress' };
        }
        selectedPages.set(key, { ...plan, reservationId });
      }
      return { key, handled: false, candidates: null,
        ...(pageStore ? { singlePage: true, pageToken: plan.pageToken, pageIndex: plan.index } : {}) };
    },
    async stage(key, candidates, metadata = null) {
      const plan = selectedPages.get(key);
      const row = plan ? await pageStore.stage(plan, candidates, metadata) : await store.stage(key, candidates);
      if (!row) throw new Error('Checkpoint staging was not confirmed');
      if (row.state === 'handled') { handled.add(key); unresolved.delete(key); return []; }
      if (row.state !== 'fetched' || !Array.isArray(row.candidates)) throw new Error('Invalid staged checkpoint payload');
      const transferred = await transfer(key, row.candidates);
      unresolved.delete(key); fetched.add(key);
      return transferred;
    },
    async fail(key, failure) {
      const plan = selectedPages.get(key);
      if (plan) await pageStore.fail(plan, failure);
    },
    async acknowledge() {
      const keys = [...pending];
      await store.acknowledge(keys);
      keys.forEach(key => { pending.delete(key); handled.add(key); });
    },
    snapshot() {
      const coverageApprovalManifest = require('./scoutCoverageEpoch').projectCoverageApprovalManifest({
        tenantId: store.tenantId, fingerprint: store.fingerprint, rootQueryKeys: [...observedCheckpointRoots],
      });
      return { observedQueries: observed.size, handledQueries: handled.size, skippedHandled: skipped.size,
        ...(tenantMemory ? { tenantMemory:tenantMemory.snapshot() } : {}),
        ...(coverageApprovalManifest ? { coverageApprovalManifest } : {}),
        ...(pageStore ? { continuation: { enabled: true, maxNewPages: 1, newPagesReserved, statuses: [...pageStatuses.values()].reduce((counts, status) => {
          counts[status] = (counts[status] || 0) + 1; return counts;
        }, {}) } } : {}),
        fetchedQueries: fetched.size, replayedQueries: replayed.size, pendingAcknowledgements: pending.size,
        unresolvedQueries: unresolved.size,
        status: !observed.size ? 'not_observed' : handled.size !== observed.size || pending.size || unresolved.size
          ? 'pending_query_work' : 'observed_query_set_handled' };
    },
  };
}

module.exports = { PLAN_VERSION, queryIdentity, createCheckpointStore, createCheckpointSession };
