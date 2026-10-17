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
function createCheckpointSession({ store }) {
  if (!store) throw new Error('Checkpoint session requires a store');
  const pending = new Set();
  const observed = new Set();
  const handled = new Set();
  const unresolved = new Set();
  const skipped = new Set();
  const fetched = new Set();
  const replayed = new Set();
  return {
    assertTenant(tenantId) {
      if (store.tenantId && String(tenantId || '') !== store.tenantId) throw new Error('Checkpoint tenant mismatch');
    },
    async lookup(query, requireWebsite) {
      const key = queryIdentity(query, requireWebsite);
      observed.add(key); unresolved.add(key);
      const row = await store.load(key);
      if (row?.state === 'handled') { unresolved.delete(key); handled.add(key); skipped.add(key); return { key, handled: true, candidates: [] }; }
      if (row?.state === 'fetched') {
        if (!Array.isArray(row.candidates)) throw new Error('Invalid staged checkpoint payload');
        unresolved.delete(key); pending.add(key); replayed.add(key);
        return { key, handled: false, candidates: row.candidates };
      }
      unresolved.add(key);
      return { key, handled: false, candidates: null };
    },
    async stage(key, candidates) {
      const row = await store.stage(key, candidates);
      if (!row) throw new Error('Checkpoint staging was not confirmed');
      if (row.state === 'handled') { handled.add(key); unresolved.delete(key); return []; }
      if (row.state !== 'fetched' || !Array.isArray(row.candidates)) throw new Error('Invalid staged checkpoint payload');
      unresolved.delete(key); pending.add(key); fetched.add(key);
      return row.candidates;
    },
    async acknowledge() {
      const keys = [...pending];
      await store.acknowledge(keys);
      keys.forEach(key => { pending.delete(key); handled.add(key); });
    },
    snapshot() {
      return { observedQueries: observed.size, handledQueries: handled.size, skippedHandled: skipped.size,
        fetchedQueries: fetched.size, replayedQueries: replayed.size, pendingAcknowledgements: pending.size,
        unresolvedQueries: unresolved.size,
        status: !observed.size ? 'not_observed' : handled.size !== observed.size || pending.size || unresolved.size
          ? 'pending_query_work' : 'observed_query_set_handled' };
    },
  };
}

module.exports = { PLAN_VERSION, queryIdentity, createCheckpointStore, createCheckpointSession };
