'use strict';
const assert = require('node:assert/strict');

// Strict synthetic storage. Never opens sockets or uses an environment DB URL.
function createFakePool({ capacity = 1, onQuery = null } = {}) {
  const events = new Map();
  const locks = new Map();
  const calls = [];
  const releases = [];
  let active = 0;
  let nextClient = 0;
  const faults = {};
  function scopeRows(params, pending) {
    const [tenant, type, fingerprint, version] = params;
    return [...events.values(), ...pending.values()].filter(row => row.tenant_id === tenant && row.event_type === type
      && row.payload.searchFingerprint === fingerprint && row.payload.cursorVersion === version);
  }
  async function execute(sql, params = [], client = null) {
    calls.push({ sql, params, client: client?.id || null });
    if (sql === 'BEGIN') { client.transaction = true; return { rows: [] }; }
    if (sql === 'COMMIT') {
      for (const [id, row] of client.pending) events.set(id, structuredClone(row));
      client.pending.clear(); client.transaction = false;
      for (const [key, owner] of locks) if (owner === client.id) locks.delete(key);
      if (faults.uncertainCommit) throw Error('synthetic unknown commit outcome');
      return { rows: [] };
    }
    if (sql === 'ROLLBACK') {
      if (faults.rollbackFailure) throw Error('synthetic rollback failure');
      client.pending.clear(); client.transaction = false;
      for (const [key, owner] of locks) if (owner === client.id) locks.delete(key);
      return { rows: [] };
    }
    if (/pg_try_advisory_xact_lock/.test(sql)) {
      assert.ok(client?.transaction);
      const key = params.join(':');
      if (faults.busy || locks.has(key)) return { rows: [{ locked: false }] };
      locks.set(key, client.id);
      return { rows: [{ locked: true }] };
    }
    if (/SELECT id,payload FROM acquisition_outbound_events/.test(sql)) {
      if (faults.readFailure) throw Error('synthetic cursor read failure');
      if (/ORDER BY/.test(sql)) {
        return { rows: scopeRows(params, client?.pending || new Map()).sort((a, b) => b.payload.sequence - a.payload.sequence)
          .slice(0, 1).map(row => ({ id: row.id, payload: structuredClone(row.payload) })) };
      }
      if (faults.unconfirmed) return { rows: [] };
      const [id, tenant, type, fingerprint, version] = params;
      const row = client?.pending.get(id) || events.get(id);
      return { rows: row && row.tenant_id === tenant && row.event_type === type
        && row.payload.searchFingerprint === fingerprint && row.payload.cursorVersion === version
        ? [{ id: row.id, payload: structuredClone(row.payload) }] : [] };
    }
    if (/INSERT INTO acquisition_outbound_events/.test(sql)) {
      assert.ok(client?.transaction);
      if (faults.insertFailure) throw Error('synthetic cursor insert failure');
      const [id, tenant, type, json] = params;
      if (!events.has(id) && !client.pending.has(id)) {
        const payload = JSON.parse(json);
        if (faults.conflictingReceipt) payload.afterKey = 'f'.repeat(64);
        client.pending.set(id, { id, tenant_id: tenant, event_type: type, payload });
      }
      return { rows: [] };
    }
    if (onQuery) return onQuery(sql, params);
    throw Error(`Unexpected synthetic SQL: ${sql}`);
  }
  return { events, calls, faults, releases, get active() { return active; },
    async query(sql, params) {
      assert.ok(active < capacity, 'pool query would deadlock behind a pinned cursor client');
      return execute(sql, params);
    },
    async connect() {
      assert.ok(active < capacity, 'synthetic pool is exhausted'); active++;
      const client = { id: ++nextClient, transaction: false, pending: new Map(),
        query: (sql, params) => execute(sql, params, client),
        release(destroy) {
          assert.equal(client.released, undefined); client.released = true;
          releases.push(Boolean(destroy)); active--;
          if (destroy) {
            for (const [key, owner] of locks) if (owner === client.id) locks.delete(key);
          } else assert.equal(client.transaction, false, 'transaction must finish before releasing client');
        },
      };
      return client;
    },
  };
}
module.exports = { createFakePool };
