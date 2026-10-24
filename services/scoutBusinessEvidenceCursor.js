'use strict';

const { createHash } = require('node:crypto');
const { normalizeDomain } = require('../utils/canonicalEmailEligibility');

const CURSOR_VERSION = 'scout-business-evidence-v1';
const EVENT_TYPE = 'scout_business_evidence_cursor';
// Deliberately distinct from governed outbound's 261020 session lock.
const LOCK_NAMESPACE = 261021;
const INVESTIGATION_LIMIT = 20;
const KEY_RE = /^[a-f0-9]{64}$/;
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');

function candidateKey(company) {
  const domain = normalizeDomain(company?.domain || company?.website);
  return domain ? digest([CURSOR_VERSION, domain]) : null;
}

function assertCursorTenantBindings({ tenantId, store, scoutContext }) {
  const bindings = [tenantId, store?.tenantId, store?.clientId, store?.ctx?.tenantId, store?.ctx?.clientId,
    scoutContext?.tenantId, scoutContext?.clientId, scoutContext?.authorizedTenantId,
    scoutContext?.governedContext?.tenantId, scoutContext?.governedContext?.clientId,
    scoutContext?.governedContext?.transport?.tenantId, scoutContext?.governedContext?.transport?.clientId,
    scoutContext?.program?.tenant_id, scoutContext?.program?.tenantId, scoutContext?.program?.policy?.tenantId];
  if (bindings.filter(value => value != null).some(value => !['string', 'number'].includes(typeof value)
    || String(value).trim() !== String(tenantId))) throw Error('Scout evidence cursor tenant binding mismatch');
}

function createSelection({ tenantId, afterKey = null }) {
  let active = true;
  let selected = null;
  const consumed = new Set();
  function assertActive() {
    if (!active) throw Error('Scout evidence cursor pass is closed');
  }
  return {
    assertTenant(value) {
      assertActive();
      if (String(value) !== tenantId) throw Error('Scout evidence cursor tenant mismatch');
    },
    select(companies) {
      assertActive();
      if (selected) throw Error('Scout evidence cursor selection already made');
      const keys = [...new Set(companies.map(candidateKey).filter(Boolean))].sort();
      const next = afterKey ? keys.findIndex(key => key > afterKey) : 0;
      const start = next < 0 ? 0 : next;
      selected = [...keys.slice(start), ...keys.slice(0, start)].slice(0, INVESTIGATION_LIMIT);
    },
    take(company) {
      assertActive();
      if (!selected) throw Error('Scout evidence cursor selection is missing');
      const key = candidateKey(company);
      if (!selected.includes(key) || consumed.has(key)) return false;
      consumed.add(key);
      return true;
    },
    finish() {
      assertActive();
      active = false;
      return { afterKey: selected?.at(-1) || null, selected: selected?.length || 0,
        investigated: consumed.size };
    },
    close() { active = false; },
  };
}

function createBusinessEvidenceCursorStore({ pool, tenantId, fingerprint }) {
  const validTenant = (typeof tenantId === 'string' || (typeof tenantId === 'number' && Number.isSafeInteger(tenantId)))
    && /^[1-9][0-9]{0,9}$/.test(String(tenantId));
  const validFingerprint = typeof fingerprint === 'string' && fingerprint.length > 0 && fingerprint.length <= 256
    && fingerprint.trim() === fingerprint;
  if (!pool || typeof pool.connect !== 'function' || typeof pool.query !== 'function' || !validTenant || !validFingerprint) {
    throw Error('Scout evidence cursor requires pool, tenant and mission fingerprint');
  }
  const tenant = String(tenantId);
  const mission = String(fingerprint);
  const scope = [tenant, mission, CURSOR_VERSION];
  const lockKey = Buffer.from(digest(scope).slice(0, 8), 'hex').readInt32BE();
  const eventId = sequence => digest([EVENT_TYPE, ...scope, sequence]);
  function validateReceipt(row) {
    const p = row?.payload;
    if (!p || p.searchFingerprint !== mission || p.cursorVersion !== CURSOR_VERSION
      || !Number.isSafeInteger(p.sequence) || p.sequence < 1
      || p.previousSequence !== p.sequence - 1 || !KEY_RE.test(p.afterKey)
      || !Number.isInteger(p.selected) || p.selected < 1 || p.selected > INVESTIGATION_LIMIT
      || !Number.isInteger(p.investigated) || p.investigated < 0 || p.investigated > p.selected
      || row.id !== eventId(p.sequence)) {
      throw Error('Invalid Scout evidence cursor receipt');
    }
    return p;
  }
  async function load(db) {
    const latest = await db.query(`SELECT id,payload FROM acquisition_outbound_events
      WHERE tenant_id=$1 AND event_type=$2 AND payload->>'searchFingerprint'=$3
        AND payload->>'cursorVersion'=$4
      ORDER BY (payload->>'sequence')::bigint DESC LIMIT 1`,
    [tenant, EVENT_TYPE, mission, CURSOR_VERSION]);
    return latest.rows.length ? validateReceipt(latest.rows[0]) : null;
  }
  return {
    async withPass(fn) {
      // Snapshot without pinning a connection during website/recovery work.
      // Existing helpers may use the global pool; a long-lived cursor client
      // would deadlock small or saturated pools. A short CAS transaction below
      // rejects a stale snapshot, including delayed writers and timestamp ties.
      const prior = await load(pool);
      const previousSequence = prior?.sequence || 0;
      if (previousSequence >= Number.MAX_SAFE_INTEGER) throw Error('Scout evidence cursor sequence limit reached');
      const selection = createSelection({ tenantId: tenant, afterKey: prior?.afterKey });
      let result;
      let progress;
      try {
        result = await fn({ assertTenant: selection.assertTenant, select: selection.select, take: selection.take });
        progress = selection.finish();
      } finally { selection.close(); }
      if (!progress.selected) return { result, progression: { selected: 0, investigated: 0,
        advanced: false, sequence: previousSequence } };
      const db = await pool.connect();
      let transaction = false;
      let broken = false;
      try {
        await db.query('BEGIN');
        transaction = true;
        const lock = await db.query('SELECT pg_try_advisory_xact_lock($1,$2) AS locked', [LOCK_NAMESPACE, lockKey]);
        if (lock.rows[0]?.locked !== true) throw Object.assign(Error('Scout evidence cursor commit is busy'), { cursorCode: 'busy' });
        const current = await load(db);
        if ((current?.sequence || 0) !== previousSequence) throw Object.assign(Error('Scout evidence cursor snapshot is stale'), { cursorCode: 'stale' });
        const sequence = previousSequence + 1;
        const payload = { searchFingerprint: mission, cursorVersion: CURSOR_VERSION,
          sequence, previousSequence, ...progress };
        const id = eventId(sequence);
        await db.query(`INSERT INTO acquisition_outbound_events(id,tenant_id,event_type,payload)
          VALUES($1,$2,$3,$4::jsonb) ON CONFLICT DO NOTHING`,
        [id, tenant, EVENT_TYPE, JSON.stringify(payload)]);
        const confirmed = await db.query(`SELECT id,payload FROM acquisition_outbound_events
          WHERE id=$1 AND tenant_id=$2 AND event_type=$3
            AND payload->>'searchFingerprint'=$4 AND payload->>'cursorVersion'=$5`,
        [id, tenant, EVENT_TYPE, mission, CURSOR_VERSION]);
        const receipt = confirmed.rows.length ? validateReceipt(confirmed.rows[0]) : null;
        if (!receipt || Object.keys(payload).some(key => receipt[key] !== payload[key])) {
          throw Error('Scout evidence cursor receipt was not confirmed');
        }
        await db.query('COMMIT');
        transaction = false;
        return { result, progression: { selected: progress.selected, investigated: progress.investigated,
          advanced: true, sequence } };
      } catch (error) {
        error.evidenceProgression = { status: ['busy', 'stale'].includes(error.cursorCode) ? error.cursorCode : 'failed',
          selected: progress.selected, investigated: progress.investigated, advanced: false, sequence: null };
        if (transaction) {
          try { await db.query('ROLLBACK'); transaction = false; }
          catch { broken = true; }
        } else broken = true;
        throw error;
      } finally {
        db.release(broken);
      }
    },
  };
}

module.exports = { CURSOR_VERSION, EVENT_TYPE, LOCK_NAMESPACE, INVESTIGATION_LIMIT,
  candidateKey, assertCursorTenantBindings, createBusinessEvidenceCursorStore };
