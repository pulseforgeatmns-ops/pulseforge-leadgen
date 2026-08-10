'use strict';

/**
 * In-memory Scout work-request / handoff store (SPEC-077).
 * Review-only — never writes CRM prospects or sends outreach.
 */

function createMemoryScoutWorkRequestStore() {
  /** @type {Map<string, object>} */
  const byWorkRequestId = new Map();
  /** @type {Map<string, string>} */
  const handoffToWorkRequest = new Map();

  function save(record) {
    if (!record || !record.workRequestId) {
      throw new Error('scout_work_request_store_requires_workRequestId');
    }
    const copy = structuredClone
      ? structuredClone(record)
      : JSON.parse(JSON.stringify(record));
    byWorkRequestId.set(copy.workRequestId, copy);
    if (copy.handoffId) {
      handoffToWorkRequest.set(copy.handoffId, copy.workRequestId);
    }
    return copy;
  }

  function getByWorkRequestId(workRequestId) {
    if (!workRequestId) return null;
    const row = byWorkRequestId.get(String(workRequestId));
    return row
      ? structuredClone
        ? structuredClone(row)
        : JSON.parse(JSON.stringify(row))
      : null;
  }

  function getByHandoffId(handoffId) {
    if (!handoffId) return null;
    const wrId = handoffToWorkRequest.get(String(handoffId));
    return wrId ? getByWorkRequestId(wrId) : null;
  }

  function get(opts = {}) {
    if (opts.workRequestId) return getByWorkRequestId(opts.workRequestId);
    if (opts.handoffId) return getByHandoffId(opts.handoffId);
    return null;
  }

  function update(workRequestId, patch = {}) {
    const existing = byWorkRequestId.get(String(workRequestId));
    if (!existing) return null;
    const next = {
      ...existing,
      ...patch,
      workRequestId: existing.workRequestId,
      handoffId: patch.handoffId || existing.handoffId,
      updatedAt: patch.updatedAt || new Date().toISOString(),
    };
    return save(next);
  }

  function clear() {
    byWorkRequestId.clear();
    handoffToWorkRequest.clear();
  }

  function size() {
    return byWorkRequestId.size;
  }

  return {
    save,
    get,
    getByWorkRequestId,
    getByHandoffId,
    update,
    clear,
    size,
  };
}

/** Process-local default store for Scout work requests. */
const defaultScoutWorkRequestStore = createMemoryScoutWorkRequestStore();

module.exports = {
  createMemoryScoutWorkRequestStore,
  defaultScoutWorkRequestStore,
};
