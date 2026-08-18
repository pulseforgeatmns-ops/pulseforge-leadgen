'use strict';

/**
 * SPEC-113 — in-memory Acquisition Intelligence Compiler store.
 */

const { clone, asText, nowIso } = require('./types');
const { emptyWorkspace } = require('./Ingestion');

function createMemoryAicStore(opts = {}) {
  const workspaces = new Map();

  function putWorkspace(workspace) {
    const copy = clone(workspace);
    copy.updatedAt = nowIso();
    workspaces.set(copy.id, copy);
    return clone(copy);
  }

  function getWorkspace(id) {
    const found = workspaces.get(asText(id));
    return found ? clone(found) : null;
  }

  function listWorkspaces(clientKey) {
    const rows = [...workspaces.values()].map(clone);
    if (!clientKey) return rows;
    return rows.filter((w) => w.clientKey === asText(clientKey));
  }

  function createWorkspace(partial = {}) {
    return putWorkspace(emptyWorkspace(partial));
  }

  function requireWorkspace(id) {
    const workspace = getWorkspace(id);
    if (!workspace) {
      const err = new Error(`AIC workspace not found: ${id}`);
      err.code = 'aic_not_found';
      throw err;
    }
    return workspace;
  }

  for (const extra of opts.seeds || []) putWorkspace(extra);

  return {
    putWorkspace,
    getWorkspace,
    listWorkspaces,
    createWorkspace,
    requireWorkspace,
  };
}

module.exports = {
  createMemoryAicStore,
};
