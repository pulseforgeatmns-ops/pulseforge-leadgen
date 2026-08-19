'use strict';

/**
 * SPEC-113 — Acquisition Intelligence Compiler service facade.
 */

const {
  createCompiler,
  createMemoryAicStore,
  loadFixtureDocuments,
} = require('../packages/aic');
const { getAimStore } = require('./acquisitionIntelligenceModel');

const {
  persistAicWorkspace,
  loadAicWorkspace,
  listAicWorkspaces,
  persistPublishedAim,
} = require('./aicPersistence');

let compiler = null;

function getCompiler(opts = {}) {
  if (opts.compiler) return opts.compiler;
  if (!compiler) {
    compiler = createCompiler({
      store: createMemoryAicStore(),
      aimStore: getAimStore(opts),
    });
  }
  return compiler;
}

async function rememberWorkspace(workspace, opts = {}) {
  if (!workspace) return workspace;
  getCompiler(opts).store.putWorkspace(workspace);
  if (opts.persist !== false) {
    try {
      await persistAicWorkspace(workspace, opts.pool);
    } catch (err) {
      console.error('[aic] persist workspace:', err.message);
    }
  }
  return workspace;
}

async function hydrateWorkspace(workspaceId, opts = {}) {
  const existing = getCompiler(opts).getWorkspace(workspaceId);
  if (existing) return existing;
  const loaded = await loadAicWorkspace(workspaceId, opts.pool);
  if (loaded) {
    getCompiler(opts).store.putWorkspace(loaded);
    return loaded;
  }
  return null;
}

async function hydrateClientWorkspaces(clientId, opts = {}) {
  const rows = await listAicWorkspaces({ clientId, clientKey: opts.clientKey }, opts.pool);
  const compilerInstance = getCompiler(opts);
  for (const ws of rows) compilerInstance.store.putWorkspace(ws);
  return rows;
}

function createWorkspace(partial, opts = {}) {
  return getCompiler(opts).createWorkspace(partial);
}

function addDocuments(workspaceId, documents, opts = {}) {
  return getCompiler(opts).addDocuments(workspaceId, documents);
}

function compileWorkspace(workspaceId, opts = {}) {
  return getCompiler(opts).compile(workspaceId);
}

function reviewConcept(workspaceId, conceptId, input, opts = {}) {
  return getCompiler(opts).review(workspaceId, conceptId, input, opts);
}

function approveWorkspace(workspaceId, opts = {}) {
  return getCompiler(opts).approve(workspaceId, opts);
}

function publishWorkspace(workspaceId, opts = {}) {
  return getCompiler(opts).publish(workspaceId, opts);
}

async function publishAndPersist(workspaceId, opts = {}) {
  const result = publishWorkspace(workspaceId, opts);
  const clientId = opts.clientId ?? result.workspace?.clientId ?? result.workspace?.client_id;
  if (result.workspace) {
    result.workspace.clientId = clientId;
    result.workspace.client_id = clientId;
    await rememberWorkspace(result.workspace, opts);
  }
  if (result.aim) {
    result.aim.clientId = clientId;
    result.aim.client_id = clientId;
    try {
      await persistPublishedAim(result.aim, { clientId }, opts.pool);
    } catch (err) {
      console.error('[aic] persist aim:', err.message);
    }
    const store = getAimStore(opts);
    if (store && typeof store.putAim === 'function') {
      store.putAim(result.aim);
    }
  }
  return result;
}

function getWorkspace(workspaceId, opts = {}) {
  return getCompiler(opts).getWorkspace(workspaceId);
}

function listWorkspaces(clientKey, opts = {}) {
  return getCompiler(opts).listWorkspaces(clientKey);
}

module.exports = {
  getCompiler,
  createWorkspace,
  addDocuments,
  compileWorkspace,
  reviewConcept,
  approveWorkspace,
  publishWorkspace,
  publishAndPersist,
  getWorkspace,
  listWorkspaces,
  loadFixtureDocuments,
  rememberWorkspace,
  hydrateWorkspace,
  hydrateClientWorkspaces,
};
