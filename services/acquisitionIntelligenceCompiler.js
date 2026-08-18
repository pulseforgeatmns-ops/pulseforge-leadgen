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
  getWorkspace,
  listWorkspaces,
  loadFixtureDocuments,
};
