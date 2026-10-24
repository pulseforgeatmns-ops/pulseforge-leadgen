'use strict';

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const path = require('node:path');
const { describe, it } = require('node:test');

const REPO_ROOT = path.join(__dirname, '..');
const SIGNAL_ROUTE = path.join(REPO_ROOT, 'routes/signalV1.js');

describe('Signal V1 production startup chain', () => {
  it('routes/signalV1.js passes node --check (no illegal await in sync handlers)', () => {
    execFileSync(process.execPath, ['--check', SIGNAL_ROUTE], {
      cwd: REPO_ROOT,
      stdio: 'pipe',
    });
  });

  it('loads packages/signal-v1 before SignalService/replayEngine (server.js line ~56)', () => {
    assert.doesNotThrow(() => {
      require('../packages/signal-v1');
      require('../packages/signal-v1/SignalService');
    });
  });

  it('loads routes/signalV1.js (server.js app.use line ~285)', () => {
    const routePath = require.resolve('../routes/signalV1');
    delete require.cache[routePath];
    let router;
    assert.doesNotThrow(() => {
      router = require('../routes/signalV1');
    });
    assert.equal(typeof router, 'function');
    assert.ok(Array.isArray(router.stack));
    assert.ok(router.stack.length > 0);
  });
});
