'use strict';

/**
 * SPEC-CI-LEAN-001 — single global production startup boundary.
 * Complements domain-specific startup tests (Signal, governed outbound).
 */

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const path = require('node:path');
const { describe, it } = require('node:test');

const REPO_ROOT = path.join(__dirname, '..');
const SERVER = path.join(REPO_ROOT, 'server.js');

describe('Global production startup smoke', () => {
  it('server.js passes node --check', () => {
    execFileSync(process.execPath, ['--check', SERVER], {
      cwd: REPO_ROOT,
      stdio: 'pipe',
    });
  });

  it('loads the shared database pool module (no live connection required at import)', () => {
    assert.doesNotThrow(() => {
      require('../db');
    });
  });
});
