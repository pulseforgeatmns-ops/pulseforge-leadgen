'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const fs = require('node:fs');

const root = path.join(__dirname, '..');
const cli = path.join(root, 'scripts', 'contentOutcome.js');

describe('contentOutcome CLI (static)', () => {
  it('prints help and lists commands', () => {
    const result = spawnSync(process.execPath, [cli, '--help'], {
      cwd: root,
      encoding: 'utf8',
    });
    assert.equal(result.status, 0);
    assert.match(result.stdout, /Content Outcome Intelligence/);
    assert.match(result.stdout, /publish/);
    assert.match(result.stdout, /performance/);
    assert.match(result.stdout, /add-outcome/);
    assert.match(result.stdout, /add-signal/);
    assert.match(result.stdout, /show/);
    assert.match(result.stdout, /list/);
    assert.match(result.stdout, /compare/);
  });

  it('uses the shared service module (no duplicate business logic)', () => {
    const source = fs.readFileSync(cli, 'utf8');
    assert.match(source, /require\('\.\.\/services\/contentOutcomeIntelligence'\)/);
    assert.doesNotMatch(source, /INSERT INTO content_publications/);
  });
});
