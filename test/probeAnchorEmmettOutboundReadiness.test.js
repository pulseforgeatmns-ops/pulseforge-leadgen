'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const SCRIPT = path.join(__dirname, '..', 'scripts', 'probeAnchorEmmettOutboundReadiness.js');
const source = fs.readFileSync(SCRIPT, 'utf8');

describe('probeAnchorEmmettOutboundReadiness — safety', () => {
  it('refuses to run without --confirm-production', async () => {
    const { parseArgs, run } = require('../scripts/probeAnchorEmmettOutboundReadiness');
    assert.equal(parseArgs([]).confirmProduction, false);
    assert.equal(parseArgs(['--confirm-production']).confirmProduction, true);
    await assert.rejects(
      () => run({ confirmProduction: false }),
      (err) => err.code === 'confirm_production_required'
    );
  });

  it('never posts mail or mutates tenant activation', () => {
    assert.doesNotMatch(source, /api\.brevo\.com\/v3\/smtp/);
    assert.doesNotMatch(source, /sendEmail\s*\(/);
    assert.doesNotMatch(source, /UPDATE\s+clients/i);
    assert.doesNotMatch(source, /SET\s+autosend_enabled/i);
    assert.doesNotMatch(source, /array_append\(\s*enabled_agents/i);
    assert.match(source, /getBrevoState/);
  });

  it('scopes the probe to tenant 10 and stays read-only', () => {
    const { TENANT_ID } = require('../scripts/probeAnchorEmmettOutboundReadiness');
    assert.equal(TENANT_ID, '10');
    assert.match(source, /readOnly: true/);
    assert.match(source, /sentMail: false/);
  });
});
