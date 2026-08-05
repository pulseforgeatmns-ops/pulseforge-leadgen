'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const routePath = path.join(__dirname, '..', 'routes', 'serviceModeOperatorLoop.js');
const serverPath = path.join(__dirname, '..', 'server.js');
const source = fs.readFileSync(routePath, 'utf8');
const serverSource = fs.readFileSync(serverPath, 'utf8');

describe('serviceModeOperatorLoop routes', () => {
  it('registers GET-only service-loop endpoint with admin/manager auth', () => {
    assert.match(source, /requireRole\('admin', 'manager'\)/);
    assert.match(source, /Cache-Control',\s*'no-store'/);
    assert.match(
      source,
      /router\.get\('\/api\/v1\/operator\/service-loop'/
    );

    assert.equal((source.match(/router\.post\(/g) || []).length, 0);
    assert.equal((source.match(/router\.put\(/g) || []).length, 0);
    assert.equal((source.match(/router\.patch\(/g) || []).length, 0);
    assert.equal((source.match(/router\.delete\(/g) || []).length, 0);
  });

  it('wires read-only service mode operator loop', () => {
    assert.match(source, /getServiceModeOperatorLoop/);
    assert.match(source, /serviceModeOperatorLoop/);
    assert.match(source, /isEvidence:\s*false/);
    assert.match(source, /relationshipInteractionId/);
  });

  it('does not wire outbound sends or CRM mutations', () => {
    const codeOnly = source
      .replace(/\/\*[\s\S]*?\*\//g, '')
      .replace(/\/\/.*$/gm, '');
    assert.equal(codeOnly.includes('router.post'), false);
    assert.equal(/\bsendEmail\b/.test(codeOnly), false);
    assert.equal(/\bUPDATE\b/.test(codeOnly), false);
  });

  it('is mounted from server.js', () => {
    assert.match(serverSource, /require\('\.\/routes\/serviceModeOperatorLoop'\)/);
  });
});
