'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const routePath = path.join(__dirname, '..', 'routes', 'prospectOperatingBrief.js');
const serverPath = path.join(__dirname, '..', 'server.js');
const source = fs.readFileSync(routePath, 'utf8');
const serverSource = fs.readFileSync(serverPath, 'utf8');

describe('prospectOperatingBrief routes', () => {
  it('registers GET-only operating-brief endpoint with admin/manager auth', () => {
    assert.match(source, /requireRole\('admin', 'manager'\)/);
    assert.match(source, /Cache-Control',\s*'no-store'/);
    assert.match(
      source,
      /router\.get\('\/api\/v1\/prospects\/operating-brief'/
    );

    assert.equal((source.match(/router\.post\(/g) || []).length, 0);
    assert.equal((source.match(/router\.put\(/g) || []).length, 0);
    assert.equal((source.match(/router\.patch\(/g) || []).length, 0);
    assert.equal((source.match(/router\.delete\(/g) || []).length, 0);
  });

  it('wires read-only prospect operating brief service', () => {
    assert.match(source, /getProspectOperatingBrief/);
    assert.match(source, /prospectOperatingBrief/);
    assert.match(source, /isEvidence:\s*false/);
  });

  it('does not wire outbound sends or CRM mutations', () => {
    const codeOnly = source
      .replace(/\/\*[\s\S]*?\*\//g, '')
      .replace(/\/\/.*$/gm, '');
    assert.equal(codeOnly.includes('router.post'), false);
    assert.equal(/\bsendEmail\b/.test(codeOnly), false);
    assert.equal(/\bUPDATE\b/.test(codeOnly), false);
    assert.equal(codeOnly.includes('autonomous'), false);
  });

  it('is mounted from server.js', () => {
    assert.match(serverSource, /require\('\.\/routes\/prospectOperatingBrief'\)/);
  });
});
