'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const routePath = path.join(__dirname, '..', 'routes', 'intelligenceSeedLibraries.js');
const serverPath = path.join(__dirname, '..', 'server.js');
const source = fs.readFileSync(routePath, 'utf8');
const serverSource = fs.readFileSync(serverPath, 'utf8');

describe('intelligenceSeedLibraries routes', () => {
  it('registers GET-only intelligence seed-library endpoints with admin/manager auth', () => {
    assert.match(source, /requireRole\('admin', 'manager'\)/);
    assert.match(source, /Cache-Control',\s*'no-store'/);

    const expected = [
      '/api/v1/intelligence/seed-libraries',
      '/api/v1/intelligence/seed-libraries/:libraryId',
    ];
    for (const route of expected) {
      assert.match(
        source,
        new RegExp(`router\\.get\\('${route.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'`)
      );
    }

    assert.equal((source.match(/router\.post\(/g) || []).length, 0);
    assert.equal((source.match(/router\.put\(/g) || []).length, 0);
    assert.equal((source.match(/router\.patch\(/g) || []).length, 0);
    assert.equal((source.match(/router\.delete\(/g) || []).length, 0);
  });

  it('stamps responses as non-evidence seed references', () => {
    assert.match(source, /kind: 'seed_library_reference'/);
    assert.match(source, /isEvidence: false/);
    assert.match(source, /internal: true/);
  });

  it('does not wire Max runtime or recommendation payloads', () => {
    const codeOnly = source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '');
    assert.equal(codeOnly.includes('getMaxRuntime'), false);
    assert.equal(codeOnly.includes('recommendation'), false);
    assert.equal(/\bscoring\b/i.test(codeOnly), false);
    assert.equal(codeOnly.includes('router.post'), false);
  });

  it('is mounted from server.js', () => {
    assert.match(serverSource, /require\('\.\/routes\/intelligenceSeedLibraries'\)/);
  });

  it('supports include_disabled query for paused seeds', () => {
    assert.match(source, /include_disabled/);
  });
});
