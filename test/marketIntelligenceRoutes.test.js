'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const routePath = path.join(__dirname, '..', 'routes', 'marketIntelligence.js');
const serverPath = path.join(__dirname, '..', 'server.js');
const source = fs.readFileSync(routePath, 'utf8');
const serverSource = fs.readFileSync(serverPath, 'utf8');

describe('marketIntelligence routes', () => {
  it('registers GET-only market-intel endpoints with admin/manager auth', () => {
    assert.match(source, /requireRole\('admin', 'manager'\)/);
    assert.match(source, /Cache-Control',\s*'no-store'/);

    const expected = [
      '/api/v1/market-intel/readiness',
      '/api/v1/market-intel/briefing',
      '/api/v1/market-intel/offers',
      '/api/v1/market-intel/ctas',
      '/api/v1/market-intel/companies/cadence',
      '/api/v1/market-intel/themes',
      '/api/v1/market-intel/changes',
      '/api/v1/market-intel/import-intents',
      '/api/v1/market-intel/companies',
      '/api/v1/market-intel/companies/:id',
      '/api/v1/market-intel/companies/:id/timeline',
      '/api/v1/market-intel/emails/:id',
      '/api/v1/market-intel/cross-market/patterns',
      '/api/v1/market-intel/cross-market/sequence-stats',
    ];
    for (const route of expected) {
      assert.match(source, new RegExp(`router\\.get\\('${route.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'`));
    }

    // Static /companies/cadence must be registered before /companies/:id.
    const cadenceIdx = source.indexOf("router.get('/api/v1/market-intel/companies/cadence'");
    const companyIdIdx = source.indexOf("router.get('/api/v1/market-intel/companies/:id'");
    assert.ok(cadenceIdx > -1 && companyIdIdx > -1 && cadenceIdx < companyIdIdx);

    assert.equal((source.match(/router\.post\(/g) || []).length, 0);
    assert.equal((source.match(/router\.put\(/g) || []).length, 0);
    assert.equal((source.match(/router\.patch\(/g) || []).length, 0);
    assert.equal((source.match(/router\.delete\(/g) || []).length, 0);
  });

  it('wires SPEC-067 readiness via read-only report builder', () => {
    assert.match(source, /buildMarketIntelReadinessReport/);
    assert.match(source, /marketIntelligenceReadiness/);
    assert.match(source, /\/api\/v1\/market-intel\/readiness/);
  });

  it('wires SPEC-071 briefing surfaces as GET-only synthesis', () => {
    assert.match(source, /getMarketIntelligenceBriefing/);
    assert.match(source, /marketIntelligenceBriefing/);
    assert.match(source, /isEvidence:\s*false/);
    assert.match(source, /getTopOffers/);
    assert.match(source, /getTopCtas/);
    assert.match(source, /getCompanyCadence/);
    assert.match(source, /getMessagingThemes/);
    assert.match(source, /getRecentMessagingChanges/);
    assert.match(source, /getObservationsByIntent/);
  });

  it('does not wire Max runtime or recommendation payloads', () => {
    const codeOnly = source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '');
    assert.equal(codeOnly.includes('getMaxRuntime'), false);
    assert.equal(codeOnly.includes('recommendation'), false);
    assert.equal(/\bscoring\b/i.test(codeOnly), false);
    assert.equal(codeOnly.includes('router.post'), false);
  });

  it('is mounted from server.js', () => {
    assert.match(serverSource, /require\('\.\/routes\/marketIntelligence'\)/);
  });

  it('timeline response shape includes optional descriptive diffs', () => {
    assert.match(source, /diffTimelineField/);
    assert.match(source, /diffs/);
  });
});
