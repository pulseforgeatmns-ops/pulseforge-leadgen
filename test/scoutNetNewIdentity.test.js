'use strict';
// SPEC-SCOUT-P0-007 (#1072) unit coverage: identity matching, CRM ownership of
// contactless companies, zero-yield search rotation and the net-new scorecard.
// No database, provider or network.
const test = require('node:test');
const assert = require('node:assert/strict');
const { sameNameIsDistinctBusiness, localitiesConflict, canonicalDomainSql } = require('../packages/scout/identity/BusinessIdentity');
const { classifyInventoryOwnership, OWNERSHIP_KINDS } = require('../services/outboundInventory');
const { loadScoutDiscoveryBackoff, replenishmentScorecard, _test: { replenishmentOutcome } } = require('../services/maxOutboundControlLoop');

test('same name is a different business only when both domains AND both towns differ', () => {
  const known = { domain: 'acme-denver.example', location: 'Denver, CO 80202' };
  assert.equal(sameNameIsDistinctBusiness(known, { domain: 'acme-nh.example', location: '1 Elm St, Manchester, NH 03101' }), true);
  // Equivalent domain formatting is the same business.
  assert.equal(sameNameIsDistinctBusiness({ website: 'HTTPS://WWW.Acme.example/home' }, { domain: 'acme.example', location: 'Denver' }), false);
  // Missing evidence never splits a name match (fail toward "already known").
  assert.equal(sameNameIsDistinctBusiness({ domain: null, location: 'Denver, CO' }, { domain: 'acme.example', location: 'Manchester, NH' }), false);
  assert.equal(sameNameIsDistinctBusiness(known, { domain: 'acme-nh.example', location: null }), false);
  // Different domains in the same town: possibly one business with two sites.
  assert.equal(sameNameIsDistinctBusiness({ domain: 'a.example', location: 'Manchester, NH' },
    { domain: 'b.example', location: '9 Main St, Manchester, NH 03101' }), false);
  assert.equal(localitiesConflict('Bedford, NH', 'Manchester, NH 03101'), true);
  assert.equal(localitiesConflict('', 'Manchester'), false);
});

test('canonical domain SQL is a single trusted expression over the given column', () => {
  const sql = canonicalDomainSql('c.website');
  assert.match(sql, /COALESCE\(c\.website,''\)/);
  assert.match(sql, /\(www\\\.\)\?/);
});

function ownershipPool(rows) {
  const seen = [];
  return { seen, pool: { query: async (sql, params) => { seen.push({ sql, params }); return { rows }; } } };
}

test('a CRM company without prospects is previously known, never clear', async () => {
  const { pool, seen } = ownershipPool([{ id: null, company_id: null, email: null, crm_company_id: 'c9', client_id: null,
    name: 'Imported Co', domain: 'WWW.imported.example', website: null, company_location: null }]);
  const result = await classifyInventoryOwnership({}, { company: 'Imported Co', domain: 'imported.example' }, { pool, clientId: 13 });
  assert.equal(result.kind, OWNERSHIP_KINDS.KNOWN_COMPANY);
  assert.equal(result.companyId, 'c9');
  assert.match(seen[0].sql, /RIGHT JOIN companies c/);
  assert.match(seen[0].sql, /WHERE c\.client_id=\$4/, 'still scoped to the calling tenant');
});

test('a same-name CRM company in another town with another domain does not block a new business', async () => {
  const { pool } = ownershipPool([{ id: null, company_id: null, email: null, crm_company_id: 'c1', client_id: null,
    name: 'Twin Services', domain: 'twin-denver.example', website: null, company_location: 'Denver, CO' }]);
  const result = await classifyInventoryOwnership({}, { company: 'Twin Services', domain: 'twin-nh.example',
    location: '1 Elm St, Manchester, NH 03101' }, { pool, clientId: 13 });
  assert.equal(result.kind, OWNERSHIP_KINDS.CLEAR);
});

test('a contacted prospect matched through a www/URL website is still a prior-contact collision', async () => {
  const { pool } = ownershipPool([{ id: 'p1', company_id: 'c1', client_id: 13, email: 'a@acme.example', name: 'Acme',
    domain: null, website: 'https://www.acme.example/contact', last_contacted_at: new Date(Date.now() - 86400000) }]);
  const result = await classifyInventoryOwnership({}, { company: 'Acme Holdings', domain: 'acme.example' }, { pool, clientId: 13 });
  assert.equal(result.kind, OWNERSHIP_KINDS.VALID_COLLISION);
});

test('an expired zero-yield cooldown resumes at the next generation, never generation 0', async () => {
  const now = new Date('2026-11-06T12:00:00Z');
  const zero = (generation, minutesAgo) => ({ created_at: new Date(+now - minutesAgo * 60_000),
    payload: { scoutSearchGeneration: generation, newCleanInventoryAdded: 0, newPromotions: 0, recoveredExisting: 0 } });
  const pool = rows => ({ query: async () => ({ rows }) });
  const resumed = await loadScoutDiscoveryBackoff(pool([zero(1, 120), zero(1, 135), zero(0, 150)]), '13', 'fp', now);
  assert.deepEqual([resumed.reason, resumed.searchGeneration, resumed.diversify], ['repeated_zero_yield_rotate_search_resume', 2, true]);
  // Past the final generation the legacy retry remains: durable checkpoints
  // already forbid repurchasing handled queries.
  assert.equal(await loadScoutDiscoveryBackoff(pool([zero(3, 120), zero(3, 135), zero(3, 150)]), '13', 'fp', now), null);
  // Within the cooldown the existing rotation is unchanged.
  const rotating = await loadScoutDiscoveryBackoff(pool([zero(1, 10), zero(1, 20), zero(0, 30)]), '13', 'fp', now);
  assert.deepEqual([rotating.reason, rotating.searchGeneration], ['repeated_zero_yield_rotate_search', 2]);
  // Research-only retention counts as yield and does not trigger rotation.
  const productive = await loadScoutDiscoveryBackoff(pool([{ ...zero(0, 10), payload: { researchOnly: true, researchRetained: 3 } },
    zero(0, 20), zero(0, 30)]), '13', 'fp', now);
  assert.equal(productive, null);
});

test('the scorecard counts only durable net-new rows and never calls research outreach-ready', () => {
  const admission = { discovered: 10, evaluated: 9, fit: 6, previouslyKnown: 4, admittedToEnrichment: 0,
    retainedForResearch: 2, netNewQualified: 2, identityCollisions: 1 };
  const research = replenishmentScorecard({ admissionMeasured: true, admission }, 5, { researchOnly: true, paidResearchUsdMicros: 64000 });
  assert.deepEqual(research, { discovered: 10, evaluated: 9, previouslyKnown: 4, qualified: 6, identityCollisions: 1,
    newlyRetained: 2, newlyQueued: 0, netNewQualified: 2, netNewCleanInventory: 0, outreachReady: false,
    paidResearchUsdMicros: 64000, costPerNetNewQualifiedUsdMicros: 32000 });
  const none = replenishmentScorecard({ admissionMeasured: true, admission: { ...admission, retainedForResearch: 0, netNewQualified: 0 } },
    0, { researchOnly: true, paidResearchUsdMicros: 64000 });
  assert.equal(none.costPerNetNewQualifiedUsdMicros, null, 'no cost-per-prospect without a positive denominator');
  assert.equal(replenishmentScorecard(null, 0).netNewQualified, null, 'unmeasured admission is unknown, not zero');
  assert.equal(replenishmentOutcome({ plan: { shouldReplenish: true }, execute: true,
    scout: { discoveryAttempted: true, admission }, newCleanInventoryAdded: 0 }).status, 'zero_yield');
});
