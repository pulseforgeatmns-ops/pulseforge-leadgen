'use strict';
// SPEC-SCOUT-P1-001 Rev. 2 (#1048). Offline, deterministic before/after
// planning fixture. No database, network, clock or paid call: each "cycle"
// takes the planner's first query and marks it searched in memory.
const coverage = require('../services/scoutFacilityResearchCoverage');
const { buildQueriesForEvidence } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { ANCHOR_FACILITY_TIERS: CONFIG } = require('../config/anchorFacilityTiers');
const { largestRemainder } = require('../services/scoutFacilityTierAllocation');

const SCOPE = { eligibleSubsegments: ['managed_facilities', 'restaurant_foh'],
  cities: ['Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown', 'Nashua', 'Merrimack'] };
const REQUEST = { tenantId: '10', segment: 'managed_facilities', evidenceType: 'identity',
  geography: { cities: SCOPE.cities, state: 'NH' } };
const ACTIVATED = '2026-11-10T00:00:00.000Z';
const tierOf = category => CONFIG.tierByCategory[category];

async function run({ cycles, tiered }) {
  const searched = [], selections = [];
  for (let n = 0; n < cycles; n++) {
    const planner = coverage.createFacilityResearchCoverage({ tenantId: '10', scope: SCOPE,
      loadHistory: async () => searched.map(s => ({ query_text: s.query, attempts: 1 })),
      ...(tiered ? { tierAllocation: CONFIG, loadAllocation: async () => ({ activations: [{ allocation_version: CONFIG.version, activated_at: ACTIVATED }],
        pages: searched.map((s, i) => ({ search_fingerprint: 'fixture', query_key: s.rootKey, root_query_key: s.rootKey,
          page_index: 0, state: 'fetched', created_at: new Date(Date.parse(ACTIVATED) + (i + 1) * 1000).toISOString() })),
        claimedRoots: searched.map(s => s.rootKey) }) } : {}) });
    const ordered = await planner.plan(REQUEST, buildQueriesForEvidence);
    const plan = coverage.approvedQueryPlan(SCOPE, 0, buildQueriesForEvidence, tiered ? CONFIG : {});
    const taken = new Set(searched.map(s => s.query));
    const query = ordered.find(q => !taken.has(q));
    if (!query) break;
    const row = plan.find(r => r.query === query);
    searched.push({ query, rootKey: row.rootKey });
    const snap = planner.snapshot().tierAllocation;
    selections.push({ cycle: n + 1, query, category: row.category, tier: tierOf(row.category),
      ...(snap ? { reason: snap.reason } : {}) });
  }
  const byTier = { 1: 0, 2: 0, 3: 0 }, byCategory = {};
  for (const s of selections) { byTier[s.tier]++; byCategory[s.category] = (byCategory[s.category] || 0) + 1; }
  return { approvedQueries: coverage.approvedQueryPlan(SCOPE, 0, buildQueriesForEvidence, tiered ? CONFIG : {}).length,
    byTier, byCategory, firstTen: selections.slice(0, 10).map(s => `${s.tier}:${s.category}`), selections };
}

async function fixture(cycles = 88) {
  return { spec: 'SPEC-SCOUT-P1-001 Rev. 2', issue: 1048, allocationVersion: CONFIG.version, cycles,
    targetQuotas: largestRemainder(cycles, CONFIG.shares),
    assumptions: 'Empty prior history, every query eligible, one selection per cycle, no lookup-level skips.',
    before: await run({ cycles, tiered: false }), after: await run({ cycles, tiered: true }) };
}

const FIXTURE_PATH = require('node:path').join(__dirname, '../test/fixtures/scoutTierAllocation.before-after.json');
// Usage: node scripts/scoutTierAllocationFixture.js [--write]
if (require.main === module) fixture().then(f => {
  const text = `${JSON.stringify(f, null, 2)}\n`;
  if (process.argv.includes('--write')) require('node:fs').writeFileSync(FIXTURE_PATH, text);
  else process.stdout.write(text);
});
module.exports = { fixture, FIXTURE_PATH };
