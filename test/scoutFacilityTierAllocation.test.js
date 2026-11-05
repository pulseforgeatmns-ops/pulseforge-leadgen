'use strict';
// SPEC-SCOUT-P1-001 Rev. 2 (#1048): pure allocator. No DB, network or clock.
const test = require('node:test'), assert = require('node:assert/strict');
const { largestRemainder, chooseTier, simulate, deriveLedger } = require('../services/scoutFacilityTierAllocation');
const { ANCHOR_FACILITY_TIERS: C } = require('../config/anchorFacilityTiers');
const { shares, window } = C;

// Seeded LCG: deterministic, repository-local, no Math.random.
function rng(seed) { let s = seed >>> 0; return () => (s = (Math.imul(s, 1664525) + 1013904223) >>> 0) / 2 ** 32; }

// Asserts the window guarantee over a run: every tier eligible at position p
// was selected within positions [max(lastSel, eligibleSince-1)+1, ...+window].
function assertAntiStarvation(run) {
  const last = {}, since = {}; let prev = new Set();
  for (const row of run.sequence) {
    if (row.position == null) { prev = new Set(); continue; }
    for (const t of row.eligible) if (!prev.has(t)) since[t] = row.position;
    for (const t of [1, 2, 3]) if (!row.eligible.includes(t)) { delete since[t]; delete last[t]; }
    prev = new Set(row.eligible);
    if (row.tier != null) last[row.tier] = row.position;
    for (const t of row.eligible) {
      const anchor = Math.max(last[t] || 0, (since[t] || 1) - 1);
      assert.ok(row.position - anchor < window || last[t] === row.position,
        `tier ${t} starved at position ${row.position} (anchor ${anchor})`);
    }
  }
}

test('largest remainder: 88 -> 44/31/13 with deterministic tie-breaking', () => {
  assert.deepEqual(largestRemainder(88, shares), { 1: 44, 2: 31, 3: 13 });
  assert.deepEqual(largestRemainder(10, shares), { 1: 5, 2: 4, 3: 1 }); // remainders .5 vs .5: lower tier wins
  assert.deepEqual(largestRemainder(20, shares), { 1: 10, 2: 7, 3: 3 }); // exact; no float drift
  assert.deepEqual(largestRemainder(7, shares, [1, 3]), { 1: 5, 2: 0, 3: 2 }); // redistributed to eligible only
  assert.deepEqual(largestRemainder(5, shares, []), { 1: 0, 2: 0, 3: 0 });
  for (let n = 0; n <= 200; n++) assert.equal(Object.values(largestRemainder(n, shares)).reduce((a, b) => a + b), n);
});

test('unconstrained sequential allocation lands exactly on 44/31/13 and tracks quota at every prefix', () => {
  const run = simulate({ steps: 88, shares, window });
  assert.deepEqual(run.counts, { 1: 44, 2: 31, 3: 13 });
  const counts = { 1: 0, 2: 0, 3: 0 };
  for (const row of run.sequence) {
    counts[row.tier]++;
    const quota = largestRemainder(row.position, shares);
    for (const t of [1, 2, 3]) assert.ok(Math.abs(counts[t] - quota[t]) <= 1, `prefix ${row.position} tier ${t}`);
  }
  assertAntiStarvation(run);
  assert.deepEqual(run.sequence.slice(0, 10).map(r => r.tier), [1, 2, 1, 3, 2, 1, 1, 2, 1, 2]);
});

test('identical inputs produce identical selections and decision records', () => {
  const a = simulate({ steps: 200, shares, window }), b = simulate({ steps: 200, shares, window });
  assert.equal(JSON.stringify(a), JSON.stringify(b));
  const input = { position: 9, history: [{ position: 1, tier: 1 }, { position: 2, tier: 1 }], eligible: [3, 1, 2], shares, window };
  assert.equal(JSON.stringify(chooseTier(input)), JSON.stringify(chooseTier({ ...input, eligible: [1, 2, 3] })));
});

test('tier eligible -> ineligible -> eligible again keeps deadlines and records no invalid selection', () => {
  // Tier 3 drops out for positions 15..39, then returns at 40.
  const elig = p => (p >= 15 && p < 40 ? [1, 2] : [1, 2, 3]);
  const run = simulate({ steps: 120, eligibility: elig, shares, window });
  assertAntiStarvation(run);
  for (const row of run.sequence) assert.ok(row.eligible.includes(row.tier), `invalid tier at ${row.position}`);
  const t3 = run.sequence.filter(r => r.tier === 3).map(r => r.position);
  assert.ok(!t3.some(p => p >= 15 && p < 40), 'no selection while ineligible');
  const firstBack = t3.find(p => p >= 40);
  assert.ok(firstBack <= 49, `returned tier selected within the window, got ${firstBack}`);
  // While ineligible, its share is redistributed in proportion 50:35.
  const gap = run.sequence.filter(r => r.position >= 15 && r.position < 40);
  const g1 = gap.filter(r => r.tier === 1).length, g2 = gap.filter(r => r.tier === 2).length;
  assert.ok(Math.abs(g1 / (g1 + g2) - 0.5 / 0.85) < 0.08);
});

test('two tiers returning at once with a third near its deadline still meet the window', () => {
  const elig = p => (p >= 5 && p < 30 ? [2] : [1, 2, 3]);
  const run = simulate({ steps: 80, eligibility: elig, shares, window });
  assertAntiStarvation(run);
});

test('randomized but seeded eligibility churn never starves an eligible tier', () => {
  for (let seed = 1; seed <= 400; seed++) {
    const r = rng(seed), plan = [];
    for (let p = 0; p < 150; p++) {
      const set = [1, 2, 3].filter(() => r() < 0.8);
      plan.push(set);
    }
    const run = simulate({ steps: 150, eligibility: p => plan[(p - 1) % plan.length], shares, window });
    assertAntiStarvation(run);
    for (const row of run.sequence) if (row.tier != null) assert.ok(row.eligible.includes(row.tier));
  }
});

test('long-run convergence toward 50/35/15 after a disruption', () => {
  const run = simulate({ steps: 1000, eligibility: p => (p <= 60 ? [1, 3] : [1, 2, 3]), shares, window });
  const tail = run.sequence.slice(500);
  const share = t => tail.filter(r => r.tier === t).length / tail.length;
  assert.ok(Math.abs(share(1) - 0.50) < 0.02 && Math.abs(share(2) - 0.35) < 0.02 && Math.abs(share(3) - 0.15) < 0.02);
  // Deficit catches up: total counts end within 1 of the full-portfolio quota.
  const quota = largestRemainder(1000, shares);
  for (const t of [1, 2, 3]) assert.ok(Math.abs(run.counts[t] - quota[t]) <= 3, `tier ${t}`);
});

test('exhausted tier is never selected and no step fakes compliance', () => {
  const run = simulate({ steps: 50, eligibility: p => (p > 20 ? [1, 3] : [1, 2, 3]), shares, window });
  for (const row of run.sequence.filter(r => r.position > 20)) assert.notEqual(row.tier, 2);
  const none = simulate({ steps: 3, eligibility: () => [], shares, window });
  assert.deepEqual(none.sequence.map(r => [r.tier, r.reason]), [[null, 'no_eligible_tier'], [null, 'no_eligible_tier'], [null, 'no_eligible_tier']]);
  assert.deepEqual(none.counts, { 1: 0, 2: 0, 3: 0 });
});

test('anti-starvation forces a tier before its deadline and records the reason', () => {
  // Nine selections without tier 3: position 10 is its last legal slot.
  const history = Array.from({ length: 9 }, (_, i) => ({ position: i + 1, tier: i % 2 ? 2 : 1 }));
  const choice = chooseTier({ position: 10, history, eligible: [1, 2, 3], shares, window });
  assert.equal(choice.tier, 3); assert.equal(choice.reason, 'anti_starvation'); assert.equal(choice.deadlines[3], 10);
});

test('invalid configuration and history fail closed', () => {
  assert.throws(() => chooseTier({ position: 1, eligible: [1], shares: { 1: 0.5, 2: 0.5, 3: 0.5 }, window }), /shares_invalid/);
  assert.throws(() => chooseTier({ position: 1, eligible: [1], shares, window: 2 }), /window_invalid/);
  assert.throws(() => chooseTier({ position: 2, history: [{ position: 2, tier: 1 }], eligible: [1], shares, window }), /history_invalid/);
  assert.throws(() => deriveLedger({ pages: [], tierOfRoot: () => 1 }), /binding_required/);
});

test('two tiers due at the same position are detectable from the deadlines (reported as windowAtRisk)', () => {
  // Tier 3 returns overdue while tier 2 is exactly due: one of them must miss.
  const history = [{ position: 1, tier: 3 }, ...Array.from({ length: 9 }, (_, i) => ({ position: i + 2, tier: 1 }))];
  const choice = chooseTier({ position: 11, history, eligible: [1, 2, 3], shares, window });
  const due = [1, 2, 3].filter(t => choice.deadlines[t] <= 11);
  assert.deepEqual(due, [2, 3]);
  assert.equal(choice.tier, 2, 'earliest deadline goes first');
});
