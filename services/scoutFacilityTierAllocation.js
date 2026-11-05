'use strict';

// SPEC-SCOUT-P1-001 Rev. 2 (#1048). Pure, deterministic tier allocation.
// No I/O, no clock, no randomness. The allocation ledger is DERIVED from the
// existing P0-005 query lifecycle (scout_query_pages); this module never
// records, reserves, claims or charges anything itself.

const TIERS = Object.freeze([1, 2, 3]);
const fail = code => Object.assign(Error(code), { code });

function validate(shares, window) {
  const sum = TIERS.reduce((s, t) => s + Number(shares?.[t]), 0);
  if (TIERS.some(t => !(Number(shares?.[t]) > 0)) || Math.abs(sum - 1) > 1e-9) throw fail('tier_allocation_shares_invalid');
  if (!Number.isInteger(window) || window < TIERS.length) throw fail('tier_allocation_window_invalid');
}

function eligibleTiers(eligible) {
  const set = new Set([...(eligible || [])].map(Number));
  return TIERS.filter(t => set.has(t));
}

function normalizedShares(shares, tiers) {
  const total = tiers.reduce((s, t) => s + shares[t], 0);
  return Object.fromEntries(tiers.map(t => [t, shares[t] / total]));
}

// Hamilton / largest remainder. Ties go to the lower tier number.
function largestRemainder(n, shares, eligible = TIERS) {
  if (!Number.isInteger(n) || n < 0) throw fail('tier_allocation_portfolio_invalid');
  const tiers = eligibleTiers(eligible);
  const quotas = Object.fromEntries(TIERS.map(t => [t, 0]));
  if (!tiers.length || !n) return quotas;
  const norm = normalizedShares(shares, tiers);
  const exact = tiers.map(t => ({ t, q: norm[t] * n }));
  // Integer-safe floor: guard binary rounding such as 0.35 * 20 = 6.999...
  for (const row of exact) { row.f = Math.floor(row.q + 1e-9); row.r = row.q - row.f; quotas[row.t] = row.f; }
  let left = n - exact.reduce((s, row) => s + row.f, 0);
  for (const row of exact.slice().sort((a, b) => (b.r - a.r) || (a.t - b.t))) {
    if (left-- <= 0) break;
    quotas[row.t] += 1;
  }
  return quotas;
}

// history: completed selections in this allocation version, positions 1..p-1.
// eligibleSince: first position of each tier's current continuous eligible run.
function deadlinesAt({ history, eligibleSince, eligible, window }) {
  const last = {};
  for (const row of history) last[row.tier] = Math.max(last[row.tier] || 0, row.position);
  return Object.fromEntries(eligible.map(t =>
    [t, Math.max(last[t] || 0, (eligibleSince?.[t] || 1) - 1) + window]));
}

function chooseTier({ position, history = [], eligible, eligibleSince = {}, shares, window }) {
  validate(shares, window);
  if (!Number.isInteger(position) || position < 1) throw fail('tier_allocation_position_invalid');
  const tiers = eligibleTiers(eligible);
  const counts = Object.fromEntries(TIERS.map(t => [t, 0]));
  for (const row of history) {
    if (!TIERS.includes(row.tier) || !(row.position < position)) throw fail('tier_allocation_history_invalid');
    counts[row.tier] += 1;
  }
  if (!tiers.length) return { tier: null, reason: 'no_eligible_tier', order: [], counts, deadlines: {}, deficits: {} };
  const deadlines = deadlinesAt({ history, eligibleSince, eligible: tiers, window });
  const norm = normalizedShares(shares, tiers);
  const deficits = Object.fromEntries(tiers.map(t => [t, norm[t] * position - counts[t]]));
  // Earliest-deadline-first feasibility: if the i-th most urgent tier has at
  // most i positions left before its deadline, the most urgent must go now.
  const urgency = tiers.slice().sort((a, b) => (deadlines[a] - deadlines[b]) || (a - b));
  const forced = urgency.some((t, i) => deadlines[t] - position + 1 <= i + 1);
  const byDeficit = tiers.slice().sort((a, b) => (deficits[b] - deficits[a]) || (a - b));
  const tier = forced ? urgency[0] : byDeficit[0];
  // Fallback order when the chosen tier has no executable query this cycle:
  // urgent tiers by deadline, then the rest by deficit.
  const isUrgent = (t, i) => deadlines[t] - position + 1 <= i + 1;
  const urgent = urgency.filter((t, i) => isUrgent(t, i));
  const order = [tier, ...urgent.filter(t => t !== tier),
    ...byDeficit.filter(t => t !== tier && !urgent.includes(t))];
  return { tier, reason: forced ? 'anti_starvation' : 'deficit', order, counts, deadlines, deficits };
}

// Deterministic simulation used by tests and the allocation fixture.
// eligibility(position) -> eligible tiers at that position.
function simulate({ steps, eligibility = () => TIERS, shares, window }) {
  const history = [], eligibleSince = {}, sequence = [];
  let prev = new Set(), position = 0;
  for (let step = 1; step <= steps; step++) {
    const now = eligibleTiers(eligibility(position + 1));
    for (const t of now) if (!prev.has(t)) eligibleSince[t] = position + 1;
    prev = new Set(now);
    const choice = chooseTier({ position: position + 1, history, eligible: now, eligibleSince, shares, window });
    if (choice.tier == null) { sequence.push({ step, position: null, tier: null, reason: choice.reason, eligible: now }); continue; }
    position += 1;
    history.push({ position, tier: choice.tier });
    sequence.push({ step, position, tier: choice.tier, reason: choice.reason, eligible: now });
  }
  const counts = Object.fromEntries(TIERS.map(t => [t, history.filter(r => r.tier === t).length]));
  return { sequence, history, counts };
}

// Activation windows: version v owns [activated_at(v), next activated_at).
// Pages before the first activation are pre-ledger and get NO tier.
function activationWindows(activations = []) {
  const rows = activations.map(a => ({ version: String(a.allocation_version), at: new Date(a.activated_at).getTime() }));
  if (rows.some(r => !Number.isFinite(r.at))) throw fail('tier_allocation_activation_invalid');
  rows.sort((a, b) => (a.at - b.at) || a.version.localeCompare(b.version));
  if (new Set(rows.map(r => r.version)).size !== rows.length) throw fail('tier_allocation_activation_duplicate');
  return rows.map((r, i) => ({ ...r, until: rows[i + 1]?.at ?? Infinity }));
}

// Source-query tier as of selection: the version whose window contains the
// page, and that version's own pinned universe. Never today's mapping.
function attributePage(page, windows, universeOf) {
  const created = new Date(page.created_at).getTime();
  if (!Number.isFinite(created)) throw fail('tier_allocation_ledger_time_invalid');
  const w = windows.find(x => x.at <= created && created < x.until);
  if (!w) return { created, attribution: 'pre_ledger' };
  const universe = universeOf(w.version);
  if (!universe) return { created, version: w.version, attribution: 'unknown_version' };
  const tier = universe.get(page.root_query_key);
  if (!TIERS.includes(tier)) return { created, version: w.version, attribution: 'unattributed' };
  return { created, version: w.version, attribution: 'attributed', tier };
}

// Ledger derived from the existing lifecycle. One page row = one paid-search
// selection. Status follows the page state only:
//   fetched | blocked  -> completed (provider call issued, terminal outcome)
//   reserved           -> uncertain (may have been charged; the lifecycle never
//                         refetches it). Counted conservatively toward allocation,
//                         reported separately from completed.
// A pre-network release (fail({noNetwork:true})) deletes the reservation, so
// an aborted selection simply has no row and consumes no position.
function deriveLedger({ pages = [], activations = [], version, universeOf }) {
  if (!version || typeof universeOf !== 'function') throw fail('tier_allocation_ledger_binding_required');
  const windows = activationWindows(activations);
  const window = windows.find(w => w.version === version);
  if (!window) throw fail('tier_allocation_version_not_activated');
  const rows = [], seen = new Set();
  const attribution = { pre_ledger: 0, other_version: 0, unknown_version: 0, unattributed: 0 };
  for (const page of pages) {
    const key = `${page.search_fingerprint}|${page.query_key}`;
    if (seen.has(key)) throw fail('tier_allocation_ledger_duplicate_page');
    seen.add(key);
    if (!['reserved', 'fetched', 'blocked'].includes(page.state)) throw fail('tier_allocation_ledger_state_invalid');
    const a = attributePage(page, windows, universeOf);
    if (a.attribution === 'pre_ledger') { attribution.pre_ledger++; continue; }
    if (a.version !== version) { attribution.other_version++; continue; }
    if (a.attribution !== 'attributed') { attribution[a.attribution]++; continue; }
    rows.push({ created: a.created, rootKey: page.root_query_key, pageIndex: Number(page.page_index),
      fingerprint: page.search_fingerprint, tier: a.tier, status: page.state === 'reserved' ? 'uncertain' : 'completed' });
  }
  rows.sort((a, b) => (a.created - b.created) || a.rootKey.localeCompare(b.rootKey)
    || (a.pageIndex - b.pageIndex) || a.fingerprint.localeCompare(b.fingerprint));
  const history = rows.map((row, i) => ({ position: i + 1, tier: row.tier, status: row.status,
    rootKey: row.rootKey, pageIndex: row.pageIndex }));
  const counts = Object.fromEntries(TIERS.map(t => [t, history.filter(r => r.tier === t).length]));
  const countsByStatus = Object.fromEntries(TIERS.map(t => [t, {
    completed: history.filter(r => r.tier === t && r.status === 'completed').length,
    uncertain: history.filter(r => r.tier === t && r.status === 'uncertain').length }]));
  return { version, history, counts, countsByStatus, uncertain: history.filter(r => r.status === 'uncertain').length,
    attribution, superseded: window.until !== Infinity, nextPosition: history.length + 1 };
}

module.exports = { TIERS, largestRemainder, chooseTier, simulate, deriveLedger, deadlinesAt, activationWindows, attributePage };
