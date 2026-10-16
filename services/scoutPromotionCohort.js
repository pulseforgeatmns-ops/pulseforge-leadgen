'use strict';

// Transient identity correlation only. Symbols are omitted from JSON logs/events.
const PROMOTION_IDS = Symbol('scoutPromotionIds');
function retainPromotionIds(target, ids) {
  Object.defineProperty(target, PROMOTION_IDS, { value: ids });
  return target;
}
// PostgreSQL prospects IDs are UUIDs; older callers/tests also use integer IDs.
// Normalize both sides identically without coercing objects or unsafe numbers.
function normalizeProspectId(value) {
  if (typeof value === 'number') return Number.isSafeInteger(value) && value > 0 ? String(value) : null;
  if (typeof value !== 'string') return null;
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) return value.toLowerCase();
  return /^[1-9][0-9]*$/.test(value) ? value : null;
}
function promotionCohort(summaries, inventory) {
  if (!Array.isArray(summaries) || !Array.isArray(inventory?.clean) || !Array.isArray(inventory?.excluded)) return null;
  let outcomes = 0;
  let identifiedOutcomes = 0;
  const ids = new Set();
  for (const summary of summaries) {
    if (![summary?.promoted, summary?.recovered].every(n => Number.isSafeInteger(n) && n >= 0)) return null;
    const expected = summary.promoted + summary.recovered;
    if (!Number.isSafeInteger(expected + outcomes)) return null;
    outcomes += expected;
    const retained = summary[PROMOTION_IDS];
    if (!Array.isArray(retained)) continue;
    // Never count more identities than successful outcomes from this batch.
    for (const id of retained.slice(0, expected)) {
      const normalized = normalizeProspectId(id);
      if (normalized === null) continue;
      identifiedOutcomes++;
      ids.add(normalized);
    }
  }
  const clean = new Set(inventory.clean.map(row => normalizeProspectId(row?.prospectId)).filter(id => id !== null));
  const excluded = new Map(inventory.excluded.map(row => [normalizeProspectId(row?.prospectId), row?.reason])
    .filter(([id]) => id !== null));
  const exclusionCounts = Object.create(null);
  let cleanCount = 0;
  let unmatched = 0;
  for (const id of ids) {
    if (clean.has(id)) cleanCount++;
    else if (excluded.has(id)) {
      const reason = excluded.get(id);
      exclusionCounts[reason] = (exclusionCounts[reason] || 0) + 1;
    } else unmatched++;
  }
  return { outcomes, identifiedProspects: ids.size, unidentifiedOutcomes: outcomes - identifiedOutcomes,
    duplicateOutcomeReferences: identifiedOutcomes - ids.size, clean: cleanCount, unmatched, exclusionCounts };
}
module.exports = { retainPromotionIds, promotionCohort };
