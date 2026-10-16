'use strict';

// Transient identity correlation only. Symbols are omitted from JSON logs/events.
const PROMOTION_IDS = Symbol('scoutPromotionIds');
function retainPromotionIds(target, ids) {
  Object.defineProperty(target, PROMOTION_IDS, { value: ids });
  return target;
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
      if (!['string', 'number'].includes(typeof id) || !/^[1-9][0-9]*$/.test(String(id))) continue;
      identifiedOutcomes++;
      ids.add(String(id));
    }
  }
  const clean = new Set(inventory.clean.map(row => String(row.prospectId)));
  const excluded = new Map(inventory.excluded.map(row => [String(row.prospectId), row.reason]));
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
