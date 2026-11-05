'use strict';

// Ordering only: reuses approved concepts, exact query identities and all native
// checkpoint/page/provider gates. No new authority, query reset, DDL or paid call.
const { expandConcepts } = require('../packages/scout/coverage/ConceptLibrary');
const { queryIdentity } = require('./scoutQueryCheckpoints');
const VERSION = 'anchor-facility-coverage-v1';
const normalize = value => String(value || '').toLowerCase().replace(/\s+/g, ' ').trim();
// Relative order of the original eight is unchanged, so v1 plans are identical.
const CATEGORIES = ['early_learning', 'school', 'campus', 'property_manager', 'manufacturer', 'warehouse', 'dealership',
  'medical', 'commercial_office', 'restaurant'];
// Coverage workloads use canonical labels for these approved facility categories.
// Resolve only their mission authority, never their broader ordinary query sets.
const CANONICAL_FACILITY_SEGMENTS = Object.freeze(['property_management', 'commercial_office']);
function category(concept) {
  const s = normalize(concept);
  // Before school: "preschool" is early learning, not a K-12 school.
  if (/daycare|day care|preschool|pre-school|early learning|child ?care/.test(s)) return 'early_learning';
  if (/school|academy/.test(s)) return 'school';
  if (/college|university|campus/.test(s)) return 'campus';
  if (/property manag/.test(s)) return 'property_manager';
  if (/manufactur|industrial facility/.test(s)) return 'manufacturer';
  if (/warehouse|distribution/.test(s)) return 'warehouse';
  if (/dealership|dealer group/.test(s)) return 'dealership';
  // Before office: "medical office" and "dental office" are medical facilities.
  if (/medical|dental|dentist|clinic|physician/.test(s)) return 'medical';
  if (/office|business center/.test(s)) return 'commercial_office';
  if (/restaurant|cafe|bistro|tavern|diner|eatery|bar and grill|gastropub/.test(s)) return 'restaurant';
  return null;
}

function approvedQueryPlan(scope, generation, buildQueries, options = {}) {
  const segments = scope.eligibleSubsegments || [];
  if (!segments.length || segments.some(x => !['managed_facilities', 'restaurant_foh'].includes(x)) || !scope.cities?.length)
    throw Error('facility_research_approved_scope_required');
  const added = options.additionalConcepts || {};
  const concepts = segments.flatMap(segment => [...new Set([
    ...expandConcepts({ segments: [segment], businessNeed: 'commercial_cleaning', discoveryGeneration: generation }),
    ...(added[segment] || [])])]
    .map(concept => ({ segment, concept, category: category(concept) })));
  if (concepts.some(row => !row.category)) throw Error('facility_research_unknown_approved_concept');
  const categories = CATEGORIES.filter(c => concepts.some(row => row.category === c));
  const queries = [], seen = new Set();
  const synonyms = Math.max(...categories.map(c => concepts.filter(row => row.category === c).length));
  // Diagonal town ordering avoids a category-first OR city-first prefix.
  for (let alias = 0; alias < synonyms; alias++) for (let round = 0; round < scope.cities.length; round++)
    for (let c = 0; c < categories.length; c++) {
      const row = concepts.filter(row => row.category === categories[c])[alias];
      if (!row) continue;
      const city = scope.cities[(round + c) % scope.cities.length];
      for (const query of buildQueries({ segment: row.segment, evidenceType: 'identity', cities: [city], state: 'NH', discoveryConcept: row.concept })) {
        if (!seen.has(normalize(query))) {
          seen.add(normalize(query)); queries.push({ ...row, city, query, rootKey: queryIdentity(query, false), priority: queries.length });
        }
      }
    }
  return queries;
}

function rankQueries(plan, history = []) {
  const byKey = new Map(plan.flatMap(row => [[row.rootKey, row], [queryIdentity(row.query, true), row]]));
  const byQuery = new Map(plan.map(row => [normalize(row.query), row]));
  const counts = { categories: {}, towns: {}, concepts: {}, roots: {} };
  const counted = new Set();
  for (const row of history) {
    const known = byQuery.get(normalize(row.query_text || row.query)) || byKey.get(row.root_query_key);
    if (!known || counted.has(known.rootKey)) continue;
    counted.add(known.rootKey);
    const attempts = Math.max(1, Number(row.attempts || 1));
    if (!Number.isSafeInteger(attempts)) throw Error('facility_research_history_invalid');
    for (const [bucket, key] of [['categories', known.category], ['towns', normalize(known.city)], ['concepts', normalize(known.concept)], ['roots', known.rootKey]])
      counts[bucket][key] = (counts[bucket][key] || 0) + attempts;
  }
  const score = row => [counts.categories[row.category] || 0, counts.towns[normalize(row.city)] || 0,
    counts.concepts[normalize(row.concept)] || 0, counts.roots[row.rootKey] || 0, row.priority];
  const ordered = plan.slice().sort((a, b) => {
    const x = score(a), y = score(b);
    for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return x[i] - y[i];
    return 0;
  });
  return { ordered, counts };
}

const allocation = require('./scoutFacilityTierAllocation');
const { createHash } = require('node:crypto');
const { ANCHOR_FACILITY_TIER_VERSIONS } = require('../config/anchorFacilityTiers');

// A version's universe: every generation's approved plan under the version's
// pinned scope and concepts. rootKey -> tier. Deterministic and memoized.
const universes = new Map();
function versionUniverse(config, buildQueries) {
  if (!universes.has(config.version)) {
    const map = new Map();
    for (let g = 0; g < config.generations; g++)
      for (const row of approvedQueryPlan(config.scope, g, buildQueries, config)) {
        const tier = config.tierByCategory[row.category];
        if (!allocation.TIERS.includes(tier)) throw Error('facility_research_unmapped_tier');
        if (map.has(row.rootKey) && map.get(row.rootKey) !== tier) throw Error('facility_research_tier_ambiguous');
        map.set(row.rootKey, tier);
      }
    universes.set(config.version, map);
  }
  return universes.get(config.version);
}
function universeSha256(map) {
  return createHash('sha256').update(JSON.stringify([...map].sort((a, b) => a[0].localeCompare(b[0])))).digest('hex');
}
const sameScope = (a, b) => JSON.stringify([...a.eligibleSubsegments].sort()) === JSON.stringify([...b.eligibleSubsegments].sort())
  && JSON.stringify([...a.cities].map(normalize).sort()) === JSON.stringify([...b.cities].map(normalize).sort());

// Read-only allocation state from the existing lifecycle tables. Nothing here
// writes, reserves or claims.
async function loadAllocationState(pool, config, keys) {
  let activations;
  try {
    activations = (await pool.query(`SELECT allocation_version,activated_at FROM scout_query_allocation_activations
      WHERE tenant_id=$1 ORDER BY activated_at,allocation_version`, [config.tenantId])).rows;
  } catch (error) {
    if (error?.code === '42P01') return { activations: [] }; // Migration not applied.
    throw error;
  }
  if (!activations.length) return { activations };
  const [pages, claimed] = await Promise.all([
    pool.query(`SELECT search_fingerprint,query_key,root_query_key,page_index,state,created_at
      FROM scout_query_pages WHERE tenant_id=$1 AND created_at>=$2 AND root_query_key=ANY($3::text[])`,
    [config.tenantId, activations[0].activated_at, keys]),
    pool.query(`SELECT root_query_key FROM scout_tenant_query_owners WHERE tenant_id=$1 AND root_query_key=ANY($2::text[])
      UNION SELECT root_query_key FROM scout_query_pages WHERE tenant_id=$1 AND root_query_key=ANY($2::text[])`,
    [config.tenantId, keys]),
  ]);
  return { activations, pages: pages.rows, claimedRoots: claimed.rows.map(r => r.root_query_key) };
}

function tierOrdering(plan, ranked, config, state, buildQueries) {
  const tierOf = row => config.tierByCategory[row.category];
  const universeOf = version => ANCHOR_FACILITY_TIER_VERSIONS[version]
    ? versionUniverse(ANCHOR_FACILITY_TIER_VERSIONS[version], buildQueries) : null;
  const ledger = allocation.deriveLedger({ pages: state.pages || [], activations: state.activations,
    version: config.version, universeOf });
  const claimed = new Set(state.claimedRoots || []);
  const universe = universeOf(config.version);
  // Eligible = an unclaimed first-page query exists in THIS cycle's plan.
  // Nothing is manufactured or repeated to satisfy a target.
  const openInPlan = Object.fromEntries(allocation.TIERS.map(t =>
    [t, plan.filter(r => tierOf(r) === t && !claimed.has(r.rootKey)).length]));
  const eligible = allocation.TIERS.filter(t => openInPlan[t] > 0);
  const eligibilityShortfalls = allocation.TIERS.filter(t => !eligible.includes(t)).map(t => ({ tier: t,
    reason: [...universe].some(([key, tier]) => tier === t && !claimed.has(key))
      ? 'no_unclaimed_query_in_current_generation' : 'tier_exhausted_in_version' }));
  const choice = allocation.chooseTier({ position: ledger.nextPosition, history: ledger.history, eligible,
    shares: config.shares, window: config.window });
  const tierRank = new Map([...choice.order, ...allocation.TIERS.filter(t => !choice.order.includes(t))].map((t, i) => [t, i]));
  // Unclaimed first-page queries in tier order first; already-claimed roots
  // (continuations only) after every unclaimed query, as before.
  const orderedRows = ranked.map((row, i) => ({ row, i, tier: tierOf(row), open: !claimed.has(row.rootKey) }))
    .sort((a, b) => (Number(b.open) - Number(a.open)) || (tierRank.get(a.tier) - tierRank.get(b.tier)) || (a.i - b.i))
    .map(x => x.row);
  return { orderedRows, record: { version: config.version, status: 'active', universeSha256: config.universeSha256,
    position: ledger.nextPosition, intendedTier: choice.tier, reason: choice.reason, fallbackOrder: choice.order,
    eligibleTiers: eligible, eligibilityShortfalls, openQueriesThisCycle: openInPlan,
    counts: choice.counts, countsByStatus: ledger.countsByStatus, uncertainSelections: ledger.uncertain,
    deadlines: choice.deadlines, deficits: choice.deficits,
    // A continuously eligible tier can never be past its deadline (EDF). A tier
    // past it has returned after being ineligible and is scheduled at once.
    // If two tiers must go at this same position, one window is missed: that
    // is reported, never hidden. The generation only advances once no
    // unclaimed query remains, so production should not reach it.
    overdueOnReturn: eligible.filter(t => choice.deadlines[t] < ledger.nextPosition),
    windowAtRisk: eligible.filter(t => choice.deadlines[t] <= ledger.nextPosition).length > 1
      ? eligible.filter(t => choice.deadlines[t] <= ledger.nextPosition) : [],
    targetQuotas: allocation.largestRemainder(ledger.nextPosition, config.shares, eligible),
    excludedPages: ledger.attribution } };
}

// Inert states fall back to the approved v1 plan and order, unchanged.
function inertStatus(config, state, scope, buildQueries) {
  if (!sameScope(scope, config.scope)) return 'scope_version_mismatch';
  if (universeSha256(versionUniverse(config, buildQueries)) !== config.universeSha256) return 'universe_version_mismatch';
  const windows = allocation.activationWindows(state?.activations || []);
  const mine = windows.find(w => w.version === config.version);
  if (!mine) return 'not_activated';
  if (mine.until !== Infinity) return 'superseded';
  return null;
}

function createFacilityResearchCoverage({ pool, tenantId, scope, generation = 0, loadHistory, tierAllocation = null, loadAllocation }) {
  if (String(tenantId) !== '10' || (!loadHistory && !pool?.query)) throw Error('facility_research_tenant_required');
  if (tierAllocation && (String(tierAllocation.tenantId) !== '10' || (!loadAllocation && !pool?.query)))
    throw Error('facility_research_tier_allocation_binding_required');
  let plan = null, ranking = null, tierRecord = null;
  const decisions = [];
  return {
    async plan(request, buildQueries) {
      if (String(request.tenantId) !== '10') throw Error('facility_research_tenant_mismatch');
      // Places also treats a top-level businessName as entity work and disables
      // market checkpoints for it. It cannot enter this one-page market planner.
      if (request.entity || request.businessName || request.scope === 'entity' || request.evidenceType !== 'identity')
        throw Object.assign(Error('facility_research_identity_only'), { code: 'facility_research_identity_only' });
      const missionSegment = CANONICAL_FACILITY_SEGMENTS.includes(request.segment)
        ? 'managed_facilities' : request.segment;
      if (!scope.eligibleSubsegments.includes(missionSegment)
        || request.geography?.cities?.some(c => !scope.cities.map(normalize).includes(normalize(c)))
        || (request.geography?.state && normalize(request.geography.state) !== 'nh'))
        throw Object.assign(Error('facility_research_request_scope_mismatch'), { code: 'facility_research_request_scope_mismatch' });
      if (plan) return []; // A cycle can contain repeated identity tasks, never a second allocation.
      let state = null, inert = null;
      if (tierAllocation) {
        const keys = [...new Set(Object.values(ANCHOR_FACILITY_TIER_VERSIONS).flatMap(v => [...versionUniverse(v, buildQueries).keys()])
          .concat(approvedQueryPlan(scope, generation, buildQueries).map(row => row.rootKey)))];
        state = loadAllocation ? await loadAllocation(keys) : await loadAllocationState(pool, tierAllocation, keys);
        inert = inertStatus(tierAllocation, state, scope, buildQueries);
        if (inert) tierRecord = { version: tierAllocation.version, status: inert };
      }
      const active = Boolean(tierAllocation) && !inert;
      plan = approvedQueryPlan(scope, generation, buildQueries, active ? tierAllocation : {});
      const keys = plan.flatMap(row => [row.rootKey, queryIdentity(row.query, true)]);
      const history = loadHistory ? await loadHistory(keys) : (await pool.query(`
        SELECT q.query_text,q.root_query_key,GREATEST(1,(SELECT count(*) FROM scout_query_pages p
          WHERE p.tenant_id=q.tenant_id AND p.search_fingerprint=q.search_fingerprint AND p.root_query_key=q.root_query_key))::int AS attempts
        FROM scout_tenant_query_owners q WHERE q.tenant_id='10' AND q.query_key=ANY($1::text[])
        UNION ALL
        SELECT NULL,p.root_query_key,count(*)::int FROM scout_query_pages p
          WHERE p.tenant_id='10' AND p.root_query_key=ANY($1::text[])
          AND NOT EXISTS (SELECT 1 FROM scout_tenant_query_owners q WHERE q.tenant_id=p.tenant_id AND q.root_query_key=p.root_query_key)
          GROUP BY p.root_query_key`, [keys])).rows;
      ranking = rankQueries(plan, history);
      if (active) {
        const tiered = tierOrdering(plan, ranking.ordered, tierAllocation, state, buildQueries);
        ranking = { ...ranking, ordered: tiered.orderedRows };
        tierRecord = tiered.record;
      }
      return ranking.ordered.map(row => row.query);
    },
    note(query, prior) {
      if (decisions.length >= 256) throw Error('facility_research_receipt_limit');
      const row = plan?.find(x => x.query === query);
      decisions.push({ query, category: row?.category || null, city: row?.city || null,
        ...(tierRecord?.status === 'active' ? { tier: tierAllocation.tierByCategory[row?.category] ?? null } : {}),
        status: prior?.handled ? 'skipped' : Array.isArray(prior?.candidates) ? 'replayed' : 'selected',
        reason: prior?.continuationStatus || (prior?.handled ? 'already_handled' : Array.isArray(prior?.candidates) ? 'durable_payload_reuse' : 'least_covered_available_query'),
        pageIndex: prior?.pageIndex ?? null });
    },
    snapshot() {
      const active = tierRecord?.status === 'active';
      let tier = tierRecord;
      if (active) {
        // Outcome is reported from what the lifecycle actually did; a fall-through
        // is a recorded redistribution, never counted as compliance.
        const selected = decisions.find(d => d.status === 'selected');
        const shortfall = selected ? decisions.slice(0, decisions.indexOf(selected))
          .filter(d => d.tier === tierRecord.intendedTier).map(d => d.reason) : null;
        tier = { ...tierRecord, selectedTier: selected?.tier ?? null,
          outcome: !selected ? 'no_new_selection' : selected.tier === tierRecord.intendedTier ? 'as_intended' : 'redistributed',
          ...(selected && selected.tier !== tierRecord.intendedTier ? { redistribution: { fromTier: tierRecord.intendedTier,
            skippedQueries: shortfall.length, reasons: [...new Set(shortfall)].sort() } } : {}) };
      }
      return { version: VERSION, maxNewPages: 1,
        selectionRationale: active ? 'tier allocation (anti-starvation, then 50/35/15 deficit), then least category attempts, town, concept, root attempts, deterministic order'
          : 'least category attempts, then town, concept, root attempts, then deterministic diagonal order',
        coverage: ranking?.counts || null, approvedQueries: plan?.length || 0,
        planned: (ranking?.ordered || []).map(({ query, category, city }) => ({ query, category, city })),
        decisions: decisions.slice(), ...(tier ? { tierAllocation: tier } : {}) };
    },
  };
}

module.exports = { VERSION, CATEGORIES, category, approvedQueryPlan, rankQueries, createFacilityResearchCoverage, loadAllocationState, versionUniverse, universeSha256 };
