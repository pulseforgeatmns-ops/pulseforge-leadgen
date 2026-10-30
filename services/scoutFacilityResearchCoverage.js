'use strict';

// Ordering only: reuses approved concepts, exact query identities and all native
// checkpoint/page/provider gates. No new authority, query reset, DDL or paid call.
const { expandConcepts } = require('../packages/scout/coverage/ConceptLibrary');
const { queryIdentity } = require('./scoutQueryCheckpoints');
const VERSION = 'anchor-facility-coverage-v1';
const normalize = value => String(value || '').toLowerCase().replace(/\s+/g, ' ').trim();
const CATEGORIES = ['school', 'campus', 'property_manager', 'manufacturer', 'warehouse', 'dealership', 'commercial_office', 'restaurant'];
function category(concept) {
  const s = normalize(concept);
  if (/school|academy/.test(s)) return 'school';
  if (/college|university|campus/.test(s)) return 'campus';
  if (/property manag/.test(s)) return 'property_manager';
  if (/manufactur|industrial facility/.test(s)) return 'manufacturer';
  if (/warehouse|distribution/.test(s)) return 'warehouse';
  if (/dealership|dealer group/.test(s)) return 'dealership';
  if (/office|business center/.test(s)) return 'commercial_office';
  if (/restaurant|cafe|bistro|tavern|diner|eatery|bar and grill|gastropub/.test(s)) return 'restaurant';
  return null;
}

function approvedQueryPlan(scope, generation, buildQueries) {
  const segments = scope.eligibleSubsegments || [];
  if (!segments.length || segments.some(x => !['managed_facilities', 'restaurant_foh'].includes(x)) || !scope.cities?.length)
    throw Error('facility_research_approved_scope_required');
  const concepts = segments.flatMap(segment => expandConcepts({ segments: [segment], businessNeed: 'commercial_cleaning', discoveryGeneration: generation })
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

function createFacilityResearchCoverage({ pool, tenantId, scope, generation = 0, loadHistory }) {
  if (String(tenantId) !== '10' || (!loadHistory && !pool?.query)) throw Error('facility_research_tenant_required');
  let plan = null, ranking = null;
  const decisions = [];
  return {
    async plan(request, buildQueries) {
      if (String(request.tenantId) !== '10') throw Error('facility_research_tenant_mismatch');
      if (request.entity || request.scope === 'entity' || request.evidenceType !== 'identity')
        throw Error('facility_research_identity_only');
      if (!scope.eligibleSubsegments.includes(request.segment)
        || request.geography?.cities?.some(c => !scope.cities.map(normalize).includes(normalize(c)))
        || (request.geography?.state && normalize(request.geography.state) !== 'nh'))
        throw Error('facility_research_request_scope_mismatch');
      if (plan) return []; // A cycle can contain repeated identity tasks, never a second allocation.
      plan = approvedQueryPlan(scope, generation, buildQueries);
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
      return ranking.ordered.map(row => row.query);
    },
    note(query, prior) {
      if (decisions.length >= 256) throw Error('facility_research_receipt_limit');
      const row = plan?.find(x => x.query === query);
      decisions.push({ query, category: row?.category || null, city: row?.city || null,
        status: prior?.handled ? 'skipped' : Array.isArray(prior?.candidates) ? 'replayed' : 'selected',
        reason: prior?.continuationStatus || (prior?.handled ? 'already_handled' : Array.isArray(prior?.candidates) ? 'durable_payload_reuse' : 'least_covered_available_query'),
        pageIndex: prior?.pageIndex ?? null });
    },
    snapshot() {
      return { version: VERSION, maxNewPages: 1, selectionRationale: 'least category attempts, then town, concept, root attempts, then deterministic diagonal order',
        coverage: ranking?.counts || null, approvedQueries: plan?.length || 0,
        planned: (ranking?.ordered || []).map(({ query, category, city }) => ({ query, category, city })),
        decisions: decisions.slice() };
    },
  };
}

module.exports = { VERSION, CATEGORIES, category, approvedQueryPlan, rankQueries, createFacilityResearchCoverage };
