/**
 * SPEC-224 -- Canonical Projection for Blueprint Backward Compatibility
 *
 * Wraps the existing SPEC-223C reconstructor (reconstructCanonicalSemanticProjection)
 * and derives a normalizedFacts-shaped view for legacy Blueprint consumers.
 * This module does NOT re-implement snapshot/entity/fact loading -- that
 * authority belongs solely to lib/canonicalSemanticProjection.js.
 */

const { reconstructCanonicalSemanticProjection } = require('./canonicalSemanticProjection');

function emptyNormalizedFacts() {
  return {
    business_name: '',
    services: undefined,
    ideal_customers: '',
    ideal_customers_role: '',
    ideal_customers_stage: '',
    ideal_customers_employee_range: '',
    ideal_customers_geography: '',
    avoid_customers: '',
    target_markets: '',
    differentiation: '',
    growth_focus: null,
    ninety_day_outcomes: undefined,
    business_facts: undefined,
  };
}

/**
 * @param {Object} input
 * @param {string} input.tenant_id
 * @param {string} input.snapshot_id
 * @param {Object} input.pool
 * @param {Object} [input.evaluation_context]
 * @returns {Promise<Object>} normalizedFacts-compatible object with
 *   _projection_metadata (version, source_snapshot_id, completeness, freshness)
 *   and _canonical_trace (entity/fact ids, unresolved fields).
 */
async function deriveBlueprintCompatibility(input) {
  const { tenant_id, snapshot_id, pool, evaluation_context } = input;
  const version = '1.0.0-spec-224-v1';

  let projection;
  try {
    projection = await reconstructCanonicalSemanticProjection(pool, {
      tenant_id,
      snapshot_id,
      evaluation_context,
    });
  } catch (err) {
    return {
      ...emptyNormalizedFacts(),
      _projection_metadata: {
        version,
        source_snapshot_id: snapshot_id,
        completeness: 'UNAVAILABLE',
        freshness: 'STALE',
        generated_at: new Date().toISOString(),
        error: err.message,
      },
    };
  }

  const entityById = new Map(projection.entities.map(e => [e.id, e]));
  const business = projection.entities.find(e => e.entity_type === 'BUSINESS');
  if (!business) {
    return {
      ...emptyNormalizedFacts(),
      _projection_metadata: {
        version,
        source_snapshot_id: snapshot_id,
        completeness: 'UNAVAILABLE',
        freshness: 'STALE',
        generated_at: new Date().toISOString(),
        error: 'No BUSINESS entity in snapshot',
      },
    };
  }

  const activeFacts = projection.facts.filter(f => f.active_at_evaluation);
  const factsBySubjectPredicate = new Map();
  for (const fact of activeFacts) {
    const key = `${fact.subject_entity_id}::${fact.predicate}`;
    const list = factsBySubjectPredicate.get(key) || [];
    list.push(fact);
    factsBySubjectPredicate.set(key, list);
  }
  function factsFor(subjectId, predicate) {
    return factsBySubjectPredicate.get(`${subjectId}::${predicate}`) || [];
  }
  function objectEntity(fact) {
    return fact.object_value?.type === 'ENTITY_REF' ? entityById.get(fact.object_value.value) : null;
  }
  function objectLiteral(fact) {
    return fact.object_value && fact.object_value.type !== 'ENTITY_REF' ? fact.object_value.value : null;
  }

  const unresolved = [];

  // business_name <- has_description(BUSINESS)
  let business_name = '';
  const descFacts = factsFor(business.id, 'has_description');
  if (descFacts[0]) {
    if (descFacts[0].epistemic_state !== 'KNOWN') unresolved.push('business_name');
    business_name = objectLiteral(descFacts[0]) || business.canonical_label || '';
  } else {
    business_name = business.canonical_label || '';
  }

  // services <- offers(BUSINESS) + contains_program(OFFER)
  const services = [];
  for (const offerFact of factsFor(business.id, 'offers')) {
    const offerEntity = objectEntity(offerFact);
    if (!offerEntity) continue;
    const variants = factsFor(offerEntity.id, 'contains_program')
      .map(pf => objectEntity(pf))
      .filter(Boolean)
      .map(programEntity => ({ name: programEntity.canonical_label || programEntity.identity_key }));
    services.push({
      name: offerEntity.canonical_label || offerEntity.identity_key,
      variants: variants.length ? variants : undefined,
    });
  }

  // ideal_customers <- targets_customer_profile(BUSINESS) + profile attributes
  let ideal_customers = '';
  let ideal_customers_role = '';
  let ideal_customers_stage = '';
  let ideal_customers_employee_range = '';
  let ideal_customers_geography = '';
  const targetProfileFact = factsFor(business.id, 'targets_customer_profile')[0];
  if (targetProfileFact) {
    const profileEntity = objectEntity(targetProfileFact);
    if (profileEntity) {
      ideal_customers = profileEntity.canonical_label || profileEntity.identity_key;

      const roleFact = factsFor(profileEntity.id, 'has_role')[0];
      if (roleFact) ideal_customers_role = objectLiteral(roleFact) || '';

      const stageFact = factsFor(profileEntity.id, 'has_business_stage')[0];
      if (stageFact) ideal_customers_stage = objectLiteral(stageFact) || '';

      const empFact = factsFor(profileEntity.id, 'has_employee_range')[0];
      if (empFact) {
        const range = objectLiteral(empFact);
        if (range && typeof range === 'object') {
          ideal_customers_employee_range = `${range.min}-${range.max}`;
        }
      }

      const geoFact = factsFor(profileEntity.id, 'has_geography')[0];
      if (geoFact) ideal_customers_geography = objectLiteral(geoFact) || '';
    }
  }

  // avoid_customers <- excludes_customer_profile(BUSINESS)
  let avoid_customers = '';
  const excludesFact = factsFor(business.id, 'excludes_customer_profile')[0];
  if (excludesFact) {
    const excludedEntity = objectEntity(excludesFact);
    if (excludedEntity) avoid_customers = excludedEntity.canonical_label || excludedEntity.identity_key;
  }

  // target_markets <- has_geography(BUSINESS, scope=service_area)
  let target_markets = '';
  const marketFact = factsFor(business.id, 'has_geography').find(f => f.qualifiers?.scope === 'service_area');
  if (marketFact) target_markets = objectLiteral(marketFact) || '';

  // differentiation <- has_buying_reason(BUSINESS)
  let differentiation = '';
  const buyingReasonFact = factsFor(business.id, 'has_buying_reason')[0];
  if (buyingReasonFact) {
    if (buyingReasonFact.epistemic_state !== 'KNOWN') unresolved.push('differentiation');
    differentiation = objectLiteral(buyingReasonFact) || '';
  }

  // ninety_day_outcomes <- targets_outcome(*) -> OUTCOME entities
  const ninety_day_outcomes = projection.entities
    .filter(e => e.entity_type === 'OUTCOME' && e.identity_key?.includes('90day'))
    .map(e => ({ name: e.canonical_label || e.identity_key }));

  // growth_focus, business_facts -> UNREPRESENTABLE per SPEC-224 (no BUSINESS-level canonical predicate)
  const growth_focus = null;
  const business_facts = [];

  const requiredFields = [business_name, services.length > 0, ideal_customers, target_markets, differentiation];
  const completeness = requiredFields.every(Boolean) ? 'COMPLETE' : 'PARTIAL';

  let freshness = 'STALE';
  try {
    const client = await pool.connect();
    try {
      const active = await client.query(
        `SELECT id FROM canonical_business_snapshots
         WHERE tenant_id = $1 ORDER BY created_at DESC, id DESC LIMIT 1`,
        [tenant_id]
      );
      if (active.rows[0] && active.rows[0].id === projection.snapshot_id) freshness = 'CURRENT';
    } finally {
      client.release();
    }
  } catch (_) {
    /* freshness check is best-effort */
  }

  return {
    business_name,
    services: services.length ? services : undefined,
    ideal_customers,
    ideal_customers_role,
    ideal_customers_stage,
    ideal_customers_employee_range,
    ideal_customers_geography,
    avoid_customers,
    target_markets,
    differentiation,
    growth_focus,
    ninety_day_outcomes: ninety_day_outcomes.length ? ninety_day_outcomes : undefined,
    business_facts: business_facts.length ? business_facts : undefined,
    _projection_metadata: {
      version,
      source_snapshot_id: projection.snapshot_id,
      semantic_model_version: 1,
      registry_version: projection.registry_version,
      completeness,
      freshness,
      generated_at: new Date().toISOString(),
    },
    _canonical_trace: {
      entity_ids: projection.entities.map(e => e.id),
      fact_ids: projection.facts.map(f => f.id),
      unresolved_fields: unresolved.length ? unresolved : undefined,
    },
  };
}

module.exports = { deriveBlueprintCompatibility, emptyNormalizedFacts };
