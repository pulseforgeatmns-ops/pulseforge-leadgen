'use strict';

const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { after, before, describe, it } = require('node:test');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const {
  canonicalJsonString,
  commitCanonicalSemanticBatch,
  deriveInterpretationBatchKey,
} = require('../lib/canonicalSemanticWrite');
const { reconstructCanonicalSemanticProjection } = require('../lib/canonicalSemanticProjection');

const migration = fs.readFileSync(path.join(__dirname, '..', 'migrations',
  '2026-09-01-spec-223a-canonical-semantic-persistence.sql'), 'utf8');

const ENTITY_TYPES = ['BUSINESS', 'OFFER', 'PROGRAM', 'CUSTOMER_PROFILE', 'PAIN',
  'CAPABILITY', 'OUTCOME', 'OBJECTIVE', 'METRIC'];

const BABRUN_EVIDENCE = {
  identity: `The business is Babrun. We are building coaching and transformation programs for owners of small, founder-led businesses.

Today, the primary offer is a 12-week 1:1 coaching program focused on one of three areas: management and people, sales and customers, or the business idea/business model.

The goal is practical transformation rather than just education. The founder learns new capabilities while applying them directly inside their actual business.

Right now, we're in a market-validation stage in the U.S. The 1:1 model lets us learn which pains are strongest, what founders are willing to pay to solve, which offers convert, what objections arise, and what pricing the market accepts. Longer term, the intention is to use what we learn to develop scalable group transformation programs.`,
  services: `Today, the primary service is 12-week 1:1 coaching for small-business founders.

There are three main coaching programs:

Management / People: helping founders delegate effectively, manage employees, identify performance problems earlier, reduce the amount of work that comes back to the founder, and build a business that depends less on them personally.

Sales / Customers: helping founders better understand customer needs and buying decisions, sell around value rather than features, move opportunities forward, pursue larger or more valuable customers, and develop a more repeatable sales process.

Product / Business Idea: helping founders think more like entrepreneurs rather than functional experts - identifying higher-value opportunities, stronger differentiation, better economics, growth potential, and potentially a stronger business model.

For now, these are delivered 1:1. The longer-term direction is to develop group-based 12-week transformation programs once the market and offers are validated.`,
  customer: `Our ideal customer is the owner or founder of an operating small business in the United States, generally with 1-10 employees.

Initially, we want to focus on service businesses, especially businesses where results depend heavily on employees and their behavior. We don't need to restrict ourselves to one specific service vertical yet.

More important than the exact industry is the presence of a specific pattern of pain.

That could include problems with employees, having to constantly supervise or do everything themselves, being unable to step away from the business, burnout, inconsistent customers or sales, poor-quality customers, weak profit or cash flow, or feeling that the amount of work they're putting into the business isn't producing enough financial return.

The underlying pattern we're interested in is a founder who has built a real operating business, but whose capabilities as a manager, salesperson, or entrepreneur haven't necessarily evolved as quickly as the business itself.`,
};

const PREDICATES = {
  offers: entityPredicate(['BUSINESS'], ['OFFER'], 'SET'),
  contains_program: entityPredicate(['OFFER'], ['PROGRAM'], 'SET'),
  has_delivery_mode: literalPredicate(['OFFER', 'PROGRAM'], ['DELIVERY_MODE'], 'SET'),
  targets_customer_profile: entityPredicate(['BUSINESS', 'OFFER', 'PROGRAM'], ['CUSTOMER_PROFILE'], 'SET'),
  teaches_capability: entityPredicate(['PROGRAM', 'OFFER'], ['CAPABILITY'], 'SET'),
  targets_outcome: entityPredicate(['PROGRAM', 'OFFER', 'OBJECTIVE'], ['OUTCOME'], 'SET'),
  addresses_pain: entityPredicate(['PROGRAM', 'OFFER', 'CUSTOMER_PROFILE'], ['PAIN'], 'SET'),
  has_role: literalPredicate(['CUSTOMER_PROFILE'], ['ROLE'], 'SET'),
  has_business_stage: literalPredicate(['CUSTOMER_PROFILE', 'BUSINESS'], ['STAGE'], 'SINGLE'),
  has_characteristic: literalPredicate(['CUSTOMER_PROFILE', 'BUSINESS'], ['CONCEPT'], 'SET'),
  has_geography: literalPredicate(['CUSTOMER_PROFILE', 'BUSINESS', 'OBJECTIVE'], ['GEOGRAPHY'], 'SET'),
  has_employee_range: literalPredicate(['CUSTOMER_PROFILE', 'BUSINESS'], ['INTEGER_RANGE'], 'SINGLE'),
  has_vertical: literalPredicate(['CUSTOMER_PROFILE', 'BUSINESS', 'OBJECTIVE'], ['VERTICAL'], 'SET'),
  has_description: literalPredicate(ENTITY_TYPES, ['SEMANTIC_TEXT'], 'SINGLE'),
  has_buying_reason: literalPredicate(['BUSINESS', 'OFFER'], ['CONCEPT'], 'SET'),
  has_brand_voice: literalPredicate(['BUSINESS'], ['BRAND_DIRECTION'], 'SET'),
  avoids_brand_trait: literalPredicate(['BUSINESS'], ['BRAND_TRAIT'], 'SET'),
  has_validation_status: literalPredicate(['BUSINESS', 'OFFER', 'CUSTOMER_PROFILE', 'OBJECTIVE'], ['BOOLEAN'], 'SINGLE'),
};

function entityPredicate(domain, entityTypes, cardinality) {
  return { domain, range: { kind: 'ENTITY', entity_types: entityTypes }, cardinality };
}

function literalPredicate(domain, literalTypes, cardinality) {
  return { domain, range: { kind: 'LITERAL', literal_types: literalTypes }, cardinality };
}

function entity(type, identity, label = null) {
  return { type, identity, label };
}

function literal(type, value) {
  return { type, value };
}

function ref(identity) {
  return { type: 'ENTITY_REF', value: identity };
}

function proposition(id, subject, predicate, object, options = {}) {
  return {
    id, subject, predicate, object,
    epistemic_state: 'KNOWN', temporal_status: 'CURRENT', modality: 'ACTUAL',
    qualifiers: {}, evidence: 'identity', ...options,
  };
}

function expectedBabrunManifest() {
  const entities = [
    entity('BUSINESS', 'business:babrun', 'Babrun'),
    entity('OFFER', 'offer:coaching_12_week_1_to_1', '12-week 1:1 coaching'),
    entity('OFFER', 'offer:group_transformation_programs', 'Group transformation programs'),
    entity('PROGRAM', 'program:management_people', 'Management / People'),
    entity('PROGRAM', 'program:sales_customers', 'Sales / Customers'),
    entity('PROGRAM', 'program:product_business_idea', 'Product / Business Idea'),
    entity('CUSTOMER_PROFILE', 'profile:founder_led_small_business_validation', 'Founder-led small business validation'),
  ];
  for (const identity of ['employee_problems', 'constant_supervision_and_founder_dependency',
    'inability_to_step_away', 'burnout', 'inconsistent_sales_or_customers', 'poor_quality_customers',
    'weak_profit_or_cash_flow', 'insufficient_return_for_effort']) {
    entities.push(entity('PAIN', `pain:${identity}`, identity));
  }
  for (const identity of ['delegation', 'employee_management', 'early_performance_diagnosis',
    'customer_need_understanding', 'value_based_selling', 'opportunity_progression',
    'entrepreneurial_opportunity_identification']) {
    entities.push(entity('CAPABILITY', `capability:${identity}`, identity));
  }
  for (const identity of ['practical_transformation_applied_in_the_founders_business',
    'reduced_founder_dependency', 'repeatable_sales_process', 'higher_value_customers',
    'stronger_differentiation', 'better_economics', 'stronger_business_model']) {
    entities.push(entity('OUTCOME', `outcome:${identity}`, identity));
  }

  const facts = [
    proposition('current-offer', 'business:babrun', 'offers', ref('offer:coaching_12_week_1_to_1')),
    proposition('delivery', 'offer:coaching_12_week_1_to_1', 'has_delivery_mode', literal('DELIVERY_MODE', '1:1'), { evidence: 'services' }),
    proposition('offer-description', 'offer:coaching_12_week_1_to_1', 'has_description', literal('SEMANTIC_TEXT', '12-week coaching for small-business founders'), { evidence: 'services' }),
    proposition('target-profile', 'offer:coaching_12_week_1_to_1', 'targets_customer_profile', ref('profile:founder_led_small_business_validation'), { evidence: 'services' }),
    ...['management_people', 'sales_customers', 'product_business_idea'].map(identity =>
      proposition(`contains-${identity}`, 'offer:coaching_12_week_1_to_1', 'contains_program', ref(`program:${identity}`), { evidence: 'services' })),
    proposition('practical-outcome', 'offer:coaching_12_week_1_to_1', 'targets_outcome', ref('outcome:practical_transformation_applied_in_the_founders_business')),
    proposition('management-description', 'program:management_people', 'has_description', literal('SEMANTIC_TEXT', 'Management / People'), { evidence: 'services' }),
    ...['delegation', 'employee_management', 'early_performance_diagnosis'].map(identity =>
      proposition(`management-${identity}`, 'program:management_people', 'teaches_capability', ref(`capability:${identity}`), { evidence: 'services' })),
    proposition('management-outcome', 'program:management_people', 'targets_outcome', ref('outcome:reduced_founder_dependency'), { evidence: 'services' }),
    proposition('sales-description', 'program:sales_customers', 'has_description', literal('SEMANTIC_TEXT', 'Sales / Customers'), { evidence: 'services' }),
    ...['customer_need_understanding', 'value_based_selling', 'opportunity_progression'].map(identity =>
      proposition(`sales-${identity}`, 'program:sales_customers', 'teaches_capability', ref(`capability:${identity}`), { evidence: 'services' })),
    ...['repeatable_sales_process', 'higher_value_customers'].map(identity =>
      proposition(`sales-${identity}`, 'program:sales_customers', 'targets_outcome', ref(`outcome:${identity}`), { evidence: 'services' })),
    proposition('product-description', 'program:product_business_idea', 'has_description', literal('SEMANTIC_TEXT', 'Product / Business Idea'), { evidence: 'services' }),
    proposition('product-capability', 'program:product_business_idea', 'teaches_capability', ref('capability:entrepreneurial_opportunity_identification'), { evidence: 'services' }),
    ...['stronger_differentiation', 'better_economics', 'stronger_business_model'].map(identity =>
      proposition(`product-${identity}`, 'program:product_business_idea', 'targets_outcome', ref(`outcome:${identity}`), { evidence: 'services' })),
    proposition('profile-role', 'profile:founder_led_small_business_validation', 'has_role', literal('ROLE', 'owner/founder'), { evidence: 'customer' }),
    proposition('profile-stage', 'profile:founder_led_small_business_validation', 'has_business_stage', literal('STAGE', 'operating'), { evidence: 'customer' }),
    proposition('profile-geography', 'profile:founder_led_small_business_validation', 'has_geography', literal('GEOGRAPHY', 'United States'), { evidence: 'customer' }),
    proposition('profile-size', 'profile:founder_led_small_business_validation', 'has_employee_range', literal('INTEGER_RANGE', { min: 1, max: 10, unit: 'employees' }), { evidence: 'customer', qualifiers: { generally: true } }),
    proposition('profile-vertical', 'profile:founder_led_small_business_validation', 'has_vertical', literal('VERTICAL', 'service businesses'), { evidence: 'customer', epistemic_state: 'HYPOTHESIS', modality: 'INTENDED', qualifiers: { validation_scope: true } }),
    proposition('profile-employee-characteristic', 'profile:founder_led_small_business_validation', 'has_characteristic', literal('CONCEPT', 'results depend heavily on employee behavior'), { evidence: 'customer', epistemic_state: 'HYPOTHESIS', modality: 'INTENDED', qualifiers: { validation_scope: true } }),
    ...['employee_problems', 'constant_supervision_and_founder_dependency', 'inability_to_step_away',
      'burnout', 'inconsistent_sales_or_customers', 'poor_quality_customers', 'weak_profit_or_cash_flow',
      'insufficient_return_for_effort'].map(identity => proposition(`profile-pain-${identity}`,
      'profile:founder_led_small_business_validation', 'addresses_pain', ref(`pain:${identity}`),
      { evidence: 'customer', epistemic_state: 'HYPOTHESIS', modality: 'INTENDED', qualifiers: { validation_scope: true } })),
    proposition('profile-capability-gap', 'profile:founder_led_small_business_validation', 'has_characteristic',
      literal('CONCEPT', 'management, sales, or entrepreneurial capability has not evolved with the business'), { evidence: 'customer' }),
    proposition('business-stage', 'business:babrun', 'has_business_stage', literal('STAGE', 'U.S. market validation')),
    proposition('market-validation-complete', 'business:babrun', 'has_validation_status', null, { epistemic_state: 'UNKNOWN' }),
    proposition('planned-offer', 'business:babrun', 'offers', ref('offer:group_transformation_programs'), {
      evidence: 'services', temporal_status: 'PLANNED', modality: 'CONDITIONAL', condition: 'market-validation-complete',
    }),
    proposition('actual-buying-reason', 'business:babrun', 'has_buying_reason', null, { epistemic_state: 'UNKNOWN', evidence: 'customer' }),
    proposition('candidate-buying-reason', 'business:babrun', 'has_buying_reason', literal('CONCEPT', 'practical transformation may influence buying'), { epistemic_state: 'HYPOTHESIS', modality: 'INTENDED', evidence: 'customer' }),
    proposition('established-brand-voice', 'business:babrun', 'has_brand_voice', null, { epistemic_state: 'UNKNOWN', evidence: 'customer' }),
    proposition('candidate-brand-voice', 'business:babrun', 'has_brand_voice', literal('BRAND_DIRECTION', 'practical, direct, founder-to-founder, outcome-focused'), { epistemic_state: 'HYPOTHESIS', modality: 'INTENDED', evidence: 'customer' }),
    ...['overly academic language', 'generic motivational-coaching language', 'stereotypical business-guru tone'].map((value, index) =>
      proposition(`avoided-brand-${index}`, 'business:babrun', 'avoids_brand_trait', literal('BRAND_TRAIT', value), { evidence: 'customer' })),
  ];
  return { domain_business: 'business:babrun', entities, facts };
}

function semanticFact(fact, factIndex) {
  return {
    subject_entity_identity_key: fact.subject,
    predicate: fact.predicate,
    object_value: fact.object,
    qualifiers: fact.qualifiers,
    epistemic_state: fact.epistemic_state,
    epistemic_confidence: fact.epistemic_state === 'KNOWN' ? 1 : null,
    epistemic_calibration_version: 'spec-222',
    interpretation_confidence: 1,
    interpretation_calibration_version: 'spec-223d',
    temporal_status: fact.temporal_status,
    valid_from: fact.valid_from || null,
    valid_to: fact.valid_to || null,
    modality: fact.modality,
    condition_refs: fact.condition == null ? [] : [{
      fact_index: factIndex.get(fact.condition), resolution: 'CURRENT_IN_CONFLICT_SET',
    }],
  };
}

function manifestToBatch(manifest, registry, evidenceByKey) {
  const factIndex = new Map(manifest.facts.map((fact, index) => [fact.id, index]));
  const evidenceInputs = [...evidenceByKey.values()];
  const input = {
    tenant_id: 'tenant:babrun', registry_artifact_id: registry.id,
    registry_version: registry.registry_version, registry_content_digest: registry.content_digest,
    interpreter_id: 'spec-223d-oracle-adapter', interpreter_version: '1.0.0', semantic_model_version: 1,
    ordered_evidence_input_ids: evidenceInputs.map(item => item.id),
    semantic_entities: manifest.entities.map(item => ({ entity_type: item.type, identity_key: item.identity,
      ...(item.type === 'BUSINESS' ? { domain_client_id: 1 } : {}) })),
    label_assertions: manifest.entities.filter(item => item.label).map(item => ({
      entity_identity_key: item.identity, label: item.label, assertion_kind: 'CANONICAL',
      evidence_id: evidenceByKey.get('identity').id,
    })),
    semantic_facts: manifest.facts.map(fact => semanticFact(fact, factIndex)),
    fact_evidence_links: manifest.facts.map((fact, index) => {
      const evidence = evidenceByKey.get(fact.evidence);
      return { fact_index: index, evidence_id: evidence.id, source_text_sha256: evidence.source_text_sha256,
        span_start_utf16: 0, span_end_utf16: evidence.statement.length, support_type: 'DIRECT' };
    }),
    fact_relations: [], entity_merge_events: [], conflict_set_resolutions: [], snapshot_metadata: {},
  };
  input.idempotency_key = deriveInterpretationBatchKey(input,
    new Map(evidenceInputs.map(item => [item.id, item])));
  return input;
}

function normalizeExpected(manifest) {
  return {
    domain_business: manifest.domain_business,
    entities: manifest.entities.map(({ type, identity, label }) => ({ type, identity, label })).sort(byJson),
    facts: manifest.facts.map(fact => ({ id: fact.id, subject: fact.subject, predicate: fact.predicate,
      object: fact.object, epistemic_state: fact.epistemic_state, temporal_status: fact.temporal_status,
      modality: fact.modality, qualifiers: fact.qualifiers, condition: fact.condition || null,
      evidence_count: 1 })).sort(byJson),
  };
}

function normalizeProjection(projection, manifest) {
  const identityByEntityId = new Map(projection.entities.map(item => [item.id, item.identity_key]));
  const expectedBySemanticKey = new Map(manifest.facts.map(fact => [semanticKey(fact), fact.id]));
  const factIdBySemanticId = new Map();
  for (const fact of projection.facts) {
    const normalized = normalizeProjectedFact(fact, identityByEntityId);
    factIdBySemanticId.set(expectedBySemanticKey.get(semanticKey(normalized)), fact.id);
  }
  const semanticIdByFactId = new Map([...factIdBySemanticId].map(([semanticId, factId]) => [factId, semanticId]));
  const evidenceCount = new Map();
  for (const evidence of projection.evidence_references) {
    const semanticId = semanticIdByFactId.get(evidence.fact_id);
    evidenceCount.set(semanticId, (evidenceCount.get(semanticId) || 0) + 1);
  }
  return {
    domain_business: projection.entities.find(item => item.entity_type === 'BUSINESS').identity_key,
    entities: projection.entities.map(item => ({ type: item.entity_type, identity: item.identity_key,
      label: item.canonical_label })).sort(byJson),
    facts: projection.facts.map(fact => {
      const normalized = normalizeProjectedFact(fact, identityByEntityId);
      const id = expectedBySemanticKey.get(semanticKey(normalized));
      const conditionRef = fact.qualifiers.condition_refs?.[0];
      return { id, ...normalized, qualifiers: stripConditionRefs(normalized.qualifiers),
        condition: conditionRef ? semanticIdByFactId.get(conditionRef.fact_id) : null,
        evidence_count: evidenceCount.get(id) || 0 };
    }).sort(byJson),
  };
}

function normalizeProjectedFact(fact, identityByEntityId) {
  const object = fact.object_value?.type === 'ENTITY_REF'
    ? ref(identityByEntityId.get(fact.object_value.value)) : fact.object_value;
  return { subject: identityByEntityId.get(fact.subject_entity_id), predicate: fact.predicate, object,
    epistemic_state: fact.epistemic_state, temporal_status: fact.temporal_status,
    modality: fact.modality, qualifiers: fact.qualifiers };
}

function stripConditionRefs(qualifiers) {
  const { condition_refs: ignored, ...semanticQualifiers } = qualifiers;
  return semanticQualifiers;
}

function semanticKey(fact) {
  return canonicalJsonString({ subject: fact.subject, predicate: fact.predicate, object: fact.object,
    epistemic_state: fact.epistemic_state, temporal_status: fact.temporal_status,
    modality: fact.modality, qualifiers: stripConditionRefs(fact.qualifiers || {}) });
}

function byJson(left, right) {
  return JSON.stringify(left).localeCompare(JSON.stringify(right));
}

function semanticDiff(expected, actual) {
  try {
    assert.deepEqual(actual, expected);
    return null;
  } catch (error) {
    return error.message;
  }
}

describe('SPEC-223D canonical semantic Babrun round-trip acceptance', () => {
  let postgres;
  let pool;
  let registryV1;
  let registryV2;
  let evidenceByKey;
  let sessionId;
  let baselineManifest;
  let baselineBatch;
  let baselineCommit;
  let baselineProjection;

  async function addRegistry(version, predicates = PREDICATES) {
    const digest = (await pool.query(`SELECT encode(digest(jsonb_build_object(
      'entity_vocabulary',$1::jsonb,'predicate_definitions',$2::jsonb,
      'registry_version',$3::text)::text,'sha256'),'hex') AS digest`,
    [JSON.stringify(ENTITY_TYPES), JSON.stringify(predicates), version])).rows[0].digest;
    return (await pool.query(`INSERT INTO canonical_registry_artifacts
      (registry_version,entity_vocabulary,predicate_definitions,content_digest)
      VALUES ($1,$2::jsonb,$3::jsonb,$4) RETURNING *`,
    [version, JSON.stringify(ENTITY_TYPES), JSON.stringify(predicates), digest])).rows[0];
  }

  async function addEvidence(category, statement) {
    return (await pool.query(`INSERT INTO cie_evidence
      (client_id,session_id,category,statement,source_text_sha256,immutable_at)
      VALUES (1,$1,$2,$3,encode(digest($3,'sha256'),'hex'),NOW())
      RETURNING id,statement,source_text_sha256`, [sessionId, category, statement])).rows[0];
  }

  async function commitChange({ statement, registry = registryV1, entities = [], facts = [],
    labels = [], relations = [], merges = [] }) {
    const evidence = await addEvidence('acceptance', statement);
    const factIndex = new Map(facts.map((fact, index) => [fact.id, index]));
    const input = {
      tenant_id: 'tenant:babrun', registry_artifact_id: registry.id,
      registry_version: registry.registry_version, registry_content_digest: registry.content_digest,
      interpreter_id: 'spec-223d-scenario', interpreter_version: '1.0.0', semantic_model_version: 1,
      ordered_evidence_input_ids: [evidence.id],
      semantic_entities: [{ entity_type: 'BUSINESS', identity_key: 'business:babrun', domain_client_id: 1 },
        ...entities.map(item => ({ entity_type: item.type, identity_key: item.identity }))],
      label_assertions: labels.map(label => ({ ...label, evidence_id: evidence.id })),
      semantic_facts: facts.map(fact => semanticFact({ evidence: 'change', ...fact }, factIndex)),
      fact_evidence_links: facts.map((fact, index) => ({ fact_index: index, evidence_id: evidence.id,
        source_text_sha256: evidence.source_text_sha256, span_start_utf16: 0,
        span_end_utf16: evidence.statement.length, support_type: 'DIRECT' })),
      fact_relations: relations, entity_merge_events: merges.map(merge => ({ ...merge, evidence_id: evidence.id })),
      conflict_set_resolutions: [], snapshot_metadata: {},
    };
    input.idempotency_key = deriveInterpretationBatchKey(input, new Map([[evidence.id, evidence]]));
    return commitCanonicalSemanticBatch(pool, input);
  }

  async function project(snapshotId, evaluationContext) {
    return reconstructCanonicalSemanticProjection(pool, { tenant_id: 'tenant:babrun',
      snapshot_id: snapshotId, ...(evaluationContext ? { evaluation_context: evaluationContext } : {}) });
  }

  function projectedFact(projection, predicate, subjectIdentity, objectValue) {
    const entityById = new Map(projection.entities.map(item => [item.id, item.identity_key]));
    return projection.facts.find(fact => fact.predicate === predicate
      && entityById.get(fact.subject_entity_id) === subjectIdentity
      && (objectValue === undefined || fact.object_value?.value === objectValue));
  }

  before(async () => {
    postgres = await startDisposablePostgres('spec-223d-pg-');
    pool = new Pool({ connectionString: postgres.connectionString });
    await pool.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
      CREATE TABLE clients (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
      CREATE TABLE tenant_workspaces (client_id INTEGER PRIMARY KEY REFERENCES clients(id), tenant_key TEXT NOT NULL UNIQUE);
      CREATE TABLE cie_interview_sessions (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL);
      CREATE TABLE cie_evidence (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL,
        session_id UUID NOT NULL REFERENCES cie_interview_sessions(id), category TEXT NOT NULL, statement TEXT NOT NULL);
      CREATE TABLE "normalizedFacts" (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), payload JSONB);
      CREATE TABLE business_facts (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), payload JSONB);
      CREATE TABLE interview_state (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), payload JSONB);
      CREATE TABLE blueprints (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), payload JSONB);
      CREATE TABLE knowledge_nodes (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), payload JSONB);
      INSERT INTO clients VALUES (1,'Babrun');
      INSERT INTO tenant_workspaces VALUES (1,'tenant:babrun');`);
    sessionId = (await pool.query('INSERT INTO cie_interview_sessions(client_id) VALUES (1) RETURNING id')).rows[0].id;
    await pool.query(migration);
    registryV1 = await addRegistry('v1');
    evidenceByKey = new Map();
    for (const [key, statement] of Object.entries(BABRUN_EVIDENCE)) {
      evidenceByKey.set(key, await addEvidence(key, statement));
    }
  });

  after(async () => {
    await pool.end();
    await postgres.stop();
  });

  it('A. round-trips the complete SPEC-222 Babrun graph without semantic loss', async () => {
    baselineManifest = expectedBabrunManifest();
    baselineBatch = manifestToBatch(baselineManifest, registryV1, evidenceByKey);
    baselineCommit = await commitCanonicalSemanticBatch(pool, baselineBatch);
    baselineProjection = await project(baselineCommit.snapshot_id);
    const expected = normalizeExpected(baselineManifest);
    const actual = normalizeProjection(baselineProjection, baselineManifest);
    assert.equal(semanticDiff(expected, actual), null);
  });

  it('B. replays without duplicate canonical state', async () => {
    const before = await pool.query(`SELECT
      (SELECT count(*)::int FROM canonical_business_entities) AS entities,
      (SELECT count(*)::int FROM canonical_business_facts) AS facts,
      (SELECT count(*)::int FROM canonical_business_snapshots) AS snapshots`);
    const replay = await commitCanonicalSemanticBatch(pool, baselineBatch);
    assert.equal(replay.replayed, true);
    assert.equal(replay.snapshot_id, baselineCommit.snapshot_id);
    assert.deepEqual((await pool.query(`SELECT
      (SELECT count(*)::int FROM canonical_business_entities) AS entities,
      (SELECT count(*)::int FROM canonical_business_facts) AS facts,
      (SELECT count(*)::int FROM canonical_business_snapshots) AS snapshots`)).rows, before.rows);
    assert.deepEqual(normalizeProjection(await project(replay.snapshot_id), baselineManifest),
      normalizeExpected(baselineManifest));
  });

  it('C. expands evidence without duplicating proposition identity', async () => {
    const currentOffer = baselineManifest.facts.find(fact => fact.id === 'current-offer');
    const result = await commitChange({ statement: 'Babrun still offers its 12-week 1:1 coaching program.',
      entities: [entity('OFFER', 'offer:coaching_12_week_1_to_1')], facts: [currentOffer] });
    assert.equal(result.fact_ids_created.length, 0);
    assert.equal(result.fact_ids_reused.length, 1);
    const projection = await project(result.snapshot_id);
    const currentOfferId = projection.entities.find(item =>
      item.identity_key === 'offer:coaching_12_week_1_to_1').id;
    const offerFact = projectedFact(projection, 'offers', 'business:babrun', currentOfferId);
    assert.equal(projection.evidence_references.filter(item => item.fact_id === offerFact.id).length, 2);
    assert.equal(projection.facts.filter(item => item.id === offerFact.id).length, 1);
  });

  it('D. evolves labels while preserving old snapshot labels', async () => {
    const result = await commitChange({ statement: 'The current formal label is Babrun LLC.',
      labels: [{ entity_identity_key: 'business:babrun', label: 'Babrun LLC', assertion_kind: 'CANONICAL' }] });
    const oldLabel = (await project(baselineCommit.snapshot_id)).entities
      .find(item => item.entity_type === 'BUSINESS').canonical_label;
    const newLabel = (await project(result.snapshot_id)).entities
      .find(item => item.entity_type === 'BUSINESS').canonical_label;
    const labelRows = (await pool.query(`SELECT label,created_at::text,created_by_batch_id
      FROM canonical_entity_label_assertions WHERE label IN ('Babrun','Babrun LLC')
      ORDER BY created_at,created_by_batch_id`)).rows;
    assert.equal(oldLabel, 'Babrun');
    assert.equal(newLabel, 'Babrun LLC', `canonical label assertions: ${JSON.stringify(labelRows)}`);
  });

  it('E. keeps the conditional group offer planned rather than current', () => {
    const planned = projectedFact(baselineProjection, 'offers', 'business:babrun',
      baselineProjection.entities.find(item => item.identity_key === 'offer:group_transformation_programs').id);
    assert.equal(planned.temporal_status, 'PLANNED');
    assert.equal(planned.modality, 'CONDITIONAL');
    assert.equal(baselineProjection.facts.filter(fact => fact.predicate === 'offers'
      && fact.temporal_status === 'CURRENT' && fact.modality === 'ACTUAL').length, 1);
    assert.equal(planned.qualifiers.condition_refs.length, 1);
  });

  it('F. preserves KNOWN, HYPOTHESIS, and UNKNOWN as distinct states', () => {
    const states = new Set(baselineProjection.facts.map(fact => fact.epistemic_state));
    assert.deepEqual([...states].sort(), ['HYPOTHESIS', 'KNOWN', 'UNKNOWN']);
    assert.equal(projectedFact(baselineProjection, 'has_validation_status', 'business:babrun').object_value, null);
  });

  it('G. preserves customer-profile-scoped facts', () => {
    const entityById = new Map(baselineProjection.entities.map(item => [item.id, item.identity_key]));
    const scoped = baselineProjection.facts.filter(fact => ['addresses_pain', 'has_vertical'].includes(fact.predicate));
    assert.equal(scoped.length, 9);
    assert.ok(scoped.every(fact => entityById.get(fact.subject_entity_id)
      === 'profile:founder_led_small_business_validation'));
  });

  it('H. keeps differentiation separate from brand semantics', () => {
    const differentiation = projectedFact(baselineProjection, 'targets_outcome',
      'program:product_business_idea', baselineProjection.entities.find(item => item.identity_key
        === 'outcome:stronger_differentiation').id);
    assert.ok(differentiation);
    assert.equal(baselineProjection.facts.filter(fact => ['has_brand_voice', 'avoids_brand_trait'].includes(fact.predicate)).length, 5);
    assert.notEqual(differentiation.predicate, 'has_brand_voice');
  });

  it('I. reconstructs a v1 snapshot identically after registry v2 installation', async () => {
    registryV2 = await addRegistry('v2', { ...PREDICATES,
      has_description: literalPredicate(ENTITY_TYPES, ['SEMANTIC_TEXT'], 'SET') });
    const reconstructed = await project(baselineCommit.snapshot_id);
    assert.equal(reconstructed.registry_version, 'v1');
    assert.equal(reconstructed.projection_digest, baselineProjection.projection_digest);
  });

  it('J. selects one SINGLE fact and preserves compatible SET facts', () => {
    const stage = projectedFact(baselineProjection, 'has_business_stage', 'business:babrun');
    const offers = baselineProjection.facts.filter(fact => fact.predicate === 'offers');
    assert.equal(stage.selected_in_conflict, true);
    assert.equal(baselineProjection.conflict_resolutions.find(item =>
      item.conflict_set_id === stage.conflict_set_id).active_fact_ids.length, 1);
    assert.equal(offers.length, 2);
    assert.ok(offers.every(fact => fact.selected_in_conflict));
  });

  it('K. preserves A to contradictory B to superseding C history', async () => {
    const objective = entity('OBJECTIVE', 'objective:positioning');
    const factA = proposition('correction-a', objective.identity, 'has_description',
      literal('SEMANTIC_TEXT', 'Positioning A'));
    const a = await commitChange({ statement: 'Positioning is A.', entities: [objective], facts: [factA] });
    const factB = proposition('correction-b', objective.identity, 'has_description',
      literal('SEMANTIC_TEXT', 'Positioning B'));
    const b = await commitChange({ statement: 'Correction: positioning is B.', entities: [objective], facts: [factB],
      relations: [{ from_fact_index: 0, to_fact_id: a.fact_ids_created[0], relation_type: 'CORRECTION_OF' }] });
    const factC = proposition('correction-c', objective.identity, 'has_description',
      literal('SEMANTIC_TEXT', 'Positioning C'));
    const c = await commitChange({ statement: 'Positioning C supersedes B.', entities: [objective], facts: [factC],
      relations: [{ from_fact_index: 0, to_fact_id: b.fact_ids_created[0], relation_type: 'SUPERSEDES' }] });
    assert.equal(projectedFact(await project(a.snapshot_id), 'has_description', objective.identity).object_value.value, 'Positioning A');
    assert.equal(projectedFact(await project(b.snapshot_id), 'has_description', objective.identity, 'Positioning B').selected_in_conflict, true);
    assert.equal(projectedFact(await project(c.snapshot_id), 'has_description', objective.identity, 'Positioning C').selected_in_conflict, true);
  });

  it('L. reclassifies compatible facts only in a new registry snapshot', async () => {
    const objective = entity('OBJECTIVE', 'objective:positioning');
    const result = await commitChange({ statement: 'Positioning A and C are compatible descriptions.', registry: registryV2,
      entities: [objective], facts: [
        proposition('compatible-a', objective.identity, 'has_description', literal('SEMANTIC_TEXT', 'Positioning A')),
        proposition('compatible-c', objective.identity, 'has_description', literal('SEMANTIC_TEXT', 'Positioning C')),
      ] });
    const projection = await project(result.snapshot_id);
    assert.equal(projection.registry_version, 'v2');
    assert.equal(projection.facts.filter(fact => fact.predicate === 'has_description'
      && fact.selected_in_conflict).length, 2);
    assert.equal((await project(baselineCommit.snapshot_id)).projection_digest, baselineProjection.projection_digest);
  });

  it('M. reconstructs pre-merge, merged, and post-revocation snapshots', async () => {
    const left = entity('OBJECTIVE', 'objective:merge-left');
    const right = entity('OBJECTIVE', 'objective:merge-right');
    const pre = await commitChange({ statement: 'Two objective candidates exist.', entities: [left, right] });
    const merged = await commitChange({ statement: 'The objective candidates are the same.', entities: [left, right],
      merges: [{ source_entity_identity_key: left.identity, target_entity_identity_key: right.identity, event_kind: 'MERGED' }] });
    const revoked = await commitChange({ statement: 'The objective merge was incorrect.', entities: [left, right],
      merges: [{ source_entity_identity_key: left.identity, target_entity_identity_key: right.identity, event_kind: 'MERGE_REVOKED' }] });
    assert.equal((await project(pre.snapshot_id)).resolved_merges.length, 0);
    assert.equal((await project(merged.snapshot_id)).resolved_merges.length, 1);
    assert.equal((await project(revoked.snapshot_id)).resolved_merges.length, 0);
  });

  it('N. changes temporal applicability without changing stored semantics or snapshot digest', async () => {
    const objective = entity('OBJECTIVE', 'objective:timed');
    const timedFact = { ...proposition('timed', objective.identity, 'has_description',
      literal('SEMANTIC_TEXT', '2027 objective')), valid_from: '2027-01-01T00:00:00.000Z',
    valid_to: '2027-12-31T23:59:59.000Z' };
    const result = await commitChange({ statement: 'This objective applies during 2027.', entities: [objective], facts: [timedFact] });
    const before = await project(result.snapshot_id, { at: '2026-06-01T00:00:00.000Z' });
    const during = await project(result.snapshot_id, { at: '2027-06-01T00:00:00.000Z' });
    assert.equal(projectedFact(before, 'has_description', objective.identity, '2027 objective').active_at_evaluation, false);
    assert.equal(projectedFact(during, 'has_description', objective.identity, '2027 objective').active_at_evaluation, true);
    assert.deepEqual(before.facts.map(({ active_at_evaluation, ...fact }) => fact),
      during.facts.map(({ active_at_evaluation, ...fact }) => fact));
    assert.equal((await pool.query('SELECT manifest_digest FROM canonical_business_snapshots WHERE id=$1',
      [result.snapshot_id])).rows[0].manifest_digest, result.manifest_digest);
  });

  it('O. deterministically reconstructs the same snapshot and evaluation context', async () => {
    const context = { at: '2026-09-01T12:00:00.000Z' };
    const first = await project(baselineCommit.snapshot_id, context);
    const second = await project(baselineCommit.snapshot_id, context);
    assert.deepEqual(second, first);
    assert.equal(second.projection_digest, first.projection_digest);
  });

  it('P. remains independent of all legacy semantic stores', async () => {
    for (const table of ['"normalizedFacts"', 'business_facts', 'interview_state', 'blueprints', 'knowledge_nodes']) {
      await pool.query(`INSERT INTO ${table}(payload) VALUES ('{"poison":"legacy"}'::jsonb)`);
    }
    const reconstructed = await project(baselineCommit.snapshot_id);
    assert.equal(reconstructed.projection_digest, baselineProjection.projection_digest);
    assert.deepEqual(normalizeProjection(reconstructed, baselineManifest), normalizeExpected(baselineManifest));
  });
});