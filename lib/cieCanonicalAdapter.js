/**
 * SPEC-224 -- CIE Canonical Adapter
 *
 * Transforms approved Client Intelligence Engine interpretation state into a
 * CanonicalSemanticBatch consumable by lib/canonicalSemanticWrite.js
 * (commitCanonicalSemanticBatch). Wire format matches the SPEC-223B contract
 * exactly (uppercase typed object_value, SET/SINGLE cardinalities, ENTITY_REF
 * by identity_key) -- not the SPEC-222 prose notation.
 *
 * ADAPTER OWNS: semantic translation only.
 * ADAPTER DOES NOT OWN: entity/fact/proposition/snapshot IDs, persistence deduplication --
 * those remain owned by commitCanonicalSemanticBatch (SPEC-223B).
 *
 * Constraint: only predicates already defined in the pinned SPEC-222 registry
 * may be used. Fields with no valid predicate mapping are left UNREPRESENTABLE
 * and recorded in snapshot_metadata; the registry is never expanded here.
 */

const { deriveInterpretationBatchKey, canonicalJsonString } = require('./canonicalSemanticWrite');

const UNREPRESENTABLE_FIELDS = [
  'growth_focus', 'business_facts', 'ideal_customer_traits', 'disqualified_customers',
  'geography', 'success_metrics', 'learning_signals', 'pains', 'transformation_areas',
];

class CIECanonicalAdapter {
  static buildBatch(input) {
    const {
      tenant_id,
      client_id,
      blueprint,
      blueprint_id,
      blueprint_version,
      cie_evidence_records = [],
      registry_artifact,
      interpreter_id,
      interpreter_version,
      session_id,
    } = input;

    if (!tenant_id) throw new Error('CIE adapter: tenant_id required');
    if (!client_id) throw new Error('CIE adapter: client_id required');
    if (!blueprint) throw new Error('CIE adapter: blueprint required');
    if (!registry_artifact) throw new Error('CIE adapter: registry_artifact required');
    if (!interpreter_id) throw new Error('CIE adapter: interpreter_id required');
    if (!interpreter_version) throw new Error('CIE adapter: interpreter_version required');

    const businessKey = `client:${client_id}`;
    const semantic_entities = [{ entity_type: 'BUSINESS', identity_key: businessKey, domain_client_id: client_id }];
    const label_assertions = [];
    const semantic_facts = [];
    const fact_evidence_links = [];
    const unrepresentable = [];

    const normalizedFacts = blueprint.normalizedFacts || {};
    const evidenceByCategory = new Map();
    for (const record of cie_evidence_records) {
      const list = evidenceByCategory.get(record.category) || [];
      list.push(record);
      evidenceByCategory.set(record.category, list);
    }

    function firstEvidence(category) {
      const list = evidenceByCategory.get(category);
      return list && list.length ? list[0] : cie_evidence_records[0] || null;
    }

    // The pinned registry chooses collection cardinality and wire representation.
    // Runtime checks only validate the normalized input against that contract.
    function definitionFor(predicate) {
      const definition = registry_artifact.predicate_definitions?.[predicate];
      if (!definition || !['SINGLE', 'OPTIONAL_SINGLE', 'SET', 'ORDERED_SET'].includes(definition.cardinality)) {
        throw new Error(`CIE adapter: missing or invalid registry metadata for ${predicate}`);
      }
      return definition;
    }

    function valuesFor(predicate, value) {
      const definition = definitionFor(predicate);
      if (value == null) return [];
      const values = Array.isArray(value) ? value : [value];
      if (!['SET', 'ORDERED_SET'].includes(definition.cardinality) && Array.isArray(value)) {
        throw new Error(`CIE adapter: ${predicate} ${definition.cardinality} requires a scalar value`);
      }
      const seen = new Set();
      return values.filter(item => {
        const key = canonicalJsonString(item);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    function entityTypeFor(predicate) {
      const range = definitionFor(predicate).range;
      if (range?.kind !== 'ENTITY' || range.entity_types?.length !== 1) {
        throw new Error(`CIE adapter: ${predicate} requires an unambiguous entity range`);
      }
      return range.entity_types[0];
    }

    function namedRecord(value) {
      if (typeof value === 'string') return { name: value };
      if (!value || Object.getPrototypeOf(value) !== Object.prototype) {
        throw new Error('CIE adapter: named entities require strings or named records');
      }
      if (value.name != null && typeof value.name !== 'string') {
        throw new Error('CIE adapter: canonical entity labels require scalar strings');
      }
      return value;
    }

    function addLabel(assertion) {
      if (typeof assertion.label !== 'string') {
        throw new Error('CIE adapter: canonical entity labels require scalar strings');
      }
      label_assertions.push(assertion);
    }

    const emittedFacts = new Set();
    function addFact(fact, evidenceCategory) {
      const definition = definitionFor(fact.predicate);
      const range = definition.range;
      const wireType = range?.kind === 'ENTITY' ? 'ENTITY_REF'
        : range?.kind === 'LITERAL' && range.literal_types?.length === 1 ? range.literal_types[0] : null;
      if (!wireType || wireType !== fact.object_value.type) {
        throw new Error(`CIE adapter: ${fact.predicate} has incompatible value kind`);
      }
      const subject = semantic_entities.find(entity => entity.identity_key === fact.subject_entity_identity_key);
      if (!definition.domain?.includes(subject?.entity_type)) {
        throw new Error(`CIE adapter: ${fact.predicate} has incompatible subject entity type`);
      }
      for (const value of valuesFor(fact.predicate, fact.object_value.value)) {
        // INTEGER_RANGE is the only structured literal emitted by this adapter.
        if (wireType === 'INTEGER_RANGE') {
          if (!value || Array.isArray(value) || !Number.isInteger(value.min) || !Number.isInteger(value.max)) {
            throw new Error(`CIE adapter: ${fact.predicate} requires an integer range`);
          }
        } else if (typeof value !== 'string') {
          throw new Error(`CIE adapter: ${fact.predicate} requires scalar string values`);
        }
        if (wireType === 'ENTITY_REF') {
          const entity = semantic_entities.find(item => item.identity_key === value);
          if (!range.entity_types.includes(entity?.entity_type)) {
            throw new Error(`CIE adapter: ${fact.predicate} has incompatible object entity type`);
          }
        }
        const expanded = { ...fact, object_value: { type: wireType, value } };
        const key = canonicalJsonString(expanded);
        if (emittedFacts.has(key)) continue;
        emittedFacts.add(key);
        const index = semantic_facts.length;
        semantic_facts.push(expanded);
        const source = firstEvidence(evidenceCategory);
        if (source && source.source_text_sha256) {
          fact_evidence_links.push({
            fact_index: index,
            evidence_id: source.id,
            source_text_sha256: source.source_text_sha256,
            span_start_utf16: 0,
            span_end_utf16: (source.statement || '').length,
            support_type: 'DIRECT',
          });
        }
      }
    }

    function baseFact(overrides) {
      return {
        epistemic_state: 'KNOWN',
        epistemic_confidence: 0.9,
        epistemic_calibration_version: 'spec-224-v1',
        interpretation_confidence: 0.9,
        interpretation_calibration_version: 'spec-224-v1',
        temporal_status: 'CURRENT',
        valid_from: null,
        valid_to: null,
        modality: 'ACTUAL',
        qualifiers: {},
        ...overrides,
      };
    }

    if (normalizedFacts.business_name) {
      addFact(
        baseFact({
          subject_entity_identity_key: businessKey,
          predicate: 'has_description',
          object_value: { type: 'SEMANTIC_TEXT', value: normalizedFacts.business_name },
          qualifiers: { language: 'en' },
        }),
        'identity'
      );
      addLabel({
        entity_identity_key: businessKey,
        label: normalizedFacts.business_name,
        assertion_kind: 'CANONICAL',
        evidence_id: firstEvidence('identity')?.id || null,
      });
    }

    if (normalizedFacts.services != null) {
      valuesFor('offers', normalizedFacts.services).forEach((value, serviceIdx) => {
        const service = namedRecord(value);
        const offerKey = `offer:${serviceIdx}`;
        semantic_entities.push({ entity_type: entityTypeFor('offers'), identity_key: offerKey });
        if (service.name) {
          addLabel({
            entity_identity_key: offerKey,
            label: service.name,
            assertion_kind: 'CANONICAL',
            evidence_id: firstEvidence('services')?.id || null,
          });
        }
        addFact(
          baseFact({
            subject_entity_identity_key: businessKey,
            predicate: 'offers',
            object_value: { type: 'ENTITY_REF', value: offerKey },
          }),
          'services'
        );

        valuesFor('contains_program', service.variants).forEach((value, variantIdx) => {
          const variant = namedRecord(value);
          const programKey = `program:${serviceIdx}:${variantIdx}`;
          semantic_entities.push({ entity_type: entityTypeFor('contains_program'), identity_key: programKey });
          if (variant.name) {
            addLabel({
              entity_identity_key: programKey,
              label: variant.name,
              assertion_kind: 'CANONICAL',
              evidence_id: firstEvidence('services')?.id || null,
            });
          }
          addFact(
            baseFact({
              subject_entity_identity_key: offerKey,
              predicate: 'contains_program',
              object_value: { type: 'ENTITY_REF', value: programKey },
            }),
            'services'
          );
        });
      });
    }

    valuesFor('targets_customer_profile', normalizedFacts.ideal_customers || null).forEach((customer, index) => {
      const profileKey = index === 0 ? 'customer_profile:primary' : `customer_profile:primary:${index}`;
      semantic_entities.push({ entity_type: entityTypeFor('targets_customer_profile'), identity_key: profileKey });
      addLabel({
        entity_identity_key: profileKey,
        label: customer,
        assertion_kind: 'CANONICAL',
        evidence_id: firstEvidence('customer')?.id || null,
      });
      addFact(
        baseFact({
          subject_entity_identity_key: businessKey,
          predicate: 'targets_customer_profile',
          object_value: { type: 'ENTITY_REF', value: profileKey },
        }),
        'customer'
      );

      if (normalizedFacts.ideal_customers_role) {
        addFact(
          baseFact({
            subject_entity_identity_key: profileKey,
            predicate: 'has_role',
            object_value: { type: 'ROLE', value: normalizedFacts.ideal_customers_role },
          }),
          'customer'
        );
      }
      if (normalizedFacts.ideal_customers_stage) {
        addFact(
          baseFact({
            subject_entity_identity_key: profileKey,
            predicate: 'has_business_stage',
            object_value: { type: 'STAGE', value: normalizedFacts.ideal_customers_stage },
          }),
          'customer'
        );
      }
      if (normalizedFacts.ideal_customers_employee_range) {
        const range = parseEmployeeRange(normalizedFacts.ideal_customers_employee_range);
        if (range) {
          addFact(
            baseFact({
              subject_entity_identity_key: profileKey,
              predicate: 'has_employee_range',
              object_value: { type: 'INTEGER_RANGE', value: { min: range.min, max: range.max, unit: 'employees' } },
            }),
            'customer'
          );
        }
      }
      if (normalizedFacts.ideal_customers_geography) {
        addFact(
          baseFact({
            subject_entity_identity_key: profileKey,
            predicate: 'has_geography',
            object_value: { type: 'GEOGRAPHY', value: normalizedFacts.ideal_customers_geography },
            qualifiers: { scope: 'customer_profile' },
          }),
          'customer'
        );
      }
    });

    valuesFor('excludes_customer_profile', normalizedFacts.avoid_customers || null).forEach((customer, index) => {
      const excludedKey = index === 0 ? 'customer_profile:excluded' : `customer_profile:excluded:${index}`;
      semantic_entities.push({ entity_type: entityTypeFor('excludes_customer_profile'), identity_key: excludedKey });
      addLabel({
        entity_identity_key: excludedKey,
        label: customer,
        assertion_kind: 'CANONICAL',
        evidence_id: firstEvidence('customer')?.id || null,
      });
      addFact(
        baseFact({
          subject_entity_identity_key: businessKey,
          predicate: 'excludes_customer_profile',
          object_value: { type: 'ENTITY_REF', value: excludedKey },
          qualifiers: { strength: 'LOW_PRIORITY' },
        }),
        'customer'
      );
    });

    if (normalizedFacts.target_markets) {
      addFact(
        baseFact({
          subject_entity_identity_key: businessKey,
          predicate: 'has_geography',
          object_value: { type: 'GEOGRAPHY', value: normalizedFacts.target_markets },
          qualifiers: { scope: 'service_area' },
        }),
        'identity'
      );
    }

    if (normalizedFacts.differentiation) {
      addFact(
        baseFact({
          subject_entity_identity_key: businessKey,
          predicate: 'has_buying_reason',
          object_value: { type: 'CONCEPT', value: normalizedFacts.differentiation },
          epistemic_state: 'HYPOTHESIS',
          epistemic_confidence: 0.6,
          modality: 'INTENDED',
        }),
        'identity'
      );
    }

    if (normalizedFacts.ninety_day_outcomes != null) {
      valuesFor('targets_outcome', normalizedFacts.ninety_day_outcomes).forEach((value, idx) => {
        const outcome = namedRecord(value);
        const outcomeKey = `outcome:90day:${idx}`;
        semantic_entities.push({ entity_type: entityTypeFor('targets_outcome'), identity_key: outcomeKey });
        if (outcome.name) {
          addLabel({
            entity_identity_key: outcomeKey,
            label: outcome.name,
            assertion_kind: 'CANONICAL',
            evidence_id: firstEvidence('identity')?.id || null,
          });
        }
        const firstOffer = semantic_entities.find(e => e.entity_type === 'OFFER');
        if (firstOffer) {
          addFact(
            baseFact({
              subject_entity_identity_key: firstOffer.identity_key,
              predicate: 'targets_outcome',
              object_value: { type: 'ENTITY_REF', value: outcomeKey },
            }),
            'identity'
          );
        }
      });
    }

    UNREPRESENTABLE_FIELDS.forEach(field => {
      if (normalizedFacts[field]) unrepresentable.push(field);
    });

    const ordered_evidence_input_ids = [...cie_evidence_records]
      .sort((a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0))
      .map(e => e.id);

    const evidenceById = new Map(cie_evidence_records.map(e => [e.id, e]));

    const batch = {
      tenant_id,
      registry_artifact_id: registry_artifact.id,
      registry_version: registry_artifact.registry_version,
      registry_content_digest: registry_artifact.content_digest,
      interpreter_id,
      interpreter_version,
      semantic_model_version: 1,
      ordered_evidence_input_ids,
      semantic_entities,
      label_assertions,
      semantic_facts,
      fact_evidence_links,
      fact_relations: [],
      entity_merge_events: [],
      conflict_set_resolutions: [],
      snapshot_metadata: {
        blueprint_id,
        blueprint_version,
        session_id: session_id || null,
        unrepresentable_fields: unrepresentable,
      },
    };

    batch.idempotency_key = deriveInterpretationBatchKey(batch, evidenceById);
    return batch;
  }
}

function parseEmployeeRange(rangeStr) {
  if (!rangeStr) return null;
  if (typeof rangeStr !== 'string') throw new Error('CIE adapter: employee range requires a scalar string');
  const dash = rangeStr.match(/(\d+)\s*[-\u2013to]+\s*(\d+)/i);
  if (dash) return { min: parseInt(dash[1], 10), max: parseInt(dash[2], 10) };
  const upTo = rangeStr.match(/up\s+to\s+(\d+)/i);
  if (upTo) return { min: 1, max: parseInt(upTo[1], 10) };
  const plus = rangeStr.match(/(\d+)\+/);
  if (plus) return { min: parseInt(plus[1], 10), max: 100000 };
  return null;
}

module.exports = { CIECanonicalAdapter, UNREPRESENTABLE_FIELDS };
