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
 * ADAPTER DOES NOT OWN: entity/fact/proposition/snapshot IDs, deduplication --
 * those remain owned by commitCanonicalSemanticBatch (SPEC-223B).
 *
 * Constraint: only predicates already defined in the pinned SPEC-222 registry
 * may be used. Fields with no valid predicate mapping are left UNREPRESENTABLE
 * and recorded in snapshot_metadata; the registry is never expanded here.
 */

const { deriveInterpretationBatchKey } = require('./canonicalSemanticWrite');

const UNREPRESENTABLE_FIELDS = ['growth_focus', 'business_facts'];

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

    function addFact(fact, evidenceCategory) {
      const index = semantic_facts.length;
      semantic_facts.push(fact);
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
      return index;
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
      label_assertions.push({
        entity_identity_key: businessKey,
        label: normalizedFacts.business_name,
        assertion_kind: 'CANONICAL',
        evidence_id: firstEvidence('identity')?.id || null,
      });
    }

    if (Array.isArray(normalizedFacts.services)) {
      normalizedFacts.services.forEach((service, serviceIdx) => {
        const offerKey = `offer:${serviceIdx}`;
        semantic_entities.push({ entity_type: 'OFFER', identity_key: offerKey });
        if (service.name) {
          label_assertions.push({
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

        (service.variants || []).forEach((variant, variantIdx) => {
          const programKey = `program:${serviceIdx}:${variantIdx}`;
          semantic_entities.push({ entity_type: 'PROGRAM', identity_key: programKey });
          if (variant.name) {
            label_assertions.push({
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

    const profileKey = 'customer_profile:primary';
    if (normalizedFacts.ideal_customers) {
      semantic_entities.push({ entity_type: 'CUSTOMER_PROFILE', identity_key: profileKey });
      label_assertions.push({
        entity_identity_key: profileKey,
        label: normalizedFacts.ideal_customers,
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
    }

    if (normalizedFacts.avoid_customers) {
      const excludedKey = 'customer_profile:excluded';
      semantic_entities.push({ entity_type: 'CUSTOMER_PROFILE', identity_key: excludedKey });
      label_assertions.push({
        entity_identity_key: excludedKey,
        label: normalizedFacts.avoid_customers,
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
    }

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

    if (Array.isArray(normalizedFacts.ninety_day_outcomes)) {
      normalizedFacts.ninety_day_outcomes.forEach((outcome, idx) => {
        const outcomeKey = `outcome:90day:${idx}`;
        semantic_entities.push({ entity_type: 'OUTCOME', identity_key: outcomeKey });
        if (outcome.name) {
          label_assertions.push({
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
  const dash = rangeStr.match(/(\d+)\s*[-\u2013to]+\s*(\d+)/i);
  if (dash) return { min: parseInt(dash[1], 10), max: parseInt(dash[2], 10) };
  const upTo = rangeStr.match(/up\s+to\s+(\d+)/i);
  if (upTo) return { min: 1, max: parseInt(upTo[1], 10) };
  const plus = rangeStr.match(/(\d+)\+/);
  if (plus) return { min: parseInt(plus[1], 10), max: 100000 };
  return null;
}

module.exports = { CIECanonicalAdapter, UNREPRESENTABLE_FIELDS };
