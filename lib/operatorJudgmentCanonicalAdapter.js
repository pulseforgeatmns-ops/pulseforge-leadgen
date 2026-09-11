'use strict';

/**
 * SPEC-250 — Canonical Operator Judgment Producer (adapter)
 *
 * Converts an *explicit typed* operator judgment into the CanonicalSemanticBatch
 * contract expected by commitCanonicalSemanticBatch().
 *
 * Semantic primitive (smallest SPEC-222 extension):
 *   entity_type OPERATOR_JUDGMENT
 *   predicates  has_operator_judgment | has_judgment_kind | expresses_judgment
 *               | associated_with_objective
 *
 * Distinctions (machine-inspectable):
 *   BUSINESS FACT     — subject entity_type is not OPERATOR_JUDGMENT
 *   OPERATOR JUDGMENT — subject entity_type is OPERATOR_JUDGMENT and
 *                       predicate is expresses_judgment with epistemic_state KNOWN
 *                       (known that the operator decided this; not a market fact)
 *   HYPOTHESIS        — epistemic_state HYPOTHESIS (including learning hypotheses
 *                       attached to an operator judgment)
 *
 * This adapter does not classify Max conversation. Unstructured text is rejected.
 * Operator origin is required; model/specialist/external sources fail closed.
 */

const { deriveInterpretationBatchKey, canonicalJsonString, CanonicalSemanticError } =
  require('./canonicalSemanticWrite');

const INTERPRETER_ID = 'spec-250-operator-judgment-producer';
const INTERPRETER_VERSION = '1.0.0';
const SEMANTIC_MODEL_VERSION = 1;

const REQUIRED_PREDICATES = [
  'has_operator_judgment',
  'has_judgment_kind',
  'expresses_judgment',
  'associated_with_objective',
];

const ALLOWED_ORIGINS = new Set(['OPERATOR']);
const ALLOWED_ORIGIN_KINDS = new Set(['operator_authored', 'operator_confirmed']);
const FORBIDDEN_ORIGINS = new Set([
  'MODEL', 'MODEL_INFERENCE', 'SPECIALIST', 'SPECIALIST_RECOMMENDATION', 'EXTERNAL', 'EXTERNAL_EVIDENCE',
]);
const EPISTEMIC_STATES = new Set(['KNOWN', 'HYPOTHESIS', 'UNKNOWN', 'UNRESOLVED', 'NOT_APPLICABLE']);
const TEMPORAL_STATUSES = new Set(['CURRENT', 'PLANNED', 'HISTORICAL', 'RETIRED']);
const MODALITIES = new Set(['ACTUAL', 'INTENDED', 'CONDITIONAL']);

function fail(code, message, details) {
  throw new CanonicalSemanticError(code, message, details);
}

function vocabularyTypes(value) {
  return new Set(Array.isArray(value) ? value : Object.keys(value || {}));
}

function requireNonEmptyString(value, code, message) {
  if (typeof value !== 'string' || !value.trim()) fail(code, message);
  return value.trim();
}

function utf16Length(text) {
  return String(text || '').length;
}

function spanFor(statement, fragment) {
  const source = String(statement || '');
  if (!fragment) return { start: 0, end: source.length };
  const index = source.indexOf(fragment);
  if (index < 0) return { start: 0, end: source.length };
  return { start: index, end: index + fragment.length };
}

function operatorProvenance(input) {
  const provenance = input.provenance;
  if (!provenance || typeof provenance !== 'object' || Array.isArray(provenance)) {
    fail('OPERATOR_PROVENANCE_REQUIRED', 'operator provenance is required');
  }
  const origin = String(provenance.origin || '').toUpperCase();
  const originKind = String(provenance.origin_kind || '').trim();
  if (FORBIDDEN_ORIGINS.has(origin) || FORBIDDEN_ORIGINS.has(originKind.toUpperCase())) {
    fail('OPERATOR_PROVENANCE_INVALID', 'model/specialist/external origin cannot be labeled operator judgment');
  }
  if (!ALLOWED_ORIGINS.has(origin) || !ALLOWED_ORIGIN_KINDS.has(originKind)) {
    fail('OPERATOR_PROVENANCE_INVALID', 'origin must be operator-authored or operator-confirmed');
  }
  const operator = input.operator && typeof input.operator === 'object' ? input.operator : {};
  const actorId = operator.id != null ? String(operator.id).trim() : (provenance.actor_id != null ? String(provenance.actor_id).trim() : '');
  const actorRole = operator.role != null ? String(operator.role).trim() : (provenance.actor_role != null ? String(provenance.actor_role).trim() : '');
  return {
    origin: 'OPERATOR',
    origin_kind: originKind,
    actor_kind: provenance.actor_kind || (actorId ? 'authenticated_operator' : 'authenticated_operator_unidentified'),
    ...(actorId ? { actor_id: actorId } : {}),
    ...(actorRole ? { actor_role: actorRole } : {}),
  };
}

function validateRegistry(registry) {
  if (!registry || !registry.id || !registry.registry_version || !registry.content_digest) {
    fail('REGISTRY_REQUIRED', 'SPEC-250 registry artifact is required');
  }
  const vocabulary = vocabularyTypes(registry.entity_vocabulary);
  if (!vocabulary.has('OPERATOR_JUDGMENT') || !vocabulary.has('BUSINESS')) {
    fail('UNSUPPORTED_SEMANTIC_PRIMITIVE', 'pinned registry does not include OPERATOR_JUDGMENT');
  }
  for (const predicate of REQUIRED_PREDICATES) {
    if (!registry.predicate_definitions?.[predicate]) {
      fail('UNSUPPORTED_SEMANTIC_PRIMITIVE', `pinned registry lacks ${predicate}`);
    }
  }
  return registry;
}

function judgmentIdentityKey(judgmentKey) {
  return `operator-judgment:${judgmentKey}`;
}

function buildCanonicalJudgmentEvidenceText(input, provenance) {
  const lines = [
    'CANONICAL_OPERATOR_JUDGMENT',
    `judgment_key:${input.judgment_key}`,
    `judgment_kind:${input.judgment_kind}`,
    `origin:${provenance.origin}`,
    `origin_kind:${provenance.origin_kind}`,
    provenance.actor_id ? `actor_id:${provenance.actor_id}` : 'actor_id:',
  ];
  if (input.associated_mission_id) lines.push(`associated_mission_id:${input.associated_mission_id}`);
  if (input.associated_objective_identity_key) {
    lines.push(`associated_objective_identity_key:${input.associated_objective_identity_key}`);
  }
  for (const proposition of input.propositions) {
    lines.push(`slot:${proposition.judgment_slot}:${proposition.epistemic_state}:${proposition.statement}`);
  }
  return lines.join('\n');
}

function isOperatorJudgmentFact(fact, entitiesById) {
  const subject = entitiesById.get(fact.subject_entity_id);
  return subject?.entity_type === 'OPERATOR_JUDGMENT' && fact.predicate === 'expresses_judgment';
}

function isOrdinaryBusinessFact(fact, entitiesById) {
  const subject = entitiesById.get(fact.subject_entity_id);
  return Boolean(subject) && subject.entity_type !== 'OPERATOR_JUDGMENT';
}

class OperatorJudgmentCanonicalAdapter {
  static buildBatch(input = {}) {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
      fail('JUDGMENT_INPUT_INVALID', 'operator judgment input must be a typed object');
    }
    if (input.kind !== 'OPERATOR_JUDGMENT') {
      fail('JUDGMENT_KIND_REQUIRED', 'explicit kind OPERATOR_JUDGMENT is required; conversational text is not canonicalized');
    }
    if (input.resolve_mission === true && !String(input.associated_mission_id || '').trim()) {
      fail('MISSION_BINDING_AMBIGUOUS', 'active mission cannot be resolved safely; supply associated_mission_id explicitly');
    }
    if (input.resolve_objective === true && !String(input.associated_objective_identity_key || '').trim()) {
      fail('OBJECTIVE_BINDING_AMBIGUOUS', 'active objective cannot be resolved safely; supply associated_objective_identity_key explicitly');
    }

    const tenantId = requireNonEmptyString(input.tenant_id, 'TENANT_IDENTITY_REQUIRED', 'tenant_id is required');
    const clientId = Number(input.client_id);
    if (!Number.isInteger(clientId) || clientId < 1) fail('TENANT_IDENTITY_REQUIRED', 'client_id must be a positive integer');
    const judgmentKey = requireNonEmptyString(input.judgment_key, 'JUDGMENT_IDENTITY_REQUIRED', 'judgment_key is required');
    const judgmentKind = requireNonEmptyString(input.judgment_kind, 'JUDGMENT_KIND_INVALID', 'judgment_kind is required');
    const propositions = Array.isArray(input.propositions) ? input.propositions : null;
    if (!propositions || !propositions.length) fail('JUDGMENT_PROPOSITIONS_REQUIRED', 'at least one typed proposition is required');

    const provenance = operatorProvenance(input);
    const registry = validateRegistry(input.registry_artifact);
    const evidenceRecords = Array.isArray(input.evidence_records) ? input.evidence_records : [];
    if (!evidenceRecords.length) fail('EVIDENCE_REQUIRED', 'canonical evidence records are required');

    const evidenceByRole = new Map();
    for (const record of evidenceRecords) {
      if (!record?.id || !record.source_text_sha256 || typeof record.statement !== 'string') {
        fail('EVIDENCE_INVALID', 'evidence records require id, statement, and source_text_sha256');
      }
      evidenceByRole.set(record.role || record.category || record.id, record);
    }
    const canonicalEvidence = evidenceRecords.find(record => record.role === 'canonical_judgment') || evidenceRecords[0];

    const businessKey = `client:${clientId}`;
    const judgmentEntityKey = judgmentIdentityKey(judgmentKey);
    const associatedMissionId = String(input.associated_mission_id || '').trim() || null;
    const associatedObjectiveKey = String(input.associated_objective_identity_key || '').trim() || null;

    const semantic_entities = [
      { entity_type: 'BUSINESS', identity_key: businessKey, domain_client_id: clientId },
      { entity_type: 'OPERATOR_JUDGMENT', identity_key: judgmentEntityKey },
    ];
    if (associatedObjectiveKey) {
      semantic_entities.push({ entity_type: 'OBJECTIVE', identity_key: associatedObjectiveKey });
    }

    const label = typeof input.label === 'string' && input.label.trim() ? input.label.trim() : judgmentKey;
    const label_assertions = [{
      entity_identity_key: judgmentEntityKey,
      label,
      assertion_kind: 'CANONICAL',
      evidence_id: canonicalEvidence.id,
    }];

    const semantic_facts = [];
    const fact_evidence_links = [];

    const judgmentQualifiers = {
      provenance,
      ...(associatedMissionId ? { associated_mission_id: associatedMissionId } : {}),
    };

    function addFact(fact, evidence, fragment, supportType) {
      const index = semantic_facts.length;
      semantic_facts.push(fact);
      const source = evidence || canonicalEvidence;
      const span = spanFor(source.statement, fragment || fact.object_value?.value);
      fact_evidence_links.push({
        fact_index: index,
        evidence_id: source.id,
        source_text_sha256: source.source_text_sha256,
        span_start_utf16: span.start,
        span_end_utf16: Math.max(span.end, span.start + 1),
        support_type: supportType,
      });
    }

    function baseFact(overrides) {
      return {
        epistemic_state: 'KNOWN',
        epistemic_confidence: 1,
        epistemic_calibration_version: 'spec-250-v1',
        interpretation_confidence: 1,
        interpretation_calibration_version: 'spec-250-v1',
        temporal_status: 'CURRENT',
        valid_from: null,
        valid_to: null,
        modality: 'INTENDED',
        qualifiers: { ...judgmentQualifiers },
        ...overrides,
        qualifiers: { ...judgmentQualifiers, ...(overrides.qualifiers || {}) },
      };
    }

    addFact(baseFact({
      subject_entity_identity_key: businessKey,
      predicate: 'has_operator_judgment',
      object_value: { type: 'ENTITY_REF', value: judgmentEntityKey },
    }), canonicalEvidence, judgmentKey, 'OPERATOR_CONFIRMED');

    addFact(baseFact({
      subject_entity_identity_key: judgmentEntityKey,
      predicate: 'has_judgment_kind',
      object_value: { type: 'JUDGMENT_KIND', value: judgmentKind },
    }), canonicalEvidence, judgmentKind, 'OPERATOR_CONFIRMED');

    const seenSlots = new Set();
    for (const proposition of propositions) {
      if (!proposition || typeof proposition !== 'object' || Array.isArray(proposition)) {
        fail('JUDGMENT_PROPOSITION_INVALID', 'each proposition must be a typed object');
      }
      const slot = requireNonEmptyString(proposition.judgment_slot, 'JUDGMENT_SLOT_REQUIRED', 'judgment_slot is required');
      if (seenSlots.has(slot)) fail('JUDGMENT_SLOT_DUPLICATE', `duplicate judgment_slot ${slot}`);
      seenSlots.add(slot);
      const statement = requireNonEmptyString(proposition.statement, 'JUDGMENT_STATEMENT_REQUIRED', 'proposition statement is required');
      const epistemicState = String(proposition.epistemic_state || 'KNOWN').toUpperCase();
      if (!EPISTEMIC_STATES.has(epistemicState)) fail('EPISTEMIC_STATE_INVALID', 'invalid epistemic_state');
      if (['UNKNOWN', 'UNRESOLVED', 'NOT_APPLICABLE'].includes(epistemicState)) {
        fail('EPISTEMIC_STATE_INVALID', `${epistemicState} cannot carry a judgment statement`);
      }
      const temporalStatus = String(proposition.temporal_status || 'CURRENT').toUpperCase();
      const modality = String(proposition.modality || (epistemicState === 'HYPOTHESIS' ? 'INTENDED' : 'INTENDED')).toUpperCase();
      if (!TEMPORAL_STATUSES.has(temporalStatus) || !MODALITIES.has(modality)) {
        fail('TEMPORAL_MODAL_INVALID', 'invalid temporal/modal state');
      }

      const rationaleEvidence = proposition.rationale_evidence_role
        ? evidenceByRole.get(proposition.rationale_evidence_role)
        : evidenceRecords.find(record => record.role === `rationale:${slot}`) || null;

      addFact(baseFact({
        subject_entity_identity_key: judgmentEntityKey,
        predicate: 'expresses_judgment',
        object_value: { type: 'SEMANTIC_TEXT', value: statement },
        epistemic_state: epistemicState,
        epistemic_confidence: epistemicState === 'HYPOTHESIS' ? (proposition.epistemic_confidence ?? 0.6) : 1,
        temporal_status: temporalStatus,
        modality,
        qualifiers: { ...judgmentQualifiers, judgment_slot: slot },
      }), canonicalEvidence, `slot:${slot}:`, 'OPERATOR_CONFIRMED');

      if (rationaleEvidence) {
        const lastIndex = semantic_facts.length - 1;
        const rationaleText = String(proposition.rationale || rationaleEvidence.statement);
        const span = spanFor(rationaleEvidence.statement, rationaleText);
        fact_evidence_links.push({
          fact_index: lastIndex,
          evidence_id: rationaleEvidence.id,
          source_text_sha256: rationaleEvidence.source_text_sha256,
          span_start_utf16: span.start,
          span_end_utf16: Math.max(span.end, span.start + 1),
          support_type: 'OPERATOR_CONFIRMED',
        });
      }
    }

    if (associatedObjectiveKey) {
      addFact(baseFact({
        subject_entity_identity_key: judgmentEntityKey,
        predicate: 'associated_with_objective',
        object_value: { type: 'ENTITY_REF', value: associatedObjectiveKey },
      }), canonicalEvidence, associatedObjectiveKey, 'OPERATOR_CONFIRMED');
    }

    const batch = {
      tenant_id: tenantId,
      registry_artifact_id: registry.id,
      registry_version: registry.registry_version,
      registry_content_digest: registry.content_digest,
      interpreter_id: INTERPRETER_ID,
      interpreter_version: INTERPRETER_VERSION,
      semantic_model_version: SEMANTIC_MODEL_VERSION,
      ordered_evidence_input_ids: evidenceRecords.map(record => record.id),
      semantic_entities,
      label_assertions,
      semantic_facts,
      fact_evidence_links,
      fact_relations: Array.isArray(input.fact_relations) ? input.fact_relations : [],
      entity_merge_events: [],
      conflict_set_resolutions: [],
      snapshot_metadata: {
        spec: 'SPEC-250',
        judgment_key: judgmentKey,
        producer: INTERPRETER_ID,
      },
    };
    const evidenceById = new Map(evidenceRecords.map(record => [record.id, record]));
    batch.idempotency_key = deriveInterpretationBatchKey(batch, evidenceById);
    return batch;
  }
}

module.exports = {
  OperatorJudgmentCanonicalAdapter,
  INTERPRETER_ID,
  INTERPRETER_VERSION,
  judgmentIdentityKey,
  buildCanonicalJudgmentEvidenceText,
  isOperatorJudgmentFact,
  isOrdinaryBusinessFact,
  utf16Length,
};
