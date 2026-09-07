'use strict';

const crypto = require('crypto');

const SPEC = 'SPEC-247';

const OBJECT_TYPES = Object.freeze({
  PLAYBOOK: 'playbook',
  OUTREACH_ASSET: 'outreach_asset',
  PROSPECT_INTELLIGENCE: 'prospect_intelligence',
  LEARNING: 'learning',
  HYPOTHESIS: 'hypothesis',
  EXPERIMENT: 'experiment',
  VALIDATED_PRINCIPLE: 'validated_principle',
});

const OBJECT_TYPE_VALUES = Object.freeze(Object.values(OBJECT_TYPES));

const SCOPES = Object.freeze({
  ORGANIZATIONAL: 'organizational',
  CLIENT: 'client',
  MISSION: 'mission',
});

const EPISTEMIC_KINDS = Object.freeze({
  OBSERVED_FACT: 'observed_fact',
  OPERATOR_PREFERENCE: 'operator_preference',
  STAKEHOLDER_PREFERENCE: 'stakeholder_preference',
  HYPOTHESIS: 'hypothesis',
  VALIDATED_FINDING: 'validated_finding',
  CANONICAL_TRUTH: 'canonical_truth',
});

const EPISTEMIC_STATES = Object.freeze({
  OBSERVED: 'OBSERVED',
  INFERRED: 'INFERRED',
  UNKNOWN: 'UNKNOWN',
});

const VALIDATION_STATES = Object.freeze({
  UNVALIDATED: 'UNVALIDATED',
  STAKEHOLDER_VALIDATED: 'STAKEHOLDER_VALIDATED',
  MARKET_VALIDATED: 'MARKET_VALIDATED',
});

const LIFECYCLE_STATES = Object.freeze({
  HYPOTHESIS: 'HYPOTHESIS',
  STAKEHOLDER_VALIDATED: 'STAKEHOLDER_VALIDATED',
  EXPERIMENTALLY_SUPPORTED: 'EXPERIMENTALLY_SUPPORTED',
  MARKET_VALIDATED: 'MARKET_VALIDATED',
  CANONICAL: 'CANONICAL',
});

const LIFECYCLE_ORDER = Object.freeze([
  LIFECYCLE_STATES.HYPOTHESIS,
  LIFECYCLE_STATES.STAKEHOLDER_VALIDATED,
  LIFECYCLE_STATES.EXPERIMENTALLY_SUPPORTED,
  LIFECYCLE_STATES.MARKET_VALIDATED,
  LIFECYCLE_STATES.CANONICAL,
]);

const EVIDENCE_TYPES = Object.freeze({
  OBSERVED: 'observed',
  OPERATOR: 'operator',
  STAKEHOLDER: 'stakeholder',
  EXPERIMENT: 'experiment',
  MARKET: 'market',
  CAMPAIGN_OUTCOME: 'campaign_outcome',
  IMPORT: 'import',
});

const AGENT_CAPABILITIES = Object.freeze({
  scout: {
    canRead: true,
    canCreate: [OBJECT_TYPES.PROSPECT_INTELLIGENCE, OBJECT_TYPES.LEARNING],
    canPromote: false,
    note: 'Scout may produce prospect intelligence and evidence, but cannot modify doctrine.',
  },
  max: {
    canRead: true,
    canCreate: [OBJECT_TYPES.HYPOTHESIS, OBJECT_TYPES.EXPERIMENT, OBJECT_TYPES.LEARNING],
    canPromote: false,
    note: 'Max retrieves, explains, recommends strategy, and plans experiments.',
  },
  paige: {
    canRead: true,
    canCreate: [OBJECT_TYPES.OUTREACH_ASSET, OBJECT_TYPES.HYPOTHESIS],
    canPromote: false,
    note: 'Paige consumes approved assets, validated principles, and learnings.',
  },
  penny: {
    canRead: true,
    canCreate: [OBJECT_TYPES.HYPOTHESIS, OBJECT_TYPES.EXPERIMENT, OBJECT_TYPES.OUTREACH_ASSET],
    canPromote: false,
    note: 'Penny works on positioning, ICP, messaging, and experiments.',
  },
  emmett: {
    canRead: true,
    canCreate: [OBJECT_TYPES.EXPERIMENT, OBJECT_TYPES.LEARNING],
    canPromote: false,
    note: 'Emmett consumes approved assets and reports sequencing outcomes.',
  },
  rex: {
    canRead: true,
    canCreate: [OBJECT_TYPES.EXPERIMENT, OBJECT_TYPES.LEARNING],
    canPromote: false,
    note: 'Rex reports campaign metrics and outcomes that feed promotion.',
  },
  operator: {
    canRead: true,
    canCreate: OBJECT_TYPE_VALUES,
    canPromote: true,
    note: 'Operators review evidence and explicitly promote learning.',
  },
});

function clone(value) {
  return JSON.parse(JSON.stringify(value == null ? null : value));
}

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function nowIso(now) {
  return (now instanceof Date ? now : new Date(now || Date.now())).toISOString();
}

function newId(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function knowledgeError(code, message, extras = {}) {
  const err = new Error(message);
  err.code = code;
  err.spec = SPEC;
  Object.assign(err, extras);
  return err;
}

function normalizeRole(value) {
  return asText(value || 'operator').toLowerCase();
}

function actorCanCreate(actorRole, objectType) {
  const caps = AGENT_CAPABILITIES[normalizeRole(actorRole)] || AGENT_CAPABILITIES.operator;
  return caps.canCreate === OBJECT_TYPE_VALUES || caps.canCreate.includes(objectType);
}

function actorCanPromote(actorRole) {
  const caps = AGENT_CAPABILITIES[normalizeRole(actorRole)] || {};
  return caps.canPromote === true;
}

function assertTenant(tenantId) {
  const normalized = asText(tenantId);
  if (!normalized) throw knowledgeError('ak_tenant_required', 'tenantId is required.');
  return normalized;
}

function normalizeScope(value, input = {}) {
  const scope = asText(value || input.scope || (input.missionId ? SCOPES.MISSION : SCOPES.CLIENT)).toLowerCase();
  if (!Object.values(SCOPES).includes(scope)) {
    throw knowledgeError('ak_invalid_scope', `Unknown acquisition knowledge scope: ${scope}`);
  }
  return scope;
}

function normalizeObjectType(value) {
  const objectType = asText(value).toLowerCase();
  if (!OBJECT_TYPE_VALUES.includes(objectType)) {
    throw knowledgeError('ak_invalid_object_type', `Unknown acquisition knowledge object type: ${objectType}`);
  }
  return objectType;
}

function normalizeLifecycleState(value) {
  const state = asText(value || LIFECYCLE_STATES.HYPOTHESIS).toUpperCase();
  if (!LIFECYCLE_ORDER.includes(state)) {
    throw knowledgeError('ak_invalid_lifecycle_state', `Unknown acquisition knowledge lifecycle state: ${state}`);
  }
  return state;
}

function normalizeEpistemicKind(value, state) {
  const kind = asText(value).toLowerCase();
  if (Object.values(EPISTEMIC_KINDS).includes(kind)) return kind;
  if (state === LIFECYCLE_STATES.CANONICAL) return EPISTEMIC_KINDS.CANONICAL_TRUTH;
  if (state === LIFECYCLE_STATES.HYPOTHESIS) return EPISTEMIC_KINDS.HYPOTHESIS;
  return EPISTEMIC_KINDS.VALIDATED_FINDING;
}

function normalizeEpistemicState(value, legacy = {}) {
  const explicit = asText(value || legacy.epistemic_state).toUpperCase();
  if (explicit) {
    if (!Object.values(EPISTEMIC_STATES).includes(explicit)) {
      throw knowledgeError('ak_invalid_epistemic_state', `Unknown acquisition knowledge epistemic state: ${explicit}`);
    }
    return explicit;
  }
  const kind = asText(legacy.epistemicKind || legacy.kind).toLowerCase();
  if ([EPISTEMIC_KINDS.OBSERVED_FACT, EPISTEMIC_KINDS.OPERATOR_PREFERENCE,
    EPISTEMIC_KINDS.STAKEHOLDER_PREFERENCE, EPISTEMIC_KINDS.CANONICAL_TRUTH].includes(kind)) {
    return EPISTEMIC_STATES.OBSERVED;
  }
  if ([EPISTEMIC_KINDS.HYPOTHESIS, EPISTEMIC_KINDS.VALIDATED_FINDING].includes(kind)) {
    return EPISTEMIC_STATES.INFERRED;
  }
  return EPISTEMIC_STATES.UNKNOWN;
}

function normalizeValidationState(value, legacy = {}) {
  const explicit = asText(value || legacy.validation_status || legacy.validationStatus).toUpperCase();
  if (explicit) {
    if (explicit === LIFECYCLE_STATES.HYPOTHESIS) {
      throw knowledgeError('ak_invalid_validation_state', 'HYPOTHESIS is not a validation state.');
    }
    if (!Object.values(VALIDATION_STATES).includes(explicit)) {
      throw knowledgeError('ak_invalid_validation_state', `Unknown acquisition knowledge validation state: ${explicit}`);
    }
    return explicit;
  }
  const lifecycle = asText(legacy.state || legacy.lifecycleState).toUpperCase();
  if (lifecycle === LIFECYCLE_STATES.STAKEHOLDER_VALIDATED) return VALIDATION_STATES.STAKEHOLDER_VALIDATED;
  if (lifecycle === LIFECYCLE_STATES.MARKET_VALIDATED || lifecycle === LIFECYCLE_STATES.CANONICAL) {
    return VALIDATION_STATES.MARKET_VALIDATED;
  }
  return VALIDATION_STATES.UNVALIDATED;
}

function normalizeEvidenceItem(raw = {}, index = 0) {
  if (typeof raw === 'string') {
    const statement = raw.trim();
    if (!statement) return null;
    return {
      id: `evidence_${index + 1}`,
      type: EVIDENCE_TYPES.OBSERVED,
      statement,
      source: { kind: 'text', ref: statement },
      confidence: 0.5,
      observedAt: nowIso(),
    };
  }
  if (!raw || typeof raw !== 'object') return null;
  const statement = asText(raw.statement || raw.text || raw.label || raw.description);
  const source = raw.source && typeof raw.source === 'object'
    ? clone(raw.source)
    : { kind: asText(raw.sourceKind || raw.kind || 'unknown'), ref: asText(raw.source || raw.ref || '') || null };
  return {
    id: asText(raw.id) || `evidence_${index + 1}`,
    type: asText(raw.type || raw.kind || EVIDENCE_TYPES.OBSERVED).toLowerCase(),
    statement,
    source,
    passage: asText(raw.passage || raw.quote || raw.excerpt || raw.evidenceRef) || null,
    confidence: Math.min(1, Math.max(0, Number(raw.confidence != null ? raw.confidence : 0.5))),
    observedAt: nowIso(raw.observedAt || raw.at || raw.createdAt),
    payload: raw.payload && typeof raw.payload === 'object' ? clone(raw.payload) : {},
  };
}

function normalizeEvidence(value) {
  const rows = Array.isArray(value) ? value : (value ? [value] : []);
  return rows.map(normalizeEvidenceItem).filter(Boolean);
}

function evidenceMeetsState(evidence, nextState) {
  const types = new Set((evidence || []).map((row) => asText(row.type).toLowerCase()));
  if (nextState === LIFECYCLE_STATES.STAKEHOLDER_VALIDATED) {
    return types.has(EVIDENCE_TYPES.STAKEHOLDER) || types.has(EVIDENCE_TYPES.OPERATOR);
  }
  if (nextState === LIFECYCLE_STATES.EXPERIMENTALLY_SUPPORTED) {
    return types.has(EVIDENCE_TYPES.EXPERIMENT) || types.has(EVIDENCE_TYPES.CAMPAIGN_OUTCOME);
  }
  if (nextState === LIFECYCLE_STATES.MARKET_VALIDATED) {
    return types.has(EVIDENCE_TYPES.MARKET) || types.has(EVIDENCE_TYPES.CAMPAIGN_OUTCOME);
  }
  if (nextState === LIFECYCLE_STATES.CANONICAL) {
    return types.has(EVIDENCE_TYPES.OPERATOR) || types.has(EVIDENCE_TYPES.MARKET);
  }
  return true;
}

function hasAttributableSource(item = {}) {
  const source = item.source && typeof item.source === 'object' ? item.source : {};
  const sourceType = asText(source.type || source.kind || item.sourceType || item.kind);
  const sourceRef = asText(source.ref || source.id || source.url || source.reference || item.sourceRef || item.ref);
  return Boolean(sourceType && sourceRef && sourceType !== 'unknown');
}

function hasEvidence(evidence) {
  return Array.isArray(evidence) && evidence.some((item) => item && asText(item.statement) && hasAttributableSource(item));
}

function hasEvidenceType(evidence, types) {
  const wanted = new Set(types);
  return Array.isArray(evidence) && evidence.some((item) => wanted.has(asText(item.type).toLowerCase()) && hasAttributableSource(item));
}

function normalizeDerivation(value) {
  if (!value || typeof value !== 'object') return null;
  const basedOn = Array.isArray(value.basedOn)
    ? value.basedOn.map(asText).filter(Boolean)
    : Array.isArray(value.basedOnIds)
      ? value.basedOnIds.map(asText).filter(Boolean)
      : [];
  const evidenceRefs = Array.isArray(value.evidenceRefs)
    ? value.evidenceRefs.map(asText).filter(Boolean)
    : [];
  return {
    kind: asText(value.kind || value.type || 'inference'),
    explanation: asText(value.explanation || value.rationale || value.reason),
    basedOn,
    evidenceRefs,
    payload: value.payload && typeof value.payload === 'object' ? clone(value.payload) : {},
  };
}

function derivationIsComplete(derivation) {
  return Boolean(
    derivation
    && asText(derivation.explanation)
    && ((derivation.basedOn || []).length || (derivation.evidenceRefs || []).length)
  );
}

function normalizeProvenance(value) {
  if (!value || typeof value !== 'object') return {};
  return clone(value);
}

function normalizeEntityRef(value, label) {
  if (typeof value === 'string') {
    const id = asText(value);
    if (!id) throw knowledgeError('ak_relationship_entity_required', `Relationship ${label} is required.`);
    return { id };
  }
  if (!value || typeof value !== 'object') {
    throw knowledgeError('ak_relationship_entity_required', `Relationship ${label} is required.`);
  }
  const out = clone(value);
  if (!asText(out.id || out.ref || out.key || out.name)) {
    throw knowledgeError('ak_relationship_entity_required', `Relationship ${label} requires an id, ref, key, or name.`);
  }
  return out;
}

function normalizeRelationship(raw = {}, index = 0, parent = {}) {
  if (!raw || typeof raw !== 'object') return null;
  const predicate = asText(raw.predicate || raw.type || raw.relationshipType).toLowerCase();
  if (!predicate) throw knowledgeError('ak_relationship_predicate_required', 'Relationship predicate is required.');
  const evidence = normalizeEvidence(raw.evidence);
  const epistemicState = normalizeEpistemicState(raw.epistemicState || raw.epistemic_state, raw);
  const validationState = normalizeValidationState(raw.validationState || raw.validation_state, raw);
  const derivation = normalizeDerivation(raw.derivation);
  const relationship = {
    id: asText(raw.id) || `relationship_${index + 1}`,
    tenantId: assertTenant(raw.tenantId || parent.tenantId),
    type: predicate,
    predicate,
    source: normalizeEntityRef(raw.source || raw.subject || raw.from, 'source'),
    target: normalizeEntityRef(raw.target || raw.object || raw.to, 'target'),
    epistemicState,
    validationState,
    evidence,
    provenance: normalizeProvenance(raw.provenance),
    derivation,
    confidence: raw.confidence == null ? null : Math.min(1, Math.max(0, Number(raw.confidence))),
    version: Number(raw.version || 1),
    supersedesId: asText(raw.supersedesId) || null,
  };
  assertCanonicalSemantics(relationship, { relationship: true });
  return relationship;
}

function normalizeRelationships(value, parent = {}) {
  const rows = Array.isArray(value) ? value : (value ? [value] : []);
  return rows.map((row, index) => normalizeRelationship(row, index, parent)).filter(Boolean);
}

function assertCanonicalSemantics(row = {}, opts = {}) {
  const label = opts.relationship ? 'Relationship' : 'Claim';
  if (row.epistemicState === EPISTEMIC_STATES.OBSERVED && !hasEvidence(row.evidence)) {
    throw knowledgeError('ak_observed_provenance_required', `${label} OBSERVED state requires attributable source provenance.`);
  }
  if (row.epistemicState === EPISTEMIC_STATES.INFERRED) {
    if (!hasEvidence(row.evidence)) {
      throw knowledgeError('ak_inferred_evidence_required', `${label} INFERRED state requires attributable source evidence.`);
    }
    if (!derivationIsComplete(row.derivation)) {
      throw knowledgeError('ak_inferred_derivation_required', `${label} INFERRED state requires derivation metadata.`);
    }
  }
  if (row.validationState === VALIDATION_STATES.MARKET_VALIDATED
    && !hasEvidenceType(row.evidence, [EVIDENCE_TYPES.MARKET, EVIDENCE_TYPES.CAMPAIGN_OUTCOME])) {
    throw knowledgeError('ak_market_validation_evidence_required', 'MARKET_VALIDATED requires attributable market outcome evidence.');
  }
  if (row.validationState === VALIDATION_STATES.STAKEHOLDER_VALIDATED
    && !hasEvidenceType(row.evidence, [EVIDENCE_TYPES.STAKEHOLDER])) {
    throw knowledgeError('ak_stakeholder_validation_provenance_required', 'STAKEHOLDER_VALIDATED requires identifiable stakeholder validation provenance.');
  }
}

function assertTransition(currentState, nextState, evidence) {
  const from = normalizeLifecycleState(currentState);
  const to = normalizeLifecycleState(nextState);
  const fromIndex = LIFECYCLE_ORDER.indexOf(from);
  const toIndex = LIFECYCLE_ORDER.indexOf(to);
  if (toIndex === fromIndex) return { from, to };
  if (toIndex !== fromIndex + 1) {
    throw knowledgeError('ak_lifecycle_transition_invalid', `Lifecycle transition must be sequential: ${from} cannot become ${to}.`);
  }
  if (!evidence || !evidence.length) {
    throw knowledgeError('ak_lifecycle_evidence_required', `Evidence is required to promote ${from} to ${to}.`);
  }
  if (!evidenceMeetsState(evidence, to)) {
    throw knowledgeError('ak_lifecycle_evidence_insufficient', `Evidence does not support promotion to ${to}.`);
  }
  return { from, to };
}

function normalizeStatus(input = {}) {
  if (input.objectType === OBJECT_TYPES.OUTREACH_ASSET) {
    return asText(input.status || input.assetStatus || 'draft').toLowerCase();
  }
  if (input.reviewStatus) return asText(input.reviewStatus).toLowerCase();
  return input.state === LIFECYCLE_STATES.CANONICAL ? 'approved' : 'learning_candidate';
}

function normalizeKnowledgeObject(input = {}, opts = {}) {
  const tenantId = assertTenant(input.tenantId || opts.tenantId);
  const objectType = normalizeObjectType(input.objectType || input.type);
  const actorRole = normalizeRole(opts.actorRole || input.actorRole);
  if (!actorCanCreate(actorRole, objectType)) {
    throw knowledgeError('ak_role_forbidden', `${actorRole} cannot create ${objectType}.`);
  }
  const state = normalizeLifecycleState(input.state || input.lifecycleState);
  const evidence = normalizeEvidence(input.evidence);
  const epistemicState = normalizeEpistemicState(input.epistemicState || input.epistemic_state, input);
  const validationState = normalizeValidationState(input.validationState || input.validation_state, input);
  const derivation = normalizeDerivation(input.derivation);
  const scope = normalizeScope(input.scope, input);
  if (scope === SCOPES.MISSION && !asText(input.missionId || opts.missionId)) {
    throw knowledgeError('ak_mission_scope_requires_mission', 'Mission-scoped knowledge requires missionId.');
  }
  const title = asText(input.title || input.name || input.statement);
  if (!title) throw knowledgeError('ak_title_required', 'Knowledge title is required.');
  const content = input.content && typeof input.content === 'object'
    ? clone(input.content)
    : { statement: asText(input.statement || input.description || title) };
  const normalized = {
    id: asText(input.id) || newId('ak'),
    externalKey: asText(input.externalKey || input.createdFrom || input.sourceKey) || null,
    tenantId,
    clientId: input.clientId != null ? Number(input.clientId) : (Number.isFinite(Number(tenantId)) ? Number(tenantId) : null),
    missionId: asText(input.missionId || opts.missionId) || null,
    scope,
    objectType,
    title,
    content,
    epistemicState,
    validationState,
    epistemicKind: normalizeEpistemicKind(input.epistemicKind || input.kind, state),
    state,
    status: normalizeStatus({ ...input, objectType, state }),
    channel: asText(input.channel) || null,
    experimentId: asText(input.experimentId || input.experiment) || null,
    tags: Array.isArray(input.tags) ? input.tags.map(asText).filter(Boolean) : [],
    confidence: input.confidence == null ? null : Math.min(1, Math.max(0, Number(input.confidence))),
    evidence,
    provenance: normalizeProvenance(input.provenance),
    derivation,
    relationships: normalizeRelationships(input.relationships || input.relations, { tenantId }),
    approvedBy: asText(input.approvedBy) || null,
    createdFrom: asText(input.createdFrom || input.source) || null,
    createdBy: asText(input.createdBy || opts.actorId || actorRole) || actorRole,
    version: Number(input.version || 1),
    supersedesId: asText(input.supersedesId) || null,
    createdAt: nowIso(input.createdAt),
    updatedAt: nowIso(input.updatedAt),
  };
  assertCanonicalSemantics(normalized);
  return normalized;
}

function searchText(row = {}) {
  return [
    row.title,
    row.objectType,
    row.epistemicKind,
    row.epistemicState,
    row.validationState,
    row.state,
    row.status,
    ...(row.tags || []),
    JSON.stringify(row.content || {}),
    JSON.stringify(row.evidence || []),
    JSON.stringify(row.relationships || []),
  ].filter(Boolean).join(' ').toLowerCase();
}

function matchesQuery(row, query = {}) {
  if (!row) return false;
  if (query.tenantId != null && String(row.tenantId) !== String(query.tenantId)) return false;
  if (query.missionId != null && row.missionId && String(row.missionId) !== String(query.missionId)) return false;
  if (query.objectType && row.objectType !== normalizeObjectType(query.objectType)) return false;
  if (query.state && row.state !== normalizeLifecycleState(query.state)) return false;
  if (query.status && row.status !== asText(query.status).toLowerCase()) return false;
  if (query.scope && row.scope !== asText(query.scope).toLowerCase()) return false;
  if (query.channel && row.channel !== asText(query.channel)) return false;
  if (query.q && !searchText(row).includes(asText(query.q).toLowerCase())) return false;
  if (Array.isArray(query.tags) && query.tags.length) {
    const tags = new Set((row.tags || []).map(String));
    if (!query.tags.every((tag) => tags.has(String(tag)))) return false;
  }
  return true;
}

function explainRecommendation(input = {}) {
  const knowledge = Array.isArray(input.knowledge) ? input.knowledge : [];
  const recommendation = asText(input.recommendation || input.question || input.title) || 'Acquisition recommendation';
  return {
    spec: SPEC,
    recommendation,
    invented: false,
    basis: knowledge.map((row) => ({
      id: row.id,
      version: row.version,
      objectType: row.objectType,
      title: row.title,
      epistemicState: row.epistemicState,
      validationState: row.validationState,
      state: row.state,
      epistemicKind: row.epistemicKind,
      derivation: row.derivation || null,
      relationships: row.relationships || [],
      evidence: (row.evidence || []).map((item) => ({
        id: item.id,
        type: item.type,
        statement: item.statement,
        source: item.source,
        passage: item.passage,
        confidence: item.confidence,
        observedAt: item.observedAt,
      })),
    })),
    boundaries: [
      'Conversation messages are not canonical unless promoted through acquisition knowledge.',
      'Hypotheses remain hypotheses until explicit evidence-backed promotion.',
    ],
  };
}

function canonicalContextForSpecialist(rows = [], specialist = 'max') {
  const role = normalizeRole(specialist);
  const approvedAssets = rows.filter((row) =>
    row.objectType === OBJECT_TYPES.OUTREACH_ASSET
    && ['approved', 'canonical'].includes(String(row.status || '').toLowerCase())
  );
  const validatedPrinciples = rows.filter((row) =>
    [LIFECYCLE_STATES.STAKEHOLDER_VALIDATED, LIFECYCLE_STATES.EXPERIMENTALLY_SUPPORTED,
      LIFECYCLE_STATES.MARKET_VALIDATED, LIFECYCLE_STATES.CANONICAL].includes(row.state)
  );
  const hypotheses = rows.filter((row) => row.state === LIFECYCLE_STATES.HYPOTHESIS);
  return {
    spec: SPEC,
    role,
    canPromote: actorCanPromote(role),
    boundary: (AGENT_CAPABILITIES[role] || AGENT_CAPABILITIES.operator).note,
    approvedAssets,
    validatedPrinciples,
    hypotheses,
    retrieved: rows,
  };
}

module.exports = {
  SPEC,
  OBJECT_TYPES,
  OBJECT_TYPE_VALUES,
  SCOPES,
  EPISTEMIC_KINDS,
  EPISTEMIC_STATES,
  VALIDATION_STATES,
  LIFECYCLE_STATES,
  LIFECYCLE_ORDER,
  EVIDENCE_TYPES,
  AGENT_CAPABILITIES,
  clone,
  asText,
  nowIso,
  newId,
  knowledgeError,
  normalizeRole,
  actorCanCreate,
  actorCanPromote,
  assertTenant,
  normalizeScope,
  normalizeObjectType,
  normalizeLifecycleState,
  normalizeEpistemicKind,
  normalizeEpistemicState,
  normalizeValidationState,
  normalizeEvidence,
  normalizeDerivation,
  normalizeRelationships,
  assertCanonicalSemantics,
  assertTransition,
  normalizeKnowledgeObject,
  matchesQuery,
  explainRecommendation,
  canonicalContextForSpecialist,
};
