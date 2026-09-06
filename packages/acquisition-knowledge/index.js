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
  const scope = normalizeScope(input.scope, input);
  if (scope === SCOPES.MISSION && !asText(input.missionId || opts.missionId)) {
    throw knowledgeError('ak_mission_scope_requires_mission', 'Mission-scoped knowledge requires missionId.');
  }
  const title = asText(input.title || input.name || input.statement);
  if (!title) throw knowledgeError('ak_title_required', 'Knowledge title is required.');
  const content = input.content && typeof input.content === 'object'
    ? clone(input.content)
    : { statement: asText(input.statement || input.description || title) };
  return {
    id: asText(input.id) || newId('ak'),
    externalKey: asText(input.externalKey || input.createdFrom || input.sourceKey) || null,
    tenantId,
    clientId: input.clientId != null ? Number(input.clientId) : (Number.isFinite(Number(tenantId)) ? Number(tenantId) : null),
    missionId: asText(input.missionId || opts.missionId) || null,
    scope,
    objectType,
    title,
    content,
    epistemicKind: normalizeEpistemicKind(input.epistemicKind || input.kind, state),
    state,
    status: normalizeStatus({ ...input, objectType, state }),
    channel: asText(input.channel) || null,
    experimentId: asText(input.experimentId || input.experiment) || null,
    tags: Array.isArray(input.tags) ? input.tags.map(asText).filter(Boolean) : [],
    confidence: input.confidence == null ? null : Math.min(1, Math.max(0, Number(input.confidence))),
    evidence,
    provenance: input.provenance && typeof input.provenance === 'object' ? clone(input.provenance) : {},
    approvedBy: asText(input.approvedBy) || null,
    createdFrom: asText(input.createdFrom || input.source) || null,
    createdBy: asText(input.createdBy || opts.actorId || actorRole) || actorRole,
    version: Number(input.version || 1),
    supersedesId: asText(input.supersedesId) || null,
    createdAt: nowIso(input.createdAt),
    updatedAt: nowIso(input.updatedAt),
  };
}

function searchText(row = {}) {
  return [
    row.title,
    row.objectType,
    row.epistemicKind,
    row.state,
    row.status,
    ...(row.tags || []),
    JSON.stringify(row.content || {}),
    JSON.stringify(row.evidence || []),
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
      state: row.state,
      epistemicKind: row.epistemicKind,
      evidence: (row.evidence || []).map((item) => ({
        id: item.id,
        type: item.type,
        statement: item.statement,
        source: item.source,
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
  normalizeEvidence,
  assertTransition,
  normalizeKnowledgeObject,
  matchesQuery,
  explainRecommendation,
  canonicalContextForSpecialist,
};
