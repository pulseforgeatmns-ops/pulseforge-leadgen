'use strict';

/**
 * Artifact Resolver — state-aware planning (SPEC-051 / ADR-035).
 *
 * Resolves required artifacts before capability selection.
 * Capabilities are acquisition strategies; artifacts are requirements.
 */

const {
  ARTIFACT_TYPES,
  resolveArtifactType,
  TYPE_TO_ALIAS,
} = require('./ArtifactRegistry');
const { getStage, listStages, stageLabel } = require('./StageLibrary');

const ARTIFACT_SOURCES = Object.freeze({
  CURRENT_MISSION: 'current_mission',
  OPERATOR_IMPORT: 'operator_import',
  OPERATOR_EXPLICIT: 'operator_explicit',
  PREVIOUS_MISSION: 'previous_mission',
  WORKSPACE: 'workspace',
  CAPABILITY_ACQUISITION: 'capability_acquisition',
});

const SOURCE_LABELS = Object.freeze({
  [ARTIFACT_SOURCES.CURRENT_MISSION]: 'Current Mission',
  [ARTIFACT_SOURCES.OPERATOR_IMPORT]: 'Operator Import',
  [ARTIFACT_SOURCES.OPERATOR_EXPLICIT]: 'Operator Explicit Selection',
  [ARTIFACT_SOURCES.PREVIOUS_MISSION]: 'Previous Mission',
  [ARTIFACT_SOURCES.WORKSPACE]: 'Persistent Workspace',
  [ARTIFACT_SOURCES.CAPABILITY_ACQUISITION]: 'Capability Acquisition',
});

const CONFIDENCE = Object.freeze({
  HIGH: 'High',
  MEDIUM: 'Medium',
  LOW: 'Low',
});

const FRESHNESS = Object.freeze({
  CURRENT_MISSION: 'Current Mission',
  OPERATOR: 'Operator Supplied',
  PREVIOUS_MISSION: 'Previous Mission',
  WORKSPACE: 'Workspace Cache',
  TO_ACQUIRE: 'To Acquire',
});

/** Lower = preferred. */
const SOURCE_PRIORITY = Object.freeze({
  [ARTIFACT_SOURCES.CURRENT_MISSION]: 1,
  [ARTIFACT_SOURCES.OPERATOR_EXPLICIT]: 2,
  [ARTIFACT_SOURCES.OPERATOR_IMPORT]: 3,
  [ARTIFACT_SOURCES.PREVIOUS_MISSION]: 4,
  [ARTIFACT_SOURCES.WORKSPACE]: 5,
  [ARTIFACT_SOURCES.CAPABILITY_ACQUISITION]: 10,
});

/** Acquisition cost for capability producers (lower = cheaper). */
const STAGE_ACQUISITION_COST = Object.freeze({
  prospect_discovery: 100,
  company_enrichment: 80,
  knowledge_update: 40,
  opportunity_ranking: 70,
  business_intelligence: 55,
  sales_intelligence: 60,
  campaign_builder: 90,
  mail_package_generator: 50,
  campaign_review: 30,
  direct_mail_execution: 110,
  outcome_intelligence: 40,
  operator_inbox: 10,
  proposal_generator: 50,
});

/**
 * Derive required artifact types from selected stages (union of consumes).
 *
 * @param {Iterable<string>|Map<string, string>} selectedStageIds
 * @returns {string[]} canonical ARTIFACT_TYPES names
 */
function deriveRequiredArtifacts(selectedStageIds) {
  const ids =
    selectedStageIds instanceof Map
      ? [...selectedStageIds.keys()]
      : [...(selectedStageIds || [])];
  const required = new Set();
  for (const stageId of ids) {
    const def = getStage(stageId);
    if (!def) continue;
    for (const alias of def.consumes || []) {
      const type = resolveArtifactType(alias);
      if (type) required.add(type);
    }
  }
  return [...required];
}

/**
 * Normalize a catalog entry into a resolution candidate.
 * @param {object} raw
 * @returns {object|null}
 */
function normalizeCandidate(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const type =
    resolveArtifactType(raw.type || raw.artifactType || raw.alias) || null;
  if (!type) return null;
  const source = normalizeSource(raw.source || raw.provenance);
  const confidence = normalizeConfidence(raw.confidence);
  const freshness =
    raw.freshness ||
    defaultFreshness(source) ||
    FRESHNESS.CURRENT_MISSION;
  const compatible =
    raw.compatible == null ? true : Boolean(raw.compatible);
  if (!compatible) return null;
  const createdAt = raw.createdAt || raw.updatedAt || null;
  return {
    type,
    source,
    confidence,
    freshness,
    compatible: true,
    pending: Boolean(raw.pending),
    artifactId: raw.artifactId || raw.id || null,
    revision: raw.revision != null ? Number(raw.revision) : null,
    payload: raw.payload || null,
    createdAt,
    stageId: raw.stageId || null,
    producer: raw.producer || null,
  };
}

/**
 * @param {string} source
 */
function normalizeSource(source) {
  const s = String(source || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '_');
  if (
    s === 'current_mission' ||
    s === 'mission' ||
    s === 'bus' ||
    s === 'artifact_bus'
  ) {
    return ARTIFACT_SOURCES.CURRENT_MISSION;
  }
  if (
    s === 'operator_import' ||
    s === 'operator' ||
    s === 'csv' ||
    s === 'csv_import' ||
    s === 'spreadsheet' ||
    s === 'spreadsheet_paste' ||
    s === 'manual' ||
    s === 'manual_entry' ||
    s === 'import'
  ) {
    return ARTIFACT_SOURCES.OPERATOR_IMPORT;
  }
  if (
    s === 'operator_explicit' ||
    s === 'operator_selection' ||
    s === 'explicit'
  ) {
    return ARTIFACT_SOURCES.OPERATOR_EXPLICIT;
  }
  if (s === 'previous_mission' || s === 'prior_mission' || s === 'prior') {
    return ARTIFACT_SOURCES.PREVIOUS_MISSION;
  }
  if (s === 'workspace' || s === 'persistent_workspace' || s === 'cache') {
    return ARTIFACT_SOURCES.WORKSPACE;
  }
  if (s === 'capability' || s === 'capability_acquisition' || s === 'acquire') {
    return ARTIFACT_SOURCES.CAPABILITY_ACQUISITION;
  }
  return ARTIFACT_SOURCES.CURRENT_MISSION;
}

/**
 * @param {unknown} value
 */
function normalizeConfidence(value) {
  if (value == null) return CONFIDENCE.HIGH;
  const s = String(value).trim().toLowerCase();
  if (s === 'high' || s === 'h') return CONFIDENCE.HIGH;
  if (s === 'medium' || s === 'med' || s === 'm') return CONFIDENCE.MEDIUM;
  if (s === 'low' || s === 'l') return CONFIDENCE.LOW;
  const n = Number(value);
  if (Number.isFinite(n)) {
    if (n >= 0.8) return CONFIDENCE.HIGH;
    if (n >= 0.5) return CONFIDENCE.MEDIUM;
    return CONFIDENCE.LOW;
  }
  return CONFIDENCE.HIGH;
}

function defaultFreshness(source) {
  if (source === ARTIFACT_SOURCES.CURRENT_MISSION) {
    return FRESHNESS.CURRENT_MISSION;
  }
  if (
    source === ARTIFACT_SOURCES.OPERATOR_IMPORT ||
    source === ARTIFACT_SOURCES.OPERATOR_EXPLICIT
  ) {
    return FRESHNESS.OPERATOR;
  }
  if (source === ARTIFACT_SOURCES.PREVIOUS_MISSION) {
    return FRESHNESS.PREVIOUS_MISSION;
  }
  if (source === ARTIFACT_SOURCES.WORKSPACE) {
    return FRESHNESS.WORKSPACE;
  }
  return FRESHNESS.TO_ACQUIRE;
}

function confidenceRank(c) {
  if (c === CONFIDENCE.HIGH) return 3;
  if (c === CONFIDENCE.MEDIUM) return 2;
  return 1;
}

/**
 * Build candidates from Mission Plan parameters (e.g. prospectList: current).
 * @param {object|null} missionPlan
 * @returns {object[]}
 */
function candidatesFromMissionPlan(missionPlan) {
  if (!missionPlan || !missionPlan.parameters) return [];
  const out = [];
  const pl = missionPlan.parameters.prospectList;
  if (pl === 'current' || pl === 'attached' || pl === 'operator') {
    out.push({
      type: ARTIFACT_TYPES.PROSPECT_LIST,
      source:
        pl === 'current'
          ? ARTIFACT_SOURCES.OPERATOR_EXPLICIT
          : ARTIFACT_SOURCES.OPERATOR_IMPORT,
      confidence: CONFIDENCE.HIGH,
      freshness:
        pl === 'current' ? FRESHNESS.CURRENT_MISSION : FRESHNESS.OPERATOR,
      compatible: true,
      pending: true,
      producer: 'operator',
    });
  }
  return out.map(normalizeCandidate).filter(Boolean);
}

/**
 * Extract candidates from an Artifact Bus snapshot or catalog array.
 * @param {object|object[]|null} catalog
 * @returns {object[]}
 */
function candidatesFromCatalog(catalog) {
  if (!catalog) return [];
  if (Array.isArray(catalog)) {
    return catalog.map(normalizeCandidate).filter(Boolean);
  }
  if (Array.isArray(catalog.artifacts)) {
    return catalog.artifacts.map(normalizeCandidate).filter(Boolean);
  }
  if (catalog.byType && typeof catalog.byType === 'object') {
    return Object.entries(catalog.byType)
      .map(([type, entry]) =>
        normalizeCandidate({
          ...(entry && typeof entry === 'object' ? entry : {}),
          type,
        })
      )
      .filter(Boolean);
  }
  return [];
}

/**
 * Rank candidates for a single artifact type (best first).
 * @param {object[]} candidates
 * @returns {object[]}
 */
function rankCandidates(candidates) {
  return [...candidates].sort((a, b) => {
    const sp =
      (SOURCE_PRIORITY[a.source] || 99) - (SOURCE_PRIORITY[b.source] || 99);
    if (sp !== 0) return sp;
    const cr = confidenceRank(b.confidence) - confidenceRank(a.confidence);
    if (cr !== 0) return cr;
    const at = a.createdAt ? Date.parse(a.createdAt) : 0;
    const bt = b.createdAt ? Date.parse(b.createdAt) : 0;
    return bt - at;
  });
}

/**
 * Find stages that produce a given artifact type.
 * @param {string} artifactType
 * @returns {object[]} stage defs sorted by acquisition cost
 */
function producersForArtifact(artifactType) {
  const alias = TYPE_TO_ALIAS[artifactType] || null;
  const stages = listStages().filter((s) => {
    if (!s.capabilityId) return false;
    const produces = s.produces || [];
    return produces.some((p) => {
      const t = resolveArtifactType(p);
      return t === artifactType || (alias && p === alias);
    });
  });
  return stages.sort((a, b) => {
    const ca = STAGE_ACQUISITION_COST[a.id] ?? 500;
    const cb = STAGE_ACQUISITION_COST[b.id] ?? 500;
    return ca - cb;
  });
}

/**
 * Resolve required artifacts against a catalog + Mission Plan.
 *
 * @param {object} input
 * @param {string[]} [input.required]
 * @param {Iterable<string>|Map} [input.selectedStages]
 * @param {object|object[]} [input.availableArtifacts]
 * @param {object|object[]} [input.previousMissionArtifacts]
 * @param {object|object[]} [input.workspaceArtifacts]
 * @param {object} [input.missionPlan]
 * @returns {object} resolution result
 */
function resolveArtifacts(input = {}) {
  const selected = input.selectedStages;
  const required = [
    ...(input.required && input.required.length
      ? input.required.map((t) => resolveArtifactType(t) || t).filter(Boolean)
      : deriveRequiredArtifacts(selected)),
  ];
  const requiredUnique = [...new Set(required)];

  const pool = [
    ...candidatesFromCatalog(input.availableArtifacts).map((c) => ({
      ...c,
      source:
        c.source === ARTIFACT_SOURCES.CAPABILITY_ACQUISITION
          ? ARTIFACT_SOURCES.CURRENT_MISSION
          : c.source,
    })),
    ...candidatesFromMissionPlan(input.missionPlan),
    ...candidatesFromCatalog(input.previousMissionArtifacts).map((c) => ({
      ...c,
      source: ARTIFACT_SOURCES.PREVIOUS_MISSION,
      freshness: c.freshness || FRESHNESS.PREVIOUS_MISSION,
    })),
    ...candidatesFromCatalog(input.workspaceArtifacts).map((c) => ({
      ...c,
      source: ARTIFACT_SOURCES.WORKSPACE,
      freshness: c.freshness || FRESHNESS.WORKSPACE,
    })),
  ];

  const resolved = [];
  const missing = [];
  const acquisitions = [];
  /** @type {Record<string, string>} */
  const skippedStages = {};
  const acquireStageIds = [];

  for (const artifactType of requiredUnique) {
    const matches = rankCandidates(pool.filter((c) => c.type === artifactType));
    const best = matches[0] || null;

    if (best && best.source !== ARTIFACT_SOURCES.CAPABILITY_ACQUISITION) {
      resolved.push({
        type: artifactType,
        source: best.source,
        sourceLabel: SOURCE_LABELS[best.source] || best.source,
        confidence: best.confidence,
        freshness: best.freshness,
        compatible: true,
        pending: Boolean(best.pending),
        artifactId: best.artifactId,
        revision: best.revision,
        producer: best.producer,
      });

      for (const producer of producersForArtifact(artifactType)) {
        skippedStages[producer.id] =
          `Compatible ${artifactType} already exists (${SOURCE_LABELS[best.source] || best.source})`;
        acquisitions.push({
          artifactType,
          strategy: 'use_existing',
          stageId: null,
          skippedStageId: producer.id,
          reason: skippedStages[producer.id],
          cost: 0,
        });
      }
      continue;
    }

    missing.push(artifactType);
    const producers = producersForArtifact(artifactType);
    const chosen = producers[0] || null;
    if (chosen) {
      acquireStageIds.push(chosen.id);
      acquisitions.push({
        artifactType,
        strategy: 'capability_acquisition',
        stageId: chosen.id,
        stageName: chosen.name,
        capabilityId: chosen.capabilityId,
        reason: `No compatible ${artifactType}; acquire via ${chosen.name}`,
        cost: STAGE_ACQUISITION_COST[chosen.id] ?? 500,
        alternatives: producers.slice(1).map((p) => ({
          stageId: p.id,
          name: p.name,
          cost: STAGE_ACQUISITION_COST[p.id] ?? 500,
        })),
      });
    } else {
      acquisitions.push({
        artifactType,
        strategy: 'unavailable',
        stageId: null,
        reason: `No acquisition capability registered for ${artifactType}`,
        cost: Infinity,
      });
    }
  }

  return {
    required: requiredUnique,
    resolved,
    missing,
    acquisitions,
    skippedStages,
    acquireStageIds: [...new Set(acquireStageIds)],
    summary: buildResolutionSummary({
      required: requiredUnique,
      resolved,
      missing,
      acquisitions,
      skippedStages,
    }),
  };
}

/**
 * Apply resolution to a selected/skipped stage map (mutates maps).
 *
 * @param {Map<string, string>} selected
 * @param {Map<string, string>} skipped
 * @param {object} resolution
 * @param {object} [opts]
 * @param {Set<string>} [opts.protectedStages]
 */
function applyResolutionToSelection(selected, skipped, resolution, opts = {}) {
  const protectedStages = opts.protectedStages || new Set();
  const res = resolution || { skippedStages: {}, resolved: [] };

  for (const [stageId, reason] of Object.entries(res.skippedStages || {})) {
    if (protectedStages.has(stageId)) continue;
    if (!selected.has(stageId)) {
      skipped.set(stageId, reason);
      continue;
    }
    const def = getStage(stageId);
    if (!def) continue;
    const produced = (def.produces || [])
      .map((a) => resolveArtifactType(a))
      .filter(Boolean);
    if (!produced.length) continue;
    const allSatisfied = produced.every((t) =>
      (res.resolved || []).some((r) => r.type === t)
    );
    if (!allSatisfied) continue;

    selected.delete(stageId);
    skipped.set(stageId, reason);
  }

  return { selected, skipped };
}

function buildResolutionSummary(parts) {
  const lines = [];
  for (const r of parts.resolved || []) {
    lines.push(
      `${r.type}: ${r.sourceLabel || r.source}` +
        (r.pending ? ' (pending supply)' : '') +
        ' — acquisition skipped'
    );
  }
  for (const a of parts.acquisitions || []) {
    if (a.strategy === 'use_existing') continue;
    if (a.strategy === 'capability_acquisition') {
      lines.push(`${a.artifactType}: acquire via ${a.stageName || a.stageId}`);
    } else if (a.strategy === 'unavailable') {
      lines.push(`${a.artifactType}: no acquisition path`);
    }
  }
  for (const [stageId, reason] of Object.entries(parts.skippedStages || {})) {
    lines.push(`Skip ${stageLabel(stageId)}: ${reason}`);
  }
  return lines;
}

/**
 * Operator-facing acquisition options when an artifact is missing.
 * @param {string} artifactType
 * @returns {object[]}
 */
function acquisitionOptions(artifactType) {
  const type = resolveArtifactType(artifactType) || artifactType;
  const options = [];
  if (type === ARTIFACT_TYPES.PROSPECT_LIST) {
    options.push(
      { id: 'prospect_discovery', label: 'Discovery', strategy: 'capability' },
      { id: 'import_csv', label: 'Import CSV', strategy: 'operator_import' },
      {
        id: 'upload_spreadsheet',
        label: 'Upload Spreadsheet',
        strategy: 'operator_import',
      }
    );
  }
  for (const p of producersForArtifact(type)) {
    if (!options.some((o) => o.id === p.id)) {
      options.push({
        id: p.id,
        label: p.name,
        strategy: 'capability',
        cost: STAGE_ACQUISITION_COST[p.id] ?? 500,
      });
    }
  }
  return options;
}

module.exports = {
  ARTIFACT_SOURCES,
  SOURCE_LABELS,
  SOURCE_PRIORITY,
  CONFIDENCE,
  FRESHNESS,
  STAGE_ACQUISITION_COST,
  deriveRequiredArtifacts,
  normalizeCandidate,
  candidatesFromMissionPlan,
  candidatesFromCatalog,
  rankCandidates,
  producersForArtifact,
  resolveArtifacts,
  applyResolutionToSelection,
  acquisitionOptions,
  buildResolutionSummary,
};
