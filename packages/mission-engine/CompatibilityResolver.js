'use strict';

/**
 * Compatibility Resolver — rank registry producers for required artifacts
 * (SPEC-054 / ADR-038). Planner queries the Capability Registry; never code.
 */

const {
  resolveArtifactType,
  TYPE_TO_ALIAS,
} = require('./ArtifactRegistry');
const {
  buildMissingProducerDiagnostic,
  EXPECTED_PRODUCER_HINTS,
} = require('./PlanningDiagnostics');
const { DEFAULT_ACQUISITION_COST } = require('../capabilities/CapabilityRegistry');

/**
 * Resolve ranked producers for an artifact from the Capability Registry.
 *
 * @param {string} artifactType - PascalCase bus type or snake_case alias
 * @param {object} [opts]
 * @param {import('../capabilities').CapabilityRegistry} [opts.registry]
 * @param {number} [opts.version]
 * @returns {{
 *   ok: boolean,
 *   artifactType: string,
 *   chosen: object|null,
 *   ranked: object[],
 *   disabled: object[],
 *   rankingLosses: object[],
 *   diagnostic: object|null
 * }}
 */
function resolveCompatibleProducer(artifactType, opts = {}) {
  const registry = opts.registry || null;
  const type =
    resolveArtifactType(artifactType) || String(artifactType || '').trim();
  const alias = TYPE_TO_ALIAS[type] || null;

  if (!registry) {
    const diagnostic = buildMissingProducerDiagnostic({
      artifact: type,
      expectedProducer: EXPECTED_PRODUCER_HINTS[type] || EXPECTED_PRODUCER_HINTS[alias],
      recommendedAction:
        'Pass the Capability Registry to the Compatibility Resolver so producers can be discovered.',
    });
    return {
      ok: false,
      artifactType: type,
      chosen: null,
      ranked: [],
      disabled: [],
      rankingLosses: [],
      diagnostic,
    };
  }

  const queryKeys = [type, alias].filter(Boolean);
  /** @type {Map<string, object>} */
  const byId = new Map();
  for (const key of queryKeys) {
    for (const cap of registry.producersOf(key, {
      enabledOnly: false,
      version: opts.version,
    })) {
      byId.set(cap.id, cap);
    }
    // Also try without version filter to detect version mismatch
    if (opts.version != null) {
      for (const cap of registry.producersOf(key, { enabledOnly: false })) {
        if (!byId.has(cap.id)) byId.set(cap.id, { ...cap, _versionMiss: true });
      }
    }
  }

  const all = [...byId.values()];
  const disabled = all.filter((c) => c.enabled === false);
  const versionMismatched = all.filter((c) => c._versionMiss);
  const ranked = all
    .filter((c) => c.enabled !== false && !c._versionMiss)
    .map((c) => ({
      capabilityId: c.id,
      name: c.name,
      version: c.version,
      enabled: c.enabled !== false,
      cost:
        Number.isFinite(Number(c.acquisitionCost))
          ? Number(c.acquisitionCost)
          : DEFAULT_ACQUISITION_COST[c.id] ?? 500,
      produces: c.produces || [],
    }))
    .sort((a, b) => a.cost - b.cost);

  const chosen = ranked[0] || null;
  const rankingLosses = ranked.slice(1).map((candidate) =>
    registry.explainSelection(candidate.capabilityId, {
      artifactType: type,
      selectedId: chosen && chosen.capabilityId,
    })
  );

  if (chosen) {
    return {
      ok: true,
      artifactType: type,
      chosen,
      ranked,
      disabled,
      rankingLosses,
      diagnostic: null,
    };
  }

  const versionMismatch = versionMismatched.length > 0 && ranked.length === 0;
  const diagnostic = buildMissingProducerDiagnostic({
    artifact: type,
    expectedProducer:
      EXPECTED_PRODUCER_HINTS[type] ||
      EXPECTED_PRODUCER_HINTS[alias] ||
      null,
    registeredProducers: [],
    disabledProducers: disabled.map((d) => ({ id: d.id, name: d.name })),
    versionMismatch,
    recommendedAction: disabled.length
      ? `Enable ${disabled.map((d) => d.name).join(' or ')} to produce ${type}.`
      : versionMismatch
        ? `Register a version-compatible producer for ${type}.`
        : `Register a capability that produces ${type}.`,
  });

  return {
    ok: false,
    artifactType: type,
    chosen: null,
    ranked: [],
    disabled: disabled.map((d) => ({
      capabilityId: d.id,
      name: d.name,
    })),
    rankingLosses: [],
    diagnostic,
  };
}

/**
 * Resolve producers for many artifact types.
 * @param {string[]} artifactTypes
 * @param {object} [opts]
 */
function resolveCompatibleProducers(artifactTypes, opts = {}) {
  const results = {};
  for (const t of artifactTypes || []) {
    results[t] = resolveCompatibleProducer(t, opts);
  }
  return results;
}

module.exports = {
  resolveCompatibleProducer,
  resolveCompatibleProducers,
};
