'use strict';

const { DEFAULT_RUNTIME_VERSION } = require('@pulseforge/replay');

/**
 * Laboratory-aware bundle resolver (SPEC-019).
 *
 * Extends the market default so experiments can inject alternate Strategy Pack
 * or Ontology descriptors without touching production wiring.
 *
 * @param {import('@pulseforge/replay').ReplayRunInput} input
 */
function labResolveBundle(input) {
  const ontologyKey =
    typeof input.ontology === 'string' ? input.ontology : input.ontology?.id;
  const packIsObject =
    input.strategyPack &&
    typeof input.strategyPack === 'object' &&
    typeof input.strategyPack.initialize === 'function';

  // Injectable strategy pack object — always honored in the laboratory.
  if (packIsObject) {
    return buildBundleFromPack(input, input.strategyPack);
  }

  // Ontology object with embedded pack / providers.
  if (input.ontology && typeof input.ontology === 'object') {
    if (typeof input.ontology.strategyPack?.initialize === 'function') {
      return buildBundleFromPack(input, input.ontology.strategyPack, {
        ontologyVersion: ontologyVersionLabel(input.ontology),
      });
    }
    if (typeof input.ontology.createRuntime === 'function') {
      return {
        domain: input.ontology.domain || ontologyKey || 'market',
        strategyPack: input.ontology.strategyPack,
        contextProvider: input.ontology.contextProvider || input.contextProvider,
        recommendationProvider: input.ontology.recommendationProvider,
        versions: {
          ontology: ontologyVersionLabel(input.ontology),
          strategyPack: packVersionLabel(input.strategyPack),
          runtime:
            input.runtimeVersion ||
            input.ontology.runtimeVersion ||
            DEFAULT_RUNTIME_VERSION,
        },
        createRuntime: input.ontology.createRuntime.bind(input.ontology),
      };
    }
  }

  // Fall through to market default for string "market" (and bare defaults).
  return defaultMarketBundle(input);
}

/**
 * @param {object} input
 * @param {object} strategyPack
 * @param {object} [opts]
 */
function buildBundleFromPack(input, strategyPack, opts = {}) {
  const {
    createReasoningRuntime,
  } = require('@pulseforge/reasoning-runtime');
  const {
    createMarketContextProvider,
    createResearchRecommendationProvider,
  } = require('@pulseforge/market-strategy');

  const recommendationProvider =
    input.recommendationProvider ||
    strategyPack.recommendationProvider ||
    (strategyPack._recommendationProvider
      ? strategyPack._recommendationProvider
      : createResearchRecommendationProvider());

  const contextProvider =
    input.contextProvider ||
    strategyPack.contextProvider ||
    createMarketContextProvider();

  let runtimeVersion = DEFAULT_RUNTIME_VERSION;
  try {
    runtimeVersion = require('@pulseforge/reasoning-runtime/package.json').version;
  } catch {
    /* optional */
  }

  return {
    domain: strategyPack.domain || 'market',
    strategyPack,
    contextProvider,
    recommendationProvider,
    versions: {
      ontology: opts.ontologyVersion || ontologyVersionLabel(input.ontology),
      strategyPack: packVersionLabel(strategyPack),
      runtime: input.runtimeVersion || runtimeVersion,
    },
    createRuntime({
      strategyPack: pack,
      contextProvider: ctx,
      recommendationProvider: rec,
      clock,
    }) {
      return createReasoningRuntime({
        strategyPack: pack,
        contextProvider: ctx,
        recommendationProvider: rec,
        clock,
      });
    },
  };
}

/**
 * Delegate to ReplayEngine's built-in market resolver without re-implementing it.
 * We duplicate the market wiring here so laboratory does not depend on private exports.
 * @param {object} input
 */
function defaultMarketBundle(input) {
  const ontologyKey =
    typeof input.ontology === 'string' ? input.ontology : input.ontology?.id;
  const packKey =
    typeof input.strategyPack === 'string'
      ? input.strategyPack
      : input.strategyPack?.id;

  if (ontologyKey && ontologyKey !== 'market') {
    throw new Error(
      `EvidenceLab: unsupported ontology "${ontologyKey}" — pass an ontology object or inject resolveBundle`
    );
  }
  if (packKey && packKey !== 'market') {
    throw new Error(
      `EvidenceLab: unsupported strategyPack "${packKey}" — pass a StrategyPack object or inject resolveBundle`
    );
  }

  const {
    createReasoningRuntime,
  } = require('@pulseforge/reasoning-runtime');
  const {
    createMarketStrategyPack,
    createMarketContextProvider,
    createResearchRecommendationProvider,
  } = require('@pulseforge/market-strategy');

  let ontologyVersion = '1.0.0';
  let strategyPackVersion = '1.0.0';
  let runtimeVersion = DEFAULT_RUNTIME_VERSION;
  try {
    ontologyVersion = require('@pulseforge/market-ontology/package.json').version;
  } catch {
    /* optional */
  }
  try {
    strategyPackVersion = require('@pulseforge/market-strategy/package.json').version;
  } catch {
    /* optional */
  }
  try {
    runtimeVersion = require('@pulseforge/reasoning-runtime/package.json').version;
  } catch {
    /* optional */
  }

  // Allow ontology string overrides like "market@2.0.0" via object { id, version }.
  const ontologyLabel =
    typeof input.ontology === 'object' && input.ontology
      ? ontologyVersionLabel(input.ontology)
      : `market@${ontologyVersion}`;

  const recommendationProvider = createResearchRecommendationProvider();
  const strategyPack = createMarketStrategyPack({ recommendationProvider });
  const contextProvider =
    input.contextProvider || createMarketContextProvider();

  return {
    domain: 'market',
    strategyPack,
    contextProvider,
    recommendationProvider,
    versions: {
      ontology: ontologyLabel,
      strategyPack: `market@${strategyPackVersion}`,
      runtime: runtimeVersion,
    },
    createRuntime({ strategyPack: pack, contextProvider: ctx, recommendationProvider: rec, clock }) {
      return createReasoningRuntime({
        strategyPack: pack,
        contextProvider: ctx,
        recommendationProvider: rec,
        clock,
      });
    },
  };
}

/**
 * @param {string|object|null|undefined} ontology
 */
function ontologyVersionLabel(ontology) {
  if (ontology == null) return 'market@unknown';
  if (typeof ontology === 'string') {
    return ontology.includes('@') ? ontology : `market@${ontology === 'market' ? 'default' : ontology}`;
  }
  const id = ontology.id || 'market';
  const version = ontology.version || ontology.revision || 'unknown';
  return `${id}@${version}`;
}

/**
 * @param {string|object|null|undefined} pack
 */
function packVersionLabel(pack) {
  if (pack == null) return 'unknown';
  if (typeof pack === 'string') {
    return pack.includes('@') ? pack : `${pack}@default`;
  }
  const id = pack.id || 'pack';
  const version = pack.version || pack.revision || '1';
  return `${id}@${version}`;
}

module.exports = {
  labResolveBundle,
  ontologyVersionLabel,
  packVersionLabel,
};
