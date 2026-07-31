'use strict';

/**
 * Acquisition Provider Registry (SPEC-060).
 * Register providers once; Mission Planning / Workspace discover via metadata.
 */

const { assertProviderContract } = require('./providerContract');
const {
  createPlacesAcquisitionProvider,
} = require('./providers/PlacesAcquisitionProvider');
const {
  createManualProspectProvider,
} = require('./providers/ManualProspectProvider');
const { createCsvImportProvider } = require('./providers/CsvImportProvider');
const {
  createExistingProspectRepositoryProvider,
} = require('./providers/ExistingProspectRepositoryProvider');

class AcquisitionProviderRegistry {
  constructor() {
    /** @type {Map<string, object>} */
    this._providers = new Map();
  }

  /**
   * @param {object} provider
   */
  register(provider) {
    const check = assertProviderContract(provider);
    if (!check.ok) {
      throw new Error(
        `Invalid acquisition provider: ${check.errors.join('; ')}`
      );
    }
    const meta = provider.metadata();
    this._providers.set(meta.id, provider);
    return this;
  }

  /**
   * @param {string} id
   */
  get(id) {
    return this._providers.get(String(id)) || null;
  }

  /**
   * @returns {object[]}
   */
  list() {
    return [...this._providers.values()];
  }

  /**
   * @returns {object[]}
   */
  listMetadata() {
    return this.list().map((p) => p.metadata());
  }

  /**
   * @returns {object[]}
   */
  available() {
    return this.list().filter((p) => {
      try {
        return Boolean(p.available());
      } catch {
        return false;
      }
    });
  }

  /**
   * @returns {object[]}
   */
  healthReport() {
    return this.list().map((p) => {
      try {
        return p.health();
      } catch (err) {
        return {
          ok: false,
          status: 'error',
          provider: (p.metadata && p.metadata().id) || 'unknown',
          error: String(err && err.message ? err.message : err),
        };
      }
    });
  }
}

/**
 * Default registry with initial SPEC-060 providers.
 * @param {object} [deps]
 */
function createDefaultAcquisitionRegistry(deps = {}) {
  const registry = new AcquisitionProviderRegistry();
  registry.register(createPlacesAcquisitionProvider(deps));
  registry.register(createManualProspectProvider(deps));
  registry.register(createCsvImportProvider(deps));
  registry.register(createExistingProspectRepositoryProvider(deps));
  if (Array.isArray(deps.extraProviders)) {
    for (const p of deps.extraProviders) {
      registry.register(p);
    }
  }
  return registry;
}

module.exports = {
  AcquisitionProviderRegistry,
  createDefaultAcquisitionRegistry,
};
