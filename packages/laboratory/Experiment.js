'use strict';

const { createHash, randomUUID } = require('crypto');
const {
  normalizeObservationList,
  toImmutableObservation,
} = require('@pulseforge/replay');

/**
 * Experiment — isolated, disposable laboratory scenario (SPEC-019).
 *
 * Holds a frozen baseline observation set plus local scenario mutations.
 * Never writes to production stores. Cloning always yields a new experiment.
 */
class Experiment {
  /**
   * @param {object} seed
   * @param {string} [seed.id]
   * @param {string} [seed.name]
   * @param {string} seed.subjectId
   * @param {string} [seed.startTime]
   * @param {string} [seed.endTime]
   * @param {string|object} [seed.ontology]
   * @param {string|object} [seed.strategyPack]
   * @param {string} [seed.runtimeVersion]
   * @param {string} [seed.domain]
   * @param {object[]} [seed.observations]
   * @param {string} [seed.hypothesis]
   * @param {Record<string, unknown>} [seed.metadata]
   * @param {string[]} [seed.removedObservationIds]
   * @param {object[]} [seed.injectedObservations]
   * @param {string} [seed.parentId]
   */
  constructor(seed = {}) {
    if (!seed.subjectId) {
      throw new Error('Experiment requires subjectId');
    }

    const domain = String(
      seed.domain ||
        (typeof seed.ontology === 'string' ? seed.ontology : null) ||
        (typeof seed.strategyPack === 'string' ? seed.strategyPack : null) ||
        'market'
    );

    const baseline = normalizeObservationList(seed.observations || [], {
      subjectId: String(seed.subjectId),
      domain,
      startTime: seed.startTime || null,
      endTime: seed.endTime || null,
    });

    this.id = String(seed.id || `exp_${randomUUID().replace(/-/g, '').slice(0, 16)}`);
    this.name = String(seed.name || `experiment:${this.id}`);
    this.subjectId = String(seed.subjectId);
    this.startTime = seed.startTime ? String(seed.startTime) : null;
    this.endTime = seed.endTime ? String(seed.endTime) : null;
    this.ontology = seed.ontology != null ? seed.ontology : 'market';
    this.strategyPack = seed.strategyPack != null ? seed.strategyPack : 'market';
    this.runtimeVersion = seed.runtimeVersion ? String(seed.runtimeVersion) : null;
    this.domain = domain;
    this.hypothesis = seed.hypothesis ? String(seed.hypothesis) : null;
    this.metadata = Object.freeze({ ...(seed.metadata || {}) });
    this.parentId = seed.parentId ? String(seed.parentId) : null;
    this.createdAt = String(seed.createdAt || new Date().toISOString());
    this.status = 'open';

    /** @type {import('@pulseforge/replay').ImmutableObservation[]} */
    this._baseline = Object.freeze(baseline.slice());

    /** @type {Set<string>} */
    this._removedIds = new Set(
      (seed.removedObservationIds || []).map((id) => String(id))
    );

    /** @type {object[]} */
    this._injected = Object.freeze(
      (seed.injectedObservations || []).map((raw) =>
        toImmutableObservation(raw, { subjectId: this.subjectId, domain })
      )
    );

    Object.freeze(this);
  }

  /** @returns {boolean} */
  get isIsolated() {
    return true;
  }

  /** @returns {boolean} */
  get mutatesProduction() {
    return false;
  }

  /**
   * Baseline observations before scenario mutations.
   * @returns {object[]}
   */
  baselineObservations() {
    return this._baseline.slice();
  }

  /**
   * Effective observation set after remove/inject mutations.
   * @returns {object[]}
   */
  getObservations() {
    const kept = this._baseline.filter((obs) => !this._removedIds.has(obs.id));
    const injected = this._injected.filter((obs) => !this._removedIds.has(obs.id));
    return normalizeObservationList([...kept, ...injected], {
      subjectId: this.subjectId,
      domain: this.domain,
      startTime: this.startTime,
      endTime: this.endTime,
    });
  }

  /**
   * @returns {string[]}
   */
  removedObservationIds() {
    return [...this._removedIds].sort();
  }

  /**
   * @returns {object[]}
   */
  injectedObservations() {
    return this._injected.slice();
  }

  /**
   * Deterministic fingerprint of the effective observation set + versions.
   * @returns {string}
   */
  fingerprint() {
    const obsIds = this.getObservations().map((o) => o.id);
    const payload = {
      subjectId: this.subjectId,
      ontology: versionKey(this.ontology),
      strategyPack: versionKey(this.strategyPack),
      runtimeVersion: this.runtimeVersion,
      startTime: this.startTime,
      endTime: this.endTime,
      observations: obsIds,
      removed: this.removedObservationIds(),
      injected: this._injected.map((o) => o.id),
    };
    return createHash('sha256')
      .update(JSON.stringify(payload))
      .digest('hex')
      .slice(0, 24);
  }

  /**
   * Snapshot for inspection / ComparisonWorkspace (no live references).
   * @returns {object}
   */
  snapshot() {
    return Object.freeze({
      id: this.id,
      name: this.name,
      subjectId: this.subjectId,
      startTime: this.startTime,
      endTime: this.endTime,
      ontology: versionKey(this.ontology),
      strategyPack: versionKey(this.strategyPack),
      runtimeVersion: this.runtimeVersion,
      domain: this.domain,
      hypothesis: this.hypothesis,
      metadata: { ...this.metadata },
      parentId: this.parentId,
      createdAt: this.createdAt,
      status: this.status,
      fingerprint: this.fingerprint(),
      observationCount: this.getObservations().length,
      removedObservationIds: this.removedObservationIds(),
      injectedObservationIds: this._injected.map((o) => o.id),
      isolated: true,
      mutatesProduction: false,
    });
  }

  /**
   * ReplayEngine.run input derived from this experiment.
   * @returns {object}
   */
  toReplayInput() {
    return {
      subjectId: this.subjectId,
      startTime: this.startTime || undefined,
      endTime: this.endTime || undefined,
      ontology: this.ontology,
      strategyPack: this.strategyPack,
      runtimeVersion: this.runtimeVersion || undefined,
      observations: this.getObservations(),
    };
  }

  /**
   * Derive a child experiment with additional removals (copy-on-write).
   * @param {string|string[]} observationIds
   * @param {object} [opts]
   * @returns {Experiment}
   */
  withRemoved(observationIds, opts = {}) {
    const ids = Array.isArray(observationIds) ? observationIds : [observationIds];
    const nextRemoved = new Set(this._removedIds);
    for (const id of ids) {
      if (id == null || id === '') {
        throw new Error('withRemoved requires observation id(s)');
      }
      nextRemoved.add(String(id));
    }
    return new Experiment({
      ...this._seedFields(),
      id: opts.id,
      name: opts.name || `${this.name}:remove`,
      parentId: this.id,
      removedObservationIds: [...nextRemoved],
      injectedObservations: this._injected.slice(),
      hypothesis: opts.hypothesis || this.hypothesis,
      metadata: { ...this.metadata, ...(opts.metadata || {}), mutation: 'removeObservation' },
    });
  }

  /**
   * Derive a child experiment with an injected observation (copy-on-write).
   * @param {object|object[]} observations
   * @param {object} [opts]
   * @returns {Experiment}
   */
  withInjected(observations, opts = {}) {
    const list = Array.isArray(observations) ? observations : [observations];
    if (list.length === 0) {
      throw new Error('withInjected requires at least one observation');
    }
    return new Experiment({
      ...this._seedFields(),
      id: opts.id,
      name: opts.name || `${this.name}:inject`,
      parentId: this.id,
      removedObservationIds: [...this._removedIds],
      injectedObservations: [...this._injected, ...list],
      hypothesis: opts.hypothesis || this.hypothesis,
      metadata: { ...this.metadata, ...(opts.metadata || {}), mutation: 'injectObservation' },
    });
  }

  /**
   * Derive a child with alternate ontology / strategy pack / window.
   * @param {object} patch
   * @returns {Experiment}
   */
  withConfig(patch = {}) {
    return new Experiment({
      ...this._seedFields(),
      id: patch.id,
      name: patch.name || `${this.name}:config`,
      parentId: this.id,
      ontology: patch.ontology != null ? patch.ontology : this.ontology,
      strategyPack: patch.strategyPack != null ? patch.strategyPack : this.strategyPack,
      runtimeVersion:
        patch.runtimeVersion != null ? patch.runtimeVersion : this.runtimeVersion,
      startTime: patch.startTime != null ? patch.startTime : this.startTime,
      endTime: patch.endTime != null ? patch.endTime : this.endTime,
      hypothesis: patch.hypothesis != null ? patch.hypothesis : this.hypothesis,
      metadata: { ...this.metadata, ...(patch.metadata || {}), mutation: 'config' },
      removedObservationIds: [...this._removedIds],
      injectedObservations: this._injected.slice(),
      observations: this._baseline.slice(),
    });
  }

  /** @private */
  _seedFields() {
    return {
      subjectId: this.subjectId,
      startTime: this.startTime,
      endTime: this.endTime,
      ontology: this.ontology,
      strategyPack: this.strategyPack,
      runtimeVersion: this.runtimeVersion,
      domain: this.domain,
      observations: this._baseline.slice(),
      createdAt: new Date().toISOString(),
    };
  }
}

/**
 * @param {string|object|null|undefined} value
 * @returns {string}
 */
function versionKey(value) {
  if (value == null) return 'unknown';
  if (typeof value === 'string') return value;
  if (typeof value === 'object') {
    return String(value.id || value.version || value.name || 'object');
  }
  return String(value);
}

/**
 * @param {object} seed
 * @returns {Experiment}
 */
function createExperiment(seed) {
  return new Experiment(seed);
}

module.exports = {
  Experiment,
  createExperiment,
  versionKey,
};
