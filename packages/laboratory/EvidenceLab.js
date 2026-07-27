'use strict';

const { createReplayEngine, createReplayComparator } = require('@pulseforge/replay');
const {
  createEqlEngine,
  createEvidenceCatalog,
  catalogFromResult,
  parseEql,
} = require('@pulseforge/eql');
const { Experiment, createExperiment } = require('./Experiment');
const { ScenarioRunner, createScenarioRunner } = require('./ScenarioRunner');
const { EvidenceQuery, createEvidenceQuery } = require('./EvidenceQuery');
const {
  ComparisonWorkspace,
  createComparisonWorkspace,
} = require('./ComparisonWorkspace');
const { labResolveBundle } = require('./resolveBundle');

/**
 * EvidenceLab — exploratory environment for the Evidence Platform (SPEC-019).
 *
 * Guiding principle: The Laboratory asks questions. The Evidence Platform answers.
 *
 * Not paper trading. Experiments are isolated. Nothing produced here affects production.
 *
 * @example
 *   const lab = createEvidenceLab();
 *   const exp = lab.createExperiment({ subjectId: 'BTC', observations });
 *   const withoutY = lab.removeObservation({ experiment: exp, observationId: '…' });
 *   const comparison = await lab.compareReplay({ left: exp, right: withoutY });
 */
class EvidenceLab {
  /**
   * @param {object} [deps]
   * @param {{ run: Function }} [deps.replayEngine]
   * @param {{ compare: Function }} [deps.comparator]
   * @param {ScenarioRunner} [deps.scenarioRunner]
   * @param {EvidenceQuery} [deps.query]
   * @param {ComparisonWorkspace} [deps.workspace]
   * @param {(input: object) => object[]|Promise<object[]>} [deps.analogFinder]
   * @param {import('@pulseforge/eql').EqlEngine} [deps.eql]
   * @param {import('@pulseforge/eql').EvidenceCatalog} [deps.catalog]
   */
  constructor(deps = {}) {
    this._replayEngine =
      deps.replayEngine ||
      createReplayEngine({
        resolveBundle: deps.resolveBundle || labResolveBundle,
        observationLoader: deps.observationLoader || null,
      });
    this._comparator = deps.comparator || createReplayComparator();
    this.runner =
      deps.scenarioRunner ||
      createScenarioRunner({ replayEngine: this._replayEngine });
    this.queryHelpers = deps.query || createEvidenceQuery();
    this.workspace =
      deps.workspace ||
      createComparisonWorkspace({ comparator: this._comparator });
    this._analogFinder =
      typeof deps.analogFinder === 'function' ? deps.analogFinder : null;
    this._catalog = deps.catalog || createEvidenceCatalog();
    this.eql =
      deps.eql ||
      createEqlEngine({
        catalog: this._catalog,
      });
    this._learning =
      deps.learning ||
      (() => {
        try {
          const { createLearningEngine } = require('@pulseforge/learning');
          return createLearningEngine();
        } catch {
          return null;
        }
      })();

    /** @type {Map<string, Experiment>} */
    this._experiments = new Map();
    /** @type {Map<string, object>} */
    this._results = new Map();

    // Callable EQL entrypoint that also exposes EvidenceQuery helpers (SPEC-019/020).
    const lab = this;
    const queryFn = async function query(source, options) {
      return lab.queryEql(source, options);
    };
    queryFn.observationsSupportingClaim = (...args) =>
      lab.queryHelpers.observationsSupportingClaim(...args);
    queryFn.observationsContradictingClaim = (...args) =>
      lab.queryHelpers.observationsContradictingClaim(...args);
    queryFn.claimEvidence = (...args) => lab.queryHelpers.claimEvidence(...args);
    queryFn.beliefAt = (...args) => lab.queryHelpers.beliefAt(...args);
    queryFn.recommendations = (...args) => lab.queryHelpers.recommendations(...args);
    queryFn.observationDiff = (...args) => lab.queryHelpers.observationDiff(...args);
    this.query = queryFn;
  }

  /**
   * Create an isolated experiment from observations / window.
   * @param {object} seed
   * @returns {Experiment}
   */
  createExperiment(seed) {
    const experiment = createExperiment(seed);
    this._experiments.set(experiment.id, experiment);
    return experiment;
  }

  /**
   * @param {string} id
   * @returns {Experiment|null}
   */
  getExperiment(id) {
    return this._experiments.get(String(id)) || null;
  }

  /**
   * Run an experiment (isolated replay).
   * @param {Experiment|object} experimentOrSeed
   * @returns {Promise<object>}
   */
  async run(experimentOrSeed) {
    const experiment = coerceExperiment(this, experimentOrSeed);
    const result = await this.runner.run(experiment);
    this._results.set(experiment.id, result);
    this._ingestResult(result);
    return result;
  }

  /**
   * Execute an Evidence Query Language (EQL) statement (SPEC-020).
   *
   * Domain-neutral: the same query string works for CRM and Market subjects.
   *
   * @example
   *   await lab.query(`FIND Claims WHERE subject = "BTC" AND confidence > 0.75`);
   *   await lab.query(`SHOW Evidence SUPPORTING Claim("momentum_continuation")`);
   *   await lab.query(`REPLAY FROM "2026-07-26T09:30:00Z" TO "2026-07-26T10:00:00Z"`);
   *
   * @param {string} source - EQL source
   * @param {object} [options]
   * @param {object} [options.result] - replay/lab result to project into the catalog
   * @param {Experiment|object} [options.experiment] - run first, then query
   * @param {import('@pulseforge/eql').EvidenceCatalog} [options.catalog]
   * @returns {Promise<object>}
   */
  async queryEql(source, options = {}) {
    if (source == null || String(source).trim() === '') {
      throw new Error('lab.query requires an EQL string');
    }
    // Validate early for clearer errors
    parseEql(String(source));

    let catalog = options.catalog || null;

    if (!catalog && options.result) {
      catalog = catalogFromResult(options.result, {
        compareFn: (args) => this._compareViaWorkspace(args),
      });
    }

    if (!catalog && options.experiment) {
      const result = await this.run(coerceExperiment(this, options.experiment));
      catalog = catalogFromResult(result, {
        compareFn: (args) => this._compareViaWorkspace(args),
      });
    }

    const engine = catalog ? createEqlEngine({ catalog }) : this.eql;
    return engine.query(String(source));
  }

  /**
   * Project a lab/replay result into the shared EQL catalog.
   * @param {object} result
   * @private
   */
  _ingestResult(result) {
    if (!result) return;
    const projected = catalogFromResult(result);
    for (const target of [
      'subjects',
      'observations',
      'evidence',
      'claims',
      'outcomes',
      'recommendations',
      'replay_sessions',
      'calibrations',
      'accuracies',
      'strategy_packs',
    ]) {
      const rows = projected.store[target] || [];
      for (const row of rows) {
        const existing = this._catalog.store[target] || [];
        const id = row.id || row.claimType || row.subjectId;
        if (id && existing.some((e) => (e.id || e.claimType || e.subjectId) === id)) {
          continue;
        }
        this._catalog.add(target, row);
      }
    }
  }

  /**
   * @param {{ subject: string|null, from: string, to: string }} args
   * @param {object} [hint]
   * @private
   */
  async _replayViaEngine(args, hint) {
    const subjectId =
      args.subject ||
      (hint && (hint.subjectId || hint.subject)) ||
      null;
    if (!subjectId) {
      return this._catalog.list('replay_sessions');
    }
    const observations =
      (hint && hint.observations) ||
      (this._experiments.get(hint && hint.experimentId)
        ? this._experiments.get(hint.experimentId).getObservations()
        : []);
    const experiment = this.createExperiment({
      subjectId,
      startTime: args.from,
      endTime: args.to,
      observations,
      ontology: (hint && hint.versions && hint.versions.ontology) || 'market',
      strategyPack: (hint && hint.versions && hint.versions.strategyPack) || 'market',
      name: `eql-replay:${subjectId}`,
    });
    return this.run(experiment);
  }

  /**
   * @param {{ left: unknown, right: unknown }} args
   * @private
   */
  async _compareViaWorkspace(args) {
    const leftId =
      typeof args.left === 'string'
        ? args.left
        : args.left && args.left.id;
    const rightId =
      typeof args.right === 'string'
        ? args.right
        : args.right && args.right.id;
    const leftResult = this._results.get(String(leftId)) || args.left;
    const rightResult = this._results.get(String(rightId)) || args.right;
    if (
      leftResult &&
      rightResult &&
      Array.isArray(leftResult.steps) &&
      Array.isArray(rightResult.steps)
    ) {
      return this.workspace.add(
        `eql-compare:${leftId}:${rightId}`,
        leftResult,
        rightResult,
        { kind: 'eqlCompare' }
      );
    }
    return Object.freeze({
      left: leftResult,
      right: rightResult,
      leftId,
      rightId,
      equal: false,
    });
  }

  /**
   * Compare two replays side-by-side.
   *
   * Accepts Experiments, run inputs, or already-run laboratory results.
   *
   * @param {object} args
   * @param {Experiment|object} args.left
   * @param {Experiment|object} args.right
   * @param {string} [args.name]
   * @returns {Promise<object>}
   */
  async compareReplay(args = {}) {
    const { left, right, name } = args;
    if (!left || !right) {
      throw new Error('compareReplay requires left and right');
    }

    const leftResult = await resolveResult(this, left);
    const rightResult = await resolveResult(this, right);
    const label =
      name ||
      `compare:${leftResult.experimentId || leftResult.subjectId}:${rightResult.experimentId || rightResult.subjectId}`;

    return this.workspace.add(label, leftResult, rightResult, {
      kind: 'compareReplay',
    });
  }

  /**
   * What if Observation Y never happened?
   * Returns a child experiment with Y removed (does not mutate parent).
   *
   * @param {object} args
   * @param {Experiment|object} args.experiment
   * @param {string|string[]} args.observationId
   * @param {string} [args.name]
   * @param {boolean} [args.run=false] - when true, also execute the scenario
   * @returns {Promise<Experiment|object>|Experiment}
   */
  removeObservation(args = {}) {
    const { experiment: seed, observationId, name, run = false } = args;
    if (observationId == null) {
      throw new Error('removeObservation requires observationId');
    }
    const parent = coerceExperiment(this, seed);
    const child = parent.withRemoved(observationId, {
      name,
      hypothesis:
        args.hypothesis ||
        `what if observation ${Array.isArray(observationId) ? observationId.join(',') : observationId} never happened?`,
    });
    this._experiments.set(child.id, child);

    if (run) {
      return this.runner.run(child).then((result) =>
        Object.freeze({
          experiment: child,
          result,
          parentId: parent.id,
          mutation: 'removeObservation',
        })
      );
    }
    return child;
  }

  /**
   * Inject a counterfactual observation into an isolated copy.
   *
   * @param {object} args
   * @param {Experiment|object} args.experiment
   * @param {object|object[]} args.observation
   * @param {string} [args.name]
   * @param {boolean} [args.run=false]
   * @returns {Promise<object>|Experiment}
   */
  injectObservation(args = {}) {
    const { experiment: seed, observation, name, run = false } = args;
    if (!observation) {
      throw new Error('injectObservation requires observation');
    }
    const parent = coerceExperiment(this, seed);
    const child = parent.withInjected(observation, {
      name,
      hypothesis:
        args.hypothesis || 'what if this observation had occurred?',
    });
    this._experiments.set(child.id, child);

    if (run) {
      return this.runner.run(child).then((result) =>
        Object.freeze({
          experiment: child,
          result,
          parentId: parent.id,
          mutation: 'injectObservation',
        })
      );
    }
    return child;
  }

  /**
   * Find similar historical situations (analogs).
   *
   * Prefers an injected analogFinder; otherwise runs a market evaluate and
   * returns analogs from the regenerated explanation / pack output.
   *
   * @param {object} args
   * @param {string} [args.subjectId]
   * @param {object[]} [args.observations]
   * @param {Experiment|object} [args.experiment]
   * @param {object} [args.memory]
   * @returns {Promise<object>}
   */
  async findAnalogs(args = {}) {
    const experiment = args.experiment
      ? coerceExperiment(this, args.experiment)
      : this.createExperiment({
          subjectId: args.subjectId,
          observations: args.observations || [],
          ontology: args.ontology || 'market',
          strategyPack: args.strategyPack || 'market',
          startTime: args.startTime,
          endTime: args.endTime,
          name: args.name || 'findAnalogs',
        });

    if (this._analogFinder) {
      const analogs = await this._analogFinder({
        subjectId: experiment.subjectId,
        observations: experiment.getObservations(),
        memory: args.memory,
        experiment: experiment.snapshot(),
      });
      return Object.freeze({
        subjectId: experiment.subjectId,
        experimentId: experiment.id,
        analogs: Array.isArray(analogs) ? analogs : [],
        source: 'analogFinder',
        isolated: true,
        mutatesProduction: false,
      });
    }

    const result = await this.runner.run(experiment);
    const analogs =
      (result.explanation && result.explanation.historicalAnalogs) ||
      (result.ranked && result.ranked.analogs) ||
      [];

    // Prefer pack-level analogs by re-running through market strategy when empty.
    if ((!analogs || analogs.length === 0) && !args.skipDefaultAnalogs) {
      const fromPack = await defaultMarketAnalogs(experiment, args.memory);
      return Object.freeze({
        subjectId: experiment.subjectId,
        experimentId: experiment.id,
        analogs: fromPack,
        source: 'market-strategy',
        resultFingerprint: result.replayFingerprint,
        isolated: true,
        mutatesProduction: false,
      });
    }

    return Object.freeze({
      subjectId: experiment.subjectId,
      experimentId: experiment.id,
      analogs: Array.isArray(analogs) ? analogs : [],
      source: 'replay',
      resultFingerprint: result.replayFingerprint,
      isolated: true,
      mutatesProduction: false,
    });
  }

  /**
   * Compare two strategy packs over the same history.
   *
   * @param {object} args
   * @param {object[]} [args.observations]
   * @param {Experiment|object} [args.experiment]
   * @param {string|object} args.left
   * @param {string|object} args.right
   * @param {string} [args.subjectId]
   * @param {string} [args.name]
   * @returns {Promise<object>}
   */
  async compareStrategies(args = {}) {
    if (args.left == null || args.right == null) {
      throw new Error('compareStrategies requires left and right strategy packs');
    }

    const base = args.experiment
      ? coerceExperiment(this, args.experiment)
      : this.createExperiment({
          subjectId: args.subjectId,
          observations: args.observations || [],
          ontology: args.ontology || 'market',
          strategyPack: args.left,
          startTime: args.startTime,
          endTime: args.endTime,
          name: args.name || 'compareStrategies:base',
        });

    const leftExp = base.withConfig({
      strategyPack: args.left,
      name: `${base.name}:strategy:left`,
      metadata: { role: 'left-strategy' },
    });
    const rightExp = base.withConfig({
      strategyPack: args.right,
      name: `${base.name}:strategy:right`,
      metadata: { role: 'right-strategy' },
    });
    this._experiments.set(leftExp.id, leftExp);
    this._experiments.set(rightExp.id, rightExp);

    const comparison = await this.compareReplay({
      left: leftExp,
      right: rightExp,
      name: args.name || `strategies:${versionLabel(args.left)}:${versionLabel(args.right)}`,
    });

    return Object.freeze({
      ...comparison,
      kind: 'compareStrategies',
      leftStrategy: versionLabel(args.left),
      rightStrategy: versionLabel(args.right),
      observationDiff: this.queryHelpers.observationDiff(leftExp, rightExp),
    });
  }

  /**
   * Compare ontology versions over the same history.
   *
   * @param {object} args
   * @param {object[]} [args.observations]
   * @param {Experiment|object} [args.experiment]
   * @param {string|object} args.left
   * @param {string|object} args.right
   * @param {string} [args.subjectId]
   * @param {string} [args.name]
   * @returns {Promise<object>}
   */
  async compareOntologies(args = {}) {
    if (args.left == null || args.right == null) {
      throw new Error('compareOntologies requires left and right ontologies');
    }

    const base = args.experiment
      ? coerceExperiment(this, args.experiment)
      : this.createExperiment({
          subjectId: args.subjectId,
          observations: args.observations || [],
          ontology: args.left,
          strategyPack: args.strategyPack || 'market',
          startTime: args.startTime,
          endTime: args.endTime,
          name: args.name || 'compareOntologies:base',
        });

    const leftExp = base.withConfig({
      ontology: args.left,
      name: `${base.name}:ontology:left`,
      metadata: { role: 'left-ontology' },
    });
    const rightExp = base.withConfig({
      ontology: args.right,
      name: `${base.name}:ontology:right`,
      metadata: { role: 'right-ontology' },
    });
    this._experiments.set(leftExp.id, leftExp);
    this._experiments.set(rightExp.id, rightExp);

    const comparison = await this.compareReplay({
      left: leftExp,
      right: rightExp,
      name: args.name || `ontologies:${versionLabel(args.left)}:${versionLabel(args.right)}`,
    });

    return Object.freeze({
      ...comparison,
      kind: 'compareOntologies',
      leftOntology: versionLabel(args.left),
      rightOntology: versionLabel(args.right),
      observationDiff: this.queryHelpers.observationDiff(leftExp, rightExp),
    });
  }

  /**
   * Convenience: claim evidence query over a run result or experiment.
   *
   * @param {object} args
   * @param {object} [args.result]
   * @param {Experiment|object} [args.experiment]
   * @param {string} args.claimId
   * @returns {Promise<object>}
   */
  async observationsForClaim(args = {}) {
    if (!args.claimId) throw new Error('observationsForClaim requires claimId');
    const result = args.result
      ? args.result
      : await this.run(coerceExperiment(this, args.experiment));
    return this.queryHelpers.claimEvidence(result, args.claimId);
  }

  /**
   * Compare calibration overlays for two claim/outcome histories (SPEC-021).
   * Isolated — does not mutate production, replay history, or runtime confidence.
   *
   * @param {object} args
   * @param {object} args.left - { claims, outcomes, strategyPack?, name? }
   * @param {object} args.right - { claims, outcomes, strategyPack?, name? }
   * @param {string} [args.name]
   * @returns {object}
   */
  compareCalibration(args = {}) {
    if (!args.left || !args.right) {
      throw new Error('compareCalibration requires left and right');
    }
    const learning = requireLearning(this);
    const leftResult = learning.learn({
      ...args.left,
      name: (args.left && args.left.name) || 'calibration:left',
      persistBeliefs: false,
    });
    const rightResult = learning.learn({
      ...args.right,
      name: (args.right && args.right.name) || 'calibration:right',
      persistBeliefs: false,
    });

    return Object.freeze({
      kind: 'compareCalibration',
      name: args.name || 'compareCalibration',
      left: leftResult,
      right: rightResult,
      leftAccuracy: leftResult.produced.historicalAccuracy,
      rightAccuracy: rightResult.produced.historicalAccuracy,
      delta: Object.freeze({
        accuracy:
          nullableDelta(
            leftResult.produced.historicalAccuracy.accuracy,
            rightResult.produced.historicalAccuracy.accuracy
          ),
        precision:
          nullableDelta(
            leftResult.produced.historicalAccuracy.precision,
            rightResult.produced.historicalAccuracy.precision
          ),
        recall:
          nullableDelta(
            leftResult.produced.historicalAccuracy.recall,
            rightResult.produced.historicalAccuracy.recall
          ),
      }),
      isolated: true,
      mutatesProduction: false,
      mutatesHistory: false,
      mutatesReplay: false,
      mutatesRuntime: false,
    });
  }

  /**
   * Replay an experiment and overlay belief calibration (SPEC-021).
   * Returns replay result + calibration adjustments. Does not mutate replay
   * steps, history, or runtime confidence.
   *
   * @param {object} args
   * @param {Experiment|object} [args.experiment]
   * @param {object} [args.result] - already-run laboratory/replay result
   * @param {object[]} [args.outcomes]
   * @param {string} [args.strategyPack]
   * @param {number} [args.blendWeight]
   * @returns {Promise<object>}
   */
  async replayWithCalibration(args = {}) {
    const learning = requireLearning(this);
    const result = args.result
      ? args.result
      : await this.run(coerceExperiment(this, args.experiment));

    const claims = flattenResultClaims(result);
    const outcomes =
      args.outcomes ||
      result.outcomes ||
      (result.explanation && result.explanation.outcomes) ||
      [];

    const learningResult = learning.learn({
      claims,
      outcomes,
      evidence: result.evidence || [],
      observations: result.observations || [],
      strategyPack:
        args.strategyPack ||
        (result.versions && result.versions.strategyPack) ||
        null,
      blendWeight: args.blendWeight,
      persistBeliefs: false,
      name: `replayWithCalibration:${result.experimentId || result.subjectId || 'session'}`,
    });

    // Project calibrations into the shared catalog for EQL SHOW Calibration FOR …
    for (const row of learningResult.calibrations) {
      this._catalog.add('calibrations', row);
    }
    if (learningResult.accuracy) {
      this._catalog.add('accuracies', learningResult.accuracy);
    }
    for (const pack of learning.toCatalogSeed().strategy_packs) {
      const existing = this._catalog.store.strategy_packs || [];
      if (!existing.some((e) => e.id === pack.id)) {
        this._catalog.add('strategy_packs', pack);
      }
    }

    return Object.freeze({
      kind: 'replayWithCalibration',
      replay: result,
      learning: learningResult,
      calibrations: learningResult.calibrations,
      accuracy: learningResult.produced.historicalAccuracy,
      confidenceAdjustments: learningResult.produced.confidenceAdjustments,
      isolated: true,
      mutatesProduction: false,
      mutatesHistory: false,
      mutatesReplay: false,
      mutatesRuntime: false,
    });
  }

  /**
   * Dispose registered experiments and comparison workspace.
   * Does not touch production.
   */
  reset() {
    this._experiments.clear();
    this._results.clear();
    this.workspace.clear();
  }
}

/**
 * @param {EvidenceLab} lab
 * @param {Experiment|object} value
 * @returns {Experiment}
 */
function coerceExperiment(lab, value) {
  if (!value) {
    throw new Error('Experiment seed is required');
  }
  if (value instanceof Experiment) {
    lab._experiments.set(value.id, value);
    return value;
  }
  if (typeof value.getObservations === 'function' && value.subjectId) {
    lab._experiments.set(value.id, value);
    return value;
  }
  return lab.createExperiment(value);
}

/**
 * @param {EvidenceLab} lab
 * @param {Experiment|object} value
 */
async function resolveResult(lab, value) {
  if (value && Array.isArray(value.steps) && value.subjectId) {
    return value;
  }
  const experiment = coerceExperiment(lab, value);
  return lab.runner.run(experiment);
}

/**
 * @param {string|object} value
 */
function versionLabel(value) {
  if (value == null) return 'unknown';
  if (typeof value === 'string') return value;
  const id = value.id || value.name || 'object';
  if (value.version != null || value.revision != null) {
    return `${id}@${value.version || value.revision}`;
  }
  return String(id);
}

/**
 * Lazy default analog path via market-strategy (no production writes).
 * @param {Experiment} experiment
 * @param {object} [memory]
 */
async function defaultMarketAnalogs(experiment, memory) {
  try {
    const {
      createMarketStrategyPack,
      createMarketContextProvider,
      findMarketAnalogs,
    } = require('@pulseforge/market-strategy');
    const { createReasoningRuntime } = require('@pulseforge/reasoning-runtime');

    const pack = createMarketStrategyPack();
    const contextProvider = createMarketContextProvider();
    const runtime = createReasoningRuntime({
      strategyPack: pack,
      contextProvider,
    });

    const asOf =
      experiment.endTime ||
      (experiment.getObservations().slice(-1)[0] || {}).observedAt ||
      '1970-01-01T00:00:00.000Z';

    const evaluated = await runtime.evaluate({
      subjectId: experiment.subjectId,
      observations: experiment.getObservations().map((o) => ({
        ...(o.payload || {}),
        id: o.id,
        type: o.observationType,
        timestamp: o.observedAt,
        observedAt: o.observedAt,
        asset: (o.payload && o.payload.asset) || o.subjectId,
        subjectId: o.subjectId,
      })),
      asOf,
      builtAt: asOf,
    });

    if (Array.isArray(evaluated.analogs) && evaluated.analogs.length > 0) {
      return evaluated.analogs;
    }

    return findMarketAnalogs({
      context: evaluated.context,
      strategyResults: evaluated.claims?.results || [],
      memory,
    });
  } catch {
    return [];
  }
}

/**
 * @param {EvidenceLab} lab
 */
function requireLearning(lab) {
  if (lab._learning) return lab._learning;
  try {
    const { createLearningEngine } = require('@pulseforge/learning');
    lab._learning = createLearningEngine();
    return lab._learning;
  } catch (err) {
    throw new Error(
      `EvidenceLab calibration requires @pulseforge/learning (${err.message})`
    );
  }
}

/**
 * @param {object} result
 * @returns {object[]}
 */
function flattenResultClaims(result) {
  if (!result) return [];
  const claims = result.claims;
  if (Array.isArray(claims)) return claims.filter(Boolean);
  if (claims && typeof claims === 'object') {
    return []
      .concat(claims.derived || [])
      .concat(claims.results || [])
      .concat(claims.graph || [])
      .concat(claims.items || [])
      .concat(claims.active || [])
      .filter(Boolean);
  }
  return [];
}

/**
 * @param {number|null} left
 * @param {number|null} right
 */
function nullableDelta(left, right) {
  if (left == null || right == null) return null;
  return Math.round((right - left) * 10000) / 10000;
}

/**
 * @param {object} [deps]
 * @returns {EvidenceLab}
 */
function createEvidenceLab(deps) {
  return new EvidenceLab(deps);
}

module.exports = {
  EvidenceLab,
  createEvidenceLab,
};
