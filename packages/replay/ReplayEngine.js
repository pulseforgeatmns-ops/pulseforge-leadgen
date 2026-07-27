'use strict';

const { createHash } = require('crypto');
const { createReplayTimeline } = require('./ReplayTimeline');
const { createReplaySession } = require('./ReplaySession');
const { canonicalizeResult, hashJson } = require('./ReplayComparator');
const { DEFAULT_RUNTIME_VERSION } = require('./types');

/**
 * ReplayEngine — rebuilds reasoning from immutable observations (SPEC-018).
 *
 * No database snapshots. No cached reasoning. Reasoning is regenerated.
 */
class ReplayEngine {
  /**
   * @param {object} [deps]
   * @param {(input: import('./types').ReplayRunInput) => Promise<object>|object} [deps.resolveBundle]
   * @param {(args: object) => Promise<object[]>|object[]} [deps.observationLoader]
   */
  constructor(deps = {}) {
    this._resolveBundle =
      typeof deps.resolveBundle === 'function'
        ? deps.resolveBundle
        : defaultResolveBundle;
    this._observationLoader =
      typeof deps.observationLoader === 'function'
        ? deps.observationLoader
        : null;
  }

  /**
   * Full deterministic replay over a subject and time window.
   *
   * @param {import('./types').ReplayRunInput} input
   * @returns {Promise<import('./types').ReplayRunResult & { queries: object }>}
   */
  async run(input) {
    if (!input || !input.subjectId) {
      throw new Error('ReplayEngine.run requires subjectId');
    }

    const subjectId = String(input.subjectId);
    const startTime = input.startTime ? String(input.startTime) : null;
    const endTime = input.endTime ? String(input.endTime) : null;

    const bundle = await this._resolveBundle(input);
    const versions = Object.freeze({
      ontology: String(bundle.versions.ontology),
      strategyPack: String(bundle.versions.strategyPack),
      runtime: String(
        input.runtimeVersion || bundle.versions.runtime || DEFAULT_RUNTIME_VERSION
      ),
    });

    const rawObservations = await this._loadObservations(input, subjectId, startTime, endTime);
    const domain = resolveDomain(input, bundle);
    const observations = normalizeObservationList(rawObservations, {
      subjectId,
      domain,
      startTime,
      endTime,
    });

    const timeline = createReplayTimeline(observations);
    const session = createReplaySession({ subjectId, versions });

    const asOf =
      endTime ||
      (observations.length
        ? observations[observations.length - 1].observedAt
        : '1970-01-01T00:00:00.000Z');

    // Step through each observation; regenerate reasoning cumulatively.
    const cumulative = [];
    for (;;) {
      const observation = timeline.next();
      if (!observation) break;
      cumulative.push(observation);

      const evaluated = await evaluateAt({
        bundle,
        subjectId,
        observations: cumulative,
        asOf: observation.observedAt,
      });

      const step = buildStep({
        observation,
        evaluated,
        previous: session.steps[session.steps.length - 1] || null,
      });
      session.applyStep(step);
    }

    // If no observations, still produce an empty-but-valid shell via one evaluate.
    if (session.steps.length === 0) {
      const evaluated = await evaluateAt({
        bundle,
        subjectId,
        observations: [],
        asOf,
      });
      session.applyStep(
        buildStep({
          observation: null,
          evaluated,
          previous: null,
        })
      );
    }

    const last = session.steps[session.steps.length - 1];
    const result = Object.freeze({
      subjectId,
      startTime,
      endTime,
      observations: timeline.toArray(),
      evidence: last.generatedEvidence,
      claims: last.affectedClaims,
      confidence: last.confidence,
      recommendations: session.recommendations.slice(),
      explanation: last.explanation || buildExplanation(last),
      reasoningTrace: last.reasoningTrace,
      steps: session.steps.slice(),
      versions,
      ranked: last.ranked || null,
    });

    session.close();

    return Object.freeze({
      ...result,
      fingerprint: hashJson(canonicalizeResult(result)),
      queries: createTemporalQueries(result),
    });
  }
}

/**
 * @param {object} args
 */
async function evaluateAt({ bundle, subjectId, observations, asOf }) {
  const rawForProvider = observations.map(toProviderObservation);
  const clock = () => String(asOf);

  const contextProvider = wrapContextProvider(bundle.contextProvider, {
    builtAt: asOf,
    asOf,
  });

  const runtime = bundle.createRuntime({
    strategyPack: bundle.strategyPack,
    contextProvider,
    recommendationProvider: bundle.recommendationProvider,
    clock,
  });

  const evaluated = await runtime.evaluate({
    subjectId,
    observations: rawForProvider,
    asOf,
    builtAt: asOf,
  });

  return canonicalizeEvaluation(evaluated, asOf);
}

/**
 * Strip wall-clock / timing noise so identical inputs hash identically.
 * @param {object} evaluated
 * @param {string} asOf
 */
function canonicalizeEvaluation(evaluated, asOf) {
  const trace = evaluated.trace
    ? {
        ...evaluated.trace,
        steps: (evaluated.trace.steps || []).map((s) => ({
          step: s.step,
          count: s.count,
          at: asOf,
        })),
      }
    : { steps: [], at: asOf };

  const explanation = evaluated.explanation
    ? {
        ...evaluated.explanation,
        reasoningTrace: canonicalizePackTrace(
          evaluated.explanation.reasoningTrace,
          asOf
        ),
      }
    : null;

  const context = evaluated.context
    ? { ...evaluated.context, builtAt: asOf }
    : null;

  const recommendation = evaluated.recommendation
    ? stabilizeRecommendation(evaluated.recommendation, asOf)
    : null;

  return {
    ...evaluated,
    context,
    recommendation,
    explanation,
    trace,
    meta: {
      ...(evaluated.meta || {}),
      executionTimeMs: 0,
      withinTarget: true,
      asOf,
    },
  };
}

/**
 * @param {object|null|undefined} packTrace
 * @param {string} asOf
 */
function canonicalizePackTrace(packTrace, asOf) {
  if (!packTrace || typeof packTrace !== 'object') return packTrace;
  const copy = { ...packTrace, asOf };
  delete copy.strategyTimings;
  return copy;
}

/**
 * @param {object} recommendation
 * @param {string} asOf
 */
function stabilizeRecommendation(recommendation, asOf) {
  if (!recommendation || typeof recommendation !== 'object') return recommendation;
  // Prefer already-deterministic ids; rewrite Date.now()-style suffixes if present.
  const id = String(recommendation.id || '');
  const looksVolatile = /:\d{13}$/.test(id);
  if (!looksVolatile) return recommendation;
  return {
    ...recommendation,
    id: `research:${recommendation.subject?.id || 'unknown'}:${asOf}:${recommendation.recommendedAction}:${recommendation.score}:${recommendation.confidence}`,
  };
}

/**
 * @param {object} args
 * @returns {import('./types').ReplayStep}
 */
function buildStep({ observation, evaluated, previous }) {
  const confidence =
    evaluated.ranked && evaluated.ranked.confidence != null
      ? Number(evaluated.ranked.confidence)
      : evaluated.recommendation && evaluated.recommendation.confidence != null
        ? Number(evaluated.recommendation.confidence)
        : null;

  const prevConf = previous ? previous.confidence : null;
  const confidenceChanges = [
    ...((evaluated.explanation && evaluated.explanation.confidenceChanges) ||
      []),
  ];
  if (prevConf != null && confidence != null && prevConf !== confidence) {
    confidenceChanges.push({
      field: 'replay.confidence',
      from: prevConf,
      to: confidence,
      delta: round(confidence - prevConf),
      observationId: observation ? observation.id : null,
    });
  }

  const prevClaimIds = new Set(extractClaimIds(previous && previous.affectedClaims));
  const nextClaimIds = extractClaimIds(evaluated.claims);
  const affectedClaims = {
    ...(typeof evaluated.claims === 'object' && evaluated.claims
      ? evaluated.claims
      : { items: evaluated.claims }),
    appeared: nextClaimIds.filter((id) => !prevClaimIds.has(id)),
    disappeared: [...prevClaimIds].filter((id) => !nextClaimIds.includes(id)),
  };

  return Object.freeze({
    observation,
    generatedEvidence: evaluated.evidence,
    affectedClaims,
    confidenceChanges,
    recommendation: evaluated.recommendation,
    reasoningTrace: evaluated.trace || evaluated.explanation?.reasoningTrace || {},
    explanation: evaluated.explanation,
    confidence,
    ranked: evaluated.ranked || null,
  });
}

/**
 * @param {import('./types').ReplayStep} step
 */
function buildExplanation(step) {
  return {
    evidence: step.generatedEvidence,
    claims: step.affectedClaims,
    confidence: step.confidence,
    recommendation: step.recommendation,
    reasoningTrace: step.reasoningTrace,
    confidenceChanges: step.confidenceChanges,
  };
}

/**
 * Temporal query surface over a completed replay result.
 * @param {import('./types').ReplayRunResult} result
 */
function createTemporalQueries(result) {
  return {
    /** What did we believe at time T? */
    beliefAt(timestamp) {
      const step = stepAtOrBefore(result.steps, timestamp);
      if (!step) return null;
      return {
        at: timestamp,
        observation: step.observation,
        claims: step.affectedClaims,
        confidence: step.confidence,
        recommendation: step.recommendation,
        evidence: step.generatedEvidence,
      };
    },

    /** Why did confidence increase? */
    whyConfidenceIncreased() {
      const rises = [];
      for (const step of result.steps || []) {
        for (const change of step.confidenceChanges || []) {
          const delta =
            change.delta != null
              ? Number(change.delta)
              : change.from != null && change.to != null
                ? Number(change.to) - Number(change.from)
                : null;
          if (delta != null && delta > 0) {
            rises.push({
              observationId: step.observation ? step.observation.id : null,
              observedAt: step.observation ? step.observation.observedAt : null,
              change,
              recommendation: step.recommendation,
            });
          }
        }
      }
      return rises;
    },

    /** Which observation changed the recommendation? */
    whichObservationChangedRecommendation() {
      const changes = [];
      let prevAction = null;
      for (const step of result.steps || []) {
        const action =
          step.recommendation && step.recommendation.recommendedAction;
        if (prevAction != null && action !== prevAction) {
          changes.push({
            observationId: step.observation ? step.observation.id : null,
            observedAt: step.observation ? step.observation.observedAt : null,
            from: prevAction,
            to: action,
            recommendation: step.recommendation,
          });
        }
        if (action != null) prevAction = action;
      }
      return changes;
    },

    /** When did this claim first appear? */
    whenClaimFirstAppeared(claimId) {
      const target = String(claimId);
      for (const step of result.steps || []) {
        const appeared = (step.affectedClaims && step.affectedClaims.appeared) || [];
        const ids = extractClaimIds(step.affectedClaims);
        if (appeared.includes(target) || ids.includes(target)) {
          return {
            claimId: target,
            observationId: step.observation ? step.observation.id : null,
            observedAt: step.observation ? step.observation.observedAt : null,
            confidence: step.confidence,
          };
        }
      }
      return null;
    },

    /** When did this claim become dominant (highest confidence among derived)? */
    whenClaimBecameDominant(claimId) {
      const target = String(claimId);
      for (const step of result.steps || []) {
        const dominant = dominantClaimId(step.affectedClaims);
        if (dominant === target) {
          return {
            claimId: target,
            observationId: step.observation ? step.observation.id : null,
            observedAt: step.observation ? step.observation.observedAt : null,
            confidence: step.confidence,
          };
        }
      }
      return null;
    },

    /** What evidence contradicted this claim? */
    whatEvidenceContradictedClaim(claimId) {
      const target = String(claimId);
      const last = result.steps[result.steps.length - 1];
      const explanation = (last && last.explanation) || result.explanation;
      const contradicting =
        (explanation && explanation.contradictingEvidence) || [];
      const derived = flattenClaims(last && last.affectedClaims);
      const claim = derived.find(
        (c) => c.id === target || c.claimType === target || c.strategy === target
      );
      return {
        claimId: target,
        claim: claim || null,
        contradictingEvidence: contradicting,
      };
    },

    /** Show every recommendation generated for this subject. */
    showEveryRecommendation() {
      return (result.recommendations || []).map((rec, index) => ({
        index,
        id: rec.id,
        recommendedAction: rec.recommendedAction,
        score: rec.score,
        confidence: rec.confidence,
        observationId:
          result.steps[index] && result.steps[index].observation
            ? result.steps[index].observation.id
            : null,
        observedAt:
          result.steps[index] && result.steps[index].observation
            ? result.steps[index].observation.observedAt
            : null,
      }));
    },
  };
}

/**
 * @param {import('./types').ReplayStep[]} steps
 * @param {string} timestamp
 */
function stepAtOrBefore(steps, timestamp) {
  const ts = Date.parse(String(timestamp));
  if (Number.isNaN(ts)) {
    throw new Error(`beliefAt requires a valid timestamp: ${timestamp}`);
  }
  let found = null;
  for (const step of steps || []) {
    if (!step.observation) continue;
    const obsTs = Date.parse(step.observation.observedAt);
    if (!Number.isNaN(obsTs) && obsTs <= ts) {
      found = step;
    }
  }
  return found;
}

/**
 * @param {unknown} claims
 * @returns {string[]}
 */
function extractClaimIds(claims) {
  return flattenClaims(claims)
    .map((c) => c.id || c.claimType || c.strategy)
    .filter(Boolean)
    .map(String);
}

/**
 * @param {unknown} claims
 * @returns {object[]}
 */
function flattenClaims(claims) {
  if (!claims) return [];
  if (Array.isArray(claims)) return claims.filter(Boolean);
  if (typeof claims === 'object') {
    const lists = [
      claims.derived,
      claims.results,
      claims.observations,
      claims.graph,
      claims.items,
    ];
    return lists.flatMap((list) => (Array.isArray(list) ? list : []));
  }
  return [];
}

/**
 * @param {unknown} claims
 * @returns {string|null}
 */
function dominantClaimId(claims) {
  const list = flattenClaims(claims).filter(
    (c) => c && (c.id || c.claimType || c.strategy)
  );
  if (list.length === 0) return null;
  let best = list[0];
  for (const c of list) {
    if (Number(c.confidence || 0) > Number(best.confidence || 0)) {
      best = c;
    }
  }
  return String(best.id || best.claimType || best.strategy);
}

/**
 * @param {import('./types').ReplayRunInput} input
 * @param {string} subjectId
 * @param {string|null} startTime
 * @param {string|null} endTime
 */
async function _loadObservationsImpl(engine, input, subjectId, startTime, endTime) {
  if (Array.isArray(input.observations) && input.observations.length > 0) {
    return input.observations;
  }
  const loader = input.observationLoader || engine._observationLoader;
  if (typeof loader === 'function') {
    return await loader({ subjectId, startTime, endTime });
  }
  // Market default fixtures — empty means context provider may supply defaults;
  // for replay we prefer explicit empty over silent fixture injection when a
  // window was requested without observations. Callers should pass observations.
  return [];
}

// Bind loader onto the class prototype via method for clarity
ReplayEngine.prototype._loadObservations = async function _loadObservations(
  input,
  subjectId,
  startTime,
  endTime
) {
  return _loadObservationsImpl(this, input, subjectId, startTime, endTime);
};

/**
 * @param {object[]} raw
 * @param {object} opts
 * @returns {import('./types').ImmutableObservation[]}
 */
function normalizeObservationList(raw, opts) {
  const { subjectId, domain, startTime, endTime } = opts;
  const startMs = startTime ? Date.parse(startTime) : null;
  const endMs = endTime ? Date.parse(endTime) : null;

  const normalized = [];
  for (const rawObs of raw || []) {
    const obs = toImmutableObservation(rawObs, { subjectId, domain });
    const t = Date.parse(obs.observedAt);
    if (startMs != null && !Number.isNaN(startMs) && t < startMs) continue;
    if (endMs != null && !Number.isNaN(endMs) && t > endMs) continue;
    normalized.push(obs);
  }

  normalized.sort((a, b) => {
    const ta = Date.parse(a.observedAt);
    const tb = Date.parse(b.observedAt);
    if (ta !== tb) return ta - tb;
    return String(a.id).localeCompare(String(b.id));
  });

  // Deduplicate by deterministic id
  const seen = new Set();
  return normalized.filter((obs) => {
    if (seen.has(obs.id)) return false;
    seen.add(obs.id);
    return true;
  });
}

/**
 * @param {object} raw
 * @param {{ subjectId: string, domain: string }} opts
 * @returns {import('./types').ImmutableObservation}
 */
function toImmutableObservation(raw, opts) {
  if (!raw || typeof raw !== 'object') {
    throw new Error('Observation must be an object');
  }

  const observationType = String(raw.observationType || raw.type || '');
  if (!observationType) {
    throw new Error('Observation requires observationType or type');
  }

  const observedAt = String(raw.observedAt || raw.timestamp || '');
  if (!observedAt) {
    throw new Error('Observation requires observedAt or timestamp');
  }

  const subjectId = String(
    raw.subjectId ||
      raw.asset ||
      (Array.isArray(raw.symbols) && raw.symbols[0]) ||
      opts.subjectId
  );

  const venue =
    raw.venue ||
    (raw.payload && raw.payload.venue) ||
    undefined;

  const id =
    raw.id ||
    deterministicObservationId({
      domain: opts.domain,
      observationType,
      subjectKey: subjectId,
      observedAt,
      venue,
      payload: raw.payload || raw,
    });

  return Object.freeze({
    id: String(id),
    subjectId,
    observationType,
    observedAt,
    venue: venue ? String(venue) : undefined,
    type: observationType,
    timestamp: observedAt,
    payload: Object.freeze({
      ...(raw.payload && typeof raw.payload === 'object' ? raw.payload : {}),
      ...pickRawFields(raw),
    }),
  });
}

/**
 * @param {object} raw
 */
function pickRawFields(raw) {
  const skip = new Set([
    'id',
    'payload',
    'subjectId',
    'observationType',
    'observedAt',
    'type',
    'timestamp',
    'venue',
  ]);
  const out = {};
  for (const [k, v] of Object.entries(raw)) {
    if (!skip.has(k)) out[k] = v;
  }
  return out;
}

/**
 * Provider-facing observation shape (market context expects type/timestamp).
 * @param {import('./types').ImmutableObservation} obs
 */
function toProviderObservation(obs) {
  const payload = obs.payload || {};
  return {
    ...payload,
    id: obs.id,
    type: obs.observationType,
    observationType: obs.observationType,
    timestamp: obs.observedAt,
    observedAt: obs.observedAt,
    asset: payload.asset || obs.subjectId,
    subjectId: obs.subjectId,
    venue: obs.venue || payload.venue,
  };
}

/**
 * Deterministic observation identity — never positional.
 * @param {object} parts
 */
function deterministicObservationId(parts) {
  const segments = [
    parts.domain,
    parts.observationType,
    parts.subjectKey,
    parts.observedAt,
  ];
  if (parts.venue) segments.push(parts.venue);
  // Include a stable payload fingerprint so distinct same-second events differ.
  if (parts.payload && typeof parts.payload === 'object') {
    segments.push(hashJson(sortKeys(parts.payload)));
  }
  const normalized = segments.map((p) => String(p).trim().toLowerCase()).join('|');
  return createHash('sha256').update(normalized).digest('hex').slice(0, 32);
}

function sortKeys(value) {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = sortKeys(value[key]);
    }
    return out;
  }
  return value;
}

/**
 * @param {object} provider
 * @param {{ builtAt: string, asOf: string }} fixed
 */
function wrapContextProvider(provider, fixed) {
  return {
    id: provider.id,
    build(input) {
      return provider.build({
        ...input,
        builtAt: fixed.builtAt,
        asOf: fixed.asOf,
      });
    },
  };
}

/**
 * @param {import('./types').ReplayRunInput} input
 * @param {object} bundle
 */
function resolveDomain(input, bundle) {
  if (typeof input.ontology === 'string') return input.ontology;
  if (bundle.domain) return bundle.domain;
  if (typeof input.strategyPack === 'string') return input.strategyPack;
  return 'market';
}

/**
 * Default resolver — wires market ontology + market strategy pack + runtime.
 * @param {import('./types').ReplayRunInput} input
 */
function defaultResolveBundle(input) {
  const ontologyKey =
    typeof input.ontology === 'string' ? input.ontology : input.ontology?.id;
  const packKey =
    typeof input.strategyPack === 'string'
      ? input.strategyPack
      : input.strategyPack?.id;

  if (ontologyKey && ontologyKey !== 'market') {
    throw new Error(
      `ReplayEngine: unsupported ontology "${ontologyKey}" (built-in resolver supports "market")`
    );
  }
  if (packKey && packKey !== 'market') {
    throw new Error(
      `ReplayEngine: unsupported strategyPack "${packKey}" (built-in resolver supports "market")`
    );
  }

  // Lazy requires keep CRM-only installs from loading market packages until needed.
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

  const recommendationProvider =
    input.strategyPack &&
    typeof input.strategyPack === 'object' &&
    input.strategyPack.recommendationProvider
      ? input.strategyPack.recommendationProvider
      : createResearchRecommendationProvider();

  const strategyPack =
    input.strategyPack &&
    typeof input.strategyPack === 'object' &&
    typeof input.strategyPack.initialize === 'function'
      ? input.strategyPack
      : createMarketStrategyPack({ recommendationProvider });

  const contextProvider =
    input.contextProvider || createMarketContextProvider();

  return {
    domain: 'market',
    strategyPack,
    contextProvider,
    recommendationProvider,
    versions: {
      ontology: `market@${ontologyVersion}`,
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

function round(n) {
  return Math.round(Number(n) * 1000) / 1000;
}

/**
 * @param {object} [deps]
 * @returns {ReplayEngine}
 */
function createReplayEngine(deps) {
  return new ReplayEngine(deps);
}

module.exports = {
  ReplayEngine,
  createReplayEngine,
  createTemporalQueries,
  normalizeObservationList,
  toImmutableObservation,
  deterministicObservationId,
  canonicalizeEvaluation,
};
