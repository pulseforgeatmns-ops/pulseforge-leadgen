'use strict';

const { randomUUID } = require('node:crypto');
const { performance } = require('node:perf_hooks');
const { parseDecision } = require('./schema');
const { snapshotInput, missionSnapshot, redactText } = require('./privacy');
const { observedRoute } = require('./observedRoute');
const { JevProvider } = require('./providers/JevProvider');
const { NoopProvider } = require('./providers/NoopProvider');
const { createShadowEventSink, getDefaultShadowEventSink } = require('./ShadowEventSink');

function boundedInteger(value, fallback, min, max) {
  const n = Number(value);
  return Number.isInteger(n) && n >= min && n <= max ? n : fallback;
}
function readConfig(env = process.env) {
  return {
    enabled: env.DECISION_SHADOW_ENABLED === 'true',
    provider: env.DECISION_PROVIDER || 'noop',
    jevEnabled: env.JEV_ENABLED === 'true',
    apiKey: env.JEV_API_KEY || env.TYPESAFE_API_KEY || '',
    model: env.JEV_MODEL || 'jev-latest',
    timeoutMs: boundedInteger(env.JEV_TIMEOUT_MS, 1500, 10, 10000),
    maxPending: boundedInteger(env.DECISION_SHADOW_MAX_PENDING, 64, 1, 1000),
    logRaw: env.DECISION_SHADOW_LOG_RAW === 'true',
  };
}
function selectProvider(config, fetchImpl) {
  if (!config.enabled) return new NoopProvider('disabled');
  if (config.provider === 'noop') return new NoopProvider('noop_selected');
  if (config.provider !== 'jev') return new NoopProvider('unsupported_provider');
  if (!config.jevEnabled) return new NoopProvider('jev_disabled');
  if (!config.apiKey.trim()) return new NoopProvider('missing_api_key');
  if (!/^jev-[a-zA-Z0-9._-]{1,80}$/.test(config.model)) return new NoopProvider('invalid_model');
  return new JevProvider({ apiKey: config.apiKey, model: config.model, fetchImpl });
}
function defaultAudit(row) {
  console.info('[DECISION_SHADOW_EVALUATED]', JSON.stringify(row));
}
function safeError(error) {
  const codes = ['invalid_response', 'http_error', 'timeout'];
  return {
    code: codes.includes(error?.code) ? error.code : 'provider_error',
    http_status: Number.isInteger(error?.http_status) ? error.http_status : null,
  };
}

/** Observation only: no method can execute or return a production route. */
class DecisionService {
  constructor({ env = process.env, provider, audit = defaultAudit, persistence, fetchImpl } = {}) {
    this.config = readConfig(env);
    /** @type {import('./types').DecisionProvider} */
    this.provider = provider || selectProvider(this.config, fetchImpl);
    this._audit = audit;
    this._persistence = persistence || (env === process.env
      ? getDefaultShadowEventSink() : createShadowEventSink({ env }));
    this._pending = new Set();
  }

  begin(input, session = null, source = 'workspace') {
    if (!this.config.enabled || input?._miepInternal || !String(input?.question || '').trim()) return null;
    const state = snapshotInput(input, session);
    const started = performance.now();
    const base = {
      event: 'DECISION_SHADOW_EVALUATED', spec: 'SPEC-JEV-001', schema_version: 1,
      decision_id: randomUUID(), mode: 'shadow', source,
      session_id: redactText(input.sessionId || session?.id, 160),
      tenant_id: redactText(input.context?.tenantId || input.rawContext?.tenantId || session?.context?.tenantId, 80),
      message_index: session?.messages?.filter(message => message.role === 'operator').length ?? null,
      message_chars: String(input.question).length,
      message_truncated: state.message_truncated,
      provider: ['jev', 'noop'].includes(this.provider.name) ? this.provider.name : 'custom',
      requested_provider: ['jev', 'noop'].includes(this.config.provider) ? this.config.provider : 'unknown',
      requested_model: /^jev-[a-zA-Z0-9._-]{1,80}$/.test(this.provider.model || '') ? this.provider.model : null,
    };
    let completed = false;
    return {
      // Enrich with a mission already read by production, before it is mutated.
      captureMission(mission) {
        try {
          if (!completed && mission) state.context.mission = missionSnapshot(mission);
        } catch (_) { /* observation cannot affect routing */ }
      },
      complete: (result, error) => {
        if (completed) return;
        completed = true;
        try {
          const current = observedRoute(result, Boolean(error));
          base.session_id = base.session_id || redactText(result?.sessionId, 160);
          base.mission_id = state.context.mission?.id || state.context.mission_id;
          base.current_route = current;
          base.routing_latency_ms = Math.round(performance.now() - started);
          this._schedule(base, state);
        } catch (_) { /* observation cannot affect routing */ }
      },
    };
  }

  _write(row) {
    if (!this.config.enabled) return;
    // Independent sinks: a failed stdout logger cannot suppress persistence,
    // and persistence never returns anything to the production route.
    try { Promise.resolve(this._persistence.write(row)).catch(() => {}); }
    catch (_) { /* best-effort persistence */ }
    try {
      // Custom asynchronous sinks must handle durability themselves. Rejection
      // cannot become an unhandled rejection or a routing error.
      Promise.resolve(this._audit(row)).catch(() => {});
    } catch (_) { /* best-effort audit, same isolation as the provider */ }
  }

  _schedule(base, state) {
    if (this._pending.size >= this.config.maxPending) {
      this._write(this._row(base, { status: 'skipped', fallback_reason: 'capacity_limit' }));
      return;
    }
    // Defer all provider work until after production has returned its result.
    const task = new Promise(resolve => setImmediate(resolve))
      .then(() => this._evaluate(base, state))
      .catch(() => this._write(this._row(base, { status: 'error', errors: [{ code: 'observer_error' }] })));
    this._pending.add(task);
    task.finally(() => this._pending.delete(task));
  }

  _row(base, values = {}) {
    return {
      ...base, timestamp: new Date().toISOString(), status: 'fallback',
      intent: null, confidence: null, mission_bound_probability: null,
      approval_probability: null, inspection_probability: null,
      requires_human_clarification: null, risk_if_misrouted: null,
      recommended_route: null, route_matches: null, comparison: 'unavailable',
      model: null, raw_redacted_response: null, latency_ms: 0,
      fallback_provider: 'noop', fallback_reason: null, errors: [], ...values,
    };
  }

  async _evaluate(base, state) {
    const started = performance.now();
    const controller = new AbortController();
    let timer;
    let values;
    try {
      const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => {
          const error = new Error('Decision evaluation timed out');
          error.code = 'timeout';
          reject(error);
          controller.abort();
        }, this.config.timeoutMs);
      });
      const evaluation = await Promise.race([
        Promise.resolve().then(() => this.provider.evaluate(state, { signal: controller.signal })), timeout,
      ]);
      if (evaluation?.decision == null && this.provider.name === 'noop') {
        values = { status: 'fallback', fallback_reason: evaluation.fallback_reason };
      } else {
        const decision = parseDecision(evaluation?.decision);
        const comparable = base.current_route.route != null && decision.recommended_route !== 'unknown';
        const matches = comparable ? base.current_route.route === decision.recommended_route : null;
        values = {
          ...decision, status: 'evaluated', fallback_provider: null,
          model: /^jev-[a-zA-Z0-9._-]{1,80}$/.test(evaluation.model || '') ? evaluation.model : null,
          route_matches: matches, comparison: matches === null ? 'unavailable' : matches ? 'match' : 'mismatch',
          // Only the first-party adapter's validated projection is eligible.
          raw_redacted_response: this.config.logRaw && this.provider instanceof JevProvider
            ? evaluation.raw_redacted_response : null,
        };
      }
    } catch (error) {
      values = { status: 'error', fallback_reason: 'provider_failed', errors: [safeError(error)] };
    } finally {
      clearTimeout(timer);
    }
    this._write(this._row(base, { ...values, latency_ms: Math.round(performance.now() - started) }));
  }

  /** Tests/shutdown callers may drain; production routing never awaits this. */
  async drain() {
    await Promise.all([...this._pending]);
    try { await this._persistence.drain?.(); }
    catch (_) { /* shutdown/test diagnostics cannot become routing errors */ }
  }
}

function beginShadow(service, input, session, source) {
  try { return service?.begin(input, session, source) || null; }
  catch (_) { return null; }
}
function completeShadow(shadow, result, error) {
  try { shadow?.complete(result, error); }
  catch (_) { /* including injected observer bugs */ }
}

module.exports = { DecisionService, readConfig, selectProvider, beginShadow, completeShadow };
