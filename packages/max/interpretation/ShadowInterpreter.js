'use strict';
const { randomUUID } = require('node:crypto');
const { ModelFrameProducer } = require('./producers/ModelFrameProducer');
const { resolveInterpreterModel, resolveInterpreterProvider, servedModelMatches } = require('./interpreterModel');
const { createGeminiProvider } = require('./providers');
const { observedRoute } = require('../../decision-service/observedRoute');
const { estimateClaudeUsage, GEMINI_RATES } = require('../../../utils/apiPricing');

/** Cost of one observed call, priced by its provider from the served model (null when unpriced). */
function estimateObservationUsd(provider, model, usage) {
  if (!usage) return null;
  if (provider === 'gemini') {
    const rates = GEMINI_RATES[model];
    if (!rates || usage.input_tokens == null) return null;
    const output = (usage.output_tokens || 0) + (usage.thinking_tokens || 0);
    return (usage.input_tokens * rates[0] + output * rates[1]) / 1e6;
  }
  return estimateClaudeUsage(model, usage);
}
const { recordObserverHealth } = require('../observability/ObserverHealth');
const { OPERATOR_DECISION_KINDS } = require('../../acquisition-mission/types');

const DECISION_KINDS = new Set(Object.values(OPERATOR_DECISION_KINDS));
const MAX_CANDIDATES = 40;

// Captured before production handles the turn. Never retain a mutable session,
// transcript, raw message, or model client in the persisted observation.
function snapshot(session) {
  const tenantId = session?.context?.tenantId;
  if (tenantId == null) return null;
  const candidates = [];
  const missionId = session.context.acquisitionMissionId || session.context.missionId;
  if (missionId) candidates.push({ id: String(missionId), kind: 'mission', label: 'Current mission' });
  const focus = require('../workspace/OperationalQuestionFrame').currentFocus(session, tenantId);
  const focusIds = (focus?.missionIds || []).slice(0,16).map(String);
  for (const id of focusIds) if (!candidates.some(c => c.id === id)) {
    candidates.push({id,kind:'mission',label:'Recent analytical focus'});
  }
  const evidence = session.operationalEvidence;
  if (String(evidence?.tenantId) === String(tenantId)) for (const mission of (evidence.missions || []).slice(0,16)) {
    if (mission.id && !candidates.some(c => c.id === String(mission.id))) candidates.push({id:String(mission.id),kind:'mission',label:String(mission.objective || mission.name || 'Recent mission').slice(0,160)});
  }
  return JSON.parse(JSON.stringify({ tenantId: String(tenantId), candidates,
    focusIds, pendingDecisionId: null, pendingDecisionKind: null }));
}

/**
 * The pending operator decision of a mission production has already resolved
 * for this turn, reduced to an opaque candidate. Only the decision KIND is
 * kept: its prompt and package content never reach the interpreter or the
 * persisted observation. A mission from another tenant, or without a tenant,
 * is ignored (fail closed). This lets validateTurnFrame bind an
 * `authorization` frame to a real decision; it grants nothing — the observer
 * has no route, approval or execution callback and frames carry
 * authority.grantsAuthority = false.
 */
function pendingDecisionCandidate(context, mission) {
  if (!context || !mission || typeof mission !== 'object') return null;
  const pending = mission.pendingOperatorDecision;
  if (!pending || typeof pending !== 'object') return null;
  if (mission.tenantId == null || String(mission.tenantId) !== String(context.tenantId)) return { rejected: 'pending_decision_tenant_mismatch' };
  if (typeof mission.id !== 'string' || !/^[A-Za-z0-9_.:-]{1,160}$/.test(mission.id)) return { rejected: 'pending_decision_mission_invalid' };
  const kind = DECISION_KINDS.has(pending.kind) ? pending.kind : 'unknown';
  return { missionId: mission.id, kind, id: `pd:${mission.id}:${kind}` };
}

class ShadowInterpreter {
  constructor({ env = process.env, producer, persist, audit = () => {} } = {}) {
    this.enabled = env.MAX_INTERPRETATION_SHADOW === 'true';
    this.env = env;
    this.producer = producer;
    this.persist = persist;
    this.audit = audit;
    this.pending = new Set();
    this.maxPending = 16;
    this.counts = { completed: 0, failed: 0, dropped: 0, notConfigured: 0 };
  }
  capture(session) {
    if (!this.enabled) return null;
    try { return snapshot(session); }
    catch (_) { this.counts.failed++; recordObserverHealth('semantic_shadow', 'failure', {reason:'context_unavailable'}); this.emit({event:'MAX_INTERPRETATION_FAILURE',reason:'context_unavailable'}); return null; }
  }
  /**
   * Add the pending decision of a mission production already read (the same
   * point at which the JEV observer snapshots it). Mutates only the observer's
   * own captured clone; never the mission or the session.
   */
  captureMission(context, mission) {
    if (!this.enabled || !context) return;
    try {
      const pd = pendingDecisionCandidate(context, mission);
      if (!pd) return;
      if (pd.rejected) { recordObserverHealth('semantic_shadow', 'failure', {reason:pd.rejected}); return; }
      if (!context.candidates.some(c => c.id === pd.missionId) && context.candidates.length < MAX_CANDIDATES) {
        context.candidates.push({ id: pd.missionId, kind: 'mission', label: 'Current mission' });
      }
      if (!context.candidates.some(c => c.id === pd.id) && context.candidates.length < MAX_CANDIDATES) {
        context.candidates.push({ id: pd.id, kind: 'pending_decision', label: `Pending ${pd.kind.replace(/_/g, ' ')}` });
      }
      context.pendingDecisionId = pd.id;
      context.pendingDecisionKind = pd.kind;
    } catch (_) { recordObserverHealth('semantic_shadow', 'failure', {reason:'pending_decision_capture_failed'}); }
  }
  observe({ context, text, result }) {
    if (!this.enabled || !context) return;
    if (this.pending.size >= this.maxPending) { this.counts.dropped++; recordObserverHealth('semantic_shadow', 'dropped', {pending:this.pending.size}); return; }
    // No result/frame is returned to the request or routing pipeline.
    let legacy;
    try { legacy = observedRoute(result); }
    catch (_) { this.counts.failed++; recordObserverHealth('semantic_shadow', 'failure', {reason:'legacy_metadata_invalid'}); this.emit({event:'MAX_INTERPRETATION_FAILURE',reason:'legacy_metadata_invalid'}); return; }
    let producer = this.producer;
    if (!producer) {
      const resolved = resolveInterpreterModel(this.env);
      if (!resolved.configured) {
        // Reported, never guessed: no model call, no row.
        this.counts.notConfigured++;
        recordObserverHealth('semantic_shadow', 'failure', {reason:resolved.reason});
        this.emit({ event: 'MAX_INTERPRETATION_FAILURE', reason: resolved.reason });
        return;
      }
      const transport = resolveInterpreterProvider(this.env);
      if (!transport.configured) {
        this.counts.notConfigured++;
        recordObserverHealth('semantic_shadow', 'failure', {reason:transport.reason});
        this.emit({ event: 'MAX_INTERPRETATION_FAILURE', reason: transport.reason });
        return;
      }
      if (transport.provider === 'gemini') {
        // Shared spend ledger (default reserve/settle), bound to this observation's tenant.
        const { createGeminiClient } = require('../../../utils/meteredGemini');
        const client = createGeminiClient({ apiKey: this.env.GEMINI_API_KEY, agentName: 'max-interpretation-shadow', tenantId: context.tenantId });
        if (!client) {
          this.counts.notConfigured++;
          recordObserverHealth('semantic_shadow', 'failure', {reason:'interpreter_credentials_missing'});
          this.emit({ event: 'MAX_INTERPRETATION_FAILURE', reason: 'interpreter_credentials_missing' });
          return;
        }
        producer = new ModelFrameProducer({ provider: createGeminiProvider({ client, tenantId: context.tenantId }), model: resolved.model });
      } else {
        const Client = require('../../../utils/meteredAnthropic').forAgent('max-interpretation-shadow');
        // Bind spend accounting to each observation's tenant, never a shared mutable tenant.
        const client = new Client({ maxRetries: 0 }, { tenantId: context.tenantId });
        producer = new ModelFrameProducer({ client, model: resolved.model });
      }
    }
    const task = new Promise(resolve => setImmediate(resolve))
      .then(() => this.run(producer, context, text, legacy))
      .catch(() => { this.counts.failed++; recordObserverHealth('semantic_shadow', 'failure', {reason:'observer_failed'}); this.emit({ event: 'MAX_INTERPRETATION_FAILURE', reason: 'observer_failed' }); });
    this.pending.add(task);
    task.finally(() => this.pending.delete(task));
  }
  async run(producer, context, text, legacy) {
    const { frame, telemetry } = await producer.produce({ text, context });
    const degradedReason = telemetry.degradedReason || null;
    const frameSource = degradedReason ? 'degraded_fallback' : 'model';
    const servedModel = telemetry.servedModel || null;
    const intentRoute = { authorization: 'approval', prohibition: 'clarification', ambiguous: 'clarification' }[frame.intent]
      || (frame.subjectKind === 'none' ? 'conversation' : ['status'].includes(frame.requestedOutcome) ? 'inspection' : 'intelligence');
    const row = { id: randomUUID(), tenantId: context.tenantId, timestamp: new Date().toISOString(),
      frame, frameSource, legacy,
      // A degraded frame is the lexical fallback, not the model's reading:
      // comparing it with the legacy route would report agreement that the
      // model never produced. It is incomparable (null), like a missing route.
      disagreement: degradedReason || legacy.route == null ? null : intentRoute !== legacy.route,
      pendingDecisionBound: Boolean(context.pendingDecisionId),
      pendingDecisionKind: context.pendingDecisionKind || null,
      latencyMs: telemetry.latencyMs, usage: telemetry.usage,
      provider: telemetry.provider || 'anthropic',
      model: telemetry.model, servedModel,
      modelMismatch: servedModel ? !servedModelMatches(telemetry.model, servedModel, telemetry.provider || 'anthropic') : null,
      stopReason: telemetry.stopReason || null,
      truncated: degradedReason === 'stop_reason_max_tokens',
      estimatedUsd: estimateObservationUsd(telemetry.provider || 'anthropic',
        // A served version that matches the requested id is priced at the requested model's rate.
        servedModel && !servedModelMatches(telemetry.model, servedModel, telemetry.provider || 'anthropic') ? servedModel : (servedModel && (telemetry.provider || 'anthropic') === 'anthropic' ? servedModel : telemetry.model),
        telemetry.usage),
      ambiguityCount: frame.ambiguities.length, failure: degradedReason,
      // Informational classification only; never an authority token.
      shadowOnly: true };
    if (!this.persist) {
      const { createShadowPool } = require('../../decision-service/ShadowEventSink');
      if (!this.pool) { this.pool = createShadowPool(this.env); this.pool.on('error', () => {}); }
      await this.pool.query('INSERT INTO max_interpretation_shadow (id, tenant_id, observed_at, observation) VALUES ($1,$2,$3,$4)',
        [row.id, row.tenantId, row.timestamp, JSON.stringify(row)]);
    } else await this.persist(row);
    this.counts.completed++;
    // A persisted degraded observation is a successful observation of a failed
    // model call: health records both, so readiness cannot read it as healthy.
    if (degradedReason) recordObserverHealth('semantic_shadow', 'failure', {reason:`model_${degradedReason}`, pending:this.pending.size});
    else recordObserverHealth('semantic_shadow', 'success', {pending:this.pending.size});
    this.emit({ event: 'MAX_INTERPRETATION_SHADOW', id: row.id, disagreement: row.disagreement, latencyMs: row.latencyMs,
      failure: row.failure, frameSource, truncated: row.truncated, modelMismatch: row.modelMismatch });
  }
  emit(row) { try { Promise.resolve(this.audit(row)).catch(() => {}); } catch (_) { /* observation cannot affect production */ } }
  async drain() { await Promise.all([...this.pending]); }
  async close() { await this.drain(); if (this.pool) await this.pool.end(); }
}
module.exports = { ShadowInterpreter, snapshot, pendingDecisionCandidate };
