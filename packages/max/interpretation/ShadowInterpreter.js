'use strict';
const { randomUUID } = require('node:crypto');
const { ModelFrameProducer } = require('./producers/ModelFrameProducer');
const { observedRoute } = require('../../decision-service/observedRoute');
const { estimateClaudeUsage } = require('../../../utils/apiPricing');

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
    focusIds, pendingDecisionId: null }));
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
    this.counts = { completed: 0, failed: 0, dropped: 0 };
  }
  capture(session) { return this.enabled ? snapshot(session) : null; }
  observe({ context, text, result }) {
    if (!this.enabled || !context) return;
    if (this.pending.size >= this.maxPending) { this.counts.dropped++; return; }
    // No result/frame is returned to the request or routing pipeline.
    const legacy = observedRoute(result);
    const task = new Promise(resolve => setImmediate(resolve)).then(async () => {
      if (!this.producer) {
        const Client = require('../../../utils/meteredAnthropic').forAgent('max-interpretation-shadow');
        // Bind spend accounting to each observation's tenant, never a shared mutable tenant.
        const client = new Client({ maxRetries: 0 }, { tenantId: context.tenantId });
        const producer = new ModelFrameProducer({ client, model: this.env.MAX_INTERPRETATION_MODEL || this.env.MAX_WORKSPACE_MODEL || 'claude-sonnet-4-6' });
        return this.run(producer, context, text, legacy);
      }
      return this.run(this.producer, context, text, legacy);
    }).catch(() => { this.counts.failed++; this.emit({ event: 'MAX_INTERPRETATION_FAILURE', reason: 'observer_failed' }); });
    this.pending.add(task);
    task.finally(() => this.pending.delete(task));
  }
  async run(producer, context, text, legacy) {
    const { frame, telemetry } = await producer.produce({ text, context });
    const intentRoute = { authorization: 'approval', prohibition: 'clarification', ambiguous: 'clarification' }[frame.intent]
      || (frame.subjectKind === 'none' ? 'conversation' : ['status'].includes(frame.requestedOutcome) ? 'inspection' : 'intelligence');
    const row = { id: randomUUID(), tenantId: context.tenantId, timestamp: new Date().toISOString(),
      frame, legacy, disagreement: legacy.route == null ? null : intentRoute !== legacy.route,
      latencyMs: telemetry.latencyMs, usage: telemetry.usage, model: telemetry.model,
      estimatedUsd: telemetry.usage ? estimateClaudeUsage(telemetry.model, telemetry.usage) : null,
      ambiguityCount: frame.ambiguities.length, failure: telemetry.degradedReason,
      // Informational classification only; never an authority token.
      shadowOnly: true };
    if (!this.persist) {
      const { createShadowPool } = require('../../decision-service/ShadowEventSink');
      if (!this.pool) { this.pool = createShadowPool(this.env); this.pool.on('error', () => {}); }
      await this.pool.query('INSERT INTO max_interpretation_shadow (id, tenant_id, observed_at, observation) VALUES ($1,$2,$3,$4)',
        [row.id, row.tenantId, row.timestamp, JSON.stringify(row)]);
    } else await this.persist(row);
    this.counts.completed++;
    this.emit({ event: 'MAX_INTERPRETATION_SHADOW', id: row.id, disagreement: row.disagreement, latencyMs: row.latencyMs, failure: row.failure });
  }
  emit(row) { try { this.audit(row); } catch (_) { /* observation cannot affect production */ } }
  async drain() { await Promise.all([...this.pending]); }
  async close() { await this.drain(); if (this.pool) await this.pool.end(); }
}
module.exports = { ShadowInterpreter, snapshot };
