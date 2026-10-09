'use strict';

const { createHash } = require('crypto');
const { knowledgeAt } = require('../prospective/knowledgeClock');
const { buildLatencyDiagnostics } = require('./latencyDiagnostics');

// No transport, external network, public route, or automatic enqueue hook.
// Callers must use authenticated internal access after destination approval.
function buildOperatorAlert({
  evidence,
  snapshot,
  approvedChannelId,
  researchState = 'PENDING_RESEARCH',
  independentConvergence = 'none',
  timestamps = {},
}) {
  if (evidence?.provenance?.dataClass !== 'EMPIRICAL' || evidence.provenance.testOnly || evidence.provenance.synthetic
    || evidence.sourceId !== 'telegram-front-runners' || !evidence.externalMessageId
    || !evidence.extractedCa || !approvedChannelId
    || String(evidence.provenance.telegramChannelId) !== String(approvedChannelId)) {
    throw new Error('operator_alert_requires_front_runners_empirical_evidence');
  }
  if (!['PENDING_RESEARCH', 'FIRST_CALLER', 'PENDING_OUTCOME_24H'].includes(researchState)) {
    throw new Error('invalid_operator_research_state');
  }
  const knownAt = knowledgeAt(evidence.occurredAt, evidence.ingestedAt);
  if (!Number.isFinite(knownAt.getTime())) throw new Error('invalid_evidence_time');
  const id = createHash('sha256').update(JSON.stringify([
    evidence.sourceId, evidence.externalMessageId, evidence.extractedCa,
  ])).digest('hex');
  const valid = snapshot?.tokenAddress === evidence.extractedCa
    && snapshot.provenance?.dataClass === 'EMPIRICAL' && !snapshot.provenance.testOnly && !snapshot.provenance.synthetic;
  const metric = name => valid && snapshot[name] != null && Number.isFinite(Number(snapshot[name]))
    && Number(snapshot[name]) >= 0 ? Number(snapshot[name]) : null;
  const createdAt = timestamps.alertCreatedAt || new Date();
  const latency = buildLatencyDiagnostics(evidence, {
    ...timestamps,
    alertCreatedAt: createdAt,
  });
  const convergenceStates = new Set(['none', 'pending', 'confirmed']);
  if (!convergenceStates.has(independentConvergence)) {
    throw new Error('invalid_independent_convergence_state');
  }
  return {
    id, sourceId: evidence.sourceId, externalMessageId: evidence.externalMessageId,
    tokenAddress: evidence.extractedCa, evidenceId: evidence.id,
    channelId: String(evidence.provenance.telegramChannelId),
    occurredAt: new Date(evidence.occurredAt).toISOString(),
    ingestedAt: new Date(evidence.ingestedAt).toISOString(), knowledgeAt: knownAt.toISOString(),
    researchState,
    independentConvergence,
    experimentalLabel: 'EXPERIMENTAL / MANUAL DECISION',
    paperOnly: true,
    createdAt: new Date(createdAt).toISOString(),
    market: {
      priceUsd: metric('priceUsd'), marketCapUsd: metric('marketCapUsd'), liquidityUsd: metric('liquidityUsd'),
      volumeIntervalUsd: metric('volumeIntervalUsd'),
      tokenAgeSeconds: valid ? snapshot.tokenAgeSeconds ?? null : null,
      sampledAt: valid ? snapshot.observedTimestamp || null : null,
      providerTimestamp: valid ? snapshot.providerTimestamp || null : null,
      provider: valid ? snapshot.provider : null,
      freshness: valid ? (snapshot.provenance?.freshness === 'STALE' ? 'STALE' : 'UNKNOWN') : 'UNAVAILABLE',
    },
    risks: ['Unverified caller claim', 'Source independence UNKNOWN', 'Market freshness not verified', 'No trade executed'],
    tradeDestination: null,
    latency,
  };
}

class OperatorAlertOutbox {
  constructor(pool, { channelId } = {}) { this.pool = pool; this.channelId = channelId; }
  async enqueue(input) {
    const alert = buildOperatorAlert({ ...input, approvedChannelId: this.channelId });
    await this.pool.query(`INSERT INTO signal_operator_alert_outbox
      (id, source_id, external_message_id, token_address, evidence_id, payload)
      VALUES ($1,$2,$3,$4,$5,$6::jsonb)
      ON CONFLICT (source_id,external_message_id,token_address) DO NOTHING`,
    [alert.id,alert.sourceId,alert.externalMessageId,alert.tokenAddress,alert.evidenceId,JSON.stringify(alert)]);
    return { id: alert.id, deliveryState: 'PENDING', delivered: false };
  }
  async listPending(limit = 20) {
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new Error('invalid_outbox_limit');
    const result = await this.pool.query(`SELECT id,payload,created_at FROM signal_operator_alert_outbox
      WHERE delivery_state='PENDING' ORDER BY created_at,id LIMIT $1`, [limit]);
    return result.rows;
  }
}
module.exports = { buildOperatorAlert, OperatorAlertOutbox };
