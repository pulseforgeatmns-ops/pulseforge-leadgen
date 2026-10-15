'use strict';
const {performance}=require('node:perf_hooks');
function assertSendFence(fence) {
  if(!fence || !Number.isFinite(fence.deadline) || performance.now()>=fence.deadline) throw new Error('activation_window_closed');
}
// Importing this module never loads agents, credentials, databases or providers.
function formatDistributionAlert(event) {
  const prefix = event.namespace === 'test' ? 'TEST ONLY — ' : '';
  const metrics = event.metrics || {};
  const value = key => metrics[key] == null ? 'UNKNOWN' : String(metrics[key]);
  return [
    `${prefix}Signal ${event.kind === 'MILESTONE_2X' ? `observed ${(Number.isFinite(event.milestoneMultiple) ? event.milestoneMultiple : 'UNKNOWN')}x milestone` : `risk ${event.state}`}`,
    `CA: ${event.tokenAddress || 'UNKNOWN'}`,
    `As of: ${event.observedAt || 'UNKNOWN'}`,
    `Reasons: ${(event.reasons || []).map(r => typeof r === 'string' ? r : r.code || 'UNKNOWN').join(', ') || 'none'}`,
    `Reference multiple: ${value('referenceMultiple')}; observed MFE: ${value('observedMfeMultiple')}; drawdown fraction: ${value('drawdownFromObservedPeak')}`,
    `Coverage: ${JSON.stringify(event.coverage || {wallet:'UNKNOWN',developer:'UNKNOWN'})}`,
    `Observed-sample basis: price ${value('priceUsd')}; reserve ${value('liquidityUsd')}; sampled ${event.observedAt || 'UNKNOWN'}; provider timestamp ${event.evidence?.providerTimestamp || event.providerTimestamp || 'UNKNOWN'}`,
    `Evidence: pair ${event.evidence?.pairId || 'UNKNOWN'}; basis ${event.evidence?.timestampBasis || 'UNKNOWN'}; provider ${event.evidence?.provider || 'UNKNOWN'}`,
    `Rolling 5m: volume USD ${value('volume5mUsd')}; buy/sell counts ${value('buys5m')}/${value('sells5m')}; buy/sell volume USD ${value('buyVolume5mUsd')}/${value('sellVolume5mUsd')}`,
    'Aggregate reserve decline is not confirmed liquidity withdrawal. Wallet/developer coverage remains UNKNOWN without a verified provider.',
    'EXPERIMENTAL / MANUAL DECISION. Observed samples only. No trade executed.',
  ].join('\n');
}
function formatDistributionSms(event) {
  // Restrict to GSM7 single-septet ASCII. GSM extension characters are removed.
  const clean = value => String(value ?? '').replace(/[^a-zA-Z0-9 .,;:_+%()\/=!?@#&*-]/g,' ').replace(/\s+/g,' ').trim();
  const metric = key => Number.isFinite(event.metrics?.[key]) ? Number(event.metrics[key].toPrecision(4)).toString() : 'UNKNOWN';
  const label = event.kind === 'MILESTONE_2X'
    ? `milestone ${Number.isFinite(event.milestoneMultiple) ? event.milestoneMultiple : 'UNKNOWN'}x`
    : `risk ${clean(event.state).slice(0,8)}`;
  const header = `${event.namespace === 'test' ? 'TEST ' : ''}Signal ${label} CA:${clean(event.tokenAddress || 'UNKNOWN').slice(0,44)} At:${clean(event.observedAt || 'UNKNOWN').slice(0,24)} `;
  const details = `Mult:${metric('referenceMultiple')} DD:${Number.isFinite(event.metrics?.drawdownFromObservedPeak) ? Number((100*event.metrics.drawdownFromObservedPeak).toPrecision(4))+'%' : 'UNKNOWN'} `;
  const footer = ' SOURCE TIME UNKNOWN; Manual only; no trade';
  const reasons = clean((event.reasons || []).map(r=>typeof r==='string'?r:r.code).join(','));
  return (header + details + 'Why:' + reasons.slice(0,Math.max(0,300-header.length-details.length-footer.length-4)) + footer).slice(0,300);
}
function createSmsAdapter({enabled = () => false, consented = () => false, destination, from, client} = {}) {
  return { id: 'sms', async send(event, _deliveryId, fence) {
    if (event.namespace !== 'operational' || !enabled() || !consented() || !/^\+[1-9]\d{7,14}$/.test(destination || '')
      || !/^\+[1-9]\d{7,14}$/.test(from || '') || typeof client?.messages?.create !== 'function') throw new Error('transport_disabled');
    // Existing Twilio ops primitive, without samAgent's database/tenant imports.
    const payload={body:formatDistributionSms(event),from,to:destination};
    assertSendFence(fence);
    const result = await client.messages.create(payload);
    if (typeof result?.sid !== 'string' || !result.sid || result.sid.length > 512) throw new Error('transport_unknown');
    return {accepted: true, receipt: result.sid};
  }};
}
function createEmailAdapter({enabled = () => false, consented = () => false, sendMail} = {}) {
  return { id: 'email', async send(event, deliveryId, fence) {
    if (event.namespace !== 'operational' || !enabled() || !consented() || typeof sendMail !== 'function') throw new Error('transport_disabled');
    // Inject the approved recipient-specific Brevo sender, never an outreach agent.
    const payload={id: deliveryId, subject: `[Signal] ${event.kind === 'MILESTONE_2X' ? `${(Number.isFinite(event.milestoneMultiple) ? event.milestoneMultiple : 'UNKNOWN')}x observed` : event.state}`,
      textContent: formatDistributionAlert(event)};
    assertSendFence(fence);
    const result = await sendMail(payload);
    if (!result?.accepted || typeof result.receipt !== 'string' || !result.receipt || result.receipt.length > 512) throw new Error('transport_unknown');
    return result;
  }};
}
function createDistributionDispatcher({store, adapters = [], enabled = () => false, consented = () => false} = {}) {
  return { async sendOnce({tenantId, namespace = 'operational', channel} = {}) {
    const adapter = adapters.find(a => a.id === channel);
    if (namespace !== 'operational' || !enabled(channel) || !consented(channel) || !adapter) return {skipped:true};
    const row = await store.claim({tenantId, namespace, channel});
    if (!row) return {empty:true};
    try {
      if (!enabled(channel) || !consented(channel) || row.namespace !== 'operational' || row.channel !== channel
        || String(row.tenant_id) !== String(tenantId)) throw new Error('transport_disabled');
      const fenceStarted=performance.now();
      const authorization=await store.authorizeSend(row.id);
      if(!authorization.authorized)return {accepted:false,suppressed:true,reason:authorization.reason};
      // Starting before the DB roundtrip makes the remaining-time estimate conservative.
      const fence={deadline:fenceStarted+authorization.remainingMs};
      assertSendFence(fence);
      const result = await adapter.send(row.payload, row.id, fence);
      if (!result?.accepted || !result.receipt) throw new Error('transport_unknown');
      await store.accepted(row.id, result.receipt);
      return {accepted:true, received:false, displayed:false};
    } catch (_) {
      // Provider errors/timeouts and expired claims are ambiguous. No automatic
      // SMS retry: the email provider's idempotency guarantee does not apply.
      await store.unknown(row.id);
      return {accepted:false, unknown:true};
    }
  }};
}
module.exports = {formatDistributionAlert, formatDistributionSms, createSmsAdapter, createEmailAdapter, createDistributionDispatcher};
