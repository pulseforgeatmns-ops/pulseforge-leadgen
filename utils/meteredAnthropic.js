'use strict';

const { estimateClaudeReservation, estimateClaudeUsage, sanitizedUsage } = require('./apiPricing');
const { reserveApiCall, settleApiCall } = require('./apiSpendControl');

// Explicit caller label, no prompts, credentials or response text in telemetry.
function meterClient(client, { agentName = 'unknown', tenantId = null, db, reserve = reserveApiCall, settle = settleApiCall } = {}) {
  const create = client.messages.create.bind(client.messages);
  client.messages.create = async (params, options = {}) => {
    if (params.stream) throw Object.assign(new Error('api_usage_stream_not_supported'), { code: 'api_usage_stream_not_supported' });
    const { apiUsageContext = {}, ...requestOptions } = options;
    const id = await reserve({ provider: 'anthropic', agentName,
      tenantId: apiUsageContext.tenantId ?? (typeof tenantId === 'function' ? tenantId() : tenantId), operation: 'messages.create',
      model: params.model, estimatedUsd: estimateClaudeReservation(params), pricingBasis: 'claude_standard_token_estimate_2026_10_14' }, { db });
    let response;
    try {
      // Each retry is a separate paid attempt. Disable invisible SDK retries;
      // existing agent retry logic goes through this guard and ledger each time.
      response = await create(params, { ...requestOptions, maxRetries: 0 });
    } catch (err) {
      await settle(id, { status: 'uncertain', httpStatus: err.status }, { db }).catch(() => {
        console.warn('[api-usage] Failed request remains reserved; charge is uncertain.');
      });
      throw err;
    }
    await settle(id, { status: 'ok', estimatedUsd: estimateClaudeUsage(response.model || params.model, response.usage, params),
      usage: sanitizedUsage(response.usage), model: response.model || params.model, httpStatus: 200 }, { db }).catch(() => {
      // Never repeat a completed provider call because telemetry settlement failed.
      console.warn('[api-usage] Completed request remains conservatively reserved.');
    });
    return response;
  };
  return client;
}
function forAgent(agentName) {
  // Lazy SDK load allows pure tests and report scripts without API credentials.
  const SDK = require('@anthropic-ai/sdk');
  const Anthropic = SDK.Anthropic || SDK;
  return class MeteredAnthropic extends Anthropic {
    constructor(options, telemetry = {}) { super(options); meterClient(this, { ...telemetry, agentName }); }
  };
}
module.exports = { forAgent, meterClient };
