'use strict';

const { performance } = require('node:perf_hooks');
const { estimateGeminiReservation, estimateGeminiUsage, sanitizedGeminiUsage } = require('./apiPricing');
const { reserveApiCall, settleApiCall } = require('./apiSpendControl');

const API_ORIGIN = 'https://generativelanguage.googleapis.com';

class GeminiApiError extends Error {
  constructor(status, body, requestId) {
    super('Gemini API request failed');
    this.name = 'GeminiApiError';
    this.status = status;
    this.error = body && typeof body === 'object' ? body.error : null;
    this.requestID = requestId || null;
  }
}

function createGeminiClient({ apiKey = process.env.GEMINI_API_KEY, fetchImpl = globalThis.fetch,
  agentName = 'max_workspace', tenantId = null, db, reserve = reserveApiCall, settle = settleApiCall } = {}) {
  if (!apiKey || typeof fetchImpl !== 'function') return null;
  return {
    async generateContent(params, options = {}) {
      const timingStarted = performance.now();
      const id = await reserve({ provider: 'google_gemini', agentName,
        tenantId: options.apiUsageContext?.tenantId ?? (typeof tenantId === 'function' ? tenantId() : tenantId),
        operation: 'models.generateContent', model: params.model,
        estimatedUsd: estimateGeminiReservation(params),
        pricingBasis: 'gemini_3_1_flash_lite_standard_2026_11_12' }, { db });
      const reservedAt = performance.now();
      const controller = new AbortController();
      const timeoutMs = Number.isFinite(options.timeout) && options.timeout > 0 ? options.timeout : 30000;
      const timeout = setTimeout(() => controller.abort(), timeoutMs);
      let response;
      let headersAt = null;
      let bodyAt = null;
      try {
        response = await fetchImpl(`${API_ORIGIN}/v1beta/models/${encodeURIComponent(params.model)}:generateContent`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'x-goog-api-key': apiKey },
          body: JSON.stringify(params.body),
          signal: controller.signal,
        });
        headersAt = performance.now();
        const body = await response.json().catch(() => null);
        bodyAt = performance.now();
        if (!response.ok) {
          throw new GeminiApiError(response.status, body, response.headers?.get?.('x-request-id'));
        }
        await settle(id, { status: 'ok', estimatedUsd: estimateGeminiUsage(params.model, body?.usageMetadata),
          usage: sanitizedGeminiUsage(body?.usageMetadata), model: params.model, httpStatus: response.status }, { db }).catch(() => {
          console.warn('[api-usage] Completed request remains conservatively reserved.');
        });
        if (body && typeof body === 'object') {
          Object.defineProperty(body, '__geminiTiming', { enumerable: false, value: {
            reserveMs: Math.round(reservedAt - timingStarted),
            fetchToHeadersMs: Math.round(headersAt - reservedAt),
            bodyReadMs: Math.round(bodyAt - headersAt),
            settleMs: Math.round(performance.now() - bodyAt),
            totalMs: Math.round(performance.now() - timingStarted),
          } });
        }
        return body;
      } catch (err) {
        await settle(id, { status: 'uncertain', httpStatus: err && err.status }, { db }).catch(() => {
          console.warn('[api-usage] Failed request remains reserved; charge is uncertain.');
        });
        if (err && typeof err === 'object') err.geminiTiming = {
          reserveMs: Math.round(reservedAt - timingStarted),
          fetchToHeadersMs: headersAt === null ? Math.round(performance.now() - reservedAt) : Math.round(headersAt - reservedAt),
          bodyReadMs: headersAt === null ? null : Math.round((bodyAt || performance.now()) - headersAt),
          settleMs: null,
          totalMs: Math.round(performance.now() - timingStarted),
        };
        throw err;
      } finally {
        clearTimeout(timeout);
      }
    },
  };
}

module.exports = { createGeminiClient, GeminiApiError, API_ORIGIN };
