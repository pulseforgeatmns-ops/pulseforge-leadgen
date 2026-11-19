'use strict';

/**
 * Free inspection of the Anthropic Messages request the interpretation eval
 * actually sends. Models API preflight cannot see this: it never POSTs
 * /v1/messages and never compiles json_schema.
 *
 * The eval client must send anthropic-version 2026-10-21 (SDK 0.90.0 defaults
 * to 2023-06-01). Current Messages structured-output docs use 2026-10-21; that
 * version also 400s temperature/top_p/top_k, so the body must omit them.
 */

const { TURN_FRAME_OUTPUT_SCHEMA } = require('../TurnFrame');
const { buildRequestParams } = require('../providers/AnthropicProvider');
const { providerHttpError, sanitizeProviderMessage } = require('../providers/anthropicHttpError');

const ANTHROPIC_MESSAGES_API_VERSION = '2026-10-21';
const HAIKU = 'claude-haiku-5-5';

const UNSUPPORTED_SCHEMA_KEYS = Object.freeze([
  'oneOf', 'not', 'minimum', 'maximum', 'multipleOf', 'minLength', 'maxLength', 'maxItems',
]);

function headerMap(headers) {
  const out = {};
  if (!headers) return out;
  if (typeof headers.forEach === 'function') {
    headers.forEach((value, key) => { out[String(key).toLowerCase()] = value; });
    return out;
  }
  for (const [key, value] of Object.entries(headers)) out[String(key).toLowerCase()] = value;
  return out;
}

function parseBody(raw) {
  if (raw == null) return null;
  if (typeof raw === 'string') {
    try { return JSON.parse(raw); } catch (_) { return null; }
  }
  return raw;
}

/**
 * Compare the intercepted SDK fetch against the documented Haiku 5.5
 * interpretation request. Does not call Anthropic: the caller supplies fetch.
 */
function inspectAnthropicMessagesWire(req, { thinking = 'disabled' } = {}) {
  const issues = [];
  if (!req) return { ok: false, issues: ['no_request_captured'] };
  const url = String(req.url || '');
  const headers = headerMap(req.headers);
  const body = parseBody(req.body);
  const method = String(req.method || 'POST').toUpperCase();
  if (method !== 'POST') issues.push(`method:${method}`);
  if (!url.includes('/v1/messages') || url.includes('/v1/models')) issues.push(`url:${url}`);
  const anthropicVersion = headers['anthropic-version'] || null;
  if (anthropicVersion !== ANTHROPIC_MESSAGES_API_VERSION) {
    issues.push(`anthropic-version:${anthropicVersion || 'missing'}`);
  }
  const beta = headers['anthropic-beta'] || null;
  if (beta) issues.push(`anthropic-beta:${beta}`);
  if (!body || typeof body !== 'object') {
    issues.push('body_missing');
    return { ok: false, issues, url, method, anthropicVersion, beta, body: null };
  }
  for (const k of ['temperature', 'top_p', 'top_k', 'output_format']) {
    if (k in body) issues.push(`unexpected_field:${k}`);
  }
  if (body.output_config?.effort !== 'low') issues.push(`effort:${body.output_config && body.output_config.effort}`);
  const format = body.output_config && body.output_config.format;
  if (!format || format.type !== 'json_schema') issues.push('output_config.format.type');
  if (JSON.stringify(format && format.schema) !== JSON.stringify(TURN_FRAME_OUTPUT_SCHEMA)) {
    issues.push('output_config.format.schema');
  }
  if (thinking === 'disabled') {
    if (!body.thinking || body.thinking.type !== 'disabled') issues.push(`thinking:${JSON.stringify(body.thinking || null)}`);
  } else if (body.thinking) {
    issues.push('thinking_unexpected');
  }
  return {
    ok: issues.length === 0,
    issues,
    url,
    method,
    anthropicVersion,
    beta,
    thinking: body.thinking || null,
    output_config: {
      effort: body.output_config && body.output_config.effort,
      formatType: format && format.type,
    },
    sampling: {
      temperature: Object.prototype.hasOwnProperty.call(body, 'temperature'),
      top_p: Object.prototype.hasOwnProperty.call(body, 'top_p'),
      top_k: Object.prototype.hasOwnProperty.call(body, 'top_k'),
    },
  };
}

function evalAnthropicClientOptions({ apiKey, fetch } = {}) {
  return {
    apiKey,
    maxRetries: 0,
    defaultHeaders: { 'anthropic-version': ANTHROPIC_MESSAGES_API_VERSION },
    ...(fetch ? { fetch } : {}),
  };
}

function interceptingFetch(bucket) {
  return async (url, init = {}) => {
    bucket.push({ url: String(url), method: init.method || 'GET', headers: init.headers, body: init.body });
    return new Response(JSON.stringify({
      type: 'error',
      error: { type: 'invalid_request_error', message: 'eval_wire_probe_not_sent' },
    }), { status: 400, headers: { 'content-type': 'application/json' } });
  };
}

/**
 * Drive the real SDK through messages.create with a fetch interceptor.
 * Never reaches api.anthropic.com.
 */
async function captureAnthropicMessagesWire({
  Anthropic,
  input,
  thinking = 'disabled',
  clientOptions = {},
} = {}) {
  if (typeof Anthropic !== 'function') throw new Error('captureAnthropicMessagesWire requires the Anthropic SDK constructor');
  const captured = [];
  const client = new Anthropic({
    ...evalAnthropicClientOptions({ apiKey: 'sk-ant-eval-wire-probe-not-sent', fetch: interceptingFetch(captured) }),
    ...clientOptions,
    fetch: interceptingFetch(captured),
  });
  const params = buildRequestParams({
    model: HAIKU,
    text: input.text,
    context: input.context,
    transcript: input.transcript || [],
    thinking,
  });
  try {
    await client.messages.create(params, { maxRetries: 0 });
  } catch (_) { /* interceptor always 400s */ }
  const messagesReq = captured.find((r) => String(r.url).includes('/v1/messages'));
  return { captured, inspection: inspectAnthropicMessagesWire(messagesReq, { thinking }) };
}

function walkSchema(node, path, issues) {
  if (!node || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    node.forEach((item, i) => walkSchema(item, `${path}[${i}]`, issues));
    return;
  }
  for (const key of UNSUPPORTED_SCHEMA_KEYS) {
    if (Object.prototype.hasOwnProperty.call(node, key)) issues.push(`${path}.${key}`);
  }
  if (Object.prototype.hasOwnProperty.call(node, 'minItems') && node.minItems !== 0 && node.minItems !== 1) {
    issues.push(`${path}.minItems`);
  }
  if (node.$ref && typeof node.$ref === 'string' && /^https?:/i.test(node.$ref)) issues.push(`${path}.$ref`);
  if (node.anyOf && (node.type || node.properties)) issues.push(`${path}.anyOf_with_type_or_properties`);
  if (node.type === 'object') {
    if (node.additionalProperties !== false) issues.push(`${path}.additionalProperties`);
  }
  for (const [key, value] of Object.entries(node)) {
    if (key === 'enum') continue;
    walkSchema(value, `${path}.${key}`, issues);
  }
}

/** Local check against documented structured-output JSON Schema limits. */
function inspectTurnFrameSchemaForStructuredOutputs(schema = TURN_FRAME_OUTPUT_SCHEMA) {
  const issues = [];
  walkSchema(schema, '$', issues);
  return { ok: issues.length === 0, issues };
}

module.exports = {
  ANTHROPIC_MESSAGES_API_VERSION,
  evalAnthropicClientOptions,
  inspectAnthropicMessagesWire,
  captureAnthropicMessagesWire,
  inspectTurnFrameSchemaForStructuredOutputs,
  providerHttpError,
  sanitizeProviderMessage,
};
