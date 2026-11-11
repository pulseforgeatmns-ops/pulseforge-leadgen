'use strict';

const { AsyncLocalStorage } = require('node:async_hooks');
const { randomUUID } = require('node:crypto');
const turns = new AsyncLocalStorage();
const TYPES = new Set(['invalid_request_error', 'authentication_error', 'permission_error',
  'not_found_error', 'request_too_large', 'rate_limit_error', 'api_error', 'overloaded_error']);
const CODES = new Set(['provider_auth', 'rate_limited', 'provider_overloaded', 'timeout',
  'provider_unreachable', 'spend_blocked', 'billing_unavailable', 'invalid_request', 'provider_error']);
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
// Only known request-schema paths, never arbitrary keys or user-supplied values.
const FIELD = /^(?:model|max_tokens|temperature|top_p|top_k|stream|stop_sequences|system|thinking(?:\.(?:type|budget_tokens))?|output_config(?:\.effort)?|tools|tool_choice|messages(?:\.[0-9]{1,6})?(?:\.(?:role|content)(?:\.[0-9]{1,6})?(?:\.(?:type|text|source|tool_use_id|name|input))?)?)$/;

// Do not invoke exception getters, toJSON, coercion hooks, or read headers.
function own(object, key) {
  if (!object || typeof object !== 'object') return undefined;
  const descriptor = Object.getOwnPropertyDescriptor(object, key);
  return descriptor && Object.hasOwn(descriptor, 'value') ? descriptor.value : undefined;
}
function matched(value, pattern, max) {
  return typeof value === 'string' && value.length <= max && pattern.test(value) ? value : null;
}
function field(value) { return matched(value, FIELD, 120); }

function validation(inner) {
  const explicit = field(own(inner, 'param')) || field(own(inner, 'parameter'));
  const message = own(inner, 'message');
  if (typeof message !== 'string' || message.length > 4096) {
    return { parameter: explicit, description: 'Provider validation details unavailable or omitted.' };
  }
  // A field prefix is explicit provider evidence, not inference from status/model.
  const prefix = /^([a-z_]+(?:\.(?:[a-z_]+|[0-9]{1,6}))*)\s*:\s*([^\n]*)$/.exec(message);
  const parameter = explicit || (prefix && field(prefix[1])) || null;
  const detail = prefix ? prefix[2] : message;
  let description = 'Provider validation details unavailable or omitted.';
  // Emit fixed summaries only. Never copy even a fragment of provider prose.
  if (parameter) {
    description = 'Provider rejected an explicit request field; message omitted.';
    if (/^(?:Field required|field required|is required|Missing required field)\.?$/.test(detail)) description = 'Required field missing.';
    else if (/^(?:Extra inputs are not permitted|extra fields not permitted|Unknown field)\.?$/.test(detail)) description = 'Field is not permitted.';
    else if (/^(?:Input should be a valid (?:string|integer|number|list|dictionary|boolean)|must be (?:a string|an integer|a number|a list|an object|a boolean))\.?$/.test(detail)) description = 'Field has an invalid type.';
    else if (/^(?:Input should be greater than(?: or equal to)?|Input should be less than(?: or equal to)?|must be greater than(?: or equal to)?|must be less than(?: or equal to)?) -?[0-9]{1,9}(?:\.[0-9]{1,9})?\.?$/.test(detail)) description = 'Field is outside the permitted numeric range.';
  }
  return { parameter, description };
}

function buildFailureDiagnostic(error, errorCode) {
  const envelope = own(error, 'error');
  const inner = own(envelope, 'error') || envelope;
  const type = own(inner, 'type');
  const status = own(error, 'status');
  const context = turns.getStore() || {};
  const details = validation(inner);
  return {
    event: 'MAX_PRESENTATION_PROVIDER_FAILURE', schemaVersion: 1,
    provider: 'anthropic', stage: 'presentation.messages.create',
    errorCode: CODES.has(errorCode) ? errorCode : 'provider_error',
    providerErrorType: TYPES.has(type) ? type : null,
    httpStatus: Number.isInteger(status) && status >= 100 && status <= 599 ? status : null,
    requestId: matched(own(error, 'requestID'), /^req_[A-Za-z0-9]{1,100}$/, 104)
      || matched(own(error, 'request_id'), /^req_[A-Za-z0-9]{1,100}$/, 104)
      || matched(own(envelope, 'request_id'), /^req_[A-Za-z0-9]{1,100}$/, 104),
    parameter: details.parameter, validationDescription: details.description,
    turnId: matched(context.turnId, UUID, 36),
    sessionId: matched(context.sessionId, UUID, 36),
    tenantId: matched(context.tenantId, /^[0-9]{1,20}$/, 20),
    operatorTurnSeq: Number.isSafeInteger(context.operatorTurnSeq) && context.operatorTurnSeq > 0 ? context.operatorTurnSeq : null,
    revision: matched(process.env.RAILWAY_GIT_COMMIT_SHA || process.env.GIT_COMMIT, /^[a-f0-9]{7,40}$/i, 40),
  };
}

function logPresentationFailure(error, errorCode) {
  // Neither malformed SDK errors nor a broken log sink may break fallback.
  try {
    console.warn('[max-presentation] model completion failed; deterministic prose used', buildFailureDiagnostic(error, errorCode));
  } catch (_) { /* diagnostic only; never retry a model or interrupt fallback */ }
}
function withPresentationTurn(fn) {
  return turns.run({ turnId: randomUUID() }, fn);
}
function bindPresentationTurn(session) {
  const context = turns.getStore();
  if (!context) return;
  context.sessionId = session.id;
  context.tenantId = session.context?.tenantId;
  context.operatorTurnSeq = session.operatorTurnSeq;
}

module.exports = { buildFailureDiagnostic, logPresentationFailure, withPresentationTurn, bindPresentationTurn };
