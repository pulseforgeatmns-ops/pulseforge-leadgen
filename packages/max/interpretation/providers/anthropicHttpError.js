'use strict';

function sanitizeProviderMessage(message) {
  if (typeof message !== 'string' || !message) return null;
  return message.replace(/sk-ant-[a-zA-Z0-9_-]+/g, '[redacted]').slice(0, 500);
}

/**
 * Anthropic SDK APIError: `error` is the JSON body
 * `{ type, error: { type, message } }`. Never echo request or completion text.
 */
function providerHttpError(err) {
  if (!err || !Number.isInteger(err.status)) return null;
  const body = err.error && typeof err.error === 'object' ? err.error : null;
  const inner = body && body.error && typeof body.error === 'object' ? body.error : body;
  const message = sanitizeProviderMessage(
    (inner && typeof inner.message === 'string' && inner.message)
    || (body && typeof body.message === 'string' && body.message)
    || (typeof err.message === 'string' ? err.message : null)
  );
  const type = (inner && typeof inner.type === 'string' && inner.type)
    || (typeof err.type === 'string' && err.type)
    || null;
  return { status: err.status, type, message };
}

module.exports = { providerHttpError, sanitizeProviderMessage };
