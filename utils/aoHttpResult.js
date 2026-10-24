'use strict';

/**
 * AO services often return `{ error, status }` for HTTP failures. The same payloads
 * must not use `status` for domain state (conversation "active", CRM account state, etc.).
 */
function aoResultHttpStatus(result) {
  if (!result || result.error == null || result.error === '') return null;
  const code = result.httpStatus ?? result.statusCode ?? result.status;
  if (typeof code === 'number' && Number.isInteger(code) && code >= 400 && code <= 599) {
    return code;
  }
  return null;
}

function sendAoServiceResult(res, result, options = {}) {
  const httpStatus = aoResultHttpStatus(result);
  if (httpStatus != null) {
    const body = typeof options.errorBody === 'function'
      ? options.errorBody(result)
      : (options.errorBody ?? result);
    return res.status(httpStatus).json(body);
  }
  if (typeof options.successBody === 'function') {
    return res.json(options.successBody(result));
  }
  return res.json(result);
}

module.exports = {
  aoResultHttpStatus,
  sendAoServiceResult,
};
