'use strict';

/**
 * MAX-P0-003 — correlation ids. A caller-supplied X-Correlation-Id is accepted
 * only when it matches a conservative pattern (no free text, no PII); otherwise
 * a fresh id is minted. The id is echoed on the response.
 */

const { newCorrelationId } = require('../missionLease');

const CORRELATION_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$/;

function correlationIdFrom(req) {
  const offered = req && req.get ? req.get('x-correlation-id') : null;
  return offered && CORRELATION_PATTERN.test(offered) ? offered : newCorrelationId();
}

function correlationMiddleware(req, res, next) {
  req.correlationId = correlationIdFrom(req);
  res.setHeader('X-Correlation-Id', req.correlationId);
  next();
}

module.exports = { CORRELATION_PATTERN, correlationIdFrom, correlationMiddleware, newCorrelationId };
