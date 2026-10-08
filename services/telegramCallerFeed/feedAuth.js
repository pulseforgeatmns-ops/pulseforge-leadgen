'use strict';

/**
 * Private caller-feed bearer auth. Used by HTTP routes and the dedicated
 * service entrypoint only — importing this module must never terminate the process.
 */
function feedAuthConfigured(env = process.env) {
  const token = env.SIGNAL_OPERATOR_FEED_TOKEN;
  return typeof token === 'string' && token.length >= 32;
}

function assertDedicatedServiceFeedAuth(env = process.env) {
  if (!feedAuthConfigured(env)) {
    return { ok: false, reason: 'feed_auth_not_configured' };
  }
  return { ok: true };
}

module.exports = {
  feedAuthConfigured,
  assertDedicatedServiceFeedAuth,
};
