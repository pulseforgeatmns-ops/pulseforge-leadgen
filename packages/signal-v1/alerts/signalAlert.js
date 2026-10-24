'use strict';

const { randomUUID } = require('crypto');

/**
 * @param {object} alert
 */
function createSignalAlert(alert) {
  return {
    id: alert.id || randomUUID(),
    tokenAddress: alert.tokenAddress,
    state: alert.state,
    previousState: alert.previousState ?? null,
    score: alert.score,
    reasons: alert.reasons || [],
    risks: alert.risks || [],
    createdAt: alert.createdAt || new Date(),
  };
}

module.exports = {
  createSignalAlert,
};
