'use strict';

const DEFAULT_LEASE_MS = 5 * 60 * 1000;
const TRANSACTION_TIMEOUT_MS = 30 * 1000;

function validateLeaseMs(leaseMs) {
  if (!Number.isSafeInteger(leaseMs) || leaseMs <= 0 || leaseMs > 24 * 60 * 60 * 1000) {
    throw new TypeError('Attention lease must be a positive integer of at most one day');
  }
  return leaseMs;
}

function requireClaimToken(claimToken) {
  if (typeof claimToken !== 'string' || !claimToken.trim()) {
    throw new TypeError('Attention claim token is required');
  }
}

function claimLost() {
  return Object.assign(new Error('Attention claim expired or belongs to another worker'), {
    code: 'ATTENTION_CLAIM_LOST',
  });
}

module.exports = { DEFAULT_LEASE_MS, TRANSACTION_TIMEOUT_MS, validateLeaseMs, requireClaimToken, claimLost };
