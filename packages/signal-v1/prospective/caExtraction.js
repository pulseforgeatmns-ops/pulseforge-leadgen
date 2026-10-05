'use strict';

const { createHash } = require('crypto');
const { isValidSolanaAddress } = require('../acquisition/solanaAddress');

const CA_PATTERN = /\b([1-9A-HJ-NP-Za-km-z]{32,44})\b/g;

/**
 * Extract verified Solana contract addresses from caller text. Does not infer from tickers.
 *
 * @param {string} text
 * @returns {string[]} canonical unique addresses
 */
function extractSolanaContractAddresses(text) {
  if (!text || typeof text !== 'string') return [];
  const seen = new Set();
  const out = [];
  let match;
  while ((match = CA_PATTERN.exec(text)) !== null) {
    const candidate = match[1].trim();
    if (!isValidSolanaAddress(candidate)) continue;
    const canonical = canonicalizeSolanaAddress(candidate);
    if (seen.has(canonical)) continue;
    seen.add(canonical);
    out.push(canonical);
  }
  return out;
}

function canonicalizeSolanaAddress(address) {
  return String(address).trim();
}

function deterministicCallEventId(sourceId, externalMessageId, tokenAddress) {
  return createHash('sha256')
    .update(`signal-call:${sourceId}:${externalMessageId}:${tokenAddress}`)
    .digest('hex')
    .slice(0, 32);
}

function deterministicEvidenceId(sourceId, externalMessageId, tokenAddress) {
  return createHash('sha256')
    .update(`signal-raw-evidence:${sourceId}:${externalMessageId}:${tokenAddress}`)
    .digest('hex')
    .slice(0, 32);
}

module.exports = {
  extractSolanaContractAddresses,
  canonicalizeSolanaAddress,
  deterministicCallEventId,
  deterministicEvidenceId,
};
