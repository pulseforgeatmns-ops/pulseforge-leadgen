'use strict';

const { isValidSolanaAddress } = require('../acquisition/solanaAddress');

const FOMO_ORIGIN = 'https://fomo.family';
const FOMO_SOLANA_CHAIN = 'solana';

/**
 * Build FOMO's canonical public token URL for a Signal V1 Solana token.
 * Returns null when identity is missing or ambiguous so alert delivery fails closed.
 */
function buildFomoTokenUrl(tokenAddress) {
  if (typeof tokenAddress !== 'string') return null;
  const normalizedAddress = tokenAddress.trim();
  if (!isValidSolanaAddress(normalizedAddress)) return null;
  return `${FOMO_ORIGIN}/tokens/${FOMO_SOLANA_CHAIN}/${normalizedAddress}`;
}

module.exports = {
  FOMO_ORIGIN,
  FOMO_SOLANA_CHAIN,
  buildFomoTokenUrl,
};
