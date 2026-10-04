'use strict';

const BASE58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

function isValidSolanaAddress(address) {
  if (typeof address !== 'string') return false;
  const trimmed = address.trim();
  if (trimmed.length < 32 || trimmed.length > 44) return false;
  for (const ch of trimmed) {
    if (!BASE58.includes(ch)) return false;
  }
  return true;
}

module.exports = {
  isValidSolanaAddress,
  BASE58,
};
