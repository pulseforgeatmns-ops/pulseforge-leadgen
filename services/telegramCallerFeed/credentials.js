'use strict';

/**
 * Telegram MTProto credentials — fail closed when incomplete.
 * Never log secret values.
 */
function loadTelegramCredentials() {
  const apiId = Number(process.env.TELEGRAM_API_ID);
  const apiHash = process.env.TELEGRAM_API_HASH;
  const sessionString = process.env.TELEGRAM_SESSION_STRING;

  const missing = [];
  if (!apiId || Number.isNaN(apiId)) missing.push('TELEGRAM_API_ID');
  if (!apiHash) missing.push('TELEGRAM_API_HASH');
  if (!sessionString) missing.push('TELEGRAM_SESSION_STRING');

  if (missing.length) {
    return {
      ok: false,
      missing,
      reason: `Telegram MTProto credentials absent (${missing.join(', ')})`,
    };
  }

  return {
    ok: true,
    apiId,
    apiHash,
    sessionString,
  };
}

module.exports = {
  loadTelegramCredentials,
};
