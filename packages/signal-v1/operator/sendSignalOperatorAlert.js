'use strict';

/**
 * Dispatch a durable operator alert through one or more transport adapters.
 * Telegram ingestion and research code must not import mail providers directly.
 */
async function sendSignalOperatorAlert(alert, options = {}) {
  const transports = options.transports || [];
  const enabled = transports.filter(t => (typeof t.enabled === 'function' ? t.enabled() : t.enabled !== false));
  if (!enabled.length) throw new Error('relay_disabled_or_not_ready');

  const sentAt = options.sentAt || new Date();
  const payload = {
    ...alert,
    transportSentAt: sentAt.toISOString(),
    latency: {
      ...(alert.latency || {}),
      telegramToAlertSentMs: alert.latency?.telegramToAlertSentMs
        ?? (alert.occurredAt ? sentAt.getTime() - Date.parse(alert.occurredAt) : null),
    },
  };

  const results = [];
  for (const transport of enabled) {
    try {
      const result = await transport.send(payload);
      results.push({ provider: transport.id || 'unknown', ...result });
    } catch (err) {
      if (enabled.length === 1) throw err;
      results.push({ provider: transport.id || 'unknown', error: String(err.message || err) });
    }
  }
  const hardFailure = results.every(r => r.error);
  if (hardFailure) throw new Error('relay_transport_unknown');
  return results;
}

module.exports = {
  sendSignalOperatorAlert,
};
