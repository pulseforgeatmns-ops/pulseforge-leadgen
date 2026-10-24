'use strict';

function formatUsd(value) {
  return value == null || !Number.isFinite(Number(value)) ? 'UNAVAILABLE' : String(value);
}

function formatSeconds(ms) {
  if (ms == null || !Number.isFinite(ms)) return 'UNAVAILABLE';
  return `${(ms / 1000).toFixed(2)} sec`;
}

function formatOperatorAlertText(alert) {
  if (alert.kind === 'OPERATIONAL_TRANSPORT_TEST') {
    return alert.textContent || `SIGNAL_OPERATIONAL_TRANSPORT_TEST_V1 ${alert.id}\nOperational delivery test only.`;
  }

  const lat = alert.latency || {};
  const market = alert.market || {};
  const lines = [
    'FRONT RUNNERS — NEW CALL',
    '',
    'CA:',
    alert.tokenAddress,
    '',
    'Telegram post time:',
    alert.occurredAt || 'UNAVAILABLE',
    '',
    'Signal learned:',
    alert.knowledgeAt || 'UNAVAILABLE',
    '',
    'Ingestion latency:',
    formatSeconds(lat.telegramToCallerServiceMs),
    '',
    'Current market snapshot:',
    `- price: ${formatUsd(market.priceUsd)}`,
    `- market cap: ${formatUsd(market.marketCapUsd)}`,
    `- liquidity: ${formatUsd(market.liquidityUsd)}`,
    `- token age: ${market.tokenAgeSeconds != null ? `${market.tokenAgeSeconds}s` : 'UNAVAILABLE'}`,
    `- short-term volume: ${formatUsd(market.volumeIntervalUsd)}`,
    '',
    'Signal research state:',
    alert.researchState || 'PENDING_RESEARCH',
    '',
    'Independent convergence:',
    alert.independentConvergence || 'none',
    '',
    'Known risk information:',
    ...(Array.isArray(alert.risks) && alert.risks.length ? alert.risks.map(r => `- ${r}`) : ['- none beyond defaults']),
    '',
    'Label: EXPERIMENTAL / MANUAL DECISION',
    'No automatic trading was executed.',
  ];

  if (alert.tradeDestination) {
    lines.push('', 'Trade destination:', alert.tradeDestination);
  } else {
    lines.push('', 'Copy the CA above for manual inspection. FOMO deeplink unverified.');
  }

  if (lat.telegramToAlertSentMs != null) {
    lines.push('', 'Telegram → alert transport:', formatSeconds(lat.telegramToAlertSentMs));
  }

  return lines.join('\n');
}

module.exports = {
  formatOperatorAlertText,
};
