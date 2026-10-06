'use strict';

const fs = require('fs');
const path = require('path');

const DEFAULT_STATE_PATH = path.join(process.cwd(), '.data', 'telegram-caller-feed-state.json');

function defaultState() {
  return {
    version: 1,
    channels: {},
    seenMessageKeys: {},
    messageSnapshots: {},
    latencyMs: [],
    stats: {
      messagesObserved: 0,
      callsEmitted: 0,
      caBearingMessages: 0,
    },
    lastTelegramUpdateAt: null,
    lastSuccessfulPollAt: null,
    lastError: null,
  };
}

function loadState(statePath = process.env.TELEGRAM_CALLER_FEED_STATE_PATH || DEFAULT_STATE_PATH) {
  try {
    if (!fs.existsSync(statePath)) {
      return { state: defaultState(), statePath };
    }
    const parsed = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    return { state: { ...defaultState(), ...parsed }, statePath };
  } catch {
    return { state: defaultState(), statePath };
  }
}

function saveState(state, statePath) {
  const dir = path.dirname(statePath);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = `${statePath}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(state, null, 2));
  fs.renameSync(tmp, statePath);
}

function messageKey(channelId, messageId) {
  return `${channelId}:${messageId}`;
}

function externalId(channelId, messageId) {
  return `telegram:${channelId}:${messageId}`;
}

function recordLatency(state, occurredAtIso, ingestedAtIso) {
  const ms = new Date(ingestedAtIso).getTime() - new Date(occurredAtIso).getTime();
  if (!Number.isFinite(ms) || ms < 0) return;
  state.latencyMs.push(ms);
  if (state.latencyMs.length > 500) {
    state.latencyMs = state.latencyMs.slice(-500);
  }
}

function percentile(values, p) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[idx];
}

function buildHealth(state, meta = {}) {
  return {
    connected: Boolean(meta.connected),
    credentialsConfigured: Boolean(meta.credentialsConfigured),
    lastTelegramUpdate: state.lastTelegramUpdateAt,
    lastSuccessfulPoll: state.lastSuccessfulPollAt,
    activeChannels: meta.activeChannels || [],
    sources: meta.sources || [],
    messagesObserved: state.stats.messagesObserved,
    callsEmitted: state.stats.callsEmitted,
    caBearingMessages: state.stats.caBearingMessages,
    errors: meta.errors || (state.lastError ? [state.lastError] : []),
    medianIngestionLatencyMs: percentile(state.latencyMs, 50),
    p95IngestionLatencyMs: percentile(state.latencyMs, 95),
  };
}

module.exports = {
  loadState,
  saveState,
  defaultState,
  messageKey,
  externalId,
  recordLatency,
  buildHealth,
  percentile,
};
