'use strict';

const { loadTelegramCredentials } = require('./credentials');
const { loadConfiguredSources } = require('./sources');
const {
  loadState,
  saveState,
  messageKey,
  recordLatency,
  buildHealth,
} = require('./stateStore');
const { toFeedCall } = require('./normalize');
const {operationFromEnv}=require('../signalOperator/operationGate');
const {
  createGramJsClient,
  resolvePublicChannel,
  getLatestMessageId,
  fetchNewMessages,
} = require('./telegramAdapter');

const SOLANA_CA_RE = /\b[1-9A-HJ-NP-Za-km-z]{32,44}\b/;

function createTelegramCallerFeedEngine(options = {}) {
  const now = options.now || (() => new Date());
  const pollIntervalMs = options.pollIntervalMs ?? Number(process.env.TELEGRAM_CALLER_FEED_POLL_MS || 15000);
  const { state, statePath } = loadState(options.statePath);
  let client = options.client || null;
  let clientPromise = null;
  let pollTimer = null;
  let runningPoll = false;
  const pilotGate=options.pilotGate || (process.env.SIGNAL_OPERATION_MODE ? operationFromEnv() : null);

  const channelRuntime = new Map();
  let bootstrapAt=0;
  let sourceStatuses = [];
  const maxRecentCalls = options.maxRecentCalls ?? Math.max(500,Math.min(10000,Number(process.env.TELEGRAM_CALLER_MAX_RETAINED_CALLS)||500));
  const recentCalls = (state.recentCalls || []).slice(-maxRecentCalls);
  state.recentCalls = recentCalls;

  function credentialsStatus() {
    return loadTelegramCredentials();
  }

  async function ensureClient() {
    if (client) return client;
    if (options.client) {
      client = options.client;
      return client;
    }
    if (clientPromise) return clientPromise;
    clientPromise = (async () => {
      const creds = credentialsStatus();
      if (!creds.ok) {
        throw new Error(creds.reason);
      }
      if (options.createClient) {
        client = await options.createClient(creds);
      } else {
        client = await createGramJsClient(creds);
      }
      return client;
    })();
    return clientPromise;
  }

  async function bootstrapChannels(activeClient) {
    if(channelRuntime.size&&+now()-bootstrapAt<3600000)return sourceStatuses;
    const sources = loadConfiguredSources();
    channelRuntime.clear();
    const results = [];
    for (const src of sources) {
      const entry = {
        sourceId: src.sourceId,
        displayName: src.displayName,
        username: src.username,
        platform: src.platform,
        role: src.sourceRole,
        clusterId: src.clusterId || null,
        relationshipStatus: src.clusterRelationshipStatus,
        relatedExternalHandles: Array.isArray(src.relatedExternalHandles)
          ? [...src.relatedExternalHandles]
          : [],
        collector: src.collector,
        active: false,
        available: false,
        channelId: null,
        reason: null,
        identityStatus:'UNVERIFIED',universeVersion:src.universeVersion||null,identityEvidence:src.identityEvidence||null,
      };
      try {
        const entity = await resolvePublicChannel(activeClient, src.username);
        if(src.universeVersion&&(!entity.broadcast||!entity.username||entity.username.toLowerCase()!==src.username.toLowerCase()||!new RegExp(src.titlePattern,'i').test(entity.title||'')))throw Error('public_channel_identity_not_verified');
        entry.channelId = String(entity.id);
        if (src.expectedChannelId && entry.channelId !== String(src.expectedChannelId)) {
          throw new Error('configured_channel_identity_mismatch');
        }
        entry.available = true;
        entry.active = true;
        // Pin authenticated numeric identities durably. Renamed/reassigned
        // usernames cannot silently switch the monitored publisher.
        state.verifiedSources=state.verifiedSources||{};
        const pinned=state.verifiedSources[src.sourceId];
        if(pinned&&pinned.channelId!==entry.channelId)throw Error('pinned_channel_identity_mismatch');
        await getLatestMessageId(activeClient,entity); // A resolve alone is not evidence of read access.
        entry.identityStatus='VERIFIED';
        entry.verifiedAt=pinned?.verifiedAt||now().toISOString();
        entry.verifiedTitle=entity.title||null;
        state.verifiedSources[src.sourceId]={channelId:entry.channelId,username:src.username,verifiedAt:entry.verifiedAt,title:entry.verifiedTitle};
        const cursorKey = entry.channelId;
        if (state.channels[cursorKey] == null) {
          const maxId = await getLatestMessageId(activeClient, entity);
          state.channels[cursorKey] = { lastMessageId: maxId, sourceId: src.sourceId, username: src.username };
        } else if (!state.channels[cursorKey].sourceId) {
          state.channels[cursorKey].sourceId = src.sourceId;
        }
        channelRuntime.set(entry.channelId, { entity, source: src, entry });
      } catch (err) {
        entry.reason = String(err.message || err);
        entry.available = false;
        entry.active = false;
      }
      results.push(entry);
    }
    sourceStatuses = results;
    bootstrapAt=+now();
    saveState(state, statePath);
    return results;
  }

  function ingestMessage(msg, sourceMeta) {
    const ingestedAt = now();
    const key = messageKey(msg.channelId, msg.messageId);
    const prevText = state.messageSnapshots[key];
    const isEdit = prevText != null && prevText !== msg.text && msg.editDate;
    if (state.seenMessageKeys[key] && !isEdit) {
      return null;
    }

    if (isEdit) {
      msg.edit = {
        editAt: msg.editDate.toISOString(),
        priorText: prevText,
        originalMessageId: msg.messageId,
      };
    } else {
      state.seenMessageKeys[key] = ingestedAt.toISOString();
    }
    state.messageSnapshots[key] = msg.text;

    const cursor = state.channels[msg.channelId];
    if (cursor && msg.messageId > (cursor.lastMessageId || 0)) {
      cursor.lastMessageId = msg.messageId;
    }

    state.stats.messagesObserved += 1;
    state.lastTelegramUpdateAt = ingestedAt.toISOString();

    const call = toFeedCall({
      ...msg,
      clusterId:sourceMeta.clusterId||null,sourceRole:sourceMeta.sourceRole||'UNKNOWN',
      clusterRelationshipStatus:sourceMeta.clusterRelationshipStatus||'UNKNOWN',
      universeVersion:sourceMeta.universeVersion||null,
      sourceId: sourceMeta.sourceId,
      ingestedAt,
    });
    if (!call) return null;

    state.stats.callsEmitted += 1;
    state.sourceStats=state.sourceStats||{};
    const counts=state.sourceStats[sourceMeta.sourceId]||(state.sourceStats[sourceMeta.sourceId]={messages:0,caBearing:0});counts.messages++;
    const addresses=require('../../packages/signal-v1/prospective/caExtraction').extractSolanaContractAddresses(call.text||'');
    if(addresses.length)counts.caBearing++;
    state.dailyCalls=state.dailyCalls||[];
    state.dailyCalls.push({sourceId:call.sourceId,at:call.ingestedAt,addresses});
    state.dailyCalls=state.dailyCalls.filter(c=>+ingestedAt-Date.parse(c.at)<86400000).slice(-50000);
    if (addresses.length) {
      state.stats.caBearingMessages += 1;
    }
    recordLatency(state, call.occurredAt, call.ingestedAt);
    recentCalls.push(call);
    if (recentCalls.length > maxRecentCalls) recentCalls.shift();
    return call;
  }

  async function pollOnce() {
    if (runningPoll) return { skipped: true };
    runningPoll = true;
    const emitted = [];
    const errors = [];
    try {
      if(pilotGate && !pilotGate().ok) {
        sourceStatuses=sourceStatuses.map(s=>({...s,active:false,available:false,reason:'pilot_not_ready'}));
        if(client) {try{await client.disconnect();}catch{} client=null;clientPromise=null;}
        return {emitted,errors:['pilot_not_ready'],connected:false,sources:sourceStatuses};
      }
      const creds = credentialsStatus();
      if (!creds.ok) {
        state.lastError = creds.reason;
        saveState(state, statePath);
        return { emitted, errors: [creds.reason], connected: false };
      }
      if (!loadConfiguredSources().length) {
        sourceStatuses = [];
        return { emitted, errors: ['no_configured_sources'], connected: false, sources: [] };
      }
      const activeClient = await ensureClient();
      const sources = await bootstrapChannels(activeClient);
      for (const src of sources) {
        if (!src.available || !src.channelId) continue;
        const runtime = channelRuntime.get(src.channelId);
        if (!runtime) continue;
        const afterId = state.channels[src.channelId]?.lastMessageId || 0;
        try {
          const messages = await fetchNewMessages(activeClient, runtime.entity, afterId);
          src.available=true;src.active=true;src.reason=null;src.lastSuccessfulReadAt=now().toISOString();
          for (const msg of messages) {
            if(pilotGate && !pilotGate().ok)break;
            if (msg.messageId < afterId) continue;
            if (msg.messageId === afterId && !msg.editDate) continue;
            const call = ingestMessage(msg, runtime.source);
            if (call) emitted.push(call);
          }
        } catch (err) {
          src.available = false;
          src.active = false;
          src.reason = String(err.message || err);
          const msg = `${src.sourceId}: ${err.message || err}`;
          errors.push(msg);
          state.lastError = msg;
        }
      }
      state.lastSuccessfulPollAt = now().toISOString();
      if (!errors.length) state.lastError = null;
      saveState(state, statePath);
      return { emitted, errors, connected: sources.some(src => src.available && src.active), sources };
    } catch (err) {
      const msg = String(err.message || err);
      errors.push(msg);
      state.lastError = msg;
      saveState(state, statePath);
      return { emitted, errors, connected: false };
    } finally {
      runningPoll = false;
    }
  }

  function getHealth(meta = {}) {
    const creds = credentialsStatus();
    const sources = sourceStatuses.map(source => ({ ...source }));
    return buildHealth(state, {
      connected: Boolean(meta.connected),
      credentialsConfigured: creds.ok,
      activeChannels: sources.filter(s => s.active).map(s => s.sourceId),
      sources,
      errors: meta.errors,
      expansionHealth:{windowHours:24,callsPerDay:(state.dailyCalls||[]).filter(c=>+now()-Date.parse(c.at)<86400000&&c.addresses.length).length,
        uniqueTokensPerDay:new Set((state.dailyCalls||[]).filter(c=>+now()-Date.parse(c.at)<86400000).flatMap(c=>c.addresses)).size,
        caParseSuccess:state.stats.messagesObserved?state.stats.caBearingMessages/state.stats.messagesObserved:null,
        caParseSuccessBasis:'CA-bearing messages / all observed text messages; non-CA conversation included',
        clusterUnknownRate:sources.length?sources.filter(s=>s.relationshipStatus==='UNKNOWN').length/sources.length:null,
        unavailableSources:sources.filter(s=>!s.active).length,activeSources:sources.filter(s=>s.active).length,
        sourceStats:state.sourceStats||{},retainedCallLimit:maxRecentCalls},
    });
  }

  function listCallsSince(sinceIso) {
    if (!sinceIso) return [...recentCalls];
    const sinceMs = new Date(sinceIso).getTime();
    return recentCalls.filter(c => new Date(c.ingestedAt).getTime() >= sinceMs);
  }

  function startPolling() {
    if (pollTimer) return;
    pollTimer = setInterval(() => {
      pollOnce().catch(err => {
        state.lastError = String(err.message || err);
        saveState(state, statePath);
      });
    }, pollIntervalMs);
    if (pollTimer.unref) pollTimer.unref();
  }

  function stopPolling() {
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = null;
  }

  return {
    credentialsStatus,
    ensureClient,
    bootstrapChannels,
    pollOnce,
    getHealth,
    listCallsSince,
    startPolling,
    stopPolling,
    getState: () => state,
    getRecentCalls: () => [...recentCalls],
  };
}

module.exports = {
  createTelegramCallerFeedEngine,
  SOLANA_CA_RE,
};
