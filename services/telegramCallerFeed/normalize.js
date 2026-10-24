'use strict';

const { externalId } = require('./stateStore');

/**
 * @param {object} msg — normalized internal message
 * @returns {object|null}
 */
function toFeedCall(msg) {
  if (!msg?.sourceId || msg.channelId == null || msg.messageId == null) return null;
  const occurredAt = msg.occurredAt instanceof Date ? msg.occurredAt.toISOString() : msg.occurredAt;
  const ingestedAt = msg.ingestedAt instanceof Date ? msg.ingestedAt.toISOString() : msg.ingestedAt;
  const row = {
    sourceId: msg.sourceId,
    externalId: externalId(msg.channelId, msg.messageId),
    externalMessageId: externalId(msg.channelId, msg.messageId),
    communityId: String(msg.channelId),
    occurredAt,
    messageTimestamp: occurredAt,
    ingestedAt,
    text: msg.text || '',
    rawText: msg.text || '',
    url: msg.url || null,
    rawReferenceUrl: msg.url || null,
    forwardedFrom: msg.forwardedFrom || null,
    forwarding: msg.forwarding || null,
    provenance: {
      dataClass: 'EMPIRICAL',
      collectorId: 'telegram-caller-feed',
      telegramChannelId: String(msg.channelId),
      telegramMessageId: String(msg.messageId),
      ...(msg.edit ? { messageEditAt: msg.edit.editAt, priorText: msg.edit.priorText } : {}),
      ...(msg.provenance || {}),
    },
  };
  return row;
}

function buildFeedPayload(calls, health) {
  return {
    calls,
    observations: calls,
    health,
    generatedAt: new Date().toISOString(),
  };
}

module.exports = {
  toFeedCall,
  buildFeedPayload,
};
