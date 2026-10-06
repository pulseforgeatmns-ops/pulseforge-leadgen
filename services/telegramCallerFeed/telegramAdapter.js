'use strict';

/**
 * Thin MTProto adapter — injectable for tests.
 */

async function createGramJsClient(credentials) {
  const { TelegramClient } = require('telegram');
  const { StringSession } = require('telegram/sessions');
  const session = new StringSession(credentials.sessionString);
  const client = new TelegramClient(session, credentials.apiId, credentials.apiHash, {
    connectionRetries: 3,
  });
  await client.connect();
  if (!(await client.checkAuthorization())) {
    throw new Error('Telegram session is not authorized');
  }
  return client;
}

/**
 * Resolve public channel entity and attempt read-only access.
 *
 * @param {object} client
 * @param {string} username — without @
 */
async function resolvePublicChannel(client, username) {
  const entity = await client.getEntity(username);
  return entity;
}

/**
 * Fetch messages with id strictly greater than afterMessageId (no history backfill).
 */
async function getLatestMessageId(client, entity) {
  const batch = await client.getMessages(entity, { limit: 1 });
  return batch?.[0]?.id || 0;
}

async function fetchNewMessages(client, entity, afterMessageId) {
  const minId = afterMessageId || 0;
  const batch = await client.getMessages(entity, { minId, limit: 100 });
  const rows = (batch || [])
    .map(m => mapGramMessage(m, entity))
    .filter(Boolean)
    .sort((a, b) => a.messageId - b.messageId);
  return rows;
}

function mapGramMessage(message, entity) {
  if (!message || message.id == null) return null;
  const channelId = entity.id != null ? String(entity.id) : String(message.peerId?.channelId || message.chatId);
  const text = message.message || message.text || '';
  const occurredAt = message.date ? new Date(message.date * 1000) : new Date();
  const username = entity.username || null;
  const url = username ? `https://t.me/${username}/${message.id}` : null;

  let forwarding = null;
  let forwardedFrom = null;
  const fwd = message.fwdFrom;
  if (fwd) {
    forwardedFrom = fwd.fromName || fwd.channelPost != null ? String(fwd.channelPost) : null;
    forwarding = {
      fromName: fwd.fromName || null,
      fromId: fwd.fromId ? String(fwd.fromId) : null,
      channelPost: fwd.channelPost != null ? String(fwd.channelPost) : null,
      date: fwd.date ? new Date(fwd.date * 1000).toISOString() : null,
    };
  }

  return {
    channelId,
    messageId: message.id,
    occurredAt,
    text,
    url,
    forwardedFrom,
    forwarding,
    editDate: message.editDate ? new Date(message.editDate * 1000) : null,
  };
}

module.exports = {
  createGramJsClient,
  resolvePublicChannel,
  getLatestMessageId,
  fetchNewMessages,
  mapGramMessage,
};
