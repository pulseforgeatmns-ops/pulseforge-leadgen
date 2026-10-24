'use strict';

/**
 * Decision clock for prospective research — Signal cannot know before ingestion completes.
 *
 * @param {Date|string} occurredAt — original provider/source timestamp
 * @param {Date|string} ingestedAt — when Signal persisted the observation
 */
function knowledgeAt(occurredAt, ingestedAt) {
  const occurredMs = new Date(occurredAt).getTime();
  const ingestedMs = new Date(ingestedAt).getTime();
  return new Date(Math.max(occurredMs, ingestedMs));
}

function ingestionLatencyMs(occurredAt, ingestedAt) {
  return new Date(ingestedAt).getTime() - new Date(occurredAt).getTime();
}

module.exports = {
  knowledgeAt,
  ingestionLatencyMs,
};
