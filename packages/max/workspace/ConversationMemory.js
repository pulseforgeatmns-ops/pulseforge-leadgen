'use strict';

/**
 * SPEC-147 — Conversation memory for mission discussions.
 * Tracks what Max has already explained so follow-up turns avoid repetition.
 */

const MAX_TOPICS = 24;

function createConversationMemory() {
  return {
    explainedTopics: [],
    summaries: {},
    turnCount: 0,
    recentIntents: [],
    relationshipMode: 'understanding',
  };
}

function ensureConversationMemory(session) {
  if (!session) return createConversationMemory();
  if (!session.conversationMemory || typeof session.conversationMemory !== 'object') {
    session.conversationMemory = createConversationMemory();
  }
  return session.conversationMemory;
}

function deriveTopicKey(input = {}) {
  const intent = input.intent || 'general';
  const property =
    (input.inspection && input.inspection.property) ||
    input.inspectionProperty ||
    null;
  const stage = input.stage || 'unknown';
  const specialist = input.specialist || null;
  if (property) return `${intent}:${property}:${stage}`;
  if (specialist) return `${intent}:specialist:${specialist}:${stage}`;
  return `${intent}:${stage}`;
}

function hasExplained(memory, topicKey) {
  if (!memory || !topicKey) return false;
  return Array.isArray(memory.explainedTopics) && memory.explainedTopics.includes(topicKey);
}

function recordExplanation(session, input = {}) {
  const memory = ensureConversationMemory(session);
  const topicKey = input.topicKey || deriveTopicKey(input);
  if (!memory.explainedTopics.includes(topicKey)) {
    memory.explainedTopics.push(topicKey);
    if (memory.explainedTopics.length > MAX_TOPICS) {
      memory.explainedTopics = memory.explainedTopics.slice(-MAX_TOPICS);
    }
  }
  if (input.summary) {
    memory.summaries[topicKey] = String(input.summary).slice(0, 280);
  }
  memory.turnCount += 1;
  if (input.intent) {
    memory.recentIntents.push(String(input.intent));
    if (memory.recentIntents.length > 12) {
      memory.recentIntents = memory.recentIntents.slice(-12);
    }
  }
  if (input.relationshipMode) {
    memory.relationshipMode = input.relationshipMode;
  }
  return memory;
}

function inferRelationshipMode(conversationIntent, memory) {
  const intent = conversationIntent && conversationIntent.intent;
  if (!intent) return memory && memory.relationshipMode ? memory.relationshipMode : 'understanding';
  if (intent === 'execute' || intent === 'edit') return 'executing';
  if (intent === 'teach' || intent === 'explain' || intent === 'challenge' || intent === 'strategy') {
    return 'understanding';
  }
  if (intent === 'brainstorm' || intent === 'compare') return 'exploring';
  return memory && memory.relationshipMode ? memory.relationshipMode : 'understanding';
}

function repetitionPreface(memory, topicKey) {
  if (!hasExplained(memory, topicKey)) return '';
  const prior = memory.summaries && memory.summaries[topicKey];
  if (prior) {
    return `As I mentioned — ${prior} `;
  }
  return 'Building on what we already covered — ';
}

module.exports = {
  createConversationMemory,
  ensureConversationMemory,
  deriveTopicKey,
  hasExplained,
  recordExplanation,
  inferRelationshipMode,
  repetitionPreface,
};
