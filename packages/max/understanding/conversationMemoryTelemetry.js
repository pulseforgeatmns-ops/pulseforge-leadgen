'use strict';

function emptyConversationMemoryTelemetry() {
  return {
    conversation_memory_load_count: 0,
    conversation_memory_hit_count: 0,
    conversation_memory_miss_count: 0,
    conversation_memory_write_count: 0,
    conversation_memory_expired_count: 0,
    conversation_memory_superseded_count: 0,
    conversation_memory_reference_resolved_count: 0,
    conversation_memory_reference_blocked_count: 0,
    conversation_memory_load_failed: false,
  };
}

function mergeConversationMemoryTelemetry(target, patch) {
  if (!target || !patch) return target;
  for (const [key, value] of Object.entries(patch)) {
    if (key === 'conversation_memory_load_failed') {
      target[key] = Boolean(target[key] || value);
      continue;
    }
    if (typeof value === 'number') {
      target[key] = (target[key] || 0) + value;
    }
  }
  return target;
}

module.exports = {
  emptyConversationMemoryTelemetry,
  mergeConversationMemoryTelemetry,
};
