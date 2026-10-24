'use strict';

const { ConversationMemory } = require('./conversationMemory');
const { interpretConversationalInput } = require('./interpreter');
const { persistConversationMemoryTurn } = require('./conversationMemoryPersistence');
const {
  emptyConversationMemoryTelemetry,
  mergeConversationMemoryTelemetry,
} = require('./conversationMemoryTelemetry');
const { mergeUnderstandingTelemetry } = require('./telemetry');

async function loadConversationMemory({ repository, tenantId, conversationId, actorId, now }) {
  const { records, expiredCount } = await repository.loadActive({
    tenantId,
    conversationId,
    actorId,
    now,
  });
  const memory = ConversationMemory.fromDurableRecords({
    conversationId,
    records,
  });
  return { memory, records, expiredCount };
}

async function interpretWithDurableConversationContext(input = {}) {
  const memoryTelemetry = emptyConversationMemoryTelemetry();
  const tenantId = input.tenantId ?? input.clientId;
  const conversationId = input.conversationId;
  const actorId = input.actor?.userId ?? input.actorId ?? null;
  let memory = input.memory instanceof ConversationMemory ? input.memory : null;
  let durableRecords = [];

  const shouldUseDurable = Boolean(input.memoryRepository && conversationId && tenantId != null);

  if (shouldUseDurable) {
    memoryTelemetry.conversation_memory_load_count = 1;
    try {
      const loaded = await loadConversationMemory({
        repository: input.memoryRepository,
        tenantId,
        conversationId,
        actorId,
        now: input.now ? new Date(input.now) : new Date(),
      });
      durableRecords = loaded.records;
      memory = loaded.memory;
      memoryTelemetry.conversation_memory_expired_count = loaded.expiredCount || 0;
      if (loaded.records.length) memoryTelemetry.conversation_memory_hit_count = 1;
      else memoryTelemetry.conversation_memory_miss_count = 1;
    } catch (err) {
      memoryTelemetry.conversation_memory_load_failed = true;
      memory = new ConversationMemory({ conversationId, durableLoadFailed: true });
    }
  }

  const interpreted = interpretConversationalInput({
    ...input,
    memory: memory || input.memory,
    conversationId,
  });

  if (interpreted.memory?.durableReferenceResolved) {
    memoryTelemetry.conversation_memory_reference_resolved_count += 1;
  }
  if (interpreted.validation?.blockCommit && (interpreted.situationModel?.ambiguities?.length
    || interpreted.situationModel?.threads?.some(t => t.ambiguities?.length))) {
    memoryTelemetry.conversation_memory_reference_blocked_count += 1;
  }

  if (shouldUseDurable && !memoryTelemetry.conversation_memory_load_failed) {
    const persist = await persistConversationMemoryTurn({
      repository: input.memoryRepository,
      tenantId,
      conversationId,
      actorId,
      actorRole: input.actor?.role || null,
      situationModel: interpreted.situationModel,
      validation: interpreted.validation,
      sourceInputId: interpreted.situationModel?.inputId || input.inputId,
      now: input.now ? new Date(input.now) : new Date(),
    });
    memoryTelemetry.conversation_memory_write_count = persist.written || persist.records?.length || 0;
    memoryTelemetry.conversation_memory_superseded_count = persist.superseded || 0;
  }

  interpreted.conversationMemoryTelemetry = memoryTelemetry;
  if (input.telemetry && memoryTelemetry) {
    mergeConversationMemoryTelemetry(input.telemetry, memoryTelemetry);
  }
  if (interpreted.understandingTelemetry && input.mergeTelemetry !== false) {
    mergeUnderstandingTelemetry(input.telemetry || {}, interpreted.understandingTelemetry);
  }

  return interpreted;
}

module.exports = {
  loadConversationMemory,
  interpretWithDurableConversationContext,
};
