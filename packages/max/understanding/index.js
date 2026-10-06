'use strict';

const types = require('./types');
const { interpretConversationalInput, isTrustedStructuredInput, conversationalText } = require('./interpreter');
const { validateSituationModel } = require('./ambiguityGate');
const { formatUnderstandingPreview } = require('./preview');
const { ConversationMemory } = require('./conversationMemory');
const { buildUnderstandingDiagnostics } = require('./diagnostics');
const { recordUnderstandingTelemetry, mergeUnderstandingTelemetry } = require('./telemetry');
const { deriveRecommendedNextActions } = require('./recommendations');
const { interpretWithDurableConversationContext, loadConversationMemory } = require('./durableConversationContext');
const {
  emptyConversationMemoryTelemetry,
  mergeConversationMemoryTelemetry,
} = require('./conversationMemoryTelemetry');
const {
  MemoryConversationMemoryRepository,
  PostgresConversationMemoryRepository,
} = require('./conversationMemoryRepository');
const { persistConversationMemoryTurn, buildMemoryRecordsFromTurn } = require('./conversationMemoryPersistence');
const { SEMANTIC_TYPE } = require('./conversationMemoryTypes');

module.exports = {
  ...types,
  interpretConversationalInput,
  interpretWithDurableConversationContext,
  loadConversationMemory,
  isTrustedStructuredInput,
  conversationalText,
  validateSituationModel,
  formatUnderstandingPreview,
  ConversationMemory,
  buildUnderstandingDiagnostics,
  recordUnderstandingTelemetry,
  mergeUnderstandingTelemetry,
  deriveRecommendedNextActions,
  emptyConversationMemoryTelemetry,
  mergeConversationMemoryTelemetry,
  MemoryConversationMemoryRepository,
  PostgresConversationMemoryRepository,
  persistConversationMemoryTurn,
  buildMemoryRecordsFromTurn,
  SEMANTIC_TYPE,
};
