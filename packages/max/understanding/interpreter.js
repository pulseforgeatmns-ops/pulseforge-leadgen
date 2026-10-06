'use strict';

const { createSituationModel, newSituationId } = require('./types');
const { segmentIntoThreads, mergeThreadInterpretations } = require('./threadSegmentation');
const { dedupeCorrections } = require('./temporal');
const { interpretThreadSegment } = require('./semanticInterpreter');
const { ConversationMemory } = require('./conversationMemory');
const { validateSituationModel } = require('./ambiguityGate');
const { formatUnderstandingPreview } = require('./preview');
const { buildUnderstandingDiagnostics } = require('./diagnostics');
const { recordUnderstandingTelemetry } = require('./telemetry');
const { deriveRecommendedNextActions } = require('./recommendations');

function isTrustedStructuredInput(input = {}) {
  if (Array.isArray(input.claims) && input.claims.length) return true;
  if (input.structured && Array.isArray(input.structured.claims) && input.structured.claims.length) {
    return true;
  }
  return false;
}

function conversationalText(input = {}) {
  if (typeof input.text === 'string' && input.text.trim()) return input.text.trim();
  if (typeof input.message === 'string' && input.message.trim()) return input.message.trim();
  return null;
}

function interpretConversationalInput(input = {}) {
  const text = conversationalText(input);
  if (!text) {
    return {
      situationModel: null,
      skipped: true,
      reason: 'no_conversational_text',
    };
  }

  const inputId = input.inputId || newSituationId('in');
  const now = input.now ? new Date(input.now) : new Date();
  const memory = input.memory instanceof ConversationMemory
    ? input.memory
    : ConversationMemory.fromSeed(input.conversationMemory || {});

  const contextAccounts = [
    ...(input.contextAccounts || []),
    ...(memory.knownAccountNames?.() || []),
  ];
  let segments = segmentIntoThreads(text);
  if (/actually,?\s+that was\s+\w+\s+at/i.test(text)) {
    segments = [{ text, accountHint: null }];
  }
  let threads = segments.map((seg, idx) =>
    interpretThreadSegment({
      text: seg.text,
      threadId: `thread_${idx + 1}`,
      inputId,
      memory,
      now,
      accountHint: seg.accountHint,
      contextAccounts,
    })
  );
  threads = mergeThreadInterpretations(threads);
  for (const t of threads) {
    t.corrections = dedupeCorrections(t.corrections || []);
  }

  const flatCorrections = dedupeCorrections(threads.flatMap(t => t.corrections || []));

  const situationModel = createSituationModel({
    inputId,
    conversationId: input.conversationId || memory.conversationId,
    actor: input.actor || {},
    occurredAt: input.occurredAt || null,
    interpretedAt: now.toISOString(),
    rawText: text,
    threads,
    entities: threads.flatMap(t => t.entities),
    events: threads.flatMap(t => t.events),
    claims: threads.flatMap(t => t.claims),
    corrections: flatCorrections,
    commitments: threads.flatMap(t => t.commitments),
    painPoints: threads.flatMap(t => t.painPoints),
    objections: threads.flatMap(t => t.objections),
    decisionMakerSignals: threads.flatMap(t => t.decisionMakerSignals),
    temporalReferences: threads.flatMap(t => t.temporalReferences),
    questions: threads.flatMap(t => t.questions),
    requestedActions: threads.flatMap(t => t.requestedActions),
    commentary: threads.flatMap(t => t.commentary),
    ambiguities: threads.flatMap(t => t.ambiguities),
    confidence: scoreConfidence(threads),
  });

  const validation = validateSituationModel(situationModel);
  situationModel.validation = validation;
  situationModel.recommendedNextActions = deriveRecommendedNextActions(situationModel);
  situationModel.diagnostics = buildUnderstandingDiagnostics(situationModel);
  situationModel.preview = formatUnderstandingPreview(situationModel);
  const understandingTelemetry = recordUnderstandingTelemetry(situationModel, validation, {
    input_type: isTrustedStructuredInput(input) ? 'structured' : 'conversational',
    actor_role: input.actor?.role || null,
  });

  memory.recordTurn({ inputId, text, situationModel });

  return {
    situationModel,
    memory,
    validation,
    preview: situationModel.preview,
    diagnostics: situationModel.diagnostics,
    understandingTelemetry,
    skipped: false,
  };
}

function scoreConfidence(threads) {
  if (!threads.length) {
    return { overall: 0, entityResolution: 0, semanticInterpretation: 0 };
  }
  let entityScore = 0;
  let semanticScore = 0;
  for (const t of threads) {
    entityScore += t.accountName ? 0.85 : 0.35;
    semanticScore += (t.events?.length || 0) > 0 ? 0.2 : 0;
    semanticScore += (t.painPoints?.length || 0) > 0 ? 0.15 : 0;
    semanticScore += (t.commitments?.length || 0) > 0 ? 0.15 : 0;
    if ((t.ambiguities || []).length) entityScore -= 0.4;
  }
  entityScore /= threads.length;
  semanticScore = Math.min(1, semanticScore / threads.length + 0.5);
  const overall = Math.max(0, Math.min(1, (entityScore + semanticScore) / 2));
  return {
    overall: Number(overall.toFixed(2)),
    entityResolution: Number(Math.max(0, Math.min(1, entityScore)).toFixed(2)),
    semanticInterpretation: Number(Math.max(0, Math.min(1, semanticScore)).toFixed(2)),
  };
}

module.exports = {
  interpretConversationalInput,
  isTrustedStructuredInput,
  conversationalText,
};
