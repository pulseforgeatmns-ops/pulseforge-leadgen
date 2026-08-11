'use strict';

/**
 * Max Synthesis Layer
 *
 * Shared classify → memory → normalize → synthesize path for Growth /
 * Campaign artifacts (and future Max conversations).
 *
 * Rules:
 * - Renderers consume ArtifactSynthesisContext / CampaignSynthesisContext
 *   phrases and durable operator learnings — not raw answers alone.
 * - Raw answers stay on evidence for citation only.
 * - Guardrails unchanged: no lists, outreach, CRM, or account changes
 *   without explicit approval.
 */

const MessageIntentClassifier = require('./MessageIntentClassifier');
const ConversationMemoryUpdater = require('./ConversationMemoryUpdater');
const BusinessFactNormalizer = require('./BusinessFactNormalizer');
const ArtifactSynthesisContext = require('./ArtifactSynthesisContext');
const CampaignSynthesisContext = require('./CampaignSynthesisContext');

module.exports = {
  // Classifier
  MESSAGE_INTENTS: MessageIntentClassifier.MESSAGE_INTENTS,
  MESSAGE_CLASSES: MessageIntentClassifier.MESSAGE_CLASSES,
  classifyMessageIntent: MessageIntentClassifier.classifyMessageIntent,
  classifyReasoningMessage: MessageIntentClassifier.classifyReasoningMessage,
  looksLikeApproval: MessageIntentClassifier.looksLikeApproval,
  looksLikeApprovalPlusNextRequest:
    MessageIntentClassifier.looksLikeApprovalPlusNextRequest,
  looksLikeArtifactRequest: MessageIntentClassifier.looksLikeArtifactRequest,

  // Memory
  applyConversationMemoryUpdate:
    ConversationMemoryUpdater.applyConversationMemoryUpdate,
  updateMemoryForCampaignArtifactTurn:
    ConversationMemoryUpdater.updateMemoryForCampaignArtifactTurn,
  ensureReasoningMemory: ConversationMemoryUpdater.ensureReasoningMemory,
  resolveCampaignArtifactAction:
    ConversationMemoryUpdater.resolveCampaignArtifactAction,
  markArtifactApproved: ConversationMemoryUpdater.markArtifactApproved,
  markArtifactGenerated: ConversationMemoryUpdater.markArtifactGenerated,

  // Normalizer
  stripInstructionFraming: BusinessFactNormalizer.stripInstructionFraming,
  asEmbeddablePhrase: BusinessFactNormalizer.asEmbeddablePhrase,
  normalizeBusinessFacts: BusinessFactNormalizer.normalizeBusinessFacts,
  normalizeTargetSegmentPhrase:
    BusinessFactNormalizer.normalizeTargetSegmentPhrase,
  normalizeTargetSubtypePhrase:
    BusinessFactNormalizer.normalizeTargetSubtypePhrase,
  normalizeMarketBoundPhrase: BusinessFactNormalizer.normalizeMarketBoundPhrase,
  normalizeObjectivePhrase: BusinessFactNormalizer.normalizeObjectivePhrase,
  containsRawPromptFragment: BusinessFactNormalizer.containsRawPromptFragment,
  findRawPromptFragments: BusinessFactNormalizer.findRawPromptFragments,
  naturalList: BusinessFactNormalizer.naturalList,
  DEFAULT_TOWNS: BusinessFactNormalizer.DEFAULT_TOWNS,
  DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS:
    BusinessFactNormalizer.DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS,

  // Context for renderers
  buildArtifactSynthesisContext:
    ArtifactSynthesisContext.buildArtifactSynthesisContext,
  shortBusinessName: ArtifactSynthesisContext.shortBusinessName,

  // Campaign Memory / CampaignSynthesisContext
  DEFAULT_OPERATOR_LEARNINGS:
    CampaignSynthesisContext.DEFAULT_OPERATOR_LEARNINGS,
  emptyCampaignMemory: CampaignSynthesisContext.emptyCampaignMemory,
  ensureCampaignMemory: CampaignSynthesisContext.ensureCampaignMemory,
  upsertOperatorLearning: CampaignSynthesisContext.upsertOperatorLearning,
  mergeOperatorLearnings: CampaignSynthesisContext.mergeOperatorLearnings,
  applyBatchReviewLearnings:
    CampaignSynthesisContext.applyBatchReviewLearnings,
  buildCampaignSynthesisContext:
    CampaignSynthesisContext.buildCampaignSynthesisContext,
  resolveSubjectLines: CampaignSynthesisContext.resolveSubjectLines,
  resolveSenderVoiceLine: CampaignSynthesisContext.resolveSenderVoiceLine,
  buildPersonalizationNote:
    CampaignSynthesisContext.buildPersonalizationNote,
  filterColdBatchCandidates:
    CampaignSynthesisContext.filterColdBatchCandidates,
  rejectsStreetAddressPersonalization:
    CampaignSynthesisContext.rejectsStreetAddressPersonalization,
  findCampaignMemoryDraftConflicts:
    CampaignSynthesisContext.findCampaignMemoryDraftConflicts,
  outreachDraftPreviewConflictsWithCampaignMemory:
    CampaignSynthesisContext.outreachDraftPreviewConflictsWithCampaignMemory,
};
