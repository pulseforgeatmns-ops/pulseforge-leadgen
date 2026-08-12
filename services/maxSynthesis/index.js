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
const OperatorChatResponsiveness = require('./OperatorChatResponsiveness');
const ConversationalResponsePolicy = require('./ConversationalResponsePolicy');

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

  // Operator chat responsiveness
  RESPONSE_MODES: OperatorChatResponsiveness.RESPONSE_MODES,
  PRIORITY_ORDER: OperatorChatResponsiveness.PRIORITY_ORDER,
  emptyCampaignWorkingState:
    OperatorChatResponsiveness.emptyCampaignWorkingState,
  ensureCampaignWorkingState:
    OperatorChatResponsiveness.ensureCampaignWorkingState,
  looksLikeOperatorWorkflowRevision:
    OperatorChatResponsiveness.looksLikeOperatorWorkflowRevision,
  looksLikeForceRebuildConfirmation:
    OperatorChatResponsiveness.looksLikeForceRebuildConfirmation,
  parseOperatorChatDirectives:
    OperatorChatResponsiveness.parseOperatorChatDirectives,
  applyOperatorDirectivesToWorkingState:
    OperatorChatResponsiveness.applyOperatorDirectivesToWorkingState,
  markDirectivesApplied: OperatorChatResponsiveness.markDirectivesApplied,
  recordRejectedOutput: OperatorChatResponsiveness.recordRejectedOutput,
  countRejectedFingerprint:
    OperatorChatResponsiveness.countRejectedFingerprint,
  selectResponseMode: OperatorChatResponsiveness.selectResponseMode,
  draftOutputFingerprint: OperatorChatResponsiveness.draftOutputFingerprint,
  validateOutreachDraftAgainstInstructions:
    OperatorChatResponsiveness.validateOutreachDraftAgainstInstructions,
  buildStaleSourceDiagnostic:
    OperatorChatResponsiveness.buildStaleSourceDiagnostic,
  identifyStaleInjectionSources:
    OperatorChatResponsiveness.identifyStaleInjectionSources,
  markAwaitingForceRebuild:
    OperatorChatResponsiveness.markAwaitingForceRebuild,
  markForceRebuildBypass: OperatorChatResponsiveness.markForceRebuildBypass,
  clearForceRebuildBypass: OperatorChatResponsiveness.clearForceRebuildBypass,
  buildFollowUpEmailDrafts:
    OperatorChatResponsiveness.buildFollowUpEmailDrafts,
  formatOperatorChatDraftResponse:
    OperatorChatResponsiveness.formatOperatorChatDraftResponse,

  // Conversational response policy (post workflow/state)
  CONVERSATION_MODES: ConversationalResponsePolicy.CONVERSATION_MODES,
  assessConversationContext:
    ConversationalResponsePolicy.assessConversationContext,
  selectConversationMode: ConversationalResponsePolicy.selectConversationMode,
  composeConversationResponse:
    ConversationalResponsePolicy.composeConversationResponse,
  applyConversationalPolicy:
    ConversationalResponsePolicy.applyConversationalPolicy,
  selectResponseModeWithPolicy:
    ConversationalResponsePolicy.selectResponseModeWithPolicy,
  formatApprovedLaunchGateConversational:
    ConversationalResponsePolicy.formatApprovedLaunchGateConversational,
  formatOperatorDiagnosticMessage:
    ConversationalResponsePolicy.formatOperatorDiagnosticMessage,
  containsRendererBoilerplate:
    ConversationalResponsePolicy.containsRendererBoilerplate,
  compactSafetyLockLine: ConversationalResponsePolicy.compactSafetyLockLine,
  expandedSafetyBlock: ConversationalResponsePolicy.expandedSafetyBlock,
  approvalLanguageForGate:
    ConversationalResponsePolicy.approvalLanguageForGate,
  looksLikeExecutionRequest:
    ConversationalResponsePolicy.looksLikeExecutionRequest,
  looksLikeNonExecutionIntent:
    ConversationalResponsePolicy.looksLikeNonExecutionIntent,
  looksLikeOperatorReadinessCheck:
    ConversationalResponsePolicy.looksLikeOperatorReadinessCheck,
  looksLikeLowSignalAmbiguousInput:
    ConversationalResponsePolicy.looksLikeLowSignalAmbiguousInput,
  looksLikeReadinessSubstepSelection:
    ConversationalResponsePolicy.looksLikeReadinessSubstepSelection,
  detectSelectedReadinessItem:
    ConversationalResponsePolicy.detectSelectedReadinessItem,
  looksLikeReadinessFieldCorrection:
    ConversationalResponsePolicy.looksLikeReadinessFieldCorrection,
  parseSenderIdentityFields:
    ConversationalResponsePolicy.parseSenderIdentityFields,
  parseReplyHandlingFields:
    ConversationalResponsePolicy.parseReplyHandlingFields,
  mergeSenderIdentityState:
    ConversationalResponsePolicy.mergeSenderIdentityState,
  mergeReplyHandlingState:
    ConversationalResponsePolicy.mergeReplyHandlingState,
  isSenderFieldValueLine:
    ConversationalResponsePolicy.isSenderFieldValueLine,
  isReplyFieldValueLine:
    ConversationalResponsePolicy.isReplyFieldValueLine,
  composeExecutionConfirmation:
    ConversationalResponsePolicy.composeExecutionConfirmation,
  composeOperatorReadinessCheck:
    ConversationalResponsePolicy.composeOperatorReadinessCheck,
  composeReadinessSubstep:
    ConversationalResponsePolicy.composeReadinessSubstep,
  composeReadinessFieldCorrection:
    ConversationalResponsePolicy.composeReadinessFieldCorrection,
  composeClarificationNeeded:
    ConversationalResponsePolicy.composeClarificationNeeded,
  unresolvedReadinessItems:
    ConversationalResponsePolicy.unresolvedReadinessItems,
  extractOperatorReadinessChecklist:
    ConversationalResponsePolicy.extractOperatorReadinessChecklist,
  mergeOperatorReadinessChecklist:
    ConversationalResponsePolicy.mergeOperatorReadinessChecklist,
  evaluateReadinessItemAgainstState:
    ConversationalResponsePolicy.evaluateReadinessItemAgainstState,
  DEFAULT_UNRESOLVED_READINESS_ITEMS:
    ConversationalResponsePolicy.DEFAULT_UNRESOLVED_READINESS_ITEMS,
  READINESS_CHECKLIST_CLOSING_ASK:
    ConversationalResponsePolicy.READINESS_CHECKLIST_CLOSING_ASK,
  READINESS_CHECKLIST_SAFETY_LINE:
    ConversationalResponsePolicy.READINESS_CHECKLIST_SAFETY_LINE,
  CLARIFICATION_NEEDED_ASK:
    ConversationalResponsePolicy.CLARIFICATION_NEEDED_ASK,
  READINESS_SUBSTEPS: ConversationalResponsePolicy.READINESS_SUBSTEPS,
  READINESS_SUBSTEP_SAFETY_LINE:
    ConversationalResponsePolicy.READINESS_SUBSTEP_SAFETY_LINE,
  READINESS_NEXT_ITEM_PROMPTS:
    ConversationalResponsePolicy.READINESS_NEXT_ITEM_PROMPTS,
  dedupeOperatorStateUpdateMessage:
    ConversationalResponsePolicy.dedupeOperatorStateUpdateMessage,
  sanitizeApprovedStateLeadIn:
    ConversationalResponsePolicy.sanitizeApprovedStateLeadIn,
};
