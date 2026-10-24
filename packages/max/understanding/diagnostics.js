'use strict';

function buildUnderstandingDiagnostics(situationModel) {
  if (!situationModel) {
    return {
      threadCount: 0,
      claimsExtracted: 0,
      correctionsApplied: 0,
      ambiguities: 0,
      blocked: false,
    };
  }
  const threads = situationModel.threads || [];
  const claims = threads.flatMap(t => t.claims || []).length
    + (situationModel.claims?.length || 0);
  const ingestionClaims = threads.flatMap(t => t.ingestionClaims || []).length;
  const corrections = (situationModel.corrections || []).length
    + threads.flatMap(t => t.corrections || []).length;
  const ambiguities = (situationModel.validation?.materialAmbiguities || situationModel.ambiguities || []).length
    + threads.flatMap(t => t.ambiguities || []).length;

  return {
    threadCount: threads.length,
    claimsExtracted: ingestionClaims || claims,
    correctionsApplied: corrections,
    ambiguities,
    blocked: Boolean(situationModel.validation?.blockCommit),
    clarificationRequired: Boolean(situationModel.validation?.blockCommit),
    lowConfidenceClaims: threads.flatMap(t => t.decisionMakerSignals || [])
      .filter(s => s.epistemic === 'uncertain' || s.epistemic === 'inferred').length,
  };
}

module.exports = {
  buildUnderstandingDiagnostics,
};
