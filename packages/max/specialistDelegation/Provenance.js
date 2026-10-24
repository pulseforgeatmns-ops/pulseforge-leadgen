'use strict';

/**
 * SPEC-098 — explainability / provenance chain.
 *
 * Max priority change → Max evaluation → SpecialistResult → SpecialistDelegation → Evidence
 */

function buildProvenanceChain(input = {}) {
  const evaluation = input.evaluation || null;
  const result = input.result || null;
  const delegation = input.delegation || null;

  const evidence = [];
  const seen = new Set();
  const pushEvidence = (refs) => {
    for (const ref of refs || []) {
      if (!ref || !ref.id || seen.has(ref.id)) continue;
      seen.add(ref.id);
      evidence.push({
        id: ref.id,
        kind: ref.kind || null,
        sourceKind: ref.sourceKind || null,
        label: ref.label || null,
      });
    }
  };
  pushEvidence(result && result.evidenceRefs);
  pushEvidence(delegation && delegation.evidenceRefs);
  pushEvidence(evaluation && evaluation.provenance && evaluation.provenance.evidence);

  return {
    priorityChange:
      evaluation && evaluation.suggestedPriorityChange
        ? {
            domain: evaluation.suggestedPriorityChange.domain,
            from: evaluation.suggestedPriorityChange.from,
            to: evaluation.suggestedPriorityChange.to,
            applied: evaluation.priorityApplied === true,
            reason: evaluation.suggestedPriorityChange.reason || null,
          }
        : null,
    evaluation: evaluation
      ? {
          id: evaluation.id || null,
          explanation: evaluation.explanation,
          objectiveSatisfied: evaluation.objectiveSatisfied,
          materialChange: evaluation.materialChange,
          acceptedAsGroundTruth: evaluation.acceptedAsGroundTruth === true,
          operatorDirectionHonored: evaluation.operatorDirectionHonored !== false,
        }
      : null,
    result: result
      ? {
          id: result.id,
          status: result.status,
          summary: result.summary,
          confidence: result.confidence,
          uncertainties: result.uncertainties || [],
          specialist: result.specialist,
          capability: result.capability,
        }
      : null,
    delegation: delegation
      ? {
          id: delegation.id,
          objective: delegation.objective,
          reason: delegation.reason,
          authority: delegation.authority,
          specialist: delegation.specialist,
          capability: delegation.capability,
        }
      : null,
    evidence,
  };
}

function formatProvenanceNarrative(chain) {
  if (!chain || !chain.delegation) {
    return 'No delegation trail is available.';
  }
  const parts = [];
  if (chain.priorityChange) {
    const applied = chain.priorityChange.applied ? 'I changed' : 'I considered changing';
    parts.push(
      `${applied} ${chain.priorityChange.domain} from ${chain.priorityChange.from} to ${chain.priorityChange.to}.`
    );
  }
  if (chain.evaluation && chain.evaluation.explanation) {
    parts.push(chain.evaluation.explanation);
  }
  if (chain.result) {
    parts.push(
      `${chain.result.specialist} returned ${chain.result.status}` +
        (chain.result.confidence != null ? ` (confidence ${chain.result.confidence})` : '') +
        '.'
    );
  }
  parts.push(`That work was delegated as: ${chain.delegation.objective}`);
  if (chain.delegation.reason) {
    parts.push(`Reason: ${chain.delegation.reason}`);
  }
  if (chain.evidence.length) {
    const labels = chain.evidence.map((e) => e.label || e.id).join('; ');
    parts.push(`Evidence: ${labels}.`);
  }
  return parts.join(' ');
}

module.exports = {
  buildProvenanceChain,
  formatProvenanceNarrative,
};
