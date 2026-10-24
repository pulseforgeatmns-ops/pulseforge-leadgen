'use strict';

/**
 * Evaluate agent delegation output before treating it as canonical evidence.
 */
function evaluateAgentOutput({ delegation, output }) {
  if (!output || typeof output !== 'object') {
    return {
      accepted: false,
      reason: 'missing_output',
      epistemic: 'UNKNOWN',
    };
  }
  if (output.insufficient || output.status === 'INSUFFICIENT_EVIDENCE') {
    return {
      accepted: false,
      reason: 'agent_reported_insufficient_evidence',
      epistemic: 'UNKNOWN',
    };
  }
  const evidenceIds = Array.isArray(output.evidence_ids) ? output.evidence_ids : [];
  if (evidenceIds.length === 0 && !output.canonical) {
    return {
      accepted: false,
      reason: 'unsupported_agent_result',
      epistemic: 'INFERRED',
    };
  }
  return {
    accepted: true,
    reason: 'verified_agent_output',
    epistemic: 'KNOWN',
    evidence_ids: evidenceIds,
    delegation_id: delegation?.id,
  };
}

module.exports = {
  evaluateAgentOutput,
};
