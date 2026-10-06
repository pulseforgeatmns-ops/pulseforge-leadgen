'use strict';

const { AMBIGUITY_KIND } = require('./types');

function emptyUnderstandingTelemetry() {
  return {
    understanding_input_count: 0,
    understanding_thread_count: 0,
    understanding_commit_blocked_count: 0,
    understanding_clarification_required_count: 0,
    understanding_correction_count: 0,
    understanding_low_confidence_claim_count: 0,
    understanding_entity_ambiguity_count: 0,
    understanding_reference_ambiguity_count: 0,
  };
}

function recordUnderstandingTelemetry(situationModel, validation = {}, dimensions = {}) {
  const telemetry = emptyUnderstandingTelemetry();
  telemetry.understanding_input_count = 1;
  telemetry.understanding_thread_count = situationModel?.threads?.length || 0;
  if (validation.blockCommit) {
    telemetry.understanding_commit_blocked_count = 1;
    telemetry.understanding_clarification_required_count = 1;
  }
  const corrections = situationModel?.corrections?.length
    || (situationModel?.threads || []).reduce((n, t) => n + (t.corrections?.length || 0), 0);
  telemetry.understanding_correction_count = corrections;

  const ambiguities = validation.materialAmbiguities || validation.ambiguities || [];
  for (const a of ambiguities) {
    if (a.kind === AMBIGUITY_KIND.ACCOUNT) telemetry.understanding_entity_ambiguity_count += 1;
    if (a.kind === AMBIGUITY_KIND.PRONOUN || a.kind === AMBIGUITY_KIND.CONTACT) {
      telemetry.understanding_reference_ambiguity_count += 1;
    }
  }

  const lowConf = (situationModel?.threads || []).flatMap(t => t.decisionMakerSignals || [])
    .filter(s => s.epistemic === 'uncertain' || s.epistemic === 'inferred');
  telemetry.understanding_low_confidence_claim_count = lowConf.length;

  telemetry.dimensions = {
    input_type: dimensions.input_type || 'conversational',
    actor_role: dimensions.actor_role || null,
    thread_count: telemetry.understanding_thread_count,
    blocked_reason: validation.blockCommit
      ? (ambiguities[0]?.kind || 'material_ambiguity')
      : null,
  };
  return telemetry;
}

function mergeUnderstandingTelemetry(targetTelemetry, understandingTelemetry) {
  if (!targetTelemetry || !understandingTelemetry) return targetTelemetry;
  for (const [key, value] of Object.entries(understandingTelemetry)) {
    if (key === 'dimensions') {
      targetTelemetry.understanding_dimensions = value;
      continue;
    }
    if (typeof value === 'number') {
      targetTelemetry[key] = (targetTelemetry[key] || 0) + value;
    }
  }
  return targetTelemetry;
}

module.exports = {
  emptyUnderstandingTelemetry,
  recordUnderstandingTelemetry,
  mergeUnderstandingTelemetry,
};
