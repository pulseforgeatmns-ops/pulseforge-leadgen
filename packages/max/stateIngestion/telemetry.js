'use strict';

function emptyTelemetry() {
  return {
    ingestions_received: 0,
    evidence_artifacts_created: 0,
    claims_extracted: 0,
    claims_resolved: 0,
    claims_ambiguous: 0,
    claims_conflicted: 0,
    entities_created: 0,
    entities_reconciled: 0,
    mutations_proposed: 0,
    mutations_committed: 0,
    mutations_verified: 0,
    verification_failures: 0,
    duplicate_claims_suppressed: 0,
    operator_corrections: 0,
  };
}

function bump(telemetry, key, delta = 1) {
  telemetry[key] = (telemetry[key] || 0) + delta;
  return telemetry;
}

module.exports = {
  emptyTelemetry,
  bump,
};
