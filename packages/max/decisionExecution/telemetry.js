'use strict';

function createTelemetryCounters() {
  return {
    decisions_created: 0,
    no_action_decisions: 0,
    actions_selected: 0,
    actions_executed: 0,
    actions_verified: 0,
    actions_blocked: 0,
    verification_failures: 0,
    decisions_superseded: 0,
    human_escalations: 0,
    human_corrections: 0,
    agent_delegations: 0,
    agent_output_rejections: 0,
    insufficient_evidence_abstentions: 0,
    duplicate_evaluations_resolved: 0,
  };
}

function bump(counter, field, amount = 1) {
  counter[field] = (counter[field] || 0) + amount;
}

module.exports = {
  createTelemetryCounters,
  bump,
};
