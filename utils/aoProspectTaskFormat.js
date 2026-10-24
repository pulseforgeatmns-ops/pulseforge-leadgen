'use strict';

function formatAoTask(routing, { assignedAoName } = {}) {
  const reasoning = routing.reasoning || {};
  return {
    account: reasoning.account,
    segment: reasoning.segment,
    location: reasoning.location,
    assigned_ao: assignedAoName || routing.recommended_ao_name || null,
    motion: routing.recommended_motion,
    priority: reasoning.priority || 'normal',
    why_this_account_matters: reasoning.why_account_matters,
    recommended_angle: routing.recommended_angle,
    first_action: routing.recommended_first_action,
    discovery_objective: routing.discovery_objective,
    suggested_opener: routing.suggested_opener,
    desired_next_outcome: routing.desired_next_outcome,
    what_to_log: routing.required_log_fields,
  };
}

module.exports = {
  formatAoTask,
};
