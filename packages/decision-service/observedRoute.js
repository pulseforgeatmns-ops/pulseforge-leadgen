'use strict';

const { redactText } = require('./privacy');

// Compare observed handler metadata, never reclassify the operator's words.
// Unknown/compound routes remain incomparable rather than false mismatches.
function observedRoute(result, failed = false) {
  const trace = result?.routingTrace || {};
  const metadata = result?.structured?.metadata || {};
  const owner = result?.workspaceOwnership?.owner || trace.owner;
  const objective = result?.objectiveResolution?.primaryObjective || trace.primaryObjective;
  const messageType = result?.messageClassification?.type || trace.messageType;
  const action = result?.resolution?.action;
  const pending = metadata.pendingDecisionResolution || result?.operatorIntent?.pendingDecisionResolution;
  let route = null;
  if (!failed && result && !result.error && !result.metadata?.miep && trace.pipeline !== 'MultiIntentExecutionPlanner') {
    if (action === 'clarify' || pending?.outcome === 'ambiguous') route = 'clarification';
    else if (/^(approve_|reject|cancel)|_(approved|rejected)$/.test(action || '') || ['affirm', 'reject'].includes(pending?.outcome)) route = 'approval';
    else if (['mission_inspection', 'execution_inspection', 'session_inspection'].includes(objective)
      || ['session_inspection', 'execution_inspection'].includes(messageType)
      || ['session_inspected', 'execution_inspected', 'session_explained', 'inspected'].includes(action)
      || ['mission_inspection', 'execution_state_manager'].includes(owner)
      || action === 'diagnosed' || pending?.outcome === 'question') route = 'inspection';
    else if (owner === 'session_state_manager') route = 'session_configuration';
    else if (owner === 'conversation_identity' || result.route === 'identity') route = 'identity';
    else if (['active_mission', 'mission_creation'].includes(owner) || result.route === 'mission') route = 'mission';
    else if (owner?.startsWith('specialist_')) route = 'specialist';
    else if (['conversation_layer', 'reflection', 'reasoning'].includes(owner)) route = 'conversation';
    else if (result.route === 'intelligence' || result.route === 'ao_briefing') route = 'intelligence';
    else if (result.route === 'inspection') route = 'inspection';
  }
  return {
    route, raw_route: redactText(result?.route, 80), owner: redactText(owner, 100),
    pipeline: redactText(trace.pipeline, 100), primary_objective: redactText(objective, 100),
    message_type: redactText(messageType, 100),
    action: redactText(action, 100), failed: Boolean(failed || result?.error),
  };
}
module.exports = { observedRoute };
