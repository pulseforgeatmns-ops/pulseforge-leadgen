'use strict';

const { redactText } = require('./privacy');

const AO_INTENT_ROUTES = Object.freeze({
  account_prioritization: 'intelligence',
  account_briefing: 'intelligence',
  coaching: 'conversation',
  prospect_brief: 'intelligence',
  general: 'conversation',
});

const AO_SOURCE_PREFIX = /^ao_/;

function observedAoRoute(result = {}, source = '') {
  if (!result || result.error) return null;
  if (result.intent && AO_INTENT_ROUTES[result.intent]) {
    return {
      route: AO_INTENT_ROUTES[result.intent],
      raw_route: redactText(result.intent, 80),
      owner: redactText('ao_max', 100),
      pipeline: redactText('AoMaxConversation', 100),
      primary_objective: redactText(result.intent, 100),
      message_type: redactText(result.mode || 'conversation', 100),
      action: result.escalate ? redactText('escalate', 100) : null,
      kind: null,
      failed: false,
    };
  }
  if (result.mode && result.mode !== 'conversation') {
    return {
      route: 'conversation',
      raw_route: redactText(result.mode, 80),
      owner: redactText('ao_session', 100),
      pipeline: redactText('AoMaxFlow', 100),
      primary_objective: redactText(result.mode, 100),
      message_type: redactText('guided_session', 100),
      action: result.completed ? redactText('session_completed', 100) : null,
      kind: null,
      failed: false,
    };
  }
  if (AO_SOURCE_PREFIX.test(source || '') && (result.reply != null || result.session_id)) {
    return {
      route: 'conversation',
      raw_route: redactText(source, 80),
      owner: redactText('ao_max', 100),
      pipeline: redactText('AoMaxConversation', 100),
      primary_objective: redactText('general_conversation', 100),
      message_type: redactText('conversation', 100),
      action: null,
      kind: null,
      failed: false,
    };
  }
  return null;
}

// Compare observed handler metadata, never reclassify the operator's words.
// Unknown/compound routes remain incomparable rather than false mismatches.
function observedRoute(result, failed = false, meta = {}) {
  const trace = result?.routingTrace || {};
  const metadata = result?.structured?.metadata || {};
  const owner = result?.workspaceOwnership?.owner || trace.owner;
  const objective = result?.objectiveResolution?.primaryObjective || trace.primaryObjective;
  const messageType = result?.messageClassification?.type || trace.messageType;
  const action = result?.resolution?.action;
  const pending = metadata.pendingDecisionResolution || result?.operatorIntent?.pendingDecisionResolution;
  const pendingKind = result?.pendingOperatorDecision?.kind
    || metadata.pendingOperatorDecision?.kind
    || pending?.decisionKind
    || null;
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
    else if (result.route === 'prospect_brief') route = 'intelligence';
  }
  if (route == null && !failed && result && !result.error) {
    const ao = observedAoRoute(result, meta.source);
    if (ao) return ao;
  }
  return {
    route, raw_route: redactText(result?.route, 80), owner: redactText(owner, 100),
    pipeline: redactText(trace.pipeline, 100), primary_objective: redactText(objective, 100),
    message_type: redactText(messageType, 100),
    action: redactText(action, 100),
    kind: redactText(pendingKind, 100),
    failed: Boolean(failed || result?.error),
  };
}
module.exports = { observedRoute, observedAoRoute, AO_INTENT_ROUTES };
