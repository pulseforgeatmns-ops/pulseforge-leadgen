'use strict';

const { AUDIENCE_TIER } = require('./types');
const { ACTION_TYPE } = require('../decisionExecution/types');

function audienceTierForDecision(decision) {
  const action = decision?.selected_action?.action_type;
  if (action === ACTION_TYPE.NO_ACTION && decision?.reevaluate_after) {
    return AUDIENCE_TIER.WAITING_SILENT;
  }
  if ([ACTION_TYPE.ASK_AO_STATUS, ACTION_TYPE.CREATE_AO_TASK].includes(action)) {
    return AUDIENCE_TIER.AO_HANDLES;
  }
  if (action === ACTION_TYPE.ESCALATE_OPERATOR) {
    return AUDIENCE_TIER.OPERATOR_VISIBLE;
  }
  if ([ACTION_TYPE.DELEGATE_AGENT, ACTION_TYPE.EXECUTE_INTERNAL_UPDATE].includes(action)) {
    return AUDIENCE_TIER.MAX_HANDLES;
  }
  return AUDIENCE_TIER.WAITING_SILENT;
}

function operatorVisibleItems(items, { limit = 10 } = {}) {
  return items
    .filter(i => i.audience_tier === AUDIENCE_TIER.OPERATOR_VISIBLE && !i.resolved_at)
    .sort((a, b) => scorePriority(b) - scorePriority(a))
    .slice(0, limit);
}

function scorePriority(item) {
  const p = item.priority || {};
  return (
    Number(p.urgency || 0) * 2
    + Number(p.importance || 0)
    + Number(p.mission_relevance || 0)
    + (item.status === 'OVERDUE' ? 5 : 0)
  );
}

module.exports = {
  audienceTierForDecision,
  operatorVisibleItems,
  scorePriority,
};
