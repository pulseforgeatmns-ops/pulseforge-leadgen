'use strict';

const AO_ROUTING_ISSUE_TYPES = Object.freeze([
  'wrong_route',
  'wrong_prospect',
  'wrong_mission',
  'lost_context',
  'should_have_opened_brief',
  'should_have_opened_conversation',
  'treated_as_done_incorrectly',
  'other',
]);

const AO_CONVERSATION_STATUSES = Object.freeze([
  'active',
  'done',
  'archived',
  'closed',
  'reopened',
]);

function isValidRoutingIssueType(value) {
  return AO_ROUTING_ISSUE_TYPES.includes(String(value || '').trim());
}

function isRestorableConversationStatus(status) {
  return ['done', 'archived', 'closed', 'reopened'].includes(String(status || '').trim());
}

function isActiveConversationStatus(status) {
  return ['active', 'reopened'].includes(String(status || 'active').trim());
}

module.exports = {
  AO_ROUTING_ISSUE_TYPES,
  AO_CONVERSATION_STATUSES,
  isValidRoutingIssueType,
  isRestorableConversationStatus,
  isActiveConversationStatus,
};
