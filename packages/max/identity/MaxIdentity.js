'use strict';

/**
 * SPEC-149A / SPEC-151 — Max Identity & Operating Model (ADR-059).
 * Canonical organizational identity for Max across workspace, chat, digest, and UI.
 * Structured operating model knowledge lives in OperatingModel.js; this module
 * retains presentation helpers and backward-compatible re-exports.
 */

const {
  OPERATING_MODEL,
  MAX_CORE_MISSION,
  MAX_ROLE,
  MAX_OWNS,
  OPERATOR_OWNS,
  MAX_DOES_NOT,
  RESPONSIBILITY_BOUNDARIES,
  DELEGATION_RULES,
  SPECIALIST_ROSTER,
  DECISION_FRAMEWORK,
  getRelationship,
  listSpecialistNames,
} = require('./OperatingModel');

const FORBIDDEN_IDENTITY_TERMS = Object.freeze([
  'ai assistant',
  'chatbot',
  'mission manager',
  'manager agent',
  'intelligence advisor',
  'llm',
  'prompt',
]);

const PRESENTATION_IDENTITY =
  'You are Max, the business operating system for this workspace. ' +
  'You are a PRESENTATION ENGINE only (ADR-005). ' +
  'Describe organizational responsibility — not implementation or product labels.';

const LEGACY_CHAT_SYSTEM =
  'You are Max, the business operating system for this business. ' +
  'You answer operator questions using only the provided database context. ' +
  'Be concise, direct, and evidence-backed. Say "Based on current evidence..." rather than "I think...". ' +
  'If the context does not contain enough evidence, say what is missing instead of inventing details. ' +
  'Prioritize warm signals, pipeline risk, next actions, and anomalies. ' +
  'When the user asks you to take an action like triggering Emmett or flagging prospects, respond with the specific prospect names and emails from the context provided, and confirm what action you would take. ' +
  'Specialists perform domain-specific work; the operator retains final authority over business decisions and external actions.';

const DIGEST_IDENTITY =
  'You are Max, the business operating system for this business. ' +
  'Write the digest in an operator voice — evidence-backed, not speculative.';

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function resolveBusinessName(session) {
  const ctx = (session && session.context) || {};
  return normalizeText(ctx.businessName || ctx.tenantName || ctx.companyName || '');
}

/**
 * Workspace-scoped opening line — identity stays constant, only context changes.
 * @param {object} [session]
 */
function composeWorkspaceIntroduction(session) {
  const businessName = resolveBusinessName(session);
  if (businessName) {
    return (
      `I am the operating system responsible for helping ${businessName} achieve its business objectives. ` +
      `${MAX_CORE_MISSION}`
    );
  }
  return MAX_ROLE;
}

function operatingModeLabel(session) {
  const ctx = (session && session.context) || {};
  if (ctx.missionId || ctx.acquisitionMissionId) {
    return 'Active mission — mission runtime owns execution context.';
  }
  if (ctx.executionDomain) {
    return `Workspace mode (${ctx.executionDomain}).`;
  }
  return 'Workspace intelligence mode — read-only unless you issue an execution command.';
}

function containsForbiddenIdentityTerm(text) {
  const normalized = normalizeText(text).toLowerCase();
  return FORBIDDEN_IDENTITY_TERMS.some((term) => normalized.includes(term));
}

function assertIdentityCompliance(text) {
  if (containsForbiddenIdentityTerm(text)) {
    throw new Error('Max identity response contains forbidden implementation label');
  }
}

module.exports = {
  OPERATING_MODEL,
  FORBIDDEN_IDENTITY_TERMS,
  MAX_CORE_MISSION,
  MAX_ROLE,
  MAX_OWNS,
  OPERATOR_OWNS,
  MAX_DOES_NOT,
  RESPONSIBILITY_BOUNDARIES,
  DELEGATION_RULES,
  SPECIALIST_ROSTER,
  DECISION_FRAMEWORK,
  PRESENTATION_IDENTITY,
  LEGACY_CHAT_SYSTEM,
  DIGEST_IDENTITY,
  composeWorkspaceIntroduction,
  operatingModeLabel,
  containsForbiddenIdentityTerm,
  assertIdentityCompliance,
  resolveBusinessName,
  getRelationship,
  listSpecialistNames,
};
