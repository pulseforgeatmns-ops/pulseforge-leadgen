'use strict';

const { POLICY_ACTIONS, POLICY_SEVERITIES } = require('../PolicyTypes');

/**
 * @typedef {object} RuleResult
 * @property {boolean} passed
 * @property {string} severity
 * @property {string} reason
 * @property {'allow'|'requireApproval'|'block'|'warn'} action
 * @property {object} [details]
 */

/**
 * @typedef {object} PolicyRule
 * @property {string} id
 * @property {string} name
 * @property {number} priority - lower runs first
 * @property {(context: object) => RuleResult} evaluate
 */

/**
 * Build a normalized RuleResult.
 * @param {object} input
 * @returns {RuleResult}
 */
function ruleResult(input) {
  const action = String(input.action || POLICY_ACTIONS.ALLOW);
  if (!Object.values(POLICY_ACTIONS).includes(action)) {
    throw new Error(`Unknown policy action: ${action}`);
  }
  const severity = String(input.severity || POLICY_SEVERITIES.NONE);
  if (!Object.values(POLICY_SEVERITIES).includes(severity)) {
    throw new Error(`Unknown policy severity: ${severity}`);
  }
  return {
    passed: Boolean(input.passed),
    severity,
    reason: String(input.reason || ''),
    action,
    details: input.details && typeof input.details === 'object' ? { ...input.details } : {},
  };
}

/**
 * @param {PolicyRule} rule
 */
function assertRule(rule) {
  if (!rule || typeof rule !== 'object') {
    throw new Error('PolicyRule must be an object');
  }
  if (!rule.id) throw new Error('PolicyRule requires id');
  if (!rule.name) throw new Error('PolicyRule requires name');
  if (typeof rule.priority !== 'number' || !Number.isFinite(rule.priority)) {
    throw new Error(`PolicyRule ${rule.id} requires numeric priority`);
  }
  if (typeof rule.evaluate !== 'function') {
    throw new Error(`PolicyRule ${rule.id} requires evaluate(context)`);
  }
}

module.exports = {
  ruleResult,
  assertRule,
  POLICY_ACTIONS,
  POLICY_SEVERITIES,
};
