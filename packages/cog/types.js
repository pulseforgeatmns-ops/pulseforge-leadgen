'use strict';

/**
 * COG — Cognitive Evaluation Framework
 * Core types and constants.
 *
 * Knowledge and cognition are different. COG evaluates cognition.
 */

const COG_VERSION = '0.1.0';

const RUN_STATUS = Object.freeze({
  PENDING: 'pending',
  RUNNING: 'running',
  COMPLETED: 'completed',
  FAILED: 'failed',
  NEEDS_REVIEW: 'needs_review',
});

const REVIEW_STATUS = Object.freeze({
  PENDING: 'pending',
  APPROVED: 'approved',
  REJECTED: 'rejected',
  NOT_REQUIRED: 'not_required',
});

const SCORE_RANGE = Object.freeze({ min: 0, max: 10 });

/**
 * @typedef {object} ConversationTurn
 * @property {number} turnIndex
 * @property {string} role - 'operator' | 'max'
 * @property {string} content
 * @property {string} [timestamp]
 * @property {object} [metadata]
 */

/**
 * @typedef {object} BenchmarkConversation
 * @property {string} id
 * @property {string} title
 * @property {string} [description]
 * @property {Array<{role: 'operator', content: string, turnIndex?: number}>} turns
 * @property {object} [context] - Workspace context passed to Max
 */

/**
 * @typedef {object} ExpectedBehavior
 * @property {string} id
 * @property {string} description
 * @property {number} [turnIndex] - Turn this applies to (null = whole conversation)
 * @property {'pattern'|'absence'|'continuity'|'identity'|'confidence'|'counterfactual'|'revision'|'abstraction'|'proposition'|'graph'} checkType
 * @property {string|string[]} [pattern] - Regex or string to match
 * @property {string|string[]} [absencePattern] - Must NOT match
 * @property {string} [propositionKey] - Key proposition that must persist
 * @property {string} [failureCode] - R-00x code when violated
 * @property {boolean} [requiresHumanReview] - Subjective judgment needed
 */

/**
 * @typedef {object} ScoringRubric
 * @property {string} domainId
 * @property {Array<{score: number, label: string, criteria: string}>} levels
 * @property {string} [notes]
 */

/**
 * @typedef {object} CognitiveDomain
 * @property {string} id - e.g. COG-101
 * @property {string} shortName
 * @property {string} objective
 * @property {BenchmarkConversation} conversation
 * @property {ExpectedBehavior[]} expectedBehaviors
 * @property {string[]} evaluationCriteria
 * @property {ScoringRubric} rubric
 */

/**
 * @typedef {object} BenchmarkSuite
 * @property {string} id - e.g. COG-001
 * @property {string} version
 * @property {string} label
 * @property {string} [description]
 * @property {string[]} domainIds
 * @property {string} createdAt
 */

/**
 * @typedef {object} FailureClassification
 * @property {string} code - R-00x
 * @property {string} label
 * @property {string} [description]
 * @property {number} [turnIndex]
 * @property {string} [behaviorId]
 * @property {string} [evidence]
 * @property {boolean} [requiresHumanReview]
 */

/**
 * @typedef {object} DomainResult
 * @property {string} domainId
 * @property {string} status
 * @property {BenchmarkConversation['id']} conversationId
 * @property {ConversationTurn[]} transcript
 * @property {FailureClassification[]} failures
 * @property {Array<{behaviorId: string, passed: boolean, evidence?: string}>} behaviorResults
 * @property {number|null} score - null until scored
 * @property {string} reviewStatus
 * @property {string} [error]
 * @property {number} durationMs
 */

/**
 * @typedef {object} CogRunResult
 * @property {string} runId
 * @property {string} suiteId
 * @property {string} suiteVersion
 * @property {string} cogVersion
 * @property {string} status
 * @property {string} startedAt
 * @property {string} [completedAt]
 * @property {DomainResult[]} domains
 * @property {number|null} overallScore
 * @property {object} [metadata]
 */

module.exports = {
  COG_VERSION,
  RUN_STATUS,
  REVIEW_STATUS,
  SCORE_RANGE,
};
