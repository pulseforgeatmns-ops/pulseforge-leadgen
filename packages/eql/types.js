'use strict';

/**
 * Evidence Query Language (EQL) types — SPEC-020.
 *
 * Domain-neutral declarative queries over the Evidence Graph,
 * replay history, and reasoning outputs. No mutation statements.
 */

/** @typedef {'FIND'|'SHOW'|'REPLAY'|'COMPARE'|'EXPLAIN'} EqlStatementKind */

/**
 * Canonical query targets (singular + plural normalize to these).
 * @typedef {'subjects'|'observations'|'evidence'|'claims'|'outcomes'|'recommendations'|'replay_sessions'|'calibrations'|'accuracies'|'strategy_packs'|'trades'|'screenshots'|'daily_reviews'|'weekly_reviews'|'best_hypotheses'|'trade_calibrations'|'findings'|'similar_trades'|'periods'} EqlTarget
 */

/**
 * @typedef {object} EqlEntityRef
 * @property {EqlTarget} target
 * @property {string} id
 */

/**
 * @typedef {'='|'!='|'>'|'>='|'<'|'<='|'CONTAINS'} EqlOperator
 */

/**
 * @typedef {object} EqlCondition
 * @property {string} field
 * @property {EqlOperator} operator
 * @property {string|number|boolean|null} value
 */

/**
 * @typedef {object} EqlOrderBy
 * @property {string} field
 * @property {'ASC'|'DESC'} direction
 */

/**
 * @typedef {object} EqlFindNode
 * @property {'FIND'} kind
 * @property {EqlTarget} target
 * @property {EqlEntityRef|null} entity
 * @property {EqlCondition[]} where
 * @property {EqlOrderBy|null} orderBy
 * @property {number|null} limit
 * @property {boolean} explain
 */

/**
 * @typedef {object} EqlShowNode
 * @property {'SHOW'} kind
 * @property {EqlTarget} target
 * @property {'SUPPORTING'|'CONTRADICTING'|'FOR'|null} relation
 * @property {EqlEntityRef|null} related
 * @property {EqlCondition[]} where
 * @property {boolean} explain
 */

/**
 * @typedef {object} EqlReplayNode
 * @property {'REPLAY'} kind
 * @property {string|null} subject
 * @property {string} from
 * @property {string} to
 * @property {boolean} explain
 */

/**
 * @typedef {object} EqlCompareNode
 * @property {'COMPARE'} kind
 * @property {EqlEntityRef|string} left
 * @property {EqlEntityRef|string} right
 * @property {boolean} explain
 */

/**
 * @typedef {object} EqlExplainNode
 * @property {'EXPLAIN'} kind
 * @property {EqlEntityRef|null} entity
 * @property {boolean} explain
 */

/** @typedef {EqlFindNode|EqlShowNode|EqlReplayNode|EqlCompareNode|EqlExplainNode} EqlAst */

/**
 * @typedef {object} EqlPlanStep
 * @property {string} op
 * @property {Record<string, unknown>} [args]
 */

/**
 * @typedef {object} EqlPlan
 * @property {EqlStatementKind} kind
 * @property {EqlAst} ast
 * @property {EqlPlanStep[]} steps
 * @property {boolean} explain
 */

/**
 * @typedef {object} EqlResult
 * @property {EqlStatementKind} kind
 * @property {EqlTarget|null} target
 * @property {unknown[]} rows
 * @property {number} count
 * @property {object|null} explanation
 * @property {EqlAst} ast
 * @property {EqlPlan} plan
 * @property {boolean} mutatesProduction
 */

const EQL_RULES = Object.freeze({
  DOMAIN_NEUTRAL: 'eql_is_domain_neutral',
  NO_MUTATION: 'eql_has_no_mutation_statements',
  NO_RUNTIME_BRANCHING: 'eql_has_no_domain_runtime_branching',
  DECLARATIVE: 'eql_queries_are_declarative',
  EXPLAINABLE: 'eql_queries_support_explain',
});

const STATEMENT_KINDS = Object.freeze([
  'FIND',
  'SHOW',
  'REPLAY',
  'COMPARE',
  'EXPLAIN',
]);

const MUTATION_KEYWORDS = Object.freeze([
  'UPDATE',
  'DELETE',
  'INSERT',
  'CREATE',
  'DROP',
  'SET',
  'MERGE',
  'UPSERT',
]);

/** Plural canonical target → aliases (case-insensitive). */
const TARGET_ALIASES = Object.freeze({
  subjects: ['subject', 'subjects'],
  observations: ['observation', 'observations'],
  evidence: ['evidence'],
  claims: ['claim', 'claims'],
  outcomes: ['outcome', 'outcomes'],
  recommendations: ['recommendation', 'recommendations'],
  replay_sessions: [
    'replaysession',
    'replaysessions',
    'replay_session',
    'replay_sessions',
    'replay',
    'replays',
  ],
  calibrations: ['calibration', 'calibrations'],
  accuracies: ['accuracy', 'accuracies'],
  strategy_packs: [
    'strategypack',
    'strategypacks',
    'strategy_pack',
    'strategy_packs',
    'pack',
    'packs',
  ],
  trades: ['trade', 'trades'],
  screenshots: ['screenshot', 'screenshots', 'chart_snapshot', 'chartsnapshot'],
  daily_reviews: ['dailyreview', 'dailyreviews'],
  weekly_reviews: ['weeklyreview', 'weeklyreviews'],
  best_hypotheses: ['besthypotheses', 'besthypothesis'],
  trade_calibrations: [
    'tradecalibration',
    'tradecalibrations',
    'trade_calibration',
    'trade_calibrations',
  ],
  findings: ['finding', 'findings'],
  similar_trades: ['similartrades', 'similartrade'],
  periods: ['period', 'periods'],
});

/** Fields treated as the logical "subject" identity (domain-neutral). */
const SUBJECT_FIELD_ALIASES = Object.freeze([
  'subject',
  'subjectId',
  'subject_id',
  'companyId',
  'company_id',
  'entityId',
  'entity_id',
]);

/** Fields treated as the logical confidence score. */
const CONFIDENCE_FIELD_ALIASES = Object.freeze([
  'confidence',
  'score',
  'confidenceScore',
  'confidence_score',
]);

/** Fields treated as the logical id / claim type key. */
const ID_FIELD_ALIASES = Object.freeze([
  'id',
  'claimId',
  'claim_id',
  'claimType',
  'claim_type',
  'strategy',
  'type',
  'observationType',
  'observation_type',
]);

module.exports = {
  EQL_RULES,
  STATEMENT_KINDS,
  MUTATION_KEYWORDS,
  TARGET_ALIASES,
  SUBJECT_FIELD_ALIASES,
  CONFIDENCE_FIELD_ALIASES,
  ID_FIELD_ALIASES,
};
