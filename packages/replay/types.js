'use strict';

/**
 * Deterministic Replay types (SPEC-018).
 *
 * History is stored once. Reasoning is regenerated.
 */

/** @typedef {'market'|'crm'|string} DomainPackId */

/**
 * @typedef {object} ReplayVersions
 * @property {string} ontology
 * @property {string} strategyPack
 * @property {string} runtime
 */

/**
 * @typedef {object} ImmutableObservation
 * @property {string} id - deterministic identity (never positional)
 * @property {string} subjectId
 * @property {string} observationType
 * @property {string} observedAt - ISO-8601
 * @property {Record<string, unknown>} [payload]
 * @property {string} [venue]
 * @property {string} [type] - alias for observationType (raw market form)
 * @property {string} [timestamp] - alias for observedAt (raw market form)
 */

/**
 * @typedef {object} ReplayStep
 * @property {ImmutableObservation} observation
 * @property {unknown} generatedEvidence
 * @property {unknown} affectedClaims
 * @property {object[]} confidenceChanges
 * @property {object|null} recommendation
 * @property {object} reasoningTrace
 * @property {number|null} confidence
 * @property {object|null} ranked
 */

/**
 * @typedef {object} ReplayRunInput
 * @property {string} subjectId
 * @property {string} [startTime]
 * @property {string} [endTime]
 * @property {DomainPackId|object} ontology
 * @property {DomainPackId|object} strategyPack
 * @property {string} [runtimeVersion]
 * @property {ImmutableObservation[]|object[]} [observations]
 * @property {(args: { subjectId: string, startTime?: string, endTime?: string }) => Promise<object[]>|object[]} [observationLoader]
 */

/**
 * @typedef {object} ReplayRunResult
 * @property {string} subjectId
 * @property {string|null} startTime
 * @property {string|null} endTime
 * @property {ImmutableObservation[]} observations
 * @property {unknown} evidence
 * @property {unknown} claims
 * @property {number|null} confidence
 * @property {object[]} recommendations
 * @property {object|null} explanation
 * @property {object} reasoningTrace
 * @property {ReplayStep[]} steps
 * @property {ReplayVersions} versions
 */

const REPLAY_RULES = Object.freeze({
  IMMUTABLE_OBSERVATIONS_ONLY: 'replay_only_consumes_immutable_observations',
  NEVER_MODIFY_HISTORY: 'replay_never_modifies_history',
  REGENERATE_REASONING: 'replay_regenerates_reasoning',
  DETERMINISTIC: 'replay_is_deterministic',
  EXPLAINABLE: 'replay_is_explainable',
});

const DEFAULT_RUNTIME_VERSION = '1.0.0';

module.exports = {
  REPLAY_RULES,
  DEFAULT_RUNTIME_VERSION,
};
