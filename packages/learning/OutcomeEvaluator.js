'use strict';

const { OUTCOME_VERDICTS } = require('./types');

/**
 * OutcomeEvaluator — classify claim vs outcome once reality is known (SPEC-021).
 *
 * Determines: correct | incorrect | partially_correct | unresolved.
 * Never mutates claims, evidence, or outcomes.
 */
class OutcomeEvaluator {
  /**
   * @param {object} [opts]
   * @param {number} [opts.partialThreshold=0.5] - minimum partialScore to count as partial credit
   */
  constructor(opts = {}) {
    this._partialThreshold =
      opts.partialThreshold != null ? Number(opts.partialThreshold) : 0.5;
  }

  /**
   * Evaluate a claim against an outcome record.
   *
   * @param {object} args
   * @param {import('./types').ClaimRef|object} args.claim
   * @param {import('./types').OutcomeRef|object} args.outcome
   * @returns {import('./types').EvaluationRecord}
   */
  evaluate(args = {}) {
    const claim = args.claim || {};
    const outcome = args.outcome || {};
    const claimId = claimIdOf(claim, outcome);
    if (!claimId) {
      throw new Error('OutcomeEvaluator.evaluate requires claim.id or outcome.claimId');
    }

    const verdict = resolveVerdict(outcome, this._partialThreshold);
    const credit = creditFor(verdict, outcome);
    const observationsConsidered = collectObservations(claim, outcome);
    const confidenceBefore = normalizeConfidence(claim.confidence);

    const record = Object.freeze({
      id:
        args.id ||
        `eval:${claimId}:${outcome.id || outcome.observedAt || 'unknown'}`,
      claimId: String(claimId),
      claimType: claimTypeOf(claim, outcome),
      subjectId: subjectOf(claim, outcome),
      strategyPack: strategyOf(claim, outcome),
      confidenceBefore,
      verdict,
      credit,
      outcomeId: outcome.id != null ? String(outcome.id) : null,
      observedAt: outcome.observedAt != null ? String(outcome.observedAt) : null,
      observationsConsidered: Object.freeze(observationsConsidered.slice()),
      explanation: explainVerdict(verdict, claim, outcome, credit),
      mutatesHistory: false,
      mutatesReplay: false,
      mutatesRuntime: false,
    });

    return record;
  }

  /**
   * Evaluate many claim/outcome pairs.
   * @param {Array<{ claim: object, outcome: object }>} pairs
   * @returns {import('./types').EvaluationRecord[]}
   */
  evaluateMany(pairs) {
    return (pairs || []).map((pair) => this.evaluate(pair));
  }
}

/**
 * @param {import('./types').OutcomeRef} outcome
 * @param {number} partialThreshold
 * @returns {import('./types').OutcomeVerdict}
 */
function resolveVerdict(outcome, partialThreshold) {
  if (outcome == null) return OUTCOME_VERDICTS.UNRESOLVED;

  const explicit = outcome.verdict || outcome.result || outcome.status || null;
  if (explicit != null) {
    const normalized = String(explicit).toLowerCase().replace(/[\s-]/g, '_');
    if (
      normalized === 'correct' ||
      normalized === 'true' ||
      normalized === 'success' ||
      normalized === 'successful' ||
      normalized === 'confirmed'
    ) {
      return OUTCOME_VERDICTS.CORRECT;
    }
    if (
      normalized === 'incorrect' ||
      normalized === 'false' ||
      normalized === 'failure' ||
      normalized === 'unsuccessful' ||
      normalized === 'refuted'
    ) {
      return OUTCOME_VERDICTS.INCORRECT;
    }
    if (
      normalized === 'partially_correct' ||
      normalized === 'partial' ||
      normalized === 'partiallycorrect'
    ) {
      return OUTCOME_VERDICTS.PARTIALLY_CORRECT;
    }
    if (
      normalized === 'unresolved' ||
      normalized === 'pending' ||
      normalized === 'unknown' ||
      normalized === 'inconclusive'
    ) {
      return OUTCOME_VERDICTS.UNRESOLVED;
    }
  }

  if (typeof outcome.correct === 'boolean') {
    return outcome.correct
      ? OUTCOME_VERDICTS.CORRECT
      : OUTCOME_VERDICTS.INCORRECT;
  }

  if (outcome.partialScore != null) {
    const score = Number(outcome.partialScore);
    if (!Number.isFinite(score)) return OUTCOME_VERDICTS.UNRESOLVED;
    if (score >= 1) return OUTCOME_VERDICTS.CORRECT;
    if (score <= 0) return OUTCOME_VERDICTS.INCORRECT;
    if (score >= partialThreshold) return OUTCOME_VERDICTS.PARTIALLY_CORRECT;
    return OUTCOME_VERDICTS.INCORRECT;
  }

  // No reality signal → unresolved (do not invent correctness).
  return OUTCOME_VERDICTS.UNRESOLVED;
}

/**
 * @param {import('./types').OutcomeVerdict} verdict
 * @param {import('./types').OutcomeRef} outcome
 */
function creditFor(verdict, outcome) {
  switch (verdict) {
    case OUTCOME_VERDICTS.CORRECT:
      return 1;
    case OUTCOME_VERDICTS.INCORRECT:
      return 0;
    case OUTCOME_VERDICTS.PARTIALLY_CORRECT: {
      const score = Number(outcome.partialScore);
      if (Number.isFinite(score) && score > 0 && score < 1) return score;
      return 0.5;
    }
    case OUTCOME_VERDICTS.UNRESOLVED:
    default:
      return null;
  }
}

function claimIdOf(claim, outcome) {
  return (
    (claim && (claim.id || claim.claimId || claim.claimType || claim.type)) ||
    (outcome && (outcome.claimId || outcome.claimType)) ||
    null
  );
}

function claimTypeOf(claim, outcome) {
  const value =
    (claim && (claim.claimType || claim.type || claim.strategy)) ||
    (outcome && (outcome.claimType || outcome.claim_type)) ||
    null;
  return value != null ? String(value) : null;
}

function subjectOf(claim, outcome) {
  const value =
    (claim && (claim.subjectId || claim.subject)) ||
    (outcome && (outcome.subjectId || outcome.subject)) ||
    null;
  return value != null ? String(value) : null;
}

function strategyOf(claim, outcome) {
  const value =
    (claim && (claim.strategyPack || claim.pack || claim.strategy)) ||
    (outcome && (outcome.strategyPack || outcome.pack)) ||
    null;
  return value != null ? String(value) : null;
}

function collectObservations(claim, outcome) {
  const fromClaim = []
    .concat((claim && claim.observations) || [])
    .concat((claim && claim.supportingEvidence) || [])
    .concat((claim && claim.evidence) || []);
  const fromOutcome = []
    .concat((outcome && outcome.observations) || [])
    .concat((outcome && outcome.evidence) || []);
  const seen = new Set();
  const out = [];
  for (const item of fromClaim.concat(fromOutcome)) {
    const key =
      item && typeof item === 'object'
        ? String(item.id || item.observationId || JSON.stringify(item))
        : String(item);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

function normalizeConfidence(raw) {
  if (raw == null) return null;
  let n = Number(raw);
  if (!Number.isFinite(n)) return null;
  if (n > 1 && n <= 100) n = n / 100;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

function explainVerdict(verdict, claim, outcome, credit) {
  const claimLabel =
    (claim && (claim.statement || claim.claimType || claim.id)) || 'claim';
  const outcomeLabel =
    (outcome && (outcome.statement || outcome.outcomeType || outcome.id)) ||
    'outcome';
  switch (verdict) {
    case OUTCOME_VERDICTS.CORRECT:
      return `Claim "${claimLabel}" matched outcome "${outcomeLabel}" (correct).`;
    case OUTCOME_VERDICTS.INCORRECT:
      return `Claim "${claimLabel}" did not match outcome "${outcomeLabel}" (incorrect).`;
    case OUTCOME_VERDICTS.PARTIALLY_CORRECT:
      return `Claim "${claimLabel}" partially matched outcome "${outcomeLabel}" (credit=${credit}).`;
    case OUTCOME_VERDICTS.UNRESOLVED:
    default:
      return `Outcome not yet decisive for claim "${claimLabel}" (unresolved).`;
  }
}

/**
 * @param {object} [opts]
 * @returns {OutcomeEvaluator}
 */
function createOutcomeEvaluator(opts) {
  return new OutcomeEvaluator(opts);
}

module.exports = {
  OutcomeEvaluator,
  createOutcomeEvaluator,
  resolveVerdict,
  creditFor,
  normalizeConfidence,
};
