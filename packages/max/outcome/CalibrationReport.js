'use strict';

const {
  CONFIDENCE_BANDS,
  OUTCOME_RESULTS,
  bandForConfidence,
} = require('./OutcomeTypes');

/**
 * Confidence calibration reports (SPEC-013).
 * Empirically grounds confidence bands — never mutates recommendation.confidence.
 */

/**
 * @param {object} input
 * @param {object[]} input.records - latest RecommendationOutcome rows
 * @returns {object}
 */
function buildCalibrationReport(input = {}) {
  const records = input.records || [];
  const observed = records.filter(
    (r) =>
      r.outcome === OUTCOME_RESULTS.SUCCESSFUL ||
      r.outcome === OUTCOME_RESULTS.UNSUCCESSFUL ||
      r.outcome === OUTCOME_RESULTS.INCONCLUSIVE
  );

  const bands = CONFIDENCE_BANDS.map((band) => {
    const inBand = observed.filter(
      (r) => bandForConfidence(r.confidenceAtRecommendation) === band.id
    );
    const successful = inBand.filter(
      (r) => r.outcome === OUTCOME_RESULTS.SUCCESSFUL
    );
    const unsuccessful = inBand.filter(
      (r) => r.outcome === OUTCOME_RESULTS.UNSUCCESSFUL
    );
    const inconclusive = inBand.filter(
      (r) => r.outcome === OUTCOME_RESULTS.INCONCLUSIVE
    );
    const decisive = successful.length + unsuccessful.length;
    return {
      band: band.id,
      min: band.min,
      max: band.max,
      observed: inBand.length,
      successful: successful.length,
      unsuccessful: unsuccessful.length,
      inconclusive: inconclusive.length,
      /** Historical success rate among decisive outcomes (excludes inconclusive). */
      successRate: decisive === 0 ? null : round3(successful.length / decisive),
      /** Including inconclusive as non-success. */
      successRateInclusive:
        inBand.length === 0
          ? null
          : round3(successful.length / inBand.length),
    };
  });

  const promoted = records.filter((r) => r.promotedFromWatch === true);
  const promotedObserved = promoted.filter((r) => r.outcome != null);
  const promotedSuccessful = promotedObserved.filter(
    (r) => r.outcome === OUTCOME_RESULTS.SUCCESSFUL
  );

  return {
    generatedAt: new Date().toISOString(),
    sampleSize: observed.length,
    bands,
    /**
     * How often 70–80 confidence recommendations later succeed (promotion / watch → action).
     * Uses band 70-79 + promotedFromWatch when available.
     */
    midConfidencePromotion: {
      band70to79: bands.find((b) => b.band === '70-79') || null,
      promotedFromWatch: {
        total: promoted.length,
        observed: promotedObserved.length,
        successful: promotedSuccessful.length,
        successRate:
          promotedObserved.length === 0
            ? null
            : round3(promotedSuccessful.length / promotedObserved.length),
      },
    },
    narrative: narrativeFor(bands),
    /** Explicit: this report does not alter live confidence scores. */
    mutatesConfidence: false,
    customerFacing: false,
  };
}

function narrativeFor(bands) {
  const high = bands.find((b) => b.band === '90+');
  if (high && high.successRate != null) {
    return `Historically, recommendations at 90+ confidence have succeeded ${Math.round(high.successRate * 100)}% of the time (${high.successful}/${high.successful + high.unsuccessful} decisive).`;
  }
  if (high && high.observed === 0) {
    return 'Insufficient observed outcomes in the 90+ band for empirical calibration.';
  }
  return 'Calibration pending — record Observed → Successful/Unsuccessful outcomes to ground confidence.';
}

function round3(n) {
  return Math.round(n * 1000) / 1000;
}

module.exports = { buildCalibrationReport };
