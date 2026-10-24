'use strict';

const { resolvePeriodWindow } = require('./PeriodWindow');
const { BRIEFING_PERIODS } = require('../BriefingTypes');

/**
 * Digest Builder — multi-horizon period support for brief().
 * Thin wrapper around PeriodWindow; keeps digest API explicit.
 */
class DigestBuilder {
  /**
   * @param {object} input
   * @param {'daily'|'weekly'|'monthly'} [input.period]
   * @param {string} [input.asOf]
   * @param {string} [input.periodStart]
   * @param {string} [input.periodEnd]
   */
  buildWindow(input = {}) {
    return resolvePeriodWindow(input);
  }

  /**
   * Convenience: daily window ending at asOf.
   * @param {string} [asOf]
   */
  daily(asOf) {
    return this.buildWindow({ period: BRIEFING_PERIODS.DAILY, asOf });
  }

  /**
   * @param {string} [asOf]
   */
  weekly(asOf) {
    return this.buildWindow({ period: BRIEFING_PERIODS.WEEKLY, asOf });
  }

  /**
   * @param {string} [asOf]
   */
  monthly(asOf) {
    return this.buildWindow({ period: BRIEFING_PERIODS.MONTHLY, asOf });
  }
}

module.exports = {
  DigestBuilder,
  BRIEFING_PERIODS,
};
