'use strict';

const { LIFECYCLE, OUTCOME_RESULTS } = require('./OutcomeTypes');

/**
 * Drift detection for Outcome Intelligence (SPEC-013).
 * Alerts Pulseforge engineers — never customers.
 */

const DEFAULT_RECENT_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_BASELINE_MS = 30 * 24 * 60 * 60 * 1000;

/**
 * @param {object} input
 * @param {object[]} input.records
 * @param {object} [input.operatorQuality] - optional SPEC-012 quality snapshot
 * @param {number} [input.now]
 * @param {number} [input.recentMs]
 * @param {number} [input.baselineMs]
 * @param {number} [input.minSample=3]
 */
function detectDrift(input = {}) {
  const now = input.now != null ? Number(input.now) : Date.now();
  const recentMs = input.recentMs != null ? Number(input.recentMs) : DEFAULT_RECENT_MS;
  const baselineMs =
    input.baselineMs != null ? Number(input.baselineMs) : DEFAULT_BASELINE_MS;
  const minSample = input.minSample != null ? Number(input.minSample) : 3;
  const records = input.records || [];

  const recentCutoff = now - recentMs;
  const baselineCutoff = now - baselineMs;

  const recent = records.filter((r) => ts(r) >= recentCutoff);
  const baseline = records.filter(
    (r) => ts(r) >= baselineCutoff && ts(r) < recentCutoff
  );

  /** @type {object[]} */
  const alerts = [];

  const recentSuccess = successRate(recent);
  const baselineSuccess = successRate(baseline);
  if (
    recentSuccess.sample >= minSample &&
    baselineSuccess.sample >= minSample &&
    baselineSuccess.rate != null &&
    recentSuccess.rate != null &&
    recentSuccess.rate < baselineSuccess.rate - 0.15
  ) {
    alerts.push({
      type: 'strategy_underperforming',
      severity: 'high',
      message: `Overall success rate dropped from ${pct(baselineSuccess.rate)} to ${pct(recentSuccess.rate)}`,
      recent: recentSuccess,
      baseline: baselineSuccess,
    });
  }

  const recentFp = falsePositiveRate(recent);
  const baselineFp = falsePositiveRate(baseline);
  if (
    recentFp.sample >= minSample &&
    baselineFp.sample >= minSample &&
    baselineFp.rate != null &&
    recentFp.rate != null &&
    recentFp.rate > baselineFp.rate + 0.15
  ) {
    alerts.push({
      type: 'false_positives_increasing',
      severity: 'high',
      message: `False positive rate rose from ${pct(baselineFp.rate)} to ${pct(recentFp.rate)}`,
      recent: recentFp,
      baseline: baselineFp,
    });
  }

  // Per-strategy underperformance
  const strategyIds = new Set(
    records.map((r) => r.strategyId).filter(Boolean)
  );
  for (const sid of strategyIds) {
    const rS = successRate(recent.filter((r) => r.strategyId === sid));
    const bS = successRate(baseline.filter((r) => r.strategyId === sid));
    if (
      rS.sample >= minSample &&
      bS.sample >= minSample &&
      bS.rate != null &&
      rS.rate != null &&
      rS.rate < bS.rate - 0.2
    ) {
      alerts.push({
        type: 'strategy_underperforming',
        severity: 'medium',
        strategyId: sid,
        message: `Strategy ${sid} success rate dropped from ${pct(bS.rate)} to ${pct(rS.rate)}`,
        recent: rS,
        baseline: bS,
      });
    }
  }

  // Evidence source quality: unsuccessful outcomes with shared evidence sources
  const sourceHits = {};
  for (const r of recent) {
    if (r.outcome !== OUTCOME_RESULTS.UNSUCCESSFUL) continue;
    for (const src of r.evidenceSourceIds || []) {
      sourceHits[src] = (sourceHits[src] || 0) + 1;
    }
  }
  for (const [src, count] of Object.entries(sourceHits)) {
    if (count >= minSample) {
      alerts.push({
        type: 'evidence_source_quality_dropping',
        severity: 'medium',
        evidenceSourceId: src,
        message: `Evidence source ${src} appeared in ${count} recent unsuccessful outcomes`,
        count,
      });
    }
  }

  // Operator acceptance falling (from SPEC-012 quality if provided)
  const oq = input.operatorQuality;
  if (
    oq &&
    oq.recommendationAcceptanceRate != null &&
    Number(oq.recommendationAcceptanceRate) < 0.35 &&
    oq.totals &&
    Number(oq.totals.decided) >= minSample
  ) {
    alerts.push({
      type: 'recommendation_acceptance_falling',
      severity: 'medium',
      message: `Operator acceptance rate is ${pct(oq.recommendationAcceptanceRate)} (${oq.totals.decided} decided)`,
      acceptanceRate: oq.recommendationAcceptanceRate,
    });
  }

  // Watch alerts arriving too early
  const earlyWatches = recent.filter((r) => r.watchAlertEarly === true);
  if (earlyWatches.length >= minSample) {
    const earlyUnsuccessful = earlyWatches.filter(
      (r) => r.outcome === OUTCOME_RESULTS.UNSUCCESSFUL
    );
    if (earlyUnsuccessful.length >= Math.ceil(earlyWatches.length * 0.5)) {
      alerts.push({
        type: 'watch_alerts_too_early',
        severity: 'low',
        message: `${earlyUnsuccessful.length}/${earlyWatches.length} early watch alerts ended unsuccessful`,
        earlyCount: earlyWatches.length,
      });
    }
  }

  // Overflow too conservative: high generated, low promotion, low executed
  const overflow = recent.filter((r) => r.strategyId === 'overflow');
  if (overflow.length >= minSample) {
    const promoted = overflow.filter((r) => r.promotedFromWatch).length;
    const executed = overflow.filter((r) => r.executed).length;
    if (promoted / overflow.length < 0.1 && executed / overflow.length < 0.15) {
      alerts.push({
        type: 'overflow_too_conservative',
        severity: 'low',
        message: `Overflow promotion ${pct(promoted / overflow.length)} and execution ${pct(executed / overflow.length)} look conservative`,
        generated: overflow.length,
        promoted,
        executed,
      });
    }
  }

  return {
    generatedAt: new Date().toISOString(),
    window: {
      recentMs,
      baselineMs,
      recentCount: recent.length,
      baselineCount: baseline.length,
    },
    alerts,
    alertCount: alerts.length,
    customerFacing: false,
  };
}

function ts(row) {
  const raw =
    row.observedAt || row.executedAt || row.generatedAt || row.recordedAt;
  const t = new Date(raw).getTime();
  return Number.isFinite(t) ? t : 0;
}

function successRate(rows) {
  const decisive = rows.filter(
    (r) =>
      r.outcome === OUTCOME_RESULTS.SUCCESSFUL ||
      r.outcome === OUTCOME_RESULTS.UNSUCCESSFUL
  );
  const successful = decisive.filter(
    (r) => r.outcome === OUTCOME_RESULTS.SUCCESSFUL
  );
  return {
    sample: decisive.length,
    successful: successful.length,
    rate:
      decisive.length === 0
        ? null
        : round3(successful.length / decisive.length),
  };
}

function falsePositiveRate(rows) {
  const executed = rows.filter(
    (r) =>
      r.executed ||
      r.lifecycle === LIFECYCLE.EXECUTED ||
      r.outcome != null
  );
  const unsuccessful = executed.filter(
    (r) => r.outcome === OUTCOME_RESULTS.UNSUCCESSFUL
  );
  return {
    sample: executed.length,
    unsuccessful: unsuccessful.length,
    rate:
      executed.length === 0
        ? null
        : round3(unsuccessful.length / executed.length),
  };
}

function pct(rate) {
  return `${Math.round(Number(rate) * 100)}%`;
}

function round3(n) {
  return Math.round(n * 1000) / 1000;
}

module.exports = { detectDrift, DEFAULT_RECENT_MS, DEFAULT_BASELINE_MS };
