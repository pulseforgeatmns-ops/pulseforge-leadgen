'use strict';

const { detectRegressions } = require('./RegressionDetector');
const { getDomain } = require('../domains');

const DOMAIN_SHORT_NAMES = {
  'COG-101': 'Identity',
  'COG-102': 'Conversation',
  'COG-103': 'Assumptions',
  'COG-104': 'Counterfactual',
  'COG-105': 'Revision',
  'COG-106': 'Evidence',
  'COG-107': 'Abstraction',
  'COG-108': 'Confidence',
  'COG-109': 'Long Conversation',
  'COG-110': 'Reasoning Graph',
};

/**
 * @param {import('../types').CogRunResult} run
 * @param {object} [regression]
 * @returns {string}
 */
function formatRunReportText(run, regression = null) {
  const lines = [];
  lines.push(`COG Run Report`);
  lines.push(`Run ID: ${run.runId}`);
  lines.push(`Suite: ${run.suiteId} v${run.suiteVersion}`);
  lines.push(`COG Framework: v${run.cogVersion}`);
  lines.push(`Status: ${run.status}`);
  lines.push(`Started: ${run.startedAt}`);
  if (run.completedAt) lines.push(`Completed: ${run.completedAt}`);
  lines.push('');

  const overall = run.overallScore !== null ? String(run.overallScore) : '— (not scored)';
  lines.push(`COG Overall: ${overall}`);
  lines.push('');

  for (const domain of run.domains) {
    const shortName = DOMAIN_SHORT_NAMES[domain.domainId] || domain.domainId;
    const pad = '.'.repeat(Math.max(1, 20 - shortName.length));
    const score = domain.score !== null ? String(domain.score) : '—';
    const failCount = domain.failures.length;
    const suffix = failCount > 0 ? ` (${failCount} failures)` : '';
    lines.push(`${shortName} ${pad} ${score}${suffix}`);
  }

  if (regression?.hasRegression) {
    lines.push('');
    lines.push('⚠ REGRESSION DETECTED');
    lines.push(`Baseline: ${regression.baselineRunId}`);
    if (regression.overallRegression) {
      const o = regression.overallRegression;
      lines.push(`Overall: ${o.baselineScore} → ${o.currentScore} (${o.delta})`);
    }
    for (const dr of regression.domainRegressions) {
      if (dr.type === 'score_regression') {
        lines.push(`  ${dr.domainId}: ${dr.baselineScore} → ${dr.currentScore}`);
      } else if (dr.type === 'behavior_regression') {
        lines.push(`  ${dr.domainId}: behaviors ${dr.baselinePassed} → ${dr.currentPassed} passed`);
      }
    }
    for (const nf of regression.newFailures) {
      lines.push(`  NEW ${nf.domainId}: ${nf.failure.code} ${nf.failure.label}`);
    }
  }

  return lines.join('\n');
}

/**
 * Per-domain trend across multiple runs.
 * @param {import('../types').CogRunResult[]} runs - Chronological (oldest first)
 */
function buildTrendReport(runs) {
  const domainIds = new Set();
  for (const run of runs) {
    for (const d of run.domains) domainIds.add(d.domainId);
  }

  const trends = {};
  for (const domainId of domainIds) {
    const points = runs.map(run => {
      const d = run.domains.find(x => x.domainId === domainId);
      return {
        runId: run.runId,
        startedAt: run.startedAt,
        suiteVersion: run.suiteVersion,
        score: d?.score ?? null,
        failureCount: d?.failures?.length ?? 0,
        status: d?.status,
      };
    });
    trends[domainId] = {
      domainId,
      shortName: DOMAIN_SHORT_NAMES[domainId] || domainId,
      objective: getDomain(domainId)?.objective || null,
      points,
    };
  }

  return {
    runCount: runs.length,
    oldestRun: runs[0]?.startedAt || null,
    newestRun: runs[runs.length - 1]?.startedAt || null,
    trends,
  };
}

/**
 * @param {import('../types').CogRunResult} run
 * @param {import('../results/ResultStore').ResultStore} store
 */
function buildFullReport(run, store) {
  const baseline = store.getPreviousRun(run);
  const regression = detectRegressions(run, baseline);
  const history = store.listRuns({ suiteId: run.suiteId });
  const trend = buildTrendReport(history.reverse());

  return {
    run,
    regression,
    trend,
    text: formatRunReportText(run, regression),
  };
}

module.exports = {
  DOMAIN_SHORT_NAMES,
  formatRunReportText,
  buildTrendReport,
  buildFullReport,
};
