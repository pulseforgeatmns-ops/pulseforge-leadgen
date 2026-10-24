'use strict';

require('dotenv').config();

const { createCogEngine, buildTrendReport } = require('../packages/cog');
const { tokenizeArgs, assertAllowed } = require('../utils/maxCli');

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, {
    values: ['--run', '--suite', '--store', '--compare'],
    flags: ['--text', '--trend', '--check'],
  });

  return {
    runId: parsed.values.get('--run') || null,
    suiteId: parsed.values.get('--suite') || 'COG-001',
    storePath: parsed.values.get('--store') || null,
    text: parsed.flags.has('--text'),
    trend: parsed.flags.has('--trend'),
    compareRunId: parsed.values.get('--compare') || null,
    check: parsed.flags.has('--check'),
  };
}

function run(options = parseArgs()) {
  const engine = createCogEngine({
    storeOptions: options.storePath ? { storePath: options.storePath } : undefined,
  });

  if (options.trend) {
    const runs = engine.listRuns({ suiteId: options.suiteId });
    const chronological = [...runs].reverse();
    const trend = buildTrendReport(chronological);
    return { mode: 'trend', suiteId: options.suiteId, trend };
  }

  if (options.compareRunId && options.runId) {
    const comparison = engine.compareRuns(options.runId, options.compareRunId);
    return {
      mode: 'compare',
      currentRunId: options.runId,
      baselineRunId: options.compareRunId,
      comparison,
      ok: !comparison.hasRegression,
    };
  }

  const latest = options.runId
    ? engine.store.getRun(options.runId)
    : engine.listRuns({ suiteId: options.suiteId, limit: 1 })[0];

  if (!latest) {
    throw new Error(`No COG runs found for suite ${options.suiteId}. Run: npm run cog:run`);
  }

  const report = engine.getReport(latest.runId);
  const output = {
    mode: 'report',
    runId: latest.runId,
    report: {
      run: report.run,
      regression: report.regression,
      trend: report.trend,
    },
    ok: !report.regression.hasRegression,
  };

  if (options.text) {
    output.text = report.text;
  }

  if (options.check && report.regression.hasRegression) {
    output.checkFailed = true;
  }

  return output;
}

module.exports = { parseArgs, run };

if (require.main === module) {
  try {
    const result = run(parseArgs());
    if (result.text) {
      console.log(result.text);
      console.log('');
    }
    console.log(JSON.stringify(result, null, 2));
    process.exitCode = result.checkFailed ? 1 : 0;
  } catch (err) {
    console.error(err.stack || err.message);
    process.exitCode = 1;
  }
}
