'use strict';

require('dotenv').config();

const {
  createCogEngine,
  createStubAskFn,
  createMaxAskFn,
  listSuites,
  listDomains,
  getSuite,
} = require('../packages/cog');
const { tokenizeArgs, assertAllowed } = require('../utils/maxCli');

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, {
    values: ['--suite', '--domain', '--store'],
    flags: ['--text', '--score', '--stub', '--max', '--check', '--list-suites', '--list-domains'],
  });

  return {
    suiteId: parsed.values.get('--suite') || 'COG-001',
    domainId: parsed.values.get('--domain') || null,
    storePath: parsed.values.get('--store') || null,
    text: parsed.flags.has('--text'),
    score: parsed.flags.has('--score'),
    stub: parsed.flags.has('--stub'),
    useMax: parsed.flags.has('--max'),
    check: parsed.flags.has('--check'),
    listSuites: parsed.flags.has('--list-suites'),
    listDomains: parsed.flags.has('--list-domains'),
  };
}

async function run(options = parseArgs()) {
  if (options.listSuites) {
    return { suites: listSuites() };
  }
  if (options.listDomains) {
    return {
      domains: listDomains().map(d => ({
        id: d.id,
        shortName: d.shortName,
        objective: d.objective,
      })),
    };
  }

  const engine = createCogEngine({
    storeOptions: options.storePath ? { storePath: options.storePath } : undefined,
  });

  let askFn;
  if (options.stub) {
    askFn = createStubAskFn((question, turnIndex) =>
      `[stub turn ${turnIndex}] Acknowledged: ${question.slice(0, 80)}`
    );
  } else if (options.useMax) {
    askFn = await createMaxAskFn({ disableLlm: true, missionsEnabled: false });
  } else {
    askFn = createStubAskFn((question, turnIndex) =>
      `[deterministic stub turn ${turnIndex}] Processing: ${question.slice(0, 60)}`
    );
  }

  engine.setAskFn(askFn);

  if (options.domainId) {
    const result = await engine.runDomain(options.domainId, { score: options.score, askFn });
    return { mode: 'domain', domainId: options.domainId, result };
  }

  const suite = getSuite(options.suiteId);
  if (!suite) {
    throw new Error(`Unknown suite: ${options.suiteId}. Use --list-suites.`);
  }

  const runResult = await engine.runSuite(options.suiteId, { score: options.score, askFn });
  const report = engine.getReport(runResult.runId);

  const output = {
    mode: 'suite',
    run: runResult,
    regression: report.regression,
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
  run(parseArgs())
    .then(result => {
      if (result.text) {
        console.log(result.text);
        console.log('');
      }
      console.log(JSON.stringify(result, null, 2));
      process.exitCode = result.checkFailed ? 1 : 0;
    })
    .catch(err => {
      console.error(err.stack || err.message);
      process.exitCode = 1;
    });
}
