'use strict';

const {
  buildTrainingRecord,
  formatTrainingRecordText,
  assertRegressionSuite,
  buildRegressionSuite,
} = require('../packages/max/training');
const { assertAllowed, tokenizeArgs } = require('../utils/maxCli');

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, { values: [], flags: ['--text', '--regression', '--check'] });
  return {
    text: parsed.flags.has('--text'),
    regression: parsed.flags.has('--regression'),
    check: parsed.flags.has('--check'),
  };
}

function run(options = parseArgs()) {
  if (options.regression || options.check) {
    const suite = options.check ? assertRegressionSuite() : buildRegressionSuite();
    return suite;
  }

  const record = buildTrainingRecord();
  if (options.text) {
    return { ...record, text: formatTrainingRecordText(record) };
  }
  return record;
}

module.exports = { parseArgs, run };

if (require.main === module) {
  const options = parseArgs();
  try {
    const result = run(options);
    if (options.text && result.text) {
      console.log(result.text);
      console.log('');
      delete result.text;
    }
    console.log(JSON.stringify(result, null, 2));
    process.exitCode = options.check && result.ok === false ? 1 : 0;
  } catch (error) {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }
}
