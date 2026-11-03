#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const { evaluateCases } = require('./jevEvaluation');
function main(args) {
  if (args.length !== 1 || !args[0].endsWith('.json')) {
    process.stderr.write('Usage: node packages/decision-service/jevEvaluationCli.js <cases.json>\n');
    process.exitCode = 2;
    return;
  }
  try {
    const rows = JSON.parse(fs.readFileSync(args[0], 'utf8'));
    process.stdout.write(JSON.stringify(evaluateCases(rows), null, 2) + '\n');
  } catch (error) {
    process.stderr.write('JEV offline evaluation failed: ' + error.message + '\n');
    process.exitCode = 1;
  }
}
if (require.main === module) main(process.argv.slice(2));
module.exports = { main };
