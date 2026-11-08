'use strict';

const { buildScenarios } = require('./scenarios');
const { runCaseOnEngine, runCaseOnResponder } = require('./runner');
const { gradeRuns, summarize } = require('./graders');
const { NEGATIVE_CONTROLS, oracle } = require('./controls');

async function runSuite({ responder = null, filter = null } = {}) {
  const cases = buildScenarios().filter((c) => !filter || filter(c));
  const runs = [];
  for (const c of cases) {
    runs.push(responder ? await runCaseOnResponder(c, responder) : await runCaseOnEngine(c));
  }
  const grades = gradeRuns(runs);
  return { runs, grades, summary: summarize(grades) };
}

module.exports = { runSuite, buildScenarios, NEGATIVE_CONTROLS, oracle };
