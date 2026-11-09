#!/usr/bin/env node
'use strict';

/**
 * Max semantic generalization evaluation — CLI.
 *
 *   node scripts/maxSemanticEval.js                 # engine run, writes report
 *   node scripts/maxSemanticEval.js --controls      # also grade negative/positive controls
 *   node scripts/maxSemanticEval.js --scenario A    # filter by scenario prefix
 *   node scripts/maxSemanticEval.js --judge         # + optional model judge (needs ANTHROPIC_API_KEY)
 *   node scripts/maxSemanticEval.js --out DIR       # default artifacts/max-semantic-eval
 *   node scripts/maxSemanticEval.js --write-baseline  # rewrite eval/semantic/baseline.json (review the diff!)
 *
 * Hermetic: no network, no model (unless --judge), fixture Postgres.
 */

const path = require('node:path');
const fs = require('node:fs');

const args = process.argv.slice(2);
const flag = (n) => args.includes(n);
const val = (n, d) => (args.includes(n) ? args[args.indexOf(n) + 1] : d);

// The judge is the only part allowed to reach the network.
if (!flag('--judge')) require('../test/helpers/denyWebsiteEvidenceNetwork.cjs');

async function main() {
  const saved = { info: console.info, warn: console.warn };
  console.info = () => {};
  console.warn = () => {};
  const { runSuite, NEGATIVE_CONTROLS, oracle } = require('../packages/max/workspace/eval/semantic');
  const { renderMarkdown } = require('../packages/max/workspace/eval/semantic/report');
  const scenario = val('--scenario', null);
  const filter = scenario
    ? (c) => String(c.scenario).startsWith(scenario) || String(c.scenario).startsWith(`E/${scenario}`)
    : null;
  const started = Date.now();
  const engine = await runSuite({ filter });
  const runtimeMs = Date.now() - started;
  const controls = {};
  if (flag('--controls')) {
    for (const [name, responder] of Object.entries({ ...NEGATIVE_CONTROLS, oracle })) {
      controls[name] = (await runSuite({ responder, filter })).summary;
    }
  }
  let judge = null;
  if (flag('--judge')) {
    const { judgeGrades } = require('../packages/max/workspace/eval/semantic/modelJudge');
    judge = await judgeGrades(engine.runs, engine.grades, { repeats: Number(val('--judge-repeats', 1)) });
  }
  Object.assign(console, saved);
  const out = val('--out', path.join(__dirname, '..', 'artifacts', 'max-semantic-eval'));
  fs.mkdirSync(out, { recursive: true });
  const payload = { generatedAt: new Date().toISOString(), runtimeMs, summary: engine.summary, controls, judge, grades: engine.grades };
  fs.writeFileSync(path.join(out, 'results.json'), JSON.stringify(payload, null, 2));
  fs.writeFileSync(path.join(out, 'results.md'), renderMarkdown(payload));
  if (flag('--write-baseline')) {
    if (scenario) throw new Error('--write-baseline requires the full suite (no --scenario)');
    const file = path.join(__dirname, '..', 'packages', 'max', 'workspace', 'eval', 'semantic', 'baseline.json');
    const prev = JSON.parse(fs.readFileSync(file, 'utf8'));
    const key = (g) => `${g.caseId} :: ${g.q}`;
    const next = {
      ...prev,
      passing: engine.grades.filter((g) => g.pass).map(key),
      semanticPassing: engine.grades.filter((g) => g.semanticPass).map(key),
      knownSafetyViolations: engine.grades.filter((g) => !g.safetyPass).map(key),
    };
    fs.writeFileSync(file, `${JSON.stringify(next, null, 2)}\n`);
    console.log(`baseline written: ${file}`);
  }
  const p = (x) => `${(x * 100).toFixed(1)}%`;
  console.log(`graded turns: ${engine.summary.n}  pass: ${p(engine.summary.pass)}  semantic: ${p(engine.summary.semantic)}  safety: ${p(engine.summary.safety)}  (${(runtimeMs / 1000).toFixed(1)}s)`);
  for (const [k, s] of Object.entries(controls)) console.log(`control ${k}: pass ${p(s.pass)} semantic ${p(s.semantic)}`);
  console.log(`report: ${path.join(out, 'results.md')}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
