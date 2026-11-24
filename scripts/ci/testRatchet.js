#!/usr/bin/env node
'use strict';

/**
 * MAX-P0-002 — known-failure ratchet for suites that are not yet green.
 *
 * Runs each test file with node:test (TAP), keys every failing test as
 * "<file> :: <test name>", and compares the set with a reviewed baseline.
 *
 *   exit 0  no failure outside the baseline (fixed baseline entries are reported)
 *   exit 1  at least one failure that is not in the baseline (a regression)
 *   exit 2  usage error, or no test files matched
 *
 * The baseline is a ratchet, not a quarantine: tests still run and their
 * results are printed. Removing an entry (after a fix) is always allowed;
 * adding one requires a reviewed change to the baseline file.
 *
 * Usage:
 *   node scripts/ci/testRatchet.js --baseline <file.json> <test files...>
 *   node scripts/ci/testRatchet.js --baseline <file.json> --write <test files...>
 */

const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

function parseArgs(argv) {
  const out = { baseline: null, write: false, files: [] };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--baseline') out.baseline = argv[++i];
    else if (arg === '--write') out.write = true;
    else out.files.push(arg);
  }
  return out;
}

/** Extract failing test names from node:test TAP output (nested subtests included). */
function failingTests(tap) {
  const names = [];
  const stack = [];
  for (const line of String(tap).split('\n')) {
    const sub = /^(\s*)# Subtest: (.*)$/.exec(line);
    if (sub) {
      const depth = sub[1].length / 4;
      stack.length = depth;
      stack[depth] = sub[2].trim();
      continue;
    }
    const m = /^(\s*)not ok \d+ - (.*?)(?:\s+#\s+(?:SKIP|TODO).*)?$/.exec(line);
    if (!m) continue;
    if (/#\s+TODO/.test(line)) continue;
    const depth = m[1].length / 4;
    const parents = stack.slice(0, depth).filter(Boolean);
    names.push([...parents, m[2].trim()].join(' > '));
  }
  return names;
}

function runFile(file) {
  const result = spawnSync(process.execPath, ['--test', '--test-reporter=tap', file], {
    encoding: 'utf8',
    env: process.env,
    maxBuffer: 64 * 1024 * 1024,
    timeout: 10 * 60 * 1000,
  });
  const tap = `${result.stdout || ''}\n${result.stderr || ''}`;
  const failures = failingTests(tap);
  if (result.status !== 0 && failures.length === 0) {
    failures.push(`<file exited ${result.status ?? result.signal}>`);
  }
  return failures.map(name => `${file} :: ${name}`);
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.baseline || args.files.length === 0) {
    console.error('usage: testRatchet.js --baseline <file.json> [--write] <test files...>');
    return 2;
  }
  const files = [...new Set(args.files.map(f => path.relative(process.cwd(), path.resolve(f))))].sort();
  const observed = new Set();
  for (const file of files) for (const key of runFile(file)) observed.add(key);

  if (args.write) {
    const body = { generatedAt: new Date().toISOString(), files: files.length, knownFailures: [...observed].sort() };
    fs.writeFileSync(args.baseline, `${JSON.stringify(body, null, 2)}\n`);
    console.log(`wrote ${observed.size} known failures to ${args.baseline}`);
    return 0;
  }

  const baseline = new Set(JSON.parse(fs.readFileSync(args.baseline, 'utf8')).knownFailures || []);
  const regressions = [...observed].filter(k => !baseline.has(k)).sort();
  const fixed = [...baseline].filter(k => !observed.has(k) && files.some(f => k.startsWith(`${f} :: `))).sort();

  console.log(`files: ${files.length}; failing: ${observed.size}; baseline: ${baseline.size}`);
  if (fixed.length) {
    console.log(`\nFixed (remove from baseline): ${fixed.length}`);
    for (const k of fixed) console.log(`  + ${k}`);
  }
  if (regressions.length) {
    console.error(`\nNEW FAILURES (not in baseline): ${regressions.length}`);
    for (const k of regressions) console.error(`  - ${k}`);
    return 1;
  }
  console.log('\nNo failures outside the reviewed baseline.');
  return 0;
}

if (require.main === module) process.exitCode = main();

module.exports = { failingTests, parseArgs };
