#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const REA_VERSION = '6.4.0';
const REPO_ROOT = path.resolve(__dirname, '..');
const BRIEF_PATH = path.join(
  REPO_ROOT,
  'signal-v1',
  'rea',
  'investigations',
  'fomo-token-actionability.json'
);

function readBrief() {
  return JSON.parse(fs.readFileSync(BRIEF_PATH, 'utf8'));
}

function assertString(value, field) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`invalid_brief:${field}`);
  }
}

function validateBrief(brief) {
  if (brief.schemaVersion !== 1) throw new Error('invalid_brief:schemaVersion');
  for (const field of ['id', 'title', 'deadline', 'status', 'purpose']) {
    assertString(brief[field], field);
  }
  if (!Array.isArray(brief.questions) || brief.questions.length === 0) {
    throw new Error('invalid_brief:questions');
  }
  const ids = new Set();
  for (const [index, question] of brief.questions.entries()) {
    assertString(question.id, `questions[${index}].id`);
    assertString(question.text, `questions[${index}].text`);
    if (ids.has(question.id)) throw new Error(`invalid_brief:duplicate_question:${question.id}`);
    ids.add(question.id);
    if (!Array.isArray(question.requiredEvidence) || question.requiredEvidence.length === 0) {
      throw new Error(`invalid_brief:questions[${index}].requiredEvidence`);
    }
  }
  const requiredFalse = [
    'allowTrade',
    'allowWalletConnection',
    'allowWalletSignature',
    'allowMessageSend',
    'allowCredentialCapture',
    'allowEntitlementBypass',
    'feedProspectiveResearch'
  ];
  for (const field of requiredFalse) {
    if (brief.constraints?.[field] !== false) {
      throw new Error(`unsafe_brief:${field}_must_be_false`);
    }
  }
  if (!Array.isArray(brief.acceptance) || brief.acceptance.length < 3) {
    throw new Error('invalid_brief:acceptance');
  }
  return brief;
}

function runRea(args, spawn = spawnSync) {
  const result = spawn(
    'npx',
    ['-y', `rea-agents@${REA_VERSION}`, ...args],
    { cwd: REPO_ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }
  );
  if (result.error) throw result.error;
  if (result.status !== 0) {
    const detail = (result.stderr || result.stdout || '').trim();
    throw new Error(`rea_failed:${result.status}:${detail}`);
  }
  return result.stdout;
}

function doctor(spawn = spawnSync) {
  const raw = runRea(['doctor', '--client', 'codex', '--skill', '--format', 'json'], spawn);
  const report = JSON.parse(raw);
  if (report.healthy !== true) throw new Error('rea_not_ready');
  return {
    healthy: true,
    reaVersion: report.identity?.cli_package_version || REA_VERSION,
    skillVersion: report.identity?.expected_skill_version || null,
    node: report.scope_checks?.find(check => check.name === 'node')?.detail || null,
    note: 'Native decompilers are optional for browser inspection. Android code inspection still requires a caller-supplied JDK and headless JADX.'
  };
}

function print(value) {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

function main(argv = process.argv.slice(2)) {
  const command = argv[0] || 'check';
  if (command === 'check') {
    const brief = validateBrief(readBrief());
    print({ valid: true, id: brief.id, deadline: brief.deadline, status: brief.status });
    return;
  }
  if (command === 'brief') {
    print(validateBrief(readBrief()));
    return;
  }
  if (command === 'doctor') {
    print(doctor());
    return;
  }
  throw new Error(`unknown_command:${command}`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  }
}

module.exports = {
  REA_VERSION,
  BRIEF_PATH,
  readBrief,
  validateBrief,
  runRea,
  doctor,
  main
};
