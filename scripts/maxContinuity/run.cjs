#!/usr/bin/env node
'use strict';
// One-shot, nonpublic entry point. No Railway writes or deployment API calls.
const path = require('node:path');
const fs = require('node:fs');
const os = require('node:os');
const { spawn } = require('node:child_process');
const { stagingConfig, cleanEnvironment } = require('./policy.cjs');

async function runIsolated(config, entry = path.join(__dirname, 'workerEntry.cjs')) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'max-continuity-worker-'));
  try {
    return await new Promise(resolve => {
      const child = spawn(process.execPath, [entry, '--worker'], {
        cwd: directory, env: cleanEnvironment(config), stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
      });
      let report;
      let settled = false;
      const finish = value => { if (!settled) { settled = true; clearTimeout(timer); resolve(value); } };
      const timer = setTimeout(() => { child.kill('SIGKILL'); }, 90000);
      child.on('message', message => { report = message; });
      child.on('error', () => finish({ version: 1, result: 'fail', reason: 'worker_start_failed', cleanup: 'unknown' }));
      child.on('exit', code => finish(report && ((code === 0 && report.result === 'pass') || (code === 1 && report.result === 'fail')) ? report :
        { version: 1, result: 'fail', reason: 'worker_failed_or_timed_out', cleanup: 'unknown' }));
    });
  } finally { fs.rmSync(directory, { recursive: true, force: true }); }
}
async function main() {
  let config;
  try { config = stagingConfig(process.env); } catch (_) {
    process.stdout.write(JSON.stringify({ version: 1, result: 'fail', reason: 'staging_guard_rejected', cleanup: 'not-needed' }) + '\n');
    process.exitCode = 1; return;
  }
  // The child receives only the validated config and fixed target IDs, never
  // the full inherited environment. The child repeats scope/URL validation.
  const report = await runIsolated(config, path.join(__dirname, 'workerEntry.cjs'));
  process.stdout.write(JSON.stringify(report) + '\n');
  process.exitCode = report.result === 'pass' ? 0 : 1;
}
if (require.main === module) main().catch(() => { process.stdout.write('{"version":1,"result":"fail","reason":"runner_failed"}\n'); process.exitCode = 1; });
module.exports = { runIsolated };
