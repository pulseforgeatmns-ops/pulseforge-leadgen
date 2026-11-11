'use strict';
// Regression coverage for .github/actions/setup-postgres-environment/provision.sh
// and the workflows that use it. CI once failed on every PostgreSQL-backed job
// because `apt-get install postgresql` needed the Ubuntu mirrors (DNS outage),
// although the runner image already ships the PostgreSQL binaries. The script
// now prefers validated runner-image binaries and keeps apt as the fallback.
//
// Hermetic: fake pg_config/initdb/pg_ctl/psql/apt-get on a private PATH; no
// sudo, no network, no real package manager.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync, execFileSync } = require('node:child_process');

const ROOT = path.join(__dirname, '..');
const SCRIPT = path.join(ROOT, '.github/actions/setup-postgres-environment/provision.sh');
const WORKFLOWS = path.join(ROOT, '.github/workflows');
// Only the utilities the script and fakes use, so a host PostgreSQL install
// (e.g. /usr/bin/pg_config) can never leak into the hermetic cases.
const UTILITIES = ['mkdir', 'ls', 'cp', 'rm', 'chmod', 'cat'];
const BASH = execFileSync('bash', ['-c', 'command -v bash'], { encoding: 'utf8' }).trim();
function systemDir(root) {
  const dir = path.join(root, 'sys');
  fs.mkdirSync(dir, { recursive: true });
  for (const name of UTILITIES) {
    const real = execFileSync('bash', ['-c', `command -v ${name}`], { encoding: 'utf8' }).trim();
    fs.symlinkSync(real, path.join(dir, name));
  }
  return dir;
}

function exe(file, body) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `#!/bin/bash\n${body}\n`, { mode: 0o755 });
}

// Creates a fake PostgreSQL install. `omit` drops binaries; `brokenInitdb`
// makes initdb fail its --version check.
function fakePostgres(root, { omit = [], brokenInitdb = false } = {}) {
  const bindir = path.join(root, 'pg/bin');
  fs.mkdirSync(bindir, { recursive: true });
  for (const name of ['initdb', 'pg_ctl', 'psql']) {
    if (omit.includes(name)) continue;
    exe(path.join(bindir, name), name === 'initdb' && brokenInitdb ? 'exit 1' : `echo "${name} (PostgreSQL) 18.0"`);
  }
  if (!omit.includes('pg_config')) exe(path.join(root, 'tools/pg_config'), `echo "${bindir}"`);
  return bindir;
}

// Fake apt-get: records every call; on `install` it optionally creates a
// complete PostgreSQL install (simulating a successful package install).
function fakeApt(root, { installs = true, fail = false } = {}) {
  const log = path.join(root, 'apt.log');
  const apt = path.join(root, 'apt-get');
  const install = installs
    ? `rm -rf "${root}/pg" "${root}/tools/pg_config"; mkdir -p "${root}/pg/bin" "${root}/tools"
       for n in initdb pg_ctl psql; do printf '#!/bin/bash\\necho "%s (PostgreSQL) 18.0"\\n' "$n" > "${root}/pg/bin/$n"; chmod +x "${root}/pg/bin/$n"; done
       printf '#!/bin/bash\\necho "${root}/pg/bin"\\n' > "${root}/tools/pg_config"; chmod +x "${root}/tools/pg_config"`
    : 'true';
  exe(apt, `echo "$*" >> "${log}"\n${fail ? 'exit 100' : `if [ "$1" = install ]; then\n${install}\nfi`}`);
  return { apt, log, calls: () => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').trim().split('\n').filter(Boolean) : []) };
}

function run(root, apt, extraEnv = {}) {
  const out = path.join(root, 'github_output');
  const ghPath = path.join(root, 'github_path');
  fs.writeFileSync(out, ''); fs.writeFileSync(ghPath, '');
  const r = spawnSync(BASH, [SCRIPT], { encoding: 'utf8', env: {
    PATH: `${path.join(root, 'tools')}:${systemDir(root)}`, HOME: root,
    CACHE_DIR: path.join(root, 'apt-cache'), GITHUB_OUTPUT: out, GITHUB_PATH: ghPath,
    PG_PROVISION_SUDO: '', PG_PROVISION_APT_GET: apt.apt, ...extraEnv } });
  const outputs = Object.fromEntries(fs.readFileSync(out, 'utf8').trim().split('\n').filter(Boolean).map(l => l.split(/=(.*)/s).slice(0, 2)));
  return { ...r, outputs, ghPath: fs.readFileSync(ghPath, 'utf8').trim() };
}

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'pg-provision-'));

test('runner-image binaries are used without any apt call when pg_config, initdb, pg_ctl and psql validate', () => {
  const root = tmp(); const bindir = fakePostgres(root); const apt = fakeApt(root);
  const r = run(root, apt);
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(apt.calls(), []);
  assert.equal(r.outputs['postgres-source'], 'runner-image');
  assert.equal(r.outputs.bindir, bindir);
  assert.equal(r.ghPath, bindir);
  assert.match(r.stdout, /Using runner-image PostgreSQL binaries \(no apt access\)/);
  assert.match(r.stdout, /psql \(PostgreSQL\) 18\.0/);
});

for (const [label, opts] of [
  ['pg_config is missing', { omit: ['pg_config'] }],
  ['initdb is missing', { omit: ['initdb'] }],
  ['pg_ctl is missing', { omit: ['pg_ctl'] }],
  ['psql is missing', { omit: ['psql'] }],
  ['initdb does not run', { brokenInitdb: true }],
]) {
  test(`apt fallback runs the unchanged install when ${label}`, () => {
    const root = tmp(); fakePostgres(root, opts); const apt = fakeApt(root);
    const r = run(root, apt);
    assert.equal(r.status, 0, r.stderr);
    assert.deepEqual(apt.calls(), ['update -qq', 'install -y -qq --no-install-recommends postgresql postgresql-client']);
    assert.equal(r.outputs['postgres-source'], 'apt');
    assert.equal(r.outputs.bindir, path.join(root, 'pg/bin'));
    assert.match(r.stdout, /falling back to apt/);
  });
}

test('fails when binaries are still unavailable after the apt fallback', () => {
  const root = tmp(); fakePostgres(root, { omit: ['psql'] }); const apt = fakeApt(root, { installs: false });
  const r = run(root, apt);
  assert.notEqual(r.status, 0);
  assert.match(r.stderr, /psql is missing or not executable/);
  assert.match(r.stderr, /still unavailable after the apt fallback/);
  assert.equal(r.outputs['postgres-source'], undefined);
  assert.equal(r.ghPath, '');
});

test('fails when apt itself fails (no binaries, no mirror access)', () => {
  const root = tmp(); fakePostgres(root, { omit: ['pg_config'] }); const apt = fakeApt(root, { fail: true });
  const r = run(root, apt);
  assert.notEqual(r.status, 0);
  assert.deepEqual(apt.calls(), ['update -qq']);
  assert.equal(r.outputs['postgres-source'], undefined);
});

test('the apt fallback requires CACHE_DIR, as before', () => {
  const root = tmp(); fakePostgres(root, { omit: ['pg_config'] }); const apt = fakeApt(root);
  const r = run(root, apt, { CACHE_DIR: '' });
  assert.notEqual(r.status, 0);
  assert.match(r.stderr, /CACHE_DIR is required/);
  assert.deepEqual(apt.calls(), []);
});

test('on a host with a real PostgreSQL install, the runner-image path is taken', t => {
  let real = null;
  try { real = execFileSync('pg_config', ['--bindir'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim(); } catch (_) { /* absent */ }
  if (!real || !['initdb', 'pg_ctl', 'psql'].every(n => fs.existsSync(path.join(real, n)))) {
    t.skip('no complete PostgreSQL install on this host; the hermetic cases above cover the logic');
    return;
  }
  const root = tmp(); const apt = fakeApt(root, { fail: true });
  const out = path.join(root, 'out'); fs.writeFileSync(out, '');
  const r = spawnSync('bash', [SCRIPT], { encoding: 'utf8', env: { ...process.env, GITHUB_OUTPUT: out,
    GITHUB_PATH: path.join(root, 'path'), CACHE_DIR: path.join(root, 'c'), PG_PROVISION_SUDO: '', PG_PROVISION_APT_GET: apt.apt } });
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(apt.calls(), []);
  assert.match(fs.readFileSync(out, 'utf8'), /postgres-source=runner-image/);
});

// ---------------------------------------------------------------- workflows
const workflows = () => fs.readdirSync(WORKFLOWS).filter(f => /\.ya?ml$/.test(f))
  .map(f => [f, fs.readFileSync(path.join(WORKFLOWS, f), 'utf8')]);

test('no workflow installs PostgreSQL with apt directly; the shared action owns provisioning', () => {
  for (const [file, text] of workflows()) {
    assert.doesNotMatch(text, /apt-get\s+install[^\n]*\bpostgresql\b/, file);
  }
  const action = fs.readFileSync(path.join(ROOT, '.github/actions/setup-postgres-environment/action.yml'), 'utf8');
  assert.match(action, /run: bash "\$\{\{ github\.action_path \}\}\/provision\.sh"/);
});

test('consolidated jobs keep their /usr/local/bin symlinks, now from the action output', () => {
  const linked = { 'pr-ci-lean.yml': 1, 'signal-v1-core.yml': 1, 'full-regression.yml': 1, 'phase3f-disposable-postgres-release-gate.yml': 1 };
  for (const [file, count] of Object.entries(linked)) {
    const text = fs.readFileSync(path.join(WORKFLOWS, file), 'utf8');
    const links = text.match(/bindir="\$\{\{ steps\.postgres\.outputs\.bindir \}\}"\n\s+sudo ln -sf "\$bindir\/initdb" \/usr\/local\/bin\/initdb\n\s+sudo ln -sf "\$bindir\/pg_ctl" \/usr\/local\/bin\/pg_ctl/g) || [];
    assert.equal(links.length, count, file);
    assert.match(text, /id: postgres\n\s+uses: \.\/\.github\/actions\/setup-postgres-environment/, file);
  }
});

test('PostgreSQL service-container configurations are unchanged', () => {
  const phase3f = fs.readFileSync(path.join(WORKFLOWS, 'phase3f-disposable-postgres-release-gate.yml'), 'utf8');
  assert.match(phase3f, /services:\n\s+postgres:\n\s+image: postgres:18\.4/);
  assert.match(phase3f, /- 5432:5432/);
  const usage = fs.readFileSync(path.join(WORKFLOWS, 'api-usage-control.yml'), 'utf8');
  assert.match(usage, /services:\n\s+postgres:\n\s+image: postgres:16/);
});

// Dependency-free: this file runs in the plan job, which has no npm install.
// Splits a workflow into its top-level jobs (two-space keys under `jobs:`).
function jobsOf(text) {
  const lines = text.split('\n');
  const start = lines.findIndex(l => /^jobs:\s*$/.test(l));
  const jobs = [];
  if (start < 0) return jobs;
  for (const line of lines.slice(start + 1)) {
    if (/^\S/.test(line)) break;
    const key = line.match(/^ {2}([A-Za-z0-9_-]+):\s*$/);
    if (key) jobs.push({ id: key[1], lines: [] });
    else if (jobs.length) jobs[jobs.length - 1].lines.push(line);
  }
  return jobs;
}

test('every steps.postgres reference follows the step that defines it, in the same job', () => {
  let references = 0;
  for (const [file, text] of workflows()) {
    for (const job of jobsOf(text)) {
      let defined = false;
      job.lines.forEach((line, i) => {
        if (/steps\.postgres\./.test(line)) { references += 1; assert.ok(defined, `${file}#${job.id}: steps.postgres used before definition`); }
        if (/^\s+(?:- )?id: postgres\s*$/.test(line)) {
          assert.match(job.lines[i + 1] || '', /^\s+uses: \.\/\.github\/actions\/setup-postgres-environment\s*$/, `${file}#${job.id}`);
          defined = true;
        }
      });
    }
  }
  assert.ok(references >= 4);
});

test('a change to the shared action re-runs the PostgreSQL-backed PR suites', () => {
  const { buildPlan } = require('../.github/scripts/ci-plan-pr.js');
  const suites = buildPlan(['.github/actions/setup-postgres-environment/provision.sh']).suites;
  for (const suite of ['signal-v1', 'walkthrough', 'revenue-postgres', 'anchor-outbound', 'decision-shadow', 'paige-social', 'spec245-evidence']) {
    assert.ok(suites.includes(suite), suite);
  }
});
