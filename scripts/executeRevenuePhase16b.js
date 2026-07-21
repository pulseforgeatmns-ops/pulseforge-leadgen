#!/usr/bin/env node
'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');
const { Pool } = require('pg');
const { executePhase16b } = require('../services/revenuePhase16bRunner');

const ROOT = path.join(__dirname, '..');
const DEFAULT_AUTHORIZATION = path.join(ROOT, 'artifacts', 'revenue', 'phase16b-production-authorization-draft.json');
const DEFAULT_EVIDENCE = path.join(ROOT, 'artifacts', 'revenue', 'phase16b-production-execution.json');

function parse(argv) {
  const options = { mode: 'rehearsal', authorization: DEFAULT_AUTHORIZATION, evidence: DEFAULT_EVIDENCE };
  for (const arg of argv) {
    if (arg === '--rehearsal') options.mode = 'rehearsal';
    else if (arg === '--production') options.mode = 'production';
    else if (arg.startsWith('--authorization=')) options.authorization = path.resolve(arg.slice(16));
    else if (arg.startsWith('--observed=')) options.observed = path.resolve(arg.slice(11));
    else if (arg.startsWith('--evidence=')) options.evidence = path.resolve(arg.slice(11));
    else if (arg.startsWith('--fail-at=')) options.failAt = arg.slice(10);
    else if (arg.startsWith('--confirm=')) options.confirm = arg.slice(10);
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return options;
}

function sha256File(relativePath) {
  const absolute = path.join(ROOT, relativePath);
  return {
    path: relativePath,
    sha256: crypto.createHash('sha256').update(fs.readFileSync(absolute)).digest('hex'),
  };
}

function phase16aEvidence() {
  const read = name => JSON.parse(fs.readFileSync(path.join(ROOT, 'artifacts', 'revenue', name), 'utf8'));
  return {
    closure: read('phase16a-durable-backup-closure.json'),
    backup: read('phase16a-backup-evidence.json'),
    restore: read('phase16a-restore-evidence.json'),
  };
}

function freshIdentity() {
  const protectedMain = execFileSync('git', ['ls-remote', 'origin', 'main'], {
    cwd: ROOT,
    encoding: 'utf8',
  }).trim().split(/\s+/)[0];
  const deployments = JSON.parse(execFileSync('railway', [
    'deployment', 'list',
    '--service', 'pulseforge-leadgen',
    '--environment', 'production',
    '--json',
  ], { cwd: ROOT, encoding: 'utf8' }));
  const active = deployments[0];
  if (!active || active.status !== 'SUCCESS') {
    throw new Error('The current Railway production deployment is not successful');
  }
  return {
    observed_at: new Date().toISOString(),
    protected_main_commit: protectedMain,
    railway_deployment: {
      id: active.id,
      status: active.status,
      commit: active.meta?.commitHash,
      service: 'pulseforge-leadgen',
      environment: 'production',
      image_digest: active.meta?.imageDigest || null,
    },
    migration_checksums: {
      phase1: sha256File('migrations/2026-07-18-anchor-closed-loop-revenue-phase1.sql'),
      phase15: sha256File('migrations/2026-07-18-anchor-closed-loop-revenue-phase15.sql'),
      phase15_operational_rollback: sha256File('migrations/2026-07-18-anchor-closed-loop-revenue-phase15.rollback.sql'),
    },
    phase16a: phase16aEvidence(),
  };
}

async function main() {
  const options = parse(process.argv.slice(2));
  const authorization = JSON.parse(fs.readFileSync(options.authorization, 'utf8'));
  if (options.mode === 'production') {
    if (process.env.REVENUE_PHASE16B_PRODUCTION_ENABLED !== 'true'
      || options.confirm !== authorization.authorization_id) {
      throw new Error('Production remains disabled; explicit environment gate and authorization-ID confirmation are required');
    }
  }
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required');
  if (options.mode === 'rehearsal') {
    const databaseUrl = new URL(process.env.DATABASE_URL);
    if (!['127.0.0.1', 'localhost', '::1'].includes(databaseUrl.hostname)
      || process.env.REVENUE_PHASE16B_DISPOSABLE_CONFIRMED !== 'true') {
      throw new Error('Rehearsal requires a loopback PostgreSQL URL and REVENUE_PHASE16B_DISPOSABLE_CONFIRMED=true');
    }
  }
  if (options.mode === 'production' && options.observed) {
    throw new Error('--observed is rehearsal-only; production must perform a fresh identity observation');
  }
  const observed = options.observed
    ? JSON.parse(fs.readFileSync(options.observed, 'utf8'))
    : freshIdentity();
  let pool;
  const lazyRevenue = new Proxy({}, {
    get(_target, property) {
      return (...args) => require('../services/revenueService')[property](...args);
    },
  });
  const operations = require('../services/revenueOperations');
  const evidence = await executePhase16b(authorization, {
    observeIdentity: async () => observed,
    openDatabase: async () => {
      pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: process.env.DATABASE_SSL === 'false' ? false : { rejectUnauthorized: false },
      });
      return pool.connect();
    },
    assertDisposableDatabase: async db => {
      if (options.mode !== 'rehearsal') return false;
      const identity = await db.query(
        'SELECT inet_server_addr()::text AS server_addr, current_database() AS database'
      );
      return ['127.0.0.1', '::1'].includes(identity.rows[0]?.server_addr);
    },
    readMigration: async relativePath => fs.readFileSync(path.join(ROOT, relativePath), 'utf8'),
    revenue: lazyRevenue,
    operations,
  }, {
    mode: options.mode,
    productionEnabled: options.mode === 'production',
    failAt: options.failAt,
  });
  fs.mkdirSync(path.dirname(options.evidence), { recursive: true });
  fs.writeFileSync(options.evidence, `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600 });
  process.stdout.write(`${JSON.stringify(evidence, null, 2)}\n`);
  process.exitCode = evidence.verdict === 'COMPLETE' ? 0 : 2;
  if (pool) await pool.end();
}

if (require.main === module) {
  main().catch(error => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}

module.exports = { freshIdentity, parse, phase16aEvidence, sha256File };
