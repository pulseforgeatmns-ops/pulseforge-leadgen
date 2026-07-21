'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { Pool } = require('pg');
const {
  PORT_LOCK_DIRECTORY,
  releasePortLock,
  reservePort,
  startDisposablePostgres,
} = require('./helpers/disposablePostgres');

const execFileAsync = promisify(execFile);
const root = path.join(__dirname, '..');
const CONCURRENT_INSTANCES = 5;

function uniquePrefix(label) {
  return `${label}-${process.pid}-${Math.random().toString(36).slice(2, 8)}-`;
}

function tmpEntriesWithPrefix(prefix) {
  const bareName = path.basename(prefix);
  const roots = new Set([os.tmpdir(), '/tmp']);
  const matches = [];
  for (const directory of roots) {
    let entries = [];
    try {
      entries = fs.readdirSync(directory);
    } catch {
      continue;
    }
    for (const entry of entries) {
      if (entry.startsWith(bareName)) matches.push(path.join(directory, entry));
    }
  }
  return matches;
}

function processAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error.code === 'EPERM';
  }
}

function assertFullyCleaned(instance) {
  assert.equal(fs.existsSync(instance.rootDirectory), false,
    `cleanup root must be removed: ${instance.rootDirectory}`);
  assert.equal(fs.existsSync(instance.dataDirectory), false,
    `data directory must be removed: ${instance.dataDirectory}`);
  assert.equal(fs.existsSync(instance.socketDirectory), false,
    `socket directory must be removed: ${instance.socketDirectory}`);
  assert.equal(fs.existsSync(instance.logFile), false,
    `log file must be removed: ${instance.logFile}`);
  assert.equal(fs.existsSync(instance.portLockPath), false,
    `port lock must be released: ${instance.portLockPath}`);
  if (instance.postmasterPid) {
    assert.equal(processAlive(instance.postmasterPid), false,
      `postmaster ${instance.postmasterPid} must be stopped`);
  }
}

test('port reservation is lock-protected and yields no duplicates under concurrency', async () => {
  const reservations = await Promise.all(
    Array.from({ length: 16 }, () => reservePort())
  );
  try {
    const ports = reservations.map(reservation => reservation.port);
    assert.equal(new Set(ports).size, ports.length, 'every reserved port must be unique');
    for (const reservation of reservations) {
      assert.equal(fs.existsSync(reservation.lockPath), true, 'lock file must exist while reserved');
      assert.equal(fs.readFileSync(reservation.lockPath, 'utf8').trim(), String(process.pid));
      assert.equal(path.dirname(reservation.lockPath), PORT_LOCK_DIRECTORY);
    }
  } finally {
    for (const reservation of reservations) releasePortLock(reservation.lockPath);
  }
  for (const reservation of reservations) {
    assert.equal(fs.existsSync(reservation.lockPath), false, 'released locks must be removed');
  }
});

test('concurrent disposable PostgreSQL instances share no resources and clean up completely', {
  timeout: 300000,
}, async () => {
  const prefix = uniquePrefix('pg-stress');
  const socketPrefix = `pgt${(process.pid % 100)}-`;
  const instances = await Promise.all(
    Array.from({ length: CONCURRENT_INSTANCES }, () => startDisposablePostgres(prefix, { socketPrefix }))
  );
  try {
    for (const resource of ['port', 'rootDirectory', 'dataDirectory', 'socketDirectory', 'logFile', 'portLockPath', 'postmasterPid']) {
      const values = instances.map(instance => instance[resource]);
      assert.equal(values.every(value => value !== null && value !== undefined), true,
        `${resource} must be populated`);
      assert.equal(new Set(values).size, values.length,
        `${resource} must be collision-free across concurrent instances: ${JSON.stringify(values)}`);
    }
    for (const instance of instances) {
      assert.equal(fs.existsSync(path.join(instance.socketDirectory, `.s.PGSQL.${instance.port}`)), true,
        'each instance must own its Unix socket in its own socket directory');
      assert.equal(fs.existsSync(instance.portLockPath), true,
        'each instance must hold its port lock while running');
      assert.equal(processAlive(instance.postmasterPid), true,
        'each postmaster process must be alive and distinct');
    }

    // Concurrent writes against every instance: shared memory, socket, or
    // port collisions would surface as connection or query failures here.
    await Promise.all(instances.map(async (instance, index) => {
      const pool = new Pool({ connectionString: instance.connectionString });
      try {
        await pool.query('CREATE TABLE stress_probe(id INT PRIMARY KEY, marker TEXT NOT NULL)');
        await pool.query('INSERT INTO stress_probe VALUES ($1,$2)', [index, `instance-${index}`]);
        const { rows } = await pool.query('SELECT marker FROM stress_probe');
        assert.deepEqual(rows, [{ marker: `instance-${index}` }],
          'each instance must be fully isolated from its concurrent peers');
      } finally {
        await pool.end();
      }
    }));
  } finally {
    await Promise.all(instances.map(instance => instance.stop()));
  }
  for (const instance of instances) assertFullyCleaned(instance);
  assert.deepEqual(tmpEntriesWithPrefix(prefix), [], 'no stress data directories may remain');
  assert.deepEqual(tmpEntriesWithPrefix(socketPrefix), [], 'no stress socket directories may remain');
});

test('cleanup occurs after successful completion and after a failing test body', {
  timeout: 120000,
}, async () => {
  const prefix = uniquePrefix('pg-cleanup');

  const success = await startDisposablePostgres(prefix);
  const pool = new Pool({ connectionString: success.connectionString });
  await pool.query('SELECT 1');
  await pool.end();
  await success.stop();
  assertFullyCleaned(success);

  // Simulated test failure: an assertion throws while a client connection
  // is still open; the caller's cleanup path must still tear everything down.
  const failing = await startDisposablePostgres(prefix);
  const openPool = new Pool({ connectionString: failing.connectionString });
  // The still-open idle client is terminated by the fast shutdown below;
  // that termination error is the expected outcome, not a failure.
  openPool.on('error', () => {});
  await openPool.query('SELECT 1');
  await assert.rejects(async () => {
    try {
      assert.fail('deliberate test-body failure while a connection is open');
    } finally {
      await failing.stop();
      await openPool.end().catch(() => {});
    }
  }, /deliberate test-body failure/);
  assertFullyCleaned(failing);
  assert.deepEqual(tmpEntriesWithPrefix(prefix), [], 'no cleanup-scenario directories may remain');
});

test('cleanup occurs when initialization fails and when server start fails', {
  timeout: 120000,
}, async () => {
  const initPrefix = uniquePrefix('pg-initfail');
  const initSocketPrefix = `pgi${(process.pid % 100)}-`;
  await assert.rejects(
    startDisposablePostgres(initPrefix, {
      socketPrefix: initSocketPrefix,
      binaries: { initdb: '/usr/bin/false', pg_ctl: binaryPath('pg_ctl') },
    }),
    /initdb failed/
  );
  assert.deepEqual(tmpEntriesWithPrefix(initPrefix), [], 'initdb failure must remove its directories');
  assert.deepEqual(tmpEntriesWithPrefix(initSocketPrefix), [], 'initdb failure must remove its socket directory');

  const startPrefix = uniquePrefix('pg-startfail');
  const startSocketPrefix = `pgf${(process.pid % 100)}-`;
  const locksBefore = fs.existsSync(PORT_LOCK_DIRECTORY) ? fs.readdirSync(PORT_LOCK_DIRECTORY).length : 0;
  await assert.rejects(
    startDisposablePostgres(startPrefix, {
      socketPrefix: startSocketPrefix,
      binaries: { initdb: binaryPath('initdb'), pg_ctl: '/usr/bin/false' },
    }),
    /failed to start/
  );
  assert.deepEqual(tmpEntriesWithPrefix(startPrefix), [], 'start failure must remove its directories');
  assert.deepEqual(tmpEntriesWithPrefix(startSocketPrefix), [], 'start failure must remove its socket directory');
  const locksAfter = fs.existsSync(PORT_LOCK_DIRECTORY) ? fs.readdirSync(PORT_LOCK_DIRECTORY).length : 0;
  assert.equal(locksAfter <= locksBefore, true, 'start failure must not leak port locks');
});

test('cleanup occurs when a concurrent runner process dies with an unhandled exception', {
  timeout: 300000,
}, async () => {
  const script = `
    const { startDisposablePostgres } = require(${JSON.stringify(path.join(root, 'test', 'helpers', 'disposablePostgres'))});
    const mode = process.argv[1];
    startDisposablePostgres(process.argv[2]).then(async instance => {
      const { Pool } = require('pg');
      const pool = new Pool({ connectionString: instance.connectionString });
      const { rows } = await pool.query('SELECT 41+1 AS answer');
      await pool.end();
      console.log(JSON.stringify({
        answer: rows[0].answer,
        port: instance.port,
        rootDirectory: instance.rootDirectory,
        dataDirectory: instance.dataDirectory,
        socketDirectory: instance.socketDirectory,
        logFile: instance.logFile,
        portLockPath: instance.portLockPath,
        postmasterPid: instance.postmasterPid,
      }));
      if (mode === 'crash') throw new Error('deliberate unhandled runner exception');
      await instance.stop();
    });
  `;
  const spawnRunner = async (mode, prefix) => {
    try {
      const { stdout } = await execFileAsync(process.execPath, ['-e', script, mode, prefix], {
        cwd: root,
        timeout: 240000,
      });
      return { code: 0, report: JSON.parse(stdout.trim().split('\n').pop()) };
    } catch (error) {
      if (!error.stdout) throw error;
      return { code: error.code, report: JSON.parse(error.stdout.trim().split('\n').pop()) };
    }
  };

  const cleanPrefix = uniquePrefix('pg-child');
  const crashPrefix = uniquePrefix('pg-crash');
  const runners = await Promise.all([
    spawnRunner('clean', cleanPrefix),
    spawnRunner('clean', cleanPrefix),
    spawnRunner('crash', crashPrefix),
  ]);

  for (const runner of runners) assert.equal(runner.report.answer, 42);
  assert.equal(runners[0].code, 0);
  assert.equal(runners[1].code, 0);
  assert.notEqual(runners[2].code, 0, 'the crashing runner must exit non-zero');

  const resources = ['port', 'rootDirectory', 'dataDirectory', 'socketDirectory', 'logFile', 'portLockPath', 'postmasterPid'];
  for (const resource of resources) {
    const values = runners.map(runner => runner.report[resource]);
    assert.equal(new Set(values).size, values.length,
      `${resource} must be collision-free across concurrent processes`);
  }
  for (const runner of runners) assertFullyCleaned(runner.report);
  assert.deepEqual(tmpEntriesWithPrefix(cleanPrefix), [], 'child runners must leave no directories');
  assert.deepEqual(tmpEntriesWithPrefix(crashPrefix), [], 'crashed runner must leave no directories');
});

function binaryPath(name) {
  const { execFileSync } = require('node:child_process');
  return execFileSync('sh', ['-c', `command -v ${name}`], { encoding: 'utf8' }).trim();
}
