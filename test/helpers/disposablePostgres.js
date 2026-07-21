'use strict';

const fs = require('fs');
const net = require('net');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');
const { Pool } = require('pg');

// Cross-process registry of TCP ports reserved by disposable PostgreSQL
// instances. An OS-assigned ephemeral port alone is racy: two concurrent
// suites can be handed the same port between server.close() and postgres
// bind. The atomic 'wx' lock file closes that window between harnesses,
// and the bind-failure retry loop below closes it against unrelated
// processes.
const PORT_LOCK_DIRECTORY = path.join(os.tmpdir(), 'disposable-postgres-port-locks');
const MAX_PORT_RESERVATION_ATTEMPTS = 20;
const MAX_START_ATTEMPTS = 5;
// PostgreSQL rejects Unix socket paths longer than ~103 bytes.
const MAX_SOCKET_PATH_BYTES = 103;

const activeInstances = new Set();
let exitCleanupInstalled = false;

function binary(name) {
  try {
    return execFileSync('sh', ['-c', `command -v ${name}`], { encoding: 'utf8' }).trim();
  } catch {
    try {
      const bindir = execFileSync('pg_config', ['--bindir'], { encoding: 'utf8' }).trim();
      const candidate = path.join(bindir, name);
      return fs.existsSync(candidate) ? candidate : null;
    } catch {
      return null;
    }
  }
}

function osAssignedPort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

function processAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error.code === 'EPERM';
  }
}

function tryAcquirePortLock(port) {
  fs.mkdirSync(PORT_LOCK_DIRECTORY, { recursive: true, mode: 0o700 });
  const lockPath = path.join(PORT_LOCK_DIRECTORY, `${port}.lock`);
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const descriptor = fs.openSync(lockPath, 'wx', 0o600);
      fs.writeSync(descriptor, String(process.pid));
      fs.closeSync(descriptor);
      return lockPath;
    } catch (error) {
      if (error.code !== 'EEXIST') throw error;
      let owner = NaN;
      try {
        owner = Number(fs.readFileSync(lockPath, 'utf8').trim());
      } catch {
        return null;
      }
      if (Number.isInteger(owner) && owner > 0 && processAlive(owner)) return null;
      // The owning process is gone; reclaim the stale lock and retry once.
      try {
        fs.rmSync(lockPath, { force: true });
      } catch {
        return null;
      }
    }
  }
  return null;
}

async function reservePort() {
  for (let attempt = 0; attempt < MAX_PORT_RESERVATION_ATTEMPTS; attempt += 1) {
    const port = await osAssignedPort();
    const lockPath = tryAcquirePortLock(port);
    if (lockPath) return { port, lockPath };
  }
  throw new Error('Could not reserve a lock-protected TCP port for disposable PostgreSQL');
}

function releasePortLock(lockPath) {
  if (!lockPath) return;
  try {
    fs.rmSync(lockPath, { force: true });
  } catch { /* best-effort */ }
}

function createSocketDirectory(socketPrefix) {
  const candidates = [path.join(os.tmpdir(), socketPrefix), path.join('/tmp', socketPrefix)];
  for (const prefix of candidates) {
    let directory;
    try {
      directory = fs.mkdtempSync(prefix);
    } catch {
      continue;
    }
    if (Buffer.byteLength(path.join(directory, '.s.PGSQL.65535.lock')) <= MAX_SOCKET_PATH_BYTES) {
      return directory;
    }
    fs.rmSync(directory, { recursive: true, force: true });
  }
  throw new Error('Could not create a Unix socket directory short enough for PostgreSQL');
}

function cleanupInstance(instance, stopMode) {
  if (instance.cleanedUp) return;
  instance.cleanedUp = true;
  activeInstances.delete(instance);
  if (instance.started) {
    try {
      execFileSync(instance.pgCtl,
        ['-D', instance.dataDirectory, '-m', stopMode, '-t', '30', '-w', 'stop'],
        instance.commandOptions);
    } catch {
      try {
        execFileSync(instance.pgCtl,
          ['-D', instance.dataDirectory, '-m', 'immediate', '-t', '10', '-w', 'stop'],
          instance.commandOptions);
      } catch { /* the directory removal below still reclaims disk */ }
    }
    instance.started = false;
  }
  for (const directory of [instance.rootDirectory, instance.socketDirectory]) {
    if (!directory) continue;
    try {
      fs.rmSync(directory, { recursive: true, force: true });
    } catch { /* best-effort */ }
  }
  releasePortLock(instance.portLockPath);
  instance.portLockPath = null;
}

function installExitCleanup() {
  if (exitCleanupInstalled) return;
  exitCleanupInstalled = true;
  process.on('exit', () => {
    for (const instance of [...activeInstances]) cleanupInstance(instance, 'immediate');
  });
}

function readPostmasterPid(dataDirectory) {
  try {
    const firstLine = fs.readFileSync(path.join(dataDirectory, 'postmaster.pid'), 'utf8')
      .split('\n', 1)[0].trim();
    const pid = Number(firstLine);
    return Number.isInteger(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

function portCollisionInLog(logFile) {
  try {
    const log = fs.readFileSync(logFile, 'utf8');
    return /could not bind|[Aa]ddress already in use|[Aa]ddress in use|lock file ".*\.s\.PGSQL\.\d+\.lock" already exists/.test(log);
  } catch {
    return false;
  }
}

async function startDisposablePostgres(prefix = 'phase16b-pg-', options = {}) {
  const initdb = options.binaries?.initdb || binary('initdb');
  const pgCtl = options.binaries?.pg_ctl || binary('pg_ctl');
  if (!initdb || !pgCtl) throw new Error('PostgreSQL initdb and pg_ctl are required; integration tests may not skip');

  installExitCleanup();
  const instance = {
    pgCtl,
    rootDirectory: null,
    dataDirectory: null,
    socketDirectory: null,
    logFile: null,
    portLockPath: null,
    started: false,
    cleanedUp: false,
    commandOptions: { stdio: 'ignore', env: { ...process.env, LANG: 'C', LC_ALL: 'C' } },
  };
  activeInstances.add(instance);

  try {
    // mkdtemp gives a collision-proof random name with 0700 permissions;
    // every per-instance path lives under this one cleanup root.
    instance.rootDirectory = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
    instance.dataDirectory = path.join(instance.rootDirectory, 'data');
    instance.logFile = path.join(instance.rootDirectory, 'postgres.log');
    instance.socketDirectory = createSocketDirectory(options.socketPrefix || 'pgs-');

    try {
      execFileSync(initdb, ['-A', 'trust', '-U', 'postgres', '-D', instance.dataDirectory],
        instance.commandOptions);
    } catch (error) {
      throw new Error(`Disposable PostgreSQL initdb failed for ${instance.dataDirectory}`, { cause: error });
    }

    let port = null;
    let lastStartError = null;
    for (let attempt = 0; attempt < MAX_START_ATTEMPTS && !instance.started; attempt += 1) {
      const reservation = await reservePort();
      instance.portLockPath = reservation.lockPath;
      port = reservation.port;
      fs.rmSync(instance.logFile, { force: true });
      try {
        execFileSync(pgCtl, [
          '-D', instance.dataDirectory,
          '-l', instance.logFile,
          '-o', `-p ${port} -h 127.0.0.1 -k ${instance.socketDirectory}`,
          '-w',
          'start',
        ], instance.commandOptions);
        instance.started = true;
      } catch (error) {
        lastStartError = error;
        releasePortLock(instance.portLockPath);
        instance.portLockPath = null;
        if (!portCollisionInLog(instance.logFile)) break;
      }
    }
    if (!instance.started) {
      const log = fs.existsSync(instance.logFile)
        ? fs.readFileSync(instance.logFile, 'utf8')
        : 'No PostgreSQL log was created.';
      throw new Error(`Disposable PostgreSQL failed to start:\n${log}`, { cause: lastStartError });
    }

    return {
      connectionString: `postgresql://postgres@127.0.0.1:${port}/postgres`,
      port,
      rootDirectory: instance.rootDirectory,
      dataDirectory: instance.dataDirectory,
      socketDirectory: instance.socketDirectory,
      logFile: instance.logFile,
      portLockPath: path.join(PORT_LOCK_DIRECTORY, `${port}.lock`),
      postmasterPid: readPostmasterPid(instance.dataDirectory),
      async stop() {
        cleanupInstance(instance, 'fast');
      },
    };
  } catch (error) {
    cleanupInstance(instance, 'immediate');
    throw error;
  }
}

async function prepareRevenueDatabase(connectionString, root) {
  const pool = new Pool({ connectionString });
  await pool.query(fs.readFileSync(path.join(root, 'test', 'fixtures', 'revenueBaseSchema.sql'), 'utf8'));
  await pool.query(`INSERT INTO clients(id,name) VALUES (10,'Anchor Cleaning'),(11,'Other Tenant')`);
  return pool;
}

function resetRevenueService(connectionString) {
  process.env.DATABASE_URL = connectionString;
  process.env.DATABASE_SSL = 'false';
  for (const name of [
    '../../db',
    '../../services/revenueService',
  ]) {
    delete require.cache[require.resolve(name)];
  }
  return {
    pool: require('../../db'),
    revenue: require('../../services/revenueService'),
  };
}

module.exports = {
  PORT_LOCK_DIRECTORY,
  prepareRevenueDatabase,
  reservePort,
  releasePortLock,
  resetRevenueService,
  startDisposablePostgres,
};
