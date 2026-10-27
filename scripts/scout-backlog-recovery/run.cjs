'use strict';
// Explicit standalone operator lifecycle for the authorized Scout backlog recovery.
// No app bootstrap, auto-install, activation setting, provider transport, replay
// or retry. All non-database network transport is denied for the whole process.
const fs = require('node:fs');
const MAX_INPUT_BYTES = 128 * 1024;
const MODES = new Set(['--draft', '--preview', '--install-approved', '--execute-approved', '--receipt', '--revoke-approved']);
const MUTATIONS = new Set(['--install-approved', '--execute-approved', '--revoke-approved']);
const STOP = 'Scout backlog recovery stopped. Inspect the durable grant/run/item and candidate-work records (--receipt) before any further action. No retry was attempted.';
const stop = code => Object.assign(Error(code), { code });
function stopCode(error) {
  const code = error && Object.getOwnPropertyDescriptor(error, 'code')?.value;
  if (['42P01', '42703', '42883', '3F000'].includes(code)) return 'required_schema_unavailable';
  if (typeof code === 'string' && /^backlog_recovery_[a-z0-9_]{1,96}(?![\s\S])/.test(code)) return code;
  return 'backlog_recovery_stopped';
}
function invocation(args) {
  if (![2, 4].includes(args.length) || !MODES.has(args[0]) || typeof args[1] !== 'string' || !args[1]
    || args[1] !== '-' && args[1].startsWith('--')
    || args.length === 4 && (args[2] !== '--confirm' || !/^[a-f0-9]{64}(?![\s\S])/.test(args[3]))
    || MUTATIONS.has(args[0]) !== (args.length === 4)) throw stop('backlog_recovery_explicit_invocation_required');
  return { mode: args[0], file: args[1], confirmHash: args[3] };
}
async function inputFrom(file) {
  const stream = file === '-' ? process.stdin : fs.createReadStream(file, { highWaterMark: 16384 });
  const chunks = []; let bytes = 0;
  for await (const chunk of stream) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.length;
    if (bytes > MAX_INPUT_BYTES) throw stop('backlog_recovery_input_too_large');
    chunks.push(buffer);
  }
  if (!bytes) throw stop('backlog_recovery_input_required');
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}
async function output(result) {
  const forbidden = /^(execution_token|executionToken|claimToken|claim_token|apiKey|api_key|password|authorization|database_url|DATABASE_URL)$/;
  (function safe(value, seen = new Set()) {
    if (!value || typeof value !== 'object') return;
    if (seen.has(value)) throw stop('backlog_recovery_output_not_safe');
    seen.add(value);
    for (const [key, entry] of Object.entries(value)) {
      if (forbidden.test(key)) throw stop('backlog_recovery_output_not_safe');
      safe(entry, seen);
    }
    seen.delete(value);
  })(result);
  const text = JSON.stringify(result) + '\n';
  if (Buffer.byteLength(text) > 256 * 1024) throw stop('backlog_recovery_output_not_safe');
  for (const secret of [process.env.DATABASE_URL, process.env.GOOGLE_PLACES_KEY]) {
    if (secret && secret.length >= 8 && text.includes(secret)) throw stop('backlog_recovery_output_not_safe');
  }
  await new Promise((resolve, reject) => process.stdout.write(text, error => error ? reject(error) : resolve()));
}
async function main() {
  const { mode, file, confirmHash } = invocation(process.argv.slice(2));
  const raw = await inputFrom(file);
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw stop('backlog_recovery_manifest_invalid');
  if (!process.env.DATABASE_URL) throw stop('backlog_recovery_existing_credentials_required');
  const { Pool } = require('pg');
  // Confinement is installed before ANY application-service import. The returned
  // Places fetch is deliberately never used: this path makes no provider call.
  require('../scout-bounded/transport.cjs').installTransport({ databaseUrl: process.env.DATABASE_URL });
  const pool = new Pool({ connectionString: process.env.DATABASE_URL,
    ssl: String(process.env.DATABASE_SSL || '').toLowerCase() === 'false' ? false : { rejectUnauthorized: false },
    max: 6, connectionTimeoutMillis: 2000, statement_timeout: 30000, query_timeout: 30000, lock_timeout: 2000 });
  const methods = ['log', 'info', 'warn', 'error', 'debug'];
  const previousConsole = Object.fromEntries(methods.map(name => [name, console[name]]));
  for (const name of methods) console[name] = () => {};
  try {
    const recovery = require('../../services/scoutBacklogRecovery');
    let result;
    if (mode === '--draft') result = await recovery.draftBacklogRecoveryManifest(pool, raw);
    else if (mode === '--preview') result = await recovery.previewBacklogRecovery(pool, raw);
    else if (mode === '--receipt') result = await recovery.inspectBacklogRecovery(pool, raw);
    else if (mode === '--install-approved') result = await recovery.installBacklogRecovery(pool, raw, confirmHash);
    else if (mode === '--revoke-approved') result = await recovery.revokeBacklogRecovery(pool, raw, confirmHash);
    else result = await recovery.executeBacklogRecovery({ pool, manifest: raw, confirmationHash: confirmHash });
    await output(result);
  } finally {
    try { await pool.end(); }
    finally { for (const name of methods) console[name] = previousConsole[name]; }
  }
}
if (require.main === module) main().catch(error => { process.stderr.write(STOP + ' Reason: ' + stopCode(error) + '.\n'); process.exitCode = 1; });
module.exports = { main, invocation };
