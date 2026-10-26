'use strict';
// Explicit standalone operator lifecycle only. No app bootstrap, auto-install,
// persistent activation setting, provider fallback, replay or uncertain-run retry.
const fs = require('node:fs');
const MAX_INPUT_BYTES = 128 * 1024;
const MODES = new Set(['--preview', '--install-approved', '--execute-approved', '--receipt', '--revoke-approved']);
const MUTATIONS = new Set(['--install-approved', '--execute-approved', '--revoke-approved']);
const STOP = 'Bounded Scout stopped. Inspect durable authority/run/page/attempt records before any further action. No retry was attempted.';
function stopCode(error) {
  const code = error && Object.getOwnPropertyDescriptor(error, 'code')?.value;
  if (['42P01', '42703', '42883', '3F000'].includes(code)) return 'required_schema_unavailable';
  if (typeof code === 'string' && /^(bounded|coverage)_[a-z0-9_]{1,96}(?![\s\S])/.test(code)) return code;
  return 'bounded_stopped';
}

function invocation(args) {
  if (![2, 4].includes(args.length) || !MODES.has(args[0]) || typeof args[1] !== 'string' || !args[1]
    || args[1] !== '-' && args[1].startsWith('--')
    || args.length === 4 && (args[2] !== '--confirm' || !/^[a-f0-9]{64}(?![\s\S])/.test(args[3]))
    || !MUTATIONS.has(args[0]) && args.length !== 2) throw Object.assign(Error('bounded_explicit_invocation_required'), { code: 'bounded_explicit_invocation_required' });
  return { mode: args[0], file: args[1], confirmHash: args[3] };
}
async function inputFrom(file) {
  // A single bounded read before opening a DB connection or loading providers.
  // Missing EOF/interrupted stdin cannot install, claim or execute anything.
  const stream = file === '-' ? process.stdin : fs.createReadStream(file, { highWaterMark: 16384 });
  const chunks = []; let bytes = 0;
  for await (const chunk of stream) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.length;
    if (bytes > MAX_INPUT_BYTES) throw Object.assign(Error('bounded_input_too_large'), { code: 'bounded_input_too_large' });
    chunks.push(buffer);
  }
  if (!bytes) throw Object.assign(Error('bounded_input_required'), { code: 'bounded_input_required' });
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}
async function output(result) {
  const forbidden = /^(execution_token|executionToken|pageToken|next_page_token|apiKey|api_key|password|authorization|database_url|DATABASE_URL)$/;
  const seen = new Set();
  function safe(value) {
    if (!value || typeof value !== 'object') return;
    if (seen.has(value)) throw Object.assign(Error('bounded_output_not_safe'), { code: 'bounded_output_not_safe' });
    seen.add(value);
    for (const [key, entry] of Object.entries(value)) {
      if (forbidden.test(key)) throw Object.assign(Error('bounded_output_not_safe'), { code: 'bounded_output_not_safe' });
      safe(entry);
    }
    seen.delete(value);
  }
  safe(result);
  const text = JSON.stringify(result) + '\n';
  if (Buffer.byteLength(text) > 256 * 1024) throw Object.assign(Error('bounded_output_not_safe'), { code: 'bounded_output_not_safe' });
  // Services expose only bounded business/receipt fields, never private ledger
  // execution tokens or provider continuation tokens. Defense in depth against
  // an accidental existing credential being included in a future response.
  for (const secret of [process.env.DATABASE_URL, process.env.GOOGLE_PLACES_KEY]) {
    if (secret && secret.length >= 8 && text.includes(secret)) throw Object.assign(Error('bounded_output_not_safe'), { code: 'bounded_output_not_safe' });
  }
  await new Promise((resolve, reject) => {
    const onError = error => reject(error);
    process.stdout.once('error', onError);
    process.stdout.write(text, error => {
      process.stdout.removeListener('error', onError);
      if (error) reject(error); else resolve();
    });
  });
}
async function main() {
  const { mode, file, confirmHash } = invocation(process.argv.slice(2));
  const raw = await inputFrom(file);
  if (!raw || typeof raw !== 'object' || Array.isArray(raw) || ![1, 2].includes(raw.version)) throw Object.assign(Error('bounded_manifest_invalid'), { code: 'bounded_manifest_invalid' });
  if (raw.version === 1) {
    if (mode !== '--execute-approved' || confirmHash !== undefined) throw Object.assign(Error('bounded_legacy_execution_only'), { code: 'bounded_legacy_execution_only' });
    if (process.env.SCOUT_BOUNDED_APPROVAL_REF !== raw.approvalRef
      || process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED !== 'false') throw Object.assign(Error('bounded_process_activation_required'), { code: 'bounded_process_activation_required' });
  } else if (MUTATIONS.has(mode) && !confirmHash) throw Object.assign(Error('bounded_exact_confirmation_required'), { code: 'bounded_exact_confirmation_required' });
  if (!process.env.DATABASE_URL || mode === '--execute-approved' && !process.env.GOOGLE_PLACES_KEY) throw Object.assign(Error('bounded_existing_credentials_required'), { code: 'bounded_existing_credentials_required' });

  // Preserve the reviewed db.js connection policy without importing its shared
  // pool or dotenv/app startup. This pool alone belongs to this standalone CLI.
  const { Pool } = require('pg');
  // Confinement is installed before ANY application-service import, even for
  // DB-only commands. Only execute receives or invokes the captive fetch.
  const fetchImpl = require('./transport.cjs').installTransport({ databaseUrl: process.env.DATABASE_URL });
  const pool = new Pool({ connectionString: process.env.DATABASE_URL,
    ssl: String(process.env.DATABASE_SSL || '').toLowerCase() === 'false' ? false : { rejectUnauthorized: false },
    max: 6, connectionTimeoutMillis: 2000, statement_timeout: 30000, query_timeout: 30000, lock_timeout: 2000 });
  const methods = ['log', 'info', 'warn', 'error', 'debug'];
  const previousConsole = Object.fromEntries(methods.map(name => [name, console[name]]));
  // Dependency logs can contain raw SQL/provider failures. This process emits
  // only explicit bounded JSON and fixed reason codes; never those raw logs.
  for (const name of methods) console[name] = () => {};
  try {
    let result;
    if (mode === '--execute-approved') {
      const acquisition = require('../../services/scoutBoundedAcquisition');
      const manifest = acquisition.validateManifest(raw);
      if (raw.version === 2 && confirmHash !== manifest.manifestHash) throw Object.assign(Error('bounded_exact_confirmation_required'), { code: 'bounded_exact_confirmation_required' });
      result = await acquisition.runBoundedAcquisition({ pool, manifest: raw, apiKey: process.env.GOOGLE_PLACES_KEY, fetchImpl });
    } else {
      const authority = require('../../services/scoutBoundedResearchAuthority');
      if (mode === '--preview') result = await authority.previewResearchAuthority(pool, raw);
      else if (mode === '--receipt') result = await authority.inspectResearchAuthority(pool, raw);
      else if (mode === '--install-approved') result = await authority.installResearchAuthority(pool, raw, confirmHash);
      else result = await authority.revokeResearchAuthority(pool, raw, confirmHash);
    }
    if (mode === '--preview' || mode === '--receipt') result = { ...result, manifest: raw };
    await output(result);
  } finally {
    try { await pool.end(); }
    finally { for (const name of methods) console[name] = previousConsole[name]; }
  }
}
if (require.main === module) main().catch(error => { process.stderr.write(STOP + ' Reason: ' + stopCode(error) + '.\n'); process.exitCode = 1; });
module.exports = { main };
