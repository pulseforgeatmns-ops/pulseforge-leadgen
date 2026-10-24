'use strict';
// SPEC-264. Standalone one-shot process only; no route, cron or shared app pool.
function reject() { throw Object.assign(new Error('anchor_crm_invocation_rejected'), { code: 'anchor_crm_invocation_rejected' }); }
function parse(argv) {
  const values = {}, allowed = new Set(['--tenant', '--revision', '--manifest-sha256', '--source-sha256']);
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === '--apply' && !values.apply) { values.apply = true; continue; }
    if (!allowed.has(key) || Object.hasOwn(values, key) || !argv[i + 1] || argv[i + 1].startsWith('--')) reject();
    values[key] = argv[++i];
  }
  if (values['--tenant'] !== '10' || !/^[a-f0-9]{40}$/.test(values['--revision'] || '')
    || ['--manifest-sha256', '--source-sha256'].some(k => !/^[a-f0-9]{64}$/.test(values[k] || ''))) reject();
  return values;
}
async function run(argv, env = process.env) {
  const args = parse(argv);
  if (env.RAILWAY_GIT_COMMIT_SHA !== args['--revision'] || !env.DATABASE_URL) reject();
  const { Pool } = require('pg');
  // Same existing app TLS selection; credentials are never accepted in flags/output.
  const pool = new Pool({ connectionString: env.DATABASE_URL,
    ssl: String(env.DATABASE_SSL || '').toLowerCase() === 'false' ? false : { rejectUnauthorized: false },
    connectionTimeoutMillis: 6000, query_timeout: 6000, max: 8 });
  pool.on('error', () => {}); // Raw driver diagnostics must not expose connection data.
  let db, begun = false, applyStarted = false;
  try {
    db = await pool.connect();
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY'); begun = true;
    await db.query("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='5s'");
    const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
    const recovery = require('../services/anchorEmailRecovery');
    const state = await recovery.loadRecoveryState(db, { env, now: new Date(), lockControl: false });
    if (state.mode !== 'scoped_resume' || hash(state.manifest) !== args['--manifest-sha256']) reject();
    const manifest = state.manifest;
    if (manifest.tenantId !== '10' || manifest.scopes.email.enabled || manifest.scopes.research.enabled
      || manifest.scopes.preparation.enabled !== true || !manifest.scopes.preparation.operations.includes('prepare_existing_crm')
      || manifest.admissions.length < 1 || manifest.admissions.length > 5) reject();
    const programRows = (await db.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2', [manifest.programId, '10'])).rows;
    if (programRows.length !== 1) reject();
    const program = programRows[0];
    recovery.validateLivePolicy(program, manifest, new Date());
    if (program.source_mission_id !== manifest.sourceMissionId || program.scope_hash !== manifest.scopeHash) reject();
    const sources = (await db.query('SELECT id,tenant_id,payload FROM acquisition_missions WHERE id=$1 AND tenant_id=$2', [manifest.sourceMissionId, '10'])).rows;
    const source = sources[0];
    if (sources.length !== 1 || source.id !== manifest.sourceMissionId || source.tenant_id !== '10'
      || source.payload?.id !== source.id || String(source.payload?.tenantId) !== '10'
      || source.payload?.stage !== 'discover' || source.payload?.structuredMission?.immutable !== true
      || source.payload?.planCancelled || /cancel/i.test(String(source.payload?.status))
      || hash(missionScope(source.payload)) !== manifest.scopeHash || hash(source.payload) !== args['--source-sha256']) reject();
    await require('../services/anchorEmailQuarantine').requireQuarantine(db, manifest.quarantineDecisionHash);
    await db.query('COMMIT'); begun = false;
    db.release(); db = null;
    const summary = { mode: 'read_only', tenantId: '10', manifestHash: args['--manifest-sha256'],
      sourceHash: args['--source-sha256'], candidateCount: manifest.admissions.length,
      manifestAndSourceValidated: true, completeEligibilityChecked: false, providerReadiness: 'not_checked', writesAttempted: false };
    if (!args.apply) return summary;
    if (env.RAILWAY_GIT_COMMIT_SHA !== args['--revision'] || env.ANCHOR_EMAIL_SCOPED_RESUME_ID !== manifest.id) reject();
    // Never create activation. Pin its already-present ID, while preserving every
    // other live environment field (especially maintenance/stop signals).
    const pinnedEnv = new Proxy(env, { get(target, key) {
      if (key === 'ANCHOR_EMAIL_SCOPED_RESUME_ID' && target[key] !== manifest.id) return undefined;
      return Reflect.get(target, key);
    }, set() { reject(); } });
    applyStarted = true;
    const result = await require('../services/anchorScopedResumeCrmPreparation').prepareExistingCrm(pool,
      { programId: manifest.programId, sourceMissionHash: args['--source-sha256'], admissions: manifest.admissions }, { env: pinnedEnv });
    if (result.readyForReview !== true || result.executionApproved !== false || result.envelopeAuthorized !== false
      || result.sent !== 0 || result.providerCalls !== 0 || !/^mission_crm_[a-f0-9]{32}$/.test(result.missionId || '')
      || result.candidateCount !== manifest.admissions.length) reject();
    return { ...summary, mode: 'applied', writesAttempted: true, completeEligibilityChecked: true,
      missionId: result.missionId, readyForReview: true, executionApproved: false, envelopeAuthorized: false, sent: 0, providerCalls: 0 };
  } catch (error) {
    const safe = Object.assign(new Error('anchor_crm_invocation_rejected'), { code: 'anchor_crm_invocation_rejected', writesMayHaveOccurred: applyStarted });
    throw safe;
  } finally {
    if (begun && db) await db.query('ROLLBACK').catch(() => {});
    if (db) db.release(true);
    try { await pool.end(); } catch { throw Object.assign(new Error('anchor_crm_invocation_rejected'),
      { code: 'anchor_crm_invocation_rejected', writesMayHaveOccurred: applyStarted }); } // Private one-shot pool only.
  }
}
async function cli(argv = process.argv.slice(2)) {
  const output = process.stdout.write.bind(process.stdout);
  // Suppress nested library/debug/driver output in this private process. Only the
  // fixed summary below is emitted; no copy, contact, sender, secrets or raw error.
  process.stdout.write = process.stderr.write = () => true;
  try { output(JSON.stringify(await run(argv)) + '\n'); }
  catch (error) { output(JSON.stringify({ status: 'rejected', code: 'anchor_crm_invocation_rejected',
    writesMayHaveOccurred: error.writesMayHaveOccurred === true }) + '\n'); process.exitCode = 1; }
}
if (require.main === module) void cli();
module.exports = { run, parse, cli };
