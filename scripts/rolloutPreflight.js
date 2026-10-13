#!/usr/bin/env node
'use strict';

// SPEC-PF-ACCEL-001: deliberately independent of db.js, dotenv and app/bootstrap.
// Never accept SQL, print connection configuration, or contact a provider here.
const TIMEOUTS = Object.freeze({ connect: 3000, query: 3500, close: 1000 });
const LOCK_TABLES = Object.freeze([
  'users', 'prospects', 'companies', 'ao_leads', 'ao_contacts', 'ao_follow_up_tasks',
  'ao_prospect_activity', 'touchpoints', 'ao_prospect_tasks', 'max_ao_follow_up_tasks',
  'tenant_outreach_suppressions', 'prospect_notes', 'prospect_lifecycle_events',
  'max_spreadsheet_contacts', 'max_spreadsheet_suppressions', 'max_spreadsheet_relationships',
]);
const SHAPES = Object.freeze({
  prospects: { ao_outreach_review_required: 'bool!', ao_call_suppressed: 'bool!', ao_source_address: 'text', website: 'text', ao_current_status: 'text' },
  max_spreadsheet_proposals: { id: 'uuid!', client_id: 'int4!', actor_id: 'int4!', ao_id: 'int4!', conversation_id: 'text!', source_hash: 'text!', digest: 'text!', baseline_hash: 'text!', plan: 'jsonb!', created_at: 'timestamptz!', status: 'text!', receipt: 'jsonb', approved_by: 'int4', approved_at: 'timestamptz' },
  max_spreadsheet_commit_requests: { client_id: 'int4!', idempotency_key: 'text!', request_digest: 'text!', proposal_id: 'uuid!' },
  max_spreadsheet_effects: { client_id: 'int4!', semantic_key: 'text!', proposal_id: 'uuid!', operation: 'jsonb!', observed: 'jsonb!', created_at: 'timestamptz!' },
  max_spreadsheet_contacts: { id: 'uuid!', client_id: 'int4!', prospect_id: 'uuid!', data: 'jsonb!' },
  max_spreadsheet_suppressions: { id: 'uuid!', client_id: 'int4!', prospect_id: 'uuid!', contact_id: 'uuid', channel: 'text!', data: 'jsonb!' },
  max_spreadsheet_relationships: { id: 'uuid!', client_id: 'int4!', prospect_id: 'uuid!', provider_id: 'uuid!', data: 'jsonb!' },
});
const TABLES = Object.freeze([...new Set(['clients', ...LOCK_TABLES, ...Object.keys(SHAPES)])]);
const PRIMARY_KEYS = Object.freeze({
  max_spreadsheet_proposals: ['id'], max_spreadsheet_commit_requests: ['client_id', 'idempotency_key'],
  max_spreadsheet_effects: ['client_id', 'semantic_key'], max_spreadsheet_contacts: ['id'],
  max_spreadsheet_suppressions: ['id'], max_spreadsheet_relationships: ['id'],
});
const UNIQUE_KEYS = Object.freeze({ clients: [['id']], users: [['id'], ['client_id', 'id']], prospects: [['client_id', 'id']] });
const FOREIGN_KEYS = Object.freeze([
  ['max_spreadsheet_proposals', ['client_id'], 'clients', ['id']],
  ['max_spreadsheet_proposals', ['actor_id'], 'users', ['id']],
  ['max_spreadsheet_proposals', ['ao_id'], 'users', ['id']],
  ['max_spreadsheet_proposals', ['approved_by'], 'users', ['id']],
  ['max_spreadsheet_proposals', ['client_id', 'ao_id'], 'users', ['client_id', 'id']],
  ['max_spreadsheet_commit_requests', ['client_id'], 'clients', ['id']],
  ['max_spreadsheet_commit_requests', ['proposal_id'], 'max_spreadsheet_proposals', ['id']],
  ['max_spreadsheet_effects', ['client_id'], 'clients', ['id']],
  ['max_spreadsheet_effects', ['proposal_id'], 'max_spreadsheet_proposals', ['id']],
  ['max_spreadsheet_contacts', ['client_id', 'prospect_id'], 'prospects', ['client_id', 'id']],
  ['max_spreadsheet_suppressions', ['client_id', 'prospect_id'], 'prospects', ['client_id', 'id']],
  ['max_spreadsheet_suppressions', ['contact_id'], 'max_spreadsheet_contacts', ['id']],
  ['max_spreadsheet_relationships', ['client_id', 'prospect_id'], 'prospects', ['client_id', 'id']],
  ['max_spreadsheet_relationships', ['client_id', 'provider_id'], 'prospects', ['client_id', 'id']],
]);
const STATUSES = Object.freeze(['researching', 'ready_to_call', 'call_attempted', 'contacted', 'gatekeeper_reached', 'decision_maker_reached', 'follow_up_needed', 'warm', 'walkthrough_target', 'walkthrough_booked', 'proposal_needed', 'proposal_sent', 'won', 'lost', 'not_a_fit', 'dead', 'application_in_progress']);
const IMMUTABLE_BODY = `BEGIN
 IF (NEW.client_id,NEW.actor_id,NEW.ao_id,NEW.conversation_id,NEW.source_hash,NEW.digest,NEW.baseline_hash,NEW.plan)
 IS DISTINCT FROM (OLD.client_id,OLD.actor_id,OLD.ao_id,OLD.conversation_id,OLD.source_hash,OLD.digest,OLD.baseline_hash,OLD.plan)
 THEN RAISE EXCEPTION 'Spreadsheet proposal is immutable'; END IF;
 IF OLD.status<>'pending' AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'Finalized spreadsheet proposal is immutable'; END IF;
 RETURN NEW;
END`;
const QUERIES = Object.freeze({
  safety: `SELECT current_setting('transaction_read_only') AS read_only,
    current_setting('statement_timeout') AS statement_timeout,
    current_setting('lock_timeout') AS lock_timeout`,
  tables: `SELECT c.relname AS table_name, c.relkind FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='public' AND c.relname=ANY($1::text[])`,
  columns: `SELECT c.relname AS table_name, a.attname AS column_name, t.typname AS type_name,
    a.attnotnull AS not_null, pg_catalog.pg_get_expr(d.adbin,d.adrelid) AS default_expr
    FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
    LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    WHERE n.nspname='public' AND c.relname=ANY($1::text[]) AND a.attnum>0 AND NOT a.attisdropped`,
  constraints: `SELECT r.relname AS table_name, c.contype AS kind, c.convalidated AS validated,
    c.condeferrable AS deferrable, c.confdeltype AS delete_action, c.confupdtype AS update_action,
    c.confmatchtype AS match_type, f.relname AS foreign_table, fn.nspname AS foreign_schema,
    (SELECT count(*)=4 AND COALESCE(bool_and(t.tgisinternal AND t.tgenabled IN ('O','A')),false)
      FROM pg_catalog.pg_trigger t WHERE t.tgconstraint=c.oid) AS triggers_enforced,
    ARRAY(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY k(num,ord)
      JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.num ORDER BY k.ord) AS keys,
    ARRAY(SELECT a.attname FROM unnest(c.confkey) WITH ORDINALITY k(num,ord)
      JOIN pg_catalog.pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=k.num ORDER BY k.ord) AS foreign_keys,
    pg_catalog.pg_get_expr(c.conbin,c.conrelid) AS expression
    FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class r ON r.oid=c.conrelid
    JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
    LEFT JOIN pg_catalog.pg_class f ON f.oid=c.confrelid LEFT JOIN pg_catalog.pg_namespace fn ON fn.oid=f.relnamespace
    WHERE n.nspname='public' AND r.relname=ANY($1::text[])`,
  indexes: `SELECT r.relname AS table_name, i.indisunique AS is_unique, i.indisvalid AS valid,
    i.indisready AS ready, i.indimmediate AS immediate, i.indpred IS NULL AS no_predicate,
    i.indexprs IS NULL AS no_expression,
    ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(num,ord)
      LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num
      WHERE k.ord<=i.indnkeyatts ORDER BY k.ord) AS keys
    FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class r ON r.oid=i.indrelid
    JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
    WHERE n.nspname='public' AND r.relname=ANY($1::text[])`,
  triggers: `SELECT t.tgname AS trigger_name, t.tgenabled AS enabled, t.tgtype AS type,
    t.tgqual IS NULL AS unconditional, t.tgnargs AS argument_count, t.tgattr::text AS column_filter,
    p.proname AS function_name, pn.nspname AS function_schema, p.prosrc AS body,
    l.lanname AS language, p.prosecdef AS security_definer
    FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class r ON r.oid=t.tgrelid
    JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
    JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid JOIN pg_catalog.pg_namespace pn ON pn.oid=p.pronamespace
    JOIN pg_catalog.pg_language l ON l.oid=p.prolang
    WHERE n.nspname='public' AND r.relname='max_spreadsheet_proposals' AND NOT t.tgisinternal`,
});
const HELP = `Usage: node scripts/rolloutPreflight.js [--test-database] [--local-config]
Default: no database/network access. JSON report; exit 0=checked prerequisites ready,
1=blocked, 2=unknown. This never authorizes a deployment.
--test-database reads only PREFLIGHT_TEST_DATABASE_URL (not DATABASE_URL or .env).
Use an explicitly selected disposable/local test database. This flag is an operator
assertion about the target, not a host-based proof. Production execution is outside
this implementation slice's authorization. Never place credentials in CLI arguments.
--local-config checks only local SIGNAL_OPERATOR_FEED_TOKEN validity, without
printing it or contacting services. Cross-service equality remains unknown.`;

const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const compact = s => String(s || '').replace(/\s+/g, '');
function check(id, ok, action) { return { id, status: ok ? 'ready' : 'blocked', ...(ok ? {} : { action }) }; }
function summarize(checks) { return checks.some(c => c.status === 'blocked') ? 'blocked' : checks.some(c => c.status === 'unknown') ? 'unknown' : 'ready'; }
function unique(catalog, table, keys) {
  return catalog.indexes.some(i => i.table_name === table && same(i.keys, keys) && i.is_unique && i.valid && i.ready && i.immediate && i.no_predicate && i.no_expression);
}
// Only recognize the exact PostgreSQL-deparsed shape of the reviewed CHECK.
// Presence of a word inside an unrelated/negated expression is never evidence.
function allowedExpression(column, values, nullable = false) {
  const array = `ARRAY[${values.map(v => `'${v}'::text`).join(',')}]`;
  const membership = `(${column}=ANY(${array}))`;
  return nullable ? `((${column}ISNULL)OR${membership})` : membership;
}
function classifySchema(catalog) {
  const checks = [];
  const fix = 'Review/apply the approved PR897 migration in a separately authorized change; rerun preflight.';
  for (const table of TABLES) {
    checks.push(check(`table.${table}`, catalog.tables.some(r => r.table_name === table && r.relkind === 'r'), fix));
  }
  for (const [table, columns] of Object.entries(SHAPES)) {
    for (const [name, shape] of Object.entries(columns)) {
      const column = catalog.columns.find(r => r.table_name === table && r.column_name === name);
      const ok = column && column.type_name === shape.replace('!', '') && column.not_null === shape.endsWith('!');
      checks.push(check(`column.${table}.${name}`, Boolean(ok), fix));
    }
  }
  for (const name of ['ao_call_suppressed', 'ao_outreach_review_required']) {
    const column = catalog.columns.find(r => r.table_name === 'prospects' && r.column_name === name);
    checks.push(check(`default.prospects.${name}`, compact(column?.default_expr) === 'false', fix));
  }
  for (const [table, name, expected] of [
    ['max_spreadsheet_proposals', 'created_at', 'now()'],
    ['max_spreadsheet_effects', 'created_at', 'now()'],
    ['max_spreadsheet_proposals', 'status', "'pending'::text"],
  ]) {
    const column = catalog.columns.find(r => r.table_name === table && r.column_name === name);
    checks.push(check(`default.${table}.${name}`, compact(column?.default_expr) === expected, fix));
  }
  for (const [table, keys] of Object.entries(PRIMARY_KEYS)) {
    const pk = catalog.constraints.some(c => c.table_name === table && c.kind === 'p' && c.validated && !c.deferrable && same(c.keys, keys));
    checks.push(check(`primary_key.${table}`, pk && unique(catalog, table, keys), fix));
  }
  for (const [table, keysList] of Object.entries(UNIQUE_KEYS)) {
    for (const keys of keysList) checks.push(check(`unique.${table}.${keys.join('+')}`, unique(catalog, table, keys), 'Restore the validated, immediate, nonpartial composite/primary unique index before applying PR897.'));
  }
  for (const [table, keys, foreignTable, foreignKeys] of FOREIGN_KEYS) {
    const ok = catalog.constraints.some(c => c.table_name === table && c.kind === 'f' && c.validated && !c.deferrable
      && same(c.keys, keys) && c.foreign_schema === 'public' && c.foreign_table === foreignTable
      && same(c.foreign_keys, foreignKeys) && c.delete_action === 'a' && c.update_action === 'a' && c.match_type === 's' && c.triggers_enforced);
    checks.push(check(`foreign_key.${table}.${keys.join('+')}`, ok, fix));
  }
  for (const [table, column, expression] of [
    ['prospects', 'ao_current_status', allowedExpression('ao_current_status', STATUSES, true)],
    ['max_spreadsheet_proposals', 'status', allowedExpression('status', ['pending', 'committed', 'superseded'])],
    ['max_spreadsheet_suppressions', 'channel', `(channel='call'::text)`],
  ]) {
    const relevant = catalog.constraints.filter(c => c.table_name === table && c.kind === 'c' && c.keys.includes(column));
    const expected = relevant.find(c => compact(c.expression) === expression && c.validated);
    const knownBlocked = relevant.length === 1 && (compact(relevant[0].expression) === expression && !relevant[0].validated
      || table === 'prospects' && compact(relevant[0].expression) === allowedExpression(column, STATUSES.slice(0, -1), true));
    const status = !relevant.length || knownBlocked ? 'blocked' : expected && relevant.length === 1 ? 'ready' : 'unknown';
    checks.push({ id: `check.${table}.${column}`, status, ...(status === 'ready' ? {} : { action: 'Require the reviewed, validated CHECK definition. Missing, older, extra, or noncanonical checks require migration review; no SQL is evaluated from catalog text.' }) });
  }
  // A constant CHECK (false) has no conkey entries. Do not overlook it (or
  // another unreviewed predicate on newly introduced fields/tables) merely
  // because it does not mention the expected status/channel column.
  for (const table of Object.keys(SHAPES)) {
    const expectedColumn = table === 'prospects' ? 'ao_current_status'
      : table === 'max_spreadsheet_proposals' ? 'status'
        : table === 'max_spreadsheet_suppressions' ? 'channel' : null;
    const extra = catalog.constraints.some(c => c.table_name === table && c.kind === 'c'
      && (!c.keys.length || !expectedColumn || !same(c.keys, [expectedColumn]))
      && (table !== 'prospects' || !c.keys.length || c.keys.some(k => Object.hasOwn(SHAPES.prospects, k))));
    checks.push(extra ? unknown(`check.${table}.additional`, 'Review unexpected constant or additional CHECK predicates on migration fields/tables before relying on readiness.')
      : { id: `check.${table}.additional`, status: 'ready' });
  }
  const trigger = catalog.triggers.find(t => t.trigger_name === 'max_spreadsheet_proposal_immutable');
  checks.push(check('trigger.max_spreadsheet_proposal_immutable', Boolean(trigger && ['O', 'A'].includes(trigger.enabled)
    && trigger.type === 19 && trigger.unconditional && trigger.argument_count === 0 && trigger.column_filter === ''
    && trigger.function_schema === 'public' && trigger.function_name === 'max_spreadsheet_proposal_immutable'
    && trigger.language === 'plpgsql' && !trigger.security_definer && compact(trigger.body) === compact(IMMUTABLE_BODY)), fix));
  return { status: summarize(checks), checks };
}
function localConfig(env) {
  const token = env.SIGNAL_OPERATOR_FEED_TOKEN;
  return { id: 'signal.local_token', status: typeof token === 'string' && token.length >= 32 ? 'ready' : 'blocked',
    scope: 'this process only; length check, not entropy or service equality',
    action: 'Verify a >=32-character shared token separately in both services; never copy values or hashes into reports.' };
}
function unknown(id, action) { return { id, status: 'unknown', action }; }
function report(schema, config) {
  return {
    format: 'pulseforge-rollout-preflight/v1', status: summarize([schema, ...config]),
    scope: 'inspected prerequisites only; not deployment approval', schema, local_config: config,
    rollout: { status: 'unknown', checks: [
      unknown('workbook.staging_contention', 'Stage concurrent tenant/outbound traffic against the actual 16-table commit lock; bounded correctness tests alone do not prove acceptable load.'),
      unknown('signal.paired_configuration', 'Confirm token validity and equality in both services using an approved secure operator procedure.'),
      unknown('signal.collector_feed_compatibility', 'Deploy the bearer-auth collector and authenticated feed as a coordinated reviewed change; this command never requests /health or /feed because they can poll Telegram.'),
      unknown('deployment.autodeploy', 'Review current main/caller auto-deploy, CI gating, migration ordering and rollback before any merge. This command does not inspect Railway.'),
      unknown('production.approval', 'Obtain explicit approval for production reads, migrations, configuration, merges and deployments as applicable.'),
    ] },
  };
}
function safeCode(error) {
  if (error?.code === 'PREFLIGHT_TIMEOUT' || error?.code === '57014' || error?.code === '55P03') return 'timeout';
  if (error?.code === '28P01' || error?.code === '28000') return 'authentication_failed';
  if (error?.code === '42501') return 'catalog_permission_denied';
  if (error?.code === 'PREFLIGHT_READ_ONLY') return 'read_only_not_verified';
  return 'database_check_failed';
}
function failure(code) { const e = new Error('Preflight failed'); e.code = code; return e; }
async function bounded(promise, ms) {
  let timer;
  try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(failure('PREFLIGHT_TIMEOUT')), ms); })]); }
  finally { clearTimeout(timer); }
}
function connectionConfig(value) {
  // Parse into explicit pg fields: URL query parameters must not override our
  // read-only startup settings, install SSL files, or select another target.
  const url = new URL(value);
  if (!['postgres:', 'postgresql:'].includes(url.protocol) || !url.hostname || !url.username || url.search || url.hash
    || url.pathname.length < 2 || url.pathname.slice(1).includes('/')) throw failure('PREFLIGHT_INPUT');
  return { host: url.hostname.replace(/^\[|\]$/g, ''), port: Number(url.port || 5432),
    user: decodeURIComponent(url.username), password: () => decodeURIComponent(url.password),
    database: decodeURIComponent(url.pathname.slice(1)), client_encoding: 'UTF8', replication: 'false',
    ssl: ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname) ? false : { rejectUnauthorized: true },
    application_name: 'pulseforge_rollout_preflight', connectionTimeoutMillis: TIMEOUTS.connect,
    query_timeout: TIMEOUTS.query, options: '-c default_transaction_read_only=on -c statement_timeout=3000 -c lock_timeout=500 -c idle_in_transaction_session_timeout=5000 -c search_path=pg_catalog' };
}
async function inspectDatabase(value, { Client, timeouts = TIMEOUTS } = {}) {
  let client;
  let schema;
  let connected = false;
  const catalog = {};
  try {
    const config = connectionConfig(value);
    const PgClient = Client || require('pg').Client;
    client = new PgClient(config);
    client.on('error', () => {}); // Never let a driver error dump a secret-bearing stack.
    await bounded(client.connect(), timeouts.connect);
    connected = true;
    await bounded(client.query('BEGIN READ ONLY'), timeouts.query);
    for (const [key, text] of Object.entries(QUERIES)) {
      const result = await bounded(client.query({ text, values: ['safety', 'triggers'].includes(key) ? [] : [TABLES], query_timeout: timeouts.query }), timeouts.query);
      if (key === 'safety') {
        const row = result.rows[0];
        if (row?.read_only !== 'on' || row.statement_timeout !== '3s' || row.lock_timeout !== '500ms') throw failure('PREFLIGHT_READ_ONLY');
      } else catalog[key] = result.rows;
    }
    schema = classifySchema(catalog);
  } catch (error) {
    schema = { status: 'unknown', checks: [unknown(safeCode(error), 'Database evidence is incomplete. Check test-target selection, credentials, permissions and timeouts privately, then retry; no raw error details are emitted.')] };
  } finally {
    if (client) {
      let cleanupFailed = false;
      if (connected) {
        try { await bounded(client.query('ROLLBACK'), timeouts.close); }
        catch { cleanupFailed = true; }
      }
      try { await bounded(client.end(), timeouts.close); }
      catch { cleanupFailed = true; }
      // pg.Client.end() destroys its socket when a query is active. If its end
      // callback stalls, explicitly close our owned socket, never a shared pool.
      if (client.connection?.stream && !client.connection.stream.destroyed) client.connection.stream.destroy();
      if (cleanupFailed) schema = { status: 'unknown', checks: [unknown('connection_cleanup_failed', 'The owned connection was closed; cleanup acknowledgement was not verified. Retry before relying on schema readiness.')] };
    }
  }
  return schema;
}
function parseArgs(args) {
  if (args.includes('--help') || args.includes('-h')) return { help: true };
  if (args.some(arg => !['--test-database', '--local-config'].includes(arg)) || new Set(args).size !== args.length) throw failure('PREFLIGHT_INPUT');
  return { database: args.includes('--test-database'), config: args.includes('--local-config') };
}
async function main(args = process.argv.slice(2), env = process.env, dependencies = {}) {
  let options;
  try { options = parseArgs(args); }
  catch { return { exitCode: 2, output: JSON.stringify({ status: 'unknown', error: 'invalid_arguments', action: 'Use --help; never pass URLs, tokens or SQL as arguments.' }) }; }
  if (options.help) return { exitCode: 0, output: HELP };
  const schema = options.database
    ? env.PREFLIGHT_TEST_DATABASE_URL ? await inspectDatabase(env.PREFLIGHT_TEST_DATABASE_URL, dependencies)
      : { status: 'unknown', checks: [unknown('test_database_not_selected', 'Set PREFLIGHT_TEST_DATABASE_URL privately to an explicitly selected test database.')] }
    : { status: 'unknown', checks: [unknown('database_not_requested', 'Default dry run: use --test-database for an explicitly selected disposable/local test database.')] };
  const result = report(schema, options.config ? [localConfig(env)] : []);
  return { exitCode: { ready: 0, blocked: 1, unknown: 2 }[result.status], output: JSON.stringify(result, null, 2) };
}
if (require.main === module) main().then(result => { console.log(result.output); process.exitCode = result.exitCode; }, () => {
  console.log(JSON.stringify({ status: 'unknown', error: 'preflight_failed' })); process.exitCode = 2;
});
module.exports = { TIMEOUTS, TABLES, LOCK_TABLES, SHAPES, PRIMARY_KEYS, UNIQUE_KEYS, FOREIGN_KEYS, STATUSES, IMMUTABLE_BODY, QUERIES, classifySchema, allowedExpression, connectionConfig, inspectDatabase, localConfig, main };
