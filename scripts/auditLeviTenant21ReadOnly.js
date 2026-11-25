#!/usr/bin/env node
'use strict';
// Isolated Levi tenant 21 audit. Never loads server.js, Scout, or application bootstrap.
// Requires a dedicated SELECT-only PostgreSQL credential in AUDIT_DATABASE_URL.
// No contact names, addresses, emails, phones, URLs, or raw metadata are emitted.
const { Client } = require('pg');
const TENANT = '21';
const TABLES = ['prospects', 'companies', 'scout_research_only_cycles', 'scout_research_candidates'];
const SAFE_GROUP_FIELDS = ['source', 'status', 'qualification_status', 'research_status', 'contact_status', 'outreach_status', 'setter_visible', 'outreach_authorized', 'campaign_enrollment_authorized'];
const IDENT = /^[a-z_][a-z0-9_]*$/;
const quote = value => {
  if (!IDENT.test(value)) throw new Error('invalid_identifier');
  return '"' + value + '"';
};
function chooseTenantColumn(cols) {
  return ['client_id', 'tenant_id'].find(x => cols.includes(x));
}
async function inspect(client, table) {
  const { rows } = await client.query(
    'SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1 ORDER BY ordinal_position',
    [table]
  );
  const cols = rows.map(r => r.column_name);
  if (!cols.length) return { table, available: false };
  const tenantColumn = chooseTenantColumn(cols);
  if (!tenantColumn) return { table, available: true, tenantScoped: false, columns: cols.filter(x => SAFE_GROUP_FIELDS.includes(x)) };
  const where = quote(tenantColumn) + '::text = $1';
  const { rows: counts } = await client.query('SELECT count(*)::integer AS total FROM ' + quote(table) + ' WHERE ' + where, [TENANT]);
  const groups = {};
  for (const field of SAFE_GROUP_FIELDS.filter(x => cols.includes(x))) {
    const { rows: grouped } = await client.query(
      'SELECT left(coalesce(' + quote(field) + '::text, \'<null>\'), 80) AS value, count(*)::integer AS count FROM ' +
      quote(table) + ' WHERE ' + where + ' GROUP BY 1 ORDER BY count DESC, value LIMIT 30', [TENANT]
    );
    // Only aggregate group values for categorical/status fields. Never group free-form provenance text.
    groups[field] = grouped;
  }
  const completeness = {};
  if (table === 'prospects') {
    for (const field of ['email', 'phone', 'linkedin_url', 'assigned_ao_id', 'company_id'].filter(x => cols.includes(x))) {
      const { rows: result } = await client.query(
        'SELECT count(*) FILTER (WHERE nullif(btrim(' + quote(field) + '::text), \'\') IS NOT NULL)::integer AS populated FROM ' +
        quote(table) + ' WHERE ' + where, [TENANT]
      );
      completeness[field] = result[0].populated;
    }
  }
  return { table, available: true, tenantScoped: true, tenantColumn, total: counts[0].total, groups, completeness,
    auditedColumns: cols.filter(x => SAFE_GROUP_FIELDS.includes(x) || ['email','phone','linkedin_url','assigned_ao_id','company_id'].includes(x)) };
}
async function main() {
  if (process.argv.slice(2).join(' ') !== '--tenant 21 --confirm-read-only') throw new Error('explicit_tenant_confirmation_required');
  const url = process.env.AUDIT_DATABASE_URL;
  if (!url) throw new Error('audit_database_url_required');
  if (process.env.DATABASE_URL && url === process.env.DATABASE_URL) throw new Error('application_database_credential_forbidden');
  const client = new Client({ connectionString: url, connectionTimeoutMillis: 10000, application_name: 'levi_tenant21_readonly_audit' });
  let inTx = false;
  try {
    await client.connect();
    await client.query('BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ');
    inTx = true;
    await client.query("SET LOCAL statement_timeout = '10000ms'");
    await client.query("SET LOCAL lock_timeout = '2000ms'");
    const role = await client.query("SELECT rolsuper, rolcreaterole, rolcreatedb FROM pg_roles WHERE rolname = current_user");
    if (!role.rows.length || role.rows[0].rolsuper || role.rows[0].rolcreaterole || role.rows[0].rolcreatedb) throw new Error('privileged_audit_role_forbidden');
    const report = { audit: 'levi_tenant21', tenant: TENANT, readOnly: true, generatedAt: new Date().toISOString(), tables: [] };
    for (const table of TABLES) report.tables.push(await inspect(client, table));
    await client.query('COMMIT');
    inTx = false;
    process.stdout.write(JSON.stringify(report, null, 2) + '\n');
  } finally {
    if (inTx) await client.query('ROLLBACK').catch(() => {});
    await client.end().catch(() => {});
  }
}
if (require.main === module) main().catch(e => {
  const known = ['explicit_tenant_confirmation_required', 'audit_database_url_required', 'application_database_credential_forbidden', 'privileged_audit_role_forbidden'];
  console.error(JSON.stringify({ status: 'blocked', error: known.includes(e.message) ? e.message : 'audit_unavailable' }));
  process.exitCode = 1;
});
module.exports = { chooseTenantColumn, inspect };
