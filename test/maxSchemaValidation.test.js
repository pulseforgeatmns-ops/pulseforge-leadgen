const assert = require('node:assert/strict');
const test = require('node:test');
const { REQUIRED_COLUMNS, REQUIRED_COLUMN_TYPES, REQUIRED_CONSTRAINTS, REQUIRED_INDEXES, REQUIRED_JSON_DEFAULTS, REQUIRED_TABLES, REQUIRED_TRIGGERS, validateSchema } = require('../scripts/validateMaxOrchestrationSchema');

test('schema smoke validator confirms required objects without modifying status', async () => {
  const defaults = new Set(REQUIRED_JSON_DEFAULTS);
  const columnRows = Object.entries(REQUIRED_COLUMNS).flatMap(([table_name, names]) => names.map(column_name => {
    const name = `${table_name}.${column_name}`;
    return { table_name, column_name, data_type: REQUIRED_COLUMN_TYPES[name] || 'text', column_default: defaults.has(name) ? "'{}'::jsonb" : null };
  }));
  for (const [name, data_type] of Object.entries(REQUIRED_COLUMN_TYPES)) {
    const [table_name, column_name] = name.split('.');
    if (!columnRows.some(row => row.table_name===table_name && row.column_name===column_name)) columnRows.push({ table_name, column_name, data_type, column_default: defaults.has(name) ? "'{}'::jsonb" : null });
  }
  for (const name of REQUIRED_JSON_DEFAULTS) {
    const [table_name, column_name] = name.split('.');
    if (!columnRows.some(row => row.table_name===table_name && row.column_name===column_name)) columnRows.push({ table_name, column_name, data_type:'jsonb', column_default:"'{}'::jsonb" });
  }
  let call = 0;
  const db = { async query() {
    call++;
    if (call === 1) return { rows: columnRows };
    if (call === 2) return { rows: REQUIRED_TABLES.map(table_name => ({ table_name })) };
    if (call === 3) return { rows: REQUIRED_INDEXES.map(indexname => ({ indexname })) };
    if (call === 4) return { rows: REQUIRED_CONSTRAINTS.map(constraint_name => ({
      constraint_name,
      contype: constraint_name.endsWith('_client_fk') ? 'f' : 'c',
      convalidated: true,
      definition: constraint_name.endsWith('_client_fk') ? 'FOREIGN KEY (client_id) REFERENCES clients(id)' : 'CHECK (true)',
    })) };
    if (call === 5) return { rows: REQUIRED_TRIGGERS.map(trigger_name => ({ trigger_name })) };
    return { rows: [{ total: 100, null_status: 0, values: ['cold', 'warm'] }] };
  } };
  const report = await validateSchema(db);
  assert.equal(report.valid, true);
  assert.equal(report.status_mutated, false);
  assert.equal(report.operational_status_fingerprint.total, 100);
});
