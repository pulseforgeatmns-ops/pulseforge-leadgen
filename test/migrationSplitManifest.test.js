'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const {classify,manifest} = require('../scripts/migration/buildSplitManifest');

test('Pulseforge acquisition signals are not Signal product tables', () => {
  assert.equal(classify('prospect_signal_events'), 'pulseforge');
  assert.equal(classify('warm_signal_state'), 'pulseforge');
  assert.equal(classify('signal_opportunity_decisions'), 'signal_provisional');
});
test('manifest lists cross-boundary FK and does not mark migration approved', () => {
  const input = {database:'railway', postgres_version:'18.6', database_bytes:1200,
    tables:[
      {table_name:'signal_events',schema_name:'public',total_bytes:800,estimated_rows:10},
      {table_name:'prospects',schema_name:'public',total_bytes:400,estimated_rows:20},
    ],
    foreign_keys:[{source_table:'signal_events',target_table:'prospects',constraint_name:'bad_fk'}] };
  const output=manifest(input);
  assert.equal(output.status,'PROVISIONAL_UNVERIFIED_NOT_FOR_EXPORT');
  assert.equal(output.provisionalSignalBytes,800);
  assert.equal(output.provisionalPulseforgeBytes,400);
  assert.deepEqual(output.crossBoundaryForeignKeys,[{from:'signal_events',to:'prospects',constraint:'bad_fk'}]);
});
test('missing FK target and nonpublic schema cause fail-closed error', () => {
  const input={database_bytes:1,tables:[{table_name:'signal_events',schema_name:'public',total_bytes:1,estimated_rows:-1}],
    foreign_keys:[{source_table:'signal_events',target_table:'missing',constraint_name:'x'}]};
  assert.throws(()=>manifest(input),/fk_target_missing_from_inventory/);
  input.tables[0].schema_name='nonpublic';
  assert.throws(()=>manifest(input),/non_public_schema_requires_manual_review/);
});
