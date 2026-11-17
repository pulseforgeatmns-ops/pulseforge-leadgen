'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const {classify,manifest} = require('../scripts/migration/buildSplitManifest');

test('acquisition signals stay with Pulseforge and Signal-owned tables are candidates', () => {
  for (const name of ['prospect_signal_events','warm_signal_state','warm_signal_events','warm_trigger_fires']) {
    assert.equal(classify('public.'+name),'PULSEFORGE_CANDIDATE');
  }
  assert.equal(classify('signal_opportunity_decisions'),'SIGNAL_CANDIDATE');
  assert.equal(classify('public.signal_dashboard_records'),'SIGNAL_CANDIDATE');
  assert.equal(classify('market_observations'),'PULSEFORGE_CANDIDATE');
});

test('snapshot counts and zero crossing FK relationships are exactly reconciled', () => {
  const snapshot=JSON.parse(fs.readFileSync(path.resolve(__dirname,
    '../docs/migration/railway-source-catalog-20261117.json'),'utf8'));
  const output=manifest(snapshot);
  assert.equal(output.status,'PROVISIONAL_UNVERIFIED_NOT_FOR_EXPORT_OR_RESTORE');
  assert.equal(output.physicalTableCount,319);
  assert.equal(output.ownerCounts.SIGNAL_CANDIDATE,45);
  assert.equal(output.ownerCounts.PULSEFORGE_CANDIDATE,274);
  assert.equal(output.crossBoundaryForeignKeys.length,0);
  assert.equal(output.crossBoundaryViewDependencies.length,0);
  assert.ok(output.sharedIdentityAuthDependency);
  assert.equal(output.sourceRelations.length,319);
  assert.ok(output.sourceBytes>9e9);
});

test('crossing FK is flagged, not silently ignored', () => {
  const input={database_bytes:1200,tables:[
    {table_name:'signal_events',schema_name:'public',total_bytes:800,estimated_rows:10},
    {table_name:'prospects',schema_name:'public',total_bytes:400,estimated_rows:20},
  ],foreign_keys:[{source_table:'signal_events',target_table:'prospects',constraint_name:'cross'}]};
  const output=manifest(input);
  assert.deepEqual(output.crossBoundaryForeignKeys,
    [{from:'public.signal_events',to:'public.prospects',constraint:'cross'}]);
});

test('missing FK target and nonpublic schemas abort the manifest', () => {
  const data={database_bytes:1,tables:[{table_name:'signal_events',schema_name:'public',total_bytes:1,estimated_rows:-1}],
    foreign_keys:[{source_table:'signal_events',target_table:'missing',constraint_name:'x'}]};
  assert.throws(()=>manifest(data),/fk_target_missing_from_inventory/);
  data.tables[0].schema_name='elsewhere';
  assert.throws(()=>manifest(data),/non_public_schema_requires_manual_review/);
});

test('negative estimated row counts are not mistaken for real counts', () => {
  const data={database_bytes:1,tables:[{table_name:'foo',schema_name:'public',total_bytes:1,estimated_rows:-1}],foreign_keys:[]};
  assert.equal(manifest(data).sourceRelations[0].estimatedRows,-1);
});
