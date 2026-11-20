#!/usr/bin/env node
'use strict';
// Offline metadata-only manifest. Output is NEVER a pg_restore table selection.
const fs = require('node:fs');

const PULSEFORGE_OVERRIDES = new Set([
  'prospect_signal_events', 'warm_signal_state', 'warm_signal_events',
  'warm_trigger_fires',
]);

function classify(fullName) {
  const name = String(fullName).split('.').at(-1);
  if (PULSEFORGE_OVERRIDES.has(name)) return 'PULSEFORGE_CANDIDATE';
  if (name.startsWith('signal_')) return 'SIGNAL_CANDIDATE';
  return 'PULSEFORGE_CANDIDATE';
}

function normalize(inventory) {
  if (!inventory || typeof inventory !== 'object') throw Error('invalid_inventory');
  const raw = inventory.migration_inventory || inventory;
  const objects = raw.objects || raw.tables;
  const fks = raw.foreignKeys || raw.foreign_keys;
  if (!Array.isArray(objects) || !Array.isArray(fks)) throw Error('invalid_inventory');
  const tables = objects.filter(t => !t.relkind || t.relkind === 'r' || t.relkind === 'p')
    .map(t => ({name: (t.schema_name || 'public') + '.' + t.table_name,
      schema: t.schema_name || 'public', bytes: Number(t.total_bytes),
      estimate: Number(t.estimated_rows), parent: t.parent_table || null }));
  if (tables.some(t => t.schema !== 'public')) throw Error('non_public_schema_requires_manual_review');
  if (new Set(tables.map(t => t.name)).size !== tables.length) throw Error('duplicate_table_name');
  if (tables.some(t => !Number.isFinite(t.bytes) || t.bytes < 0 ||
      !Number.isFinite(t.estimate))) throw Error('invalid_table_size_or_estimate');
  const foreignKeys = fks.map(f => ({
    from: f.source_table.includes('.') ? f.source_table : (f.source_schema || 'public') + '.' + f.source_table,
    to: f.target_table.includes('.') ? f.target_table : (f.target_schema || 'public') + '.' + f.target_table,
    constraint: f.constraint_name,
  }));
  const viewDependencies = (raw.viewDependencies || []).map(d => ({
    view: d.view_name, table: d.referenced_table
  }));
  return {raw, tables, foreignKeys, viewDependencies};
}

function manifest(inventory) {
  const {raw, tables, foreignKeys, viewDependencies} = normalize(inventory);
  const names = new Set(tables.map(t => t.name));
  if (foreignKeys.some(fk => !names.has(fk.from) || !names.has(fk.to)))
    throw Error('fk_target_missing_from_inventory');
  const crossFKs = foreignKeys.filter(fk => classify(fk.from) !== classify(fk.to));
  const crossViews = viewDependencies.filter(d => classify(d.view) !== classify(d.table));
  const mapped = tables.map(t => ({table:t.name, proposedOwner:classify(t.name),
    sourceBytes:t.bytes, estimatedRows:t.estimate, parent:t.parent})).sort((a,b)=>a.table.localeCompare(b.table));
  const totals = {PULSEFORGE_CANDIDATE:0,SIGNAL_CANDIDATE:0};
  const counts = {PULSEFORGE_CANDIDATE:0,SIGNAL_CANDIDATE:0};
  for (const table of mapped) { totals[table.proposedOwner] += table.sourceBytes; counts[table.proposedOwner]++; }
  const authCoupling = names.has('public.users') &&
      names.has('public.signal_dashboard_session');
  return {
    status:'PROVISIONAL_UNVERIFIED_NOT_FOR_EXPORT_OR_RESTORE',
    capturedAt:raw.capturedAt || null,
    sourceVersion:raw.sourceVersion || raw.postgres_version || null,
    sourceDatabase:raw.sourceDb || raw.database || null,
    sourceBytes:Number(raw.databaseBytes ?? raw.database_bytes),
    physicalTableCount:mapped.length,
    ownerCounts:counts,
    ownerBytes:totals,
    crossBoundaryForeignKeys:crossFKs,
    crossBoundaryViewDependencies:crossViews,
    sharedIdentityAuthDependency:authCoupling ? {
      from:'standalone/signal-dashboard/app.js',
      tables:['public.users','public.signal_dashboard_session'],
      issue:'Signal login still queries Pulseforge users table; no FK is required for this runtime dependency.'
    } : null,
    sourceRelations:mapped,
    warnings:[
      'Candidate ownership inferred from names; not a restore selection or authorization.',
      'A zero cross-boundary FK count does not prove program-level isolation.',
      'Auth, SQL functions, sequences, session stores and runtime joins require review.',
      'Estimated rows are not exact counts; negative estimates do not imply emptiness.',
      'Source metadata collected after backup may differ from backup schema.',
      'A real restore needs adequate isolated capacity and independent backup verification.'
    ],
  };
}

if (require.main === module) {
  const path = process.argv[2];
  if (!path) { console.error('Usage: node scripts/migration/buildSplitManifest.js <catalog.json>'); process.exit(2); }
  let data = JSON.parse(fs.readFileSync(path,'utf8'));
  process.stdout.write(JSON.stringify(manifest(data),null,2)+'\n');
}
module.exports = { classify, normalize, manifest };
