#!/usr/bin/env node
'use strict';
// Audit-only. Consumes the JSON returned by railwaySourceInventoryOneQuery.sql.
// No credentials, DB connections, exports, or data writes.
const fs = require('node:fs');

function classify(name) {
  // Explicitly confirmed Pulseforge marketing/prospect signals, not the Signal product.
  if (['prospect_signal_events', 'warm_signal_state', 'warm_signal_events', 'warm_trigger_fires'].includes(name))
    return 'pulseforge';
  if (name.startsWith('signal_')) return 'signal_provisional';
  return 'pulseforge_provisional';
}

function manifest(inventory) {
  const tables = inventory.tables;
  const foreignKeys = inventory.foreign_keys;
  if (!Array.isArray(tables) || !Array.isArray(foreignKeys)) throw Error('invalid_inventory');
  const tableNames = new Set(tables.map(t => t.table_name));
  if (tableNames.size !== tables.length) throw Error('duplicate_table_name');
  if (tables.some(t => t.schema_name !== 'public')) throw Error('non_public_schema_requires_manual_review');
  const mapped = tables.map(t => ({ table: t.table_name, proposedOwner: classify(t.table_name),
    sourceBytes: Number(t.total_bytes), estimatedRows: Number(t.estimated_rows) }));
  const sourceTotal = Number(inventory.database_bytes);
  const signalBytes = mapped.filter(t => t.proposedOwner === 'signal_provisional')
    .reduce((n,t)=>n+t.sourceBytes,0);
  const crosses = foreignKeys.filter(fk => {
    if (!tableNames.has(fk.source_table) || !tableNames.has(fk.target_table))
      throw Error('fk_target_missing_from_inventory');
    return classify(fk.source_table).split('_')[0] !== classify(fk.target_table).split('_')[0];
  }).map(fk=>({from:fk.source_table,to:fk.target_table,constraint:fk.constraint_name}));
  const anomalies = mapped.filter(t => !Number.isFinite(t.sourceBytes) || t.sourceBytes < 0 ||
    !Number.isFinite(t.estimatedRows));
  if (anomalies.length) throw Error('invalid_table_size_or_estimate');
  return {
    status: 'PROVISIONAL_UNVERIFIED_NOT_FOR_EXPORT',
    database: inventory.database,
    sourceVersion: inventory.postgres_version,
    sourceBytes: sourceTotal,
    provisionalSignalBytes: signalBytes,
    provisionalPulseforgeBytes: mapped.filter(t=>t.proposedOwner!=='signal_provisional')
      .reduce((n,t)=>n+t.sourceBytes,0),
    crossBoundaryForeignKeys: crosses,
    sourceRelations: mapped,
    warnings: [
      'Not a pg_dump table filter: validate triggers, views, functions, partitions, JSON references and application joins.',
      'PostgreSQL reltuples row estimates can be negative and are not actual counts.',
      'Check restore capacity, database major version and offsite backup before migration.',
    ],
  };
}

if (require.main === module) {
  const inputPath = process.argv[2];
  if (!inputPath) {
    console.error('Usage: node scripts/migration/buildSplitManifest.js /path/to/inventory.json');
    process.exit(2);
  }
  // Accept either the JSON document itself or Railway Query's {migration_inventory:...} form.
  let inv = JSON.parse(fs.readFileSync(inputPath,'utf8'));
  if (inv.migration_inventory) inv = inv.migration_inventory;
  process.stdout.write(JSON.stringify(manifest(inv),null,2)+'\n');
}
module.exports = { classify,manifest };
