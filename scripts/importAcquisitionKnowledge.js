'use strict';

/**
 * SPEC-247 acquisition knowledge import.
 *
 * Usage:
 *   node scripts/importAcquisitionKnowledge.js --tenant-id=10 --file=./babrun-ak.json
 *   node scripts/importAcquisitionKnowledge.js --tenant-id=10 --file=./babrun-ak.json --apply
 */

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const pool = require('../db');
const { importKnowledge } = require('../services/acquisitionKnowledge');

function parseArgs(argv) {
  return argv.reduce((acc, arg) => {
    if (arg === '--apply') {
      acc.apply = true;
      return acc;
    }
    const match = /^--([^=]+)=(.*)$/.exec(arg);
    if (match) acc[match[1]] = match[2];
    return acc;
  }, { apply: false });
}

function usage(message) {
  if (message) console.error(message);
  console.error('Usage: node scripts/importAcquisitionKnowledge.js --tenant-id=<tenant> --file=<path> [--source-name=<name>] [--apply]');
  process.exit(1);
}

function readImportFile(filePath) {
  const absolute = path.resolve(process.cwd(), filePath);
  const parsed = JSON.parse(fs.readFileSync(absolute, 'utf8'));
  if (Array.isArray(parsed)) return { objects: parsed, sourceName: path.basename(absolute) };
  if (parsed && Array.isArray(parsed.objects)) {
    return {
      objects: parsed.objects,
      sourceName: parsed.sourceName || parsed.source || path.basename(absolute),
    };
  }
  usage('Import file must be a JSON array or an object with an objects array.');
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args['tenant-id']) usage('--tenant-id is required.');
  if (!args.file) usage('--file is required.');

  const input = readImportFile(args.file);
  const result = await importKnowledge({
    tenantId: args['tenant-id'],
    sourceName: args['source-name'] || input.sourceName,
    objects: input.objects,
    apply: args.apply === true,
  }, {
    pool,
    actor: { id: process.env.USER || 'operator', role: 'operator' },
  });

  console.log(JSON.stringify({
    spec: result.spec,
    dryRun: result.dryRun,
    tenantId: result.tenantId,
    objectCount: result.objectCount,
    ids: (result.objects || []).map((row) => row.id),
  }, null, 2));
}

main()
  .catch((err) => {
    console.error(err.stack || err.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
