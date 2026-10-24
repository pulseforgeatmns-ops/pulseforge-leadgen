#!/usr/bin/env node
'use strict';

require('dotenv').config();
const pool = require('../db');
const { rebuildProjections } = require('../services/revenueOperations');

function usage() {
  return `Usage:
  npm run revenue:rebuild -- --client-id=10 --dry-run
  npm run revenue:rebuild -- --all-tenants --compare-only [--record] [--from=ISO] [--to=ISO]
  npm run revenue:rebuild -- --client-id=10 --apply [--from=ISO] [--to=ISO]`;
}

function parse(argv) {
  const options = { apply: false, compareOnly: false };
  for (const arg of argv) {
    if (arg === '--dry-run') continue;
    if (arg === '--compare-only') options.compareOnly = true;
    else if (arg === '--record') options.record = true;
    else if (arg === '--apply') options.apply = true;
    else if (arg === '--all-tenants') options.allTenants = true;
    else if (arg.startsWith('--client-id=')) options.clientId = Number(arg.slice(12));
    else if (arg.startsWith('--from=')) options.from = new Date(arg.slice(7)).toISOString();
    else if (arg.startsWith('--to=')) options.to = new Date(arg.slice(5)).toISOString();
    else throw new Error(`Unknown argument: ${arg}`);
  }
  if (options.apply && options.compareOnly) throw new Error('--apply and --compare-only are mutually exclusive');
  if (options.record && !options.compareOnly) throw new Error('--record requires --compare-only');
  if ((!Number.isInteger(options.clientId) || options.clientId <= 0) && !options.allTenants) throw new Error('Choose --client-id=<id> or --all-tenants');
  if (options.clientId && options.allTenants) throw new Error('--client-id and --all-tenants are mutually exclusive');
  if (options.from && options.to && options.from >= options.to) throw new Error('--from must be earlier than --to');
  return options;
}

async function main() {
  try {
    const options = parse(process.argv.slice(2));
    const report = await rebuildProjections(pool, options);
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    process.exitCode = report.status === 'passed' ? 0 : 2;
  } catch (error) {
    process.stderr.write(`${error.message}\n${usage()}\n`);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
}

if (require.main === module) main();

module.exports = { parse };
