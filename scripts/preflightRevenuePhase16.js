#!/usr/bin/env node
'use strict';

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const pool = require('../db');
const { productionPreflight } = require('../utils/revenuePhase16');

function arg(name) { return process.argv.find(value => value.startsWith(`--${name}=`))?.slice(name.length + 3) || null; }
async function main() {
  const authorizationPath = arg('authorization');
  if (!authorizationPath) throw new Error('Usage: node scripts/preflightRevenuePhase16.js --authorization=/absolute/path.json [--output=/absolute/path.json]');
  const authorization = JSON.parse(fs.readFileSync(path.resolve(authorizationPath), 'utf8'));
  const report = await productionPreflight(pool, authorization);
  const output = arg('output');
  if (output) fs.writeFileSync(path.resolve(output), `${JSON.stringify(report, null, 2)}\n`, { mode: 0o600 });
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  if (report.status !== 'ready_for_authorized_execution') process.exitCode = 2;
}
main().catch(error => { process.stderr.write(`${error.message}\n`); process.exitCode = 1; }).finally(() => pool.end());
