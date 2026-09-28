#!/usr/bin/env node
'use strict';

const anchor = require('./anchorDailyOutbound');

function normalizeArgs(argv) {
  const supplied = argv.find((arg) => arg.startsWith('--tenant-id='));
  if (supplied) argv = argv.flatMap(arg => arg === supplied ? ['--tenant-id', arg.slice(12)] : [arg]);
  const tenantIndex = argv.indexOf('--tenant-id');
  if (tenantIndex >= 0 && argv[tenantIndex + 1] !== '13') throw new Error('Babrun CLI requires tenant 13');
  if (tenantIndex < 0) argv = [argv[0] || 'status', '--tenant-id', '13', ...argv.slice(1)];
  if (argv.filter(arg => arg === '--tenant-id').length !== 1) throw new Error('Exactly one tenant argument is required');
  return argv;
}

async function run(argv = process.argv.slice(2)) {
  process.env.GOVERNED_OUTBOUND_CLI_TENANT = '13';
  argv = normalizeArgs(argv);
  return anchor.run(argv);
}

if (require.main === module) {
  run().then((r) => console.log(JSON.stringify(r, null, 2))).catch((e) => {
    console.error(JSON.stringify({ error: e.code || e.message }));
    process.exitCode = 1;
  });
}

module.exports = { run, normalizeArgs };
