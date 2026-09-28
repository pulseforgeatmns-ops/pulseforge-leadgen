#!/usr/bin/env node
'use strict';

const anchor = require('./anchorDailyOutbound');

async function run(argv = process.argv.slice(2)) {
  process.env.GOVERNED_OUTBOUND_CLI_TENANT = '13';
  const tenantFlag = argv.find((arg) => arg.startsWith('--tenant-id='));
  if (!tenantFlag) argv = ['--tenant-id=13', ...argv];
  return anchor.run(argv);
}

if (require.main === module) {
  run().then((r) => console.log(JSON.stringify(r, null, 2))).catch((e) => {
    console.error(JSON.stringify({ error: e.code || e.message }));
    process.exitCode = 1;
  });
}

module.exports = { run };
