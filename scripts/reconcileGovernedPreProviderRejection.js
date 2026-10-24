'use strict';

const { reconcileUncertainItemFromEvidence } = require('../services/governedUncertainSendReconciliation');

function parseArgs(argv = process.argv.slice(2)) {
  const value = (flag) => {
    const i = argv.indexOf(flag);
    return i >= 0 ? argv[i + 1] : null;
  };
  return {
    tenantId: value('--tenant'),
    itemId: value('--item'),
    apply: argv.includes('--apply'),
  };
}

async function main(opts = {}) {
  const args = opts.args || parseArgs();
  if (!args.tenantId || !args.itemId) {
    throw new Error('usage: --tenant <id> --item <id> [--apply]');
  }
  const pool = opts.pool || require('../db');
  const result = await reconcileUncertainItemFromEvidence(
    pool,
    args.tenantId,
    args.itemId,
    { dryRun: !args.apply }
  );
  return { mode: args.apply ? 'apply' : 'dry-run', ...result };
}

if (require.main === module) {
  main()
    .then((result) => {
      console.log(JSON.stringify(result, null, 2));
      process.exit(0);
    })
    .catch((error) => {
      console.error(error.stack || error.message);
      process.exit(1);
    });
}

module.exports = { parseArgs, main };
