#!/usr/bin/env node
'use strict';

// Default is read-only status. Authorization is two-phase and sending has its own
// explicit command, environment gate, active program and artifact-bound envelope.
function parse(argv) {
  const [command = 'status', ...rest] = argv;
  const options = {};
  for (let i = 0; i < rest.length; i += 2) {
    if (!rest[i].startsWith('--') || !rest[i + 1] || rest[i + 1].startsWith('--')) throw new Error('Expected --key value');
    options[rest[i].slice(2)] = rest[i + 1];
  }
  if (!['status','review','authorize','mode','tick','poll','reconcile'].includes(command)) throw new Error('Unknown command');
  return { command, options };
}
async function run(argv = process.argv.slice(2)) {
  process.env.DOTENV_CONFIG_QUIET = 'true';
  require('dotenv').config({ quiet: true });
  const { command, options } = parse(argv);
  const pool = require('../db');
  const service = require('../services/governedOutbound').productionService(pool);
  try {
    if (command === 'status') return await service.status();
    if (command === 'poll') return await require('../anchorDailyOutboundCron').poll({ pool });
    if (command === 'tick') {
      if (options.confirm !== 'bounded-anchor-execution') throw new Error('tick requires --confirm bounded-anchor-execution');
      return await service.tick();
    }
    if (!options.operator) throw new Error('--operator is required for audited changes');
    const actor = { id: options.operator, role: 'admin' };
    if (command === 'mode') return await service.setMode(options.id, options.mode, options['policy-hash'], actor);
    if (command === 'reconcile') return await service.reconcile(options.item, options.outcome, options['provider-message-id'], options.evidence, actor);
    const input = JSON.parse(require('fs').readFileSync(options.file, 'utf8'));
    if (command === 'review') delete input.reviewHash;
    return await service.authorize(input, actor);
  } finally { await pool.end(); }
}
if (require.main === module) run().then(r => console.log(JSON.stringify(r, null, 2))).catch(e => {
  console.error(JSON.stringify({ error: e.code || e.message })); process.exitCode = 1;
});
module.exports = { parse, run };
