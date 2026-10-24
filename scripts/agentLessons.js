require('dotenv').config();

const pool = require('../db');
const {
  ACTIVE_GUARDRAILS_SQL,
  listLessons,
  promoteLesson,
  retireLesson,
} = require('../utils/agentLessons');
const { normalizeClientId } = require('../utils/clientContext');

function parseArgs(argv = process.argv.slice(2)) {
  const options = { command: argv[0] };
  let cursor = 1;
  if (options.command && options.command !== 'list' && argv[1] && !argv[1].startsWith('--')) {
    options.id = argv[1];
    cursor = 2;
  }
  for (let i = cursor; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--client-id' || arg === '--client_id') options.clientId = argv[++i];
    else if (arg === '--global') options.global = true;
    else if (arg === '--agent') options.agent = argv[++i];
    else if (arg === '--status') options.status = argv[++i];
    else if (arg === '--confirmed-by') options.confirmedBy = argv[++i];
  }
  return options;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/agentLessons.js list --client-id 1 [--status proposed|active|retired]',
    '  node scripts/agentLessons.js promote <id> --confirmed-by Jacob [--global|--client-id N]',
    '  node scripts/agentLessons.js retire <id> [--confirmed-by Jacob]',
    '  node scripts/agentLessons.js injection-sql',
  ].join('\n');
}

async function main() {
  const options = parseArgs();
  if (!options.command || options.command === 'help') {
    console.log(usage());
    return;
  }

  if (options.command === 'list') {
    const rows = await listLessons({
      agent: options.agent || 'paige',
      clientId: options.clientId == null ? null : normalizeClientId(options.clientId),
      status: options.status || 'proposed',
    });
    console.log(JSON.stringify(rows, null, 2));
    return;
  }

  if (options.command === 'injection-sql') {
    console.log(JSON.stringify({
      sql: ACTIVE_GUARDRAILS_SQL.trim(),
      params: [options.agent || 'paige', options.clientId == null ? '<currentClientId>' : normalizeClientId(options.clientId)],
    }, null, 2));
    return;
  }

  const id = Number.parseInt(options.id, 10);
  if (!Number.isInteger(id)) throw new Error(`A numeric lesson id is required.\n${usage()}`);

  if (options.command === 'promote') {
    const row = await promoteLesson(id, options.confirmedBy, {
      global: options.global,
      clientId: options.clientId == null ? null : normalizeClientId(options.clientId),
    });
    if (!row) throw new Error(`No proposed lesson found for id ${id}`);
    console.log(JSON.stringify(row, null, 2));
    return;
  }

  if (options.command === 'retire') {
    const row = await retireLesson(id, options.confirmedBy);
    if (!row) throw new Error(`No proposed or active lesson found for id ${id}`);
    console.log(JSON.stringify(row, null, 2));
    return;
  }

  throw new Error(`Unknown command: ${options.command}\n${usage()}`);
}

main()
  .catch(err => {
    console.error(err.message);
    process.exitCode = 1;
  })
  .finally(() => pool.end());
