'use strict';

/**
 * SPEC-064 Relationship Intelligence Interview CLI (notes mode).
 *
 *   npm run relationship:intel:interview -- --type=discovery_call --company-id=... --notes="..."
 *   npm run relationship:intel:interview -- --type=walkthrough --notes="..." --json --commit
 */

require('dotenv').config();

const {
  INTERACTION_TYPES,
  startRelationshipInterview,
  summarizeRelationshipInterview,
  commitRelationshipInterview,
  RelationshipIntelligenceError,
} = require('../services/relationshipIntelligenceInterview');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    type: 'other',
    companyId: null,
    contactId: null,
    opportunityId: null,
    clientId: null,
    occurredAt: null,
    notes: null,
    json: false,
    commit: false,
    help: false,
  };

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg === '--json') {
      options.json = true;
      continue;
    }
    if (arg === '--commit') {
      options.commit = true;
      continue;
    }
    if (arg.startsWith('--type=')) {
      options.type = arg.slice('--type='.length);
      continue;
    }
    if (arg.startsWith('--company-id=')) {
      options.companyId = arg.slice('--company-id='.length);
      continue;
    }
    if (arg.startsWith('--contact-id=')) {
      options.contactId = arg.slice('--contact-id='.length);
      continue;
    }
    if (arg.startsWith('--opportunity-id=')) {
      options.opportunityId = arg.slice('--opportunity-id='.length);
      continue;
    }
    if (arg.startsWith('--client-id=')) {
      options.clientId = arg.slice('--client-id='.length);
      continue;
    }
    if (arg.startsWith('--occurred-at=')) {
      options.occurredAt = arg.slice('--occurred-at='.length);
      continue;
    }
    if (arg.startsWith('--notes=')) {
      options.notes = arg.slice('--notes='.length);
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  return options;
}

function printHelp() {
  console.log(`Relationship Intelligence Interview (SPEC-064) — notes mode

Usage:
  npm run relationship:intel:interview -- --type=<type> --notes="..." [options]

Options:
  --type=<type>              ${INTERACTION_TYPES.join(' | ')}
  --notes="..."              Interaction notes (required for v1 notes mode)
  --company-id=<id>          Soft company ref
  --contact-id=<id>          Soft contact ref
  --opportunity-id=<id>      Soft opportunity ref
  --client-id=<n>            Tenant client_id
  --occurred-at=<iso>        When the interaction happened
  --json                     Print JSON payload only
  --commit                   Commit after summarize (explicit review step)
  --help                     Show this help

Flow: start (notes) → summarize (draft) → optional --commit
`);
}

async function main() {
  const options = parseArgs();
  if (options.help) {
    printHelp();
    return;
  }

  if (!options.notes || !String(options.notes).trim()) {
    throw new Error('--notes is required for v1 notes mode');
  }
  if (!INTERACTION_TYPES.includes(options.type)) {
    throw new Error(`--type must be one of: ${INTERACTION_TYPES.join(', ')}`);
  }

  const started = await startRelationshipInterview({
    interactionType: options.type,
    companyId: options.companyId,
    contactId: options.contactId,
    opportunityId: options.opportunityId,
    clientId: options.clientId,
    occurredAt: options.occurredAt,
    notes: options.notes,
    source: 'cli_notes',
  });

  let payload = await summarizeRelationshipInterview(started.interviewId);

  if (options.commit) {
    payload = await commitRelationshipInterview(started.interviewId);
  }

  if (options.json) {
    console.log(JSON.stringify(payload, null, 2));
    return;
  }

  console.log(`Interview ${started.interviewId}`);
  console.log(`Status: ${payload.status}`);
  console.log(`Confidence: ${payload.interaction.confidence}`);
  console.log(`Insights: ${payload.insights.length}`);
  if (payload.caveats.length) {
    console.log('Caveats:');
    for (const c of payload.caveats) console.log(`  - ${c}`);
  }
  console.log('');
  console.log(JSON.stringify(payload, null, 2));
}

main().catch((err) => {
  if (err instanceof RelationshipIntelligenceError) {
    console.error(`${err.code}: ${err.message}`);
  } else {
    console.error(err && err.message ? err.message : err);
  }
  process.exitCode = 1;
});
