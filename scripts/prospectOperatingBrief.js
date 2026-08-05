'use strict';

/**
 * SPEC-074 — Prospect Operating Brief CLI
 *
 *   npm run prospect:brief -- --company-id=...
 *   npm run prospect:brief -- --prospect-id=... --json
 *   npm run prospect:brief -- --relationship-interaction-id=...
 *   npm run prospect:brief -- --company-id=... --days=30
 */

require('dotenv').config();

const pool = require('../db');
const {
  DEFAULT_DAYS,
  formatOperatingBriefReport,
  getProspectOperatingBrief,
} = require('../services/prospectOperatingBrief');

function parseArgs(argv = process.argv.slice(2)) {
  const options = {
    json: false,
    help: false,
    days: DEFAULT_DAYS,
    companyId: null,
    prospectId: null,
    opportunityId: null,
    contactId: null,
    relationshipInteractionId: null,
    includeMarketContext: true,
    includeRelationshipContext: true,
  };

  for (const arg of argv) {
    if (arg === '--json') {
      options.json = true;
      continue;
    }
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg === '--no-market') {
      options.includeMarketContext = false;
      continue;
    }
    if (arg === '--no-relationship') {
      options.includeRelationshipContext = false;
      continue;
    }
    if (arg.startsWith('--days=')) {
      options.days = Number(arg.slice('--days='.length));
      continue;
    }
    if (arg.startsWith('--company-id=')) {
      options.companyId = arg.slice('--company-id='.length);
      continue;
    }
    if (arg.startsWith('--prospect-id=')) {
      options.prospectId = arg.slice('--prospect-id='.length);
      continue;
    }
    if (arg.startsWith('--opportunity-id=')) {
      options.opportunityId = arg.slice('--opportunity-id='.length);
      continue;
    }
    if (arg.startsWith('--contact-id=')) {
      options.contactId = arg.slice('--contact-id='.length);
      continue;
    }
    if (arg.startsWith('--relationship-interaction-id=')) {
      options.relationshipInteractionId = arg.slice(
        '--relationship-interaction-id='.length
      );
      continue;
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  if (!Number.isFinite(options.days) || options.days < 1) {
    throw new Error('--days must be a positive number');
  }

  if (
    !options.help &&
    !options.companyId &&
    !options.prospectId &&
    !options.opportunityId &&
    !options.contactId &&
    !options.relationshipInteractionId
  ) {
    throw new Error(
      'At least one of --company-id, --prospect-id, --opportunity-id, --contact-id, or --relationship-interaction-id is required'
    );
  }

  return options;
}

function printHelp() {
  console.log(`Prospect Operating Brief (SPEC-074)

Usage:
  npm run prospect:brief -- [options]

Options:
  --company-id=ID                      CRM / soft company id
  --prospect-id=ID                     Prospect id
  --opportunity-id=ID                  Opportunity id (soft or CRM)
  --contact-id=ID                      Contact / soft contact id
  --relationship-interaction-id=ID     Committed relationship interaction id
  --days=30                            Market corpus lookback window
  --json                               Print full JSON brief (default: human-readable text)
  --no-market                          Skip market intelligence context
  --no-relationship                    Skip relationship intelligence context
  --help                               Show this help

Read-only synthesis for Jake. isEvidence=false. No outbound email, CRM
mutation, Composer generation, or autonomous Max execution.
`);
}

async function main(argv = process.argv.slice(2), db = pool) {
  const options = parseArgs(argv);
  if (options.help) {
    printHelp();
    return { ok: true, help: true };
  }

  const brief = await getProspectOperatingBrief({
    pool: db,
    companyId: options.companyId || undefined,
    prospectId: options.prospectId || undefined,
    opportunityId: options.opportunityId || undefined,
    contactId: options.contactId || undefined,
    relationshipInteractionId: options.relationshipInteractionId || undefined,
    days: options.days,
    includeMarketContext: options.includeMarketContext,
    includeRelationshipContext: options.includeRelationshipContext,
  });

  if (options.json) {
    console.log(JSON.stringify(brief, null, 2));
  } else {
    console.log(formatOperatingBriefReport(brief));
  }

  return brief;
}

if (require.main === module) {
  main()
    .catch((err) => {
      console.error(err && err.message ? err.message : err);
      process.exitCode = 1;
    })
    .finally(() => {
      if (typeof pool.end === 'function') {
        return pool.end().catch(() => {});
      }
      return undefined;
    });
}

module.exports = { parseArgs, main, printHelp };
