#!/usr/bin/env node
'use strict';

require('dotenv').config();

const {
  operationalizeAcquisitionProspect,
} = require('../services/acquisitionProspectOperationalization');
const pool = require('../db');

const BABRUN_FIRST_TEN = [
  'ak_babrun_prospect_p001',
  'ak_babrun_prospect_p011',
  'ak_babrun_prospect_p047',
  'ak_babrun_prospect_p012',
  'ak_babrun_prospect_p024',
  'ak_babrun_prospect_p022',
  'ak_babrun_prospect_p013',
  'ak_babrun_prospect_p051',
  'ak_babrun_prospect_p003',
  'ak_babrun_prospect_p025',
];

function parseArgs(argv = process.argv.slice(2)) {
  const args = { akIds: [] };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const [key, inline] = arg.split('=');
    const next = inline == null ? argv[i + 1] : inline;
    if (key === '--tenant-id') {
      args.tenantId = next;
      if (inline == null) i += 1;
    } else if (key === '--ak-id') {
      args.akIds.push(next);
      if (inline == null) i += 1;
    } else if (key === '--first-ten-babrun') {
      args.akIds.push(...BABRUN_FIRST_TEN);
    } else if (key === '--apply') {
      args.apply = true;
    }
  }
  return args;
}

async function run() {
  const args = parseArgs();
  if (!args.tenantId) throw new Error('--tenant-id is required.');
  if (!args.akIds.length) throw new Error('Provide --ak-id or --first-ten-babrun.');
  const uniqueIds = [...new Set(args.akIds)];
  const results = [];
  for (const id of uniqueIds) {
    results.push(await operationalizeAcquisitionProspect({
      tenantId: args.tenantId,
      acquisitionKnowledgeObjectId: id,
      apply: args.apply === true,
    }, { pool }));
  }
  console.log(JSON.stringify({
    dryRun: args.apply !== true,
    tenantId: String(args.tenantId),
    requested: uniqueIds.length,
    operationalized: results.length,
    results: results.map((result) => ({
      acquisitionKnowledgeObjectId: result.projection?.acquisitionKnowledgeObjectId || result.acquisitionKnowledgeObjectId,
      prospectId: result.prospectId || null,
      companyId: result.companyId || null,
      matchStrategy: result.matchStrategy || null,
      linkedOutreachAssetIds: result.linkedOutreachAssetIds || result.projection?.linkedOutreachAssetIds || [],
      enrichmentEligible: result.enrichmentEligible === true,
      fabricatedEmail: result.fabricatedEmail === true,
      dryRun: result.dryRun === true,
      wouldCreateCompany: result.wouldCreateCompany === true,
      wouldCreateProspect: result.wouldCreateProspect === true,
    })),
  }, null, 2));
}

if (require.main === module) {
  run()
    .then(() => pool.end())
    .catch(async (err) => {
      console.error(err.stack || err.message);
      await pool.end();
      process.exit(1);
    });
}

module.exports = { parseArgs, BABRUN_FIRST_TEN };
