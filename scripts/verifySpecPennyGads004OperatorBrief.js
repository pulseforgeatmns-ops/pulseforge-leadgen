#!/usr/bin/env node
'use strict';

/**
 * SPEC-PENNY-GADS-004 — Operator brief Google Ads evidence section verifier.
 *
 * Usage:
 *   node scripts/verifySpecPennyGads004OperatorBrief.js
 */

const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const ROOT = path.join(__dirname, '..');
const {
  buildGoogleAdsOperatorBrief,
} = require('../packages/penny-paid-acquisition/googleAdsOperatorBrief');

const REQUIRED_FIELDS = Object.freeze([
  'accountStatus',
  'spend',
  'impressions',
  'clicks',
  'ctr',
  'conversions',
  'costPerConversion',
  'activeCampaignCount',
  'warnings',
  'blockers',
  'recommendedNextAction',
]);

function fieldChecks(brief) {
  const fields = {};
  for (const key of REQUIRED_FIELDS) {
    fields[key] = Object.prototype.hasOwnProperty.call(brief, key);
  }
  return fields;
}

function scanForGoogleAdsMutate() {
  const targets = [
    'packages/penny-paid-acquisition/adapters/googleAds.js',
    'pennyAgent.js',
    'packages/max/workspace/PennyPaidAcquisitionExecutor.js',
  ];
  try {
    const out = execFileSync('rg', ['-n', 'googleAds:mutate|mutateGoogleAds', ...targets], {
      cwd: ROOT,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    return out.trim().length > 0;
  } catch (err) {
    if (err.status === 1) return false;
    throw err;
  }
}

function wiringPresent() {
  const servicePath = path.join(ROOT, 'services', 'commandDeckOperatorBrief.js');
  const deckJs = path.join(ROOT, 'public', 'command-deck', 'command-deck.js');
  const deckHtml = path.join(ROOT, 'public', 'command-deck.html');
  const serviceSrc = fs.readFileSync(servicePath, 'utf8');
  const deckJsSrc = fs.readFileSync(deckJs, 'utf8');
  const deckHtmlSrc = fs.readFileSync(deckHtml, 'utf8');
  return (
    serviceSrc.includes('loadGoogleAdsOperatorBriefSection')
    && serviceSrc.includes('googleAds')
    && deckJsSrc.includes('ob.googleAds')
    && deckHtmlSrc.includes('cdGoogleAdsBrief')
  );
}

function main() {
  const sample = buildGoogleAdsOperatorBrief({
    readiness: { accountStatus: 'READY', blockers: [], warnings: [] },
    evidence: {
      aggregates: {
        spend: 1.43,
        impressions: 55,
        clicks: 3,
        platformConversions: 2,
      },
      campaignCount: 1,
    },
  });

  const fields = fieldChecks(sample);
  const allFields = Object.values(fields).every(Boolean);
  const mutateFound = scanForGoogleAdsMutate();
  const wired = wiringPresent();

  const report = {
    spec: 'SPEC-PENNY-GADS-004',
    googleAdsBriefSectionPresent: wired && sample.title === 'Paid Acquisition — Google Ads',
    readOnly: !mutateFound,
    fields,
    pass: allFields && !mutateFound && wired,
  };

  console.log(JSON.stringify(report, null, 2));
  process.exit(report.pass ? 0 : 1);
}

main();
