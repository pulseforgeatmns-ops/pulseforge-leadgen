#!/usr/bin/env node
'use strict';

const { SignalService } = require('../packages/signal-v1/SignalService');
const { replayToken } = require('../packages/signal-v1/replay/replayEngine');
const { PILOT_COHORT_ID } = require('../packages/signal-v1/fixtures/seedResearchCohort');

function parseArg(name) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? hit.split('=').slice(1).join('=') : null;
}

const cohortId = parseArg('cohort') || PILOT_COHORT_ID;
const delay = Number(parseArg('delay') || 60);

const service = new SignalService();
const cohort = service.getResearchCohort(cohortId);
if (!cohort) {
  console.error(`Cohort not found: ${cohortId}`);
  process.exit(1);
}

const pricePaths = {
  '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump': [
    { occurredAt: '2026-09-29T18:00:00Z', price: 0.00012 },
    { occurredAt: '2026-09-29T18:10:00Z', price: 0.00016 },
    { occurredAt: '2026-09-29T18:30:00Z', price: 0.00024 },
    { occurredAt: '2026-09-29T19:00:00Z', price: 0.0003 },
    { occurredAt: '2026-09-29T20:00:00Z', price: 0.00007 },
  ],
  Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump: [
    { occurredAt: '2026-09-28T14:00:00Z', price: 0.00009 },
    { occurredAt: '2026-09-28T14:05:00Z', price: 0.0001 },
    { occurredAt: '2026-09-28T14:30:00Z', price: 0.00015 },
    { occurredAt: '2026-09-29T14:00:00Z', price: 0.0002 },
  ],
  '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump': [
    { occurredAt: '2026-09-27T20:00:00Z', price: 0.00004 },
    { occurredAt: '2026-09-27T20:10:00Z', price: 0.000045 },
    { occurredAt: '2026-09-27T21:00:00Z', price: 0.00003 },
  ],
};

for (const member of cohort.members) {
  replayToken(service.store, {
    tokenAddress: member.tokenAddress,
    pricePath: pricePaths[member.tokenAddress] || [],
  });
}

const evaluation = service.evaluateResearchCohort(cohortId, delay);
console.log(JSON.stringify(evaluation, null, 2));
