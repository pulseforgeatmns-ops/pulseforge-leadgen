#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { InMemorySignalStore } = require('../packages/signal-v1/storage/InMemorySignalStore');
const { SignalService } = require('../packages/signal-v1/SignalService');
const { seedFrontRunnersFixtures } = require('../packages/signal-v1/fixtures/seedFixtures');
const { runConvergenceIntegrityAudit } = require('../packages/signal-v1/research/convergenceIntegrityAudit');

function parseArg(name) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? hit.split('=').slice(1).join('=') : null;
}

async function main() {
  const exportPath = parseArg('export');
  const skipHoldout = process.argv.includes('--skip-holdout');

  const store = new InMemorySignalStore();
  seedFrontRunnersFixtures(store);
  const service = new SignalService(store, { seedFixtures: false });

  await service.buildValidationCohort001({ freeze: true });
  if (!skipHoldout) {
    await service.buildValidationCohort002({ freeze: true });
  }

  const audit = runConvergenceIntegrityAudit(store, { executionDelaySeconds: 60 });

  console.log(JSON.stringify(audit, null, 2));

  if (exportPath) {
    const abs = path.resolve(exportPath);
    fs.mkdirSync(path.dirname(abs), { recursive: true });
    fs.writeFileSync(abs, `${JSON.stringify(audit, null, 2)}\n`);
    console.error(`Wrote audit export: ${abs}`);
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
