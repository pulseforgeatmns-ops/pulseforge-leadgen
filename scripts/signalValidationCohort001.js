#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { InMemorySignalStore } = require('../packages/signal-v1/storage/InMemorySignalStore');
const { SignalService } = require('../packages/signal-v1/SignalService');
const { seedFrontRunnersFixtures } = require('../packages/signal-v1/fixtures/seedFixtures');
const { VALIDATION_COHORT_001_ID } = require('../packages/signal-v1/acquisition/candidateTypes');

function parseArg(name) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? hit.split('=').slice(1).join('=') : null;
}

async function main() {
  const exportPath = parseArg('export');
  const perCategory = Number(parseArg('perCategory') || 20);
  const targetSize = Number(parseArg('targetSize') || perCategory * 2);

  const store = new InMemorySignalStore();
  seedFrontRunnersFixtures(store);
  const service = new SignalService(store, { seedFixtures: false });

  const built = await service.buildValidationCohort001({
    cohortId: VALIDATION_COHORT_001_ID,
    perCategory,
    targetSize,
    freeze: true,
  });

  const artifact = await service.exportResearchCohortEvaluation(VALIDATION_COHORT_001_ID, {
    replayMembers: false,
    executionDelaySeconds: 60,
  });

  const summary = {
    candidatePoolSize: built.discovery.discovered,
    eligibleCount: built.discovery.candidates.filter(c => c.status === 'ELIGIBLE').length,
    selectedCount: built.selection.selected.length,
    selectionBreakdown: built.selection.breakdown,
    dataCoverage: artifact.evaluation.dataCoverage,
    primaryLayers: artifact.evaluation.layers,
    lowNSwarnings: artifact.evaluation.lowNSwarnings,
  };

  console.log(JSON.stringify({ built, artifact, summary }, null, 2));

  if (exportPath) {
    const abs = path.resolve(exportPath);
    fs.mkdirSync(path.dirname(abs), { recursive: true });
    fs.writeFileSync(abs, `${JSON.stringify(artifact, null, 2)}\n`);
    console.error(`Wrote export: ${abs}`);
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
