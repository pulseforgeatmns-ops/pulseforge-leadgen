#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { InMemorySignalStore } = require('../packages/signal-v1/storage/InMemorySignalStore');
const { SignalService } = require('../packages/signal-v1/SignalService');
const { GeckoTerminalMarketDataProvider } = require('../packages/signal-v1/providers/GeckoTerminalMarketDataProvider');
const { VALIDATION_COHORT_003_ID } = require('../packages/signal-v1/acquisition/candidateTypes');
const { createHistoricalCallerCatalogProvider } = require('../packages/signal-v1/acquisition/providers/historicalCallerCatalogProvider');

function parseArg(name) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? hit.split('=').slice(1).join('=') : null;
}

async function main() {
  const exportPath =
    parseArg('export') || path.join(process.cwd(), 'signal-v1/validation-cohort-003-evaluation.json');
  const targetSize = Number(parseArg('targetSize') || 50);
  const useLiveMarket = process.argv.includes('--live-market');

  const store = new InMemorySignalStore();
  const service = new SignalService(store, { seedFixtures: false });
  const catalogProvider = createHistoricalCallerCatalogProvider();
  const marketProvider = useLiveMarket ? new GeckoTerminalMarketDataProvider() : null;

  const built = await service.buildValidationCohort003({
    targetSize,
    marketProvider,
    catalogProvider,
  });

  const artifact = await service.exportResearchCohortEvaluation(VALIDATION_COHORT_003_ID, {
    replayMembers: false,
    executionDelaySeconds: 60,
    marketProviderId: useLiveMarket ? 'geckoterminal' : 'geckoterminal',
  });

  const abs = path.resolve(exportPath);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, `${JSON.stringify(artifact, null, 2)}\n`);

  console.log(
    JSON.stringify(
      {
        cohortId: VALIDATION_COHORT_003_ID,
        selectedCount: built.selection.selected.length,
        dataClass: built.dataClass,
        frozenAt: built.cohort.frozenAt,
        provenanceCounts: artifact.provenanceCounts,
        finalQuestions: artifact.finalQuestions,
        exportPath: abs,
        coverageNote:
          built.selection.selected.length < targetSize
            ? `Low N: only ${built.selection.selected.length} empirically eligible tokens in catalog.`
            : null,
      },
      null,
      2
    )
  );
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
