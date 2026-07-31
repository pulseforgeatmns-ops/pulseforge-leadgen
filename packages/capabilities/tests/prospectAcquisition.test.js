'use strict';

/**
 * SPEC-060 / ADR-044 — Prospect Acquisition Framework tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createBuiltinRegistry,
  createCapabilityRunner,
  BUILTIN_IDS,
  CAPABILITY_RESULT_STATUS,
  buildCapabilityContext,
  acquisition,
} = require('..');
const {
  createProspectAcquisition,
  selectAcquisitionStrategy,
  verifyCandidateSet,
  buildCandidateSet,
  buildCandidate,
  assertProviderContract,
  createDefaultAcquisitionRegistry,
  createManualProspectProvider,
  createCsvImportProvider,
  createExistingProspectRepositoryProvider,
  parseCsvFallback,
  ACQUISITION_STRATEGIES,
  buildAcquisitionWorkspaceView,
} = acquisition;

describe('SPEC-060 provider contract', () => {
  it('default registry registers four providers with valid contracts', () => {
    const registry = createDefaultAcquisitionRegistry();
    const metas = registry.listMetadata();
    assert.equal(metas.length, 4);
    for (const provider of registry.list()) {
      const check = assertProviderContract(provider);
      assert.equal(check.ok, true, check.errors.join('; '));
      assert.equal(provider.metadata().publishes, 'candidates');
    }
  });

  it('manual acquisition is always available', () => {
    const manual = createManualProspectProvider();
    assert.equal(manual.available(), true);
    assert.equal(manual.health().ok, true);
  });
});

describe('SPEC-060 strategy selection', () => {
  it('selects discovery / manual / csv / existing from operator language', () => {
    assert.equal(
      selectAcquisitionStrategy('Find 50 commercial cleaners'),
      ACQUISITION_STRATEGIES.DISCOVERY
    );
    assert.equal(
      selectAcquisitionStrategy("I'll type the companies"),
      ACQUISITION_STRATEGIES.MANUAL
    );
    assert.equal(
      selectAcquisitionStrategy('Import this CSV'),
      ACQUISITION_STRATEGIES.CSV
    );
    assert.equal(
      selectAcquisitionStrategy(
        'Run Campaign 001 using my Manchester list'
      ),
      ACQUISITION_STRATEGIES.EXISTING
    );
  });
});

describe('SPEC-060 CandidateSet + providers', () => {
  it('manual provider produces candidates with provenance (not ProspectList)', async () => {
    const provider = createManualProspectProvider();
    const raw = await provider.acquire({
      prospects: [
        { companyName: 'Acme Cleaning', website: 'https://acme.test' },
        { companyName: 'Beta Janitorial' },
      ],
      missionId: 'msn_1',
      operator: 'jacob@gopulseforge.com',
    });
    assert.ok(Array.isArray(raw.candidates));
    assert.equal(raw.candidates.length, 2);
    assert.ok(!raw.prospectList);
    assert.equal(
      raw.candidates[0].provenance.acquisitionSource,
      'manual_prospect_list'
    );
    assert.equal(raw.candidates[0].provenance.missionId, 'msn_1');
    assert.equal(raw.candidates[0].provenance.operator, 'jacob@gopulseforge.com');
  });

  it('CSV import produces CandidateSet preview rows', async () => {
    const provider = createCsvImportProvider();
    const csv = [
      'company,website,phone',
      'Northstar Cleaning,https://northstar.test,603-555-0100',
      'Queen City Clean,https://qcc.test,',
    ].join('\n');
    const raw = await provider.acquire({
      csv,
      missionId: 'msn_csv',
      operator: 'op',
    });
    assert.equal(raw.candidates.length, 2);
    assert.ok(raw.preview);
    assert.equal(raw.preview[0].companyName, 'Northstar Cleaning');
    assert.equal(
      raw.candidates[0].provenance.acquisitionSource,
      'csv_import'
    );
  });

  it('parseCsvFallback maps columns and preserves unknown as metadata', () => {
    const rows = parseCsvFallback(
      'Company Name,Website,Custom Score\nAcme,https://a.test,99\n'
    );
    assert.equal(rows.length, 1);
    assert.equal(rows[0].companyName, 'Acme');
    assert.equal(rows[0].metadata.customscore, '99');
  });

  it('existing repository reuses ProspectList as candidates', async () => {
    const provider = createExistingProspectRepositoryProvider();
    const raw = await provider.acquire({
      prospectList: {
        id: 'list_manchester',
        prospects: [
          { companyName: 'Reuse Co', website: 'https://reuse.test' },
        ],
      },
      missionId: 'msn_reuse',
    });
    assert.equal(raw.candidates.length, 1);
    assert.equal(
      raw.candidates[0].provenance.acquisitionSource,
      'existing_prospect_repository'
    );
  });
});

describe('SPEC-060 verification pipeline', () => {
  it('CandidateSet → ProspectList via shared verification', () => {
    const set = buildCandidateSet({
      candidates: [
        buildCandidate(
          { companyName: 'Verified Cleaning LLC', website: 'https://v.test' },
          {
            provenance: {
              acquisitionSource: 'csv_import',
              provider: 'csv_import',
              missionId: 'msn_v',
              operator: 'op',
              importMethod: 'csv',
            },
          }
        ),
      ],
      acquisitionSource: 'csv_import',
      provider: 'csv_import',
      missionId: 'msn_v',
    });
    const verified = verifyCandidateSet({
      candidateSet: set,
      options: { operatorSupplied: true },
    });
    assert.equal(verified.ok, true);
    assert.equal(verified.prospectList.type, 'ProspectList');
    assert.ok(verified.prospectList.prospectCount >= 1);
    assert.ok(verified.verificationReport);
    assert.equal(
      verified.prospectList.provenance.acquisitionSource,
      'csv_import'
    );
    assert.ok(verified.prospectList.prospects[0].provenance);
  });

  it('acquireAndVerify produces ProspectList without Discovery', async () => {
    const acq = createProspectAcquisition();
    const result = await acq.acquireAndVerify({
      acquisitionStrategy: 'manual',
      prospects: [
        { companyName: 'Manual Co One' },
        { companyName: 'Manual Co Two', website: 'https://two.test' },
      ],
      missionId: 'msn_av',
      operator: 'op',
    });
    assert.equal(result.ok, true);
    assert.equal(result.strategy, 'manual');
    assert.ok(result.prospectList.prospectCount >= 1);
    assert.equal(result.candidateSet.type, 'CandidateSet');
    assert.ok(
      result.evidence.some((e) => e.kind === 'prospect_acquisition')
    );
  });
});

describe('SPEC-060 capability registration', () => {
  it('registers prospect_acquisition as a ProspectList producer', () => {
    const registry = createBuiltinRegistry({ discovery: { useFixture: true } });
    assert.equal(registry.list().length, 15);
    const cap = registry.get(BUILTIN_IDS.PROSPECT_ACQUISITION);
    assert.ok(cap);
    assert.ok(cap.produces.includes('prospect_list'));
    assert.ok(cap.produces.includes('candidate_set'));
    const producers = registry.producersOf('ProspectList');
    assert.ok(
      producers.some((p) => p.id === BUILTIN_IDS.PROSPECT_ACQUISITION)
    );
  });

  it('capability execute returns ProspectList-shaped outputs only', async () => {
    const registry = createBuiltinRegistry({ discovery: { useFixture: true } });
    const runner = createCapabilityRunner({ registry });
    const out = await runner.run({
      capabilityId: BUILTIN_IDS.PROSPECT_ACQUISITION,
      context: buildCapabilityContext({
        missionId: 'm_acq',
        tenantId: '10',
        clientId: 10,
        objective: 'Import this CSV',
        constraints: {
          acquisitionStrategy: 'csv',
          csv: 'company,website\nCap Clean,https://cap.test\n',
        },
      }),
    });
    assert.equal(out.result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    assert.ok(out.result.outputs.prospectCount >= 1);
    assert.ok(out.result.outputs.candidateSet);
    assert.equal(
      out.result.outputs.summary.acquisitionSource ||
        out.result.outputs.acquisitionSource,
      'csv_import'
    );
    // Campaign Builder contract: prospects array present; no provider forced
    assert.ok(Array.isArray(out.result.outputs.prospects));
  });
});

describe('SPEC-060 workspace view', () => {
  it('buildAcquisitionWorkspaceView summarizes acquisition state', () => {
    const view = buildAcquisitionWorkspaceView({
      acquisition: {
        ok: true,
        providerId: 'csv_import',
        strategy: 'csv',
        candidateSet: {
          candidateCount: 3,
          acquisitionSource: 'csv_import',
          candidates: [{ companyName: 'A' }],
        },
        evidence: [{ summary: 'CSV import acquired 3 candidates' }],
        errors: [],
      },
      verificationReport: { acceptedCount: 3, rejectedCount: 0 },
      prospectList: { prospectCount: 3 },
      operator: 'op',
    });
    assert.equal(view.section, 'Prospect Acquisition');
    assert.equal(view.provider, 'csv_import');
    assert.equal(view.status, 'prospect_list_generated');
    assert.equal(view.candidateCount, 3);
    assert.equal(view.prospectListGenerated, true);
  });
});

describe('SPEC-060 failure isolation', () => {
  it('discovery provider failure recommends manual/csv', async () => {
    const registry = createDefaultAcquisitionRegistry({
      placesProvider: {
        id: 'google_places',
        available: () => false,
        search: async () => [],
      },
    });
    const acq = createProspectAcquisition({ registry });
    const result = await acq.acquire({
      acquisitionStrategy: 'discovery',
      objective: 'Find 50 cleaners',
    });
    assert.equal(result.ok, false);
    assert.ok(
      (result.recommendedStrategies || []).includes('manual') ||
        (result.warnings || []).some((w) => /manual/i.test(w))
    );
    // Manual still available independently
    const manual = await acq.acquire({
      acquisitionStrategy: 'manual',
      prospects: [{ companyName: 'Fallback Co' }],
    });
    assert.equal(manual.ok, true);
  });
});
