'use strict';

/**
 * Regression: geography extraction must stay cycle-free so EvidenceRequest
 * and DiscoveryCoverageEngine both initialize under either require order.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const GREATER_MANCHESTER_CITIES = [
  'Manchester',
  'Bedford',
  'Goffstown',
  'Hooksett',
  'Londonderry',
  'Auburn',
];

function runFreshProcess(script) {
  const result = spawnSync(process.execPath, ['-e', script], {
    cwd: ROOT,
    encoding: 'utf8',
    env: { ...process.env },
  });
  if (result.status !== 0) {
    const detail = [result.stderr, result.stdout].filter(Boolean).join('\n');
    throw new Error(`fresh process exited ${result.status}: ${detail}`);
  }
  assert.equal(
    /circular dependency/i.test(result.stderr || ''),
    false,
    `circular dependency warning:\n${result.stderr}`
  );
  assert.equal(
    /scopeSearchDefinitionForTask is not a function/.test(`${result.stderr}\n${result.stdout}`),
    false
  );
  return result.stdout;
}

describe('Scout SearchGeography cycle break', () => {
  it('SearchGeography does not import EvidenceRequest or DiscoveryCoverageEngine', () => {
    const src = fs.readFileSync(
      path.join(ROOT, 'packages/scout/coverage/SearchGeography.js'),
      'utf8'
    );
    assert.equal(/require\(['"]\.\/EvidenceRequest['"]\)/.test(src), false);
    assert.equal(/require\(['"]\.\/DiscoveryCoverageEngine['"]\)/.test(src), false);
  });

  it('EvidenceRequest no longer imports DiscoveryCoverageEngine', () => {
    const src = fs.readFileSync(
      path.join(ROOT, 'packages/scout/coverage/EvidenceRequest.js'),
      'utf8'
    );
    assert.equal(/require\(['"]\.\/DiscoveryCoverageEngine['"]\)/.test(src), false);
    assert.match(src, /require\(['"]\.\/SearchGeography['"]\)/);
  });

  it('loads EvidenceRequest then DiscoveryCoverageEngine with both symbols defined', () => {
    const stdout = runFreshProcess(`
      const assert = require('node:assert/strict');
      const evidence = require(${JSON.stringify(path.join(ROOT, 'packages/scout/coverage/EvidenceRequest'))});
      const coverage = require(${JSON.stringify(path.join(ROOT, 'packages/scout/coverage/DiscoveryCoverageEngine'))});
      assert.equal(typeof evidence.scopeSearchDefinitionForTask, 'function');
      assert.equal(typeof coverage.expandCitiesFromSearchDefinition, 'function');
      console.log('ok');
    `);
    assert.match(stdout, /ok/);
  });

  it('loads DiscoveryCoverageEngine then EvidenceRequest with both symbols defined', () => {
    const stdout = runFreshProcess(`
      const assert = require('node:assert/strict');
      const coverage = require(${JSON.stringify(path.join(ROOT, 'packages/scout/coverage/DiscoveryCoverageEngine'))});
      const evidence = require(${JSON.stringify(path.join(ROOT, 'packages/scout/coverage/EvidenceRequest'))});
      assert.equal(typeof evidence.scopeSearchDefinitionForTask, 'function');
      assert.equal(typeof coverage.expandCitiesFromSearchDefinition, 'function');
      console.log('ok');
    `);
    assert.match(stdout, /ok/);
  });

  it('Greater Manchester expansion is unchanged and still exported from DiscoveryCoverageEngine', () => {
    const {
      expandCitiesFromSearchDefinition,
    } = require('../packages/scout/coverage/SearchGeography');
    const coverage = require('../packages/scout/coverage/DiscoveryCoverageEngine');

    const searchDefinition = {
      geography: { label: 'Greater Manchester NH', state: 'NH' },
    };
    const fromGeography = expandCitiesFromSearchDefinition(searchDefinition);
    const fromCoverage = coverage.expandCitiesFromSearchDefinition(searchDefinition);

    assert.deepEqual(fromGeography, fromCoverage);
    assert.equal(fromGeography.length, 6);
    for (const city of GREATER_MANCHESTER_CITIES) {
      assert.ok(
        fromGeography.some((row) => row === `${city} NH`),
        `expected NH-formatted city ${city} NH, got ${JSON.stringify(fromGeography)}`
      );
    }
  });

  it('scopeSearchDefinitionForTask builds evidenceRequest.geography.cities for Greater Manchester', () => {
    const { scopeSearchDefinitionForTask } = require('../packages/scout/coverage/EvidenceRequest');
    const { INVESTIGATIVE_EVIDENCE } = require('../packages/scout/coverage/EvidenceRequirements');

    const scoped = scopeSearchDefinitionForTask(
      {
        tenantId: '10',
        geography: { label: 'Greater Manchester NH', state: 'NH' },
        segments: ['property_management'],
      },
      {
        id: 'task:identity',
        evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
        providers: [{ providerId: 'google_maps' }],
      },
      { segments: ['property_management'] }
    );

    assert.ok(scoped.evidenceRequest);
    assert.ok(Array.isArray(scoped.evidenceRequest.geography.cities));
    assert.equal(scoped.evidenceRequest.geography.state, 'NH');
    for (const city of GREATER_MANCHESTER_CITIES) {
      assert.ok(
        scoped.evidenceRequest.geography.cities.includes(city),
        `expected city ${city} in ${JSON.stringify(scoped.evidenceRequest.geography.cities)}`
      );
    }
  });

  it('EvidenceRequest-first load still dispatches a mocked provider past scoped request construction', () => {
    const stdout = runFreshProcess(`
      const assert = require('node:assert/strict');
      const { scopeSearchDefinitionForTask } = require(${JSON.stringify(
        path.join(ROOT, 'packages/scout/coverage/EvidenceRequest')
      )});
      const {
        buildDiscoveryPlan,
        executeCoveragePlan,
      } = require(${JSON.stringify(
        path.join(ROOT, 'packages/scout/coverage/DiscoveryCoverageEngine')
      )});
      const { createInjectedDiscoverAdapter } = require(${JSON.stringify(
        path.join(ROOT, 'packages/max/scoutAcquisition/DiscoveryAdapters')
      )});
      const { INVESTIGATIVE_EVIDENCE } = require(${JSON.stringify(
        path.join(ROOT, 'packages/scout/coverage/EvidenceRequirements')
      )});

      assert.equal(typeof scopeSearchDefinitionForTask, 'function');

      (async () => {
        const received = [];
        const adapter = createInjectedDiscoverAdapter(async (searchDefinition) => {
          received.push(searchDefinition);
          assert.ok(searchDefinition.evidenceRequest);
          assert.ok(Array.isArray(searchDefinition.evidenceRequest.geography.cities));
          return [{ id: 'disc-1', name: 'Granite PM', location: 'Manchester, NH' }];
        });

        const searchDefinition = {
          tenantId: '10',
          geography: { label: 'Greater Manchester NH', state: 'NH' },
          segments: ['property_management'],
        };
        const plan = buildDiscoveryPlan(searchDefinition, { adapters: [adapter] });
        const result = await executeCoveragePlan(plan, searchDefinition, [adapter], {
          marketDefinition: { segments: ['property_management'] },
        });

        assert.ok(received.length >= 1, 'provider discover was never called');
        assert.ok(result.executed.some((row) => row.status === 'executed'));
        assert.ok(result.candidates.length >= 1);
        assert.equal(
          result.errors.some((err) => /scopeSearchDefinitionForTask is not a function/.test(err.message || '')),
          false
        );

        const scoped = scopeSearchDefinitionForTask(
          searchDefinition,
          {
            id: 'task:identity',
            evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
            providers: [{ providerId: 'google_maps' }],
          },
          { segments: ['property_management'] }
        );
        assert.equal(typeof scoped.evidenceRequest, 'object');
        console.log(JSON.stringify({
          ok: true,
          providerCalls: received.length,
          candidates: result.candidates.length,
        }));
      })().then(() => process.exit(0)).catch((err) => {
        console.error(err && err.stack ? err.stack : err);
        process.exit(1);
      });
    `);
    const payload = JSON.parse(stdout.trim().split('\n').pop());
    assert.equal(payload.ok, true);
    assert.ok(payload.providerCalls >= 1);
    assert.ok(payload.candidates >= 1);
  });
});
