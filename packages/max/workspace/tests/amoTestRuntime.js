'use strict';

/**
 * SPEC-140 test helpers — wrap engines in a unified runtime provider.
 */

const {
  createAcquisitionMissionRuntime,
  setAcquisitionMissionRuntimeForTests,
} = require('../../../../services/acquisitionMissionRuntime');

function createTestAmoRuntime(opts = {}) {
  return createAcquisitionMissionRuntime({
    persist: false,
    pool: null,
    production: false,
    ...opts,
  });
}

/**
 * Test stand-in for a LIVE Scout provider (injected as `runScout`). The built-in
 * fixture fallback self-labels `provenance: 'synthetic_fixture'` and synthetic
 * missions can never reach outreach (P0 synthetic isolation), so suites that
 * scaffold a mission through to execution must model live discovery explicitly.
 */
async function liveScoutStub() {
  const result = require('../AmoOperatorApproval').fixtureScoutDiscoveryResult();
  delete result.provenance;
  if (result.payload) delete result.payload.provenance;
  return result;
}

function runtimeProviderFromEngine(engine) {
  const runtime = createTestAmoRuntime({ engine });
  return () => runtime;
}

function installTestAmoRuntime(opts = {}) {
  const runtime = createTestAmoRuntime(opts);
  setAcquisitionMissionRuntimeForTests(runtime);
  return runtime;
}

function createHydratingTestRuntime(sourceEngine, opts = {}) {
  const runtime = createTestAmoRuntime(opts);
  const targetEngine = runtime.engine();
  runtime.hydrate = async (tenantId) => {
    for (const row of sourceEngine.list(tenantId)) {
      targetEngine.store.putMission(row);
    }
    return targetEngine;
  };
  return runtime;
}

module.exports = {
  liveScoutStub,
  createTestAmoRuntime,
  runtimeProviderFromEngine,
  installTestAmoRuntime,
  createHydratingTestRuntime,
};
