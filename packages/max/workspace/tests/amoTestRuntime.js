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
  createTestAmoRuntime,
  runtimeProviderFromEngine,
  installTestAmoRuntime,
  createHydratingTestRuntime,
};
