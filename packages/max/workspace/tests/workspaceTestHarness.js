'use strict';

// Unit tests must not silently fall back to the application's live services.
// The guard also fails the process if an unexpected network error is caught.
require('../../../../test/helpers/denyWebsiteEvidenceNetwork.cjs');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createMemoryStore } = require('../../../../services/clientIntelligenceInterview');
const { createTestAmoRuntime } = require('./amoTestRuntime');

function hermeticWorkspaceOptions(options = {}) {
  const supplied = { disableLlm: true, ...options };
  if (supplied.disableLlm === false && !supplied.anthropic && !supplied.presentation) {
    throw new Error('Hermetic workspace requires an explicit fake LLM/presentation when LLM is enabled');
  }
  if (!supplied.runtimeProvider && !supplied.acquisitionMissionRuntime) {
    supplied.acquisitionMissionRuntime = createTestAmoRuntime();
  }
  if (!supplied.clientIntelligenceService && !supplied.clientIntelligenceOpts?.store) {
    supplied.clientIntelligenceOpts = {
      ...supplied.clientIntelligenceOpts,
      store: createMemoryStore(),
    };
  }
  return supplied;
}

function createHermeticWorkspaceEngine(options = {}) {
  return createWorkspaceEngine(hermeticWorkspaceOptions(options));
}

// Direct retrieval unit tests have the same explicit memory-backed CIE default.
function hermeticRetrievalInput(input = {}) {
  if (input.cieService || input.cieOpts?.store) return { ...input };
  return { ...input, cieOpts: { ...input.cieOpts, store: createMemoryStore() } };
}

function createHermeticRetrievalHandler(handler) {
  return input => handler(hermeticRetrievalInput(input));
}

// Explicit opt-in for reasoning tests whose premise is that no operating
// evidence is recorded. Never a global default and never a successful result.
function unavailableOperatingEvidence() {
  const unknown = async () => ({ available: false, reason: 'unit_fixture_not_recorded' });
  return Object.fromEntries(['loadCampaignAo', 'loadProspects', 'loadScout',
    'loadMissions', 'loadObjectives', 'loadActivity', 'loadOutcomes',
    'loadOperatorAttested', 'loadCapability'].map(name => [name, unknown]));
}

module.exports = { unavailableOperatingEvidence, createHermeticRetrievalHandler, createHermeticWorkspaceEngine, hermeticWorkspaceOptions, hermeticRetrievalInput };
