'use strict';

/**
 * SPEC-182 — Provider Capability Architecture acceptance tests.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  createUnifiedProviderRegistry,
  resetDefaultUnifiedRegistry,
  EVIDENCE_CAPABILITIES,
} = require('../packages/scout/coverage/ProviderCapabilityRegistry');
const { INVESTIGATIVE_EVIDENCE: IE } = require('../packages/scout/coverage/EvidenceRequirements');
const {
  assignProvidersForEvidence,
  assignProvidersForRequirements,
  buildEvidenceToProvidersMap,
} = require('../packages/scout/coverage/EvidenceProviderAssignment');
const {
  buildProviderRegistry,
  hasOperationalEvidenceProvider,
} = require('../packages/scout/coverage/ExternalDiscoveryProviderRegistry');
const { evaluateDiscoveryCapability } = require('../packages/scout/coverage/DiscoveryCapabilityGate');
const { createDefaultProviderRegistry } = require('../packages/scout/intelligence/ProviderCapabilityRegistry');
const { createInjectedDiscoverAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');

describe('SPEC-182 — Provider Capability Architecture', () => {
  afterEach(() => {
    resetDefaultUnifiedRegistry();
  });

  it('providers advertise investigative evidence capabilities', () => {
    const registry = createUnifiedProviderRegistry();
    const googleMaps = registry.get('google_maps');
    assert.ok(googleMaps.evidenceTypes.includes(IE.IDENTITY));
    assert.ok(googleMaps.evidenceTypes.includes(IE.REVIEWS));

    const linkedin = registry.get('linkedin');
    assert.ok(linkedin.evidenceTypes.includes(IE.DECISION_MAKERS));
  });

  it('selectForEvidenceType returns providers dynamically — no hardcoded lists', () => {
    const registry = createUnifiedProviderRegistry();
    const identityProviders = registry.selectForEvidenceType(IE.IDENTITY, { includeUnavailable: true });
    assert.ok(identityProviders.some((p) => p.id === 'google_maps'));
    assert.ok(identityProviders.some((p) => p.id === 'existing_pf'));

    const dmProviders = registry.selectForEvidenceType(IE.DECISION_MAKERS, { includeUnavailable: true });
    assert.ok(dmProviders.some((p) => p.id === 'linkedin'));
    assert.ok(!identityProviders.some((p) => p.id === 'linkedin'));
  });

  it('new provider registration is automatically considered by assignment', () => {
    const registry = createUnifiedProviderRegistry();
    registry.register({
      id: 'custom_registry',
      label: 'Custom Registry Provider',
      capabilities: [EVIDENCE_CAPABILITIES.BUSINESSES],
      evidenceTypes: [IE.IDENTITY],
      sourceType: 'custom_registry',
      costTier: 'free',
      reliability: 0.99,
      coverage: 0.9,
      available: () => true,
    });

    const assignments = assignProvidersForEvidence(IE.IDENTITY, { registry });
    assert.ok(assignments.some((a) => a.providerId === 'custom_registry'));
  });

  it('assignProvidersForRequirements and evaluateDiscoveryCapability share unified registry', () => {
    const registry = createUnifiedProviderRegistry();
    const requirements = [{ evidenceType: IE.IDENTITY }];
    const assignments = assignProvidersForRequirements(requirements, { registry });
    assert.ok(assignments.length > 0);

    const adapter = createInjectedDiscoverAdapter(async () => []);
    const evaluation = evaluateDiscoveryCapability({
      adapters: [adapter],
      registry,
      coveragePlan: { totals: { searches: 1 }, sources: ['places'] },
      requireExternalDiscovery: true,
    });
    assert.equal(evaluation.canExecute, true);
    assert.ok(evaluation.registry);
  });

  it('removing provider from registry changes assignments but not evidence requirements', () => {
    const registry = createUnifiedProviderRegistry();
    const withoutGoogle = createUnifiedProviderRegistry(
      registry.list().filter((p) => p.id !== 'google_maps')
    );

    const withGoogle = assignProvidersForEvidence(IE.IDENTITY, { registry });
    const without = assignProvidersForEvidence(IE.IDENTITY, { registry: withoutGoogle });

    assert.ok(withGoogle.some((a) => a.providerId === 'google_maps'));
    assert.ok(!without.some((a) => a.providerId === 'google_maps'));
    assert.ok(without.some((a) => a.providerId === 'existing_pf'));
  });

  it('intelligence registry delegates to unified registry', () => {
    const savedKey = process.env.GOOGLE_PLACES_KEY;
    process.env.GOOGLE_PLACES_KEY = 'test-key';
    resetDefaultUnifiedRegistry();

    const legacy = createDefaultProviderRegistry();
    const googleMaps = legacy.get('google_maps');
    assert.ok(googleMaps);
    assert.ok(googleMaps.capabilities.includes(EVIDENCE_CAPABILITIES.REVIEWS));

    const selected = legacy.selectForCapabilities([EVIDENCE_CAPABILITIES.REVIEWS]);
    assert.ok(selected.some((row) => row.providerId === 'google_maps'));

    if (savedKey === undefined) delete process.env.GOOGLE_PLACES_KEY;
    else process.env.GOOGLE_PLACES_KEY = savedKey;
    resetDefaultUnifiedRegistry();
  });

  it('external discovery registry delegates to unified registry', () => {
    const adapter = createInjectedDiscoverAdapter(async () => []);
    const rows = buildProviderRegistry({ adapters: [adapter], discover: adapter.discover });
    const places = rows.find((r) => r.id === 'google_places');
    assert.ok(places);
    assert.equal(places.evidenceProducing, true);
  });

  it('buildEvidenceToProvidersMap derives provider order from capabilities', () => {
    const map = buildEvidenceToProvidersMap();
    assert.ok(map[IE.IDENTITY].includes('google_maps'));
    assert.ok(map[IE.DECISION_MAKERS].includes('linkedin'));
    assert.ok(map[IE.REVIEWS].includes('google_maps'));
  });
});
