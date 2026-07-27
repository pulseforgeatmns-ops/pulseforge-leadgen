'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  OntologyRegistry,
  resetOntologyRegistry,
  getOntologyRegistry,
  createDomainOntology,
  createCrmOntology,
  CORE_NODE_CATEGORIES,
  CORE_EDGE_TYPES,
  buildProvenance,
  deterministicId,
} = require('../ontology');
const {
  createKnowledgeRuntime,
  NODE_TYPES,
  EDGE_TYPES,
} = require('..');

describe('SPEC-017 Domain Ontology Framework', () => {
  beforeEach(() => {
    resetOntologyRegistry();
  });

  describe('DomainOntology contract', () => {
    it('rejects incomplete ontology definitions', () => {
      assert.throws(
        () => createDomainOntology({ id: '', label: 'X', entityTypes: [] }),
        /DomainOntology.id must be a non-empty string/
      );
    });

    it('rejects duplicate vocabulary ids', () => {
      assert.throws(
        () =>
          createDomainOntology({
            id: 'dup',
            label: 'Dup',
            entityTypes: ['a'],
            subjectTypes: ['a'],
            relationshipTypes: [],
            observationTypes: [],
            claimVocabulary: [
              { id: 'same', label: 'One' },
              { id: 'same', label: 'Two' },
            ],
            outcomeVocabulary: [],
          }),
        /Duplicate ontology term/
      );
    });
  });

  describe('OntologyRegistry', () => {
    it('ships CRM as the default domain', () => {
      const registry = getOntologyRegistry();
      assert.equal(registry.getDomain('crm').label, 'CRM');
      assert.ok(registry.isNodeType(NODE_TYPES.COMPANY));
      assert.ok(registry.isNodeType(CORE_NODE_CATEGORIES.OBSERVATION));
      assert.ok(registry.isEdgeType(EDGE_TYPES.SUPPORTS));
      assert.ok(registry.isEdgeType(EDGE_TYPES.WORKS_FOR));
      assert.ok(registry.getClaimTerm('prospect_interested'));
    });

    it('registers additional domains without core changes', () => {
      const registry = new OntologyRegistry();
      registry.register(createCrmOntology());
      registry.register(
        createDomainOntology({
          id: 'manufacturing',
          label: 'Manufacturing',
          entityTypes: ['machine', 'conveyor'],
          subjectTypes: ['machine'],
          relationshipTypes: ['OPERATES'],
          observationTypes: ['sensor_reading'],
          claimVocabulary: [{ id: 'bearing_failure_likely', label: 'Bearing Failure Likely' }],
          outcomeVocabulary: [{ id: 'machine_broke', label: 'Machine Broke' }],
        })
      );

      assert.ok(registry.isNodeType('machine'));
      assert.ok(registry.isEdgeType('OPERATES'));
      assert.ok(registry.isObservationType('sensor_reading'));
      assert.equal(registry.getClaimTerm('bearing_failure_likely').domain, 'manufacturing');
    });

    it('prevents vocabulary collisions across domains', () => {
      const registry = new OntologyRegistry();
      registry.register(createCrmOntology());
      assert.throws(
        () =>
          registry.register(
            createDomainOntology({
              id: 'other',
              label: 'Other',
              entityTypes: ['widget'],
              subjectTypes: ['widget'],
              relationshipTypes: [],
              observationTypes: [],
              claimVocabulary: [{ id: 'prospect_interested', label: 'Collision' }],
              outcomeVocabulary: [],
            })
          ),
        /Claim vocabulary collision/
      );
    });
  });

  describe('core graph invariants', () => {
    it('builds provenance with required fields', () => {
      const provenance = buildProvenance({
        tenant: '10',
        observedAt: '2026-07-26T18:05:00.000Z',
        origin: 'coinbase',
        adapter: 'market-ingest',
      });
      assert.equal(provenance.tenant, '10');
      assert.equal(provenance.origin, 'coinbase');
      assert.equal(provenance.adapter, 'market-ingest');
      assert.ok(provenance.recordedAt);
    });

    it('produces deterministic identities', () => {
      const a = deterministicId(['BTC', 'price_tick', '2026-07-26T18:05:00Z', 'coinbase']);
      const b = deterministicId(['btc', 'price_tick', '2026-07-26t18:05:00z', 'coinbase']);
      assert.equal(a, b);
      assert.equal(a.length, 32);
    });

    it('creates immutable observation nodes', async () => {
      const { knowledge } = createKnowledgeRuntime();
      const company = await knowledge.createNode({
        tenantId: '10',
        type: NODE_TYPES.COMPANY,
        name: 'Acme',
      });
      const observation = await knowledge.createNode({
        tenantId: '10',
        type: CORE_NODE_CATEGORIES.OBSERVATION,
        observationType: 'email_sent',
        subjectId: company.id,
        observedAt: '2026-07-26T18:05:00.000Z',
        payload: { channel: 'email' },
      });
      assert.equal(observation.type, CORE_NODE_CATEGORIES.OBSERVATION);
      assert.equal(observation.observationType, 'email_sent');
      await assert.rejects(
        () => knowledge.updateNode('10', observation.id, { payload: { channel: 'sms' } }),
        /Observations are immutable/
      );
    });

    it('accepts universal core edges', async () => {
      const { knowledge } = createKnowledgeRuntime();
      const company = await knowledge.createNode({
        tenantId: '10',
        type: NODE_TYPES.COMPANY,
        name: 'Acme',
      });
      const evidence = await knowledge.evidence.createEvidence({
        tenantId: '10',
        sourceType: 'test',
        summary: 'signal',
      });
      const claim = await knowledge.claims.createClaim({
        tenantId: '10',
        statement: 'Prospect Interested',
        subjectId: company.id,
        evidenceIds: [evidence.id],
      });
      const contradicts = await knowledge.createEdge({
        tenantId: '10',
        type: CORE_EDGE_TYPES.CONTRADICTS,
        fromId: evidence.id,
        toId: claim.id,
      });
      assert.equal(contradicts.type, 'CONTRADICTS');
    });
  });
});
