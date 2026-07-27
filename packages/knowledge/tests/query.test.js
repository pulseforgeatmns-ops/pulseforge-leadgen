'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  createKnowledgeRuntime,
  NODE_TYPES,
  EDGE_TYPES,
  QueryEngine,
  PersistentGraphRepository,
} = require('..');

/**
 * Shared fixture graph for query tests.
 * @param {import('../services/KnowledgeService').KnowledgeService} knowledge
 * @param {string} [tenantId='10']
 */
async function seedQueryFixture(knowledge, tenantId = '10') {
  const company = await knowledge.createNode({
    tenantId,
    type: NODE_TYPES.COMPANY,
    name: 'Lodgism',
    metadata: {
      industry: 'property_management',
      location: 'Manchester NH',
      technology: 'Guesty',
      confidence: 0.7,
    },
  });
  const otherCompany = await knowledge.createNode({
    tenantId,
    type: NODE_TYPES.COMPANY,
    name: 'CleanCo',
    metadata: {
      industry: 'cleaning',
      location: 'Nashville TN',
      confidence: 0.4,
    },
  });
  const person = await knowledge.createNode({
    tenantId,
    type: NODE_TYPES.PERSON,
    name: 'Alex Owner',
    email: 'alex@lodgism.com',
    title: 'Owner',
    metadata: { confidence: 0.8 },
  });
  const stranger = await knowledge.createNode({
    tenantId,
    type: NODE_TYPES.PERSON,
    name: 'Sam Clean',
    email: 'sam@cleanco.com',
    title: 'Manager',
  });
  await knowledge.createEdge({
    tenantId,
    type: EDGE_TYPES.WORKS_FOR,
    fromId: person.id,
    toId: company.id,
  });
  await knowledge.createEdge({
    tenantId,
    type: EDGE_TYPES.WORKS_FOR,
    fromId: stranger.id,
    toId: otherCompany.id,
  });
  await knowledge.createEdge({
    tenantId,
    type: EDGE_TYPES.HAS_CONTACT,
    fromId: company.id,
    toId: person.id,
  });

  const interaction = await knowledge.createNode({
    tenantId,
    type: NODE_TYPES.INTERACTION,
    channel: 'email',
    actionType: 'sent',
    summary: 'Intro email',
    occurredAt: '2026-07-10T12:00:00.000Z',
  });
  await knowledge.createEdge({
    tenantId,
    type: EDGE_TYPES.PARTICIPATED_IN,
    fromId: person.id,
    toId: interaction.id,
  });

  const reply = await knowledge.createNode({
    tenantId,
    type: NODE_TYPES.INTERACTION,
    channel: 'email',
    actionType: 'reply',
    summary: 'Interested reply',
    occurredAt: '2026-07-12T15:00:00.000Z',
  });
  await knowledge.createEdge({
    tenantId,
    type: EDGE_TYPES.PARTICIPATED_IN,
    fromId: person.id,
    toId: reply.id,
  });

  const evidence = await knowledge.evidence.createEvidence({
    tenantId,
    sourceType: 'website',
    sourceId: 'https://lodgism.com',
    summary: 'Website scraped',
    confidence: 0.85,
  });
  // Force createdAt ordering for timeline determinism via update timestamps on related nodes
  await knowledge.evidence.attachEvidence(tenantId, evidence.id, company.id);

  const claim = await knowledge.claims.createClaim({
    tenantId,
    statement: 'Lodgism manages STR units in Manchester',
    subjectId: company.id,
    evidenceIds: [evidence.id],
  });

  return { company, otherCompany, person, stranger, interaction, reply, evidence, claim };
}

describe('SPEC-001C Knowledge Query Engine', () => {
  /** @type {ReturnType<typeof createKnowledgeRuntime>} */
  let runtime;
  /** @type {Awaited<ReturnType<typeof seedQueryFixture>>} */
  let fixture;

  beforeEach(async () => {
    runtime = createKnowledgeRuntime();
    fixture = await seedQueryFixture(runtime.knowledge);
  });

  describe('company filtering', () => {
    it('filters by industry, location, technology, and confidenceMin', async () => {
      const { knowledge } = runtime;
      const byIndustry = await knowledge.findCompanies({
        tenantId: '10',
        industry: 'property',
      });
      assert.equal(byIndustry.length, 1);
      assert.equal(byIndustry[0].name, 'Lodgism');

      const byLocation = await knowledge.findCompanies({
        tenantId: '10',
        location: 'Nashville',
      });
      assert.equal(byLocation.length, 1);
      assert.equal(byLocation[0].name, 'CleanCo');

      const byTech = await knowledge.findCompanies({
        tenantId: '10',
        technology: 'Guesty',
      });
      assert.equal(byTech.length, 1);

      const confident = await knowledge.findCompanies({
        tenantId: '10',
        confidenceMin: 0.6,
      });
      assert.equal(confident.length, 1);
      assert.equal(confident[0].name, 'Lodgism');
    });
  });

  describe('person filtering', () => {
    it('filters by email, title, and companyId', async () => {
      const { knowledge } = runtime;
      const byEmail = await knowledge.findPeople({
        tenantId: '10',
        email: 'alex@lodgism.com',
      });
      assert.equal(byEmail.length, 1);

      const byCompany = await knowledge.findPeople({
        tenantId: '10',
        companyId: fixture.company.id,
      });
      assert.equal(byCompany.length, 1);
      assert.equal(byCompany[0].id, fixture.person.id);

      const owners = await knowledge.findPeople({
        tenantId: '10',
        title: 'Owner',
      });
      assert.equal(owners.length, 1);
    });
  });

  describe('neighbor traversal', () => {
    it('returns deterministic WORKS_FOR / HAS_CONTACT neighbors', async () => {
      const { knowledge } = runtime;
      const contacts = await knowledge.neighbors({
        tenantId: '10',
        nodeId: fixture.company.id,
        edgeTypes: [EDGE_TYPES.HAS_CONTACT, EDGE_TYPES.WORKS_FOR],
        direction: 'both',
      });
      assert.ok(contacts.length >= 1);
      // Deterministic: sorted by edge.id
      for (let i = 1; i < contacts.length; i++) {
        assert.ok(
          String(contacts[i - 1].edge.id).localeCompare(String(contacts[i].edge.id)) <= 0
        );
      }

      const worksForOnly = await knowledge.neighbors({
        tenantId: '10',
        nodeId: fixture.person.id,
        edgeType: EDGE_TYPES.WORKS_FOR,
        direction: 'out',
      });
      assert.equal(worksForOnly.length, 1);
      assert.equal(worksForOnly[0].node.id, fixture.company.id);
    });
  });

  describe('multi-hop traversal', () => {
    it('walks Company → Person → Interaction within depth', async () => {
      const { knowledge } = runtime;
      const related = await knowledge.related({
        tenantId: '10',
        nodeId: fixture.company.id,
        depth: 2,
      });
      const types = new Set(related.map((r) => r.node.type));
      assert.ok(types.has(NODE_TYPES.PERSON));
      assert.ok(types.has(NODE_TYPES.INTERACTION) || types.has(NODE_TYPES.EVIDENCE) || types.has(NODE_TYPES.CLAIM));

      const depth1 = await knowledge.related({
        tenantId: '10',
        nodeId: fixture.company.id,
        depth: 1,
      });
      assert.ok(depth1.every((r) => r.depth === 1));
      assert.ok(depth1.length < related.length || depth1.length === related.length);
    });
  });

  describe('timeline ordering', () => {
    it('returns chronological events for a company', async () => {
      const { knowledge } = runtime;
      const events = await knowledge.timeline({
        tenantId: '10',
        nodeId: fixture.company.id,
      });
      assert.ok(events.length >= 2);
      for (let i = 1; i < events.length; i++) {
        assert.ok(String(events[i - 1].at) <= String(events[i].at));
      }
      const kinds = new Set(events.map((e) => e.kind));
      assert.ok(kinds.has(NODE_TYPES.COMPANY));
    });
  });

  describe('path finding', () => {
    it('returns Node → Edge → Node path from company to interaction', async () => {
      const { knowledge } = runtime;
      const path = await knowledge.path({
        tenantId: '10',
        fromId: fixture.company.id,
        toId: fixture.interaction.id,
      });
      assert.ok(path);
      assert.ok(path.hops.length >= 3);
      assert.equal(path.hops[0].node.id, fixture.company.id);
      assert.equal(path.hops[0].edge, null);
      assert.equal(path.hops[path.hops.length - 1].node.id, fixture.interaction.id);
      assert.ok(path.hops[1].edge);
      assert.equal(path.length, path.hops.length - 1);
    });

    it('returns null when no path exists', async () => {
      const { knowledge } = runtime;
      const path = await knowledge.path({
        tenantId: '10',
        fromId: fixture.company.id,
        toId: fixture.stranger.id,
        edgeTypes: [EDGE_TYPES.SUPPORTS],
        maxDepth: 3,
      });
      assert.equal(path, null);
    });
  });

  describe('confidence filtering', () => {
    it('filters claims and evidence by confidenceMin', async () => {
      const { knowledge } = runtime;
      const claims = await knowledge.findClaims({
        tenantId: '10',
        confidenceMin: 0.8,
      });
      assert.equal(claims.length, 1);

      const evidence = await knowledge.findEvidence({
        tenantId: '10',
        confidenceMin: 0.9,
      });
      assert.equal(evidence.length, 0);

      const evidOk = await knowledge.findEvidence({
        tenantId: '10',
        confidenceMin: 0.8,
        aboutNodeId: fixture.company.id,
      });
      assert.equal(evidOk.length, 1);
    });
  });

  describe('tenant isolation', () => {
    it('does not leak query results across tenants', async () => {
      const { knowledge } = runtime;
      await knowledge.createNode({
        tenantId: '1',
        type: NODE_TYPES.COMPANY,
        name: 'Other Tenant Co',
        metadata: { industry: 'property_management', location: 'Manchester NH' },
      });

      const companies = await knowledge.findCompanies({
        tenantId: '10',
        industry: 'property',
      });
      assert.ok(companies.every((c) => c.tenantId === '10'));
      assert.ok(!companies.some((c) => c.name === 'Other Tenant Co'));

      const bleed = await knowledge.findCompanies({ tenantId: '1', industry: 'property' });
      assert.equal(bleed.length, 1);
      assert.equal(bleed[0].name, 'Other Tenant Co');

      const neighbors = await knowledge.neighbors({
        tenantId: '1',
        nodeId: fixture.company.id,
      });
      assert.equal(neighbors.length, 0);
    });
  });

  describe('explainability', () => {
    it('includes timeline position on claim explanations', async () => {
      const { knowledge } = runtime;
      const explanation = await knowledge.explain({
        tenantId: '10',
        nodeId: fixture.claim.id,
      });
      assert.equal(explanation.claim.id, fixture.claim.id);
      assert.ok(explanation.supportingEvidence.length >= 1);
      assert.ok(explanation.originalSources[0].sourceType);
      assert.ok(explanation.timelinePosition);
      assert.ok(explanation.timelinePosition.index >= 0);
      assert.ok(explanation.reason !== undefined);

      // Legacy signature still works
      const legacy = await knowledge.explain('10', fixture.claim.id);
      assert.equal(legacy.claim.id, fixture.claim.id);
    });
  });

  describe('performance metric emission', () => {
    it('emits structured metrics for every query', async () => {
      const emitted = [];
      const { knowledge } = createKnowledgeRuntime({
        onQueryMetrics: (m) => emitted.push(m),
      });
      const seeded = await seedQueryFixture(knowledge);

      await knowledge.findCompanies({ tenantId: '10' });
      await knowledge.neighbors({ tenantId: '10', nodeId: seeded.company.id });
      await knowledge.timeline({ tenantId: '10', nodeId: seeded.company.id });

      assert.ok(emitted.length >= 3);
      for (const m of emitted) {
        assert.ok(typeof m.queryName === 'string');
        assert.ok(typeof m.executionTimeMs === 'number');
        assert.ok(typeof m.nodesVisited === 'number');
        assert.ok(typeof m.edgesTraversed === 'number');
        assert.ok(typeof m.resultsReturned === 'number');
        assert.equal(m.repositoryType, 'in-memory');
      }

      const last = knowledge.getLastQueryMetrics();
      assert.ok(last);
      assert.equal(last.repositoryType, 'in-memory');
    });
  });

  describe('KnowledgeService query surface', () => {
    it('exposes all query APIs', () => {
      const { knowledge } = runtime;
      for (const method of [
        'findCompanies',
        'findPeople',
        'findInteractions',
        'findEvidence',
        'findClaims',
        'neighbors',
        'related',
        'timeline',
        'path',
        'explain',
        'getLastQueryMetrics',
      ]) {
        assert.equal(typeof knowledge[method], 'function', method);
      }
      assert.equal(typeof QueryEngine, 'function');
      assert.equal(PersistentGraphRepository.name, 'PersistentGraphRepository');
    });

    it('requires tenantId on query objects', async () => {
      const { knowledge } = runtime;
      await assert.rejects(() => knowledge.findCompanies({}), /tenantId/);
      await assert.rejects(() => knowledge.neighbors({ nodeId: 'x' }), /tenantId/);
    });
  });
});
