'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  createKnowledgeRuntime,
  NODE_TYPES,
  EDGE_TYPES,
  KNOWLEDGE_EVENTS,
  InMemoryGraphRepository,
  isGraphRepository,
  GRAPH_REPOSITORY_METHODS,
  combineConfidences,
} = require('..');

describe('SPEC-001A Knowledge Layer Foundation', () => {
  /** @type {ReturnType<typeof createKnowledgeRuntime>} */
  let runtime;

  beforeEach(() => {
    runtime = createKnowledgeRuntime();
  });

  describe('GraphRepository contract', () => {
    it('InMemoryGraphRepository satisfies the contract', () => {
      const repo = new InMemoryGraphRepository();
      assert.equal(isGraphRepository(repo), true);
      for (const method of GRAPH_REPOSITORY_METHODS) {
        assert.equal(typeof repo[method], 'function');
      }
    });
  });

  describe('node and edge creation', () => {
    it('creates typed domain nodes', async () => {
      const { knowledge } = runtime;
      const company = await knowledge.createNode({
        tenantId: '10',
        type: NODE_TYPES.COMPANY,
        name: 'Lodgism',
      });
      assert.equal(company.type, NODE_TYPES.COMPANY);
      assert.equal(company.tenantId, '10');
      assert.ok(company.id);
      assert.ok(company.createdAt);
      assert.ok(company.updatedAt);
      assert.deepEqual(company.metadata, {});

      const person = await knowledge.createNode({
        tenantId: '10',
        type: NODE_TYPES.PERSON,
        name: 'Alex',
        email: 'alex@example.com',
      });
      const edge = await knowledge.createEdge({
        tenantId: '10',
        type: EDGE_TYPES.WORKS_FOR,
        fromId: person.id,
        toId: company.id,
      });
      assert.equal(edge.type, EDGE_TYPES.WORKS_FOR);

      const neighbors = await knowledge.findNeighbors('10', company.id, {
        edgeType: EDGE_TYPES.WORKS_FOR,
        direction: 'in',
      });
      assert.equal(neighbors.length, 1);
      assert.equal(neighbors[0].node.id, person.id);
    });

    it('rejects unknown edge types', async () => {
      const { knowledge } = runtime;
      const a = await knowledge.createNode({ tenantId: '1', type: NODE_TYPES.PERSON, name: 'A' });
      const b = await knowledge.createNode({ tenantId: '1', type: NODE_TYPES.PERSON, name: 'B' });
      await assert.rejects(
        () =>
          knowledge.createEdge({
            tenantId: '1',
            type: 'FRIENDS_WITH',
            fromId: a.id,
            toId: b.id,
          }),
        /Unknown edge type/
      );
    });
  });

  describe('tenant isolation', () => {
    it('does not leak nodes across tenants', async () => {
      const { knowledge } = runtime;
      const c10 = await knowledge.createNode({
        tenantId: '10',
        type: NODE_TYPES.COMPANY,
        name: 'Anchor Corp',
      });
      const foundOther = await knowledge.findNode('1', c10.id);
      assert.equal(foundOther, null);

      const searchBleed = await knowledge.search('1', 'Anchor');
      assert.equal(searchBleed.length, 0);

      const own = await knowledge.findNode('10', c10.id);
      assert.equal(own.name, 'Anchor Corp');
    });
  });

  describe('confidence calculation', () => {
    it('combines evidence with noisy-OR', () => {
      assert.equal(combineConfidences([0.5]), 0.5);
      const combined = combineConfidences([0.5, 0.5]);
      assert.ok(combined > 0.5);
      assert.ok(combined < 1);
      assert.equal(Number(combined.toFixed(2)), 0.75);
    });

    it('EvidenceEngine.calculateConfidence returns reason + components', async () => {
      const { knowledge } = runtime;
      const e1 = await knowledge.evidence.createEvidence({
        tenantId: '1',
        sourceType: 'scout_insert',
        confidence: 0.6,
        summary: 'Scout found company',
      });
      const e2 = await knowledge.evidence.createEvidence({
        tenantId: '1',
        sourceType: 'brevo_open',
        confidence: 0.4,
        summary: 'Email opened',
      });
      const scored = knowledge.evidence.calculateConfidence([e1, e2]);
      assert.ok(scored.confidence > 0.6);
      assert.equal(scored.components.length, 2);
      assert.match(scored.reason, /Combined 2/);
    });
  });

  describe('claim generation', () => {
    it('creates claims with SUPPORTS evidence and evaluates confidence', async () => {
      const { knowledge } = runtime;
      const company = await knowledge.createNode({
        tenantId: '10',
        type: NODE_TYPES.COMPANY,
        name: 'Lodgism',
      });
      const evidence = await knowledge.evidence.createEvidence({
        tenantId: '10',
        sourceType: 'website',
        sourceId: 'https://lodgism.com',
        summary: 'Public site lists STR cleaning',
        confidence: 0.8,
      });
      await knowledge.evidence.attachEvidence('10', evidence.id, company.id);

      const claim = await knowledge.claims.createClaim({
        tenantId: '10',
        statement: 'Lodgism manages short-term rentals in Manchester',
        subjectId: company.id,
        evidenceIds: [evidence.id],
      });
      assert.equal(claim.type, NODE_TYPES.CLAIM);
      assert.equal(claim.status, 'active');
      assert.equal(claim.confidence, 0.8);

      const evaluated = await knowledge.claims.evaluateClaim('10', claim.id);
      assert.equal(evaluated.confidence, 0.8);

      await knowledge.claims.invalidateClaim('10', claim.id, 'Operator override');
      const invalidated = await knowledge.findNode('10', claim.id);
      assert.equal(invalidated.status, 'invalidated');
      assert.equal(invalidated.confidence, 0);
    });
  });

  describe('explainability chain', () => {
    it('returns Claim → Evidence → Original Source → Confidence → Reason', async () => {
      const { knowledge } = runtime;
      const person = await knowledge.createNode({
        tenantId: '1',
        type: NODE_TYPES.PERSON,
        name: 'Sam Owner',
      });
      const evidence = await knowledge.evidence.createEvidence({
        tenantId: '1',
        sourceType: 'setter_call',
        sourceId: 'call-99',
        summary: 'Decision-maker confirmed on call',
        confidence: 0.9,
      });
      await knowledge.evidence.attachEvidence('1', evidence.id, person.id);
      const claim = await knowledge.claims.createClaim({
        tenantId: '1',
        statement: 'Sam is the cleaning vendor decision-maker',
        subjectId: person.id,
        evidenceIds: [evidence.id],
      });

      const explanation = await knowledge.explain('1', claim.id);
      assert.equal(explanation.claim.id, claim.id);
      assert.equal(explanation.claim.statement, claim.statement);
      assert.equal(explanation.evidence.length, 1);
      assert.equal(explanation.originalSources[0].sourceType, 'setter_call');
      assert.equal(explanation.originalSources[0].sourceId, 'call-99');
      assert.equal(explanation.confidence, 0.9);
      assert.ok(explanation.reason);

      const subjectExplanation = await knowledge.explain('1', person.id);
      assert.equal(subjectExplanation.claims.length, 1);
      assert.equal(subjectExplanation.claims[0].claim.id, claim.id);
    });
  });

  describe('event-driven updates', () => {
    it('ingests Scout/CRM-style events without direct repository writes from producers', async () => {
      const { knowledge, bus, repository } = runtime;

      // Producer only publishes events — does not call repository.
      assert.equal(typeof repository.createNode, 'function');

      const { results } = await bus.publish({
        type: KNOWLEDGE_EVENTS.COMPANY_OBSERVED,
        tenantId: '10',
        payload: {
          nodeId: 'company:10:evertrust',
          name: 'EverTrust PM',
          sourceType: 'scout_insert',
          sourceId: 'prospect-abc',
          confidence: 0.75,
        },
      });
      const company = results[0];
      assert.equal(company.name, 'EverTrust PM');

      await bus.publish({
        type: KNOWLEDGE_EVENTS.PERSON_OBSERVED,
        tenantId: '10',
        payload: {
          nodeId: 'person:10:jordan',
          name: 'Jordan',
          email: 'jordan@evertrustpm.com',
          companyId: company.id,
          sourceType: 'scout_insert',
          sourceId: 'person-jordan',
          confidence: 0.7,
        },
      });

      const people = await knowledge.search('10', 'Jordan', { types: [NODE_TYPES.PERSON] });
      assert.equal(people.length, 1);

      const worksFor = await knowledge.findNeighbors('10', people[0].id, {
        edgeType: EDGE_TYPES.WORKS_FOR,
        direction: 'out',
      });
      assert.equal(worksFor.length, 1);
      assert.equal(worksFor[0].node.id, company.id);

      const evidence = await knowledge.findEvidence('10');
      assert.ok(evidence.length >= 2);

      await bus.publish({
        type: KNOWLEDGE_EVENTS.CLAIM_PROPOSED,
        tenantId: '10',
        payload: {
          statement: 'EverTrust is an STR manager in NH',
          subjectId: company.id,
          evidenceIds: evidence.filter((e) => e.payload?.companyId === company.id).map((e) => e.id),
        },
      });

      const claims = await knowledge.findClaims('10');
      assert.equal(claims.length, 1);
      assert.ok(bus.history().length >= 3);
    });
  });

  describe('KnowledgeService public surface', () => {
    it('exposes required methods and hides repository details', async () => {
      const { knowledge } = runtime;
      for (const method of [
        'createNode',
        'updateNode',
        'ensureNode',
        'createEdge',
        'ensureEdge',
        'findNode',
        'findNeighbors',
        'findEvidence',
        'findClaims',
        'explain',
        'search',
      ]) {
        assert.equal(typeof knowledge[method], 'function', method);
      }
      // Repository handle is private
      assert.equal(knowledge.repository, undefined);
    });

    it('updates nodes and searches', async () => {
      const { knowledge } = runtime;
      const node = await knowledge.createNode({
        tenantId: '5',
        type: NODE_TYPES.COMPANY,
        name: 'Old Name',
      });
      const updated = await knowledge.updateNode('5', node.id, { name: 'New Name' });
      assert.equal(updated.name, 'New Name');
      const hits = await knowledge.search('5', 'New Name');
      assert.equal(hits.length, 1);
    });
  });

  describe('evidence and claim merge', () => {
    it('merges evidence and claims', async () => {
      const { knowledge } = runtime;
      const subject = await knowledge.createNode({
        tenantId: '2',
        type: NODE_TYPES.COMPANY,
        name: 'MergeCo',
      });
      const e1 = await knowledge.evidence.createEvidence({
        tenantId: '2',
        sourceType: 'a',
        confidence: 0.5,
        summary: 'A',
      });
      const e2 = await knowledge.evidence.createEvidence({
        tenantId: '2',
        sourceType: 'b',
        confidence: 0.5,
        summary: 'B',
      });
      await knowledge.evidence.attachEvidence('2', e1.id, subject.id);
      await knowledge.evidence.attachEvidence('2', e2.id, subject.id);
      const mergedEvidence = await knowledge.evidence.mergeEvidence('2', e1.id, e2.id);
      assert.equal(mergedEvidence.confidence, 0.75);
      assert.equal(await knowledge.findNode('2', e2.id), null);

      const c1 = await knowledge.claims.createClaim({
        tenantId: '2',
        statement: 'Claim one',
        subjectId: subject.id,
        evidenceIds: [e1.id],
      });
      const e3 = await knowledge.evidence.createEvidence({
        tenantId: '2',
        sourceType: 'c',
        confidence: 0.4,
        summary: 'C',
      });
      const c2 = await knowledge.claims.createClaim({
        tenantId: '2',
        statement: 'Claim two',
        subjectId: subject.id,
        evidenceIds: [e3.id],
      });
      const mergedClaim = await knowledge.claims.mergeClaims('2', c1.id, c2.id);
      assert.ok(mergedClaim.confidence >= 0.75);
      const secondary = await knowledge.findNode('2', c2.id);
      assert.equal(secondary.status, 'merged');
    });
  });
});
