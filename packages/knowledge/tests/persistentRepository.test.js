'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const path = require('path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('../../../test/helpers/disposablePostgres');
const {
  createKnowledgeRuntime,
  PersistentGraphRepository,
  ensureKnowledgeSchema,
  isGraphRepository,
  GRAPH_REPOSITORY_METHODS,
  NODE_TYPES,
  mapCompanyRow,
  mapProspectRow,
  mapTouchpointRow,
  companyNodeId,
  personNodeId,
  interactionNodeId,
  MemoryRelationalSource,
} = require('..');

const KNOWLEDGE_SERVICE_PATH = path.join(__dirname, '..', 'services', 'KnowledgeService.js');

describe('SPEC-001 PersistentGraphRepository', () => {
  /** @type {Awaited<ReturnType<typeof startDisposablePostgres>>|null} */
  let instance = null;
  /** @type {import('pg').Pool|null} */
  let pool = null;

  before(async () => {
    instance = await startDisposablePostgres(`knowledge-pg-${process.pid}-`, {
      socketPrefix: 'kpg-',
    });
    pool = new Pool({ connectionString: instance.connectionString });
    await ensureKnowledgeSchema(pool);
  });

  after(async () => {
    if (pool) await pool.end();
    if (instance) await instance.stop();
  });

  async function clearGraph() {
    await pool.query('DELETE FROM knowledge_edges');
    await pool.query('DELETE FROM knowledge_claims');
    await pool.query('DELETE FROM knowledge_evidence');
    await pool.query('DELETE FROM knowledge_nodes');
  }

  it('satisfies the GraphRepository contract with no extra interface surface', () => {
    const repo = new PersistentGraphRepository(pool);
    assert.equal(isGraphRepository(repo), true);
    for (const method of GRAPH_REPOSITORY_METHODS) {
      assert.equal(typeof repo[method], 'function');
    }
  });

  it('survives application restart (new repository + pool client against same DB)', async () => {
    await clearGraph();
    const runtime1 = createKnowledgeRuntime({
      repository: new PersistentGraphRepository(pool),
    });
    await runtime1.sync.apply(
      mapCompanyRow({
        id: 42,
        client_id: 10,
        name: 'Persistent Co',
        created_at: '2026-07-01T00:00:00.000Z',
      })
    );
    const nodeId = companyNodeId('10', 42);
    const before = await runtime1.knowledge.findNode('10', nodeId);
    assert.equal(before.name, 'Persistent Co');

    // Simulate restart: new runtime, same database
    const runtime2 = createKnowledgeRuntime({
      repository: new PersistentGraphRepository(pool),
    });
    const after = await runtime2.knowledge.findNode('10', nodeId);
    assert.ok(after);
    assert.equal(after.name, 'Persistent Co');
    assert.equal(after.type, NODE_TYPES.COMPANY);

    const evidence = await runtime2.knowledge.findEvidence('10');
    assert.equal(evidence.length, 1);
  });

  it('explain() works after restart', async () => {
    await clearGraph();
    const runtime1 = createKnowledgeRuntime({
      repository: new PersistentGraphRepository(pool),
    });
    await runtime1.sync.apply(
      mapProspectRow({
        id: 7,
        client_id: 1,
        first_name: 'Sam',
        last_name: 'Owner',
        email: 'sam@example.com',
        created_at: '2026-07-02T00:00:00.000Z',
      })
    );
    const personId = personNodeId('1', 7);
    const evidence = await runtime1.knowledge.findEvidence('1');
    const claim = await runtime1.knowledge.claims.createClaim({
      tenantId: '1',
      statement: 'Sam is reachable',
      subjectId: personId,
      evidenceIds: evidence.map((e) => e.id),
    });

    const runtime2 = createKnowledgeRuntime({
      repository: new PersistentGraphRepository(pool),
    });
    const explanation = await runtime2.knowledge.explain('1', claim.id);
    assert.ok(explanation);
    assert.equal(explanation.claim.statement, 'Sam is reachable');
    assert.equal(explanation.evidence.length, 1);
    assert.ok(explanation.originalSources[0].sourceType);
    assert.ok(explanation.confidence > 0);
  });

  it('full rebuild produces an equivalent graph on a second pass (idempotent)', async () => {
    await clearGraph();
    const source = new MemoryRelationalSource({
      companies: [
        { id: 1, client_id: 10, name: 'A Co', created_at: '2026-01-01T00:00:00.000Z' },
        { id: 2, client_id: 10, name: 'B Co', created_at: '2026-01-02T00:00:00.000Z' },
      ],
      prospects: [
        {
          id: 11,
          client_id: 10,
          company_id: 1,
          first_name: 'Pat',
          last_name: 'Lee',
          email: 'pat@a.co',
          created_at: '2026-01-03T00:00:00.000Z',
        },
      ],
      touchpoints: [
        {
          id: 21,
          client_id: 10,
          prospect_id: 11,
          channel: 'call',
          action_type: 'logged',
          content_summary: 'Intro call',
          created_at: '2026-01-04T00:00:00.000Z',
        },
      ],
    });

    const runtime = createKnowledgeRuntime({
      repository: new PersistentGraphRepository(pool),
    });
    const first = await runtime.sync.rebuildFromRelational('10', source);
    assert.equal(first.companies.applied, 2);
    assert.equal(first.prospects.applied, 1);
    assert.equal(first.touchpoints.applied, 1);

    const snapshot = async () => {
      const companies = await runtime.knowledge.search('10', 'Co', {
        types: [NODE_TYPES.COMPANY],
      });
      const people = await runtime.knowledge.search('10', 'Pat', {
        types: [NODE_TYPES.PERSON],
      });
      const interactions = await runtime.knowledge.findNode(
        '10',
        interactionNodeId('10', 21)
      );
      const evidence = await runtime.knowledge.findEvidence('10');
      return {
        companyIds: companies.map((c) => c.id).sort(),
        personIds: people.map((p) => p.id).sort(),
        interactionId: interactions?.id || null,
        evidenceCount: evidence.length,
      };
    };

    const beforeReplay = await snapshot();
    const second = await runtime.sync.rebuildFromRelational('10', source);
    assert.equal(second.companies.applied, 0);
    assert.equal(second.companies.skipped, 2);
    assert.equal(second.prospects.skipped, 1);
    assert.equal(second.touchpoints.skipped, 1);
    assert.deepEqual(await snapshot(), beforeReplay);

    assert.ok(await runtime.knowledge.findNode('10', companyNodeId('10', 1)));
    assert.ok(await runtime.knowledge.findNode('10', personNodeId('10', 11)));
  });

  it('does not modify KnowledgeService source (public API stability guard)', async () => {
    const fs = require('fs');
    const crypto = require('crypto');
    const source = fs.readFileSync(KNOWLEDGE_SERVICE_PATH);
    const hash = crypto.createHash('sha1').update(source).digest('hex');
    // Locked at SPEC-001 start; bump only if an explicit later ADR allows API changes.
    assert.equal(
      hash,
      '332e6d6aac9ddab1c9d6c7fe365c90334155fe0f',
      'KnowledgeService.js changed — SPEC-001 forbids public API / service changes'
    );
  });
});
