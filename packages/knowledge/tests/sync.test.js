'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  createKnowledgeRuntime,
  NODE_TYPES,
  EDGE_TYPES,
  MemoryRelationalSource,
  mapCompanyRow,
  mapProspectRow,
  mapTouchpointRow,
  mapImportBatchItem,
  companyNodeId,
  personNodeId,
  interactionNodeId,
  SYNC_EVENTS,
} = require('..');

describe('SPEC-001B Graph Synchronization Engine', () => {
  let runtime;

  beforeEach(() => {
    runtime = createKnowledgeRuntime();
  });

  it('exposes sync on the runtime and never requires repository access from callers', () => {
    assert.equal(typeof runtime.sync.apply, 'function');
    assert.equal(typeof runtime.sync.rebuildFromRelational, 'function');
    assert.equal(runtime.sync.repository, undefined);
  });

  it('upserts company/prospect/touchpoint through KnowledgeService with stable ids', async () => {
    const { sync, knowledge } = runtime;

    const companyEvent = mapCompanyRow({
      id: 101,
      client_id: 10,
      name: 'Lodgism',
      industry: 'property_management',
      location: 'Manchester NH',
      website: 'https://lodgism.com',
      icp_score: 82,
      created_at: '2026-07-01T00:00:00.000Z',
    });
    assert.equal(companyEvent.type, SYNC_EVENTS.COMPANY_UPSERTED);

    const first = await sync.apply(companyEvent);
    assert.equal(first.status, 'applied');

    const companyId = companyNodeId('10', 101);
    const company = await knowledge.findNode('10', companyId);
    assert.equal(company.type, NODE_TYPES.COMPANY);
    assert.equal(company.name, 'Lodgism');

    const prospectEvent = mapProspectRow({
      id: 501,
      client_id: 10,
      company_id: 101,
      first_name: 'Alex',
      last_name: 'Manager',
      email: 'alex@lodgism.com',
      job_title: 'Operations',
      icp_score: 80,
      vertical: 'property_management',
      source: 'import',
      created_at: '2026-07-02T00:00:00.000Z',
    });
    await sync.apply(prospectEvent);

    const personId = personNodeId('10', 501);
    const person = await knowledge.findNode('10', personId);
    assert.equal(person.email, 'alex@lodgism.com');

    const worksFor = await knowledge.findNeighbors('10', personId, {
      edgeType: EDGE_TYPES.WORKS_FOR,
      direction: 'out',
    });
    assert.equal(worksFor.length, 1);
    assert.equal(worksFor[0].node.id, companyId);

    const touchEvent = mapTouchpointRow({
      id: 9001,
      client_id: 10,
      prospect_id: 501,
      channel: 'email',
      action_type: 'open',
      content_summary: 'Opened sequence step 1',
      created_at: '2026-07-03T12:00:00.000Z',
    });
    await sync.apply(touchEvent);

    const interaction = await knowledge.findNode('10', interactionNodeId('10', 9001));
    assert.equal(interaction.type, NODE_TYPES.INTERACTION);
    assert.equal(interaction.channel, 'email');
  });

  it('is idempotent and replayable for the same sync envelope', async () => {
    const { sync, knowledge } = runtime;
    const event = mapCompanyRow({
      id: 77,
      client_id: 1,
      name: 'Replay Co',
      created_at: '2026-07-10T00:00:00.000Z',
    });

    const a = await sync.apply(event);
    const b = await sync.apply(event);
    assert.equal(a.status, 'applied');
    assert.equal(b.status, 'skipped');
    assert.equal(b.reason, 'idempotent_replay');

    const evidence = await knowledge.findEvidence('1');
    assert.equal(evidence.length, 1);

    // Force re-apply updates without duplicating evidence/edges
    const renamed = mapCompanyRow({
      id: 77,
      client_id: 1,
      name: 'Replay Co Renamed',
      created_at: '2026-07-10T00:00:00.000Z',
      updated_at: '2026-07-11T00:00:00.000Z',
    });
    const c = await sync.apply(renamed);
    assert.equal(c.status, 'applied');
    const node = await knowledge.findNode('1', companyNodeId('1', 77));
    assert.equal(node.name, 'Replay Co Renamed');

    // Replaying the renamed event skips
    const d = await sync.apply(renamed);
    assert.equal(d.status, 'skipped');

    const evidenceAfter = await knowledge.findEvidence('1');
    // One evidence for original revision source id company:77 — same sourceId across name change
    assert.equal(evidenceAfter.length, 1);
  });

  it('keeps tenants isolated during sync', async () => {
    const { sync, knowledge } = runtime;
    await sync.apply(
      mapCompanyRow({ id: 1, client_id: 10, name: 'Anchor Co', created_at: '2026-01-01' })
    );
    await sync.apply(
      mapCompanyRow({ id: 1, client_id: 1, name: 'Pulseforge Co', created_at: '2026-01-01' })
    );

    assert.equal((await knowledge.findNode('10', companyNodeId('10', 1))).name, 'Anchor Co');
    assert.equal((await knowledge.findNode('1', companyNodeId('1', 1))).name, 'Pulseforge Co');
    assert.equal(await knowledge.findNode('10', companyNodeId('1', 1)), null);
  });

  it('rebuilds the graph from a relational source without repository access', async () => {
    const { sync, knowledge } = runtime;
    const source = new MemoryRelationalSource({
      companies: [
        { id: 1, client_id: 10, name: 'A Co', created_at: '2026-01-01' },
        { id: 2, client_id: 10, name: 'B Co', created_at: '2026-01-02' },
        { id: 9, client_id: 99, name: 'Other Tenant', created_at: '2026-01-01' },
      ],
      prospects: [
        {
          id: 11,
          client_id: 10,
          company_id: 1,
          first_name: 'Pat',
          last_name: 'Lee',
          email: 'pat@a.co',
          created_at: '2026-01-03',
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
          created_at: '2026-01-04',
        },
      ],
    });

    const summary = await sync.rebuildFromRelational('10', source, { pageSize: 1 });
    assert.equal(summary.companies.read, 2);
    assert.equal(summary.prospects.read, 1);
    assert.equal(summary.touchpoints.read, 1);
    assert.equal(summary.companies.applied, 2);
    assert.equal(summary.failed, 0);

    assert.ok(await knowledge.findNode('10', companyNodeId('10', 1)));
    assert.ok(await knowledge.findNode('10', companyNodeId('10', 2)));
    assert.ok(await knowledge.findNode('10', personNodeId('10', 11)));
    assert.ok(await knowledge.findNode('10', interactionNodeId('10', 21)));
    assert.equal(await knowledge.findNode('10', companyNodeId('99', 9)), null);

    // Full rebuild replay is idempotent
    const replay = await sync.rebuildFromRelational('10', source, { pageSize: 50 });
    assert.equal(replay.companies.applied, 0);
    assert.equal(replay.companies.skipped, 2);
    assert.equal(replay.prospects.skipped, 1);
    assert.equal(replay.touchpoints.skipped, 1);
  });

  it('maps import batch items with distinct idempotency keys', async () => {
    const { sync, knowledge } = runtime;
    const item = mapImportBatchItem(
      {
        kind: 'prospect',
        id: 42,
        client_id: 10,
        first_name: 'Imported',
        last_name: 'Lead',
        email: 'import@example.com',
        created_at: '2026-07-20',
      },
      { importBatchId: 'batch-1' }
    );
    assert.equal(item.type, SYNC_EVENTS.IMPORT_BATCH_ITEM);
    assert.match(item.id, /import_prospect/);

    await sync.apply(item);
    const person = await knowledge.findNode('10', personNodeId('10', 42));
    assert.equal(person.name, 'Imported Lead');
    assert.equal(person.metadata.importBatchId, 'batch-1');
  });

  it('applyMany continues on error when requested', async () => {
    const { sync } = runtime;
    const good = mapCompanyRow({ id: 1, client_id: 5, name: 'Good', created_at: '2026-01-01' });
    const bad = { type: SYNC_EVENTS.COMPANY_UPSERTED, tenantId: '5', id: 'bad-key', payload: {} };
    const result = await sync.applyMany([bad, good], { continueOnError: true });
    assert.equal(result.failed, 1);
    assert.equal(result.applied, 1);
  });
});
