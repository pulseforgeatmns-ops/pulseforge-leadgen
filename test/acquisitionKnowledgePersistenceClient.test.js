'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  resolvePersistenceClient,
  ownsPersistenceTransaction,
  upsertKnowledgeObject,
  canonicalizeOutreachAssetContent,
} = require('../services/acquisitionKnowledgePersistence');
const { EVIDENCE_TYPES, LIFECYCLE_STATES, VALIDATION_STATES, OBJECT_TYPES } = require('../packages/acquisition-knowledge');

const KAYLEE_SOURCE_TEXT = `**5. Braiden & Kaylee Smith — KB Painting**

**Subject: Quick question about KB**

Hi Braiden and Kaylee,

I came across KB and noticed you've built a team while still remaining personally involved in both the field and operational sides of the business.

Do you find that too much of the company still depends on the two of you personally?

If KB continues expanding, what happens if those responsibilities continue growing with it?

I help small-business owners build teams that can carry more of the business without everything depending on the owners. Would you be open to a short conversation?

Fedir
---
`;

function dbRow(params) {
  return {
    id: params[0],
    external_key: params[1],
    tenant_id: params[2],
    client_id: params[3],
    mission_id: params[4],
    scope: params[5],
    object_type: params[6],
    title: params[7],
    content: params[8],
    epistemic_state: params[9],
    validation_state: params[10],
    epistemic_kind: params[11],
    lifecycle_state: params[12],
    status: params[13],
    channel: params[14],
    experiment_id: params[15],
    tags: params[16],
    confidence: params[17],
    evidence: typeof params[18] === 'string' ? JSON.parse(params[18]) : params[18],
    provenance: params[19],
    derivation: params[20],
    relationships: typeof params[21] === 'string' ? JSON.parse(params[21]) : params[21],
    approved_by: params[22],
    created_from: params[23],
    created_by: params[24],
    version: params[25],
    supersedes_id: params[26],
    created_at: params[27],
    updated_at: params[28],
  };
}

function createFakePool(seed = {}) {
  const state = {
    objects: seed.objects || [],
    revisions: [],
    connectCalls: 0,
    releaseCalls: 0,
  };

  const queryImpl = async (sql, params = []) => {
    const normalized = String(sql).replace(/\s+/g, ' ').trim();
    if (/^BEGIN|^COMMIT|^ROLLBACK/i.test(normalized)) return { rows: [] };
    if (/CREATE TABLE|CREATE INDEX|ALTER TABLE/i.test(normalized)) return { rows: [] };
    if (/UPDATE acquisition_knowledge_objects SET epistemic_state/i.test(normalized)) return { rows: [] };
    if (/UPDATE acquisition_knowledge_objects SET validation_state/i.test(normalized)) return { rows: [] };
    if (/SELECT \* FROM acquisition_knowledge_objects WHERE tenant_id = \$1 AND external_key = \$2/i.test(normalized)) {
      return { rows: state.objects.filter((row) => row.tenant_id === params[0] && row.external_key === params[1]) };
    }
    if (/SELECT \* FROM acquisition_knowledge_objects\s+WHERE id = \$1 AND tenant_id = \$2 AND object_type = 'outreach_asset'/i.test(normalized)) {
      return { rows: state.objects.filter((row) => row.id === params[0] && row.tenant_id === params[1] && row.object_type === 'outreach_asset') };
    }
    if (/UPDATE acquisition_knowledge_objects\s+SET content = \$1/i.test(normalized)) {
      const idx = state.objects.findIndex((row) => row.id === params[2] && row.tenant_id === params[3]);
      assert.notEqual(idx, -1, 'outreach asset row must exist');
      state.objects[idx] = {
        ...state.objects[idx],
        content: params[0],
        version: params[1],
        updated_at: new Date().toISOString(),
      };
      return { rows: [state.objects[idx]] };
    }
    if (/INSERT INTO acquisition_knowledge_objects/i.test(normalized)) {
      const row = dbRow(params);
      const idx = state.objects.findIndex((existing) => existing.id === row.id);
      if (idx >= 0) state.objects[idx] = row;
      else state.objects.push(row);
      return { rows: [row] };
    }
    if (/INSERT INTO acquisition_knowledge_revisions/i.test(normalized)) {
      state.revisions.push(params);
      return { rows: [] };
    }
    throw new Error(`Unhandled SQL in fake pool: ${normalized}`);
  };

  const pool = {
    state,
    async connect() {
      state.connectCalls += 1;
      return {
        query: queryImpl,
        release() {
          state.releaseCalls += 1;
        },
      };
    },
    query: queryImpl,
  };

  return pool;
}

function createAlreadyConnectedClient(pool) {
  const stats = { connectCalls: 0, releaseCalls: 0, endCalls: 0 };
  let connected = true;
  return {
    stats,
    async connect() {
      stats.connectCalls += 1;
      if (connected) {
        throw new Error('Client has already been connected. You cannot reuse a client.');
      }
      connected = true;
    },
    query: (...args) => pool.query(...args),
    release() {
      stats.releaseCalls += 1;
    },
    end() {
      stats.endCalls += 1;
    },
  };
}

function stakeholderEvidence() {
  return [{
    id: 'evidence_1',
    type: EVIDENCE_TYPES.STAKEHOLDER,
    statement: 'Approved package import.',
    source: { kind: 'stakeholder', ref: 'babrun-import' },
    confidence: 1,
    observedAt: '2026-09-10T12:00:00.000Z',
  }];
}

function kayleeOutreachRow() {
  return {
    id: 'ak_babrun_outreach_final_05',
    external_key: null,
    tenant_id: '13',
    client_id: 13,
    mission_id: null,
    scope: 'client',
    object_type: 'outreach_asset',
    title: 'Final first-ten outreach 5: KB Painting',
    content: {
      version: 'Final revised first ten',
      category: 'Outreach Assets',
      sourceText: KAYLEE_SOURCE_TEXT,
      prospectCompany: 'KB Painting',
      prospectContact: 'Braiden & Kaylee Smith',
      approvedPackageSource: 'S09',
    },
    epistemic_state: 'OBSERVED',
    validation_state: 'STAKEHOLDER_VALIDATED',
    epistemic_kind: 'validated_finding',
    lifecycle_state: 'STAKEHOLDER_VALIDATED',
    status: 'approved',
    channel: 'email',
    experiment_id: null,
    tags: [],
    confidence: null,
    evidence: stakeholderEvidence(),
    provenance: { importSource: 'babrun-ak.json' },
    derivation: null,
    relationships: JSON.stringify([{ type: 'targets_prospect', target: { id: 'ak_babrun_prospect_p024' } }]),
    approved_by: null,
    created_from: null,
    created_by: 'operator',
    version: 3,
    supersedes_id: null,
    created_at: '2026-09-10T12:00:00.000Z',
    updated_at: '2026-09-12T08:15:00.000Z',
  };
}

describe('resolvePersistenceClient', () => {
  it('uses opts.client without calling connect', async () => {
    const pool = createFakePool();
    const external = createAlreadyConnectedClient(pool);
    const resolved = await resolvePersistenceClient(pool, { client: external });
    assert.equal(resolved.client, external);
    assert.equal(resolved.ownsClient, false);
    assert.equal(external.stats.connectCalls, 0);
  });

  it('detects checked-out pool client by release() without calling connect', async () => {
    const pool = createFakePool();
    const checkedOut = await pool.connect();
    const resolved = await resolvePersistenceClient(checkedOut, {});
    assert.equal(resolved.client, checkedOut);
    assert.equal(resolved.ownsClient, false);
    assert.equal(pool.state.connectCalls, 1);
  });

  it('checks out and owns a pool client when none is supplied', async () => {
    const pool = createFakePool();
    const resolved = await resolvePersistenceClient(pool, {});
    assert.equal(resolved.ownsClient, true);
    assert.equal(typeof resolved.client.release, 'function');
    assert.equal(pool.state.connectCalls, 1);
    resolved.client.release();
    assert.equal(pool.state.releaseCalls, 1);
  });
});

describe('ownsPersistenceTransaction', () => {
  it('defaults to helper-owned transactions', () => {
    assert.equal(ownsPersistenceTransaction({}), true);
  });

  it('defers transaction control when caller sets inTransaction', () => {
    assert.equal(ownsPersistenceTransaction({ inTransaction: true }), false);
  });
});

describe('canonicalizeOutreachAssetContent client ownership', () => {
  it('works with an externally supplied connected client without double connect', async () => {
    const pool = createFakePool({ objects: [kayleeOutreachRow()] });
    const external = createAlreadyConnectedClient(pool);

    const saved = await canonicalizeOutreachAssetContent('ak_babrun_outreach_final_05', {
      tenantId: '13',
    }, pool, {
      client: external,
      actorId: 'spec247b_backfill',
      actorRole: 'operator',
    });

    assert.equal(external.stats.connectCalls, 0);
    assert.equal(external.stats.releaseCalls, 0);
    assert.equal(external.stats.endCalls, 0);
    assert.equal(saved.canonicalization.changed, true);
    assert.equal(saved.content.subject, 'Quick question about KB');
    assert.equal(saved.validationState, 'STAKEHOLDER_VALIDATED');
    assert.equal(saved.state, 'STAKEHOLDER_VALIDATED');
    assert.equal(saved.content.sourceText, KAYLEE_SOURCE_TEXT);
    assert.equal(pool.state.connectCalls, 0);
  });

  it('connects once and releases once when using pool directly', async () => {
    const pool = createFakePool({ objects: [kayleeOutreachRow()] });

    const saved = await canonicalizeOutreachAssetContent('ak_babrun_outreach_final_05', {
      tenantId: '13',
    }, pool, {
      actorId: 'spec247b_backfill',
      actorRole: 'operator',
    });

    assert.equal(pool.state.connectCalls, 1);
    assert.equal(pool.state.releaseCalls, 1);
    assert.equal(saved.canonicalization.changed, true);
  });

  it('is idempotent on repeat repair', async () => {
    const pool = createFakePool({ objects: [kayleeOutreachRow()] });
    const first = await canonicalizeOutreachAssetContent('ak_babrun_outreach_final_05', { tenantId: '13' }, pool);
    const second = await canonicalizeOutreachAssetContent('ak_babrun_outreach_final_05', { tenantId: '13' }, pool);
    assert.equal(first.canonicalization.changed, true);
    assert.equal(second.canonicalization.skipped, true);
    assert.equal(second.version, first.version);
  });

  it('does not commit or rollback when caller owns the transaction', async () => {
    const pool = createFakePool({ objects: [kayleeOutreachRow()] });
    const external = createAlreadyConnectedClient(pool);
    const statements = [];
    const originalQuery = external.query.bind(external);
    external.query = async (sql, params) => {
      statements.push(String(sql).replace(/\s+/g, ' ').trim());
      return originalQuery(sql, params);
    };

    await canonicalizeOutreachAssetContent('ak_babrun_outreach_final_05', { tenantId: '13' }, pool, {
      client: external,
      inTransaction: true,
    });

    assert.equal(statements.some((sql) => /^BEGIN/i.test(sql)), false);
    assert.equal(statements.some((sql) => /^COMMIT/i.test(sql)), false);
    assert.equal(statements.some((sql) => /^ROLLBACK/i.test(sql)), false);
  });

  it('rolls back only helper-owned transactions on failure', async () => {
    const pool = createFakePool({ objects: [] });
    await assert.rejects(
      () => canonicalizeOutreachAssetContent('ak_missing', { tenantId: '13' }, pool),
      (err) => err.code === 'ak_not_found'
    );
    assert.equal(pool.state.connectCalls, 1);
    assert.equal(pool.state.releaseCalls, 1);
  });
});

describe('upsertKnowledgeObject client ownership', () => {
  it('uses external client without connect or release', async () => {
    const pool = createFakePool();
    const external = createAlreadyConnectedClient(pool);
    const input = {
      tenantId: '13',
      objectType: OBJECT_TYPES.HYPOTHESIS,
      title: 'Test hypothesis',
      state: LIFECYCLE_STATES.HYPOTHESIS,
      validationState: VALIDATION_STATES.UNVALIDATED,
      evidence: [{
        id: 'evidence_1',
        type: EVIDENCE_TYPES.OPERATOR,
        statement: 'Operator note',
        source: { kind: 'operator', ref: 'test' },
        confidence: 1,
        observedAt: '2026-09-10T12:00:00.000Z',
      }],
      derivation: {
        kind: 'test',
        explanation: 'Derived from operator note.',
        basedOn: ['ak_test'],
        evidenceRefs: ['evidence_1'],
      },
    };

    await upsertKnowledgeObject(input, pool, {
      client: external,
      inTransaction: true,
      actorRole: 'operator',
    });

    assert.equal(external.stats.connectCalls, 0);
    assert.equal(external.stats.releaseCalls, 0);
    assert.equal(pool.state.connectCalls, 0);
  });
});
