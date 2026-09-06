'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const ak = require('../services/acquisitionKnowledge');
const amo = require('../packages/acquisition-mission');
const {
  extractLearningCandidateRows,
  persistLearningCandidatesForStageCommit,
} = require('../services/acquisitionKnowledgePersistence');

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
    epistemic_kind: params[9],
    lifecycle_state: params[10],
    status: params[11],
    channel: params[12],
    experiment_id: params[13],
    tags: params[14],
    confidence: params[15],
    evidence: typeof params[16] === 'string' ? JSON.parse(params[16]) : params[16],
    provenance: params[17],
    approved_by: params[18],
    created_from: params[19],
    created_by: params[20],
    version: params[21],
    supersedes_id: params[22],
    created_at: params[23],
    updated_at: params[24],
  };
}

function createFakePool() {
  const state = { objects: [], revisions: [], decisions: [], statements: [] };
  const api = {
    state,
    async connect() {
      return {
        query: api.query,
        release() {},
      };
    },
    async query(sql, params = []) {
      state.statements.push(String(sql).replace(/\s+/g, ' ').trim());
      if (/^BEGIN|^COMMIT|^ROLLBACK/i.test(sql)) return { rows: [] };
      if (/CREATE TABLE|CREATE INDEX/i.test(sql)) return { rows: [] };
      if (/SELECT \* FROM acquisition_knowledge_objects WHERE tenant_id = \$1 AND external_key = \$2/i.test(sql)) {
        return { rows: state.objects.filter((row) => row.tenant_id === params[0] && row.external_key === params[1]) };
      }
      if (/SELECT \* FROM acquisition_knowledge_objects WHERE id = \$1 AND tenant_id = \$2/i.test(sql)) {
        return { rows: state.objects.filter((row) => row.id === params[0] && row.tenant_id === params[1]) };
      }
      if (/INSERT INTO acquisition_knowledge_objects/i.test(sql)) {
        const row = dbRow(params);
        const idx = state.objects.findIndex((existing) => existing.id === row.id);
        if (idx >= 0) state.objects[idx] = row;
        else state.objects.push(row);
        return { rows: [row] };
      }
      if (/UPDATE acquisition_knowledge_objects/i.test(sql)) {
        const idx = state.objects.findIndex((row) => row.id === params[6] && row.tenant_id === params[7]);
        assert.notEqual(idx, -1);
        state.objects[idx] = {
          ...state.objects[idx],
          lifecycle_state: params[0],
          epistemic_kind: params[1],
          status: params[2],
          evidence: typeof params[3] === 'string' ? JSON.parse(params[3]) : params[3],
          approved_by: params[4],
          version: params[5],
          updated_at: new Date().toISOString(),
        };
        return { rows: [state.objects[idx]] };
      }
      if (/INSERT INTO acquisition_knowledge_revisions/i.test(sql)) {
        state.revisions.push(params);
        return { rows: [] };
      }
      if (/INSERT INTO acquisition_knowledge_decisions/i.test(sql)) {
        state.decisions.push(params);
        return { rows: [] };
      }
      if (/SELECT \* FROM acquisition_knowledge_objects/i.test(sql)) {
        let rows = [...state.objects].filter((row) => row.tenant_id === params[0]);
        if (/object_type = \$/i.test(sql)) rows = rows.filter((row) => params.includes(row.object_type));
        if (/lifecycle_state = \$/i.test(sql)) rows = rows.filter((row) => params.includes(row.lifecycle_state));
        if (/status = \$/i.test(sql)) rows = rows.filter((row) => params.includes(row.status));
        if (/\( LOWER\(title\)/i.test(sql)) {
          const q = String(params.find((param) => typeof param === 'string' && param.startsWith('%')) || '')
            .replace(/^%|%$/g, '')
            .toLowerCase();
          rows = rows.filter((row) => JSON.stringify(row).toLowerCase().includes(q));
        }
        return { rows };
      }
      throw new Error(`Unhandled fake SQL: ${sql}`);
    },
  };
  return api;
}

async function withPool(fn) {
  await fn(createFakePool());
}

function sqlTransactionCount(pool, keyword) {
  return pool.state.statements.filter((statement) => statement === keyword).length;
}

function missionFixture(overrides = {}) {
  return {
    id: 'mission_247',
    tenantId: '10',
    clientId: 10,
    stage: 'learn',
    title: 'Founder-led service campaign',
    objective: 'Acquire founder-led service businesses',
    ...overrides,
  };
}

function outcomeFixture(overrides = {}) {
  return {
    id: 'outcome_reply_1',
    type: 'reply',
    label: 'Founder replied positively',
    at: '2026-09-06T12:00:00.000Z',
    ...overrides,
  };
}

function contributionFixture(overrides = {}) {
  return {
    id: 'contrib_paige_1',
    specialist: 'paige',
    kind: 'variants',
    payload: { variantLabel: 'Founder dependency' },
    ...overrides,
  }
}

test('SPEC-247 persists tenant-scoped acquisition knowledge and explains evidence', async () => {
  await withPool(async (pool) => {
    const first = await ak.createKnowledge({
      tenantId: '10',
      objectType: ak.OBJECT_TYPES.HYPOTHESIS,
      title: 'Founder-led operators reply to dependency language',
      content: { statement: 'Founder dependency should be named directly in discovery outreach.' },
      tags: ['founder_dependency'],
      evidence: [{
        type: ak.EVIDENCE_TYPES.OBSERVED,
        statement: 'Prior founder-dependent prospects replied to owner-capacity framing.',
        source: { kind: 'operator_note', ref: 'babrun-playbook' },
      }],
    }, { pool, actor: { id: 'max', role: 'max' } });

    assert.equal(first.state, ak.LIFECYCLE_STATES.HYPOTHESIS);
    assert.equal(first.tenantId, '10');

    const hidden = await ak.retrieveKnowledge({ tenantId: '11', q: 'Founder-led' }, { pool });
    assert.equal(hidden.length, 0);

    const explained = await ak.explainRecommendation({
      tenantId: '10',
      recommendation: 'Use founder dependency language',
      query: { q: 'dependency' },
    }, { pool, actor: { id: 'operator', role: 'operator' } });

    assert.equal(explained.invented, false);
    assert.equal(explained.basis.length, 1);
    assert.equal(explained.basis[0].id, first.id);
    assert.equal(explained.basis[0].evidence[0].source.ref, 'babrun-playbook');
  });
});

test('SPEC-247 promotion is operator-only, sequential, and evidence-backed', async () => {
  await withPool(async (pool) => {
    const hypothesis = await ak.createKnowledge({
      tenantId: '10',
      objectType: ak.OBJECT_TYPES.HYPOTHESIS,
      title: 'Priority after stakeholder review',
      content: { statement: 'A stakeholder preference can become validated only with explicit review.' },
    }, { pool, actor: { id: 'max', role: 'max' } });

    await assert.rejects(
      ak.promoteKnowledge(hypothesis.id, {
        tenantId: '10',
        state: ak.LIFECYCLE_STATES.CANONICAL,
        evidence: [{ type: ak.EVIDENCE_TYPES.OPERATOR, statement: 'Approved', source: { kind: 'review' } }],
      }, { pool, actor: { id: 'operator', role: 'operator' } }),
      /sequential/
    );

    await assert.rejects(
      ak.promoteKnowledge(hypothesis.id, {
        tenantId: '10',
        state: ak.LIFECYCLE_STATES.STAKEHOLDER_VALIDATED,
        evidence: [{ type: ak.EVIDENCE_TYPES.OBSERVED, statement: 'Observed', source: { kind: 'note' } }],
      }, { pool, actor: { id: 'operator', role: 'operator' } }),
      /does not support/
    );

    await assert.rejects(
      ak.promoteKnowledge(hypothesis.id, {
        tenantId: '10',
        state: ak.LIFECYCLE_STATES.STAKEHOLDER_VALIDATED,
        evidence: [{ type: ak.EVIDENCE_TYPES.STAKEHOLDER, statement: 'Validated', source: { kind: 'call' } }],
      }, { pool, actor: { id: 'scout', role: 'scout' } }),
      /Only an operator/
    );

    const promoted = await ak.promoteKnowledge(hypothesis.id, {
      tenantId: '10',
      state: ak.LIFECYCLE_STATES.STAKEHOLDER_VALIDATED,
      evidence: [{ type: ak.EVIDENCE_TYPES.STAKEHOLDER, statement: 'Validated', source: { kind: 'call' } }],
    }, { pool, actor: { id: 'operator', role: 'operator' } });

    assert.equal(promoted.state, ak.LIFECYCLE_STATES.STAKEHOLDER_VALIDATED);
    assert.equal(promoted.version, 2);
  });
});

test('SPEC-247 specialist execution contract receives canonical acquisition knowledge context', async () => {
  const store = amo.createMemoryAmoStore();
  const engine = amo.createAcquisitionMissionEngine({ store });
  const mission = engine.create({
    tenantId: '10',
    objective: 'Acquire founder-led service businesses',
  });
  store.putAcquisitionKnowledge({
    id: 'ak_asset_1',
    tenantId: '10',
    objectType: ak.OBJECT_TYPES.OUTREACH_ASSET,
    title: 'Founder dependency email',
    status: 'approved',
    state: ak.LIFECYCLE_STATES.CANONICAL,
    evidence: [{ id: 'e1', type: ak.EVIDENCE_TYPES.OPERATOR, statement: 'Approved by operator' }],
  });

  const input = amo.buildExecutionInput({
    mission,
    specialist: amo.SPECIALISTS.PAIGE,
    store,
  });

  assert.equal(input.memoryContext.acquisitionKnowledge.spec, ak.SPEC);
  assert.equal(input.memoryContext.acquisitionKnowledge.approvedAssets.length, 1);
  assert.match(input.memoryContext.acquisitionKnowledge.boundary, /Paige consumes approved assets/);
});

test('SPEC-247 import validates by default and writes only with apply=true', async () => {
  await withPool(async (pool) => {
    const dryRun = await ak.importKnowledge({
      tenantId: '10',
      sourceName: 'babrun-fixture',
      objects: [{
        objectType: ak.OBJECT_TYPES.PLAYBOOK,
        title: 'Babrun acquisition playbook',
        content: { discovery: 'Ask about owner bottlenecks.' },
      }],
    }, { pool, actor: { id: 'operator', role: 'operator' } });

    assert.equal(dryRun.dryRun, true);
    assert.equal((await ak.retrieveKnowledge({ tenantId: '10' }, { pool })).length, 0);

    const imported = await ak.importKnowledge({
      tenantId: '10',
      apply: true,
      sourceName: 'babrun-fixture',
      objects: dryRun.objects,
    }, { pool, actor: { id: 'operator', role: 'operator' } });

    assert.equal(imported.dryRun, false);
    assert.equal((await ak.retrieveKnowledge({ tenantId: '10', objectType: ak.OBJECT_TYPES.PLAYBOOK }, { pool })).length, 1);
  });
});

test('SPEC-247 AMO outcome commit creates reviewable learning candidates atomically', async () => {
  await withPool(async (pool) => {
    const mission = missionFixture();
    const candidates = extractLearningCandidateRows({
      mission,
      outcomes: [outcomeFixture()],
      contributions: [contributionFixture()],
    });

    assert.equal(candidates.length, 1);
    assert.equal(candidates[0].state, ak.LIFECYCLE_STATES.HYPOTHESIS);
    assert.equal(candidates[0].evidence[0].type, ak.EVIDENCE_TYPES.CAMPAIGN_OUTCOME);

    await pool.query('BEGIN');
    await persistLearningCandidatesForStageCommit({
      mission,
      outcomes: [outcomeFixture()],
      contributions: [contributionFixture()],
    }, pool, {});
    await pool.query('COMMIT');

    const saved = await ak.retrieveKnowledge({
      tenantId: '10',
      objectType: ak.OBJECT_TYPES.LEARNING,
      status: 'learning_candidate',
      missionId: mission.id,
    }, { pool });

    assert.equal(saved.length, 1);
    assert.equal(sqlTransactionCount(pool, 'BEGIN'), 1);
    assert.equal(sqlTransactionCount(pool, 'COMMIT'), 1);
  });
});
