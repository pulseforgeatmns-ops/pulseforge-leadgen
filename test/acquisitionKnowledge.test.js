'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
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
      if (/ALTER TABLE/i.test(sql)) return { rows: [] };
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
      if (/UPDATE acquisition_knowledge_objects\s+SET epistemic_state/i.test(sql)) return { rows: [] };
      if (/UPDATE acquisition_knowledge_objects\s+SET validation_state/i.test(sql)) return { rows: [] };
      if (/UPDATE acquisition_knowledge_objects/i.test(sql)) {
        const idx = state.objects.findIndex((row) => row.id === params[7] && row.tenant_id === params[8]);
        assert.notEqual(idx, -1);
        state.objects[idx] = {
          ...state.objects[idx],
          lifecycle_state: params[0],
          validation_state: params[1],
          epistemic_kind: params[2],
          status: params[3],
          evidence: typeof params[4] === 'string' ? JSON.parse(params[4]) : params[4],
          approved_by: params[5],
          version: params[6],
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
        if (/epistemic_state = \$/i.test(sql)) rows = rows.filter((row) => params.includes(row.epistemic_state));
        if (/validation_state = \$/i.test(sql)) rows = rows.filter((row) => params.includes(row.validation_state));
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
        evidence: [{ type: ak.EVIDENCE_TYPES.STAKEHOLDER, statement: 'Validated', source: { kind: 'call', ref: 'call_1', stakeholderId: 'fedir' } }],
      }, { pool, actor: { id: 'scout', role: 'scout' } }),
      /Only an operator/
    );

    const promoted = await ak.promoteKnowledge(hypothesis.id, {
      tenantId: '10',
      state: ak.LIFECYCLE_STATES.STAKEHOLDER_VALIDATED,
      evidence: [{ type: ak.EVIDENCE_TYPES.STAKEHOLDER, statement: 'Validated', source: { kind: 'call', ref: 'call_1', stakeholderId: 'fedir' } }],
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
    epistemicState: ak.EPISTEMIC_STATES.OBSERVED,
    validationState: ak.VALIDATION_STATES.UNVALIDATED,
    evidence: [{ id: 'e1', type: ak.EVIDENCE_TYPES.OPERATOR, statement: 'Approved by operator', source: { kind: 'operator_review', ref: 'review_1' } }],
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

test('SPEC-247A observed unvalidated claim preserves provenance on round trip', async () => {
  await withPool(async (pool) => {
    const claim = await ak.createKnowledge({
      tenantId: '10',
      objectType: ak.OBJECT_TYPES.PROSPECT_INTELLIGENCE,
      title: 'Hugo personally leads every project',
      epistemicState: ak.EPISTEMIC_STATES.OBSERVED,
      validationState: ak.VALIDATION_STATES.UNVALIDATED,
      content: { statement: 'Hugo personally leads every project.' },
      evidence: [{
        type: ak.EVIDENCE_TYPES.OBSERVED,
        statement: 'Hugo said he leads each project personally.',
        source: { kind: 'stakeholder_call', ref: 'call_hugo_1' },
        passage: 'I personally lead every project.',
      }],
      provenance: { sourceType: 'call_transcript', sourceRef: 'call_hugo_1' },
    }, { pool, actor: { id: 'scout', role: 'scout' } });

    const [roundTrip] = await ak.retrieveKnowledge({ tenantId: '10', q: 'Hugo' }, { pool });
    assert.equal(roundTrip.id, claim.id);
    assert.equal(roundTrip.epistemicState, ak.EPISTEMIC_STATES.OBSERVED);
    assert.equal(roundTrip.validationState, ak.VALIDATION_STATES.UNVALIDATED);
    assert.equal(roundTrip.validationStatus, ak.VALIDATION_STATUS.UNVALIDATED);
    assert.equal(roundTrip.evidence[0].source.ref, 'call_hugo_1');
    assert.equal(roundTrip.provenance.sourceRef, 'call_hugo_1');
  });
});

test('SPEC-247A inferred stakeholder-validated claim preserves evidence and derivation', async () => {
  await withPool(async (pool) => {
    const claim = await ak.createKnowledge({
      tenantId: '10',
      objectType: ak.OBJECT_TYPES.HYPOTHESIS,
      title: 'Crown Coast may depend on Hugo',
      epistemicState: ak.EPISTEMIC_STATES.INFERRED,
      validationState: ak.VALIDATION_STATES.STAKEHOLDER_VALIDATED,
      evidence: [{
        type: ak.EVIDENCE_TYPES.STAKEHOLDER,
        statement: 'Fedir approved testing founder-dependency language.',
        source: { kind: 'stakeholder_review', ref: 'review_fedir_1', stakeholderId: 'fedir' },
      }],
      derivation: {
        explanation: 'The dependency relationship is inferred from Hugo personally leading every project.',
        basedOn: ['claim_hugo_leads_projects'],
      },
    }, { pool, actor: { id: 'max', role: 'max' } });

    assert.equal(claim.epistemicState, ak.EPISTEMIC_STATES.INFERRED);
    assert.equal(claim.validationState, ak.VALIDATION_STATES.STAKEHOLDER_VALIDATED);
    assert.equal(claim.derivation.basedOn[0], 'claim_hugo_leads_projects');
  });
});

test('SPEC-247A UNKNOWN remains UNKNOWN through normalization and legacy kinds', () => {
  const base = {
    tenantId: '10',
    objectType: ak.OBJECT_TYPES.LEARNING,
    title: 'Unresolved owner dependency',
    epistemicState: ak.EPISTEMIC_STATES.UNKNOWN,
    validationState: ak.VALIDATION_STATES.UNVALIDATED,
    content: { unknownReason: 'No interview evidence yet.' },
  };

  assert.equal(ak.normalizeKnowledgeObject(base).epistemicState, ak.EPISTEMIC_STATES.UNKNOWN);
  assert.equal(ak.normalizeKnowledgeObject({ ...base, kind: ak.EPISTEMIC_KINDS.HYPOTHESIS }).epistemicState, ak.EPISTEMIC_STATES.UNKNOWN);
  assert.equal(ak.normalizeKnowledgeObject({ ...base, kind: ak.EPISTEMIC_KINDS.VALIDATED_FINDING }).epistemicState, ak.EPISTEMIC_STATES.UNKNOWN);
  assert.equal(ak.normalizeKnowledgeObject({ ...base, validationStatus: ak.VALIDATION_STATUS.UNVALIDATED }).validationState, ak.VALIDATION_STATES.UNVALIDATED);
});

test('SPEC-247A validation rejects missing required provenance', () => {
  const base = {
    tenantId: '10',
    objectType: ak.OBJECT_TYPES.LEARNING,
    title: 'Market evidence must be attributed',
    epistemicState: ak.EPISTEMIC_STATES.OBSERVED,
    evidence: [{
      type: ak.EVIDENCE_TYPES.OBSERVED,
      statement: 'A reply happened.',
      source: { kind: 'campaign_outcome', ref: 'outcome_1' },
    }],
  };

  assert.throws(() => ak.normalizeKnowledgeObject({
    ...base,
    validationState: ak.VALIDATION_STATES.MARKET_VALIDATED,
  }), /MARKET_VALIDATED/);

  assert.throws(() => ak.normalizeKnowledgeObject({
    ...base,
    validationState: ak.VALIDATION_STATES.STAKEHOLDER_VALIDATED,
  }), /STAKEHOLDER_VALIDATED/);
});

test('SPEC-247A inferred claims require derivation metadata', () => {
  assert.throws(() => ak.normalizeKnowledgeObject({
    tenantId: '10',
    objectType: ak.OBJECT_TYPES.HYPOTHESIS,
    title: 'Inference without derivation',
    epistemicState: ak.EPISTEMIC_STATES.INFERRED,
    validationState: ak.VALIDATION_STATES.UNVALIDATED,
    evidence: [{
      type: ak.EVIDENCE_TYPES.OBSERVED,
      statement: 'Observed support exists.',
      source: { kind: 'note', ref: 'note_1' },
    }],
  }), /derivation/);
});

test('SPEC-247A supplied relationship survives persistence with independent states', async () => {
  await withPool(async (pool) => {
    const row = await ak.createKnowledge({
      tenantId: '10',
      objectType: ak.OBJECT_TYPES.PROSPECT_INTELLIGENCE,
      title: 'Crown Coast dependency map',
      epistemicState: ak.EPISTEMIC_STATES.OBSERVED,
      validationState: ak.VALIDATION_STATES.UNVALIDATED,
      evidence: [{
        type: ak.EVIDENCE_TYPES.OBSERVED,
        statement: 'Hugo leads every project.',
        source: { kind: 'call', ref: 'call_hugo_1' },
      }],
      relationships: [{
        predicate: 'may_depend_on',
        source: { id: 'crown_coast', type: 'company' },
        target: { id: 'hugo', type: 'person' },
        epistemicState: ak.EPISTEMIC_STATES.INFERRED,
        validationState: ak.VALIDATION_STATES.STAKEHOLDER_VALIDATED,
        evidence: [{
          type: ak.EVIDENCE_TYPES.STAKEHOLDER,
          statement: 'Fedir approved testing the Hugo dependency hypothesis.',
          source: { kind: 'stakeholder_review', ref: 'review_fedir_1', stakeholderId: 'fedir' },
        }],
        derivation: {
          explanation: 'A company may depend on the person who personally leads every project.',
          basedOn: ['claim_hugo_leads_projects'],
        },
        provenance: { sourceType: 'inference', sourceRef: 'dep_map_1' },
      }],
    }, { pool, actor: { id: 'scout', role: 'scout' } });

    const [roundTrip] = await ak.retrieveKnowledge({ tenantId: '10', q: 'Crown Coast' }, { pool });
    assert.equal(roundTrip.id, row.id);
    assert.equal(roundTrip.relationships.length, 1);
    assert.equal(roundTrip.relationships[0].predicate, 'may_depend_on');
    assert.equal(roundTrip.relationships[0].epistemicState, ak.EPISTEMIC_STATES.INFERRED);
    assert.equal(roundTrip.relationships[0].validationState, ak.VALIDATION_STATES.STAKEHOLDER_VALIDATED);
    assert.equal(roundTrip.relationships[0].evidence[0].source.ref, 'review_fedir_1');
  });
});

test('SPEC-247A migration derives only unset canonical states', () => {
  const migration = fs.readFileSync(
    path.join(__dirname, '../migrations/2026-09-06-acquisition-knowledge.sql'),
    'utf8'
  );

  assert.match(migration, /UPDATE acquisition_knowledge_objects[\s\S]+WHERE epistemic_state IS NULL OR btrim\(epistemic_state\) = '';/);
  assert.match(migration, /UPDATE acquisition_knowledge_objects[\s\S]+WHERE validation_state IS NULL OR btrim\(validation_state\) = '';/);
  assert.doesNotMatch(migration, /WHERE epistemic_state\s*=\s*'UNKNOWN'/);
});
