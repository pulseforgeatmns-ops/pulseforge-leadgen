'use strict';

const ak = require('../packages/acquisition-knowledge');

function defaultPool() {
  return require('../db');
}

async function ensureAcquisitionKnowledgeSchema(pool = defaultPool()) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_knowledge_objects (
      id TEXT PRIMARY KEY,
      external_key TEXT,
      tenant_id TEXT NOT NULL,
      client_id INTEGER,
      mission_id TEXT,
      scope TEXT NOT NULL,
      object_type TEXT NOT NULL,
      title TEXT NOT NULL,
      content JSONB NOT NULL DEFAULT '{}'::jsonb,
      epistemic_state TEXT NOT NULL DEFAULT 'UNKNOWN',
      validation_state TEXT NOT NULL DEFAULT 'UNVALIDATED',
      epistemic_kind TEXT NOT NULL,
      lifecycle_state TEXT NOT NULL,
      status TEXT NOT NULL,
      channel TEXT,
      experiment_id TEXT,
      tags TEXT[] NOT NULL DEFAULT '{}',
      confidence DOUBLE PRECISION,
      evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
      provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
      derivation JSONB,
      relationships JSONB NOT NULL DEFAULT '[]'::jsonb,
      approved_by TEXT,
      created_from TEXT,
      created_by TEXT,
      version INTEGER NOT NULL DEFAULT 1,
      supersedes_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (tenant_id, external_key)
    )
  `);
  await pool.query(`ALTER TABLE acquisition_knowledge_objects ADD COLUMN IF NOT EXISTS epistemic_state TEXT`);
  await pool.query(`ALTER TABLE acquisition_knowledge_objects ADD COLUMN IF NOT EXISTS validation_state TEXT`);
  await pool.query(`ALTER TABLE acquisition_knowledge_objects ADD COLUMN IF NOT EXISTS derivation JSONB`);
  await pool.query(`ALTER TABLE acquisition_knowledge_objects ADD COLUMN IF NOT EXISTS relationships JSONB NOT NULL DEFAULT '[]'::jsonb`);
  await pool.query(`
    UPDATE acquisition_knowledge_objects
    SET epistemic_state = CASE
      WHEN epistemic_kind IN ('observed_fact', 'operator_preference', 'stakeholder_preference', 'canonical_truth') THEN 'OBSERVED'
      WHEN epistemic_kind IN ('hypothesis', 'validated_finding') THEN 'INFERRED'
      ELSE 'UNKNOWN'
    END
    WHERE epistemic_state IS NULL OR btrim(epistemic_state) = ''
  `);
  await pool.query(`
    UPDATE acquisition_knowledge_objects
    SET validation_state = CASE
      WHEN lifecycle_state = 'STAKEHOLDER_VALIDATED' THEN 'STAKEHOLDER_VALIDATED'
      WHEN lifecycle_state IN ('MARKET_VALIDATED', 'CANONICAL') THEN 'MARKET_VALIDATED'
      ELSE 'UNVALIDATED'
    END
    WHERE validation_state IS NULL OR btrim(validation_state) = ''
  `);
  await pool.query(`
    ALTER TABLE acquisition_knowledge_objects
      ALTER COLUMN epistemic_state SET DEFAULT 'UNKNOWN',
      ALTER COLUMN epistemic_state SET NOT NULL,
      ALTER COLUMN validation_state SET DEFAULT 'UNVALIDATED',
      ALTER COLUMN validation_state SET NOT NULL
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_knowledge_revisions (
      id BIGSERIAL PRIMARY KEY,
      knowledge_id TEXT NOT NULL REFERENCES acquisition_knowledge_objects(id) ON DELETE CASCADE,
      tenant_id TEXT NOT NULL,
      version INTEGER NOT NULL,
      operation TEXT NOT NULL,
      actor_id TEXT,
      actor_role TEXT,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
      rationale TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_knowledge_decisions (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      mission_id TEXT,
      recommendation TEXT NOT NULL,
      question TEXT,
      knowledge_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
      explanation JSONB NOT NULL DEFAULT '{}'::jsonb,
      actor_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS acquisition_knowledge_objects_tenant_idx ON acquisition_knowledge_objects (tenant_id, object_type, lifecycle_state)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS acquisition_knowledge_objects_mission_idx ON acquisition_knowledge_objects (tenant_id, mission_id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS acquisition_knowledge_objects_tags_idx ON acquisition_knowledge_objects USING GIN (tags)`);
}

function rowFromDb(row = {}) {
  return {
    id: row.id,
    externalKey: row.external_key || null,
    tenantId: row.tenant_id,
    clientId: row.client_id,
    missionId: row.mission_id || null,
    scope: row.scope,
    objectType: row.object_type,
    title: row.title,
    content: row.content || {},
    epistemicState: row.epistemic_state || ak.normalizeEpistemicState(null, row),
    validationState: row.validation_state || ak.normalizeValidationState(null, row),
    validationStatus: row.validation_state || ak.normalizeValidationState(null, row),
    epistemicKind: row.epistemic_kind,
    state: row.lifecycle_state,
    status: row.status,
    channel: row.channel || null,
    experimentId: row.experiment_id || null,
    tags: row.tags || [],
    confidence: row.confidence == null ? null : Number(row.confidence),
    evidence: row.evidence || [],
    provenance: row.provenance || {},
    derivation: row.derivation || null,
    relationships: row.relationships || [],
    approvedBy: row.approved_by || null,
    createdFrom: row.created_from || null,
    createdBy: row.created_by || null,
    version: Number(row.version || 1),
    supersedesId: row.supersedes_id || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

async function insertRevision(client, row, operation, opts = {}) {
  await client.query(
    `INSERT INTO acquisition_knowledge_revisions (
      knowledge_id, tenant_id, version, operation, actor_id, actor_role, payload, evidence, rationale
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
    [
      row.id,
      row.tenantId,
      row.version,
      operation,
      opts.actorId || row.createdBy || null,
      opts.actorRole || null,
      row,
      row.evidence || [],
      opts.rationale || null,
    ]
  );
}

async function upsertKnowledgeObject(input = {}, pool = defaultPool(), opts = {}) {
  await ensureAcquisitionKnowledgeSchema(pool);
  const normalized = ak.normalizeKnowledgeObject(input, opts);
  const client = typeof pool.connect === 'function' ? await pool.connect() : pool;
  const ownsClient = client !== pool && typeof client.release === 'function';
  const inTransaction = opts.inTransaction === true;
  try {
    if (!inTransaction) await client.query('BEGIN');
    let existing = null;
    if (normalized.externalKey) {
      const found = await client.query(
        `SELECT * FROM acquisition_knowledge_objects WHERE tenant_id = $1 AND external_key = $2 FOR UPDATE`,
        [normalized.tenantId, normalized.externalKey]
      );
      existing = found.rows[0] ? rowFromDb(found.rows[0]) : null;
    }
    const row = existing
      ? { ...normalized, id: existing.id, version: existing.version + 1, createdAt: existing.createdAt }
      : normalized;
    const saved = await client.query(
      `INSERT INTO acquisition_knowledge_objects (
        id, external_key, tenant_id, client_id, mission_id, scope, object_type, title, content,
        epistemic_state, validation_state, epistemic_kind, lifecycle_state, status, channel, experiment_id, tags, confidence,
        evidence, provenance, derivation, relationships, approved_by, created_from, created_by, version, supersedes_id,
        created_at, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29
      )
      ON CONFLICT (id) DO UPDATE SET
        title = EXCLUDED.title,
        content = EXCLUDED.content,
        epistemic_state = EXCLUDED.epistemic_state,
        validation_state = EXCLUDED.validation_state,
        epistemic_kind = EXCLUDED.epistemic_kind,
        lifecycle_state = EXCLUDED.lifecycle_state,
        status = EXCLUDED.status,
        channel = EXCLUDED.channel,
        experiment_id = EXCLUDED.experiment_id,
        tags = EXCLUDED.tags,
        confidence = EXCLUDED.confidence,
        evidence = EXCLUDED.evidence,
        provenance = EXCLUDED.provenance,
        derivation = EXCLUDED.derivation,
        relationships = EXCLUDED.relationships,
        approved_by = EXCLUDED.approved_by,
        created_from = EXCLUDED.created_from,
        created_by = EXCLUDED.created_by,
        version = EXCLUDED.version,
        supersedes_id = EXCLUDED.supersedes_id,
        updated_at = EXCLUDED.updated_at
      RETURNING *`,
      [
        row.id,
        row.externalKey,
        row.tenantId,
        row.clientId,
        row.missionId,
        row.scope,
        row.objectType,
        row.title,
        row.content,
        row.epistemicState,
        row.validationState,
        row.epistemicKind,
        row.state,
        row.status,
        row.channel,
        row.experimentId,
        row.tags,
        row.confidence,
        JSON.stringify(row.evidence || []),
        row.provenance,
        row.derivation,
        JSON.stringify(row.relationships || []),
        row.approvedBy,
        row.createdFrom,
        row.createdBy,
        row.version,
        row.supersedesId,
        row.createdAt,
        ak.nowIso(),
      ]
    );
    const output = rowFromDb(saved.rows[0]);
    await insertRevision(client, output, existing ? 'update' : 'create', opts);
    if (!inTransaction) await client.query('COMMIT');
    return output;
  } catch (err) {
    if (!inTransaction) {
      try { await client.query('ROLLBACK'); } catch (_) {}
    }
    throw err;
  } finally {
    if (ownsClient) client.release();
  }
}

async function promoteKnowledgeObject(id, input = {}, pool = defaultPool(), opts = {}) {
  await ensureAcquisitionKnowledgeSchema(pool);
  const actorRole = ak.normalizeRole(opts.actorRole || input.actorRole);
  if (!ak.actorCanPromote(actorRole)) {
    throw ak.knowledgeError('ak_promotion_forbidden', 'Only an operator may promote acquisition knowledge.');
  }
  const tenantId = ak.assertTenant(input.tenantId || opts.tenantId);
  const evidence = ak.normalizeEvidence(input.evidence);
  const client = typeof pool.connect === 'function' ? await pool.connect() : pool;
  const ownsClient = client !== pool && typeof client.release === 'function';
  try {
    await client.query('BEGIN');
    const current = await client.query(
      `SELECT * FROM acquisition_knowledge_objects WHERE id = $1 AND tenant_id = $2 FOR UPDATE`,
      [id, tenantId]
    );
    if (!current.rows[0]) throw ak.knowledgeError('ak_not_found', `Acquisition knowledge not found: ${id}`);
    const existing = rowFromDb(current.rows[0]);
    const nextState = ak.normalizeLifecycleState(input.state || input.nextState);
    ak.assertTransition(existing.state, nextState, evidence);
    const mergedEvidence = [...(existing.evidence || []), ...evidence];
    const version = existing.version + 1;
    const nextEpistemic = ak.normalizeEpistemicKind(input.epistemicKind, nextState);
    const nextValidationState = ak.normalizeValidationState(input.validationState || nextState, input);
    ak.assertCanonicalSemantics({
      ...existing,
      validationState: nextValidationState,
      evidence: mergedEvidence,
    });
    const updated = await client.query(
      `UPDATE acquisition_knowledge_objects
       SET lifecycle_state = $1,
           validation_state = $2,
           epistemic_kind = $3,
           status = $4,
           evidence = $5,
           approved_by = $6,
           version = $7,
           updated_at = NOW()
       WHERE id = $8 AND tenant_id = $9
       RETURNING *`,
      [
        nextState,
        nextValidationState,
        nextEpistemic,
        nextState === ak.LIFECYCLE_STATES.CANONICAL ? 'canonical' : 'validated',
        JSON.stringify(mergedEvidence),
        input.approvedBy || opts.actorId || existing.approvedBy,
        version,
        id,
        tenantId,
      ]
    );
    const output = rowFromDb(updated.rows[0]);
    await insertRevision(client, output, 'promote', {
      ...opts,
      rationale: input.rationale || `Promoted ${existing.state} to ${nextState}.`,
    });
    await client.query('COMMIT');
    return output;
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    if (ownsClient) client.release();
  }
}

async function queryKnowledgeObjects(query = {}, pool = defaultPool()) {
  await ensureAcquisitionKnowledgeSchema(pool);
  const tenantId = ak.assertTenant(query.tenantId);
  const params = [tenantId];
  const where = ['tenant_id = $1'];
  if (query.objectType) {
    params.push(ak.normalizeObjectType(query.objectType));
    where.push(`object_type = $${params.length}`);
  }
  if (query.state) {
    params.push(ak.normalizeLifecycleState(query.state));
    where.push(`lifecycle_state = $${params.length}`);
  }
  if (query.epistemicState) {
    params.push(ak.normalizeEpistemicState(query.epistemicState));
    where.push(`epistemic_state = $${params.length}`);
  }
  if (query.validationState || query.validationStatus) {
    params.push(ak.normalizeValidationState(query.validationState || query.validationStatus));
    where.push(`validation_state = $${params.length}`);
  }
  if (query.scope) {
    params.push(ak.normalizeScope(query.scope));
    where.push(`scope = $${params.length}`);
  }
  if (query.missionId) {
    params.push(String(query.missionId));
    where.push(`(mission_id = $${params.length} OR mission_id IS NULL)`);
  }
  if (query.status) {
    params.push(String(query.status).toLowerCase());
    where.push(`status = $${params.length}`);
  }
  if (query.channel) {
    params.push(String(query.channel));
    where.push(`channel = $${params.length}`);
  }
  if (Array.isArray(query.tags) && query.tags.length) {
    params.push(query.tags.map(String));
    where.push(`tags @> $${params.length}::text[]`);
  }
  if (query.q) {
    params.push(`%${String(query.q).toLowerCase()}%`);
    where.push(`(
      LOWER(title) LIKE $${params.length}
      OR LOWER(object_type) LIKE $${params.length}
      OR LOWER(epistemic_kind) LIKE $${params.length}
      OR LOWER(epistemic_state) LIKE $${params.length}
      OR LOWER(validation_state) LIKE $${params.length}
      OR LOWER(content::text) LIKE $${params.length}
      OR LOWER(evidence::text) LIKE $${params.length}
    )`);
  }
  const limit = Math.min(100, Math.max(1, Number(query.limit || 25)));
  params.push(limit);
  const result = await pool.query(
    `SELECT * FROM acquisition_knowledge_objects
     WHERE ${where.join(' AND ')}
     ORDER BY
       CASE lifecycle_state
         WHEN 'CANONICAL' THEN 0
         WHEN 'MARKET_VALIDATED' THEN 1
         WHEN 'EXPERIMENTALLY_SUPPORTED' THEN 2
         WHEN 'STAKEHOLDER_VALIDATED' THEN 3
         ELSE 4
       END,
       updated_at DESC
     LIMIT $${params.length}`,
    params
  );
  return result.rows.map(rowFromDb);
}

async function recordRecommendationExplanation(input = {}, pool = defaultPool(), opts = {}) {
  await ensureAcquisitionKnowledgeSchema(pool);
  const tenantId = ak.assertTenant(input.tenantId || opts.tenantId);
  const knowledge = Array.isArray(input.knowledge)
    ? input.knowledge
    : await queryKnowledgeObjects({ ...input.query, tenantId, missionId: input.missionId, limit: input.limit || 10 }, pool);
  const explanation = ak.explainRecommendation({
    recommendation: input.recommendation,
    question: input.question,
    knowledge,
  });
  const id = input.id || ak.newId('ak_decision');
  await pool.query(
    `INSERT INTO acquisition_knowledge_decisions (
      id, tenant_id, mission_id, recommendation, question, knowledge_refs, explanation, actor_id
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (id) DO NOTHING`,
    [
      id,
      tenantId,
      input.missionId || null,
      explanation.recommendation,
      input.question || null,
      JSON.stringify(knowledge.map((row) => ({ id: row.id, version: row.version, state: row.state }))),
      explanation,
      opts.actorId || input.actorId || null,
    ]
  );
  return { id, ...explanation };
}

async function loadTenantKnowledge(tenantId, pool = defaultPool(), query = {}) {
  return queryKnowledgeObjects({ ...query, tenantId, limit: query.limit || 50 }, pool);
}

function extractLearningCandidateRows(bundle = {}) {
  const mission = bundle.mission || {};
  const tenantId = mission.tenantId || mission.clientId;
  if (!tenantId || !mission.id) return [];
  const outcomes = Array.isArray(bundle.outcomes) ? bundle.outcomes : [];
  const contributions = Array.isArray(bundle.contributions) ? bundle.contributions : [];
  const hasOutcomes = outcomes.length > 0;
  const hasCampaignSignal = outcomes.some((row) =>
    /reply|meeting|walkthrough|bounce|sent|open|queued/i.test(String(row.type || row.kind || ''))
  );
  if (!hasOutcomes && mission.stage !== 'learn') return [];

  const stats = outcomes.reduce((acc, row) => {
    const type = String(row.type || row.kind || 'unknown').toLowerCase();
    acc[type] = (acc[type] || 0) + 1;
    return acc;
  }, {});
  const latestContribution = contributions[contributions.length - 1] || null;
  const stableKeySource = `${tenantId}:${mission.id}:${JSON.stringify(stats)}:${hasCampaignSignal}`;
  const digest = require('crypto').createHash('sha1').update(stableKeySource).digest('hex').slice(0, 16);
  const title = hasCampaignSignal
    ? `Learning candidate for ${mission.title || mission.objective || mission.id}`
    : `Unresolved learning candidate for ${mission.title || mission.objective || mission.id}`;
  return [{
    id: `ak_candidate_${digest}`,
    externalKey: `campaign_learning:${mission.id}:${digest}`,
    tenantId,
    clientId: mission.clientId || (Number.isFinite(Number(tenantId)) ? Number(tenantId) : null),
    missionId: mission.id,
    scope: ak.SCOPES.MISSION,
    objectType: ak.OBJECT_TYPES.LEARNING,
    title,
    state: ak.LIFECYCLE_STATES.HYPOTHESIS,
    epistemicKind: ak.EPISTEMIC_KINDS.HYPOTHESIS,
    epistemicState: ak.EPISTEMIC_STATES.INFERRED,
    validationState: ak.VALIDATION_STATES.UNVALIDATED,
    status: 'learning_candidate',
    content: {
      statement: hasCampaignSignal
        ? 'Campaign outcomes created a reviewable acquisition learning candidate.'
        : 'Campaign ended without enough outcome signal; review the unknowns before promoting learning.',
      missionId: mission.id,
      campaign: mission.campaign || null,
      outcomeStats: stats,
      latestContributionId: latestContribution && latestContribution.id,
    },
    evidence: outcomes.map((row, index) => ({
      id: `outcome_${index + 1}`,
      type: ak.EVIDENCE_TYPES.CAMPAIGN_OUTCOME,
      statement: String(row.label || row.statement || row.type || row.kind || 'Campaign outcome'),
      source: { kind: 'acquisition_mission_outcome', ref: row.id || null, missionId: mission.id },
      confidence: 0.7,
      observedAt: row.at || row.createdAt || new Date().toISOString(),
      payload: row,
    })),
    derivation: {
      kind: 'campaign_learning_candidate',
      explanation: 'Campaign outcomes and specialist contributions imply a reviewable learning candidate.',
      basedOn: [mission.id, ...(contributions.map((row) => row.id).filter(Boolean))],
      evidenceRefs: outcomes.map((row, index) => row.id || `outcome_${index + 1}`),
    },
    tags: ['campaign_outcome', 'operator_review_required'],
    createdFrom: 'acquisition_mission_stage_commit',
    createdBy: 'max',
    provenance: { kind: 'campaign_outcome_review', spec: ak.SPEC },
  }];
}

async function persistLearningCandidatesForStageCommit(bundle = {}, client, opts = {}) {
  const candidates = extractLearningCandidateRows(bundle);
  if (!candidates.length) return [];
  if (opts.skipEnsure !== true) await ensureAcquisitionKnowledgeSchema(client);
  const saved = [];
  for (const candidate of candidates) {
    saved.push(await upsertKnowledgeObject(candidate, client, {
      actorId: opts.actorId || 'max',
      actorRole: opts.actorRole || 'max',
      rationale: 'Campaign outcomes must produce reviewable acquisition learning candidates.',
      inTransaction: true,
    }));
  }
  return saved;
}

module.exports = {
  ensureAcquisitionKnowledgeSchema,
  upsertKnowledgeObject,
  promoteKnowledgeObject,
  queryKnowledgeObjects,
  recordRecommendationExplanation,
  loadTenantKnowledge,
  extractLearningCandidateRows,
  persistLearningCandidatesForStageCommit,
};
