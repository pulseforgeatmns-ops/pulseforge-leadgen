'use strict';

/**
 * Postgres persistence for COG benchmark results.
 */

const COG_RESULTS_DDL = `
CREATE TABLE IF NOT EXISTS cog_runs (
  id UUID PRIMARY KEY,
  suite_id TEXT NOT NULL,
  suite_version TEXT NOT NULL,
  cog_version TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ,
  overall_score NUMERIC(4,1),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cog_domain_results (
  id BIGSERIAL PRIMARY KEY,
  run_id UUID NOT NULL REFERENCES cog_runs(id) ON DELETE CASCADE,
  domain_id TEXT NOT NULL,
  status TEXT NOT NULL,
  conversation_id TEXT,
  score NUMERIC(4,1),
  review_status TEXT NOT NULL DEFAULT 'pending',
  duration_ms INTEGER NOT NULL DEFAULT 0,
  transcript JSONB NOT NULL DEFAULT '[]'::jsonb,
  failures JSONB NOT NULL DEFAULT '[]'::jsonb,
  behavior_results JSONB NOT NULL DEFAULT '[]'::jsonb,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cog_runs_suite_started ON cog_runs (suite_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_cog_domain_results_run ON cog_domain_results (run_id);
CREATE INDEX IF NOT EXISTS idx_cog_domain_results_domain ON cog_domain_results (domain_id, created_at DESC);
`;

async function ensureCogSchema(pool) {
  await pool.query(COG_RESULTS_DDL);
}

/**
 * Persist a COG run to Postgres.
 * @param {import('pg').Pool} pool
 * @param {import('../packages/cog/types').CogRunResult} run
 */
async function saveCogRunToPostgres(pool, run) {
  await ensureCogSchema(pool);

  await pool.query(
    `INSERT INTO cog_runs (id, suite_id, suite_version, cog_version, status, started_at, completed_at, overall_score, metadata)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
     ON CONFLICT (id) DO UPDATE SET
       status = EXCLUDED.status,
       completed_at = EXCLUDED.completed_at,
       overall_score = EXCLUDED.overall_score,
       metadata = EXCLUDED.metadata`,
    [
      run.runId,
      run.suiteId,
      run.suiteVersion,
      run.cogVersion,
      run.status,
      run.startedAt,
      run.completedAt || null,
      run.overallScore,
      JSON.stringify(run.metadata || {}),
    ]
  );

  for (const domain of run.domains) {
    await pool.query(
      `INSERT INTO cog_domain_results
         (run_id, domain_id, status, conversation_id, score, review_status, duration_ms, transcript, failures, behavior_results, error, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
      [
        run.runId,
        domain.domainId,
        domain.status,
        domain.conversationId,
        domain.score,
        domain.reviewStatus,
        domain.durationMs,
        JSON.stringify(domain.transcript || []),
        JSON.stringify(domain.failures || []),
        JSON.stringify(domain.behaviorResults || []),
        domain.error || null,
        JSON.stringify(domain.metadata || {}),
      ]
    );
  }

  return { runId: run.runId, persisted: true };
}

/**
 * Load recent COG runs from Postgres for trend reporting.
 */
async function loadCogRunsFromPostgres(pool, options = {}) {
  await ensureCogSchema(pool);
  const limit = options.limit || 20;
  const suiteId = options.suiteId || null;

  const params = [limit];
  let where = '';
  if (suiteId) {
    where = 'WHERE suite_id = $2';
    params.push(suiteId);
  }

  const runsResult = await pool.query(
    `SELECT * FROM cog_runs ${where} ORDER BY started_at DESC LIMIT $1`,
    params
  );

  const runs = [];
  for (const row of runsResult.rows) {
    const domainsResult = await pool.query(
      'SELECT * FROM cog_domain_results WHERE run_id = $1 ORDER BY domain_id',
      [row.id]
    );
    runs.push({
      runId: row.id,
      suiteId: row.suite_id,
      suiteVersion: row.suite_version,
      cogVersion: row.cog_version,
      status: row.status,
      startedAt: row.started_at.toISOString(),
      completedAt: row.completed_at?.toISOString() || null,
      overallScore: row.overall_score !== null ? Number(row.overall_score) : null,
      metadata: row.metadata,
      domains: domainsResult.rows.map(d => ({
        domainId: d.domain_id,
        status: d.status,
        conversationId: d.conversation_id,
        score: d.score !== null ? Number(d.score) : null,
        reviewStatus: d.review_status,
        durationMs: d.duration_ms,
        transcript: d.transcript,
        failures: d.failures,
        behaviorResults: d.behavior_results,
        error: d.error,
        metadata: d.metadata,
      })),
    });
  }

  return runs;
}

module.exports = {
  COG_RESULTS_DDL,
  ensureCogSchema,
  saveCogRunToPostgres,
  loadCogRunsFromPostgres,
};
