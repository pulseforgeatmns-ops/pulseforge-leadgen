'use strict';

/**
 * SPEC-252 — Additive historical prepared-artifact cadence annotations.
 * Never mutates execution approvals or outbound execution records.
 */

const crypto = require('crypto');
const {
  extractOutreachSequenceSteps,
} = require('../packages/acquisition-mission/PreparedOutreachSequence');

function defaultPool() {
  return require('../db');
}

async function ensurePreparedCadenceAnnotationSchema(pool = defaultPool()) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_prepared_cadence_annotations (
      id TEXT PRIMARY KEY,
      mission_id TEXT NOT NULL,
      tenant_id TEXT NOT NULL,
      execution_record_id TEXT,
      execution_approval_contribution_id TEXT,
      prepared_artifact_revision TEXT NOT NULL,
      prospect_id TEXT,
      outreach_sequence JSONB NOT NULL,
      source JSONB NOT NULL,
      backfilled_at TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS acquisition_mission_prepared_cadence_ann_exec_uidx
      ON acquisition_mission_prepared_cadence_annotations (execution_record_id)
      WHERE execution_record_id IS NOT NULL
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS acquisition_mission_prepared_cadence_ann_mission_idx
      ON acquisition_mission_prepared_cadence_annotations (mission_id, prepared_artifact_revision)
  `);
}

function annotationFromRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    missionId: row.mission_id,
    tenantId: row.tenant_id,
    executionRecordId: row.execution_record_id,
    executionApprovalContributionId: row.execution_approval_contribution_id,
    preparedArtifactRevision: row.prepared_artifact_revision,
    prospectId: row.prospect_id,
    outreachSequence: row.outreach_sequence,
    source: row.source,
    backfilledAt: row.backfilled_at,
    createdAt: row.created_at,
  };
}

async function findPreparedCadenceAnnotation(pool = defaultPool(), criteria = {}) {
  await ensurePreparedCadenceAnnotationSchema(pool);

  if (criteria.executionRecordId) {
    const result = await pool.query(
      `SELECT * FROM acquisition_mission_prepared_cadence_annotations
       WHERE execution_record_id = $1
       LIMIT 1`,
      [String(criteria.executionRecordId)]
    );
    return annotationFromRow(result.rows[0]);
  }

  if (criteria.missionId && criteria.preparedArtifactRevision) {
    const params = [String(criteria.missionId), String(criteria.preparedArtifactRevision)];
    let sql = `
      SELECT * FROM acquisition_mission_prepared_cadence_annotations
      WHERE mission_id = $1 AND prepared_artifact_revision = $2`;
    if (criteria.prospectId) {
      sql += ' AND prospect_id = $3';
      params.push(String(criteria.prospectId));
    }
    sql += ' ORDER BY backfilled_at DESC LIMIT 1';
    const result = await pool.query(sql, params);
    return annotationFromRow(result.rows[0]);
  }

  return null;
}

async function persistPreparedCadenceAnnotation(annotation, pool = defaultPool(), opts = {}) {
  if (!annotation?.id || !annotation.missionId) return null;
  if (opts.skipEnsure !== true) await ensurePreparedCadenceAnnotationSchema(pool);

  const steps = extractOutreachSequenceSteps({ outreachSequence: annotation.outreachSequence });
  if (!steps.length) {
    throw Object.assign(new Error('Annotation outreachSequence must include at least one step.'), {
      code: 'prepared_cadence_annotation_invalid',
    });
  }

  await pool.query(
    `INSERT INTO acquisition_mission_prepared_cadence_annotations (
       id, mission_id, tenant_id, execution_record_id,
       execution_approval_contribution_id, prepared_artifact_revision,
       prospect_id, outreach_sequence, source, backfilled_at, created_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
     ON CONFLICT (id) DO UPDATE SET
       outreach_sequence = EXCLUDED.outreach_sequence,
       source = EXCLUDED.source,
       backfilled_at = EXCLUDED.backfilled_at`,
    [
      annotation.id,
      annotation.missionId,
      annotation.tenantId != null ? String(annotation.tenantId) : null,
      annotation.executionRecordId || null,
      annotation.executionApprovalContributionId || null,
      annotation.preparedArtifactRevision,
      annotation.prospectId || null,
      JSON.stringify(annotation.outreachSequence || {}),
      JSON.stringify(annotation.source || {}),
      annotation.backfilledAt || new Date().toISOString(),
      annotation.createdAt || new Date().toISOString(),
    ]
  );

  return annotation;
}

function buildAnnotationId(executionRecordId) {
  const seed = String(executionRecordId || crypto.randomUUID());
  return `cadence_ann_${crypto.createHash('sha256').update(seed).digest('hex').slice(0, 16)}`;
}

module.exports = {
  ensurePreparedCadenceAnnotationSchema,
  findPreparedCadenceAnnotation,
  persistPreparedCadenceAnnotation,
  annotationFromRow,
  buildAnnotationId,
};
