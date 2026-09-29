'use strict';

/**
 * SPEC-252 — Durable prepared outreach cadence loader (webhook-safe, no in-memory store).
 */

const { asText } = require('../packages/acquisition-mission/types');
const {
  extractOutreachSequenceSteps,
  CADENCE_PROVENANCE,
  SOURCE_KINDS,
} = require('../packages/acquisition-mission/PreparedOutreachSequence');
const { unwrapSpecialistPayload } = require('../packages/acquisition-mission/ContributionSupersession');
const {
  findPreparedCadenceAnnotation,
} = require('./preparedCadenceAnnotationPersistence');

function defaultPool() {
  return require('../db');
}

function contributionPayloadBody(row) {
  if (!row) return {};
  const payload = row.payload;
  if (payload && typeof payload === 'object') return payload;
  return row;
}

function isExecutionApprovalRow(row) {
  const payload = contributionPayloadBody(row);
  if (row.specialist !== 'operator' || row.kind !== 'approval') return false;
  if (payload.invalidated === true || payload.superseded === true) return false;
  return (
    payload.decisionKind === 'execution_approval'
    || payload.kind === 'execution_approval'
    || payload.action === 'execution_approved'
  );
}

async function loadContributionById(pool, contributionId) {
  if (!contributionId) return null;
  const result = await pool.query(
    `SELECT id, mission_id, tenant_id, specialist, kind, payload, at
     FROM acquisition_mission_contributions
     WHERE id = $1
     LIMIT 1`,
    [String(contributionId)]
  );
  if (!result.rows[0]) return null;
  const row = result.rows[0];
  const payload = row.payload && typeof row.payload === 'object' ? row.payload : {};
  return {
    id: row.id,
    missionId: row.mission_id,
    tenantId: row.tenant_id,
    specialist: row.specialist,
    kind: row.kind,
    payload,
    at: row.at,
  };
}

async function findExecutionApprovalByRevision(pool, missionId, preparedArtifactRevision) {
  if (!missionId || !preparedArtifactRevision) return null;
  const result = await pool.query(
    `SELECT id, mission_id, tenant_id, specialist, kind, payload, at
     FROM acquisition_mission_contributions
     WHERE mission_id = $1
       AND specialist = 'operator'
       AND kind = 'approval'
     ORDER BY at DESC`,
    [String(missionId)]
  );
  for (const row of result.rows) {
    const contribution = {
      id: row.id,
      missionId: row.mission_id,
      tenantId: row.tenant_id,
      specialist: row.specialist,
      kind: row.kind,
      payload: row.payload && typeof row.payload === 'object' ? row.payload : {},
      at: row.at,
    };
    if (!isExecutionApprovalRow(contribution)) continue;
    if (asText(contribution.payload.preparedArtifactRevision) === asText(preparedArtifactRevision)) {
      return contribution;
    }
  }
  return null;
}

function buildLoadedCadence(steps, provenance, extra = {}) {
  if (!steps.length) {
    return {
      steps: [],
      cadenceSource: 'unresolved',
      cadenceProvenance: null,
      outreachSequence: null,
    };
  }
  return {
    steps,
    cadenceSource: 'prepared_sequence',
    cadenceProvenance: provenance,
    outreachSequence: extra.outreachSequence || { steps },
    source: extra.source || null,
    reconstructed: provenance === CADENCE_PROVENANCE.HISTORICAL_ANNOTATION,
  };
}

/**
 * Loader precedence:
 * 1. Approval snapshot cadence (present at approval time)
 * 2. Historical cadence annotation for execution/revision
 * 3. Paige contribution cadence
 * 4. unresolved
 */
async function loadPreparedOutreachCadence(criteria = {}, pool = defaultPool()) {
  const {
    missionId,
    preparedArtifactRevision,
    executionApprovalContributionId,
    executionRecordId,
    prospectId,
  } = criteria;

  let approval = null;
  if (executionApprovalContributionId) {
    approval = await loadContributionById(pool, executionApprovalContributionId);
    if (approval && !isExecutionApprovalRow(approval)) approval = null;
  }
  if (!approval && missionId && preparedArtifactRevision) {
    approval = await findExecutionApprovalByRevision(pool, missionId, preparedArtifactRevision);
  }

  if (approval) {
    const approvalSteps = extractOutreachSequenceSteps(approval.payload);
    if (approvalSteps.length) {
      return buildLoadedCadence(approvalSteps, CADENCE_PROVENANCE.APPROVAL_SNAPSHOT, {
        outreachSequence: approval.payload.outreachSequence,
        source: approval.payload.outreachSequence?.source || null,
      });
    }
  }

  const annotation = await findPreparedCadenceAnnotation(pool, {
    executionRecordId,
    missionId,
    preparedArtifactRevision,
    prospectId,
  });
  if (annotation) {
    const annotationSteps = extractOutreachSequenceSteps({ outreachSequence: annotation.outreachSequence });
    if (annotationSteps.length) {
      return buildLoadedCadence(annotationSteps, CADENCE_PROVENANCE.HISTORICAL_ANNOTATION, {
        outreachSequence: annotation.outreachSequence,
        source: {
          ...(annotation.source || {}),
          kind: SOURCE_KINDS.HISTORICAL_BACKFILL,
          backfilledAt: annotation.backfilledAt,
        },
      });
    }
  }

  const paigeContributionId = approval?.payload?.paigeContributionId || null;
  if (paigeContributionId) {
    const paige = await loadContributionById(pool, paigeContributionId);
    const paigePayload = paige ? unwrapSpecialistPayload(paige) : {};
    const paigeSteps = extractOutreachSequenceSteps(paigePayload);
    if (paigeSteps.length) {
      return buildLoadedCadence(paigeSteps, CADENCE_PROVENANCE.PAIGE_CONTRIBUTION, {
        outreachSequence: paigePayload.outreachSequence,
        source: paigePayload.outreachSequence?.source || null,
      });
    }
  }

  return buildLoadedCadence([], null);
}

module.exports = {
  loadPreparedOutreachCadence,
  loadContributionById,
  findExecutionApprovalByRevision,
  isExecutionApprovalRow,
};
