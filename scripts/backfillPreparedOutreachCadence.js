#!/usr/bin/env node
'use strict';

/**
 * SPEC-252 — Additive historical cadence annotation backfill.
 * Never mutates execution approvals or outbound execution records.
 *
 * Usage:
 *   node scripts/backfillPreparedOutreachCadence.js --confirm-production \
 *     --mission-id <id> --execution-id <amo_send_...> [--dry-run] [--reEvaluateReactions]
 */

require('dotenv').config();

const pool = require('../db');
const { DEFAULTS } = require('./auditAnchorOutboundEvidence');
const {
  resolveOutreachSequenceAtPrepare,
  buildHistoricalCadenceAnnotation,
} = require('../packages/acquisition-mission/PreparedOutreachSequence');
const {
  ensurePreparedCadenceAnnotationSchema,
  findPreparedCadenceAnnotation,
  persistPreparedCadenceAnnotation,
  buildAnnotationId,
} = require('../services/preparedCadenceAnnotationPersistence');
const {
  ensureObserveReactionSchema,
  persistObserveReactionFromObservation,
} = require('../services/acquisitionMissionPersistence');
const { evaluateObserveReaction } = require('../packages/acquisition-mission/ObserveEvaluator');
const { interpretMissionObservation } = require('../packages/acquisition-mission/ObservationInterpretation');
const { isCommunicationObservation } = require('../packages/acquisition-mission/CommunicationObservation');
const { loadPreparedOutreachCadence } = require('../services/preparedOutreachArtifactLoader');
const {
  syncObserveReactionOperationalFollowUp,
  BACKUS_BUSINESS_NAME,
} = require('../services/observeReactionOperationalSync');

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const dryRun = argv.includes('--dry-run');
  const reEvaluateReactions = argv.includes('--reEvaluateReactions');
  const missionIdx = argv.indexOf('--mission-id');
  const executionIdx = argv.indexOf('--execution-id');
  return {
    confirmProduction,
    dryRun,
    reEvaluateReactions,
    missionId: missionIdx >= 0 ? argv[missionIdx + 1] : DEFAULTS.MISSION_ID,
    executionId: executionIdx >= 0 ? argv[executionIdx + 1] : DEFAULTS.EXECUTION_ID,
    tenantId: DEFAULTS.TENANT_ID,
    clientId: DEFAULTS.CLIENT_ID,
  };
}

async function loadExecution(db, executionId) {
  const result = await db.query(
    'SELECT * FROM acquisition_mission_outbound_executions WHERE id = $1 LIMIT 1',
    [executionId]
  );
  return result.rows[0] || null;
}

async function loadMission(db, missionId) {
  const result = await db.query(
    'SELECT * FROM acquisition_missions WHERE id = $1 LIMIT 1',
    [missionId]
  );
  return result.rows[0] || null;
}

async function loadProspect(db, prospectId) {
  const result = await db.query(
    'SELECT id, vertical, client_id FROM prospects WHERE id = $1 LIMIT 1',
    [prospectId]
  );
  return result.rows[0] || null;
}

async function loadObservations(db, missionId) {
  const result = await db.query(
    `SELECT id, mission_id, tenant_id, specialist, observation, payload, at
     FROM acquisition_mission_observations
     WHERE mission_id = $1
     ORDER BY at ASC`,
    [missionId]
  );
  return result.rows.map((row) => {
    const payload = row.payload && typeof row.payload === 'object' ? row.payload : {};
    return {
      ...payload,
      id: row.id,
      missionId: row.mission_id,
      tenantId: row.tenant_id,
      specialist: row.specialist,
      observation: row.observation,
      at: row.at,
    };
  }).filter(isCommunicationObservation);
}

async function run(options = {}) {
  const args = options.missionId != null
    ? {
      confirmProduction: options.confirmProduction === true,
      dryRun: options.dryRun === true,
      reEvaluateReactions: options.reEvaluateReactions === true,
      missionId: options.missionId,
      executionId: options.executionId || DEFAULTS.EXECUTION_ID,
      tenantId: options.tenantId || DEFAULTS.TENANT_ID,
      clientId: options.clientId || DEFAULTS.CLIENT_ID,
    }
    : parseArgs();

  if (!args.confirmProduction && !options.missionId) {
    throw Object.assign(new Error('Refusing without --confirm-production.'), { code: 'confirm_production_required' });
  }

  await ensurePreparedCadenceAnnotationSchema(pool);

  const execution = await loadExecution(pool, args.executionId);
  if (!execution) {
    throw Object.assign(new Error(`Execution not found: ${args.executionId}`), { code: 'execution_not_found' });
  }

  const missionRow = await loadMission(pool, args.missionId);
  if (!missionRow) {
    throw Object.assign(new Error(`Mission not found: ${args.missionId}`), { code: 'mission_not_found' });
  }

  const existing = await findPreparedCadenceAnnotation(pool, {
    executionRecordId: execution.id,
  });

  const mission = {
    id: missionRow.id,
    tenantId: missionRow.tenant_id,
    clientId: missionRow.client_id || args.clientId,
    targetSegment: missionRow.target_segment,
    stage: missionRow.stage,
  };

  const prospect = execution.prospect_id
    ? await loadProspect(pool, execution.prospect_id)
    : null;

  const outreachSequence = resolveOutreachSequenceAtPrepare({
    mission,
    contributions: [],
    clientId: mission.clientId,
    crmByProspectId: prospect?.vertical
      ? { [execution.prospect_id]: { vertical: prospect.vertical } }
      : null,
  });

  if (!outreachSequence) {
    throw Object.assign(new Error('Could not resolve outreach sequence for historical annotation.'), {
      code: 'outreach_sequence_unresolved',
    });
  }

  const annotation = buildHistoricalCadenceAnnotation({
    missionId: mission.id,
    tenantId: mission.tenantId,
    executionRecordId: execution.id,
    executionApprovalContributionId: execution.execution_approval_contribution_id,
    preparedArtifactRevision: execution.prepared_artifact_revision,
    prospectId: execution.prospect_id,
    outreachSequence,
    templateKey: outreachSequence.source?.templateKey || outreachSequence.id || null,
    reason: 'Cadence reconstructed post-execution from client template catalog for historical OBSERVE resolution.',
  });
  annotation.id = existing?.id || buildAnnotationId(execution.id);

  const report = {
    spec: 'SPEC-252',
    dryRun: args.dryRun,
    missionId: args.missionId,
    executionId: args.executionId,
    preparedArtifactRevision: execution.prepared_artifact_revision,
    executionApprovalContributionId: execution.execution_approval_contribution_id,
    existingAnnotationId: existing?.id || null,
    annotation,
    immutability: {
      approvalMutated: false,
      executionRecordMutated: false,
      revisionRecomputed: false,
    },
  };

  if (!args.dryRun) {
    await persistPreparedCadenceAnnotation(annotation, pool);
    report.annotationPersisted = true;
  }

  if (args.reEvaluateReactions && !args.dryRun) {
    await ensureObserveReactionSchema(pool);
    const observations = await loadObservations(pool, args.missionId);
    const reEvaluated = [];
    let priorState = {};

    for (const observation of observations) {
      const interpretation = interpretMissionObservation({
        missionId: mission.id,
        prospectId: observation.prospectId,
        observation,
        missionContext: {},
      });
      const preparedCadence = await loadPreparedOutreachCadence({
        missionId: mission.id,
        preparedArtifactRevision: execution.prepared_artifact_revision,
        executionApprovalContributionId: execution.execution_approval_contribution_id,
        executionRecordId: execution.id,
        prospectId: observation.prospectId,
      }, pool);

      const evaluated = evaluateObserveReaction({
        mission,
        observation,
        interpretation: interpretation?.interpretation || null,
        priorState,
        store: {},
        outcomes: [],
        executionRecord: {
          id: execution.id,
          preparedArtifactRevision: execution.prepared_artifact_revision,
          executionApprovalContributionId: execution.execution_approval_contribution_id,
          payload: execution.payload,
        },
        preparedCadence,
        now: new Date(observation.at || observation.occurredAt),
      });

      if (evaluated.reaction) {
        const persisted = await persistObserveReactionFromObservation({
          mission,
          observation,
          interpretation: interpretation?.interpretation || null,
          priorState,
          executionRecord: {
            id: execution.id,
            preparedArtifactRevision: execution.prepared_artifact_revision,
            executionApprovalContributionId: execution.execution_approval_contribution_id,
            payload: execution.payload,
          },
          preparedCadence,
        }, pool, {
          persist: true,
          now: observation.at || observation.occurredAt,
          reevaluate: true,
          reevaluationTriggerId: annotation.id,
          reevaluationTriggerKind: 'historical_cadence_annotation',
        });
        reEvaluated.push({
          observationId: observation.id,
          eventType: observation.eventType,
          recommendedTiming: evaluated.reaction.recommendedTiming,
          rationale: evaluated.reaction.rationale,
          reactionId: persisted.reaction?.id || null,
          reevaluated: persisted.reevaluated === true,
          duplicate: persisted.duplicate === true,
          evaluationKind: persisted.reaction?.evaluationKind || null,
        });
        priorState = persisted.candidateState || evaluated.candidateState || priorState;
      }
    }
    report.reEvaluated = reEvaluated;

    report.operationalFollowUp = await syncObserveReactionOperationalFollowUp({
      mission,
      execution,
      preparedCadence: await loadPreparedOutreachCadence({
        missionId: mission.id,
        preparedArtifactRevision: execution.prepared_artifact_revision,
        executionApprovalContributionId: execution.execution_approval_contribution_id,
        executionRecordId: execution.id,
        prospectId: execution.prospect_id,
      }, pool),
      annotation,
      clientId: args.clientId,
      businessName: BACKUS_BUSINESS_NAME,
    }, pool);
  }

  console.log(JSON.stringify(report, null, 2));
  return report;
}

if (require.main === module) {
  run().catch((err) => {
    console.error(JSON.stringify({
      error: err.message,
      code: err.code || 'backfill_prepared_outreach_cadence_failed',
    }, null, 2));
    process.exit(1);
  });
}

module.exports = { run, parseArgs };
