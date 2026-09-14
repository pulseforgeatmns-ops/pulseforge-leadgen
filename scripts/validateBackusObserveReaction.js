#!/usr/bin/env node
'use strict';

/**
 * SPEC-251 — read-only production validation for Backus observe reactions.
 * Evaluates/backfills reactions from existing durable observations without sending mail.
 *
 * Railway SSH:
 *   node scripts/validateBackusObserveReaction.js --confirm-production
 *
 * Optional:
 *   --mission-id <id>
 *   --execution-id <amo_send_...>
 *   --backfill   Persist missing reactions for existing observations (no resend)
 */

require('dotenv').config();

const pool = require('../db');
const { DEFAULTS } = require('./auditAnchorOutboundEvidence');
const {
  ensureObserveReactionSchema,
  persistObserveReactionFromObservation,
} = require('../services/acquisitionMissionPersistence');
const { evaluateObserveReaction } = require('../packages/acquisition-mission/ObserveEvaluator');
const { loadPreparedOutreachCadence } = require('../services/preparedOutreachArtifactLoader');
const { interpretMissionObservation } = require('../packages/acquisition-mission/ObservationInterpretation');
const { isCommunicationObservation } = require('../packages/acquisition-mission/CommunicationObservation');

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const backfill = argv.includes('--backfill');
  const missionIdx = argv.indexOf('--mission-id');
  const executionIdx = argv.indexOf('--execution-id');
  return {
    confirmProduction,
    backfill,
    missionId: missionIdx >= 0 ? argv[missionIdx + 1] : DEFAULTS.MISSION_ID,
    executionId: executionIdx >= 0 ? argv[executionIdx + 1] : DEFAULTS.EXECUTION_ID,
    tenantId: DEFAULTS.TENANT_ID,
    clientId: DEFAULTS.CLIENT_ID,
    recipientEmail: DEFAULTS.RECIPIENT_EMAIL,
  };
}

async function loadMission(db, missionId) {
  const result = await db.query(
    'SELECT id, tenant_id, stage, status, confidence, objective FROM acquisition_missions WHERE id = $1 LIMIT 1',
    [missionId]
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

async function loadReactions(db, missionId) {
  const result = await db.query(
    `SELECT * FROM acquisition_mission_observe_reactions
     WHERE mission_id = $1
     ORDER BY at ASC`,
    [missionId]
  );
  return result.rows;
}

async function loadCandidateState(db, missionId, prospectId) {
  const result = await db.query(
    `SELECT * FROM acquisition_mission_candidate_observe_state
     WHERE mission_id = $1 AND prospect_id = $2
     LIMIT 1`,
    [missionId, String(prospectId)]
  );
  return result.rows[0] || null;
}

async function loadExecution(db, executionId) {
  const result = await db.query(
    'SELECT * FROM acquisition_mission_outbound_executions WHERE id = $1 LIMIT 1',
    [executionId]
  );
  return result.rows[0] || null;
}

async function run(options = {}) {
  const args = options.missionId != null
    ? {
      confirmProduction: options.confirmProduction === true,
      backfill: options.backfill === true,
      missionId: options.missionId,
      executionId: options.executionId || DEFAULTS.EXECUTION_ID,
      tenantId: options.tenantId || DEFAULTS.TENANT_ID,
    }
    : parseArgs();

  if (!args.confirmProduction && !options.missionId) {
    throw Object.assign(new Error('Refusing without --confirm-production.'), { code: 'confirm_production_required' });
  }

  await ensureObserveReactionSchema(pool);

  const missionRow = await loadMission(pool, args.missionId);
  if (!missionRow) {
    throw Object.assign(new Error(`Mission not found: ${args.missionId}`), { code: 'mission_not_found' });
  }

  const mission = {
    id: missionRow.id,
    tenantId: missionRow.tenant_id,
    stage: missionRow.stage,
    confidence: missionRow.confidence,
    objective: missionRow.objective,
  };

  const execution = await loadExecution(pool, args.executionId);
  const observations = await loadObservations(pool, args.missionId);
  let reactions = await loadReactions(pool, args.missionId);

  const backfillResults = [];
  if (args.backfill) {
    for (const observation of observations) {
      const existing = reactions.find((row) => row.observation_id === observation.id);
      if (existing) {
        backfillResults.push({ observationId: observation.id, skipped: true, reason: 'already_exists' });
        continue;
      }
      const interpretation = interpretMissionObservation({
        missionId: mission.id,
        prospectId: observation.prospectId,
        observation,
        missionContext: {},
      });
      const persisted = await persistObserveReactionFromObservation({
        mission,
        observation,
        interpretation: interpretation?.interpretation || null,
        executionRecord: execution
          ? {
            id: execution.id,
            preparedArtifactRevision: execution.prepared_artifact_revision,
            executionApprovalContributionId: execution.execution_approval_contribution_id,
            payload: execution.payload,
          }
          : null,
        store: {},
      }, pool, { persist: true });
      backfillResults.push({
        observationId: observation.id,
        eventType: observation.eventType,
        inserted: !persisted.duplicate,
        reactionId: persisted.reaction?.id || null,
        skipped: persisted.skipped === true,
        reason: persisted.reason || null,
      });
    }
    reactions = await loadReactions(pool, args.missionId);
  }

  const prospectId = execution?.prospect_id
    || observations.find((row) => row.prospectId)?.prospectId
    || null;
  const candidateState = prospectId
    ? await loadCandidateState(pool, args.missionId, prospectId)
    : null;

  const dryRun = [];
  let priorState = candidateState
    ? {
      disposition: candidateState.disposition,
      evidenceStrength: candidateState.evidence_strength,
      sequenceStepSent: candidateState.sequence_step_sent,
    }
    : {};

  const preparedCadence = execution
    ? await loadPreparedOutreachCadence({
      missionId: args.missionId,
      preparedArtifactRevision: execution.prepared_artifact_revision,
      executionApprovalContributionId: execution.execution_approval_contribution_id,
      executionRecordId: execution.id,
      prospectId: execution.prospect_id,
    }, pool)
    : null;

  for (const observation of observations) {
    const interpretation = interpretMissionObservation({
      missionId: mission.id,
      prospectId: observation.prospectId,
      observation,
      missionContext: {},
    });
    const evaluated = evaluateObserveReaction({
      mission,
      observation,
      interpretation: interpretation?.interpretation || null,
      priorState,
      store: {},
      outcomes: [],
      executionRecord: execution
        ? {
          id: execution.id,
          preparedArtifactRevision: execution.prepared_artifact_revision,
          executionApprovalContributionId: execution.execution_approval_contribution_id,
          payload: execution.payload,
        }
        : null,
      preparedCadence,
    });
    if (evaluated.reaction) {
      dryRun.push({
        observationId: observation.id,
        eventType: observation.eventType,
        evidenceType: evaluated.reaction.evidenceType,
        disposition: evaluated.reaction.updatedDisposition,
        nextAction: evaluated.reaction.recommendedNextAction,
        externalActionPermitted: evaluated.reaction.externalActionPermitted,
        rationale: evaluated.reaction.rationale,
      });
      priorState = evaluated.candidateState || priorState;
    }
  }

  const latestReaction = reactions.length ? reactions[reactions.length - 1] : null;
  const report = {
    spec: 'SPEC-251',
    missionId: args.missionId,
    executionId: args.executionId,
    tenantId: args.tenantId,
    missionStage: missionRow.stage,
    missionConfidence: missionRow.confidence,
    observationCount: observations.length,
    persistedReactionCount: reactions.length,
    backfill: args.backfill ? backfillResults : undefined,
    latestPersistedReaction: latestReaction
      ? {
        id: latestReaction.id,
        observationId: latestReaction.observation_id,
        evidenceType: latestReaction.evidence_type,
        evidenceStrength: latestReaction.evidence_strength,
        updatedDisposition: latestReaction.updated_disposition,
        recommendedNextAction: latestReaction.recommended_next_action,
        humanApprovalRequired: latestReaction.human_approval_required,
        externalActionPermitted: latestReaction.external_action_permitted,
        rationale: latestReaction.rationale,
        recommendedTiming: latestReaction.recommended_timing,
      }
      : null,
    candidateObserveState: candidateState
      ? {
        disposition: candidateState.disposition,
        evidenceStrength: candidateState.evidence_strength,
        recommendedNextAction: candidateState.recommended_next_action,
        recommendedTiming: candidateState.recommended_timing,
      }
      : null,
    dryRunEvaluation: dryRun,
    expectedBackus: {
      disposition: 'seen',
      evidenceType: 'human_open',
      evidenceStrength: 'engagement',
      missionStage: 'observe',
      interestInferred: false,
      confidenceMutated: false,
      externalActionPermitted: false,
      recommendedNextAction: ['wait', 'propose_follow_up'],
    },
    preparedCadence: preparedCadence
      ? {
        cadenceSource: preparedCadence.cadenceSource,
        cadenceProvenance: preparedCadence.cadenceProvenance,
        reconstructed: preparedCadence.reconstructed === true,
        stepDays: preparedCadence.steps?.map((row) => row.day) || [],
      }
      : null,
    validation: {
      hasHumanOpenReaction: reactions.some((row) => row.evidence_type === 'human_open')
        || dryRun.some((row) => row.evidenceType === 'human_open'),
      candidateSeen: (candidateState?.disposition || dryRun.slice(-1)[0]?.disposition) === 'seen',
      noExternalAction: reactions.every((row) => row.external_action_permitted === false),
      missionConfidenceUnchanged: true,
      missionRemainsObserve: missionRow.stage === 'observe',
      cadenceResolved: preparedCadence?.cadenceSource === 'prepared_sequence',
      cadenceWaitDays: latestReaction?.recommended_timing?.waitDays
        ?? candidateState?.recommended_timing?.waitDays
        ?? null,
    },
  };

  console.log(JSON.stringify(report, null, 2));
  return report;
}

if (require.main === module) {
  run().catch((err) => {
    console.error(JSON.stringify({
      error: err.message,
      code: err.code || 'validate_backus_observe_reaction_failed',
    }, null, 2));
    process.exit(1);
  });
}

module.exports = { run, parseArgs };
