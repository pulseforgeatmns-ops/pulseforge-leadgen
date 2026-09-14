'use strict';

/**
 * SPEC-252 operational bridge — sync effective observe reaction timing into AO + CRM notes.
 * Sync-only: never evaluates reactions or reruns outbound.
 */

const { listEffectiveObserveReactions } = require('../packages/acquisition-mission/ObserveReaction');
const { extractOutreachSequenceSteps } = require('../packages/acquisition-mission/PreparedOutreachSequence');
const {
  ensureObserveReactionSchema,
  loadEffectiveObserveReactions,
} = require('./acquisitionMissionPersistence');
const { findPreparedCadenceAnnotation } = require('./preparedCadenceAnnotationPersistence');
const { loadPreparedOutreachCadence } = require('./preparedOutreachArtifactLoader');
const { ensureLifecycleSchema } = require('../utils/lifecycleSchema');
const { normalizeDueDate } = require('../utils/aoQueueFormat');

const OPERATIONAL_TASK_NEXT_ACTION = 'spec252_warm_email_open';

function buildOperationalSyncNoteSource(annotationId) {
  return `spec252_observe_reeval:${String(annotationId)}`;
}

function buildOperationalSyncMetadata(input = {}) {
  const {
    effectiveReaction = null,
    candidateState = null,
    preparedCadence = null,
    annotation = null,
    missionId = null,
    executionId = null,
    prospectId = null,
  } = input;

  const timing = effectiveReaction?.recommendedTiming || candidateState?.recommendedTiming || {};
  const sequenceStepDays = preparedCadence?.steps?.map((row) => row.day)
    ?? extractOutreachSequenceSteps(annotation?.outreachSequence)?.map((row) => row.day)
    ?? null;

  return {
    spec: 'SPEC-252',
    missionId,
    executionId,
    prospectId,
    reactionId: effectiveReaction?.id || null,
    observationId: effectiveReaction?.observationId || null,
    evaluationKind: effectiveReaction?.evaluationKind || null,
    reevaluationTriggerId: effectiveReaction?.reevaluationTriggerId || annotation?.id || null,
    recommendedTiming: {
      kind: timing.kind ?? null,
      waitDays: timing.waitDays ?? null,
      dueAt: timing.dueAt ?? null,
      cadenceSource: timing.cadenceSource ?? null,
      cadenceProvenance: timing.cadenceProvenance ?? null,
      reconstructed: timing.reconstructed ?? null,
      clockStart: timing.clockStart ?? null,
      businessDays: timing.businessDays ?? false,
    },
    candidate: {
      disposition: candidateState?.disposition || effectiveReaction?.updatedDisposition || null,
      evidenceStrength: candidateState?.evidenceStrength || effectiveReaction?.evidenceStrength || null,
      recommendedNextAction: candidateState?.recommendedNextAction
        || effectiveReaction?.recommendedNextAction
        || null,
    },
    sequenceStepDays,
    cadenceSource: preparedCadence?.cadenceSource || timing.cadenceSource || null,
    cadenceProvenance: preparedCadence?.cadenceProvenance || timing.cadenceProvenance || null,
    reconstructed: preparedCadence?.reconstructed ?? timing.reconstructed ?? null,
  };
}

function formatOperationalSyncNoteText(metadata) {
  return JSON.stringify(metadata, null, 2);
}

function pickEffectiveHumanOpenReaction(reactions = []) {
  const effective = listEffectiveObserveReactions(reactions);
  return [...effective].reverse().find((row) => row.evidenceType === 'human_open') || null;
}

async function loadExecution(db, executionId) {
  const result = await db.query(
    'SELECT * FROM acquisition_mission_outbound_executions WHERE id = $1 LIMIT 1',
    [executionId]
  );
  return result.rows[0] || null;
}

async function loadCandidateObserveState(db, missionId, prospectId) {
  const result = await db.query(
    `SELECT * FROM acquisition_mission_candidate_observe_state
     WHERE mission_id = $1 AND prospect_id = $2
     LIMIT 1`,
    [missionId, String(prospectId)]
  );
  return result.rows[0] || null;
}

async function resolveProspectCompanyName(db, prospectId) {
  const result = await db.query(
    `SELECT c.name AS company_name
     FROM prospects p
     LEFT JOIN companies c ON c.id = p.company_id
     WHERE p.id = $1
     LIMIT 1`,
    [prospectId]
  );
  return result.rows[0]?.company_name || null;
}

async function findAoLeadForProspect(db, { clientId, prospectId, businessName = null }) {
  const byCrm = await db.query(
    `SELECT * FROM ao_leads
     WHERE client_id = $1 AND crm_prospect_id = $2
     LIMIT 1`,
    [clientId, prospectId]
  );
  if (byCrm.rows[0]) return byCrm.rows[0];

  const resolvedName = businessName || await resolveProspectCompanyName(db, prospectId);
  if (!resolvedName) return null;

  const byName = await db.query(
    `SELECT * FROM ao_leads
     WHERE client_id = $1
       AND lower(regexp_replace(business_name, '[^a-z0-9]', '', 'g'))
         = lower(regexp_replace($2, '[^a-z0-9]', '', 'g'))
     LIMIT 1`,
    [clientId, resolvedName]
  );
  return byName.rows[0] || null;
}

async function findExistingProspectNote(db, { clientId, prospectId, source }) {
  const result = await db.query(
    `SELECT * FROM prospect_notes
     WHERE client_id = $1 AND prospect_id = $2 AND source = $3
     LIMIT 1`,
    [clientId, prospectId, source]
  );
  return result.rows[0] || null;
}

async function findOpenOperationalTask(db, leadId) {
  const result = await db.query(
    `SELECT * FROM ao_follow_up_tasks
     WHERE lead_id = $1
       AND next_action = $2
       AND status = 'open'
     ORDER BY created_at DESC
     LIMIT 1`,
    [leadId, OPERATIONAL_TASK_NEXT_ACTION]
  );
  return result.rows[0] || null;
}

async function syncObserveReactionOperationalFollowUp(db, options = {}) {
  const {
    missionId,
    executionId,
    clientId,
    prospectId: prospectIdOverride = null,
    businessName = null,
    dryRun = false,
  } = options;

  if (!missionId || !executionId || !clientId) {
    throw Object.assign(new Error('missionId, executionId, and clientId are required.'), {
      code: 'operational_sync_invalid_input',
    });
  }

  await ensureObserveReactionSchema(db);
  await ensureLifecycleSchema(db);

  const execution = await loadExecution(db, executionId);
  if (!execution) {
    throw Object.assign(new Error(`Execution not found: ${executionId}`), { code: 'execution_not_found' });
  }

  const prospectId = prospectIdOverride || execution.prospect_id;
  if (!prospectId) {
    throw Object.assign(new Error('Prospect id could not be resolved from execution.'), {
      code: 'prospect_not_found',
    });
  }

  const reactions = await loadEffectiveObserveReactions(missionId, db, { skipEnsure: true });
  const effectiveHumanOpen = pickEffectiveHumanOpenReaction(reactions);
  if (!effectiveHumanOpen) {
    throw Object.assign(new Error('No effective human_open observe reaction found.'), {
      code: 'effective_human_open_missing',
    });
  }

  const timing = effectiveHumanOpen.recommendedTiming || {};
  if (!timing.dueAt) {
    throw Object.assign(new Error('Effective human_open reaction lacks recommendedTiming.dueAt.'), {
      code: 'recommended_timing_unresolved',
      reactionId: effectiveHumanOpen.id,
    });
  }

  const candidateRow = await loadCandidateObserveState(db, missionId, prospectId);
  const candidateState = candidateRow
    ? {
      disposition: candidateRow.disposition,
      evidenceStrength: candidateRow.evidence_strength,
      recommendedNextAction: candidateRow.recommended_next_action,
      recommendedTiming: candidateRow.recommended_timing,
    }
    : null;

  const annotation = await findPreparedCadenceAnnotation(db, { executionRecordId: execution.id });
  const annotationId = effectiveHumanOpen.reevaluationTriggerId || annotation?.id || null;
  if (!annotationId) {
    throw Object.assign(new Error('Cadence annotation id required for idempotent operational sync.'), {
      code: 'annotation_id_missing',
    });
  }

  const preparedCadence = await loadPreparedOutreachCadence({
    missionId,
    preparedArtifactRevision: execution.prepared_artifact_revision,
    executionApprovalContributionId: execution.execution_approval_contribution_id,
    executionRecordId: execution.id,
    prospectId,
  }, db);

  const metadata = buildOperationalSyncMetadata({
    effectiveReaction: effectiveHumanOpen,
    candidateState,
    preparedCadence,
    annotation,
    missionId,
    executionId,
    prospectId,
  });

  const noteSource = buildOperationalSyncNoteSource(annotationId);
  const taskDueDate = normalizeDueDate(timing.dueAt);
  const taskSummary = [
    'SPEC-252 observe reaction: human email open detected.',
    `Follow up due ${timing.dueAt} (${timing.waitDays ?? '?'} day cadence wait).`,
    'Engagement evidence — not buying intent.',
  ].join(' ');

  const lead = await findAoLeadForProspect(db, { clientId, prospectId, businessName });
  if (!lead) {
    throw Object.assign(new Error('AO lead not found for prospect.'), {
      code: 'ao_lead_not_found',
      prospectId,
      businessName,
    });
  }

  const report = {
    spec: 'SPEC-252',
    dryRun,
    missionId,
    executionId,
    clientId,
    prospectId,
    annotationId,
    noteSource,
    effectiveReaction: {
      id: effectiveHumanOpen.id,
      observationId: effectiveHumanOpen.observationId,
      evidenceType: effectiveHumanOpen.evidenceType,
      evaluationKind: effectiveHumanOpen.evaluationKind,
      recommendedTiming: effectiveHumanOpen.recommendedTiming,
    },
    metadata,
    aoLeadId: lead.id,
    actions: {
      leadUpdated: false,
      taskCreated: false,
      taskUpdated: false,
      noteCreated: false,
      noteSkipped: false,
      crmProspectLinked: false,
    },
  };

  if (dryRun) return report;

  const linkedCrmProspect = !lead.crm_prospect_id;

  const client = await db.connect();
  try {
    await client.query('BEGIN');

    const leadUpdate = await client.query(
      `UPDATE ao_leads
       SET status = 'needs_follow_up',
           interest_level = 'high',
           crm_prospect_id = COALESCE(crm_prospect_id, $2),
           next_follow_up_date = $3,
           next_follow_up_owner_id = COALESCE(next_follow_up_owner_id, ao_owner_id),
           last_contact_date = NOW(),
           updated_at = NOW()
       WHERE id = $1
       RETURNING id, status, interest_level, crm_prospect_id, next_follow_up_date`,
      [lead.id, prospectId, taskDueDate]
    );
    report.actions.leadUpdated = true;
    report.actions.crmProspectLinked = linkedCrmProspect;
    report.aoLead = leadUpdate.rows[0];

    const existingTask = await findOpenOperationalTask(client, lead.id);
    if (existingTask) {
      const taskUpdate = await client.query(
        `UPDATE ao_follow_up_tasks
         SET due_date = $2,
             priority = 'warm',
             last_interaction_summary = $3,
             waiting_on_jake = false
         WHERE id = $1
         RETURNING *`,
        [existingTask.id, taskDueDate, taskSummary]
      );
      report.actions.taskUpdated = true;
      report.task = taskUpdate.rows[0];
    } else {
      const taskInsert = await client.query(
        `INSERT INTO ao_follow_up_tasks (
          lead_id, ao_owner_id, due_date, status, priority, next_action,
          last_interaction_summary, waiting_on_jake
        ) VALUES ($1, $2, $3, 'open', 'warm', $4, $5, false)
        RETURNING *`,
        [lead.id, lead.ao_owner_id, taskDueDate, OPERATIONAL_TASK_NEXT_ACTION, taskSummary]
      );
      report.actions.taskCreated = true;
      report.task = taskInsert.rows[0];
    }

    const existingNote = await findExistingProspectNote(client, {
      clientId,
      prospectId,
      source: noteSource,
    });
    if (existingNote) {
      report.actions.noteSkipped = true;
      report.note = existingNote;
    } else {
      const noteText = formatOperationalSyncNoteText(metadata);
      const noteInsert = await client.query(
        `INSERT INTO prospect_notes (
          client_id, prospect_id, note_type, text, author_name, source
        ) VALUES ($1, $2, 'research', $3, 'SPEC-252 observe sync', $4)
        RETURNING *`,
        [clientId, prospectId, noteText, noteSource]
      );
      report.actions.noteCreated = true;
      report.note = noteInsert.rows[0];
    }

    await client.query('COMMIT');
    return report;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  OPERATIONAL_TASK_NEXT_ACTION,
  buildOperationalSyncNoteSource,
  buildOperationalSyncMetadata,
  formatOperationalSyncNoteText,
  pickEffectiveHumanOpenReaction,
  syncObserveReactionOperationalFollowUp,
};
