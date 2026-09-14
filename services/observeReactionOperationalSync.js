'use strict';

/**
 * SPEC-252 — Bridge durable observe reaction timing into AO needs_follow_up tasks
 * and research prospect notes (exact timing preserved in metadata/notes).
 */

const crypto = require('crypto');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const { ensureLifecycleSchema } = require('../utils/lifecycleSchema');
const aoField = require('./aoFieldService');
const { buildDirectMailOpening } = require('../utils/aoMessageTemplates');
const { loadEffectiveObserveReactions } = require('./acquisitionMissionPersistence');

const BACKUS_BUSINESS_NAME = 'Backus, Meyer & Branch, LLP';

function researchNoteSource(annotationId) {
  return `spec252_observe_reeval:${String(annotationId || 'unknown')}`;
}

function buildObserveFollowUpMetadata(input = {}) {
  const {
    reaction = {},
    candidateState = {},
    mission = {},
    execution = {},
    preparedCadence = {},
    annotation = {},
  } = input;

  const timing = reaction.recommendedTiming || candidateState.recommendedTiming || {};
  return {
    spec: 'SPEC-251',
    specContext: 'SPEC-252_cadence_reevaluation',
    missionId: mission.id || reaction.missionId || null,
    executionId: execution.id || null,
    prospectId: reaction.prospectId || candidateState.prospectId || execution.prospect_id || null,
    observationId: reaction.observationId || null,
    reactionId: reaction.id || null,
    evaluationKind: reaction.evaluationKind || null,
    reevaluationTriggerKind: reaction.reevaluationTriggerKind || null,
    reevaluationTriggerId: reaction.reevaluationTriggerId || annotation.id || null,
    disposition: candidateState.disposition || reaction.updatedDisposition || null,
    evidenceStrength: candidateState.evidenceStrength || reaction.evidenceStrength || null,
    evidenceType: reaction.evidenceType || candidateState.lastEvidenceType || null,
    recommendedNextAction: candidateState.recommendedNextAction || reaction.recommendedNextAction || null,
    recommendedTiming: {
      kind: timing.kind ?? null,
      waitDays: timing.waitDays ?? null,
      dueAt: timing.dueAt ?? null,
      cadenceSource: timing.cadenceSource ?? null,
      cadenceProvenance: timing.cadenceProvenance ?? preparedCadence.cadenceProvenance ?? null,
      reconstructed: timing.reconstructed === true || preparedCadence.reconstructed === true,
      clockStart: timing.clockStart ?? null,
    },
    sequenceStepDays: Array.isArray(preparedCadence.steps)
      ? preparedCadence.steps.map((row) => row.day)
      : (preparedCadence.outreachSequence?.steps || []).map((row) => row.day),
    cadenceAnnotationId: annotation.id || null,
    preparedArtifactRevision: execution.prepared_artifact_revision || null,
  };
}

function formatResearchNoteText(metadata = {}) {
  const timing = metadata.recommendedTiming || {};
  const lines = [
    'SPEC-252 observe cadence re-evaluation (research).',
    `Evidence: ${metadata.evidenceType || 'unknown'} → disposition ${metadata.disposition || 'unknown'}.`,
    `Next action: ${metadata.recommendedNextAction || 'unknown'}.`,
    `Timing: kind=${timing.kind ?? 'null'}, waitDays=${timing.waitDays ?? 'null'}, dueAt=${timing.dueAt ?? 'null'}.`,
    `Cadence: source=${timing.cadenceSource ?? 'null'}, provenance=${timing.cadenceProvenance ?? 'null'}, reconstructed=${timing.reconstructed === true}.`,
    '',
    'metadata:',
    JSON.stringify(metadata, null, 2),
  ];
  return lines.join('\n');
}

function formatNeedsFollowUpSummary(metadata = {}) {
  const timing = metadata.recommendedTiming || {};
  return [
    'Email observe signal — needs_follow_up.',
    `Human open recorded; wait ${timing.waitDays ?? '?'} day(s) until ${timing.dueAt || 'due date unknown'}.`,
    `Cadence source: ${timing.cadenceSource || 'unresolved'} (${timing.cadenceProvenance || 'n/a'}).`,
  ].join(' ');
}

function dueDateFromTiming(timing = {}) {
  if (!timing.dueAt) return null;
  const parsed = new Date(timing.dueAt);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

async function findResearchNoteBySource(pool, clientId, prospectId, source) {
  const result = await pool.query(
    `SELECT id, note_type, text, source, created_at
     FROM prospect_notes
     WHERE client_id = $1 AND prospect_id = $2 AND source = $3
     LIMIT 1`,
    [clientId, prospectId, source]
  );
  return result.rows[0] || null;
}

async function upsertResearchProspectNote({
  pool,
  clientId,
  prospectId,
  metadata,
  annotationId,
}) {
  await ensureLifecycleSchema(pool);
  const source = researchNoteSource(annotationId);
  const existing = await findResearchNoteBySource(pool, clientId, prospectId, source);
  if (existing) {
    return { inserted: false, duplicate: true, note: existing, source };
  }

  const text = formatResearchNoteText(metadata);
  const result = await pool.query(
    `INSERT INTO prospect_notes (
      client_id, prospect_id, note_type, text, author_id, author_name, source
    ) VALUES ($1, $2, 'research', $3, NULL, 'Max OBSERVE', $4)
    RETURNING *`,
    [clientId, prospectId, text, source]
  );
  return { inserted: true, duplicate: false, note: result.rows[0], source };
}

async function syncNeedsFollowUpAoLead({
  pool,
  clientId,
  prospectId,
  businessName,
  metadata,
  aoOwnerId = null,
}) {
  await ensureAoFieldSchema();

  let lead = null;
  const byName = await aoField.findDirectMailLead(clientId, businessName);
  if (byName?.id) {
    const loaded = await pool.query('SELECT * FROM ao_leads WHERE id = $1 LIMIT 1', [byName.id]);
    lead = loaded.rows[0] || null;
  }
  if (!lead && prospectId) {
    const linked = await pool.query(
      `SELECT * FROM ao_leads
       WHERE client_id = $1 AND crm_prospect_id = $2
       LIMIT 1`,
      [clientId, prospectId]
    );
    lead = linked.rows[0] || null;
  }

  const timing = metadata.recommendedTiming || {};
  const followUpDue = dueDateFromTiming(timing) || aoField.endOfBusinessWeekISO();
  const summary = formatNeedsFollowUpSummary(metadata);

  if (!lead) {
    if (!aoOwnerId) {
      const mike = await aoField.resolveAoOwnerByName('%Mike%', clientId);
      aoOwnerId = mike?.id || null;
    }
    if (!aoOwnerId) {
      return { skipped: true, reason: 'ao_owner_not_found', businessName };
    }

    const created = await aoField.createDirectMailFollowUpLead({
      clientId,
      aoOwnerId,
      aoName: 'Mike',
      businessName,
      campaignName: 'Campaign 001',
      note: summary,
      dueDate: followUpDue,
    });
    if (created.skipped) {
      lead = created.lead;
    } else {
      lead = created.lead;
      if (prospectId && lead?.id) {
        await pool.query(
          `UPDATE ao_leads
           SET crm_prospect_id = $2, status = 'needs_follow_up', interest_level = 'high',
               last_contact_date = NOW(), updated_at = NOW()
           WHERE id = $1`,
          [lead.id, prospectId]
        );
      }
      return {
        inserted: true,
        leadId: lead?.id || null,
        taskId: created.task?.id || null,
        status: 'needs_follow_up',
        dueDate: followUpDue,
        summary,
      };
    }
  }

  await pool.query(
    `UPDATE ao_leads
     SET status = 'needs_follow_up',
         interest_level = CASE WHEN interest_level = 'low' THEN 'medium' ELSE 'high' END,
         crm_prospect_id = COALESCE(crm_prospect_id, $2),
         next_follow_up_date = $3,
         last_contact_date = NOW(),
         updated_at = NOW()
     WHERE id = $1`,
    [lead.id, prospectId || null, followUpDue]
  );

  const openTask = await pool.query(
    `SELECT id FROM ao_follow_up_tasks
     WHERE lead_id = $1 AND status = 'open'
     ORDER BY created_at DESC
     LIMIT 1`,
    [lead.id]
  );

  let taskId = openTask.rows[0]?.id || null;
  if (taskId) {
    await pool.query(
      `UPDATE ao_follow_up_tasks
       SET priority = 'warm',
           due_date = $2,
           last_interaction_summary = $3,
           next_action = COALESCE(next_action, 'in_person_revisit')
       WHERE id = $1`,
      [taskId, followUpDue, summary]
    );
  } else if (lead.ao_owner_id) {
    const inserted = await pool.query(
      `INSERT INTO ao_follow_up_tasks (
         lead_id, ao_owner_id, due_date, priority, next_action,
         last_interaction_summary, suggested_message
       ) VALUES ($1,$2,$3,'warm','in_person_revisit',$4,$5)
       RETURNING id`,
      [
        lead.id,
        lead.ao_owner_id,
        followUpDue,
        summary,
        buildDirectMailOpening('Mike'),
      ]
    );
    taskId = inserted.rows[0]?.id || null;
  }

  return {
    inserted: false,
    updated: true,
    leadId: lead.id,
    taskId,
    status: 'needs_follow_up',
    dueDate: followUpDue,
    summary,
  };
}

async function syncObserveReactionOperationalFollowUp(input = {}, pool = require('../db')) {
  const {
    mission = {},
    execution = {},
    preparedCadence = {},
    annotation = {},
    clientId = 10,
    businessName = BACKUS_BUSINESS_NAME,
    aoOwnerId = null,
  } = input;

  const effectiveReactions = await loadEffectiveObserveReactions(mission.id, pool);
  const humanOpen = [...effectiveReactions].reverse().find((row) => row.evidenceType === 'human_open');
  if (!humanOpen) {
    return { skipped: true, reason: 'no_effective_human_open_reaction' };
  }

  const candidate = await pool.query(
    `SELECT * FROM acquisition_mission_candidate_observe_state
     WHERE mission_id = $1 AND prospect_id = $2
     LIMIT 1`,
    [mission.id, String(humanOpen.prospectId)]
  );
  const candidateState = candidate.rows[0]
    ? {
      disposition: candidate.rows[0].disposition,
      evidenceStrength: candidate.rows[0].evidence_strength,
      recommendedNextAction: candidate.rows[0].recommended_next_action,
      recommendedTiming: candidate.rows[0].recommended_timing,
      prospectId: candidate.rows[0].prospect_id,
    }
    : {};

  const metadata = buildObserveFollowUpMetadata({
    reaction: humanOpen,
    candidateState,
    mission,
    execution,
    preparedCadence,
    annotation,
  });

  const prospectId = metadata.prospectId || execution.prospect_id || null;
  if (!prospectId) {
    return { skipped: true, reason: 'prospect_id_unresolved', metadata };
  }

  const researchNote = await upsertResearchProspectNote({
    pool,
    clientId,
    prospectId,
    metadata,
    annotationId: annotation.id || metadata.reevaluationTriggerId,
  });

  const aoFollowUp = await syncNeedsFollowUpAoLead({
    pool,
    clientId,
    prospectId,
    businessName,
    metadata,
    aoOwnerId,
  });

  return {
    metadata,
    researchNote,
    needsFollowUp: aoFollowUp,
  };
}

function metadataFingerprint(metadata = {}) {
  return crypto.createHash('sha256')
    .update(JSON.stringify(metadata))
    .digest('hex')
    .slice(0, 16);
}

module.exports = {
  BACKUS_BUSINESS_NAME,
  researchNoteSource,
  buildObserveFollowUpMetadata,
  formatResearchNoteText,
  formatNeedsFollowUpSummary,
  dueDateFromTiming,
  upsertResearchProspectNote,
  syncNeedsFollowUpAoLead,
  syncObserveReactionOperationalFollowUp,
  metadataFingerprint,
};
