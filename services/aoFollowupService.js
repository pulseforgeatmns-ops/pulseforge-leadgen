'use strict';

const pool = require('../db');
const { ensureAoCrmSchema } = require('../utils/aoCrmSchema');
const { composeAoFollowUp } = require('../utils/aoFollowupComposer');
const { loadIdentityForAssignedAo } = require('../utils/aoCommunicationIdentity');
const { ensureProspectAccess } = require('./aoProspectUpdateService');
const { insertActivity } = require('./aoCrmService');

function extractPainSignals({ notes, detail }) {
  const signals = [];
  const blob = String(notes || '').toLowerCase();
  if (/kitchen/.test(blob)) signals.push('kitchen area missed');
  if (/under tables|sweep under/.test(blob)) signals.push('under tables not swept');
  if (/unhappy|dissatisfied/.test(blob)) signals.push('dissatisfaction with current cleaning');
  const angle = detail?.summary?.recommended_angle;
  if (angle && /pain|concern|issue/i.test(angle)) signals.push(angle);
  return [...new Set(signals)];
}

function extractBuildingMgmt(notes, detail) {
  const blob = String(notes || '');
  const match = blob.match(/([A-Z][\w\s&]+(?:Investment Properties|Property Management|Management))/);
  if (match) return match[1].trim();
  if (/nash family/i.test(blob)) return 'Nash Family Investment Properties';
  return detail?.summary?.why_account_matters?.includes('management') ? detail.summary.why_account_matters : null;
}

function extractVendor(notes) {
  const blob = String(notes || '');
  if (/trillium/i.test(blob)) return 'Trillium';
  const match = blob.match(/\bvendor[:\s]+([A-Za-z][\w\s]+)/i);
  return match ? match[1].trim() : null;
}

function buildComposerInput({
  clientId,
  prospectId,
  detail,
  profile,
  body = {},
  source = 'crm_account',
}) {
  const notes = [body.aoNotes, detail.history?.[0]?.notes].filter(Boolean).join('\n');
  const last = detail.history?.[0];
  const assignedAoId = detail.summary.assigned_ao_id || profile?.id;
  return {
    tenantId: clientId,
    accountId: prospectId,
    accountName: detail.summary.company_name,
    accountType: detail.summary.icp_category,
    address: detail.summary.address,
    phone: detail.contact.phone,
    assignedAoId,
    assignedAoName: profile?.name || detail.summary.ao_owner || 'Anchor Cleaning',
    assignedAoEmail: profile?.email || null,
    contactName: body.contactName || detail.contact.name,
    contactRole: body.contactRole || detail.contact.role,
    contactEmail: body.contactEmail || detail.contact.email,
    contactPhone: body.contactPhone || detail.contact.phone,
    currentStage: detail.sales_state.current_status,
    recommendedNextAction: body.recommendedNextAction || detail.sales_state.next_action,
    recommendedAngle: detail.summary.recommended_angle,
    lastActivityType: last?.activity_type || null,
    lastActivityDate: last?.created_at || null,
    lastActivitySummary: last?.notes || null,
    aoNotes: notes || null,
    knownPainSignals: extractPainSignals({ notes, detail }),
    currentVendorOrProvider: body.currentVendorOrProvider || extractVendor(notes),
    buildingManagementCompany: body.buildingManagementCompany || extractBuildingMgmt(notes, detail),
    requiresJakeApproval: Boolean(body.requiresJakeApproval),
    jakeInvolvementReason: body.jakeInvolvementReason || null,
    source,
  };
}

async function generateFollowUpDraft({
  clientId,
  aoUserId,
  prospectId,
  body = {},
  profile,
  db = pool,
}) {
  await ensureAoCrmSchema(db);
  const access = await ensureProspectAccess({ prospectId, clientId, aoUserId, db });
  if (access.error) return access;

  const aoCrm = require('./aoCrmService');
  const fullDetail = await aoCrm.getAccountDetail({ clientId, prospectId, aoUserId, db });
  if (!fullDetail) return { error: 'Account not found', status: 404 };

  const assignedAoId = fullDetail.summary?.assigned_ao_id || profile?.id;
  const { identity, assignedAo } = await loadIdentityForAssignedAo(db, {
    assignedAoId,
    tenantId: clientId,
  });

  const input = buildComposerInput({
    clientId,
    prospectId,
    detail: fullDetail,
    profile,
    body: {
      ...body,
      aoCommunicationIdentity: identity,
      assignedAoContext: assignedAo,
    },
    source: body.source || 'crm_account',
  });
  input.aoCommunicationIdentity = identity;
  input.assignedAoContext = assignedAo;

  const draft = composeAoFollowUp(input);
  return { ok: true, draft, input_snapshot: input };
}

async function saveFollowUpDraft({
  clientId,
  aoUserId,
  prospectId,
  draft,
  inputSnapshot = {},
  flagJakeReview = false,
  db = pool,
}) {
  await ensureAoCrmSchema(db);
  if (!draft || !draft.recommendedFollowUpAngle) {
    return { error: 'Draft payload required', status: 400 };
  }

  const access = await ensureProspectAccess({ prospectId, clientId, aoUserId, db });
  if (access.error) return access;

  const assignedAoId = access.prospect?.assigned_ao_id || aoUserId;

  const { rows } = await db.query(`
    INSERT INTO ao_followup_drafts (
      tenant_id, account_id, assigned_ao_id,
      status, approval_path, approval_reason,
      recommended_followup_angle, subject_line, email_draft, alternate_short_note,
      next_action_after_send, input_snapshot, doctrine_checks, warnings
    ) VALUES (
      $1, $2::uuid, $3,
      $4, $5, $6,
      $7, $8, $9, $10,
      $11, $12::jsonb, $13::jsonb, $14::jsonb
    )
    RETURNING *
  `, [
    clientId,
    prospectId,
    assignedAoId,
    draft.status,
    draft.approvalPath,
    draft.approvalReason || null,
    draft.recommendedFollowUpAngle,
    draft.subjectLine || null,
    draft.emailDraft || null,
    draft.alternateShortNote || null,
    draft.nextActionAfterSend,
    JSON.stringify(inputSnapshot || {}),
    JSON.stringify(draft.doctrineChecks || {}),
    JSON.stringify(draft.warnings || []),
  ]);

  const saved = rows[0];

  await insertActivity({
    clientId,
    prospectId,
    aoUserId,
    activityType: 'followup_draft_created',
    notes: `AO Follow-Up Composer generated a draft for ${inputSnapshot.accountName || 'account'}.`,
    metadata: {
      draft_id: saved.id,
      approval_path: draft.approvalPath,
      status: draft.status,
      recommended_followup_angle: draft.recommendedFollowUpAngle,
    },
    db,
  });

  if (flagJakeReview) {
    await db.query(`
      UPDATE prospects SET
        help_requested = true,
        help_reason = COALESCE($3, help_reason, 'Jake review recommended for follow-up draft'),
        help_requested_at = COALESCE(help_requested_at, NOW()),
        updated_at = NOW()
      WHERE id = $1::uuid AND client_id = $2
    `, [prospectId, clientId, draft.approvalReason || 'Follow-up draft flagged for Jake review']);
  }

  return { ok: true, draft: mapDraftRow(saved) };
}

function mapDraftRow(row) {
  return {
    id: String(row.id),
    status: row.status,
    approvalPath: row.approval_path,
    approvalReason: row.approval_reason,
    recommendedFollowUpAngle: row.recommended_followup_angle,
    subjectLine: row.subject_line,
    emailDraft: row.email_draft,
    alternateShortNote: row.alternate_short_note,
    nextActionAfterSend: row.next_action_after_send,
    warnings: row.warnings || [],
    doctrineChecks: row.doctrine_checks || {},
    createdAt: row.created_at,
  };
}

async function listFollowUpDrafts({ clientId, prospectId, aoUserId = null, db = pool }) {
  await ensureAoCrmSchema(db);
  const params = [clientId, prospectId];
  let ownerClause = '';
  if (aoUserId) {
    params.push(aoUserId);
    ownerClause = `AND assigned_ao_id = $${params.length}`;
  }

  const { rows } = await db.query(`
    SELECT *
    FROM ao_followup_drafts
    WHERE tenant_id = $1 AND account_id = $2::uuid
      ${ownerClause}
    ORDER BY created_at DESC
    LIMIT 50
  `, params);

  return { drafts: rows.map(mapDraftRow) };
}

module.exports = {
  buildComposerInput,
  generateFollowUpDraft,
  saveFollowUpDraft,
  listFollowUpDrafts,
};
