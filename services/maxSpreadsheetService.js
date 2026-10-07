'use strict';

const crypto = require('node:crypto');
const { supports, extract } = require('../packages/max/composer/adapters/spreadsheet');
const { LIMITS } = require('../packages/max/composer/limits');
const { buildSpreadsheetProposal, formatSpreadsheetProposal } = require('../packages/max/stateIngestion/spreadsheetProposal');
const { rejected, explicitlyApproves, deniesSave } = require('../utils/maxSpreadsheetAuthorization');

function hasSpreadsheetInput(body = {}) {
  return (body.attachments || []).some(a => a.type === 'spreadsheet' || supports(a));
}

function proposalStore(db, scope) {
  const { PostgresSpreadsheetProposalStore } = require('../packages/max/stateIngestion/spreadsheetProposalStore');
  return new PostgresSpreadsheetProposalStore(db, { clientId: scope.clientId, aoId: scope.aoId,
    approverUserId: process.env.MAX_SPREADSHEET_APPROVER_USER_ID });
}

function publicProposal(proposal, scope) {
  return { ...proposal, tenantId: scope.clientId, can_approve: scope.canApprove };
}

async function getScopedProposal(store, proposalId, conversationId, scope) {
  const proposal = await store.getProposal({ proposalId, actorId: scope.actorId, aoId: scope.aoId,
    conversationId, approverRead: scope.canApprove, requestedBy: scope.authenticatedUserId });
  if (String(proposal.conversationId) !== String(conversationId)) throw rejected('conversation_scope_mismatch', 409);
  return proposal;
}

function errorStatus(error) {
  if (error.commitOutcomeUnknown || error.code === 'SPREADSHEET_DB_TIMEOUT') return 503;
  if (error.statusCode) return error.statusCode;
  if (/NOT_FOUND|unavailable/.test(error.code || '')) return 404;
  if (/APPROVAL_REQUIRED|APPROVAL_AUTHORIZATION_CHANGED|ACCESS_REVOKED|OUTSIDE.*SCOPE|SCOPE_MISMATCH/.test(error.code || '')) return 403;
  if (/STALE|DIGEST|CONFLICT|ALREADY_CONSUMED|PRECONDITION|SUPERSEDED|NOT_PENDING|IDEMPOTENCY|CALL_SUPPRESSED/.test(error.code || '')) return 409;
  if (/INVALID|UNAPPROVABLE|UNSUPPORTED|REQUIRED|DEPENDENCY/.test(error.code || '')) return 422;
  if (error.code === '22P02') return 400;
  if (['42P01', '42703', '55P03', '57014'].includes(error.code)) return 503;
  return 500;
}

async function previewSpreadsheet(body, scope, { db, store = proposalStore(db, scope) } = {}) {
  const attachments = (body.attachments || []).filter(a => a.type === 'spreadsheet' || supports(a));
  if (attachments.length !== 1 || (body.attachments || []).length !== 1) throw rejected('select_one_spreadsheet', 422);
  const conversationId = String(body.conversation_id || body.conversationId || '').trim();
  if (!conversationId || conversationId.length > 200) throw rejected('conversation_id_required', 400);
  const attachment = attachments[0];
  const input = (body.attachment_inputs || body.attachmentInputs || []).find(a => a.id === attachment.id);
  const buffer = input?.buffer || (input?.content_base64 || input?.contentBase64
    ? Buffer.from(input.content_base64 || input.contentBase64, 'base64') : null);
  if (!Buffer.isBuffer(buffer) || !buffer.length) throw rejected('spreadsheet_bytes_required', 422);
  if (buffer.length > LIMITS.maxFileBytes) throw rejected('file_too_large', 413);
  const extracted = await extract(attachment, { buffer, filename: attachment.filename });
  if (extracted.extractionStatus !== 'ready') {
    throw rejected(extracted.extractionEvidence?.error || 'spreadsheet_parse_failed', 422);
  }
  const sourceHash = crypto.createHash('sha256').update(buffer).digest('hex');
  const snapshot = await store.snapshotContext({ aoId: scope.aoId });
  const plan = buildSpreadsheetProposal({ structuredData: extracted.structuredData, snapshot,
    scope: { ...scope, sourceHash }, fileHash: sourceHash });
  plan.sourceWorkbook = extracted.structuredData;
  const proposal = await store.createProposal({ actorId: scope.actorId, aoId: scope.aoId,
    conversationId, sourceHash, plan, baseline: snapshot });
  return { ok: true, preview_only: true, committed: false, commit: false, terminal_turn: true,
    review_required: true, can_approve: scope.canApprove,
    attachment_task_intent: 'SPREADSHEET_PREVIEW',
    operational_response: `${formatSpreadsheetProposal(plan)}\n\nNothing has been saved to CRM. Jake must approve the exact selected operations.`,
    spreadsheet_proposal: publicProposal(proposal, scope) };
}

async function commitSpreadsheet(proposalId, body, scope, { db, store = proposalStore(db, scope) } = {}) {
  if (!scope.canApprove) throw rejected('jake_approval_required');
  if (deniesSave(body.text) || !explicitlyApproves(body.text)) throw rejected('explicit_approval_required', 409);
  if (body.plan || body.reconciliation_plan || body.conversation_memory || body.spreadsheet_proposal) {
    throw rejected('client_proposal_not_accepted', 400);
  }
  if (!Array.isArray(body.operation_ids) || !body.operation_ids.length
      || new Set(body.operation_ids).size !== body.operation_ids.length) throw rejected('exact_operations_required', 400);
  if (!body.proposal_digest || !body.source_hash || !body.conversation_id
      || !/^[\w-]{16,128}$/.test(String(body.idempotency_key || ''))) throw rejected('approval_binding_required', 400);
  const proposal = await getScopedProposal(store, proposalId, body.conversation_id, scope);
  const receipt = await store.commitProposal({ proposalId, actorId: proposal.actorId, aoId: scope.aoId,
    conversationId: body.conversation_id, sourceHash: body.source_hash, expectedDigest: body.proposal_digest,
    idempotencyKey: body.idempotency_key, selectedOperationIds: body.operation_ids, approvedBy: scope.authenticatedUserId });
  return { ok: true, committed: true, terminal_turn: true, preview_only: false,
    operational_response: 'The selected operations were committed and verified. Unselected and unresolved changes were not approved.',
    spreadsheet_commit: receipt, spreadsheet_proposal: publicProposal({ ...proposal, status: 'committed', receipt }, scope) };
}

async function resolveSpreadsheet(proposalId, body, scope, { db, store = proposalStore(db, scope) } = {}) {
  if (!scope.canApprove) throw rejected('jake_resolution_required');
  if (body.plan || body.spreadsheet_proposal || !Array.isArray(body.resolutions) || !body.resolutions.length) {
    throw rejected('explicit_resolutions_required', 400);
  }
  const previous = await getScopedProposal(store, proposalId, body.conversation_id, scope);
  if (previous.digest !== body.proposal_digest || previous.sourceHash !== body.source_hash) {
    throw rejected('proposal_binding_mismatch', 409);
  }
  if (!previous.plan?.sourceWorkbook) throw rejected('source_workbook_required', 409);
  for (const resolution of body.resolutions) {
    if (resolution.sourceHash !== previous.sourceHash || !previous.plan.rows.some(row =>
      row.sheet === resolution.sheet && row.rowNumber === resolution.rowNumber)
      || typeof resolution.identityEvidence !== 'string' || resolution.identityEvidence.trim().length < 12) {
      throw rejected('source_bound_resolution_evidence_required', 400);
    }
  }
  const prior = previous.plan.resolutions || [];
  const merged = new Map(prior.map(value => [`${value.sheet}:${value.rowNumber}`, value]));
  for (const resolution of body.resolutions) merged.set(`${resolution.sheet}:${resolution.rowNumber}`, resolution);
  const resolutions = [...merged.values()];
  const snapshot = await store.snapshotContext({ aoId: scope.aoId });
  const plan = buildSpreadsheetProposal({ structuredData: previous.plan.sourceWorkbook, snapshot,
    scope: { ...scope, sourceHash: previous.sourceHash }, fileHash: previous.sourceHash, resolutions });
  plan.sourceWorkbook = previous.plan.sourceWorkbook;
  plan.resolutions = resolutions;
  plan.supersedesProposalId = previous.id;
  const proposal = await store.createProposal({ actorId: previous.actorId, aoId: scope.aoId,
    conversationId: previous.conversationId, sourceHash: previous.sourceHash, plan, baseline: snapshot });
  return { ok: true, preview_only: true, committed: false, terminal_turn: true, can_approve: scope.canApprove,
    operational_response: `${formatSpreadsheetProposal(plan)}\n\nThis is a new proposal. Nothing saved. Review and approve its selected operations.`,
    spreadsheet_proposal: publicProposal(proposal, scope) };
}

module.exports = { hasSpreadsheetInput, previewSpreadsheet, commitSpreadsheet, resolveSpreadsheet,
  proposalStore, publicProposal, getScopedProposal, errorStatus };
