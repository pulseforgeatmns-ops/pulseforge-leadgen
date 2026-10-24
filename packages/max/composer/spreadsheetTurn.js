'use strict';

const { stableHash } = require('../stateIngestion/fingerprints');
const {
  buildSpreadsheetReconciliationPlan,
  commitSpreadsheetReconciliationPlan,
  formatSpreadsheetOperationalResponse,
} = require('../stateIngestion/spreadsheetReconciliation');
const { computeWorkbookDiagnostics } = require('../stateIngestion/spreadsheetRowAudit');
const { buildSpreadsheetPreview } = require('./preview');
const { bump, noteDimension } = require('./telemetry');
const {
  ATTACHMENT_TASK_INTENT,
  isSpreadsheetOperationalIntent,
} = require('./attachmentIntent');

function structuredDataFromAttachment(attachment) {
  const data = attachment?.structuredData;
  if (!data?.sheets) return null;
  return {
    filename: data.filename || attachment.filename,
    sheets: data.sheets,
  };
}

function structuredDataFromRows(spreadsheetRows = []) {
  if (!spreadsheetRows.length) return null;
  const sheetMap = new Map();
  for (const row of spreadsheetRows) {
    const sheet = row.sheet || 'Sheet1';
    if (!sheetMap.has(sheet)) sheetMap.set(sheet, []);
    sheetMap.get(sheet).push(row);
  }
  return {
    filename: spreadsheetRows[0]?.filename || 'spreadsheet',
    sheets: [...sheetMap.entries()].map(([sheet, rows]) => ({ sheet, rows })),
  };
}

function recordSpreadsheetTelemetry(telemetry, attachmentIntent) {
  bump(telemetry, 'max_attachment_intent_detected_count');
  noteDimension(telemetry, 'attachment_type', 'spreadsheet');
  noteDimension(telemetry, 'preview_only', Boolean(attachmentIntent.previewOnly));
  noteDimension(telemetry, 'intent_source', attachmentIntent.intentSource || 'unknown');
  if (attachmentIntent.intent === ATTACHMENT_TASK_INTENT.SPREADSHEET_PREVIEW) {
    bump(telemetry, 'max_spreadsheet_preview_intent_count');
  }
  if (attachmentIntent.intent === ATTACHMENT_TASK_INTENT.SPREADSHEET_RECONCILE) {
    bump(telemetry, 'max_spreadsheet_reconcile_intent_count');
  }
  if (attachmentIntent.intent === ATTACHMENT_TASK_INTENT.SPREADSHEET_COMMIT) {
    bump(telemetry, 'max_spreadsheet_commit_intent_count');
  }
}

function persistWorkbookContext(memory, {
  structuredData,
  attachmentId,
  envelopeId,
  conversationId,
  actor,
  plan,
  fileHash,
}) {
  if (!memory?.recordPendingSpreadsheetWorkbook) return;
  memory.recordPendingSpreadsheetWorkbook({
    workbookId: plan.workbookId,
    attachmentId,
    envelopeId,
    conversationId,
    reconciliationPlanId: plan.reconciliationPlanId,
    reconciliationPlan: plan,
    structuredData,
    fileHash,
    committedRowKeys: (plan.rows || []).map(r => r.rowKey).filter(Boolean),
    effectiveActor: actor,
    totalRows: plan.summary?.totalRows ?? null,
  });
}

function recordSpreadsheetReconciliationTelemetry(telemetry, plan) {
  const diag = plan.diagnostics || computeWorkbookDiagnostics(plan);
  bump(telemetry, 'max_spreadsheet_terminal_turn_count');
  bump(telemetry, 'max_spreadsheet_fields_compared_count', diag.fields_compared || 0);
  bump(telemetry, 'max_spreadsheet_unmapped_field_count', diag.fields_unmapped || 0);
  bump(telemetry, 'max_spreadsheet_noop_row_count', diag.rows_no_change || 0);
  if (!diag.rows_with_changes && diag.rows_total > 0) {
    bump(telemetry, 'max_spreadsheet_zero_change_workbook_count');
  }
}

async function runSpreadsheetAttachmentCommand({
  structuredData,
  store,
  clientId,
  instruction,
  memory,
  conversationId,
  envelopeId,
  attachmentId,
  actor,
  attachmentIntent,
  telemetry,
  confirmCommit = false,
  priorFileHash = null,
}) {
  if (!structuredData) {
    bump(telemetry, 'max_attachment_intent_fallback_error_count');
    return { error: 'missing_workbook', message: 'No spreadsheet data available to reconcile.' };
  }

  recordSpreadsheetTelemetry(telemetry, attachmentIntent);
  const wantsCommit = Boolean(attachmentIntent.commit || confirmCommit);
  const fileHash = stableHash([JSON.stringify(structuredData)]);
  const pending = memory?.getPendingSpreadsheetWorkbook?.() || null;
  const reusePendingPlan = Boolean(
    wantsCommit
    && pending?.reconciliationPlan
    && pending.fileHash === fileHash
    && pending.reconciliationPlanId
  );
  const plan = reusePendingPlan
    ? pending.reconciliationPlan
    : buildSpreadsheetReconciliationPlan({
      structuredData,
      store,
      instruction,
      fileId: envelopeId || attachmentId,
      fileHash,
      memory,
      conversationId,
      priorFileHash,
    });
  plan.reconciliationPlanId = plan.reconciliationPlanId
    || `srp_${plan.workbookId}_${String(fileHash).slice(0, 12)}`;
  recordSpreadsheetReconciliationTelemetry(telemetry, plan);

  const previewOnly = wantsCommit
    ? false
    : Boolean(attachmentIntent.previewOnly || attachmentIntent.intent !== ATTACHMENT_TASK_INTENT.SPREADSHEET_RECONCILE);

  const operationalResponse = formatSpreadsheetOperationalResponse(plan, {
    previewOnly,
    instruction,
  });

  if (previewOnly) {
    const batchPreview = buildSpreadsheetPreview({
      rows: structuredData.sheets.flatMap(s => (s.rows || []).map(r => ({ ...r, sheet: s.sheet, filename: structuredData.filename }))),
      instruction,
      store,
      memory,
      conversationId,
      filename: structuredData.filename,
    });
    batchPreview.summary = operationalResponse;
    batchPreview.reconciliation_plan = plan;
    persistWorkbookContext(memory, {
      structuredData,
      attachmentId,
      envelopeId,
      conversationId,
      actor,
      plan,
      fileHash,
    });
    return {
      preview_only: true,
      review_required: true,
      batch_preview: batchPreview,
      reconciliation_plan: plan,
      operational_response: operationalResponse,
      spreadsheet_reconciliation: plan.diagnostics || computeWorkbookDiagnostics(plan),
      terminal_turn: true,
      attachment_task_intent: attachmentIntent.intent,
      commit: false,
    };
  }

  const batch = await commitSpreadsheetReconciliationPlan({
    plan,
    clientId,
    store,
    sourceActor: actor?.userId || actor?.aoId || null,
    instruction,
    commitMode: 'safe_only',
  });
  for (const [key, value] of Object.entries(batch.telemetry || {})) {
    if (key.startsWith('max_spreadsheet_')) bump(telemetry, key, value);
  }
  const applied = batch.results.filter(r => !r.skipped && !r.commit_blocked).length;
  const noChange = plan.rows.filter(r => r.outcome === 'no_change' || r.outcome === 'ignored').length;
  const pendingReview = plan.rows.filter(r => r.outcome === 'needs_review').length;
  const commitOperationalResponse = formatSpreadsheetOperationalResponse(plan, {
    previewOnly: false,
    instruction,
    commitSummary: `Applied ${applied} safe change${applied === 1 ? '' : 's'}. ${noChange} row${noChange === 1 ? '' : 's'} required no change. ${pendingReview} row${pendingReview === 1 ? '' : 's'} remain pending review.`,
  });
  memory?.clearPendingSpreadsheetWorkbook?.();
  return {
    preview_only: false,
    committed: true,
    reconciliation_plan: plan,
    operational_response: commitOperationalResponse,
    spreadsheet_reconciliation: plan.diagnostics || computeWorkbookDiagnostics(plan),
    terminal_turn: true,
    attachment_task_intent: attachmentIntent.intent,
    commit: true,
    results: batch.results,
    batch,
  };
}

module.exports = {
  structuredDataFromAttachment,
  structuredDataFromRows,
  runSpreadsheetAttachmentCommand,
  isSpreadsheetOperationalIntent,
};
