'use strict';

const crypto = require('crypto');
const { parseSpreadsheetRow } = require('./claimParser');
const { ingestOperationalUpdate } = require('./pipeline');
const {
  buildSpreadsheetReconciliationPlan,
  commitSpreadsheetReconciliationPlan,
  buildWorkbookSummary,
} = require('./spreadsheetReconciliation');

function workbookFileHash({ filename, sheets = [] }) {
  const payload = JSON.stringify({
    filename,
    sheets: sheets.map(s => ({
      sheet: s.sheet || s.name,
      rows: (s.rows || []).map(r => r.values || r.raw || r),
    })),
  });
  return crypto.createHash('sha256').update(payload).digest('hex');
}

async function ingestSpreadsheet({
  clientId,
  filename,
  sheetName,
  rows = [],
  sheets = null,
  sourceType = 'FILE_IMPORTED',
  sourceActor = null,
  store,
  now = new Date(),
  instruction = null,
  commitMode = 'safe_only',
}) {
  const structuredData = sheets
    ? { filename, sheets }
    : {
      filename,
      sheets: [{ sheet: sheetName || 'Prospects', rows: rows.map((row, i) => ({
        rowNumber: row.__rowNumber ?? i + 2,
        values: row,
        raw: row,
      })) }],
    };

  const fileHash = workbookFileHash(structuredData);
  const plan = buildSpreadsheetReconciliationPlan({
    structuredData,
    store,
    instruction,
    fileId: `wb_${fileHash.slice(0, 16)}`,
    fileHash,
  });

  const batch = await commitSpreadsheetReconciliationPlan({
    plan,
    clientId,
    store,
    sourceActor,
    instruction,
    now,
    commitMode,
  });

  const recordResults = batch.results;
  const committed = recordResults.filter(r => !r.skipped && !r.commit_blocked).length;
  const held = recordResults.filter(r => r.skipped || r.unresolved?.length || r.conflicts?.length).length;

  return {
    recordsExamined: plan.summary.totalRows,
    workbookSummary: plan.workbookSummary,
    reconciliationPlan: plan,
    recordResults,
    summary: {
      committed,
      held,
      updated: committed,
      created: recordResults.filter(r => r.entities_created > 0).length,
      duplicates: plan.summary.duplicateSuppressed,
      safeChanges: plan.summary.safeChanges,
      conflicts: plan.summary.conflicts,
      ambiguous: plan.summary.ambiguous,
      ignored: plan.summary.ignored,
    },
    telemetry: batch.telemetry,
  };
}

module.exports = {
  ingestSpreadsheet,
  workbookFileHash,
  buildWorkbookSummary,
};
