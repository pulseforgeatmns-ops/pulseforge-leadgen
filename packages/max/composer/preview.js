'use strict';

const {
  buildSpreadsheetReconciliationPlan,
  formatSpreadsheetReconciliationPreview,
} = require('../stateIngestion/spreadsheetReconciliation');

function buildSpreadsheetPreview({
  rows = [],
  instruction = null,
  store,
  memory,
  conversationId,
  filename,
  sheetName,
}) {
  const sheetMap = new Map();
  for (const row of rows) {
    const sheet = row.sheet || sheetName || 'Prospects';
    if (!sheetMap.has(sheet)) sheetMap.set(sheet, []);
    sheetMap.get(sheet).push(row);
  }
  const structuredData = {
    filename: filename || rows[0]?.filename || 'spreadsheet',
    sheets: [...sheetMap.entries()].map(([sheet, sheetRows]) => ({
      sheet,
      rows: sheetRows,
    })),
  };

  const plan = buildSpreadsheetReconciliationPlan({
    structuredData,
    store,
    instruction,
    memory,
    conversationId,
  });

  const summary = formatSpreadsheetReconciliationPreview(plan);
  const needsClarification = plan.summary.ambiguous + (plan.summary.conflicts > 0 ? 1 : 0);
  const ready = plan.summary.safeChanges;

  const examples = [];
  for (const rowPlan of plan.rows) {
    if (examples.length >= 4) break;
    if (rowPlan.ambiguities.length) {
      examples.push(`Row ${rowPlan.row} — ${rowPlan.ambiguities[0].message || 'needs clarification'}`);
    } else if (rowPlan.conflicts.length) {
      examples.push(`Row ${rowPlan.row} — ${rowPlan.conflicts[0].message || 'conflict'}`);
    } else if (rowPlan.accountResolution?.entity) {
      const name = rowPlan.accountResolution.entity.company_name || rowPlan.accountResolution.entity.name;
      examples.push(`${name} — matched existing account`);
    }
  }

  return {
    total: plan.summary.totalRows,
    ready,
    needs_clarification: needsClarification,
    rejected: plan.summary.ignored,
    matched: plan.rows.filter(r => r.accountResolution?.entity).length,
    new_account: plan.rows.filter(r =>
      r.proposedChanges.some(c => c.type === 'CREATE_ACCOUNT_CANDIDATE')
    ).length,
    summary,
    reconciliation_plan: plan,
    counts: {
      total: plan.summary.totalRows,
      ready,
      needs_clarification: needsClarification,
      rejected: plan.summary.ignored,
      conflicts: plan.summary.conflicts,
      ambiguous: plan.summary.ambiguous,
    },
    examples,
  };
}

module.exports = {
  buildSpreadsheetPreview,
};
