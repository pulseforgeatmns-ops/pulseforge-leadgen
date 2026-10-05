'use strict';

const { parseSpreadsheetRow } = require('./claimParser');
const { ingestOperationalUpdate } = require('./pipeline');

async function ingestSpreadsheet({
  clientId,
  filename,
  sheetName,
  rows = [],
  sourceType = 'FILE_IMPORTED',
  sourceActor = null,
  store,
  now = new Date(),
}) {
  const recordResults = [];
  let committed = 0;
  let held = 0;

  for (let i = 0; i < rows.length; i += 1) {
    const rowNumber = rows[i].__rowNumber ?? i + 1;
    const row = rows[i];
    const claims = parseSpreadsheetRow(row, { sheet: sheetName, rowNumber });
    const result = await ingestOperationalUpdate({
      clientId,
      sourceType,
      sourceActor,
      structured: { claims },
      artifact: {
        artifact_type: 'spreadsheet',
        filename,
        metadata: { sheet: sheetName, row: rowNumber },
        raw_content: row,
      },
      store,
      now,
      batchParent: { filename, sheetName },
    });
    recordResults.push(result);
    if (result.unresolved?.length || result.conflicts?.length) held += 1;
    else committed += 1;
  }

  const duplicates = recordResults.filter(r => r.duplicateReplay).length;
  const updated = recordResults.filter(r => r.entities_reconciled > 0).length;
  const created = recordResults.filter(r => r.entities_created > 0).length;

  return {
    recordsExamined: rows.length,
    recordResults,
    summary: {
      committed,
      held,
      updated,
      created,
      duplicates,
    },
  };
}

module.exports = {
  ingestSpreadsheet,
};
