'use strict';

const { columnIndexToLetter } = require('../composer/cellRef');

function buildCellProvenance({
  fileId = null,
  filename = null,
  sheetName = null,
  rowNumber = null,
  columnName = null,
  rawValue = null,
  columnProvenance = null,
}) {
  const rawHeader = columnProvenance?.columns?.[columnName]?.rawHeader || columnName;
  let cellRef = null;
  if (rowNumber != null && rawHeader) {
    const colIdx = Object.keys(columnProvenance?.columns || {}).indexOf(columnName);
    if (colIdx >= 0) {
      cellRef = `${columnIndexToLetter(colIdx)}${rowNumber}`;
    }
  }
  return {
    fileId,
    filename,
    sheetName,
    rowNumber,
    columnName: rawHeader || columnName,
    rawValue,
    cellRef,
  };
}

function mergeSourceRecord(base = {}, provenance = {}) {
  return {
    ...base,
    file_id: provenance.fileId || base.file_id || null,
    filename: provenance.filename || base.filename || null,
    sheet: provenance.sheetName || base.sheet || null,
    row: provenance.rowNumber ?? base.row ?? null,
    column: provenance.columnName || base.column || null,
    cell: provenance.cellRef || base.cell || null,
  };
}

module.exports = {
  buildCellProvenance,
  mergeSourceRecord,
};
