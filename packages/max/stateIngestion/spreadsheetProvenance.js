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
  const columns = columnProvenance?.columns || {};
  const entry = columns[columnName] || Object.values(columns).find(column => column.canonical === columnName);
  const rawHeader = entry?.rawHeader || columnName;
  let cellRef = entry?.cellRef || null;
  if (rowNumber != null && rawHeader) {
    const colIdx = entry?.columnIndex ?? Object.keys(columns).indexOf(columnName);
    if (!cellRef && colIdx >= 0) {
      cellRef = `${columnIndexToLetter(colIdx)}${rowNumber}`;
    }
  }
  return {
    fileId,
    filename,
    sheetName,
    rowNumber,
    columnName: rawHeader || columnName,
    rawValue: entry && Object.prototype.hasOwnProperty.call(entry, 'rawValue') ? entry.rawValue : rawValue,
    cellRef,
    headerCellRef: entry?.headerCellRef || null,
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
