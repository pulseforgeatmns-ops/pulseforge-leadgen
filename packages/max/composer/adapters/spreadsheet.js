'use strict';

const crypto = require('node:crypto');
const XLSX = require('xlsx');
const { mapRowHeaders, canonicalHeader } = require('../headerMap');
const { LIMITS } = require('../limits');
const SPREADSHEET_MIMES = new Set(['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel', 'text/csv', 'application/csv']);
const MAX_SOURCE_ROWS = 10000;
const MAX_SOURCE_COLUMNS = 256;
const nonempty = value => value !== null && value !== undefined && String(value).trim() !== '';

function supports(attachment = {}) {
  return SPREADSHEET_MIMES.has(String(attachment.mimeType || '').toLowerCase()) || /\.(xlsx|xls|csv)$/i.test(attachment.filename || '');
}

// RFC 4180 quoting. Row numbers retain each record's physical starting line.
function csvRows(buffer) {
  const text = buffer.toString('utf8').replace(/^\uFEFF/, '');
  const rows = [];
  let values = [], value = '', quoted = false, closed = false, line = 1, start = 1;
  const field = () => { values.push(value); value = ''; closed = false; };
  const record = () => { field(); rows.push({ rowNumber: start, values }); values = []; };
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (quoted) {
      if (ch === '"') {
        if (text[i + 1] === '"') { value += '"'; i++; }
        else { quoted = false; closed = true; }
      } else { value += ch; if (ch === '\n' || (ch === '\r' && text[i + 1] !== '\n')) line++; }
    } else if (ch === '"') {
      if (value.length || closed) throw new Error('Malformed CSV quoting');
      quoted = true;
    } else if (ch === ',') field();
    else if (ch === '\r' || ch === '\n') {
      record();
      if (ch === '\r' && text[i + 1] === '\n') i++;
      line++; start = line;
    } else {
      if (closed) throw new Error('Unexpected characters after quoted CSV field');
      value += ch;
    }
    if (rows.length > MAX_SOURCE_ROWS || values.length > MAX_SOURCE_COLUMNS) throw new Error('Spreadsheet source dimensions exceed limits');
  }
  if (quoted) throw new Error('Unterminated quoted CSV field');
  if (value.length || values.length || closed) record();
  return rows.map(row => ({ rowNumber: row.rowNumber, cells: Object.fromEntries(row.values.map((v, c) => {
    const cellRef = XLSX.utils.encode_cell({ r: row.rowNumber - 1, c });
    return [cellRef, { cellRef, columnIndex: c, rawValue: v, formattedText: v, type: 's', normalizedValue: v }];
  })) }));
}

function dateValue(cell, date1904) {
  if (cell.t !== 'n' || !cell.z || !XLSX.SSF.is_date(cell.z)) return cell.v ?? null;
  const date = XLSX.SSF.parse_date_code(cell.v, { date1904 });
  if (!date || date.y < 1 || (date.y === 1900 && date.m === 2 && date.d === 29)) return cell.v;
  return [date.y, date.m, date.d].map((part, i) => String(part).padStart(i ? 2 : 4, '0')).join('-');
}

function workbookSheets(buffer) {
  const workbook = XLSX.read(buffer, { type: 'buffer', cellDates: false, cellNF: true, cellStyles: true, sheetStubs: true });
  const date1904 = !!workbook.Workbook?.WBProps?.date1904;
  return workbook.SheetNames.map(name => {
    const sheet = workbook.Sheets[name];
    const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1');
    if (range.e.r >= MAX_SOURCE_ROWS || range.e.c >= MAX_SOURCE_COLUMNS) throw new Error('Spreadsheet source dimensions exceed limits');
    const rows = [];
    for (let r = 0; r <= range.e.r; r++) {
      const cells = {};
      for (let c = 0; c <= range.e.c; c++) {
        const cellRef = XLSX.utils.encode_cell({ r, c });
        const cell = sheet[cellRef];
        if (!cell) continue;
        cells[cellRef] = {
          cellRef, columnIndex: c, rawValue: cell.v ?? null,
          formattedText: cell.w ?? (cell.v == null ? '' : String(cell.v)),
          type: cell.t, numberFormat: cell.z || null, formula: cell.f || null,
          hyperlink: cell.l ? { target: cell.l.Target, tooltip: cell.l.Tooltip || null } : null,
          style: cell.s || null, comments: cell.c || null, normalizedValue: dateValue(cell, date1904),
        };
      }
      rows.push({ rowNumber: r + 1, hidden: !!sheet['!rows']?.[r]?.hidden, cells });
    }
    return { name, sourceRows: rows, dateSystem: date1904 ? '1904' : '1900', hidden: !!workbook.Workbook?.Sheets?.find(s => s.name === name)?.Hidden, merges: (sheet['!merges'] || []).map(XLSX.utils.encode_range) };
  });
}

function headerCandidate(row) {
  const populated = Object.values(row.cells).filter(cell => nonempty(cell.rawValue));
  const fields = populated.map(cell => canonicalHeader(cell.rawValue)).filter(Boolean);
  return (fields.includes('company') || fields.includes('contact')) && (fields.length >= 2 || (populated.length === 1 && fields[0] === 'company'));
}

function normalizeSheet(sheet) {
  const sourceRows = sheet.sourceRows;
  const headerIndex = sourceRows.findIndex(headerCandidate);
  const populated = sourceRows.some(row => Object.values(row.cells).some(cell => nonempty(cell.rawValue) || cell.formula));
  if (headerIndex < 0 && populated) throw new Error('No unambiguous business table header found in sheet ' + sheet.name);
  let headers = [], inLegend = false;
  const rows = [];
  for (let i = 0; i < sourceRows.length; i++) {
    const row = sourceRows[i];
    const populatedCells = Object.values(row.cells).filter(cell => nonempty(cell.rawValue) || cell.formula);
    if (!populatedCells.length) row.classification = 'blank';
    else if (i < headerIndex) row.classification = 'metadata';
    else if (headerCandidate(row)) {
      row.classification = 'header'; inLegend = false;
      const seen = new Set();
      headers = Object.values(row.cells).filter(cell => nonempty(cell.rawValue)).map(cell => {
        const rawHeader = String(cell.rawValue);
        if (seen.has(rawHeader)) throw new Error('Duplicate header ' + rawHeader + ' in ' + sheet.name);
        seen.add(rawHeader);
        return { key: rawHeader, columnIndex: cell.columnIndex, headerCellRef: cell.cellRef };
      });
      const canonical = headers.map(h => canonicalHeader(h.key)).filter(Boolean);
      if (new Set(canonical).size !== canonical.length) throw new Error('Conflicting headers in ' + sheet.name);
    } else if (populatedCells.length === 1 && /^(legend|key)$/i.test(String(populatedCells[0].rawValue).trim())) {
      inLegend = true; row.classification = 'legend';
    } else if (inLegend) {
      // Do not silently discard a possible business record after a legend.
      const identity = headers.filter(h => ['company', 'contact'].includes(canonicalHeader(h.key)));
      if (identity.some(h => nonempty(row.cells[XLSX.utils.encode_cell({ r: row.rowNumber - 1, c: h.columnIndex })]?.rawValue))) throw new Error('Ambiguous business row after legend in ' + sheet.name + ' row ' + row.rowNumber);
      row.classification = 'legend';
    } else {
      row.classification = 'data';
      const raw = Object.create(null), normalized = Object.create(null), columns = Object.create(null);
      const maxColumn = Math.max(...headers.map(h => h.columnIndex), ...Object.values(row.cells).map(c => c.columnIndex));
      for (let c = 0; c <= maxColumn; c++) {
        const header = headers.find(h => h.columnIndex === c);
        const cellRef = XLSX.utils.encode_cell({ r: row.rowNumber - 1, c });
        const cell = row.cells[cellRef];
        const key = header?.key || 'unmapped_column_' + XLSX.utils.encode_col(c);
        raw[key] = cell?.rawValue ?? null;
        normalized[key] = cell?.normalizedValue ?? null;
        columns[key] = { rawHeader: header?.key || null, headerCellRef: header?.headerCellRef || null, cellRef, columnIndex: c, rawValue: cell?.rawValue ?? null, ...cell };
      }
      const mapped = mapRowHeaders(normalized, columns);
      rows.push({ rowNumber: row.rowNumber, raw, values: mapped.values, cells: row.cells, hidden: row.hidden || sheet.hidden, columnProvenance: mapped.provenance });
    }
  }
  return { sheet: sheet.name, hidden: sheet.hidden || false, headerRow: headerIndex < 0 ? null : sourceRows[headerIndex].rowNumber, headers: headers.map(h => h.key), dateSystem: sheet.dateSystem || null, merges: sheet.merges || [], rows, sourceRows };
}

async function extract(attachment = {}, { buffer, filename } = {}) {
  if (!buffer) return { extractionStatus: 'failed', extractionEvidence: { error: 'missing_buffer' } };
  try {
    if (buffer.length > LIMITS.maxFileBytes) throw new Error('Spreadsheet exceeds the file size limit');
    const name = filename || attachment.filename;
    const isCsv = /\.csv$/i.test(name || '') || ['text/csv', 'application/csv'].includes(attachment.mimeType);
    const sheets = (isCsv ? [{ name: 'Sheet1', sourceRows: csvRows(buffer) }] : workbookSheets(buffer)).map(normalizeSheet);
    const rowCount = sheets.reduce((n, sheet) => n + sheet.rows.length, 0);
    if (!rowCount) return { extractionStatus: 'failed', extractionEvidence: { error: 'empty_spreadsheet', message: 'No business rows found in spreadsheet.' } };
    if (rowCount > LIMITS.maxSpreadsheetRows) return { extractionStatus: 'failed', extractionEvidence: { error: 'spreadsheet_row_limit', rowCount, limit: LIMITS.maxSpreadsheetRows, message: 'Workbook exceeds the row limit; no partial extraction is available.' } };
    const sourceHash = crypto.createHash('sha256').update(buffer).digest('hex');
    return { structuredData: { filename: name, sourceHash, sheets, rowCount }, provenance: { filename: name, sourceHash, adapter: 'spreadsheet', sheets: sheets.map(s => ({ sheet: s.sheet, rowCount: s.rows.length, headerRow: s.headerRow, sourceRowCount: s.sourceRows.length })) }, extractionStatus: 'ready' };
  } catch (err) {
    return { extractionStatus: 'failed', extractionEvidence: { error: 'spreadsheet_parse_failed', message: err.message } };
  }
}

module.exports = { supports, extract };
