'use strict';

const XLSX = require('xlsx');
const { mapRowHeaders } = require('../headerMap');
const { LIMITS } = require('../limits');

const SPREADSHEET_MIMES = new Set([
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'application/vnd.ms-excel',
  'text/csv',
  'application/csv',
]);

function supports(attachment = {}) {
  const mime = String(attachment.mimeType || '').toLowerCase();
  const name = String(attachment.filename || '').toLowerCase();
  if (SPREADSHEET_MIMES.has(mime)) return true;
  return /\.(xlsx|xls|csv)$/i.test(name);
}

function parseCsvBuffer(buffer) {
  const text = buffer.toString('utf8');
  const lines = text.split(/\r?\n/).filter(l => l.trim().length);
  if (!lines.length) return [{ name: 'Sheet1', rows: [] }];
  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = lines[i].split(',').map(c => c.trim().replace(/^"|"$/g, ''));
    const raw = {};
    headers.forEach((h, idx) => {
      raw[h] = cols[idx] ?? '';
    });
    rows.push({ rowNumber: i + 1, raw });
  }
  return [{ name: 'Sheet1', rows }];
}

function parseWorkbook(buffer, filename) {
  const lower = String(filename || '').toLowerCase();
  if (lower.endsWith('.csv')) {
    return parseCsvBuffer(buffer);
  }
  const workbook = XLSX.read(buffer, { type: 'buffer', cellDates: true });
  const sheets = [];
  for (const sheetName of workbook.SheetNames) {
    const meta = workbook.Workbook?.Sheets?.find(s => s.name === sheetName);
    if (meta && (meta.Hidden === 1 || meta.Hidden === 2)) continue;
    const sheet = workbook.Sheets[sheetName];
    const json = XLSX.utils.sheet_to_json(sheet, { defval: null, raw: false });
    const rows = json.map((raw, idx) => ({
      rowNumber: idx + 2,
      raw,
    }));
    sheets.push({ name: sheetName, rows });
  }
  return sheets;
}

async function extract(attachment, { buffer, filename } = {}) {
  if (!buffer) {
    return {
      extractionStatus: 'failed',
      extractionEvidence: { error: 'missing_buffer' },
    };
  }
  try {
    const sheets = parseWorkbook(buffer, filename || attachment.filename);
    if (!sheets.length || sheets.every(s => !(s.rows || []).length)) {
      return {
        extractionStatus: 'failed',
        extractionEvidence: { error: 'empty_spreadsheet', message: 'No rows found in spreadsheet.' },
      };
    }
    let totalRows = 0;
    const normalizedSheets = [];
    for (const sheet of sheets) {
      const rows = [];
      for (const row of sheet.rows) {
        if (totalRows >= LIMITS.maxSpreadsheetRows) break;
        const mapped = mapRowHeaders(row.raw);
        rows.push({
          rowNumber: row.rowNumber,
          values: mapped.values,
          raw: row.raw,
          columnProvenance: mapped.provenance,
        });
        totalRows += 1;
      }
      normalizedSheets.push({
        sheet: sheet.name,
        headers: rows[0] ? Object.keys(rows[0].raw || {}) : [],
        rows,
      });
    }
    return {
      structuredData: {
        filename: filename || attachment.filename,
        sheets: normalizedSheets,
        rowCount: totalRows,
      },
      provenance: {
        filename: filename || attachment.filename,
        adapter: 'spreadsheet',
        sheets: normalizedSheets.map(s => ({ sheet: s.sheet, rowCount: s.rows.length })),
      },
      extractionStatus: 'ready',
    };
  } catch (err) {
    return {
      extractionStatus: 'failed',
      extractionEvidence: { error: 'spreadsheet_parse_failed', message: err.message },
    };
  }
}

module.exports = {
  supports,
  extract,
};
