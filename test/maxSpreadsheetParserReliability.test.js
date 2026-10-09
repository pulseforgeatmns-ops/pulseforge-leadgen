'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const XLSX = require('xlsx');
const { extract } = require('../packages/max/composer/adapters/spreadsheet');
const { buildCellProvenance } = require('../packages/max/stateIngestion/spreadsheetProvenance');
const FIXTURE = path.join(__dirname, 'fixtures/anchor-cleaning-actual.xlsx');
const SHA = 'b1cddfa475f244e27d8c81a381976b30911b34f53ea6c449f868a54208ba6a75';
const workbook = (sheets, extras = {}) => {
  const wb = XLSX.utils.book_new();
  for (const [name, rows] of Object.entries(sheets)) XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(rows), name);
  Object.assign(wb, extras);
  return XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
};
const parse = buffer => extract({ filename: 'unrelated-name.xlsx' }, { buffer });
const csv = text => extract({ filename: 'input.csv' }, { buffer: Buffer.from(text) });

test('a legitimate company-only table keeps every company for identity review', async () => {
  const result = await parse(workbook({ Prospects: [['Company'], ['Exeter Phillips Academy']] }));
  assert.equal(result.extractionStatus, 'ready');
  assert.equal(result.structuredData.rowCount, 1);
  assert.equal(result.structuredData.sheets[0].rows[0].values.company, 'Exeter Phillips Academy');
});

test('actual immutable workbook: every source row and populated cell accounted for', async () => {
  const buffer = fs.readFileSync(FIXTURE);
  assert.equal(buffer.length, 17672);
  assert.equal(crypto.createHash('sha256').update(buffer).digest('hex'), SHA);
  const result = await parse(buffer);
  assert.equal(result.extractionStatus, 'ready');
  assert.equal(result.structuredData.sourceHash, SHA);
  assert.equal(result.structuredData.rowCount, 12);
  const sheet = result.structuredData.sheets[0];
  assert.equal(sheet.headerRow, 2);
  assert.equal(sheet.sourceRows.length, 44);
  assert.deepEqual(sheet.rows.map(r => r.rowNumber), [3,4,5,6,7,8,9,10,11,12,13,14]);
  assert.deepEqual(sheet.rows.map(r => r.values.company), ['Wipfli', 'New Hampshire Family Dentistry', 'Hodges Development Corporation', 'Phillips Exeter Academy', 'Southern New Hampshire University', 'Nash Family Investment Properties', 'Stephen Law Group Injury Lawyers', 'Buckley Law Offices, Manchester NH', 'Manchester Family Dentistry', 'Concord NH Post Office', 'TD Bank Concord, NH', 'Grappone Ford']);
  assert.equal(sheet.sourceRows[0].classification, 'metadata');
  assert.equal(sheet.sourceRows[1].classification, 'header');
  assert.deepEqual(sheet.sourceRows.filter(r => r.classification === 'legend').map(r => r.rowNumber), [37,39,42]);
  assert.ok(sheet.sourceRows.slice(14,36).every(r => r.classification === 'blank'));
  const rawSheet = XLSX.read(buffer, { type: 'buffer', cellDates: false, sheetStubs: true }).Sheets.Sheet1;
  for (const [address, cell] of Object.entries(rawSheet)) {
    if (address.startsWith('!') || cell.v == null) continue;
    const row = XLSX.utils.decode_cell(address).r;
    assert.equal(sheet.sourceRows[row].cells[address].rawValue, cell.v, address);
  }
  assert.equal(crypto.createHash('sha256').update(fs.readFileSync(FIXTURE)).digest('hex'), SHA);
});

test('actual workbook: all headers, date serials, operational facts, links and colors retained', async () => {
  const { structuredData } = await parse(fs.readFileSync(FIXTURE));
  const s = structuredData.sheets[0], rows = Object.fromEntries(s.rows.map(r => [r.rowNumber, r]));
  assert.deepEqual(Object.keys(rows[3].values), ['company','address','phone','email','website','contact','first_call_date','follow_up_call_date','first_visit_date','provider','follow_up_needed','notes']);
  assert.equal(rows[3].values.first_call_date, '2026-09-23');
  assert.equal(rows[3].raw['Date of 1st phone call'], 46288);
  assert.equal(rows[6].values.follow_up_call_date, '2026-10-02');
  assert.equal(rows[12].values.first_visit_date, '2026-09-25');
  assert.equal(rows[13].values.first_visit_date, '2026-09-24');
  assert.equal(rows[12].values.follow_up_needed, null);
  assert.equal(rows[14].values.notes, '10/2: ');
  assert.equal(rows[4].values.phone, '(603) 625-1877');
  assert.equal(rows[4].cells.C4.hyperlink.target, 'mailto:NHFDoffice@Nhfamilydentist.com');
  assert.equal(rows[3].cells.A3.style.fgColor.rgb, '92D050');
  assert.equal(rows[11].cells.A11.style.fgColor.rgb, 'FF0000');
  assert.ok(s.merges.includes('C42:C44'));
  const provenance = buildCellProvenance({ rowNumber: 3, columnName: 'first_call_date', columnProvenance: rows[3].columnProvenance });
  assert.equal(provenance.cellRef, 'G3');
  assert.equal(provenance.headerCellRef, 'G2');
  assert.equal(provenance.rawValue, 46288);
});

test('CSV quotes, commas, embedded newlines, empty records and source lines', async () => {
  const result = await csv('\uFEFFCompany,Notes\r\n"A, Inc","First line\r\n""quoted"" line"\r\n\r\nB,\r\n');
  assert.equal(result.extractionStatus, 'ready');
  const s = result.structuredData.sheets[0];
  assert.deepEqual(s.rows.map(r => r.rowNumber), [2,5]);
  assert.equal(s.rows[0].values.company, 'A, Inc');
  assert.equal(s.rows[0].values.notes, 'First line\r\n"quoted" line');
  assert.equal(s.sourceRows[2].classification, 'blank');
  for (const text of ['Company,Notes\nA,"oops', 'Company,Notes\nA,"x"oops', 'Company,Notes\nA,x"oops']) assert.equal((await csv(text)).extractionStatus, 'failed');
});

test('limits fail closed across sheets without partial normalized rows', async () => {
  const a = Array.from({ length: 251 }, (_, i) => ['A' + i, 'note']);
  const b = Array.from({ length: 250 }, (_, i) => ['B' + i, 'note']);
  const result = await parse(workbook({ First: [['Company','Notes'], ...a], Second: [['Company','Notes'], ...b] }));
  assert.equal(result.extractionStatus, 'failed');
  assert.equal(result.extractionEvidence.error, 'spreadsheet_row_limit');
  assert.equal(result.extractionEvidence.rowCount, 501);
  assert.equal(result.structuredData, undefined);
});

test('headers can be offset, repeated, and different per sheet; unknown cells never dropped', async () => {
  const result = await parse(workbook({ First: [['Title'], [], [null,'Company','Notes'], [null,'A','n','extra'], [], [null,'Company','Notes'], [null,'B','m']], Second: [['Business Name','Email'], ['C','c@example.test']] }));
  assert.equal(result.extractionStatus, 'ready');
  assert.equal(result.structuredData.rowCount, 3);
  assert.deepEqual(result.structuredData.sheets[0].rows.map(r => r.rowNumber), [4,7]);
  const row = result.structuredData.sheets[0].rows[0];
  assert.equal(row.values.unmapped_column_d, 'extra');
  assert.equal(buildCellProvenance({ rowNumber: 4, columnName: 'company', columnProvenance: row.columnProvenance }).cellRef, 'B4');
});

test('duplicate aliases, unidentified tables, and ambiguous post-legend records fail visibly', async () => {
  for (const rows of [[['Company','Notes','Note'], ['A','one','two']], [['Title'], ['unknown','other']], [['Company','Notes'], ['A','one'], [null,'Legend'], ['B','possibly business']]]) {
    const result = await parse(workbook({ Sheet: rows }));
    assert.equal(result.extractionStatus, 'failed');
    assert.equal(result.structuredData, undefined);
  }
});

test('hidden sheet records remain visible to reconciliation with explicit hidden evidence', async () => {
  const result = await parse(workbook({ Visible: [['Company','Notes'], ['A','one']], Hidden: [['Company','Notes'], ['B','two']] }, { Workbook: { Sheets: [{ name: 'Visible', Hidden: 0 }, { name: 'Hidden', Hidden: 1 }] } }));
  assert.equal(result.structuredData.rowCount, 2);
  assert.equal(result.structuredData.sheets[1].hidden, true);
  assert.equal(result.structuredData.sheets[1].rows[0].hidden, true);
});

test('Excel 1904 dates retain numeric source evidence and convert without timezone drift', async () => {
  const wb = XLSX.utils.book_new();
  const sheet = XLSX.utils.aoa_to_sheet([['Company','Date of 1st phone call'], ['A', 0]]);
  sheet.B2.z = 'mm/dd/yy';
  XLSX.utils.book_append_sheet(wb, sheet, 'Dates');
  wb.Workbook = { WBProps: { date1904: true } };
  const result = await parse(XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' }));
  assert.equal(result.structuredData.sheets[0].rows[0].values.first_call_date, '1904-01-01');
  assert.equal(result.structuredData.sheets[0].rows[0].raw['Date of 1st phone call'], 0);
});

test('source formula and spreadsheet errors remain inspectable without evaluation', async () => {
  const wb = XLSX.utils.book_new();
  const sheet = XLSX.utils.aoa_to_sheet([['Company','Notes'], ['A','cached']]);
  sheet.B2.f = 'HYPERLINK("https://example.invalid", "cached")';
  sheet.C2 = { t: 'e', v: 0x07 };
  sheet['!ref'] = 'A1:C2';
  XLSX.utils.book_append_sheet(wb, sheet, 'Formulas');
  const result = await parse(XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' }));
  assert.equal(result.extractionStatus, 'ready');
  const row = result.structuredData.sheets[0].rows[0];
  assert.equal(row.cells.B2.formula, sheet.B2.f);
  assert.equal(row.cells.C2.type, 'e');
  assert.equal(row.cells.C2.formattedText, '#DIV/0!');
});

test('special property names are source data, not object prototype mutations', async () => {
  const result = await csv('Company,Notes,__proto__,constructor\nA,n,raw prototype,raw constructor');
  assert.equal(result.extractionStatus, 'ready');
  const row = result.structuredData.sheets[0].rows[0];
  assert.equal(row.raw.__proto__, 'raw prototype');
  assert.equal(row.raw.constructor, 'raw constructor');
  assert.equal(row.values.constructor, 'raw constructor');
  assert.equal(Object.getPrototypeOf(row.values), null);
});
