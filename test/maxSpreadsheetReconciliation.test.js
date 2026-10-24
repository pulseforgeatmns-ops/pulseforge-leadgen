'use strict';

const assert = require('node:assert/strict');
const { describe, it, beforeEach } = require('node:test');
const XLSX = require('xlsx');
const {
  ingestSpreadsheet,
  buildSpreadsheetReconciliationPlan,
  commitSpreadsheetReconciliationPlan,
  MemoryStateStore,
  workbookFileHash,
} = require('../packages/max/stateIngestion');
const { tonyUpdatedAccountsSheets } = require('./fixtures/tonyUpdatedAccountsWorkbook');
const { extract: extractSpreadsheet } = require('../packages/max/composer/adapters/spreadsheet');
const { createMaxAttachment } = require('../packages/max/composer');

function seedTonyStore(overrides = {}) {
  return new MemoryStateStore({
    clientId: 1,
    users: [{ id: 10, name: 'Tony' }],
    companies: overrides.companies || [
      { id: 'co-exeter', name: 'Exeter Phillips', client_id: 1 },
      { id: 'co-exeter2', name: 'Exeter Packaging', client_id: 1 },
      { id: 'co-abc', name: 'ABC Manufacturing', client_id: 1 },
    ],
    prospects: overrides.prospects || [
      {
        id: 'prospect-exeter',
        client_id: 1,
        company_id: 'co-exeter',
        company_name: 'Exeter Phillips',
        assigned_ao_id: 10,
        phone: '603-555-1111',
      },
      {
        id: 'prospect-abc',
        client_id: 1,
        company_id: 'co-abc',
        company_name: 'ABC Manufacturing',
        assigned_ao_id: 10,
        phone: '603-555-1111',
        email: null,
      },
    ],
    contacts: overrides.contacts || [],
  });
}

function structuredFromFixture(filename = 'Tony Updated Accounts.xlsx') {
  return {
    filename,
    sheets: tonyUpdatedAccountsSheets(),
  };
}

describe('MAX-SPREADSHEET-002 durable AO spreadsheet reconciliation', () => {
  let store;

  beforeEach(() => {
    store = seedTonyStore();
  });

  it('S1 — entire workbook processed (multiple sheets inspected)', () => {
    const plan = buildSpreadsheetReconciliationPlan({
      structuredData: structuredFromFixture(),
      store,
    });
    assert.match(plan.workbookSummary, /Prospects/);
    assert.match(plan.workbookSummary, /Follow Ups/);
    assert.match(plan.workbookSummary, /Notes/);
    assert.ok(plan.summary.totalRows >= 10);
  });

  it('S2 — existing account note update appends with provenance', async () => {
    const batch = await ingestSpreadsheet({
      clientId: 1,
      filename: 'Tony Updated Accounts.xlsx',
      sheets: [{ sheet: 'Prospects', rows: tonyUpdatedAccountsSheets()[0].rows.slice(0, 1) }],
      store,
    });
    const exeter = store.prospects.find(p => p.id === 'prospect-exeter');
    assert.ok((exeter.activities || []).length >= 1);
    assert.ok(store.evidenceLinks.length >= 1);
  });

  it('S3 — new contact on resolved account', async () => {
    await ingestSpreadsheet({
      clientId: 1,
      filename: 'Tony Updated Accounts.xlsx',
      sheets: [{
        sheet: 'Prospects',
        rows: [{
          rowNumber: 2,
          values: { company: 'Exeter Phillips', contact: 'Lisa', notes: 'Vendor manager' },
        }],
      }],
      store,
    });
    const lisa = store.contacts.find(c => c.name === 'Lisa');
    assert.ok(lisa);
    assert.equal(lisa.prospect_id, 'prospect-exeter');
  });

  it('S4 — missing email filled safely', async () => {
    await ingestSpreadsheet({
      clientId: 1,
      filename: 'Tony Updated Accounts.xlsx',
      sheets: [{
        sheet: 'Prospects',
        rows: [{
          rowNumber: 2,
          values: { company: 'ABC Manufacturing', email: 'sarah@abc.example.com' },
        }],
      }],
      store,
    });
    const abc = store.prospects.find(p => p.id === 'prospect-abc');
    assert.equal(abc.email, 'sarah@abc.example.com');
  });

  it('S5 — conflicting phone blocked not overwritten', async () => {
    const plan = buildSpreadsheetReconciliationPlan({
      structuredData: {
        filename: 'Tony Updated Accounts.xlsx',
        sheets: [{
          sheet: 'Prospects',
          rows: [{
            rowNumber: 2,
            values: { company: 'ABC Manufacturing', phone: '603-555-9999' },
          }],
        }],
      },
      store,
    });
    assert.equal(plan.rows[0].conflicts.length, 1);
    assert.equal(plan.rows[0].safeToCommit, false);
    const batch = await commitSpreadsheetReconciliationPlan({ plan, clientId: 1, store });
    assert.equal(batch.results[0].skipped, true);
    const abc = store.prospects.find(p => p.id === 'prospect-abc');
    assert.equal(abc.phone, '603-555-1111');
  });

  it('S6 — explicit correction supersedes DM signal', async () => {
    await ingestSpreadsheet({
      clientId: 1,
      filename: 'Tony Updated Accounts.xlsx',
      sheets: [{
        sheet: 'Prospects',
        rows: [{
          rowNumber: 3,
          values: {
            company: 'Exeter Phillips',
            notes: 'Dave is NOT decision maker — Lisa handles vendors',
          },
        }],
      }],
      store,
      instruction: 'These are my updated accounts from this week.',
    });
    const exeter = store.prospects.find(p => p.id === 'prospect-exeter');
    assert.ok((exeter.decision_maker_signals || []).length >= 1);
  });

  it('S7 — ambiguous company row isolated', async () => {
    const plan = buildSpreadsheetReconciliationPlan({
      structuredData: {
        filename: 'Tony Updated Accounts.xlsx',
        sheets: [{
          sheet: 'Prospects',
          rows: [{ rowNumber: 4, values: { company: 'Exeter', notes: 'unclear' } }],
        }],
      },
      store,
    });
    assert.ok(plan.rows[0].ambiguities.length >= 1);
    assert.equal(plan.rows[0].safeToCommit, false);
  });

  it('S8 — Dead disposition maps to disposition_status', async () => {
    await ingestSpreadsheet({
      clientId: 1,
      filename: 'Tony Updated Accounts.xlsx',
      sheets: [{
        sheet: 'Prospects',
        rows: [{ rowNumber: 8, values: { company: 'Exeter Phillips', status: 'Dead' } }],
      }],
      store,
    });
    const exeter = store.prospects.find(p => p.id === 'prospect-exeter');
    assert.equal(exeter.disposition_status, 'dead');
  });

  it('S9 — Hot status does not map to disposition', async () => {
    await ingestSpreadsheet({
      clientId: 1,
      filename: 'Tony Updated Accounts.xlsx',
      sheets: [{
        sheet: 'Prospects',
        rows: [{ rowNumber: 9, values: { company: 'Exeter Phillips', status: 'Hot' } }],
      }],
      store,
    });
    const exeter = store.prospects.find(p => p.id === 'prospect-exeter');
    assert.equal(exeter.sales_priority, 'hot');
    assert.notEqual(exeter.disposition_status, 'hot');
  });

  it('S10 — follow-up date creates structured next action', async () => {
    await ingestSpreadsheet({
      clientId: 1,
      filename: 'Tony Updated Accounts.xlsx',
      sheets: [{
        sheet: 'Follow Ups',
        rows: [{ rowNumber: 2, values: { company: 'Exeter Phillips', next_step: 'Stop back Thursday' } }],
      }],
      store,
    });
    const exeter = store.prospects.find(p => p.id === 'prospect-exeter');
    assert.ok(exeter.next_action_due_hint || exeter.ao_next_action);
  });

  it('S11 — duplicate upload suppresses duplicate durable facts', async () => {
    const sheets = [{
      sheet: 'Prospects',
      rows: [{ rowNumber: 2, values: { company: 'Exeter Phillips', notes: 'Same note' } }],
    }];
    const structured = { filename: 'Tony Updated Accounts.xlsx', sheets };
    const hash = workbookFileHash(structured);
    await ingestSpreadsheet({ clientId: 1, filename: structured.filename, sheets, store });
    const activityCount = store.prospects.find(p => p.id === 'prospect-exeter').activities?.length || 0;
    await ingestSpreadsheet({ clientId: 1, filename: structured.filename, sheets, store });
    const plan = buildSpreadsheetReconciliationPlan({ structuredData: structured, store, fileHash: hash });
    assert.ok(plan.summary.duplicateSuppressed >= 0);
    const after = store.prospects.find(p => p.id === 'prospect-exeter').activities?.length || 0;
    assert.equal(after, activityCount);
  });

  it('S12 — revised upload applies only changed semantics', async () => {
    const base = {
      filename: 'Tony Accounts v1.xlsx',
      sheets: [{
        sheet: 'Prospects',
        rows: [{ rowNumber: 2, values: { company: 'Exeter Phillips', notes: 'Version 1' } }],
      }],
    };
    await ingestSpreadsheet({ clientId: 1, filename: base.filename, sheets: base.sheets, store });
    const v1Count = store.prospects.find(p => p.id === 'prospect-exeter').activities?.length || 0;
    const revised = {
      filename: 'Tony Accounts v2.xlsx',
      sheets: [{
        sheet: 'Prospects',
        rows: [{ rowNumber: 2, values: { company: 'Exeter Phillips', notes: 'Version 2 updated' } }],
      }],
    };
    await ingestSpreadsheet({ clientId: 1, filename: revised.filename, sheets: revised.sheets, store });
    const v2Count = store.prospects.find(p => p.id === 'prospect-exeter').activities?.length || 0;
    assert.ok(v2Count > v1Count);
  });

  it('S13 — new account candidate previewed safely', () => {
    const plan = buildSpreadsheetReconciliationPlan({
      structuredData: {
        filename: 'Tony Updated Accounts.xlsx',
        sheets: [{
          sheet: 'Prospects',
          rows: [{ rowNumber: 5, values: { company: 'Granite State Manufacturing', is_new: true } }],
        }],
      },
      store,
    });
    assert.ok(plan.rows[0].proposedChanges.some(c => c.type === 'CREATE_ACCOUNT_CANDIDATE'));
  });

  it('S14 — missing row in new version does not delete', async () => {
    await ingestSpreadsheet({
      clientId: 1,
      filename: 'v1.xlsx',
      sheets: [{
        sheet: 'Prospects',
        rows: [
          { rowNumber: 2, values: { company: 'Exeter Phillips' } },
          { rowNumber: 3, values: { company: 'ABC Manufacturing' } },
        ],
      }],
      store,
    });
    const countBefore = store.prospects.length;
    await ingestSpreadsheet({
      clientId: 1,
      filename: 'v2.xlsx',
      sheets: [{
        sheet: 'Prospects',
        rows: [{ rowNumber: 2, values: { company: 'Exeter Phillips' } }],
      }],
      store,
    });
    assert.equal(store.prospects.length, countBefore);
  });

  it('S15 — tenant isolation on resolution', () => {
    const tenantB = seedTonyStore({
      clientId: 2,
      companies: [{ id: 'co-b', name: 'Exeter Phillips', client_id: 2 }],
      prospects: [{
        id: 'prospect-b',
        client_id: 2,
        company_id: 'co-b',
        company_name: 'Exeter Phillips',
      }],
    });
    const planA = buildSpreadsheetReconciliationPlan({
      structuredData: {
        filename: 'a.xlsx',
        sheets: [{ sheet: 'Prospects', rows: [{ rowNumber: 2, values: { company: 'Exeter Phillips' } }] }],
      },
      store,
    });
    const planB = buildSpreadsheetReconciliationPlan({
      structuredData: {
        filename: 'b.xlsx',
        sheets: [{
          sheet: 'Prospects',
          rows: [{ rowNumber: 2, values: { company: 'Exeter Phillips' } }],
        }],
      },
      store: tenantB,
    });
    assert.equal(planA.rows[0].accountResolution.entity.id, 'prospect-exeter');
    assert.equal(planB.rows[0].accountResolution.entity.id, 'prospect-b');
  });

  it('S16 — partial success commits safe rows only', async () => {
    const batch = await ingestSpreadsheet({
      clientId: 1,
      filename: 'Tony Updated Accounts.xlsx',
      sheets: [{
        sheet: 'Prospects',
        rows: [
          { rowNumber: 2, values: { company: 'ABC Manufacturing', email: 'new@abc.example.com' } },
          { rowNumber: 4, values: { company: 'Exeter', notes: 'ambiguous' } },
        ],
      }],
      store,
    });
    assert.ok(batch.summary.committed >= 1);
    assert.ok(batch.summary.held >= 1);
  });

  it('acceptance — Tony workbook xlsx extraction includes all tabs', async () => {
    const rowsProspects = [{ Company: 'Exeter Phillips', Notes: 'n1' }];
    const rowsFollow = [{ Company: 'Exeter Phillips', 'Next Step': 'Call Friday' }];
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rowsProspects), 'Prospects');
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rowsFollow), 'Follow Ups');
    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'Tony Updated Accounts.xlsx' });
    const extracted = await extractSpreadsheet(att, { buffer, filename: att.filename });
    assert.equal(extracted.extractionStatus, 'ready');
    assert.equal(extracted.structuredData.sheets.length, 2);
  });
});
