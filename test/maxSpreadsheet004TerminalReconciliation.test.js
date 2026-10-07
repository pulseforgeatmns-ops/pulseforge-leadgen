'use strict';

const assert = require('node:assert/strict');
const { describe, it, beforeEach } = require('node:test');
const XLSX = require('xlsx');
const {
  submitComposerTurn,
  createMaxIngestionEnvelope,
  createMaxAttachment,
  resetComposerIdempotencyForTests,
  clearAttachmentStore,
} = require('../packages/max/composer');
const {
  buildSpreadsheetReconciliationPlan,
  MemoryStateStore,
} = require('../packages/max/stateIngestion');
const { SPREADSHEET_NO_OP_REASON } = require('../packages/max/stateIngestion/spreadsheetRowAudit');
const { ConversationMemory } = require('../packages/max/understanding');
const { safeGuidance } = require('../utils/aoMessageTemplates');
const {
  anchorCleaningProspectSheets,
  PRODUCTION_RECONCILE_MESSAGE,
} = require('./fixtures/anchorCleaningProspectWorkbook');

function seedStore(overrides = {}) {
  return new MemoryStateStore({
    clientId: 1,
    users: [{ id: 10, name: 'Tony' }],
    companies: overrides.companies || [
      { id: 'co-exeter', name: 'Exeter Phillips', client_id: 1 },
      { id: 'co-abc', name: 'ABC Manufacturing', client_id: 1 },
      { id: 'co-granite', name: 'Granite State Daycare', client_id: 1 },
    ],
    prospects: overrides.prospects || [
      {
        id: 'prospect-exeter',
        client_id: 1,
        company_id: 'co-exeter',
        company_name: 'Exeter Phillips',
        assigned_ao_id: 10,
        phone: '603-555-1111',
        activities: [{ content: 'Follow up this week' }],
      },
      {
        id: 'prospect-abc',
        client_id: 1,
        company_id: 'co-abc',
        company_name: 'ABC Manufacturing',
        assigned_ao_id: 10,
      },
      {
        id: 'prospect-granite',
        client_id: 1,
        company_id: 'co-granite',
        company_name: 'Granite State Daycare',
        assigned_ao_id: 10,
        is_hot: true,
        sales_priority: 'hot',
      },
    ],
    contacts: overrides.contacts || [
      { id: 'c-lisa', prospect_id: 'prospect-exeter', name: 'Lisa', phone: '603-555-1111' },
    ],
  });
}

function xlsxBufferFromSheets(sheets, filename = 'workbook.xlsx') {
  const wb = XLSX.utils.book_new();
  for (const sheet of sheets) {
    const rows = (sheet.rows || []).map(r => {
      const out = {};
      for (const [k, v] of Object.entries(r.values || {})) {
        const key = k.charAt(0).toUpperCase() + k.slice(1);
        out[key === 'Company' ? 'Company' : key] = v;
      }
      if (!out.Company && r.values?.company) out.Company = r.values.company;
      return out;
    });
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rows), sheet.sheet);
  }
  return XLSX.write(wb, { type: 'buffer', bookType: 'xlsx', Props: { Title: filename } });
}

function assertNoGenericCoaching(text = '') {
  assert.doesNotMatch(text, /stay curious/i);
  assert.doesNotMatch(text, /offer a walkthrough/i);
  assert.doesNotMatch(text, /listen and capture/i);
}

describe('MAX-SPREADSHEET-004 terminal spreadsheet turns', () => {
  beforeEach(() => {
    clearAttachmentStore();
    resetComposerIdempotencyForTests();
  });

  it('N1 — exact production case: 16 rows, row-by-row, no coaching', async () => {
    const store = seedStore();
    const att = createMaxAttachment({
      type: 'spreadsheet',
      filename: 'Anchor Cleaning Prospect List.xlsx',
    });
    const buffer = xlsxBufferFromSheets(anchorCleaningProspectSheets(), att.filename);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: PRODUCTION_RECONCILE_MESSAGE,
        attachments: [att],
        actor: { userId: 10, role: 'ao', aoId: 10 },
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
    });
    assert.equal(result.ok, true);
    assert.equal(result.commit, false);
    assert.equal(result.terminal_turn, true);
    assert.equal(result.reconciliation_plan.rows.length, 16);
    assert.ok(result.spreadsheet_reconciliation);
    assert.equal(result.spreadsheet_reconciliation.terminal_turn, true);
    assert.ok(result.operational_response);
    assert.match(result.operational_response, /Row-by-row:/i);
    assert.match(result.operational_response, /Nothing has been saved yet/i);
    assertNoGenericCoaching(result.operational_response);
    assertNoGenericCoaching(result.understanding_preview || '');
  });

  it('N2 — true CRM matches use ALREADY_MATCHES_CRM with fields compared', () => {
    const store = seedStore();
    const plan = buildSpreadsheetReconciliationPlan({
      structuredData: {
        filename: 'match.xlsx',
        sheets: [{
          sheet: 'Sheet1',
          rows: [{
            rowNumber: 2,
            values: { company: 'Exeter Phillips', phone: '603-555-1111', contact: 'Lisa' },
          }],
        }],
      },
      store,
    });
    assert.equal(plan.rows.length, 1);
    assert.equal(plan.rows[0].noOpReason, SPREADSHEET_NO_OP_REASON.ALREADY_MATCHES_CRM);
    assert.ok(plan.diagnostics.fields_compared > 0);
  });

  it('N3 — empty rows → EMPTY_ROW', () => {
    const store = seedStore();
    const plan = buildSpreadsheetReconciliationPlan({
      structuredData: {
        filename: 'empty.xlsx',
        sheets: [{ sheet: 'Sheet1', rows: [{ rowNumber: 2, values: {} }] }],
      },
      store,
    });
    assert.equal(plan.rows[0].noOpReason, SPREADSHEET_NO_OP_REASON.EMPTY_ROW);
  });

  it('N4 — unmapped operational column surfaced', () => {
    const store = seedStore();
    const plan = buildSpreadsheetReconciliationPlan({
      structuredData: {
        filename: 'unmapped.xlsx',
        sheets: [{
          sheet: 'Sheet1',
          rows: [{
            rowNumber: 6,
            values: { company: 'ABC Manufacturing', 'Progress Made': 'Met facilities lead' },
          }],
        }],
      },
      store,
    });
    assert.ok(plan.rows[0].unmappedColumns.includes('Progress Made'));
    const response = require('../packages/max/stateIngestion/spreadsheetReconciliation')
      .formatSpreadsheetOperationalResponse(plan, { instruction: 'row-by-row' });
    assert.match(response, /Progress Made.*not currently mapped/i);
  });

  it('N5 — duplicate prior import not labeled as CRM equality', () => {
    const store = seedStore();
    const memory = new ConversationMemory({ conversationId: 'conv-n5' });
    const structuredData = {
      filename: 'dup.xlsx',
      sheets: [{
        sheet: 'Sheet1',
        rows: [{ rowNumber: 10, values: { company: 'Granite State Daycare' } }],
      }],
    };
    const fileHash = 'fixed-hash';
    const first = buildSpreadsheetReconciliationPlan({ structuredData, store, fileHash, memory });
    memory.recordPendingSpreadsheetWorkbook({
      fileHash,
      committedRowKeys: first.rows.map(r => r.rowKey),
    });
    const second = buildSpreadsheetReconciliationPlan({
      structuredData,
      store,
      fileHash,
      priorFileHash: fileHash,
      memory,
    });
    assert.notEqual(first.rows[0].noOpReason, SPREADSHEET_NO_OP_REASON.DUPLICATE_PRIOR_IMPORT);
    assert.equal(second.rows[0].noOpReason, SPREADSHEET_NO_OP_REASON.DUPLICATE_PRIOR_IMPORT);
  });

  it('N6 — note already exists is explicit', () => {
    const store = seedStore();
    const plan = buildSpreadsheetReconciliationPlan({
      structuredData: {
        filename: 'note.xlsx',
        sheets: [{
          sheet: 'Sheet1',
          rows: [{ rowNumber: 2, values: { company: 'Exeter Phillips', notes: 'Follow up this week' } }],
        }],
      },
      store,
    });
    assert.match(plan.rows[0].noteAudit || '', /already exists/i);
    assert.equal(plan.rows[0].proposedChanges.filter(c => c.type === 'ADD_NOTE').length, 0);
  });

  it('N7 — contact already matches', () => {
    const store = seedStore();
    const plan = buildSpreadsheetReconciliationPlan({
      structuredData: {
        filename: 'contact.xlsx',
        sheets: [{
          sheet: 'Sheet1',
          rows: [{ rowNumber: 2, values: { company: 'Exeter Phillips', contact: 'Lisa' } }],
        }],
      },
      store,
    });
    assert.match(plan.rows[0].contactAudit || '', /already matches/i);
  });

  it('N8 — follow-up without date explains no scheduled task', () => {
    const store = seedStore();
    const plan = buildSpreadsheetReconciliationPlan({
      structuredData: {
        filename: 'follow.xlsx',
        sheets: [{
          sheet: 'Sheet1',
          rows: [{ rowNumber: 2, values: { company: 'ABC Manufacturing', next_step: 'Waiting for callback' } }],
        }],
      },
      store,
    });
    const text = require('../packages/max/stateIngestion/spreadsheetReconciliation')
      .formatSpreadsheetOperationalResponse(plan, { instruction: 'row-by-row' });
    assert.match(text, /no scheduled action created because no date\/time was supplied/i);
  });

  it('N9 — row-level request lists all rows', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'Anchor Cleaning Prospect List.xlsx' });
    const buffer = xlsxBufferFromSheets(anchorCleaningProspectSheets(), att.filename);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: PRODUCTION_RECONCILE_MESSAGE,
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
    });
    const rowMentions = (result.operational_response.match(/Sheet1 row \d+/g) || []).length;
    assert.equal(rowMentions, 16);
  });

  it('N10 — aggregate-only request may stay concise', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'solo.xlsx' });
    const sheets = anchorCleaningProspectSheets().map(s => ({ ...s, rows: s.rows.slice(0, 4) }));
    const buffer = xlsxBufferFromSheets(sheets, att.filename);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: 'Review all rows in this spreadsheet before you save anything.',
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
    });
    assert.match(result.operational_response, /Summary:/);
    assert.ok(result.reconciliation_plan.rows.length === 4);
  });

  it('N11 — commit reuses pending reconciliation plan', async () => {
    const store = seedStore();
    const memory = new ConversationMemory({ conversationId: 'conv-n11' });
    const sheets = anchorCleaningProspectSheets().slice(0, 1).map(s => ({ ...s, rows: s.rows.slice(0, 2) }));
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'Anchor Cleaning Prospect List.xlsx' });
    const buffer = xlsxBufferFromSheets(sheets, att.filename);
    const preview = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        conversationId: 'conv-n11',
        text: 'Review all rows before you save anything.',
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
      conversationMemory: memory,
    });
    const planId = preview.reconciliation_plan.reconciliationPlanId;
    const pending = memory.getPendingSpreadsheetWorkbook();
    assert.ok(pending.reconciliationPlan);
    const commit = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        conversationId: 'conv-n11',
        id: 'env-n11-commit',
        text: 'Save the safe updates.',
      }),
      store,
      conversationMemory: memory,
      confirm: true,
    });
    assert.equal(commit.reconciliation_plan.reconciliationPlanId, planId);
    assert.equal(
      commit.reconciliation_plan.rows[0].noOpReason,
      preview.reconciliation_plan.rows[0].noOpReason,
    );
  });

  it('N12 — generic coaching alone would fail if appended', async () => {
    const coaching = safeGuidance('random account chatter').guidance;
    assert.match(coaching, /stay curious/i);
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'Anchor Cleaning Prospect List.xlsx' });
    const buffer = xlsxBufferFromSheets(anchorCleaningProspectSheets(), att.filename);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: PRODUCTION_RECONCILE_MESSAGE,
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
    });
    assertNoGenericCoaching(`${result.operational_response}\n${result.understanding_preview || ''}`);
  });
});
