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
  ATTACHMENT_TASK_INTENT,
  detectAttachmentTaskIntent,
} = require('../packages/max/composer');
const { MemoryStateStore } = require('../packages/max/stateIngestion');
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
      { id: 'co-exeter2', name: 'Exeter Packaging', client_id: 1 },
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
      },
      {
        id: 'prospect-abc',
        client_id: 1,
        company_id: 'co-abc',
        company_name: 'ABC Manufacturing',
        assigned_ao_id: 10,
        email: null,
      },
      {
        id: 'prospect-granite',
        client_id: 1,
        company_id: 'co-granite',
        company_name: 'Granite State Daycare',
        assigned_ao_id: 10,
        phone: '603-555-2222',
      },
    ],
    contacts: overrides.contacts || [],
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

describe('MAX-SPREADSHEET-003 attachment intent routing', () => {
  beforeEach(() => {
    clearAttachmentStore();
    resetComposerIdempotencyForTests();
  });

  it('R1 — production regression: 16-row workbook + explicit reconciliation message', async () => {
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
    assert.ok(result.reconciliation_plan);
    assert.equal(result.reconciliation_plan.summary.totalRows, 16);
    assert.match(result.attachment_task_intent, /SPREADSHEET_(PREVIEW|RECONCILE)/);
    assert.ok(result.operational_response);
    assert.match(result.operational_response, /Nothing has been saved yet/i);
    assert.doesNotMatch(result.operational_response || '', /stay curious/i);
    const genericOnly = safeGuidance(PRODUCTION_RECONCILE_MESSAGE).guidance;
    assert.notEqual(result.operational_response.trim(), genericOnly.trim());
  });

  it('R2 — AO brief context does not override explicit spreadsheet command', async () => {
    const store = seedStore();
    const memory = new ConversationMemory({ conversationId: 'conv-r2' });
    memory.recordTurn({
      inputId: 'prior-brief',
      text: 'Give me the AO brief for Realty Management Partners',
      situationModel: {
        threads: [{ accountName: 'Realty Management Partners', entities: [{ kind: 'account', name: 'Realty Management Partners' }] }],
        recommendedNextActions: [{ summary: 'Stay curious about the account.' }],
      },
    });
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'Anchor Cleaning Prospect List.xlsx' });
    const buffer = xlsxBufferFromSheets(anchorCleaningProspectSheets(), att.filename);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        conversationId: 'conv-r2',
        text: PRODUCTION_RECONCILE_MESSAGE,
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
      conversationMemory: memory,
    });
    assert.ok(result.reconciliation_plan);
    assert.equal(result.reconciliation_plan.summary.totalRows, 16);
    assert.doesNotMatch(result.operational_response || '', /stay curious/i);
  });

  it('R3 — "before you save" forces preview-only commit false', async () => {
    const intent = detectAttachmentTaskIntent({
      text: 'Review this spreadsheet before you save anything.',
      attachments: [{ type: 'spreadsheet', extractionStatus: 'ready', structuredData: { sheets: [{ sheet: 'Sheet1', rows: [{ rowNumber: 2, values: {} }] }] } }],
    });
    assert.equal(intent.previewOnly, true);
    assert.equal(intent.commit, false);
  });

  it('R4 — "Save safe updates" commits existing plan without re-upload', async () => {
    const store = seedStore();
    const memory = new ConversationMemory({ conversationId: 'conv-r4' });
    const sheets = anchorCleaningProspectSheets().slice(0, 1).map(s => ({
      ...s,
      rows: s.rows.slice(0, 2),
    }));
    const structuredData = { filename: 'Anchor Cleaning Prospect List.xlsx', sheets };
    memory.recordPendingSpreadsheetWorkbook({
      workbookId: 'wb-r4',
      attachmentId: 'att-r4',
      structuredData,
      fileHash: 'hash-r4',
      totalRows: 2,
    });
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        conversationId: 'conv-r4',
        id: 'env-r4-commit',
        text: 'Save those safe updates.',
      }),
      store,
      conversationMemory: memory,
      confirm: true,
    });
    assert.equal(result.ok, true);
    assert.equal(result.attachment_task_intent, ATTACHMENT_TASK_INTENT.SPREADSHEET_COMMIT);
    assert.ok(result.reconciliation_plan);
  });

  it('R5 — zero-change workbook returns auditable explanation', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'no-op.xlsx' });
    const sheets = [{
      sheet: 'Sheet1',
      rows: [{
        rowNumber: 2,
        values: { company: 'Exeter Phillips' },
      }],
    }];
    const buffer = xlsxBufferFromSheets(sheets, att.filename);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: 'Review all rows and compare against PulseForge before you save anything.',
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
    });
    assert.match(result.operational_response, /reviewed all|Summary:|already match CRM/i);
  });

  it('R6 — multiple spreadsheets require narrow clarification', async () => {
    const intent = detectAttachmentTaskIntent({
      text: 'Review this spreadsheet and reconcile accounts.',
      attachments: [
        { id: 'a1', type: 'spreadsheet', filename: 'a.xlsx', extractionStatus: 'ready', structuredData: { sheets: [{ sheet: 'Sheet1', rows: [{ rowNumber: 2, values: {} }] }] } },
        { id: 'a2', type: 'spreadsheet', filename: 'b.xlsx', extractionStatus: 'ready', structuredData: { sheets: [{ sheet: 'Sheet1', rows: [{ rowNumber: 2, values: {} }] }] } },
      ],
    });
    assert.ok(intent.clarification);
    assert.match(intent.clarification, /multiple spreadsheets/i);
  });

  it('R7 — "Review this" binds single attached spreadsheet', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'solo.xlsx' });
    const buffer = xlsxBufferFromSheets(anchorCleaningProspectSheets().map(s => ({ ...s, rows: s.rows.slice(0, 1) })), att.filename);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: 'Review this.',
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
    });
    assert.ok(result.reconciliation_plan);
    assert.equal(result.reconciliation_plan.summary.totalRows, 1);
  });

  it('R8 — mixed reconcile + prioritization still creates plan first', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'Anchor Cleaning Prospect List.xlsx' });
    const buffer = xlsxBufferFromSheets(anchorCleaningProspectSheets(), att.filename);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: `${PRODUCTION_RECONCILE_MESSAGE} Also tell me which accounts Tony should prioritize tomorrow.`,
        attachments: [att],
        actor: { userId: 10, role: 'ao', aoId: 10 },
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
    });
    assert.ok(result.reconciliation_plan);
    assert.equal(result.reconciliation_plan.summary.totalRows, 16);
  });

  it('R9 — "Review all 16 rows" guarantees totalRows = 16', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'Anchor Cleaning Prospect List.xlsx' });
    const buffer = xlsxBufferFromSheets(anchorCleaningProspectSheets(), att.filename);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        text: 'Review all 16 rows in this spreadsheet before you save anything.',
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
    });
    assert.equal(result.reconciliation_plan.summary.totalRows, 16);
  });

  it('R10 — active prospect context does not limit workbook scope', async () => {
    const store = seedStore();
    const memory = new ConversationMemory({ conversationId: 'conv-r10' });
    memory.recordTurn({
      inputId: 'ctx',
      text: 'Realty Management Partners',
      situationModel: {
        threads: [{ accountName: 'Realty Management Partners', entities: [{ kind: 'account', name: 'Realty Management Partners' }] }],
      },
    });
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'Anchor Cleaning Prospect List.xlsx' });
    const buffer = xlsxBufferFromSheets(anchorCleaningProspectSheets(), att.filename);
    const result = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        conversationId: 'conv-r10',
        text: 'Review all 16 rows.',
        attachments: [att],
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
      conversationMemory: memory,
    });
    assert.equal(result.reconciliation_plan.summary.totalRows, 16);
  });

  it('R11 — impersonation preserves effective actor on pending workbook context', async () => {
    const store = seedStore();
    const att = createMaxAttachment({ type: 'spreadsheet', filename: 'Anchor Cleaning Prospect List.xlsx' });
    const buffer = xlsxBufferFromSheets(anchorCleaningProspectSheets().map(s => ({ ...s, rows: s.rows.slice(0, 3) })), att.filename);
    const preview = await submitComposerTurn({
      clientId: 1,
      envelope: createMaxIngestionEnvelope({
        tenantId: '1',
        conversationId: 'conv-r11',
        text: 'Review all rows before you save anything.',
        attachments: [att],
        actor: {
          userId: 10,
          role: 'ao',
          aoId: 10,
          authenticatedUserId: 1,
          impersonated: true,
        },
      }),
      attachmentInputs: [{ id: att.id, buffer }],
      store,
      conversationMemory: new ConversationMemory({ conversationId: 'conv-r11' }),
    });
    const pending = preview.conversation_memory.getPendingSpreadsheetWorkbook();
    assert.equal(pending.effectiveActor.aoId, 10);
    assert.equal(pending.effectiveActor.authenticatedUserId, 1);
  });

  it('R12 — generic coaching alone fails operational spreadsheet command', async () => {
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
    assert.ok(result.reconciliation_plan);
    assert.notEqual((result.operational_response || '').trim(), coaching.trim());
  });
});
