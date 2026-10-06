'use strict';

const crypto = require('crypto');
const { stableHash } = require('./fingerprints');
const { normalizeText } = require('./claimParser');
const { parseSpreadsheetRow } = require('./claimParser');
const { resolveClaim } = require('./entityResolver');
const { CLAIM_TYPES, RESOLUTION } = require('./types');
const { interpretSpreadsheetStatus } = require('./spreadsheetStatus');
const { mergeSourceRecord, buildCellProvenance } = require('./spreadsheetProvenance');
const { ingestOperationalUpdate } = require('./pipeline');
const { emptyTelemetry, bump } = require('./telemetry');
const { synthesizeRowUnderstandingText } = require('../composer/rowText');
const { interpretConversationalInput } = require('../understanding');
const { CHANGE_TYPES, ROW_CLASS } = require('./spreadsheetChangeTypes');

function normalizePhone(value) {
  return String(value || '').replace(/\D/g, '');
}

function classifyIncomingField(existing, incoming, { fieldKind = 'generic' } = {}) {
  const ex = normalizeText(existing);
  const inc = normalizeText(incoming);
  if (!inc) return 'empty/no-op';
  if (!ex) return 'new';
  if (fieldKind === 'phone') {
    if (normalizePhone(ex) === normalizePhone(inc)) return 'same';
    if (/correct phone|wrong number|updated phone/i.test(String(incoming))) return 'correction';
    return 'conflict';
  }
  if (fieldKind === 'email') {
    if (ex.toLowerCase() === inc.toLowerCase()) return 'same';
    return 'conflict';
  }
  if (ex.toLowerCase() === inc.toLowerCase()) return 'same';
  return 'correction';
}

function rowSemanticHash(rowValues = {}) {
  return stableHash([JSON.stringify(rowValues)]);
}

function buildWorkbookSummary({ filename, sheets = [] }) {
  const lines = [`Workbook: ${filename || 'spreadsheet'}`];
  lines.push('');
  lines.push('Sheets:');
  for (const sheet of sheets) {
    const count = sheet.rows?.length ?? sheet.rowCount ?? 0;
    lines.push(`- ${sheet.sheet || sheet.name} — ${count} rows`);
  }
  return lines.join('\n');
}

function isSheetOperational(name = '', rows = []) {
  const lower = String(name).toLowerCase();
  if (/^sheet\d+$/.test(lower) && rows.length === 0) return false;
  if (/^(cover|template|readme|instructions)$/.test(lower)) return false;
  return rows.length > 0;
}

function flattenStructuredWorkbook(structuredData = {}, { fileId = null, fileHash = null } = {}) {
  const out = [];
  for (const sheet of structuredData.sheets || []) {
    if (!isSheetOperational(sheet.sheet, sheet.rows)) continue;
    for (const row of sheet.rows || []) {
      out.push({
        ...row,
        sheet: sheet.sheet,
        filename: structuredData.filename,
        fileId,
        fileHash,
      });
    }
  }
  return out;
}

function classifySpreadsheetRow({ rowPlan }) {
  const classes = new Set();
  if (rowPlan.accountResolution?.status === RESOLUTION.AMBIGUOUS) {
    classes.add(ROW_CLASS.AMBIGUOUS);
    return [...classes];
  }
  if (rowPlan.proposedChanges.some(c => c.type === CHANGE_TYPES.CREATE_ACCOUNT_CANDIDATE)) {
    classes.add(ROW_CLASS.NEW_ACCOUNT_CANDIDATE);
  }
  if (rowPlan.proposedChanges.some(c => c.type === CHANGE_TYPES.ADD_CONTACT)) {
    classes.add(ROW_CLASS.NEW_CONTACT);
  }
  if (rowPlan.proposedChanges.some(c => c.type === CHANGE_TYPES.ADD_NOTE)) {
    classes.add(ROW_CLASS.ACTIVITY_NOTE);
  }
  if (rowPlan.proposedChanges.some(c => c.type.startsWith('ADD_FOLLOW') || c.type === CHANGE_TYPES.UPDATE_NEXT_ACTION)) {
    classes.add(ROW_CLASS.FOLLOW_UP_UPDATE);
  }
  if (rowPlan.proposedChanges.some(c => c.type === CHANGE_TYPES.UPDATE_CONTACT || c.type === CHANGE_TYPES.ADD_EMAIL || c.type === CHANGE_TYPES.ADD_PHONE)) {
    classes.add(ROW_CLASS.EXISTING_CONTACT_UPDATE);
  }
  if (rowPlan.accountResolution?.entity) {
    classes.add(ROW_CLASS.EXISTING_ACCOUNT_UPDATE);
  }
  if (!classes.size && !rowPlan.values?.company && !rowPlan.values?.account) {
    classes.add(ROW_CLASS.IGNORE_NON_OPERATIONAL);
  }
  return [...classes];
}

function reconcileDirectFields({
  rowValues = {},
  prospect = null,
  contacts = [],
  columnProvenance = null,
  fileMeta = {},
}) {
  const proposedChanges = [];
  const conflicts = [];
  const ambiguities = [];

  const contactName = normalizeText(rowValues.contact || rowValues.contact_name || rowValues.person);
  let contact = null;
  if (contactName && prospect) {
    contact = contacts.find(c => {
      if (c.prospect_id && c.prospect_id !== prospect.id) return false;
      const nm = normalizeText(c.name || `${c.first_name || ''} ${c.last_name || ''}`);
      return nm.toLowerCase() === contactName.toLowerCase()
        || nm.toLowerCase().startsWith(contactName.toLowerCase());
    }) || null;
  }

  const phone = rowValues.phone || rowValues.mobile || rowValues.cell;
  if (phone && prospect) {
    const target = contact || prospect;
    const existingPhone = target.phone || prospect.phone;
    const classification = classifyIncomingField(existingPhone, phone, { fieldKind: 'phone' });
    const prov = buildCellProvenance({
      ...fileMeta,
      columnName: 'phone',
      rawValue: phone,
      columnProvenance,
    });
    if (classification === 'new') {
      proposedChanges.push({
        type: contact ? CHANGE_TYPES.ADD_PHONE : CHANGE_TYPES.ADD_PHONE,
        classification,
        value: phone,
        provenance: prov,
        safe: true,
      });
    } else if (classification === 'conflict') {
      conflicts.push({
        field: 'phone',
        existing: existingPhone,
        incoming: phone,
        provenance: prov,
        message: 'Phone number conflicts with existing CRM data',
      });
    }
  }

  const email = rowValues.email || rowValues.e_mail;
  if (email && prospect) {
    const target = contact || prospect;
    const existingEmail = target.email || prospect.email;
    const classification = classifyIncomingField(existingEmail, email, { fieldKind: 'email' });
    const prov = buildCellProvenance({
      ...fileMeta,
      columnName: 'email',
      rawValue: email,
      columnProvenance,
    });
    if (classification === 'new') {
      proposedChanges.push({
        type: CHANGE_TYPES.ADD_EMAIL,
        classification,
        value: email,
        provenance: prov,
        safe: true,
      });
    } else if (classification === 'conflict') {
      conflicts.push({
        field: 'email',
        existing: existingEmail,
        incoming: email,
        provenance: prov,
        message: 'Email conflicts with existing CRM data',
      });
    }
  }

  if (rowValues.status) {
    const statusInterp = interpretSpreadsheetStatus(rowValues.status);
    const prov = buildCellProvenance({
      ...fileMeta,
      columnName: 'status',
      rawValue: rowValues.status,
      columnProvenance,
    });
    if (statusInterp.kind === 'disposition' && statusInterp.confidence === 'high') {
      proposedChanges.push({
        type: CHANGE_TYPES.UPDATE_DISPOSITION,
        classification: 'correction',
        value: statusInterp.disposition_status,
        sourceLabel: statusInterp.sourceLabel,
        provenance: prov,
        safe: true,
      });
    } else if (statusInterp.kind === 'sales_priority') {
      proposedChanges.push({
        type: CHANGE_TYPES.SET_SALES_PRIORITY,
        classification: 'new',
        value: statusInterp.sales_priority,
        sourceLabel: statusInterp.sourceLabel,
        provenance: prov,
        safe: true,
      });
    } else if (statusInterp.kind === 'ambiguous_status') {
      ambiguities.push({
        field: 'status',
        value: rowValues.status,
        message: 'Status value is unclear — preserved as source label only',
        provenance: prov,
      });
    }
  }

  if (contactName && prospect && !contact) {
    proposedChanges.push({
      type: CHANGE_TYPES.ADD_CONTACT,
      classification: 'new',
      value: {
        name: contactName,
        title: rowValues.title || rowValues.role || null,
        phone: phone || null,
        email: email || null,
      },
      safe: true,
    });
  }

  return { proposedChanges, conflicts, ambiguities, contact };
}

function reconcileSpreadsheetRow({
  row,
  store,
  instruction = null,
  fileId = null,
  fileHash = null,
  memory = null,
  conversationId = null,
  appliedRowHashes = null,
}) {
  const rowValues = row.values || row;
  const semanticHash = rowSemanticHash(rowValues);
  const rowKey = `${fileHash || fileId || row.filename}:${row.sheet}:${row.rowNumber}:${semanticHash}`;
  if (appliedRowHashes?.has(rowKey)) {
    return {
      sheet: row.sheet,
      row: row.rowNumber,
      duplicateSuppressed: true,
      safeToCommit: false,
      proposedChanges: [],
      ambiguities: [],
      conflicts: [],
      classifications: [ROW_CLASS.IGNORE_NON_OPERATIONAL],
      summary: 'unchanged row suppressed',
    };
  }

  const context = store.snapshotContext();
  const claims = parseSpreadsheetRow(rowValues, { sheet: row.sheet, rowNumber: row.rowNumber });
  const accountClaim = claims.find(c => c.claim_type === CLAIM_TYPES.ACCOUNT);
  let accountResolution = null;
  if (accountClaim) {
    accountResolution = resolveClaim(accountClaim, context, {});
  } else {
    accountResolution = { status: RESOLUTION.UNRESOLVED, entity: null, candidates: [] };
  }

  let prospect = null;
  if (accountResolution?.entity) {
    if (accountResolution.entity.company_name || accountResolution.kind === 'prospect') {
      prospect = accountResolution.entity;
    } else {
      prospect = context.prospects.find(p => p.company_id === accountResolution.entity.id) || null;
    }
  }

  const fileMeta = {
    fileId,
    filename: row.filename,
    sheetName: row.sheet,
    rowNumber: row.rowNumber,
  };

  const direct = reconcileDirectFields({
    rowValues,
    prospect,
    contacts: context.contacts || [],
    columnProvenance: row.columnProvenance,
    fileMeta,
  });

  const proposedChanges = [...direct.proposedChanges];
  const conflicts = [...direct.conflicts];
  const ambiguities = [...direct.ambiguities];

  if (accountResolution?.status === RESOLUTION.AMBIGUOUS) {
    ambiguities.push({
      field: 'account',
      message: `Row ${row.rowNumber} — ${accountClaim?.payload?.name} could refer to multiple accounts`,
      candidates: (accountResolution.candidates || []).slice(0, 3).map(c => c.entity?.name || c.entity?.company_name),
    });
  } else if (accountResolution?.status === RESOLUTION.UNRESOLVED && accountClaim?.payload?.name) {
    proposedChanges.push({
      type: CHANGE_TYPES.CREATE_ACCOUNT_CANDIDATE,
      classification: 'new',
      value: { name: accountClaim.payload.name },
      safe: false,
    });
  }

  const text = synthesizeRowUnderstandingText({
    instruction,
    rowValues,
    sheetName: row.sheet,
    rowNumber: row.rowNumber,
    filename: row.filename,
  });
  const interpreted = interpretConversationalInput({
    text,
    conversationId,
    memory,
  });
  const thread = interpreted.situationModel?.threads?.[0];
  if (thread?.corrections?.length) {
    for (const corr of thread.corrections) {
      proposedChanges.push({
        type: CHANGE_TYPES.UPDATE_DECISION_MAKER_SIGNAL,
        classification: 'correction',
        value: corr,
        safe: true,
      });
    }
  }
  if (thread?.painPoints?.length) {
    for (const pain of thread.painPoints) {
      proposedChanges.push({
        type: CHANGE_TYPES.ADD_PAIN_POINT,
        classification: 'new',
        value: pain,
        safe: true,
      });
    }
  }
  if (thread?.commitments?.length || thread?.temporalReferences?.length) {
    proposedChanges.push({
      type: CHANGE_TYPES.ADD_FOLLOW_UP,
      classification: 'new',
      value: {
        commitments: thread.commitments || [],
        temporalReferences: thread.temporalReferences || [],
      },
      safe: true,
    });
  }
  const notesText = normalizeText(rowValues.notes || rowValues.update || rowValues.comments);
  if (notesText && /isn'?t the decision maker|not the decision maker|not decision maker/i.test(notesText)) {
    proposedChanges.push({
      type: CHANGE_TYPES.UPDATE_DECISION_MAKER_SIGNAL,
      classification: 'correction',
      value: { notes: notesText, source: 'spreadsheet_notes' },
      safe: true,
    });
  }
  if (notesText) {
    proposedChanges.push({
      type: CHANGE_TYPES.ADD_NOTE,
      classification: 'new',
      value: notesText,
      provenance: buildCellProvenance({
        ...fileMeta,
        columnName: 'notes',
        rawValue: notesText,
        columnProvenance: row.columnProvenance,
      }),
      safe: !accountResolution || accountResolution.status !== RESOLUTION.AMBIGUOUS,
    });
  }
  if (rowValues.next_step) {
    proposedChanges.push({
      type: CHANGE_TYPES.UPDATE_NEXT_ACTION,
      classification: 'new',
      value: normalizeText(rowValues.next_step),
      safe: true,
    });
  }

  const rowPlan = {
    sheet: row.sheet,
    row: row.rowNumber,
    values: rowValues,
    semanticHash,
    rowKey,
    accountResolution,
    contactResolution: direct.contact ? { entity: direct.contact, status: RESOLUTION.RESOLVED } : null,
    proposedChanges,
    ambiguities,
    conflicts,
    understandingPreview: interpreted.preview,
    situationModel: interpreted.situationModel,
  };
  rowPlan.classifications = classifySpreadsheetRow({ rowPlan });
  const hasStructuredClaims = claims.some(c => !['AO'].includes(c.claim_type));
  const hasSafeChanges = proposedChanges.some(c => c.safe !== false);
  rowPlan.safeToCommit = ambiguities.length === 0
    && conflicts.length === 0
    && accountResolution?.status !== RESOLUTION.AMBIGUOUS
    && (hasSafeChanges || (hasStructuredClaims && accountResolution?.entity));
  return rowPlan;
}

function buildSpreadsheetReconciliationPlan({
  structuredData,
  store,
  instruction = null,
  fileId = null,
  fileHash = null,
  memory = null,
  conversationId = null,
  priorFileHash = null,
}) {
  const workbookId = fileId || stableHash([structuredData?.filename, fileHash || '']);
  const rows = flattenStructuredWorkbook(structuredData, { fileId: workbookId, fileHash });
  const appliedRowHashes = new Map();
  const rowPlans = [];

  for (const row of rows) {
    const plan = reconcileSpreadsheetRow({
      row,
      store,
      instruction,
      fileId: workbookId,
      fileHash,
      memory,
      conversationId,
      appliedRowHashes: priorFileHash === fileHash ? appliedRowHashes : null,
    });
    if (!plan.duplicateSuppressed) {
      appliedRowHashes.set(plan.rowKey, true);
    }
    rowPlans.push(plan);
  }

  const summary = {
    totalRows: rowPlans.length,
    safeChanges: rowPlans.filter(r => r.safeToCommit).length,
    conflicts: rowPlans.reduce((n, r) => n + r.conflicts.length, 0),
    ambiguous: rowPlans.filter(r => r.ambiguities.length > 0 || r.accountResolution?.status === RESOLUTION.AMBIGUOUS).length,
    ignored: rowPlans.filter(r => r.classifications?.includes(ROW_CLASS.IGNORE_NON_OPERATIONAL)).length,
    duplicateSuppressed: rowPlans.filter(r => r.duplicateSuppressed).length,
  };

  return {
    workbookId,
    fileHash,
    workbookSummary: buildWorkbookSummary({
      filename: structuredData?.filename,
      sheets: structuredData?.sheets || [],
    }),
    rows: rowPlans,
    summary,
  };
}

function rowAccountLabel(rowPlan) {
  return rowPlan.accountResolution?.entity?.company_name
    || rowPlan.accountResolution?.entity?.name
    || rowPlan.values?.company
    || rowPlan.values?.account
    || 'Unknown account';
}

function summarizeRowOperationalChanges(rowPlan) {
  const bullets = [];
  for (const change of rowPlan.proposedChanges || []) {
    if (change.safe === false && change.type !== CHANGE_TYPES.CREATE_ACCOUNT_CANDIDATE) continue;
    if (change.type === CHANGE_TYPES.ADD_NOTE) bullets.push('Add note');
    if (change.type === CHANGE_TYPES.ADD_CONTACT) bullets.push('Add contact');
    if (change.type === CHANGE_TYPES.ADD_EMAIL) bullets.push('Add email');
    if (change.type === CHANGE_TYPES.ADD_PHONE) bullets.push('Add phone');
    if (change.type === CHANGE_TYPES.ADD_FOLLOW_UP || change.type === CHANGE_TYPES.UPDATE_NEXT_ACTION) {
      bullets.push('Add follow-up');
    }
    if (change.type === CHANGE_TYPES.SET_SALES_PRIORITY) bullets.push('Sales-priority change');
    if (change.type === CHANGE_TYPES.UPDATE_DISPOSITION) bullets.push('Status update');
  }
  for (const conflict of rowPlan.conflicts || []) {
    bullets.push(conflict.message || 'Field conflict — needs review');
  }
  for (const amb of rowPlan.ambiguities || []) {
    bullets.push(amb.message || 'Needs clarification');
  }
  return bullets;
}

function formatSpreadsheetOperationalResponse(plan, { previewOnly = true, instruction = '' } = {}) {
  const wantsRowDetail = /\b(which rows?|show me.*rows?|rows? changed|row-level)\b/i.test(String(instruction || ''));
  const safeNotes = plan.rows.reduce((n, r) => n + r.proposedChanges.filter(c => c.type === CHANGE_TYPES.ADD_NOTE && c.safe !== false).length, 0);
  const safeContacts = plan.rows.reduce((n, r) => n + r.proposedChanges.filter(c => c.type === CHANGE_TYPES.ADD_CONTACT).length, 0);
  const safeFollowUps = plan.rows.reduce((n, r) => n + r.proposedChanges.filter(c => c.type === CHANGE_TYPES.ADD_FOLLOW_UP || c.type === CHANGE_TYPES.UPDATE_NEXT_ACTION).length, 0);
  const contactUpdates = plan.rows.reduce((n, r) => n + r.proposedChanges.filter(c => c.type === CHANGE_TYPES.ADD_EMAIL || c.type === CHANGE_TYPES.ADD_PHONE).length, 0);
  const priorityChanges = plan.rows.reduce((n, r) => n + r.proposedChanges.filter(c => c.type === CHANGE_TYPES.SET_SALES_PRIORITY).length, 0);

  const rowsNeedingReview = plan.rows.filter(r => (r.ambiguities || []).length || (r.conflicts || []).length);
  const rowsWithChanges = plan.rows.filter(r => summarizeRowOperationalChanges(r).length);
  const lines = [];
  lines.push(`I reviewed all ${plan.summary.totalRows} row${plan.summary.totalRows === 1 ? '' : 's'}.`);
  lines.push('');

  if (!rowsWithChanges.length && !rowsNeedingReview.length && plan.summary.totalRows) {
    lines.push('No durable changes are needed:');
    lines.push(`- ${plan.summary.totalRows} row${plan.summary.totalRows === 1 ? '' : 's'} match existing CRM data or contain no operational updates`);
    if (plan.summary.duplicateSuppressed) {
      lines.push(`- ${plan.summary.duplicateSuppressed} row${plan.summary.duplicateSuppressed === 1 ? '' : 's'} duplicate prior uploads`);
    }
    if (previewOnly) {
      lines.push('');
      lines.push('Nothing has been saved yet.');
    }
    lines.unshift('');
    lines.unshift(plan.workbookSummary);
    return lines.join('\n');
  }

  lines.push('Ready to update:');
  lines.push(`- ${safeNotes} account note${safeNotes === 1 ? '' : 's'}`);
  lines.push(`- ${contactUpdates} contact field addition${contactUpdates === 1 ? '' : 's'}`);
  lines.push(`- ${safeContacts} new contact${safeContacts === 1 ? '' : 's'}`);
  lines.push(`- ${safeFollowUps} follow-up action${safeFollowUps === 1 ? '' : 's'}`);
  if (priorityChanges) {
    lines.push(`- ${priorityChanges} sales-priority change${priorityChanges === 1 ? '' : 's'}`);
  }

  if (rowsNeedingReview.length) {
    lines.push('');
    lines.push('Needs clarification:');
    for (const rowPlan of rowsNeedingReview) {
      const sheet = rowPlan.sheet || 'Sheet1';
      const label = rowAccountLabel(rowPlan);
      if (wantsRowDetail) {
        const detail = summarizeRowOperationalChanges(rowPlan).join('; ') || 'Needs review';
        lines.push(`${sheet} row ${rowPlan.row} — ${label}`);
        lines.push(`- ${detail}`);
      } else {
        lines.push(`- ${sheet} row ${rowPlan.row} — ${label}`);
      }
    }
  }

  if (wantsRowDetail) {
    const changedRows = plan.rows.filter(r => summarizeRowOperationalChanges(r).length && !rowsNeedingReview.includes(r));
    if (changedRows.length) {
      lines.push('');
      lines.push('Row changes:');
      for (const rowPlan of changedRows) {
        const sheet = rowPlan.sheet || 'Sheet1';
        lines.push(`${sheet} row ${rowPlan.row} — ${rowAccountLabel(rowPlan)}`);
        for (const bullet of summarizeRowOperationalChanges(rowPlan)) {
          lines.push(`- ${bullet}`);
        }
      }
    }
  }

  if (previewOnly) {
    lines.push('');
    lines.push('Nothing has been saved yet.');
  }

  lines.unshift('');
  lines.unshift(plan.workbookSummary);
  return lines.join('\n');
}

function formatSpreadsheetReconciliationPreview(plan, options = {}) {
  return formatSpreadsheetOperationalResponse(plan, { previewOnly: true, ...options });
}

async function commitSpreadsheetReconciliationPlan({
  plan,
  clientId,
  store,
  sourceActor = null,
  instruction = null,
  now = new Date(),
  commitMode = 'safe_only',
}) {
  const telemetry = emptyTelemetry();
  bump(telemetry, 'max_spreadsheet_upload_count');
  bump(telemetry, 'max_spreadsheet_rows_processed_count', plan.rows.length);

  const results = [];
  for (const rowPlan of plan.rows) {
    if (rowPlan.duplicateSuppressed) {
      bump(telemetry, 'max_spreadsheet_duplicate_suppressed_count');
      continue;
    }
    if (commitMode === 'safe_only' && !rowPlan.safeToCommit) {
      if (rowPlan.conflicts.length) bump(telemetry, 'max_spreadsheet_conflict_count', rowPlan.conflicts.length);
      if (rowPlan.ambiguities.length) bump(telemetry, 'max_spreadsheet_ambiguity_count', rowPlan.ambiguities.length);
      results.push({ row: rowPlan.row, sheet: rowPlan.sheet, skipped: true, reason: 'needs_review', rowPlan });
      continue;
    }

    const rowValues = rowPlan.values;
    const filename = plan.workbookSummary?.match(/Workbook: (.+)/)?.[1] || 'spreadsheet';
    const text = synthesizeRowUnderstandingText({
      instruction,
      rowValues,
      sheetName: rowPlan.sheet,
      rowNumber: rowPlan.row,
      filename,
    });
    const result = await ingestOperationalUpdate({
      clientId,
      text,
      sourceType: 'FILE_IMPORTED',
      sourceActor,
      store,
      now,
      spreadsheetFieldPlan: rowPlan,
      artifact: {
        artifact_type: 'spreadsheet',
        filename,
        metadata: {
          workbook_id: plan.workbookId,
          file_hash: plan.fileHash,
          sheet: rowPlan.sheet,
          row: rowPlan.row,
          semantic_hash: rowPlan.semanticHash,
          proposed_changes: rowPlan.proposedChanges.map(c => c.type),
        },
        raw_content: rowValues,
      },
    });

    bump(telemetry, 'max_spreadsheet_safe_change_count', rowPlan.proposedChanges.filter(c => c.safe).length);
    if (rowPlan.proposedChanges.some(c => c.type === CHANGE_TYPES.ADD_CONTACT)) {
      bump(telemetry, 'max_spreadsheet_new_contact_count');
    }
    if (rowPlan.proposedChanges.some(c => c.type === CHANGE_TYPES.CREATE_ACCOUNT_CANDIDATE)) {
      bump(telemetry, 'max_spreadsheet_new_account_candidate_count');
    }
    bump(telemetry, 'max_spreadsheet_commit_count');
    results.push({ row: rowPlan.row, sheet: rowPlan.sheet, ...result });
  }

  return {
    plan,
    results,
    telemetry,
    summary: {
      processed: plan.rows.length,
      committed: results.filter(r => !r.skipped && !r.commit_blocked).length,
      held: results.filter(r => r.skipped || r.unresolved?.length || r.conflicts?.length).length,
    },
  };
}

module.exports = {
  CHANGE_TYPES,
  ROW_CLASS,
  buildWorkbookSummary,
  flattenStructuredWorkbook,
  buildSpreadsheetReconciliationPlan,
  reconcileSpreadsheetRow,
  formatSpreadsheetReconciliationPreview,
  formatSpreadsheetOperationalResponse,
  rowAccountLabel,
  commitSpreadsheetReconciliationPlan,
  rowSemanticHash,
  classifyIncomingField,
};
