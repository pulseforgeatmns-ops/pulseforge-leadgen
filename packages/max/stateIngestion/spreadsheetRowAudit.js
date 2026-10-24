'use strict';

const { normalizeText } = require('./claimParser');
const { RESOLUTION } = require('./types');
const { CHANGE_TYPES } = require('./spreadsheetChangeTypes');

const SPREADSHEET_NO_OP_REASON = Object.freeze({
  ALREADY_MATCHES_CRM: 'ALREADY_MATCHES_CRM',
  NO_OPERATIONAL_FIELDS: 'NO_OPERATIONAL_FIELDS',
  DUPLICATE_PRIOR_IMPORT: 'DUPLICATE_PRIOR_IMPORT',
  EMPTY_ROW: 'EMPTY_ROW',
  UNMAPPED_COLUMNS: 'UNMAPPED_COLUMNS',
  LOW_CONFIDENCE_ACCOUNT_MATCH: 'LOW_CONFIDENCE_ACCOUNT_MATCH',
  LOW_CONFIDENCE_CONTACT_MATCH: 'LOW_CONFIDENCE_CONTACT_MATCH',
  NO_NEWER_INFORMATION: 'NO_NEWER_INFORMATION',
});

const ROW_OUTCOME = Object.freeze({
  CHANGES_READY: 'changes_ready',
  NEEDS_REVIEW: 'needs_review',
  NO_CHANGE: 'no_change',
  IGNORED: 'ignored',
});

const KNOWN_ROW_KEYS = new Set([
  'company', 'account', 'account_name', 'business', 'organization', 'prospect',
  'contact', 'contact_name', 'person', 'decision_maker', 'poc',
  'phone', 'mobile', 'cell', 'email', 'e_mail',
  'notes', 'update', 'comments', 'status', 'stage',
  'next_step', 'next_action', 'follow_up', 'action',
  'title', 'role', 'contact_title',
  'ao', 'owner', 'assigned_ao', 'ownership_ao',
  'expected_window', 'expects_inbound_call', 'next_expected_event',
  'new_prospect', 'is_new', 'record_id', 'id',
]);

function normalizeColumnKey(key) {
  return String(key || '').trim().toLowerCase().replace(/\s+/g, '_');
}

function rowHasOperationalValues(rowValues = {}) {
  const account = normalizeText(
    rowValues.company || rowValues.account || rowValues.account_name || rowValues.business
  );
  if (account) return true;
  for (const [key, val] of Object.entries(rowValues)) {
    if (!val || !String(val).trim()) continue;
    if (KNOWN_ROW_KEYS.has(normalizeColumnKey(key))) return true;
  }
  return false;
}

function isEmptyRow(rowValues = {}) {
  for (const val of Object.values(rowValues)) {
    if (val != null && String(val).trim()) return false;
  }
  return true;
}

function detectUnmappedColumns(rowValues = {}) {
  const unmapped = [];
  for (const [key, val] of Object.entries(rowValues)) {
    if (val == null || !String(val).trim()) continue;
    if (!KNOWN_ROW_KEYS.has(normalizeColumnKey(key))) {
      unmapped.push(key);
    }
  }
  return unmapped;
}

function noteAlreadyInCrm(prospect, notesText) {
  const normalized = normalizeText(notesText).toLowerCase();
  if (!normalized) return false;
  for (const act of prospect?.activities || []) {
    const content = normalizeText(act.content || act.notes || act.summary || act.text || '').toLowerCase();
    if (!content) continue;
    if (content === normalized || content.includes(normalized) || normalized.includes(content)) {
      return true;
    }
  }
  return false;
}

function wantsRowLevelDetail(instruction = '') {
  const text = String(instruction || '');
  return /\b(row-by-row|each row|every row|all \d+ rows?|show me.*rows?|which rows?|rows? changed|row-level|conflicts?.*clarification|anything that needs clarification)\b/i.test(text);
}

function countDurableChanges(rowPlan) {
  return (rowPlan.proposedChanges || []).filter(c => {
    if (c.type === CHANGE_TYPES.CREATE_ACCOUNT_CANDIDATE) return true;
    return c.safe !== false;
  }).length;
}

function finalizeRowReconciliation(rowPlan) {
  if (rowPlan.duplicateSuppressed) {
    rowPlan.outcome = ROW_OUTCOME.IGNORED;
    rowPlan.noOpReason = SPREADSHEET_NO_OP_REASON.DUPLICATE_PRIOR_IMPORT;
    rowPlan.noOpDetail = 'Same workbook row fingerprint already committed';
    return rowPlan;
  }

  const rowValues = rowPlan.values || {};
  if (isEmptyRow(rowValues)) {
    rowPlan.outcome = ROW_OUTCOME.IGNORED;
    rowPlan.noOpReason = SPREADSHEET_NO_OP_REASON.EMPTY_ROW;
    rowPlan.comparedFields = rowPlan.comparedFields || [];
    return rowPlan;
  }

  rowPlan.unmappedColumns = detectUnmappedColumns(rowValues);
  rowPlan.comparedFields = rowPlan.comparedFields || [];

  const hasConflict = (rowPlan.conflicts || []).length > 0;
  const hasAmbiguity = (rowPlan.ambiguities || []).length > 0
    || rowPlan.accountResolution?.status === RESOLUTION.AMBIGUOUS;
  const durableChanges = countDurableChanges(rowPlan);

  if (hasConflict || hasAmbiguity) {
    rowPlan.outcome = ROW_OUTCOME.NEEDS_REVIEW;
    rowPlan.noOpReason = null;
    return rowPlan;
  }

  if (durableChanges > 0) {
    rowPlan.outcome = ROW_OUTCOME.CHANGES_READY;
    rowPlan.noOpReason = null;
    return rowPlan;
  }

  if (rowPlan.unmappedColumns.length && !rowHasOperationalValues(rowValues)) {
    rowPlan.outcome = ROW_OUTCOME.NO_CHANGE;
    rowPlan.noOpReason = SPREADSHEET_NO_OP_REASON.UNMAPPED_COLUMNS;
    return rowPlan;
  }

  if (rowPlan.unmappedColumns.length && rowPlan.comparedFields.length === 0) {
    rowPlan.outcome = ROW_OUTCOME.NO_CHANGE;
    rowPlan.noOpReason = SPREADSHEET_NO_OP_REASON.UNMAPPED_COLUMNS;
    return rowPlan;
  }

  if (!rowHasOperationalValues(rowValues)) {
    rowPlan.outcome = ROW_OUTCOME.IGNORED;
    rowPlan.noOpReason = SPREADSHEET_NO_OP_REASON.NO_OPERATIONAL_FIELDS;
    return rowPlan;
  }

  if (rowPlan.accountResolution?.status === RESOLUTION.UNRESOLVED && rowValues.company) {
    rowPlan.outcome = ROW_OUTCOME.NEEDS_REVIEW;
    return rowPlan;
  }

  rowPlan.outcome = ROW_OUTCOME.NO_CHANGE;
  rowPlan.noOpReason = rowPlan.unmappedColumns.length
    ? SPREADSHEET_NO_OP_REASON.UNMAPPED_COLUMNS
    : SPREADSHEET_NO_OP_REASON.ALREADY_MATCHES_CRM;
  return rowPlan;
}

function computeWorkbookDiagnostics(plan) {
  const rows = plan.rows || [];
  let fieldsCompared = 0;
  let fieldsMatched = 0;
  let fieldsChanged = 0;
  let fieldsUnmapped = 0;

  for (const row of rows) {
    for (const cmp of row.comparedFields || []) {
      fieldsCompared += 1;
      if (cmp.result === 'matched' || cmp.result === 'same') fieldsMatched += 1;
      if (cmp.result === 'new' || cmp.result === 'correction') fieldsChanged += 1;
    }
    fieldsUnmapped += (row.unmappedColumns || []).length;
  }

  const metrics = {
    rows_total: rows.length,
    rows_with_operational_fields: rows.filter(r => rowHasOperationalValues(r.values || {})).length,
    rows_with_changes: rows.filter(r => r.outcome === ROW_OUTCOME.CHANGES_READY).length,
    rows_no_change: rows.filter(r => r.outcome === ROW_OUTCOME.NO_CHANGE).length,
    rows_conflict: rows.filter(r => (r.conflicts || []).length > 0).length,
    rows_ambiguous: rows.filter(r => r.outcome === ROW_OUTCOME.NEEDS_REVIEW).length,
    rows_ignored: rows.filter(r => r.outcome === ROW_OUTCOME.IGNORED).length,
    fields_compared: fieldsCompared,
    fields_changed: fieldsChanged,
    fields_matched: fieldsMatched,
    fields_unmapped: fieldsUnmapped,
  };

  return {
    ...metrics,
    terminal_turn: true,
    noop_by_reason: rows.reduce((acc, r) => {
      if (r.noOpReason) acc[r.noOpReason] = (acc[r.noOpReason] || 0) + 1;
      return acc;
    }, {}),
  };
}

function pushCompared(comparedFields, entry) {
  comparedFields.push(entry);
}

function formatNoOpReasonLabel(reason) {
  switch (reason) {
    case SPREADSHEET_NO_OP_REASON.ALREADY_MATCHES_CRM:
      return 'Already matches CRM';
    case SPREADSHEET_NO_OP_REASON.NO_OPERATIONAL_FIELDS:
      return 'No operational update';
    case SPREADSHEET_NO_OP_REASON.DUPLICATE_PRIOR_IMPORT:
      return 'Duplicate prior import';
    case SPREADSHEET_NO_OP_REASON.EMPTY_ROW:
      return 'Empty row';
    case SPREADSHEET_NO_OP_REASON.UNMAPPED_COLUMNS:
      return 'Unmapped column data';
    case SPREADSHEET_NO_OP_REASON.LOW_CONFIDENCE_ACCOUNT_MATCH:
      return 'Account match uncertain';
    case SPREADSHEET_NO_OP_REASON.LOW_CONFIDENCE_CONTACT_MATCH:
      return 'Contact match uncertain';
    case SPREADSHEET_NO_OP_REASON.NO_NEWER_INFORMATION:
      return 'No newer information';
    default:
      return 'No change';
  }
}

function formatRowAuditBullets(rowPlan) {
  const bullets = [];
  for (const cmp of rowPlan.comparedFields || []) {
    if (cmp.result === 'matched' || cmp.result === 'same') {
      bullets.push(`${cmp.label || cmp.field} already matches CRM`);
    } else if (cmp.result === 'empty') {
      bullets.push(`${cmp.label || cmp.field} empty`);
    } else if (cmp.result === 'note_duplicate') {
      bullets.push('Note already recorded');
    } else if (cmp.result === 'note_blocked') {
      bullets.push('Note blocked by account ambiguity');
    } else if (cmp.result === 'contact_matched') {
      bullets.push('Contact already exists');
    } else if (cmp.result === 'status_mapped') {
      bullets.push(`${cmp.incoming} → ${cmp.mappedTo}`);
    } else if (cmp.result === 'status_unmapped') {
      bullets.push(`Unmapped status — needs review (${cmp.incoming})`);
    } else if (cmp.result === 'follow_up_no_schedule') {
      bullets.push(`"${cmp.incoming}" — no scheduled action created because no date/time was supplied`);
    }
  }
  for (const col of rowPlan.unmappedColumns || []) {
    bullets.push(`Column "${col}" is not currently mapped`);
  }
  if (rowPlan.noOpReason === SPREADSHEET_NO_OP_REASON.NO_OPERATIONAL_FIELDS) {
    bullets.push('Row contains company name only');
  }
  if (rowPlan.noOpReason === SPREADSHEET_NO_OP_REASON.DUPLICATE_PRIOR_IMPORT) {
    bullets.push('Same workbook row fingerprint already committed');
  }
  if (rowPlan.noteAudit && !bullets.some(b => /note/i.test(b))) {
    bullets.push(rowPlan.noteAudit);
  }
  if (rowPlan.contactAudit && !bullets.some(b => /contact/i.test(b))) {
    bullets.push(rowPlan.contactAudit);
  }
  if (rowPlan.followUpAudit && !bullets.some(b => /scheduled action/i.test(b))) {
    bullets.push(rowPlan.followUpAudit);
  }
  for (const change of rowPlan.proposedChanges || []) {
    if (change.type === CHANGE_TYPES.ADD_NOTE) bullets.push('Add note');
    if (change.type === CHANGE_TYPES.ADD_CONTACT) bullets.push('Add contact');
    if (change.type === CHANGE_TYPES.ADD_EMAIL) bullets.push('Add email');
    if (change.type === CHANGE_TYPES.ADD_PHONE) bullets.push('Add phone');
    if (change.type === CHANGE_TYPES.SET_SALES_PRIORITY) bullets.push('Sales-priority change');
    if (change.type === CHANGE_TYPES.UPDATE_DISPOSITION) bullets.push('Status update');
    if (change.type === CHANGE_TYPES.ADD_FOLLOW_UP || change.type === CHANGE_TYPES.UPDATE_NEXT_ACTION) {
      bullets.push('Add follow-up');
    }
  }
  for (const conflict of rowPlan.conflicts || []) {
    bullets.push(conflict.message || 'Field conflict — needs review');
  }
  for (const amb of rowPlan.ambiguities || []) {
    bullets.push(amb.message || 'Needs clarification');
  }
  return bullets;
}

module.exports = {
  SPREADSHEET_NO_OP_REASON,
  ROW_OUTCOME,
  detectUnmappedColumns,
  finalizeRowReconciliation,
  computeWorkbookDiagnostics,
  wantsRowLevelDetail,
  formatRowAuditBullets,
  formatNoOpReasonLabel,
  noteAlreadyInCrm,
  pushCompared,
  rowHasOperationalValues,
  isEmptyRow,
};
