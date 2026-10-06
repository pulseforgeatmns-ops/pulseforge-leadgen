'use strict';

const { normalizeText } = require('../stateIngestion/claimParser');

const ATTACHMENT_TASK_INTENT = Object.freeze({
  SPREADSHEET_RECONCILE: 'SPREADSHEET_RECONCILE',
  SPREADSHEET_PREVIEW: 'SPREADSHEET_PREVIEW',
  SPREADSHEET_COMMIT: 'SPREADSHEET_COMMIT',
  SPREADSHEET_SUMMARIZE: 'SPREADSHEET_SUMMARIZE',
  DOCUMENT_REVIEW: 'DOCUMENT_REVIEW',
  IMAGE_REVIEW: 'IMAGE_REVIEW',
  NONE: 'NONE',
});

const SPREADSHEET_RECONCILE_RE = new RegExp(
  [
    '\\breview\\b.*\\b(spreadsheet|workbook|sheet|rows?)\\b',
    '\\breview\\s+all\\s+\\d+\\s+rows?\\b',
    '\\bcompare\\b.*\\b(pulseforge|crm|existing|already)\\b',
    '\\bcompare\\b.*\\b(this|workbook|spreadsheet)\\b',
    '\\breconcile\\b.*\\b(workbook|spreadsheet|accounts?|rows?)\\b',
    '\\bupdate\\b.*\\b(every|each|all)\\b.*\\b(account|contact|note|status|follow[- ]?up)\\b',
    '\\bimport\\b.*\\b(update|account|spreadsheet|these)\\b',
    '\\binspect\\b.*\\b(all\\s+)?rows?\\b',
    '\\bidentify\\b.*\\bchange|conflict\\b',
    '\\bshow\\b.*\\b(which\\s+)?rows?\\b.*\\b(changed|change|clarification|conflict)\\b',
    '\\bshow\\b.*\\b(conflicts?|what changed)\\b',
    '\\bread\\b.*\\b(whole\\s+)?spreadsheet\\b',
    '\\bapply\\b.*\\bsafe\\s+update',
    '\\bupdated\\s+accounts?\\b',
    '\\bthese\\s+are\\s+my\\s+updated\\s+accounts?\\b',
    '\\bfrom\\s+this\\s+week\\b.*\\baccount',
  ].join('|'),
  'i',
);

const SPREADSHEET_PREVIEW_RE = new RegExp(
  [
    "before\\s+you\\s+save",
    "don't\\s+save\\s+yet",
    'do\\s+not\\s+save',
    '\\bpreview\\b.*\\bsav',
    '\\bwithout\\s+saving\\b',
    '\\bshow\\b.*\\b(which\\s+)?rows?\\b.*\\b(changed|clarification|conflict)\\b',
    '\\bshow\\b.*\\bwhat\\s+changed\\b',
    '\\bneeds\\s+clarification\\b',
  ].join('|'),
  'i',
);

const SPREADSHEET_COMMIT_RE = new RegExp(
  [
    '\\bsave\\b.*\\b(safe|those|them|update|row|change)\\b',
    '^\\s*(save|confirm|yes)\\s*$',
    '\\bapply\\b.*\\b(change|update|safe)\\b',
    '\\bcommit\\b.*\\b(safe|row|change|update)\\b',
    '\\bgo\\s+ahead\\b.*\\b(update|save|apply)\\b',
  ].join('|'),
  'i',
);

const SPREADSHEET_SUMMARIZE_RE = /\bsummarize\b.*\b(spreadsheet|workbook|sheet)\b|\bwhat'?s in (this|the) spreadsheet\b/i;

const GENERIC_COACHING_RE = /\b(stay curious|listen for pain|talk track|relationship guidance|ao brief)\b/i;

function readySpreadsheetAttachments(attachments = []) {
  return attachments.filter(a => a.type === 'spreadsheet' && a.extractionStatus === 'ready' && a.structuredData?.sheets?.length);
}

function spreadsheetRowCount(attachments = []) {
  let total = 0;
  for (const att of readySpreadsheetAttachments(attachments)) {
    for (const sheet of att.structuredData.sheets || []) {
      total += (sheet.rows || []).length;
    }
  }
  return total;
}

function bindsImplicitSpreadsheetReference(text = '', attachments = []) {
  const normalized = normalizeText(text);
  if (!normalized) return readySpreadsheetAttachments(attachments).length === 1;
  if (SPREADSHEET_RECONCILE_RE.test(normalized)) return true;
  if (/\b(this|these|attached|uploaded|workbook|spreadsheet|sheet1|rows?)\b/i.test(normalized)) return true;
  if (/\b\d+\s+rows?\b/i.test(normalized) && readySpreadsheetAttachments(attachments).length) return true;
  return false;
}

function detectSpreadsheetTargetAttachment(attachments = [], text = '') {
  const ready = readySpreadsheetAttachments(attachments);
  if (!ready.length) return { attachment: null, clarification: null };
  if (ready.length === 1) return { attachment: ready[0], clarification: null };
  const normalized = normalizeText(text);
  for (const att of ready) {
    const name = String(att.filename || '').toLowerCase();
    const stem = name.replace(/\.(xlsx|xls|csv)$/, '');
    if (stem.length >= 3) {
      const stemRe = new RegExp(`\\b${stem.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
      if (stemRe.test(normalized)) {
        return { attachment: att, clarification: null };
      }
    }
  }
  return {
    attachment: null,
    clarification: 'You attached multiple spreadsheets — which file should I reconcile?',
  };
}

function resolvePendingWorkbook(memory) {
  const pending = memory?.getPendingSpreadsheetWorkbook?.() || memory?.pendingSpreadsheetWorkbook || null;
  if (!pending?.structuredData?.sheets?.length) return null;
  return pending;
}

function detectAttachmentTaskIntent({
  text = '',
  attachments = [],
  memory = null,
  confirm = null,
} = {}) {
  const normalized = normalizeText(text);
  const readySheets = readySpreadsheetAttachments(attachments);
  const pending = resolvePendingWorkbook(memory);
  const rowCount = spreadsheetRowCount(attachments) || pending?.totalRows || 0;
  const wantsPreviewLanguage = SPREADSHEET_PREVIEW_RE.test(normalized) || confirm === false;
  const explicitCommit = (confirm === true || SPREADSHEET_COMMIT_RE.test(normalized)) && !wantsPreviewLanguage;
  const previewOnly = wantsPreviewLanguage || (rowCount > 1 && !explicitCommit);

  if (readySheets.length > 1 && bindsImplicitSpreadsheetReference(normalized, attachments)) {
    const target = detectSpreadsheetTargetAttachment(attachments, normalized);
    if (target.clarification || !target.attachment) {
      return {
        intent: ATTACHMENT_TASK_INTENT.NONE,
        clarification: target.clarification || 'You attached multiple spreadsheets — which file should I reconcile?',
        previewOnly: true,
        commit: false,
        intentSource: 'multi_spreadsheet_ambiguous',
      };
    }
  }

  if (!readySheets.length && pending && (explicitCommit || SPREADSHEET_COMMIT_RE.test(normalized))) {
    return {
      intent: ATTACHMENT_TASK_INTENT.SPREADSHEET_COMMIT,
      targetAttachmentId: pending.attachmentId,
      previewOnly: false,
      commit: true,
      intentSource: 'pending_plan_commit',
      pendingWorkbook: pending,
    };
  }

  if (readySheets.length && (bindsImplicitSpreadsheetReference(normalized, attachments) || !normalized)) {
    const target = detectSpreadsheetTargetAttachment(attachments, normalized);
    if (target.clarification) {
      return {
        intent: ATTACHMENT_TASK_INTENT.NONE,
        clarification: target.clarification,
        previewOnly: true,
        commit: false,
        intentSource: 'multi_spreadsheet_ambiguous',
      };
    }
    let intent = ATTACHMENT_TASK_INTENT.SPREADSHEET_RECONCILE;
    if (SPREADSHEET_SUMMARIZE_RE.test(normalized)) intent = ATTACHMENT_TASK_INTENT.SPREADSHEET_SUMMARIZE;
    else if (explicitCommit) intent = ATTACHMENT_TASK_INTENT.SPREADSHEET_COMMIT;
    else if (previewOnly) intent = ATTACHMENT_TASK_INTENT.SPREADSHEET_PREVIEW;
    const resolvedPreviewOnly = intent === ATTACHMENT_TASK_INTENT.SPREADSHEET_COMMIT
      ? false
      : (intent === ATTACHMENT_TASK_INTENT.SPREADSHEET_PREVIEW || previewOnly);
    return {
      intent,
      targetAttachmentId: target.attachment?.id || readySheets[0].id,
      previewOnly: resolvedPreviewOnly,
      commit: intent === ATTACHMENT_TASK_INTENT.SPREADSHEET_COMMIT,
      intentSource: normalized ? 'explicit_user_verbs' : 'implicit_spreadsheet_binding',
      totalRows: spreadsheetRowCount(attachments),
    };
  }

  if (readySheets.length && normalized && /\breview this\b/i.test(normalized)) {
    const target = detectSpreadsheetTargetAttachment(attachments, normalized);
    return {
      intent: ATTACHMENT_TASK_INTENT.SPREADSHEET_PREVIEW,
      targetAttachmentId: target.attachment?.id || readySheets[0].id,
      previewOnly: true,
      commit: false,
      intentSource: 'review_this_binding',
      totalRows: spreadsheetRowCount(attachments),
    };
  }

  const docReady = attachments.filter(a => a.type === 'document' && a.extractionStatus === 'ready');
  if (docReady.length && /\breview\b|\binspect\b|\bparse\b/i.test(normalized)) {
    return {
      intent: ATTACHMENT_TASK_INTENT.DOCUMENT_REVIEW,
      previewOnly: true,
      commit: false,
      intentSource: 'document_attachment',
    };
  }

  const imageReady = attachments.filter(a => a.type === 'image');
  if (imageReady.length && /\breview\b|\blook at\b|\bwhat'?s in\b/i.test(normalized)) {
    return {
      intent: ATTACHMENT_TASK_INTENT.IMAGE_REVIEW,
      previewOnly: true,
      commit: false,
      intentSource: 'image_attachment',
    };
  }

  return {
    intent: ATTACHMENT_TASK_INTENT.NONE,
    previewOnly: false,
    commit: false,
    intentSource: 'none',
  };
}

function enrichSituationModelWithAttachmentIntent(situationModel, attachmentIntent = {}) {
  if (!situationModel) return situationModel;
  const spreadsheetIntents = new Set([
    ATTACHMENT_TASK_INTENT.SPREADSHEET_RECONCILE,
    ATTACHMENT_TASK_INTENT.SPREADSHEET_PREVIEW,
    ATTACHMENT_TASK_INTENT.SPREADSHEET_COMMIT,
    ATTACHMENT_TASK_INTENT.SPREADSHEET_SUMMARIZE,
  ]);
  if (!spreadsheetIntents.has(attachmentIntent.intent)) return situationModel;
  situationModel.attachmentTask = {
    requested_action: 'reconcile_attached_spreadsheet',
    intent: attachmentIntent.intent,
    target_attachment_id: attachmentIntent.targetAttachmentId || null,
    preview_only: Boolean(attachmentIntent.previewOnly),
    commit: Boolean(attachmentIntent.commit),
    intent_source: attachmentIntent.intentSource || null,
  };
  if (attachmentIntent.previewOnly) {
    situationModel.recommendedNextActions = [];
    situationModel.preview = situationModel.preview
      ? situationModel.preview.replace(GENERIC_COACHING_RE, '').trim()
      : situationModel.preview;
  }
  return situationModel;
}

function isSpreadsheetOperationalIntent(intent) {
  return [
    ATTACHMENT_TASK_INTENT.SPREADSHEET_RECONCILE,
    ATTACHMENT_TASK_INTENT.SPREADSHEET_PREVIEW,
    ATTACHMENT_TASK_INTENT.SPREADSHEET_COMMIT,
    ATTACHMENT_TASK_INTENT.SPREADSHEET_SUMMARIZE,
  ].includes(intent);
}

module.exports = {
  ATTACHMENT_TASK_INTENT,
  detectAttachmentTaskIntent,
  enrichSituationModelWithAttachmentIntent,
  isSpreadsheetOperationalIntent,
  readySpreadsheetAttachments,
  spreadsheetRowCount,
  bindsImplicitSpreadsheetReference,
};
