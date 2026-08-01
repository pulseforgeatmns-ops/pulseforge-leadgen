'use strict';

/**
 * Session-level short-lived working memory for Max.
 * Preserves the current operator task across turns without long-term memory
 * or autonomous execution.
 */

const DEFAULT_CANARY_CONSTRAINTS = Object.freeze({
  preparationOnly: true,
  noMissionCreation: true,
  noLaunch: true,
  noExecution: true,
  noApproval: true,
  noPrint: true,
  noMail: true,
  noInventedEvidence: true,
});

const LAST_OUTPUT_TYPES = Object.freeze({
  CANARY_REVIEW_PACKAGE: 'canary_review_package',
  VERIFICATION_WORK_ORDER: 'verification_work_order',
  FILLABLE_TABLE: 'fillable_table',
  PROVISIONAL_DRAFTS: 'provisional_drafts',
});

/**
 * @param {object|null|undefined} session
 * @returns {object|null}
 */
function getActiveWorkContext(session) {
  if (!session) return null;
  if (session.activeWorkContext && typeof session.activeWorkContext === 'object') {
    return session.activeWorkContext;
  }
  if (
    session.context &&
    session.context.activeWorkContext &&
    typeof session.context.activeWorkContext === 'object'
  ) {
    return session.context.activeWorkContext;
  }
  return null;
}

/**
 * @param {object} session
 * @param {object|null} ctx
 */
function setActiveWorkContext(session, ctx) {
  if (!session) return null;
  const next = ctx && typeof ctx === 'object' ? cloneActiveWorkContext(ctx) : null;
  session.activeWorkContext = next;
  if (session.context && typeof session.context === 'object') {
    session.context.activeWorkContext = next;
  }
  session.updatedAt = new Date().toISOString();
  return next;
}

/**
 * @param {object} ctx
 */
function cloneActiveWorkContext(ctx) {
  return {
    workflow: ctx.workflow != null ? String(ctx.workflow) : null,
    target:
      ctx.target && typeof ctx.target === 'object'
        ? { ...ctx.target }
        : {},
    entities: Array.isArray(ctx.entities)
      ? ctx.entities.map((e) => ({ ...e }))
      : [],
    tableRows: Array.isArray(ctx.tableRows)
      ? ctx.tableRows.map((row) => ({ ...row }))
      : [],
    constraints:
      ctx.constraints && typeof ctx.constraints === 'object'
        ? { ...ctx.constraints }
        : {},
    lastOutputType:
      ctx.lastOutputType != null ? String(ctx.lastOutputType) : null,
    pendingFields: Array.isArray(ctx.pendingFields)
      ? ctx.pendingFields.map(String)
      : [],
    nextAction: ctx.nextAction != null ? String(ctx.nextAction) : null,
  };
}

/**
 * @param {object} input
 * @param {object[]} input.prospects
 * @param {string} [input.campaignId]
 * @param {string} [input.lastOutputType]
 * @param {string|null} [input.nextAction]
 * @param {object} [input.prior]
 */
function buildCanaryActiveWorkContext(input = {}) {
  const prospects = Array.isArray(input.prospects) ? input.prospects : [];
  const prior = input.prior && typeof input.prior === 'object' ? input.prior : null;
  const campaignId =
    input.campaignId ||
    (prior && prior.target && prior.target.campaignId) ||
    '001';

  const tableRows = Array.isArray(input.tableRows)
    ? input.tableRows.map((row) => ({ ...row }))
    : prior && Array.isArray(prior.tableRows)
      ? prior.tableRows.map((row) => ({ ...row }))
      : [];

  return {
    workflow: 'campaign_canary',
    target: { campaignId: String(campaignId) },
    entities: prospects.map(prospectToEntity),
    tableRows,
    constraints: {
      ...DEFAULT_CANARY_CONSTRAINTS,
      ...(prior && prior.constraints && typeof prior.constraints === 'object'
        ? prior.constraints
        : {}),
      ...DEFAULT_CANARY_CONSTRAINTS,
    },
    lastOutputType:
      input.lastOutputType ||
      (prior && prior.lastOutputType) ||
      LAST_OUTPUT_TYPES.CANARY_REVIEW_PACKAGE,
    pendingFields: derivePendingFields(prospects),
    nextAction:
      input.nextAction != null
        ? input.nextAction
        : prior && prior.nextAction
          ? prior.nextAction
          : 'await_operator_transform_or_verification',
  };
}

/**
 * @param {object} prospect
 */
function prospectToEntity(prospect = {}) {
  return {
    type: 'prospect',
    id: prospect.id != null ? String(prospect.id) : null,
    companyName: blankToNull(prospect.companyName),
    contactName: blankToNull(prospect.contactName),
    industry: blankToNull(prospect.industry || prospect.vertical),
    website: blankToNull(prospect.website),
    mailingAddress: blankToNull(
      prospect.mailingAddress || prospect.address
    ),
    phone: blankToNull(prospect.phone),
  };
}

/**
 * @param {object[]} entities
 */
function entitiesToProspects(entities) {
  return (Array.isArray(entities) ? entities : [])
    .filter((e) => e && (e.type === 'prospect' || e.type == null))
    .map((e) => ({
      id: e.id || null,
      companyName: e.companyName || null,
      contactName: e.contactName || null,
      industry: e.industry || null,
      website: e.website || null,
      mailingAddress: e.mailingAddress || null,
      address: e.mailingAddress || null,
      phone: e.phone || null,
    }));
}

/**
 * @param {object[]} prospects
 */
function derivePendingFields(prospects) {
  const pending = new Set();
  for (const p of prospects || []) {
    if (!String(p.website || '').trim()) pending.add('website');
    if (!String(p.mailingAddress || p.address || '').trim()) {
      pending.add('mailingAddress');
    }
    if (!String(p.phone || '').trim()) pending.add('phone');
  }
  return [...pending];
}

/**
 * Operator is asking to reuse desk prospects ("same 3 prospects already listed"),
 * not paste a new list.
 * @param {string} text
 */
function isActiveWorkReuseProspectCue(text) {
  const lower = String(text || '').toLowerCase();
  if (!lower.trim()) return false;
  return (
    /\buse\s+the\s+same\s+(?:\d+\s+)?prospects?\b/.test(lower) ||
    /\b(?:the\s+)?same\s+(?:\d+\s+)?prospects?\b/.test(lower) ||
    /\bprospects?\s+already\s+listed\b/.test(lower) ||
    /\balready\s+listed\b/.test(lower)
  );
}

/**
 * Follow-up cues that should reuse activeWorkContext when no new paste is given.
 * @param {string} text
 */
function isActiveWorkFollowUpCue(text) {
  const lower = String(text || '').toLowerCase();
  if (!lower.trim()) return false;

  if (isActiveWorkReuseProspectCue(lower)) return true;
  if (isFillableTableRequest(lower)) return true;

  const cues = [
    /\bcontinue\b/,
    /\bconvert\s+this\b/,
    /\bconvert\s+the\s+(?:current\s+)?(?:campaign\s+\d+\s+)?(?:preparation[-\s]*only\s+)?canary\b/,
    /\bconvert\s+the\s+(?:verification\s+)?work\s+order\b/,
    /\bmake\s+it\s+a\s+table\b/,
    /\bupdate\s+(?:the\s+)?(?:fillable\s+)?(?:verification\s+)?table\b/,
    /\bkeep\s+the\s+same\s+(?:preparation[-\s]*only\s+)?constraints\b/,
    /\bkeep\s+the\s+same\s+constraints\b/,
    /\bturn\s+this\s+into\b/,
    /\bturn\s+it\s+into\b/,
    /\brevise\s+that\b/,
    /\bmake\s+it\s+more\s+concise\b/,
    /\bnext\s+step\b/,
    /\bwhat\s+should\s+i\s+do\s+first\b/,
  ];
  return cues.some((re) => re.test(lower));
}

/**
 * Strong transform cues that should clarify (ask for prospects) even when
 * desk context is missing — instead of falling through to General Conversation.
 * @param {string} text
 */
function isActiveWorkTransformCue(text) {
  const lower = String(text || '').toLowerCase();
  return (
    isFillableTableRequest(lower) ||
    /\bconvert\s+the\s+(?:verification\s+)?work\s+order\b/.test(lower) ||
    /\bverification\s+work\s+order\b/.test(lower)
  );
}

/**
 * Explicit new mission / campaign work — must not be intercepted by desk context.
 * @param {string} text
 */
function isExplicitNewMissionRequest(text) {
  const lower = String(text || '').toLowerCase();
  return (
    /\bbuild\s+campaign\b/.test(lower) ||
    /\bcreate\s+(?:a\s+)?(?:new\s+)?(?:campaign|mission)\b/.test(lower) ||
    /\bstart\s+(?:a\s+)?(?:new\s+)?(?:campaign|mission|direct\s+mail)\b/.test(
      lower
    )
  );
}

/**
 * Operator is replacing prior entities / campaign / starting over.
 * @param {string} text
 */
function isExplicitContextOverride(text) {
  const lower = String(text || '').toLowerCase();
  return (
    /\bstart\s+over\b/.test(lower) ||
    /\buse\s+these\s+\d+\s+prospects?\s+instead\b/.test(lower) ||
    (/\binstead\b/.test(lower) && /\b(?:prospects?|campaign)\b/.test(lower)) ||
    /\buse\s+a\s+different\s+campaign\b/.test(lower) ||
    /\bdifferent\s+campaign\b/.test(lower)
  );
}

/**
 * Explicit launch / mail / execute / approve language.
 * Active context never infers execution — these still require readiness checks.
 * @param {string} text
 */
function isExplicitExecutionRequest(text) {
  const lower = String(text || '').toLowerCase();
  return (
    /\bmail\s+(?:these|them|it|this)\s+now\b/.test(lower) ||
    /\b(?:please\s+)?mail\s+(?:these|them)\b/.test(lower) ||
    /\blaunch\s+(?:these|them|it|this|now)\b/.test(lower) ||
    /\bactually\s+launch\b/.test(lower) ||
    /\bexecute\s+(?:these|them|it|this|now|the\s+mail)\b/.test(lower) ||
    /\bapprove\s+(?:to\s+)?(?:mail|print|launch)\b/.test(lower) ||
    /\bprint\s+(?:and\s+)?mail\b/.test(lower) ||
    /\bsend\s+(?:these|them)\s+(?:out\s+)?now\b/.test(lower)
  );
}

/**
 * @param {string} text
 */
function isFillableTableRequest(text) {
  const lower = String(text || '').toLowerCase();
  // Table field mutations are handled separately — do not treat as a fresh
  // fillable-table create/regenerate request.
  if (isFillableTableUpdateRequest(text)) return false;
  return (
    /\bfillable\s+(?:verification\s+)?table\b/.test(lower) ||
    /\bmake\s+it\s+a\s+(?:fillable\s+)?(?:verification\s+)?table\b/.test(lower) ||
    /\bconvert\b[\s\S]{0,160}\b(?:into|to)\s+a\s+(?:fillable\s+)?(?:verification\s+)?table\b/.test(
      lower
    ) ||
    /\bconvert\s+the\s+(?:verification\s+)?work\s+order\s+into\s+a\s+(?:fillable\s+)?(?:verification\s+)?table\b/.test(
      lower
    ) ||
    /\bturn\s+(?:this|it|the\s+(?:current\s+)?(?:preparation[-\s]*only\s+)?canary|the\s+(?:verification\s+)?work\s+order)\s+into\s+a\s+(?:fillable\s+)?(?:verification\s+)?table\b/.test(
      lower
    )
  );
}

/** Column names accepted in fillable verification table mutations. */
const FILLABLE_TABLE_MUTABLE_FIELDS = Object.freeze([
  'prospect_id',
  'company_name',
  'contact_name',
  'contact_role_status',
  'website_status',
  'website_value',
  'mailing_address_status',
  'mailing_address_value',
  'phone_status',
  'phone_value',
  'source_to_check_first',
  'verification_status',
  'mail_readiness',
  'draft_readiness',
  'execution_readiness',
  'operator_next_action',
  'notes',
]);

/**
 * True when activeWorkContext already has a fillable verification table.
 * @param {object|null} ctx
 */
function activeContextHasFillableTable(ctx) {
  if (!ctx) return false;
  if (ctx.lastOutputType === LAST_OUTPUT_TYPES.FILLABLE_TABLE) return true;
  return Array.isArray(ctx.tableRows) && ctx.tableRows.length > 0;
}

/**
 * Known prospect ids currently on the desk.
 * @param {object|null} ctx
 * @returns {string[]}
 */
function knownActiveWorkProspectIds(ctx) {
  const ids = [];
  const seen = new Set();
  const push = (raw) => {
    if (raw == null) return;
    const id = String(raw).trim();
    if (!id) return;
    const key = id.toUpperCase();
    if (seen.has(key)) return;
    seen.add(key);
    ids.push(id);
  };
  if (ctx && Array.isArray(ctx.tableRows)) {
    for (const row of ctx.tableRows) {
      if (row) push(row.prospect_id || row.id);
    }
  }
  if (ctx && Array.isArray(ctx.entities)) {
    for (const entity of ctx.entities) {
      if (entity) push(entity.id);
    }
  }
  return ids;
}

/**
 * Operator asked for a strict fillable-table output shape:
 * table (+ optional short safety line), no heading/explanation/reasoning/next.
 * @param {string} text
 * @returns {boolean}
 */
function wantsStrictFillableTableOutputShape(text) {
  const lower = String(text || '').toLowerCase();
  if (!lower.trim()) return false;

  const onlyTable =
    /\breturn\s+only\s+(?:the\s+)?(?:updated\s+)?table\b/.test(lower) ||
    /\bonly\s+(?:the\s+)?(?:updated\s+)?table\b/.test(lower) ||
    /\bjust\s+(?:the\s+)?(?:updated\s+)?table\b/.test(lower) ||
    /\btable\s+only\b/.test(lower);

  const tablePlusSafety =
    /\b(?:updated\s+)?table\s+plus\s+(?:one\s+)?(?:short\s+)?(?:preparation[-\s]*only\s+)?safety\s+line\b/.test(
      lower
    ) ||
    /\bonly\s+(?:the\s+)?(?:updated\s+)?table\s+plus\s+(?:one\s+)?(?:short\s+)?/.test(
      lower
    ) ||
    /\breturn\s+only\s+(?:the\s+)?(?:updated\s+)?table\s+plus\b/.test(lower);

  const noReasoning =
    /\bno\s+reasoning\b/.test(lower) ||
    /\bwithout\s+reasoning\b/.test(lower) ||
    /\bdo\s+not\s+include\s+reasoning\b/.test(lower);

  const noExplanation =
    /\bno\s+explanation\b/.test(lower) ||
    /\bwithout\s+explanation\b/.test(lower) ||
    /\bno\s+explanatory\b/.test(lower) ||
    /\bdo\s+not\s+explain\b/.test(lower);

  const noNext =
    /\bno\s+next\s+steps?\b/.test(lower) ||
    /\bdo\s+not\s+include\s+next\s+steps?\b/.test(lower) ||
    /\bno\s+next\s+action\b/.test(lower) ||
    /\bwithout\s+next\s+steps?\b/.test(lower);

  return (
    onlyTable || tablePlusSafety || noReasoning || noExplanation || noNext
  );
}

/**
 * Operator explicitly asked for a heading on the fillable table response.
 * @param {string} text
 * @returns {boolean}
 */
function wantsFillableTableHeading(text) {
  const lower = String(text || '').toLowerCase();
  return (
    /\binclude\s+(?:a\s+)?heading\b/.test(lower) ||
    /\bwith\s+(?:a\s+)?heading\b/.test(lower) ||
    /\bkeep\s+(?:the\s+)?heading\b/.test(lower) ||
    /\badd\s+(?:a\s+)?heading\b/.test(lower)
  );
}

/**
 * Operator is mutating fields on an existing fillable verification table.
 * Must be detected before prospect extraction / artifact injection.
 * @param {string} text
 * @param {object|null} [activeWorkContext]
 */
function isFillableTableUpdateRequest(text, activeWorkContext = null) {
  const raw = String(text || '');
  const lower = raw.toLowerCase();
  if (!lower.trim()) return false;

  const updateCue =
    /\bupdate\s+(?:the\s+)?(?:fillable\s+)?(?:verification\s+)?table\b/.test(
      lower
    ) ||
    /\bedit\s+(?:the\s+)?(?:fillable\s+)?(?:verification\s+)?table\b/.test(
      lower
    ) ||
    /\bupdate\s+(?:the\s+)?fillable\s+verification\s+table\b/.test(lower);

  const setCue =
    /\bset\s*:/.test(lower) ||
    /\bfor\s+\S+\s+only\b[,:]?\s*(?:set\b)?/i.test(raw);
  const leaveUnchanged = /\bleave\b[\s\S]{0,80}\bunchanged\b/i.test(raw);
  const fieldCue = FILLABLE_TABLE_MUTABLE_FIELDS.some((field) =>
    new RegExp(`\\b${field}\\b`, 'i').test(raw)
  );

  const knownIds = knownActiveWorkProspectIds(activeWorkContext);
  const knownIdCue =
    knownIds.length > 0 &&
    knownIds.some((id) =>
      new RegExp(`\\b${escapeRegExp(id)}\\b`, 'i').test(raw)
    );

  // Strong explicit update phrasing — even before tableRows are required
  // by the caller (caller still gates on active fillable table context).
  if (updateCue && (setCue || leaveUnchanged || fieldCue || knownIdCue)) {
    return true;
  }
  if (updateCue && /\bfillable\b/.test(lower)) return true;

  // Desk already has a fillable table and the operator is setting fields
  // on a known prospect id.
  if (
    activeContextHasFillableTable(activeWorkContext) &&
    knownIdCue &&
    fieldCue &&
    (setCue || leaveUnchanged || updateCue || /\bset\b/.test(lower))
  ) {
    return true;
  }

  return false;
}

/**
 * Parse per-prospect field mutations from an update instruction.
 * Does not treat instruction labels as prospect rows.
 * @param {string} text
 * @returns {{ updates: Array<{ prospectId: string, fields: Record<string, string> }>, referencedIds: string[] }}
 */
function parseFillableTableFieldUpdates(text) {
  const lines = String(text || '').split(/\r?\n/);
  const columnSet = new Set(FILLABLE_TABLE_MUTABLE_FIELDS);
  /** @type {Map<string, Record<string, string>>} */
  const updatesById = new Map();
  /** @type {string[]} */
  const referencedIds = [];
  const seenRefs = new Set();
  let currentId = null;

  const rememberRef = (id) => {
    const key = String(id).toUpperCase();
    if (seenRefs.has(key)) return;
    seenRefs.add(key);
    referencedIds.push(String(id));
  };

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const forOnly =
      /^for\s+([A-Za-z0-9_-]+)\s+only\b[,:]?\s*(?:set\s*:?)?$/i.exec(trimmed) ||
      /^for\s+([A-Za-z0-9_-]+)\s+only\b[,:]?\s*set\s*:?\s*$/i.exec(trimmed);
    const forSet = /^for\s+([A-Za-z0-9_-]+)\s*,?\s*set\s*:?\s*$/i.exec(trimmed);
    if (forOnly || forSet) {
      currentId = (forOnly || forSet)[1];
      rememberRef(currentId);
      if (!updatesById.has(currentId)) updatesById.set(currentId, {});
      continue;
    }

    if (
      /^leave\b/i.test(trimmed) ||
      /^keep\s+this\b/i.test(trimmed) ||
      /^do\s+not\b/i.test(trimmed) ||
      /^update\s+the\b/i.test(trimmed) ||
      /^return\b/i.test(trimmed) ||
      /^preparation[-\s]*only\b/i.test(trimmed)
    ) {
      // Capture ids in "Leave PM-002 and PM-003 unchanged" without treating
      // them as mutation targets.
      const leaveIds = trimmed.match(/\b[A-Za-z]{1,12}-?\d{1,6}\b/g) || [];
      if (/^leave\b/i.test(trimmed)) {
        for (const id of leaveIds) rememberRef(id);
      }
      currentId = null;
      continue;
    }

    if (/^set\s*:?\s*$/i.test(trimmed)) {
      continue;
    }

    const fieldMatch = /^-?\s*([a-z][a-z0-9_]*)\s*:\s*(.*)$/i.exec(trimmed);
    if (fieldMatch && currentId) {
      const field = fieldMatch[1].toLowerCase();
      if (columnSet.has(field)) {
        if (!updatesById.has(currentId)) updatesById.set(currentId, {});
        updatesById.get(currentId)[field] = String(fieldMatch[2] || '').trim();
      }
      continue;
    }
  }

  return {
    updates: [...updatesById.entries()].map(([prospectId, fields]) => ({
      prospectId,
      fields,
    })),
    referencedIds,
  };
}

/**
 * Apply parsed field updates onto existing fillable table rows.
 * Preserves row order, shape, and untouched rows.
 * @param {object[]} rows
 * @param {Array<{ prospectId: string, fields: Record<string, string> }>} updates
 * @returns {{ rows: object[], matchedIds: string[], unknownIds: string[] }}
 */
function applyFillableTableFieldUpdates(rows, updates) {
  const next = (Array.isArray(rows) ? rows : []).map((row) => ({ ...row }));
  const indexById = new Map();
  next.forEach((row, i) => {
    const id = String(row.prospect_id || row.id || '').trim();
    if (id) indexById.set(id.toUpperCase(), i);
  });

  const matchedIds = [];
  const unknownIds = [];
  const columnSet = new Set(FILLABLE_TABLE_MUTABLE_FIELDS);

  for (const update of updates || []) {
    const prospectId = String(update.prospectId || '').trim();
    if (!prospectId) continue;
    const idx = indexById.get(prospectId.toUpperCase());
    if (idx == null) {
      unknownIds.push(prospectId);
      continue;
    }
    matchedIds.push(prospectId);
    const row = { ...next[idx] };
    const fields = update.fields && typeof update.fields === 'object' ? update.fields : {};
    for (const [key, value] of Object.entries(fields)) {
      const field = String(key).toLowerCase();
      if (!columnSet.has(field)) continue;
      // Never invent readiness — only apply explicit operator values.
      row[field] = value == null ? '' : String(value);
    }
    // Keep prospect_id stable unless operator explicitly changes it.
    if (!Object.prototype.hasOwnProperty.call(fields, 'prospect_id')) {
      row.prospect_id = next[idx].prospect_id || prospectId;
    }
    next[idx] = row;
  }

  return { rows: next, matchedIds, unknownIds };
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * @param {string} text
 * @returns {string|null}
 */
function extractCampaignIdFromText(text) {
  const match = /\bcampaign\s+(\d+)\b/i.exec(String(text || ''));
  if (!match) return null;
  const n = String(match[1]);
  return n.length >= 3 ? n : n.padStart(3, '0');
}

/**
 * True when active canary constraints still forbid execution/mail.
 * @param {object|null} ctx
 */
function activeContextBlocksExecution(ctx) {
  if (!ctx || ctx.workflow !== 'campaign_canary') return false;
  const c = ctx.constraints || {};
  return (
    c.preparationOnly === true ||
    c.noExecution === true ||
    c.noMail === true ||
    c.noLaunch === true ||
    c.noPrint === true
  );
}

/**
 * @param {object|null} ctx
 */
function activeContextHasEntities(ctx) {
  return Boolean(
    ctx &&
      Array.isArray(ctx.entities) &&
      ctx.entities.length > 0
  );
}

function blankToNull(value) {
  if (value == null) return null;
  const s = String(value).trim();
  if (!s || /^unknown$/i.test(s) || /^n\/?a$/i.test(s)) return null;
  return s;
}

module.exports = {
  DEFAULT_CANARY_CONSTRAINTS,
  LAST_OUTPUT_TYPES,
  FILLABLE_TABLE_MUTABLE_FIELDS,
  getActiveWorkContext,
  setActiveWorkContext,
  cloneActiveWorkContext,
  buildCanaryActiveWorkContext,
  prospectToEntity,
  entitiesToProspects,
  derivePendingFields,
  isActiveWorkReuseProspectCue,
  isActiveWorkFollowUpCue,
  isActiveWorkTransformCue,
  isExplicitNewMissionRequest,
  isExplicitContextOverride,
  isExplicitExecutionRequest,
  isFillableTableRequest,
  isFillableTableUpdateRequest,
  wantsStrictFillableTableOutputShape,
  wantsFillableTableHeading,
  activeContextHasFillableTable,
  knownActiveWorkProspectIds,
  parseFillableTableFieldUpdates,
  applyFillableTableFieldUpdates,
  extractCampaignIdFromText,
  activeContextBlocksExecution,
  activeContextHasEntities,
};
