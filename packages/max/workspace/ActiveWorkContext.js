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
  PACKET_REVIEW: 'packet_review',
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
    lastOutputKind:
      ctx.lastOutputKind != null ? String(ctx.lastOutputKind) : null,
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

  const lastOutputType =
    input.lastOutputType ||
    (prior && prior.lastOutputType) ||
    LAST_OUTPUT_TYPES.CANARY_REVIEW_PACKAGE;
  const lastOutputKind =
    input.lastOutputKind != null
      ? String(input.lastOutputKind)
      : lastOutputType === LAST_OUTPUT_TYPES.FILLABLE_TABLE ||
          lastOutputType === 'fillable_verification_table'
        ? 'fillable_verification_table'
        : prior && prior.lastOutputKind
          ? String(prior.lastOutputKind)
          : null;

  return {
    workflow:
      input.workflow ||
      (prior && prior.workflow) ||
      'campaign_canary',
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
    lastOutputType,
    lastOutputKind,
    pendingFields: derivePendingFields(prospects),
    nextAction:
      input.nextAction != null
        ? input.nextAction
        : prior && prior.nextAction
          ? prior.nextAction
          : 'await_operator_transform_or_verification',
  };
}

/** Canonical workflow id for Campaign 001 preparation-only canary desk work. */
const CAMPAIGN_001_PREPARATION_ONLY_CANARY =
  'campaign_001_preparation_only_canary';

/**
 * True when workflow is any preparation-only canary desk workflow.
 * @param {object|null|undefined} ctx
 */
function isCanaryDeskWorkflow(ctx) {
  if (!ctx || typeof ctx !== 'object') return false;
  const workflow = String(ctx.workflow || '').toLowerCase();
  return (
    workflow === 'campaign_canary' ||
    workflow === CAMPAIGN_001_PREPARATION_ONLY_CANARY ||
    (/canary/.test(workflow) && /preparation/.test(workflow))
  );
}

/**
 * Resolve desk workflow id from operator cues.
 * @param {string} text
 * @param {object|null} [prior]
 */
function resolveCanaryDeskWorkflow(text, prior = null) {
  const lower = String(text || '').toLowerCase();
  const campaignCue = /\bcampaign\s+0*1\b/.test(lower) || /\bcampaign\s+001\b/.test(lower);
  const canaryCue = /\bcanary\b/.test(lower);
  const prepCue =
    /\bpreparation[-\s]*only\b/.test(lower) ||
    /\bprep[-\s]*only\b/.test(lower);
  if (campaignCue || canaryCue || prepCue) {
    return CAMPAIGN_001_PREPARATION_ONLY_CANARY;
  }
  if (prior && isCanaryDeskWorkflow(prior)) {
    return String(prior.workflow);
  }
  return 'campaign_canary';
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
  if (isPacketReviewRequest(lower)) return true;

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
 * Operator wants a preparation-only packet review artifact from the desk table
 * (checklist / drafts / tracking) — not a new prospect paste and not an initial
 * multi-prospect canary package deliverable list.
 * @param {string} text
 */
function isPacketReviewRequest(text) {
  const lower = String(text || '').toLowerCase();
  if (!lower.trim()) return false;

  // Strong cues: explicit packet-review artifact generation.
  if (
    /\bpacket\s+review(?:\s+checklist)?\b/.test(lower) ||
    /\boperator\s+packet\s+review\b/.test(lower) ||
    /\bcreate\s+(?:a\s+)?(?:preparation[-\s]*only\s+)?packet(?:\s+review)?\b/.test(
      lower
    ) ||
    /\bdraft\s+(?:a\s+)?(?:PM-\d{3}\s+)?packet(?:\s+for\s+review)?\b/i.test(
      text
    ) ||
    /\bpacket\s+for\s+review\b/.test(lower) ||
    /\bprint\s*[\/-]?\s*sign\s*[\/-]?\s*mail\s+checklist\b/.test(lower) ||
    (/\buse\s+the\s+current\b/.test(lower) &&
      /\b(?:canary\s+)?table\b/.test(lower) &&
      /\bpacket\b/.test(lower))
  ) {
    return true;
  }

  // Secondary cues only when targeting a named desk prospect.
  // Avoid matching initial canary package lists that mention letter/note/cover
  // as deliverables without asking to generate a packet review for PM-00x.
  const namedProspect = /\b(?:for|of)\s+PM-\d{3}\b/i.test(text);
  if (!namedProspect) return false;

  return (
    /\bpersonalized\s+letter(?:\s+draft)?\b/.test(lower) ||
    /\bhandwritten\s+note(?:\s+draft)?\b/.test(lower) ||
    /\bscorecard\s+cover(?:\s+text)?(?:\s+draft)?\b/.test(lower) ||
    /\bfirst\s+follow[-\s]?up\s+call\s+notes\b/.test(lower) ||
    (/\btracking\s+fields?\b/.test(lower) &&
      /\b(?:after\s+mailing|once\s+mailed|to\s+log)\b/.test(lower))
  );
}

/**
 * Resolve which desk prospect a packet-review request targets.
 * @param {string} text
 * @param {string[]} [knownIds]
 * @returns {string|null}
 */
function extractPacketReviewProspectId(text, knownIds = []) {
  const raw = String(text || '');
  const known = (Array.isArray(knownIds) ? knownIds : [])
    .map((id) => String(id || '').trim())
    .filter(Boolean);
  const knownUpper = new Set(known.map((id) => id.toUpperCase()));

  const patterns = [
    /\b(?:for|of)\s+(PM-\d{3})\b/i,
    /\b(PM-\d{3})\s+(?:only|packet|review)\b/i,
    /\bpacket(?:\s+review)?(?:\s+checklist)?\s+for\s+(PM-\d{3})\b/i,
    /\bdraft\s+(PM-\d{3})\s+packet\b/i,
    /\b(PM-\d{3})\b/i,
  ];
  for (const re of patterns) {
    const match = re.exec(raw);
    if (!match) continue;
    const id = String(match[1] || '').trim();
    if (!id) continue;
    if (knownUpper.size === 0 || knownUpper.has(id.toUpperCase())) return id;
    // Named id not on desk — still return so caller can clarify unknown.
    return id;
  }

  // Single-row desk: allow omitting the id.
  if (known.length === 1) return known[0];
  return null;
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
    isFillableTableUpdateRequest(text) ||
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

/**
 * Operator-owned source columns. Reassessment must not rewrite these unless
 * the operator explicitly assigns the field in the same turn.
 */
const FILLABLE_TABLE_SOURCE_FIELDS = Object.freeze([
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
  'notes',
]);

/**
 * Readiness / next-action columns derived from source gate statuses.
 * Reassessment may rewrite these; explicit launch/approval owns execution.
 */
const FILLABLE_TABLE_DERIVED_FIELDS = Object.freeze([
  'verification_status',
  'mail_readiness',
  'draft_readiness',
  'execution_readiness',
  'operator_next_action',
]);

/** Column names accepted in fillable verification table mutations. */
const FILLABLE_TABLE_MUTABLE_FIELDS = Object.freeze([
  ...FILLABLE_TABLE_SOURCE_FIELDS,
  ...FILLABLE_TABLE_DERIVED_FIELDS,
]);

/** Gate status columns that drive derived readiness / next-action fields. */
const FILLABLE_TABLE_GATE_STATUS_FIELDS = Object.freeze([
  'website_status',
  'mailing_address_status',
  'phone_status',
  'contact_role_status',
]);

/** Free-text source fields — nested `field = value` inside the value is content. */
const FILLABLE_TABLE_FREE_TEXT_FIELDS = Object.freeze(['notes']);

/**
 * True when activeWorkContext already has a fillable verification table.
 * @param {object|null} ctx
 */
function activeContextHasFillableTable(ctx) {
  if (!ctx) return false;
  if (ctx.lastOutputType === LAST_OUTPUT_TYPES.FILLABLE_TABLE) return true;
  if (ctx.lastOutputType === 'fillable_verification_table') return true;
  if (ctx.lastOutputKind === 'fillable_verification_table') return true;
  if (ctx.lastOutputKind === 'fillable_table') return true;
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

  // Whole-table reassessment should stay table + one safety line.
  const wholeTableReassess = isFillableTableWholeTableReassessRequest(text);

  return (
    onlyTable ||
    tablePlusSafety ||
    noReasoning ||
    noExplanation ||
    noNext ||
    wholeTableReassess
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
 * Operator asked to reassess the whole fillable / canary verification table
 * (not a single named prospect readiness line / table-gates cue).
 * @param {string} text
 */
function isFillableTableWholeTableReassessRequest(text) {
  const raw = String(text || '');
  if (!raw.trim()) return false;
  // Do not treat "reassess … using the table gates" as a whole-table cue.
  if (/\busing\s+(?:the\s+)?table\s+gates\b/i.test(raw)) return false;

  if (
    /\breassess\b[\s\S]{0,160}\b(?:the\s+)?(?:campaign\s+\d+\s+)?(?:preparation[-\s]*only\s+)?canary\s+table\b/i.test(
      raw
    )
  ) {
    return true;
  }
  if (
    /\breassess\b[\s\S]{0,120}\b(?:the\s+)?(?:fillable\s+)?verification\s+table\b/i.test(
      raw
    )
  ) {
    return true;
  }
  if (/\breassess\b[\s\S]{0,80}\b(?:the\s+)?fillable\s+table\b/i.test(raw)) {
    return true;
  }
  // Bare "reassess the table" without a readiness-for-id framing.
  if (
    /\breassess\b[\s\S]{0,60}\bthe\s+table\b/i.test(raw) &&
    !/\breadiness\b/i.test(raw)
  ) {
    return true;
  }
  return false;
}

/**
 * Operator asked to recompute readiness columns from fillable-table gate statuses.
 * @param {string} text
 */
function isFillableTableReadinessReassessRequest(text) {
  const raw = String(text || '');
  if (!raw.trim()) return false;
  return (
    /\breassess\b[\s\S]{0,120}\breadiness\b/i.test(raw) ||
    /\breadiness\b[\s\S]{0,120}\breassess\b/i.test(raw) ||
    /\breassess\b[\s\S]{0,120}\btable\s+gates\b/i.test(raw) ||
    /\busing\s+(?:the\s+)?table\s+gates\b/i.test(raw) ||
    isFillableTableWholeTableReassessRequest(raw)
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
    new RegExp(`\\b${escapeRegExp(field)}\\b`, 'i').test(raw)
  );
  // Explicit assignment syntax: website_status = verified OR website_status: verified
  const fieldAssignmentCue = FILLABLE_TABLE_MUTABLE_FIELDS.some((field) =>
    new RegExp(`\\b${escapeRegExp(field)}\\s*[=:]\\s*\\S`, 'i').test(raw)
  );
  const strictShapeCue = wantsStrictFillableTableOutputShape(raw);
  const prospectOnlyCue = /\bfor\s+[A-Za-z0-9_-]+\s+only\b/i.test(raw);
  const reassessCue = isFillableTableReadinessReassessRequest(raw);

  const knownIds = knownActiveWorkProspectIds(activeWorkContext);
  const knownIdCue =
    knownIds.length > 0 &&
    knownIds.some((id) =>
      new RegExp(`\\b${escapeRegExp(id)}\\b`, 'i').test(raw)
    );

  // Strong explicit update phrasing — even when desk context is missing
  // (caller must clarify rather than fall through to briefing / General).
  if (
    updateCue &&
    (setCue ||
      leaveUnchanged ||
      fieldCue ||
      fieldAssignmentCue ||
      knownIdCue ||
      prospectOnlyCue ||
      strictShapeCue ||
      reassessCue)
  ) {
    return true;
  }
  if (updateCue && /\bfillable\b/.test(lower)) return true;

  // Desk already has a fillable table and the operator is setting fields
  // on a known prospect id.
  if (
    activeContextHasFillableTable(activeWorkContext) &&
    knownIdCue &&
    (fieldCue || fieldAssignmentCue) &&
    (setCue || leaveUnchanged || updateCue || /\bset\b/.test(lower) || reassessCue)
  ) {
    return true;
  }

  // Pure readiness / whole-table reassessment (desk preferred; missing desk
  // clarifies instead of falling through to mission / General Conversation).
  if (reassessCue) {
    if (activeContextHasFillableTable(activeWorkContext)) return true;
    if (
      isFillableTableWholeTableReassessRequest(raw) ||
      /\breassess\b[\s\S]{0,120}\breadiness\b/i.test(raw) ||
      knownIdCue ||
      prospectOnlyCue ||
      updateCue
    ) {
      return true;
    }
  }

  return false;
}

/**
 * @param {unknown} value
 */
function isVerifiedGateStatus(value) {
  return /^verified$/i.test(String(value || '').trim());
}

/**
 * @param {unknown} value
 */
function isNeedsVerificationGateStatus(value) {
  return /^needs\s+verification$/i.test(String(value || '').trim());
}

/**
 * @param {unknown} value
 */
function isBlockedGateStatus(value) {
  return /^blocked$/i.test(String(value || '').trim());
}

/**
 * True when an explicit non-gate mail blocker is recorded on the row.
 * Contact-role verification is intentionally not a mail blocker.
 * @param {object} row
 */
function hasExplicitMailBlocker(row) {
  if (!row || typeof row !== 'object') return false;
  const mail = String(row.mail_readiness || '').trim().toLowerCase();
  if (mail === 'blocked_by_operator' || mail === 'do_not_mail') return true;
  const notes = String(row.notes || '');
  return /\b(?:mail|mailing)\s+(?:explicitly\s+)?blocked\b/i.test(notes);
}

/**
 * Derive operator_next_action from remaining table-gate blockers.
 * @param {object} row
 * @returns {string}
 */
function deriveOperatorNextActionFromGates(row) {
  if (!isVerifiedGateStatus(row && row.mailing_address_status)) {
    return 'verify mailing address first';
  }
  if (!isVerifiedGateStatus(row && row.website_status)) {
    return 'verify website next';
  }
  if (!isVerifiedGateStatus(row && row.phone_status)) {
    return 'verify phone next';
  }
  if (
    isNeedsVerificationGateStatus(row && row.contact_role_status) ||
    isBlockedGateStatus(row && row.contact_role_status)
  ) {
    return 'confirm contact role';
  }
  return 'review packet contents / prepare print checklist';
}

/**
 * Prospect ids whose gate status fields were mutated in this turn.
 * @param {Array<{ prospectId: string, fields: Record<string, string> }>} updates
 * @returns {string[]}
 */
function extractGateStatusUpdatedProspectIds(updates) {
  const gateSet = new Set(FILLABLE_TABLE_GATE_STATUS_FIELDS);
  const ids = [];
  const seen = new Set();
  for (const update of updates || []) {
    const prospectId = String(update.prospectId || '').trim();
    if (!prospectId) continue;
    const fields =
      update.fields && typeof update.fields === 'object' ? update.fields : {};
    const touched = Object.keys(fields).some((key) =>
      gateSet.has(String(key).toLowerCase())
    );
    if (!touched) continue;
    const key = prospectId.toUpperCase();
    if (seen.has(key)) continue;
    seen.add(key);
    ids.push(prospectId);
  }
  return ids;
}

/**
 * Snapshot operator-owned source columns so reassessment cannot rewrite them.
 * @param {object} row
 * @returns {Record<string, unknown>}
 */
function snapshotFillableTableSourceFields(row) {
  /** @type {Record<string, unknown>} */
  const snapshot = {};
  const source = row && typeof row === 'object' ? row : {};
  for (const field of FILLABLE_TABLE_SOURCE_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(source, field)) {
      snapshot[field] = source[field];
    }
  }
  return snapshot;
}

/**
 * Restore source columns after derived-field recomputation.
 * @param {object} row
 * @param {Record<string, unknown>} snapshot
 * @returns {object}
 */
function restoreFillableTableSourceFields(row, snapshot) {
  const next = row && typeof row === 'object' ? { ...row } : {};
  for (const [field, value] of Object.entries(snapshot || {})) {
    next[field] = value;
  }
  return next;
}

/**
 * Recompute derived readiness columns from verified table gates.
 * Never mutates operator-owned source fields (gate statuses, values, notes).
 * Does not invent websites, phones, addresses, or launch/execution.
 * @param {object} row
 * @returns {object}
 */
function reassessFillableTableRowFromGates(row) {
  const sourceSnapshot = snapshotFillableTableSourceFields(row);
  const next = row && typeof row === 'object' ? { ...row } : {};
  const mailGatesVerified =
    isVerifiedGateStatus(next.mailing_address_status) &&
    isVerifiedGateStatus(next.website_status) &&
    isVerifiedGateStatus(next.phone_status);
  const contactRoleVerified = isVerifiedGateStatus(next.contact_role_status);
  const allVerificationGatesVerified = mailGatesVerified && contactRoleVerified;

  if (mailGatesVerified && !hasExplicitMailBlocker(next)) {
    next.mail_readiness = 'ready_for_review';
  } else if (!mailGatesVerified) {
    next.mail_readiness = 'blocked';
  }

  // Draft readiness stays allowed unless the operator already changed it.
  if (!String(next.draft_readiness || '').trim()) {
    next.draft_readiness = 'allowed';
  }

  // Execution never advances from table-gate reassessment alone.
  next.execution_readiness = 'blocked';

  if (allVerificationGatesVerified) {
    // All verification gates passed — advance past "needs verification".
    // ready_for_review is reserved for mail_readiness / packet review stage.
    next.verification_status = 'verified';
  } else {
    next.verification_status = 'needs verification';
  }

  next.operator_next_action = deriveOperatorNextActionFromGates(next);

  // Source fields are operator truth — restore after derived writes.
  return restoreFillableTableSourceFields(next, sourceSnapshot);
}

/**
 * Prospect ids named in a readiness reassessment instruction.
 * @param {string} text
 * @returns {string[]}
 */
function extractReadinessReassessProspectIds(text) {
  const raw = String(text || '');
  const ids = [];
  const seen = new Set();
  const patterns = [
    /\breassess\s+([A-Za-z0-9_-]+)\s+readiness\b/gi,
    /\breassess\s+readiness\s+for\s+([A-Za-z0-9_-]+)\b/gi,
    /\bfor\s+([A-Za-z0-9_-]+)\s+only\b/gi,
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(raw)) !== null) {
      const id = String(m[1] || '').trim();
      if (!id) continue;
      const key = id.toUpperCase();
      if (seen.has(key)) continue;
      seen.add(key);
      ids.push(id);
    }
  }
  return ids;
}

/**
 * Parse "For PM-001 only, set website_status: verified" or
 * "For PM-001 only: website_status = verified" as an inline assignment.
 * Returns null for header-only lines like "For PM-001 only, set:".
 * @param {string} trimmed
 * @returns {{ prospectId: string, fieldText: string }|null}
 */
function matchForOnlyInlineFieldAssignment(trimmed) {
  const text = String(trimmed || '');
  if (!text) return null;
  const columnSet = new Set(FILLABLE_TABLE_MUTABLE_FIELDS);

  const withSet =
    /\bfor\s+([A-Za-z0-9_-]+)\s+only\b[,:]?\s*set\s*:?\s*([a-z][a-z0-9_]*)\s*[:=]\s*(.+)$/i.exec(
      text
    );
  if (withSet && columnSet.has(withSet[2].toLowerCase())) {
    return {
      prospectId: withSet[1],
      fieldText: `${withSet[2]}=${withSet[3]}`,
    };
  }

  const withoutSet =
    /\bfor\s+([A-Za-z0-9_-]+)\s+only\b[,:]\s*([a-z][a-z0-9_]*)\s*[:=]\s*(.+)$/i.exec(
      text
    );
  if (withoutSet && columnSet.has(withoutSet[2].toLowerCase())) {
    return {
      prospectId: withoutSet[1],
      fieldText: `${withoutSet[2]}=${withoutSet[3]}`,
    };
  }

  return null;
}

/**
 * Parse inline "field = value, field2 = value2" assignments.
 * Values run until the next known mutable column assignment (or end of chunk).
 * Free-text fields (notes) consume the remainder — nested field names are content.
 * Semicolons/commas inside free-text notes are content, not separators.
 * @param {string} fieldText
 * @param {Set<string>} columnSet
 * @returns {Record<string, string>}
 */
function parseMutableFieldAssignments(fieldText, columnSet) {
  const chunk = String(fieldText || '').trim();
  if (!chunk || !columnSet || columnSet.size === 0) return {};

  const freeTextFields = new Set(FILLABLE_TABLE_FREE_TEXT_FIELDS);
  const fieldAlt = [...columnSet]
    .map(escapeRegExp)
    .sort((a, b) => b.length - a.length)
    .join('|');
  const startRe = new RegExp(`\\b(${fieldAlt})\\s*[:=]\\s*`, 'gi');

  /** @type {Array<{ field: string, valueStart: number, matchStart: number }>} */
  const starts = [];
  let m;
  while ((m = startRe.exec(chunk)) !== null) {
    // Once a free-text field (notes) begins, later `field =` matches are content.
    if (starts.length > 0) {
      const prev = starts[starts.length - 1];
      if (freeTextFields.has(prev.field)) break;
    }
    starts.push({
      field: m[1].toLowerCase(),
      valueStart: m.index + m[0].length,
      matchStart: m.index,
    });
  }

  /** @type {Record<string, string>} */
  const fields = {};
  for (let i = 0; i < starts.length; i++) {
    const end =
      i + 1 < starts.length ? starts[i + 1].matchStart : chunk.length;
    let value = chunk.slice(starts[i].valueStart, end).trim();
    // Drop the comma/semicolon that delimited this value from the next field.
    value = value.replace(/[,;]\s*$/, '').trim();
    // Drop trailing sentence punctuation at an instruction boundary.
    value = value.replace(/[.\s]+$/, '').trim();
    // Status / short values must not swallow a following reassess instruction.
    if (!freeTextFields.has(starts[i].field)) {
      value = value
        .replace(
          /\.\s*(?:Reassess|Leave|Return|Update|Keep|Do\s+not|Preparation)\b[\s\S]*$/i,
          ''
        )
        .trim()
        .replace(/[.\s]+$/, '')
        .trim();
    }
    fields[starts[i].field] = value;
  }

  // Fallback: single assignment when the field name was unrecognized by the
  // known-column scanner but still looks like field=value.
  if (Object.keys(fields).length === 0) {
    const single = /^([a-z][a-z0-9_]*)\s*[:=]\s*(.+)$/i.exec(chunk);
    if (single && columnSet.has(single[1].toLowerCase())) {
      let value = String(single[2] || '')
        .trim()
        .replace(/[.\s]+$/, '');
      if (!freeTextFields.has(single[1].toLowerCase())) {
        value = value
          .replace(
            /\.\s*(?:Reassess|Leave|Return|Update|Keep|Do\s+not|Preparation)\b[\s\S]*$/i,
            ''
          )
          .trim()
          .replace(/[.\s]+$/, '')
          .trim();
      }
      fields[single[1].toLowerCase()] = value;
    }
  }

  return fields;
}

/**
 * Parse per-prospect field mutations from an update instruction.
 * Does not treat instruction labels as prospect rows.
 * @param {string} text
 * @returns {{ updates: Array<{ prospectId: string, fields: Record<string, string> }>, referencedIds: string[] }}
 */
function parseFillableTableFieldUpdates(text) {
  const raw = String(text || '');
  // Split prose sentences so inline updates like
  // "Update the table. For PM-001 only, set website_status: verified. Leave…"
  // are parsed the same as multiline instructions.
  const lines = raw.split(/\r?\n/).flatMap((line) =>
    String(line || '')
      .split(
        /(?<=[.!?])\s+(?=(?:For|Leave|Return|Update|Keep|Do\s+not|Preparation|Reassess)\b)/i
      )
      .map((part) => part.trim())
      .filter(Boolean)
  );
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

  const applyFields = (prospectId, fieldText) => {
    if (!prospectId || fieldText == null) return;
    rememberRef(prospectId);
    if (!updatesById.has(prospectId)) updatesById.set(prospectId, {});
    const fields = updatesById.get(prospectId);
    const parsed = parseMutableFieldAssignments(fieldText, columnSet);
    for (const [field, value] of Object.entries(parsed)) {
      fields[field] = value;
    }
  };

  for (const line of lines) {
    const trimmed = line.trim().replace(/[.\s]+$/, '');
    if (!trimmed) continue;

    // Inline: "For PM-001 only, set website_status: verified"
    // Also: "For PM-001 only: website_status = verified"
    // Require a known mutable field + value — never treat header-only
    // "For PM-001 only, set:" as an inline mutation (that clears currentId).
    const forOnlyInline = matchForOnlyInlineFieldAssignment(trimmed);
    if (forOnlyInline) {
      currentId = forOnlyInline.prospectId;
      applyFields(currentId, forOnlyInline.fieldText);
      currentId = null;
      continue;
    }

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
      // Embedded "… for PM-001 only: field = value" inside an Update line.
      const embeddedFor = matchForOnlyInlineFieldAssignment(trimmed);
      if (embeddedFor && /^update\s+the\b/i.test(trimmed)) {
        applyFields(embeddedFor.prospectId, embeddedFor.fieldText);
      }
      currentId = null;
      continue;
    }

    if (/^set\s*:?\s*$/i.test(trimmed)) {
      continue;
    }

    const fieldMatch = /^-?\s*([a-z][a-z0-9_]*)\s*[:=]\s*(.*)$/i.exec(trimmed);
    if (fieldMatch && currentId) {
      const field = fieldMatch[1].toLowerCase();
      if (columnSet.has(field)) {
        if (!updatesById.has(currentId)) updatesById.set(currentId, {});
        updatesById.get(currentId)[field] = String(fieldMatch[2] || '')
          .trim()
          .replace(/[.\s]+$/, '');
      }
      continue;
    }
  }

  // Whole-text safety net for prose that did not split cleanly.
  if (
    updatesById.size === 0 ||
    [...updatesById.values()].every((f) => Object.keys(f).length === 0)
  ) {
    const safetyPatterns = [
      // Capture the full assignment region (including notes with semicolons);
      // applyFields splits on known column boundaries.
      /\bfor\s+([A-Za-z0-9_-]+)\s+only\b[,:]?\s*set\s*:?\s*((?:[a-z][a-z0-9_]*)\s*[:=]\s*.+?)(?=\.\s+(?:Leave|Return|Update|Keep|Do\s+not|Preparation|Reassess)\b|$)/gis,
      /\bfor\s+([A-Za-z0-9_-]+)\s+only\b[,:]\s*((?:[a-z][a-z0-9_]*)\s*[:=]\s*.+?)(?=\.\s+(?:Leave|Return|Update|Keep|Do\s+not|Preparation|Reassess)\b|$)/gis,
    ];
    for (const inlineRe of safetyPatterns) {
      let inlineMatch;
      while ((inlineMatch = inlineRe.exec(raw)) !== null) {
        applyFields(inlineMatch[1], inlineMatch[2]);
      }
      if (
        [...updatesById.values()].some((f) => Object.keys(f).length > 0)
      ) {
        break;
      }
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
 * When reassessIds is provided, recomputes derived readiness columns from
 * table gates for those rows after field mutations — never rewrites source
 * gate statuses / values / notes unless the operator assigned them above.
 * @param {object[]} rows
 * @param {Array<{ prospectId: string, fields: Record<string, string> }>} updates
 * @param {{ reassessIds?: string[] }} [options]
 * @returns {{ rows: object[], matchedIds: string[], unknownIds: string[], reassessedIds: string[] }}
 */
function applyFillableTableFieldUpdates(rows, updates, options = {}) {
  const next = (Array.isArray(rows) ? rows : []).map((row) => ({ ...row }));
  const indexById = new Map();
  next.forEach((row, i) => {
    const id = String(row.prospect_id || row.id || '').trim();
    if (id) indexById.set(id.toUpperCase(), i);
  });

  const matchedIds = [];
  const unknownIds = [];
  const columnSet = new Set(FILLABLE_TABLE_MUTABLE_FIELDS);
  const sourceSet = new Set(FILLABLE_TABLE_SOURCE_FIELDS);
  /** @type {Map<string, Set<string>>} */
  const explicitSourceById = new Map();

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
    const explicitSource = new Set();
    for (const [key, value] of Object.entries(fields)) {
      const field = String(key).toLowerCase();
      if (!columnSet.has(field)) continue;
      // Never invent readiness — only apply explicit operator values.
      row[field] = value == null ? '' : String(value);
      if (sourceSet.has(field)) explicitSource.add(field);
    }
    // Keep prospect_id stable unless operator explicitly changes it.
    if (!Object.prototype.hasOwnProperty.call(fields, 'prospect_id')) {
      row.prospect_id = next[idx].prospect_id || prospectId;
    }
    next[idx] = row;
    explicitSourceById.set(prospectId.toUpperCase(), explicitSource);
  }

  const reassessIds = Array.isArray(options.reassessIds)
    ? options.reassessIds
    : [];
  const reassessedIds = [];
  for (const prospectId of reassessIds) {
    const id = String(prospectId || '').trim();
    if (!id) continue;
    const idx = indexById.get(id.toUpperCase());
    if (idx == null) {
      if (!unknownIds.some((u) => String(u).toUpperCase() === id.toUpperCase())) {
        unknownIds.push(id);
      }
      continue;
    }
    // Capture source fields before reassess; restore any that were not
    // explicitly assigned this turn (defensive against derived-path drift).
    const before = next[idx];
    const sourceBefore = snapshotFillableTableSourceFields(before);
    const explicit =
      explicitSourceById.get(id.toUpperCase()) || new Set();
    let reassessed = reassessFillableTableRowFromGates(before);
    for (const field of FILLABLE_TABLE_SOURCE_FIELDS) {
      if (explicit.has(field)) continue;
      if (Object.prototype.hasOwnProperty.call(sourceBefore, field)) {
        reassessed[field] = sourceBefore[field];
      }
    }
    next[idx] = reassessed;
    reassessedIds.push(next[idx].prospect_id || id);
    if (
      !matchedIds.some((m) => String(m).toUpperCase() === id.toUpperCase())
    ) {
      matchedIds.push(next[idx].prospect_id || id);
    }
  }

  return { rows: next, matchedIds, unknownIds, reassessedIds };
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
  if (!isCanaryDeskWorkflow(ctx)) return false;
  const c = (ctx && ctx.constraints) || {};
  return (
    c.preparationOnly === true ||
    c.noExecution === true ||
    c.noMail === true ||
    c.noLaunch === true ||
    c.noPrint === true
  );
}

/**
 * Split a markdown table line into cells (preserves empty cells).
 * @param {string} line
 * @returns {string[]}
 */
function splitMarkdownTableCells(line) {
  let text = String(line || '').trim();
  if (!text.includes('|')) return [];
  if (text.startsWith('|')) text = text.slice(1);
  if (text.endsWith('|')) text = text.slice(0, -1);
  return text.split('|').map((cell) => String(cell == null ? '' : cell).trim());
}

/**
 * True when a markdown row is a separator (`|---|---|`).
 * @param {string[]} cells
 */
function isMarkdownTableSeparatorRow(cells) {
  if (!Array.isArray(cells) || cells.length === 0) return false;
  return cells.every((cell) => {
    const s = String(cell || '').trim();
    return !s || /^:?-{3,}:?$/.test(s);
  });
}

/**
 * Normalize a header cell to a snake_case column key.
 * @param {string} cell
 */
function normalizeFillableTableHeaderKey(cell) {
  return String(cell || '')
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
}

/**
 * Headers that identify a pasted fillable verification table.
 */
const FILLABLE_VERIFICATION_TABLE_DETECT_HEADERS = Object.freeze([
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
  'verification_status',
  'mail_readiness',
  'draft_readiness',
  'execution_readiness',
  'operator_next_action',
  'notes',
]);

/**
 * True when header cells look like a fillable verification table.
 * @param {string[]} headerCells
 */
function isFillableVerificationTableHeader(headerCells) {
  const keys = (Array.isArray(headerCells) ? headerCells : [])
    .map(normalizeFillableTableHeaderKey)
    .filter(Boolean);
  if (!keys.length) return false;
  const set = new Set(keys);
  if (!set.has('prospect_id') || !set.has('company_name')) return false;
  const hits = FILLABLE_VERIFICATION_TABLE_DETECT_HEADERS.filter((h) =>
    set.has(h)
  ).length;
  // prospect_id + company_name + enough verification/readiness columns
  return hits >= 5;
}

/**
 * Extract a markdown fillable verification table from operator text.
 * Preserves source/derived fields and empty cells; does not invent values.
 * @param {string} text
 * @returns {{ headers: string[], rows: object[], startLine: number, endLine: number }|null}
 */
function parseFillableVerificationTableFromMessage(text) {
  const raw = String(text || '').replace(/^\uFEFF/, '');
  if (!raw.trim()) return null;
  const lines = raw.split(/\r?\n/);

  for (let i = 0; i < lines.length; i += 1) {
    const headerCells = splitMarkdownTableCells(lines[i]);
    if (!isFillableVerificationTableHeader(headerCells)) continue;

    const headers = headerCells.map(normalizeFillableTableHeaderKey);
    let rowStart = i + 1;
    if (
      rowStart < lines.length &&
      isMarkdownTableSeparatorRow(splitMarkdownTableCells(lines[rowStart]))
    ) {
      rowStart += 1;
    }

    /** @type {object[]} */
    const rows = [];
    let end = i;
    for (let j = rowStart; j < lines.length; j += 1) {
      const line = String(lines[j] || '');
      if (!line.trim()) break;
      if (!line.includes('|')) break;
      const cells = splitMarkdownTableCells(line);
      if (!cells.length || isMarkdownTableSeparatorRow(cells)) continue;
      // Stop if this looks like another header rather than a data row.
      if (isFillableVerificationTableHeader(cells) && rows.length > 0) break;

      /** @type {Record<string, string>} */
      const row = {};
      headers.forEach((header, idx) => {
        if (!header) return;
        row[header] = cells[idx] != null ? String(cells[idx]) : '';
      });
      // Keep unknown trailing cells out; never invent missing columns.
      if (!String(row.prospect_id || '').trim() && !String(row.company_name || '').trim()) {
        continue;
      }
      rows.push(row);
      end = j;
    }

    if (rows.length === 0) continue;
    return { headers, rows, startLine: i, endLine: end };
  }

  return null;
}

/**
 * True when the message embeds a fillable verification markdown table.
 * @param {string} text
 */
function looksLikeFillableVerificationTablePaste(text) {
  const parsed = parseFillableVerificationTableFromMessage(text);
  return Boolean(parsed && Array.isArray(parsed.rows) && parsed.rows.length > 0);
}

/**
 * Build desk entities from pasted fillable table rows (facts only).
 * @param {object[]} rows
 */
function entitiesFromFillableTableRows(rows) {
  return (Array.isArray(rows) ? rows : []).map((row) =>
    prospectToEntity({
      id: row && row.prospect_id != null ? String(row.prospect_id).trim() : null,
      companyName: row && row.company_name,
      contactName: row && row.contact_name,
      website: blankToNull(row && row.website_value),
      mailingAddress: blankToNull(row && row.mailing_address_value),
      address: blankToNull(row && row.mailing_address_value),
      phone: blankToNull(row && row.phone_value),
    })
  );
}

/**
 * Ingest a pasted fillable verification table into session activeWorkContext.
 * Does not mutate row values. Does not create a mission or imply execution.
 * @param {{ question: string, session: object|null }} input
 * @returns {object|null} updated activeWorkContext, or null when no table paste
 */
function ingestPastedFillableVerificationTable(input = {}) {
  const question = String(input.question || '');
  const session = input.session || null;
  if (!session || !question.trim()) return null;

  const parsed = parseFillableVerificationTableFromMessage(question);
  if (!parsed || !parsed.rows.length) return null;

  const prior = getActiveWorkContext(session);
  const tableRows = parsed.rows.map((row) => ({ ...row }));

  const entities = entitiesFromFillableTableRows(tableRows);
  const prospects = entitiesToProspects(entities);
  const campaignId =
    extractCampaignIdFromText(question) ||
    (prior && prior.target && prior.target.campaignId) ||
    '001';
  const workflow = resolveCanaryDeskWorkflow(question, prior);

  return setActiveWorkContext(
    session,
    buildCanaryActiveWorkContext({
      prospects,
      campaignId,
      workflow,
      prior,
      tableRows,
      lastOutputType: LAST_OUTPUT_TYPES.FILLABLE_TABLE,
      lastOutputKind: 'fillable_verification_table',
      nextAction:
        (prior && prior.nextAction) ||
        'await_operator_transform_or_verification',
    })
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
  CAMPAIGN_001_PREPARATION_ONLY_CANARY,
  FILLABLE_TABLE_MUTABLE_FIELDS,
  FILLABLE_TABLE_SOURCE_FIELDS,
  FILLABLE_TABLE_DERIVED_FIELDS,
  FILLABLE_TABLE_GATE_STATUS_FIELDS,
  FILLABLE_TABLE_FREE_TEXT_FIELDS,
  FILLABLE_VERIFICATION_TABLE_DETECT_HEADERS,
  getActiveWorkContext,
  setActiveWorkContext,
  cloneActiveWorkContext,
  buildCanaryActiveWorkContext,
  prospectToEntity,
  entitiesToProspects,
  entitiesFromFillableTableRows,
  derivePendingFields,
  isActiveWorkReuseProspectCue,
  isActiveWorkFollowUpCue,
  isActiveWorkTransformCue,
  isPacketReviewRequest,
  extractPacketReviewProspectId,
  isExplicitNewMissionRequest,
  isExplicitContextOverride,
  isExplicitExecutionRequest,
  isFillableTableRequest,
  isFillableTableUpdateRequest,
  isFillableTableReadinessReassessRequest,
  isFillableTableWholeTableReassessRequest,
  wantsStrictFillableTableOutputShape,
  wantsFillableTableHeading,
  activeContextHasFillableTable,
  knownActiveWorkProspectIds,
  parseFillableTableFieldUpdates,
  applyFillableTableFieldUpdates,
  reassessFillableTableRowFromGates,
  extractReadinessReassessProspectIds,
  extractGateStatusUpdatedProspectIds,
  deriveOperatorNextActionFromGates,
  extractCampaignIdFromText,
  activeContextBlocksExecution,
  activeContextHasEntities,
  isCanaryDeskWorkflow,
  resolveCanaryDeskWorkflow,
  looksLikeFillableVerificationTablePaste,
  parseFillableVerificationTableFromMessage,
  ingestPastedFillableVerificationTable,
};
