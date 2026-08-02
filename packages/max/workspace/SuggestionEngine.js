'use strict';

const { PAGE_TYPES } = require('./WorkspaceTypes');
const { contextFocusLabel } = require('./ContextEnvelope');

/**
 * Contextual suggested investigations from MaxContext.
 * Templates keyed by page; filled from envelope — not a global hardcoded list.
 * When activeWorkContext is present for a desk workflow, chips follow that
 * workflow + last output kind instead of briefing defaults.
 *
 * @param {object} context - normalized MaxContext
 * @returns {string[]}
 */
function buildSuggestions(context) {
  const active = resolveActiveWorkContext(context);
  if (active && isActiveDeskWorkflow(active)) {
    return buildActiveWorkSuggestions(active, context);
  }

  const topName = topCompanyName(context);
  const focus = contextFocusLabel(context);

  if (context.page === PAGE_TYPES.COMPANY) {
    return [
      'Why did confidence increase?',
      'Show relationship history.',
      'Explain supporting signals.',
      `Compare with similar companies.`,
    ].map((s) => (focus && focus !== "today's briefing" ? s : s));
  }

  if (context.page === PAGE_TYPES.RECOMMENDATION) {
    return [
      'Explain this recommendation.',
      'Show contradicting evidence.',
      'Walk through policy evaluation.',
      'What happens if I wait?',
    ];
  }

  if (context.page === PAGE_TYPES.TIMELINE) {
    return [
      'What changed most recently?',
      'Show the strongest supporting signals.',
      'Summarize movement this period.',
    ];
  }

  if (context.page === PAGE_TYPES.MARKET) {
    return [
      'What shifted overnight?',
      'Show watch alerts.',
      'Compare top opportunities.',
    ];
  }

  // command-deck
  const suggestions = [];
  if (topName) {
    suggestions.push(`Why is ${topName} #1?`);
  } else {
    suggestions.push('Why is the top opportunity ranked first?');
  }
  suggestions.push('What changed overnight?');
  suggestions.push("Compare today's top opportunities.");
  suggestions.push('Show risks.');

  const watchCount = Number(context.briefing && context.briefing.watchAlertCount);
  if (Number.isFinite(watchCount) && watchCount > 0) {
    suggestions.push('Explain the watch alerts.');
  }

  return suggestions.slice(0, 5);
}

/**
 * @param {object|null|undefined} context
 * @returns {object|null}
 */
function resolveActiveWorkContext(context) {
  if (!context || typeof context !== 'object') return null;
  if (context.activeWorkContext && typeof context.activeWorkContext === 'object') {
    return context.activeWorkContext;
  }
  return null;
}

/**
 * True when session desk memory should own suggestion chips.
 * @param {object} awc
 */
function isActiveDeskWorkflow(awc) {
  if (!awc || typeof awc !== 'object') return false;
  const workflow = String(awc.workflow || '').toLowerCase();
  if (!workflow) return false;
  if (
    workflow === 'campaign_canary' ||
    workflow === 'campaign_001_preparation_only_canary'
  ) {
    return true;
  }
  return (
    /canary/.test(workflow) ||
    /preparation[_\s-]*only/.test(workflow) ||
    /verification/.test(workflow)
  );
}

/**
 * Normalize last output discriminator (supports lastOutputType + lastOutputKind).
 * @param {object} awc
 * @returns {string|null}
 */
function resolveLastOutputKind(awc) {
  if (!awc || typeof awc !== 'object') return null;
  const raw = awc.lastOutputKind != null ? awc.lastOutputKind : awc.lastOutputType;
  if (raw == null || String(raw).trim() === '') return null;
  const kind = String(raw).toLowerCase().trim();
  if (kind === 'fillable_verification_table') return 'fillable_table';
  if (kind === 'fillable_table') return 'fillable_table';
  if (kind === 'verification_work_order') return 'verification_work_order';
  if (kind === 'canary_review_package') return 'canary_review_package';
  if (kind === 'provisional_drafts') return 'provisional_drafts';
  if (/fillable/.test(kind) && /table/.test(kind)) return 'fillable_table';
  if (/verification/.test(kind) && /work/.test(kind)) return 'verification_work_order';
  if (/draft/.test(kind)) return 'provisional_drafts';
  if (/canary|review|package/.test(kind)) return 'canary_review_package';
  return kind;
}

/**
 * First prospect id on the desk (e.g. PM-001), for packet-draft chips.
 * @param {object} awc
 * @returns {string|null}
 */
function firstDeskProspectId(awc) {
  const entities = Array.isArray(awc.entities) ? awc.entities : [];
  for (const e of entities) {
    if (e && e.id != null && String(e.id).trim()) return String(e.id).trim();
  }
  const rows = Array.isArray(awc.tableRows) ? awc.tableRows : [];
  for (const row of rows) {
    if (row && row.prospect_id != null && String(row.prospect_id).trim()) {
      return String(row.prospect_id).trim();
    }
  }
  return null;
}

/**
 * True when preparation-only / no-execution constraints still apply.
 * @param {object} awc
 */
function activeWorkBlocksExecution(awc) {
  const c = (awc && awc.constraints) || {};
  return (
    c.preparationOnly === true ||
    c.noLaunch === true ||
    c.noExecution === true ||
    c.noMail === true ||
    c.noPrint === true ||
    c.noApproval === true
  );
}

/**
 * True when every desk row reports Ready mail readiness (strict).
 * Missing table / unknown readiness → not ready.
 * @param {object} awc
 */
function deskMailReadinessComplete(awc) {
  const rows = Array.isArray(awc.tableRows) ? awc.tableRows : [];
  if (!rows.length) return false;
  return rows.every((row) => {
    const mail = String((row && row.mail_readiness) || '').toLowerCase();
    return mail === 'ready';
  });
}

/**
 * Reject launch/execute/approve/mail action chips unless readiness + approval
 * gates are satisfied. Diagnostic phrasing ("What still blocks mailing?") is OK.
 * @param {string} chip
 * @param {object} awc
 */
function isSafeActiveWorkChip(chip, awc) {
  const text = String(chip || '').toLowerCase().trim();
  if (!text) return false;

  const asksToAct =
    /^(?:mail|launch|execute|approve|print)\b/.test(text) ||
    /\b(?:mail|launch|execute|approve|print)\s+(?:the|this|all|now|packets?|campaign)\b/.test(
      text
    ) ||
    /\b(?:send|ship)\s+(?:mail|packets?|the\s+campaign)\b/.test(text);

  if (!asksToAct) return true;

  // Action chips require completed readiness AND constraints that allow it.
  if (activeWorkBlocksExecution(awc)) return false;
  if (!deskMailReadinessComplete(awc)) return false;
  return true;
}

/**
 * Desk-workflow suggestion chips from activeWorkContext.workflow + last output.
 * Never falls back to briefing/market chips while desk work is active.
 *
 * @param {object} awc
 * @param {object} [context]
 * @returns {string[]}
 */
function buildActiveWorkSuggestions(awc, context = {}) {
  const kind = resolveLastOutputKind(awc);
  const prospectId = firstDeskProspectId(awc);
  const chips = [];

  if (kind === 'fillable_table') {
    chips.push('Show only blocked prospects.');
    chips.push('Update another verification field.');
    chips.push('Create packet review checklist.');
    chips.push('Summarize what changed in this table.');
    if (prospectId) {
      chips.push(`Draft ${prospectId} packet for review.`);
    } else {
      chips.push('Draft packet for review.');
    }
    chips.push('What still blocks mailing?');
  } else if (kind === 'verification_work_order') {
    chips.push('Convert this into a fillable verification table.');
    chips.push('Show only blocked prospects.');
    chips.push('Create packet review checklist.');
    chips.push('What still blocks mailing?');
    chips.push('Summarize verification status.');
  } else if (kind === 'provisional_drafts') {
    chips.push('Show only blocked prospects.');
    chips.push('What still blocks mailing?');
    chips.push('Create packet review checklist.');
    if (prospectId) {
      chips.push(`Revise ${prospectId} draft for review.`);
    }
    chips.push('Convert this into a fillable verification table.');
  } else {
    // canary review package / unknown canary desk output
    chips.push('Convert this into a verification work order.');
    chips.push('Convert this into a fillable verification table.');
    chips.push('Show only blocked prospects.');
    chips.push('Create packet review checklist.');
    chips.push('What still blocks mailing?');
  }

  const filtered = chips.filter((c) => isSafeActiveWorkChip(c, awc));

  // Drop chip that exactly matches the latest typed question when provided.
  const latest = String(
    (context && (context.latestQuestion || context.lastQuestion)) || ''
  )
    .trim()
    .toLowerCase();
  const withoutEcho = latest
    ? filtered.filter((c) => c.toLowerCase() !== latest)
    : filtered;

  return withoutEcho.slice(0, 6);
}

function topCompanyName(context) {
  if (context.selectedEntity && context.selectedEntity.name) {
    return context.selectedEntity.name;
  }
  const cards = context.visibleCards || [];
  for (const card of cards) {
    const payload = card.payload || {};
    const name =
      (payload.recommendation && payload.recommendation.companyName) ||
      payload.companyName ||
      (card.type === 'highest_leverage' || card.type === 'priority_item'
        ? extractNameFromTitle(card.title)
        : null);
    if (name) return name;
  }
  if (context.deck && context.deck.highestLeverageAction) {
    const hla = context.deck.highestLeverageAction;
    if (hla.recommendation && hla.recommendation.companyName) {
      return hla.recommendation.companyName;
    }
  }
  if (context.deck && context.deck.priorityQueue && context.deck.priorityQueue.items) {
    const first = context.deck.priorityQueue.items[0];
    if (first && first.companyName) return first.companyName;
    if (first && first.title) return extractNameFromTitle(first.title);
  }
  return null;
}

function extractNameFromTitle(title) {
  if (!title) return null;
  const cleaned = String(title)
    .replace(/^(Review|Pursue|Contact|Open|Call|Email)\s+/i, '')
    .trim();
  return cleaned || null;
}

module.exports = {
  buildSuggestions,
  buildActiveWorkSuggestions,
  isActiveDeskWorkflow,
  resolveLastOutputKind,
  resolveActiveWorkContext,
  isSafeActiveWorkChip,
  topCompanyName,
};
