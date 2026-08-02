'use strict';

const { PAGE_TYPES } = require('./WorkspaceTypes');
const { contextFocusLabel } = require('./ContextEnvelope');

/**
 * Contextual suggested investigations from MaxContext.
 * Templates keyed by page; filled from envelope — not a global hardcoded list.
 * When activeWorkContext is present for a desk workflow, chips follow that
 * workflow + last output kind instead of briefing defaults.
 * Packet-review responses may also supply response-level outputKind /
 * contextHints (e.g. inline known facts) without mutating activeWorkContext.
 *
 * @param {object} context - normalized MaxContext
 * @returns {string[]}
 */
function buildSuggestions(context) {
  const work = resolveSuggestionWorkContext(context);
  if (work && isActiveDeskWorkflow(work)) {
    return buildActiveWorkSuggestions(work, context);
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
 * Prefer persisted desk memory; fall back to response-level packet/canary hints.
 * @param {object|null|undefined} context
 * @returns {object|null}
 */
function resolveSuggestionWorkContext(context) {
  const active = resolveActiveWorkContext(context);
  if (active && isActiveDeskWorkflow(active)) return active;
  return resolveResponseWorkContext(context);
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
 * Synthesize a temporary desk-work context from response-level metadata /
 * outputKind / contextHints when activeWorkContext was not mutated
 * (inline known-facts packet review).
 * @param {object|null|undefined} context
 * @returns {object|null}
 */
function resolveResponseWorkContext(context) {
  if (!context || typeof context !== 'object') return null;

  if (
    context.packetReviewContext &&
    typeof context.packetReviewContext === 'object'
  ) {
    return normalizeHintWorkContext(context.packetReviewContext, context);
  }

  const metadata =
    context.metadata && typeof context.metadata === 'object'
      ? context.metadata
      : {};
  const hints =
    context.contextHints && typeof context.contextHints === 'object'
      ? context.contextHints
      : metadata.contextHints && typeof metadata.contextHints === 'object'
        ? metadata.contextHints
        : null;

  const outputKindRaw =
    context.outputKind != null
      ? context.outputKind
      : context.lastOutputKind != null
        ? context.lastOutputKind
        : metadata.outputKind != null
          ? metadata.outputKind
          : metadata.lastOutputKind != null
            ? metadata.lastOutputKind
            : hints && (hints.outputKind || hints.lastOutputKind || hints.lastOutputType);

  const isPacketReview =
    metadata.packetReview === true ||
    (hints && hints.packetReview === true) ||
    /packet/.test(String(outputKindRaw || '').toLowerCase());

  const isCanaryPrep =
    metadata.canaryPreparationOnly === true ||
    (hints &&
      (hints.preparationOnly === true ||
        /canary|preparation/.test(String(hints.workflow || ''))));

  if (!isPacketReview && !isCanaryPrep && !outputKindRaw) return null;

  if (!isPacketReview && !isCanaryPrep) {
    const kind = resolveLastOutputKind({ lastOutputKind: outputKindRaw });
    const deskKinds = new Set([
      'packet_review',
      'fillable_table',
      'verification_work_order',
      'provisional_drafts',
      'canary_review_package',
    ]);
    if (!kind || !deskKinds.has(kind)) return null;
  }

  return normalizeHintWorkContext(
    {
      ...(hints || {}),
      outputKind: outputKindRaw,
      lastOutputKind:
        (hints && (hints.lastOutputKind || hints.lastOutputType)) ||
        outputKindRaw ||
        (isPacketReview ? 'packet_review' : null),
      packetReview: isPacketReview || (hints && hints.packetReview),
      preparationOnly:
        isCanaryPrep ||
        isPacketReview ||
        (hints && hints.preparationOnly === true),
      prospectId:
        (hints && hints.prospectId) ||
        metadata.prospectId ||
        null,
      campaignId:
        (hints && hints.campaignId) ||
        metadata.campaignId ||
        '001',
      mailReadiness:
        (hints && hints.mailReadiness) ||
        metadata.mailReadiness ||
        null,
      executionReadiness:
        (hints && hints.executionReadiness) ||
        metadata.executionReadiness ||
        'blocked',
      workflow:
        (hints && hints.workflow) ||
        (isPacketReview || isCanaryPrep
          ? 'campaign_001_preparation_only_canary'
          : null),
    },
    context
  );
}

/**
 * @param {object} hints
 * @param {object} [context]
 * @returns {object|null}
 */
function normalizeHintWorkContext(hints, context = {}) {
  if (!hints || typeof hints !== 'object') return null;

  const workflow =
    hints.workflow != null
      ? String(hints.workflow)
      : hints.preparationOnly || hints.packetReview
        ? 'campaign_001_preparation_only_canary'
        : null;
  if (!workflow) return null;

  const lastRaw =
    hints.lastOutputKind != null
      ? hints.lastOutputKind
      : hints.lastOutputType != null
        ? hints.lastOutputType
        : hints.outputKind != null
          ? hints.outputKind
          : null;
  const lastKind = resolveLastOutputKind({ lastOutputKind: lastRaw }) || lastRaw;

  const metadata =
    context.metadata && typeof context.metadata === 'object'
      ? context.metadata
      : {};
  const prospectId =
    hints.prospectId != null
      ? String(hints.prospectId).trim()
      : metadata.prospectId != null
        ? String(metadata.prospectId).trim()
        : null;
  const mailReadiness =
    hints.mailReadiness != null
      ? String(hints.mailReadiness)
      : metadata.mailReadiness != null
        ? String(metadata.mailReadiness)
        : 'blocked';
  const executionReadiness =
    hints.executionReadiness != null
      ? String(hints.executionReadiness)
      : metadata.executionReadiness != null
        ? String(metadata.executionReadiness)
        : 'blocked';

  const constraints = {
    preparationOnly: true,
    noLaunch: true,
    noExecution: true,
    noMail: true,
    noPrint: true,
    noApproval: true,
    ...(hints.constraints && typeof hints.constraints === 'object'
      ? hints.constraints
      : {}),
  };

  return {
    workflow,
    target: {
      campaignId: String(
        hints.campaignId || metadata.campaignId || '001'
      ),
    },
    entities: Array.isArray(hints.entities)
      ? hints.entities.map((e) => ({ ...e }))
      : prospectId
        ? [{ type: 'prospect', id: prospectId }]
        : [],
    tableRows: Array.isArray(hints.tableRows)
      ? hints.tableRows.map((row) => ({ ...row }))
      : prospectId
        ? [
            {
              prospect_id: prospectId,
              mail_readiness: mailReadiness,
              execution_readiness: executionReadiness,
            },
          ]
        : [],
    constraints,
    lastOutputType: lastKind,
    lastOutputKind: lastKind,
    nextAction: hints.nextAction != null ? String(hints.nextAction) : null,
  };
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
  if (kind === 'packet_review') return 'packet_review';
  if (kind === 'packet_review_artifact') return 'packet_review';
  if (kind === 'verification_work_order') return 'verification_work_order';
  if (kind === 'canary_review_package') return 'canary_review_package';
  if (kind === 'provisional_drafts') return 'provisional_drafts';
  if (/packet/.test(kind)) return 'packet_review';
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
  if (
    c.preparationOnly === true ||
    c.noLaunch === true ||
    c.noExecution === true ||
    c.noMail === true ||
    c.noPrint === true ||
    c.noApproval === true
  ) {
    return true;
  }

  const rows = Array.isArray(awc && awc.tableRows) ? awc.tableRows : [];
  if (
    rows.some(
      (row) =>
        String((row && row.execution_readiness) || '')
          .toLowerCase()
          .trim() === 'blocked'
    )
  ) {
    return true;
  }
  return false;
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
  } else if (kind === 'packet_review') {
    chips.push('Show missing verification fields.');
    chips.push('Create verification plan.');
    chips.push('Update readiness fields.');
    chips.push('Create packet checklist for another prospect.');
    chips.push('Summarize final operator decision.');
    chips.push('Show what still blocks mailing.');
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
  resolveResponseWorkContext,
  resolveSuggestionWorkContext,
  isSafeActiveWorkChip,
  topCompanyName,
};
