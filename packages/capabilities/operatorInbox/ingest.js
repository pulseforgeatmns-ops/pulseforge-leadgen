'use strict';

/**
 * Ingest work items from capability outputs / events (SPEC-037).
 * Inbox coordinates — never performs the underlying workflow.
 */

const {
  INBOX_KINDS,
  INBOX_STATUS,
  buildInboxItem,
  buildDeepLink,
  WORKSPACE_TARGETS,
} = require('./types');
const { buildDedupeKey } = require('./dedupe');

function newId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Normalize capability events + priorOutputs into inbox item candidates.
 * @param {object} context
 * @returns {object[]}
 */
function ingestWorkItems(context) {
  const inputs = (context && context.inputs) || {};
  const prior = inputs.priorOutputs || {};
  const clientId =
    context.clientId != null
      ? context.clientId
      : inputs.clientId != null
        ? inputs.clientId
        : null;
  const missionId = context.missionId || inputs.missionId || null;
  const campaignId =
    inputs.campaignId ||
    (inputs.campaign && (inputs.campaign.id || inputs.campaign.name)) ||
    null;
  const campaignName =
    inputs.campaignName ||
    (inputs.campaign && inputs.campaign.name) ||
    'Campaign';

  /** @type {object[]} */
  const candidates = [];

  // Explicit capability events
  const events = Array.isArray(inputs.capabilityEvents)
    ? inputs.capabilityEvents
    : Array.isArray(inputs.inboxEvents)
      ? inputs.inboxEvents
      : [];
  for (const ev of events) {
    candidates.push(
      fromEvent(ev, { clientId, missionId, campaignId, campaignName })
    );
  }

  // Explicit work items
  const explicit = Array.isArray(inputs.workItems)
    ? inputs.workItems
    : Array.isArray(inputs.items)
      ? inputs.items
      : [];
  for (const raw of explicit) {
    candidates.push(
      buildCandidate(raw, { clientId, missionId, campaignId, campaignName })
    );
  }

  // Campaign Review validation / approval
  ingestCampaignReview(candidates, inputs, prior, {
    clientId,
    missionId,
    campaignId,
    campaignName,
  });

  // Direct Mail Execution stages
  ingestDirectMail(candidates, inputs, prior, {
    clientId,
    missionId,
    campaignId,
    campaignName,
  });

  // Outcome Intelligence pending recommendations
  ingestOutcomeIntelligence(candidates, inputs, prior, {
    clientId,
    missionId,
    campaignId,
    campaignName,
  });

  // Validation results
  const validations = Array.isArray(inputs.validationResults)
    ? inputs.validationResults
    : [];
  for (const v of validations) {
    candidates.push(
      fromValidation(v, { clientId, missionId, campaignId, campaignName })
    );
  }

  return candidates.filter(Boolean);
}

/**
 * @param {object[]} out
 * @param {object} inputs
 * @param {object} prior
 * @param {object} ctx
 */
function ingestCampaignReview(out, inputs, prior, ctx) {
  const summary = inputs.reviewSummary || prior.summary || null;
  const queue = inputs.queue || prior.queue || [];
  const status =
    (summary && summary.status) ||
    inputs.campaignStatus ||
    prior.campaignStatus ||
    null;

  if (
    status === 'in_review' ||
    status === 'blocked' ||
    inputs.campaignApproved === false
  ) {
    out.push(
      buildCandidate(
        {
          kind: INBOX_KINDS.CAMPAIGN_APPROVAL,
          title: `Campaign approval — ${ctx.campaignName}`,
          sourceCapability: 'campaign_review',
          blocking: status === 'blocked',
          deepLink: {
            workspace: WORKSPACE_TARGETS.CAMPAIGN_REVIEW,
            missionId: ctx.missionId,
            campaignId: ctx.campaignId,
          },
        },
        ctx
      )
    );
  }

  for (const p of Array.isArray(queue) ? queue : []) {
    if (p.status === 'needs_review' || p.validationFailed) {
      const missingAddress = Boolean(p.missingAddress || p.noAddress);
      const missingRecipient = Boolean(p.missingRecipient || p.noRecipient);
      const kind = missingAddress
        ? INBOX_KINDS.MISSING_ADDRESS
        : missingRecipient
          ? INBOX_KINDS.MISSING_RECIPIENT
          : INBOX_KINDS.VALIDATION_ISSUES;
      out.push(
        buildCandidate(
          {
            kind,
            title: `${defaultKindTitle(kind)} — ${p.company || p.prospectId || 'prospect'}`,
            sourceCapability: 'campaign_review',
            subjectId: p.prospectId || p.id,
            deepLink: {
              workspace: WORKSPACE_TARGETS.CAMPAIGN_REVIEW,
              missionId: ctx.missionId,
              campaignId: ctx.campaignId,
            },
          },
          ctx
        )
      );
    }
  }

  if (inputs.mailPackageNeedsApproval || prior.mailPackageNeedsApproval) {
    out.push(
      buildCandidate(
        {
          kind: INBOX_KINDS.MAIL_PACKAGE_APPROVAL,
          title: `Mail package approval — ${ctx.campaignName}`,
          sourceCapability: 'mail_package_generator',
          deepLink: {
            workspace: WORKSPACE_TARGETS.MAIL_PACKAGE,
            missionId: ctx.missionId,
            campaignId: ctx.campaignId,
          },
        },
        ctx
      )
    );
  }
}

/**
 * @param {object[]} out
 * @param {object} inputs
 * @param {object} prior
 * @param {object} ctx
 */
function ingestDirectMail(out, inputs, prior, ctx) {
  const execution = inputs.execution || prior.execution || null;
  const status =
    (execution && execution.summary && execution.summary.status) ||
    inputs.executionStatus ||
    prior.campaignStatus ||
    null;

  if (!status) return;

  if (status === 'ready_to_print' || status === 'draft') {
    out.push(
      buildCandidate(
        {
          kind: INBOX_KINDS.PRINT_CAMPAIGN,
          title: `Print campaign — ${ctx.campaignName}`,
          sourceCapability: 'direct_mail_execution',
          deepLink: {
            workspace: WORKSPACE_TARGETS.DIRECT_MAIL_EXECUTION,
            missionId: ctx.missionId,
            campaignId: ctx.campaignId,
          },
        },
        ctx
      )
    );
  }
  if (status === 'printed' || status === 'assembling') {
    out.push(
      buildCandidate(
        {
          kind: INBOX_KINDS.ASSEMBLE_MAIL,
          title: `Assemble mail — ${ctx.campaignName}`,
          sourceCapability: 'direct_mail_execution',
          deepLink: {
            workspace: WORKSPACE_TARGETS.DIRECT_MAIL_EXECUTION,
            missionId: ctx.missionId,
            campaignId: ctx.campaignId,
          },
        },
        ctx
      )
    );
  }
  if (status === 'ready_to_mail') {
    out.push(
      buildCandidate(
        {
          kind: INBOX_KINDS.MAIL_CAMPAIGN,
          title: `Mail campaign — ${ctx.campaignName}`,
          sourceCapability: 'direct_mail_execution',
          deepLink: {
            workspace: WORKSPACE_TARGETS.DIRECT_MAIL_EXECUTION,
            missionId: ctx.missionId,
            campaignId: ctx.campaignId,
          },
        },
        ctx
      )
    );
  }
  if (status === 'completed') {
    out.push(
      buildCandidate(
        {
          kind: INBOX_KINDS.CAMPAIGN_COMPLETED,
          title: `Campaign completed — ${ctx.campaignName}`,
          sourceCapability: 'direct_mail_execution',
          status: INBOX_STATUS.OPEN,
          deepLink: {
            workspace: WORKSPACE_TARGETS.DIRECT_MAIL_EXECUTION,
            missionId: ctx.missionId,
            campaignId: ctx.campaignId,
          },
        },
        ctx
      )
    );
  }
}

/**
 * @param {object[]} out
 * @param {object} inputs
 * @param {object} prior
 * @param {object} ctx
 */
function ingestOutcomeIntelligence(out, inputs, prior, ctx) {
  const recommendations =
    inputs.recommendations ||
    prior.recommendations ||
    inputs.pendingRecommendations ||
    prior.pendingRecommendations ||
    [];
  const pending = (Array.isArray(recommendations) ? recommendations : []).filter(
    (r) => r.status === 'pending' || !r.status
  );

  for (const rec of pending) {
    const target = String(rec.target || '');
    let kind = INBOX_KINDS.APPLY_RECOMMENDATION;
    if (target === 'client_playbook') kind = INBOX_KINDS.UPDATE_CLIENT_PLAYBOOK;
    if (target === 'ranking_weights') kind = INBOX_KINDS.APPLY_RANKING_CHANGES;
    out.push(
      buildCandidate(
        {
          kind,
          title: rec.summary || `Review recommendation — ${ctx.campaignName}`,
          sourceCapability: 'outcome_intelligence',
          subjectId: rec.id,
          evidenceBacked: rec.evidenceBacked !== false,
          deepLink: {
            workspace: WORKSPACE_TARGETS.OUTCOME_SUMMARY,
            missionId: ctx.missionId,
            campaignId: ctx.campaignId,
          },
        },
        ctx
      )
    );
  }

  if (inputs.outcomeSummary || prior.outcomeSummary) {
    out.push(
      buildCandidate(
        {
          kind: INBOX_KINDS.OUTCOME_SUMMARY_AVAILABLE,
          title: `Outcome summary available — ${ctx.campaignName}`,
          sourceCapability: 'outcome_intelligence',
          deepLink: {
            workspace: WORKSPACE_TARGETS.OUTCOME_SUMMARY,
            missionId: ctx.missionId,
            campaignId: ctx.campaignId,
          },
        },
        ctx
      )
    );
  }
}

/**
 * @param {object} ev
 * @param {object} ctx
 * @returns {object}
 */
function fromEvent(ev, ctx) {
  return buildCandidate(
    {
      kind: ev.kind || ev.inboxKind || INBOX_KINDS.FOLLOW_UP,
      title: ev.title || ev.summary,
      sourceCapability: ev.sourceCapability || ev.capability || null,
      subjectId: ev.subjectId || ev.prospectId || ev.recommendationId,
      dueDate: ev.dueDate,
      blocking: ev.blocking,
      evidenceBacked: ev.evidenceBacked,
      notes: ev.notes,
      deepLink: ev.deepLink,
      missionId: ev.missionId,
      campaignId: ev.campaignId,
      clientId: ev.clientId,
    },
    ctx
  );
}

/**
 * @param {object} v
 * @param {object} ctx
 * @returns {object}
 */
function fromValidation(v, ctx) {
  const code = String(v.code || v.type || '').toLowerCase();
  let kind = INBOX_KINDS.VALIDATION_ISSUES;
  if (/address/.test(code)) kind = INBOX_KINDS.MISSING_ADDRESS;
  if (/recipient|contact/.test(code)) kind = INBOX_KINDS.MISSING_RECIPIENT;
  if (/confidence|low_confidence/.test(code)) {
    kind = INBOX_KINDS.LOW_CONFIDENCE_INTELLIGENCE;
  }
  return buildCandidate(
    {
      kind,
      title: v.message || v.title || defaultKindTitle(kind),
      sourceCapability: v.sourceCapability || 'validation',
      subjectId: v.prospectId || v.subjectId,
      notes: v.detail || v.notes,
      deepLink: {
        workspace:
          kind === INBOX_KINDS.LOW_CONFIDENCE_INTELLIGENCE
            ? WORKSPACE_TARGETS.COMPANY_INTELLIGENCE
            : WORKSPACE_TARGETS.CAMPAIGN_REVIEW,
        missionId: ctx.missionId,
        campaignId: ctx.campaignId,
      },
    },
    ctx
  );
}

/**
 * @param {object} raw
 * @param {object} ctx
 * @returns {object}
 */
function buildCandidate(raw, ctx) {
  const missionId = raw.missionId || ctx.missionId;
  const campaignId = raw.campaignId || ctx.campaignId;
  const clientId = raw.clientId != null ? raw.clientId : ctx.clientId;
  const item = buildInboxItem({
    ...raw,
    id: raw.id || newId('ibox'),
    missionId,
    campaignId,
    clientId,
    status: raw.status || INBOX_STATUS.OPEN,
    deepLink: buildDeepLink({
      ...(raw.deepLink || {}),
      kind: raw.kind,
      missionId,
      campaignId,
      workspace: raw.deepLink && raw.deepLink.workspace,
    }),
  });
  item.dedupeKey = buildDedupeKey(item);
  item.evidenceBacked = raw.evidenceBacked;
  item.blocking = raw.blocking;
  return item;
}

/**
 * @param {string} kind
 * @returns {string}
 */
function defaultKindTitle(kind) {
  return String(kind || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

module.exports = {
  ingestWorkItems,
  buildCandidate,
};
