'use strict';

/**
 * Read-only routing for operator operational status questions inside the workspace.
 */

const pool = require('../../../db');
const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  classifyOperatorMissionTurnIntent,
  OPERATOR_TURN_INTENT_TYPES,
  STATUS_QUERY_DOMAINS,
  STATUS_QUERY_ACTIONS,
} = require('./OperatorMissionTurnIntent');
const { resolveTenantId } = require('./WorkspaceMissionInspection');

function normalizeChannel(channel) {
  const c = String(channel || '').toLowerCase();
  if (c.includes('facebook')) return 'facebook';
  if (c.includes('linkedin')) return 'linkedin';
  if (c.includes('gbp') || c.includes('google')) return 'google_business';
  return c || 'other';
}

async function countPendingCommentsByChannel(clientId, queryPool = pool) {
  const res = await queryPool.query(
    `SELECT channel, COUNT(*)::int AS count
     FROM pending_comments
     WHERE status = 'pending' AND client_id = $1
     GROUP BY channel`,
    [clientId]
  );
  const byChannel = { facebook: 0, linkedin: 0, google_business: 0, other: 0 };
  for (const row of res.rows) {
    const key = normalizeChannel(row.channel);
    if (byChannel[key] != null) {
      byChannel[key] += row.count;
    } else {
      byChannel.other += row.count;
    }
  }
  return byChannel;
}

async function resolveClientLabel(clientId, queryPool = pool) {
  try {
    const res = await queryPool.query(
      'SELECT name FROM clients WHERE id = $1 LIMIT 1',
      [clientId]
    );
    return res.rows[0] && res.rows[0].name ? res.rows[0].name : `client ${clientId}`;
  } catch (_) {
    return `client ${clientId}`;
  }
}

function buildPaigeSocialAnswer(clientLabel, byChannel, canonicalPending = 0) {
  const total =
    byChannel.facebook +
    byChannel.linkedin +
    byChannel.google_business +
    byChannel.other +
    canonicalPending;

  if (total === 0) {
    return {
      prose:
        'Paige has no pending social posts awaiting your approval right now.',
      structuredMeta: {
        intent: OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY,
        domain: STATUS_QUERY_DOMAINS.PAIGE_SOCIAL,
        action: STATUS_QUERY_ACTIONS.READ_PENDING_APPROVAL_QUEUE,
        mutates_mission_state: false,
        pendingCounts: { facebook: 0, linkedin: 0, total: 0 },
      },
    };
  }

  const lines = [
    `I'll check Paige's pending social queue for ${clientLabel}.`,
    '',
    'Pending approval:',
    `- Facebook: ${byChannel.facebook}`,
    `- LinkedIn: ${byChannel.linkedin}`,
  ];
  if (byChannel.google_business > 0) {
    lines.push(`- Google Business: ${byChannel.google_business}`);
  }
  if (byChannel.other > 0) {
    lines.push(`- Other channels: ${byChannel.other}`);
  }
  if (canonicalPending > 0) {
    lines.push(`- Canonical social drafts: ${canonicalPending}`);
  }
  lines.push('', 'Next action:', '- Review pending drafts');

  return {
    prose: lines.join('\n'),
    structuredMeta: {
      intent: OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY,
      domain: STATUS_QUERY_DOMAINS.PAIGE_SOCIAL,
      action: STATUS_QUERY_ACTIONS.READ_PENDING_APPROVAL_QUEUE,
      mutates_mission_state: false,
      pendingCounts: {
        facebook: byChannel.facebook,
        linkedin: byChannel.linkedin,
        google_business: byChannel.google_business,
        other: byChannel.other,
        canonicalPending,
        total,
      },
    },
  };
}

async function readPaigePendingApprovalQueue(input = {}) {
  const tenantId = resolveTenantId(input);
  const clientId = Number(tenantId);
  if (!Number.isInteger(clientId) || clientId < 1) {
    throw new Error('tenant_scope_required');
  }
  const queryPool = input.pool || pool;
  const clientLabel = await resolveClientLabel(clientId, queryPool);
  const byChannel = await countPendingCommentsByChannel(clientId, queryPool);

  let canonicalPending = 0;
  try {
    const { inspectPaigeSocialContentStatus } = require('../../../services/paigeSocialContentInspection');
    const status = await inspectPaigeSocialContentStatus({
      tenantId: String(clientId),
      clientId,
      missionId: input.missionId || null,
      store: input.socialStore,
    });
    canonicalPending = status.counts && status.counts.pendingApproval
      ? status.counts.pendingApproval
      : 0;
  } catch (_) {
    /* canonical store optional in tests */
  }

  return buildPaigeSocialAnswer(clientLabel, byChannel, canonicalPending);
}

async function readPendingOperatorApprovalItems(input = {}) {
  const tenantId = resolveTenantId(input);
  const clientId = Number(tenantId);
  const queryPool = input.pool || pool;
  const clientLabel = await resolveClientLabel(clientId, queryPool);
  const byChannel = await countPendingCommentsByChannel(clientId, queryPool);
  const total =
    byChannel.facebook +
    byChannel.linkedin +
    byChannel.google_business +
    byChannel.other;

  if (total === 0) {
    return {
      prose: `Nothing is waiting in the approval queue for ${clientLabel} right now.`,
      structuredMeta: {
        intent: OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY,
        domain: STATUS_QUERY_DOMAINS.OPERATOR_APPROVAL,
        action: STATUS_QUERY_ACTIONS.READ_PENDING_OPERATOR_APPROVAL_ITEMS,
        mutates_mission_state: false,
        pendingCounts: { total: 0 },
      },
    };
  }

  return {
    prose: [
      `Pending operator approval items for ${clientLabel}:`,
      `- Social drafts (pending_comments): ${total}`,
      `- Facebook: ${byChannel.facebook}`,
      `- LinkedIn: ${byChannel.linkedin}`,
      byChannel.google_business > 0 ? `- Google Business: ${byChannel.google_business}` : null,
      '',
      'Next action: open Approvals or ask about Paige pending posts by channel.',
    ]
      .filter(Boolean)
      .join('\n'),
    structuredMeta: {
      intent: OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY,
      domain: STATUS_QUERY_DOMAINS.OPERATOR_APPROVAL,
      action: STATUS_QUERY_ACTIONS.READ_PENDING_OPERATOR_APPROVAL_ITEMS,
      mutates_mission_state: false,
      pendingCounts: { ...byChannel, total },
    },
  };
}

function buildAmbiguousGoAheadAnswer() {
  return {
    prose:
      'I can proceed, but more than one approval may be waiting. Tell me which action to approve — mission stage, discovery, execution, or Paige social drafts.',
    structuredMeta: {
      intent: OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY,
      domain: STATUS_QUERY_DOMAINS.GENERAL,
      action: null,
      mutates_mission_state: false,
      ambiguousGoAhead: true,
    },
  };
}

async function routeStatusQuery(classification, input = {}) {
  if (classification.ambiguousGoAhead) {
    return buildAmbiguousGoAheadAnswer();
  }
  if (classification.action === STATUS_QUERY_ACTIONS.READ_PENDING_APPROVAL_QUEUE) {
    return readPaigePendingApprovalQueue(input);
  }
  if (classification.action === STATUS_QUERY_ACTIONS.READ_PENDING_OPERATOR_APPROVAL_ITEMS) {
    return readPendingOperatorApprovalItems(input);
  }
  return null;
}

/**
 * @param {object} input
 * @returns {Promise<{ reason: string, prose: string, structured: object }|null>}
 */
async function maybeHandleOperatorStatusQuery(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return null;

  const hasSinglePendingOperatorApproval =
    input.hasSinglePendingOperatorApproval === true ||
    (input.mission &&
      input.mission.pendingOperatorDecision &&
      !Array.isArray(input.mission.pendingOperatorDecision));

  const classification = classifyOperatorMissionTurnIntent(question, {
    hasSinglePendingOperatorApproval,
  });

  if (classification.type !== OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY) {
    return null;
  }

  const routed = await routeStatusQuery(classification, input);
  if (!routed) return null;

  const structured = buildStructuredResponse({
    answer: routed.prose,
    reasoning: [],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: 0.94,
    nextInvestigations: [],
    recommendedActions: [],
    confidenceContributors: ['status_query', 'read_only'],
    timelineReferences: [],
    relatedEntities: [],
    metadata: {
      ...routed.structuredMeta,
      strictOutputShape: true,
      statusQuery: true,
    },
  });

  return {
    reason: `status_query_${classification.domain || 'general'}`,
    prose: routed.prose,
    structured,
    classification,
  };
}

module.exports = {
  maybeHandleOperatorStatusQuery,
  routeStatusQuery,
  readPaigePendingApprovalQueue,
  readPendingOperatorApprovalItems,
  countPendingCommentsByChannel,
};
