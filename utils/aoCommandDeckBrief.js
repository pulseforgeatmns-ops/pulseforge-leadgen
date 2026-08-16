'use strict';

const { CAMPAIGN_NAME, DIRECT_MAIL_TARGETS } = require('../scripts/data/anchorDirectMailTargets');

function formatActionItem(item) {
  return {
    action: item.action || item.title || 'Review',
    detail: item.business || item.contact || item.detail || null,
    href: item.href || null,
    lead_id: item.lead_id || null,
    escalation_id: item.escalation_id || null,
  };
}

function buildActionCards({ openEscalations, promoCount }) {
  return [
    { id: 'ao_route', label: "Open Mike's Route", href: '/ao', kind: 'link' },
    {
      id: 'escalations',
      label: openEscalations ? `View Escalations (${openEscalations})` : 'View Escalations',
      href: '#cd-escalations',
      kind: 'link',
    },
    { id: 'field_visits', label: 'View Field Visits', href: '/admin/field-visits', kind: 'link' },
    { id: 'campaign', label: 'View Campaign 001', href: '#cd-campaign', kind: 'link' },
    ...(promoCount
      ? [{
        id: 'promo',
        label: `Promote CRM Candidates (${promoCount})`,
        href: '#cd-promo',
        kind: 'link',
      }]
      : []),
  ];
}

function buildMikeInstructions({ mikeActions, campaign, narrative }) {
  const lines = [
    'Mike — Campaign 001 field priorities',
    '',
    ...mikeActions.map((a, i) => `${i + 1}. ${a.action}${a.detail ? ` (${a.detail})` : ''}`),
    '',
    `Queue: ${campaign.remaining_route_queue ?? campaign.target_total ?? DIRECT_MAIL_TARGETS.length} stops remaining · ${campaign.target_total || DIRECT_MAIL_TARGETS.length} total targets`,
    '',
    narrative,
  ];
  return lines.filter(Boolean).join('\n');
}

function buildTodayChanges({ visitsToday = 0, campaign = {}, openEscalations = 0, paragraphs = [] } = {}) {
  if (Array.isArray(paragraphs) && paragraphs.length) {
    return paragraphs.slice(0, 5);
  }
  const targetTotal = campaign.target_total || DIRECT_MAIL_TARGETS.length;
  const changes = [];
  if (visitsToday > 0) {
    changes.push(`Mike logged ${visitsToday} field visit${visitsToday === 1 ? '' : 's'} today.`);
  } else {
    changes.push('No field visits logged today yet.');
  }
  if (targetTotal) {
    changes.push(`${targetTotal} Campaign 001 targets queued in AO.`);
  }
  if (openEscalations > 0) {
    changes.push(`${openEscalations} open escalation${openEscalations === 1 ? '' : 's'} need Jake.`);
  }
  return changes.slice(0, 5);
}

function buildCommandRailQuickActions({ openEscalationCount = 0, promoCount = 0 } = {}) {
  const actions = [
    { id: 'ao_route', label: 'Open Mike Route', href: '/ao', kind: 'link' },
    {
      id: 'escalations',
      label: openEscalationCount ? `View Escalations (${openEscalationCount})` : 'View Escalations',
      href: '#cd-escalations',
      kind: 'link',
    },
    { id: 'field_visits', label: 'View Field Visits', href: '/admin/field-visits', kind: 'link' },
    { id: 'campaign', label: 'View Campaign 001', href: '#cd-campaign', kind: 'link' },
  ];
  if (promoCount > 0) {
    actions.push({
      id: 'promo',
      label: `Promote CRM Candidates (${promoCount})`,
      href: '#cd-promo',
      kind: 'link',
    });
  }
  return actions;
}

function buildCommandRail({
  escalations = [],
  campaign = {},
  today = {},
  mikeActions = [],
  promoCount = 0,
  visitsToday = 0,
}) {
  const openEscalations = (escalations || []).filter((e) =>
    ['new', 'seen', 'in_progress'].includes(e.status)
  );

  const needsJake = openEscalations.slice(0, 6).map((e) => ({
    id: e.id,
    lead_id: e.lead_id,
    business_name: e.business_name,
    contact_name: e.contact_name,
    contact_title: e.contact_title,
    phone: e.phone,
    email: e.email,
    reason: e.reason,
    summary: e.visit_summary || e.summary,
    recommended_action: e.recommended_action,
    urgency: e.urgency,
    status: e.status,
    is_walkthrough: /walkthrough|tour/i.test(String(e.reason || '')),
    admin_visit_url: e.admin_visit_url || `/admin/field-visits/?lead=${e.lead_id}`,
  }));

  const targetTotal = campaign.target_total || DIRECT_MAIL_TARGETS.length;
  const visited = campaign.visited || 0;
  const remaining = campaign.not_yet_touched ?? campaign.remaining_route_queue ?? targetTotal;
  const walkIns = campaign.walk_in_queue ?? remaining;
  const phoneFirst = campaign.phone_first_queue || 0;

  const mikeAo = {
    status: visitsToday > 0
      ? `${visitsToday} visit${visitsToday === 1 ? '' : 's'} logged today`
      : 'No visits logged today',
    route_progress: `${visited}/${targetTotal} touched`,
    remaining: campaign.remaining_route_queue ?? remaining,
    queue_count: (campaign.remaining_route_queue || 0) + phoneFirst,
    next_action: mikeActions[0]?.action || `Start Campaign 001 route (${remaining} stops)`,
    next_detail: mikeActions[0]?.detail || 'Manchester direct-mail in-person revisits',
    visits_today: visitsToday || 0,
  };

  const campaign001 = {
    name: CAMPAIGN_NAME,
    total: targetTotal,
    visited,
    remaining,
    walk_ins: walkIns,
    phone_first: phoneFirst,
    meaningful_conversations: campaign.meaningful_conversations || 0,
    walkthrough_requests: campaign.walkthrough_requests || 0,
    details_href: '#cd-campaign',
  };

  const quickActions = buildCommandRailQuickActions({
    openEscalationCount: openEscalations.length,
    promoCount,
  });

  return { needsJake, mikeAo, campaign001, quickActions };
}

function buildDayZeroOperatorBrief() {
  const targetTotal = DIRECT_MAIL_TARGETS.length;
  const narrative = `Mike has ${targetTotal} Campaign 001 targets queued. No visits logged today yet. Highest-leverage action: have Mike start the Manchester direct-mail route and review after 3 stops.`;
  const highestLeverage = {
    title: 'Have Mike start the Manchester direct-mail route',
    detail: 'Review progress after 3 stops — then check escalations and warm leads here on Command Deck.',
  };
  const jakeActions = [
    formatActionItem({
      action: 'Review field progress after Mike\'s first 3 stops',
      detail: 'No visits logged yet today',
    }),
  ];
  const mikeActions = [
    formatActionItem({
      action: `Start Campaign 001 route (${targetTotal} stops)`,
      detail: 'Manchester direct-mail in-person revisits',
    }),
  ];
  const campaign = {
    target_total: targetTotal,
    remaining_route_queue: targetTotal,
    seeded_in_ao: 0,
    visited: 0,
    not_yet_touched: targetTotal,
    walk_in_queue: targetTotal,
    phone_first_queue: 0,
    meaningful_conversations: 0,
    walkthrough_requests: 0,
  };
  const todayChanges = buildTodayChanges({
    visitsToday: 0,
    campaign,
    openEscalations: 0,
    paragraphs: [
      `${targetTotal} Campaign 001 targets queued in AO.`,
      'No field visits logged today yet.',
    ],
  });
  const commandRail = buildCommandRail({
    escalations: [],
    campaign,
    today: { visits_today: 0, open_escalations: 0 },
    mikeActions,
    promoCount: 0,
    visitsToday: 0,
  });
  return {
    narrative,
    highestLeverage,
    todayChanges,
    jakeActions,
    mikeActions,
    actionCards: buildActionCards({ openEscalations: 0, promoCount: 0 }),
    mikeInstructions: buildMikeInstructions({ mikeActions, campaign, narrative }),
    commandRail,
    drillDown: { escalations: [], campaign, promoCandidates: [] },
    generatedAt: new Date().toISOString(),
    campaign_name: CAMPAIGN_NAME,
    mode: 'ao_operator',
  };
}

module.exports = {
  CAMPAIGN_NAME,
  DIRECT_MAIL_TARGETS,
  formatActionItem,
  buildActionCards,
  buildMikeInstructions,
  buildTodayChanges,
  buildCommandRailQuickActions,
  buildCommandRail,
  buildDayZeroOperatorBrief,
};
