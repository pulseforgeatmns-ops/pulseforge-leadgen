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
      href: '/max-briefing#escalations',
      kind: 'link',
    },
    { id: 'field_visits', label: 'View Field Visits', href: '/admin/field-visits', kind: 'link' },
    { id: 'campaign', label: 'View Campaign 001', href: '/max-briefing#campaign', kind: 'link' },
    {
      id: 'promo',
      label: promoCount ? `Promote CRM Candidates (${promoCount})` : 'Promote CRM Candidates',
      href: '/max-briefing#promo',
      kind: 'link',
    },
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
  };
  return {
    narrative,
    highestLeverage,
    jakeActions,
    mikeActions,
    actionCards: buildActionCards({ openEscalations: 0, promoCount: 0 }),
    mikeInstructions: buildMikeInstructions({ mikeActions, campaign, narrative }),
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
  buildDayZeroOperatorBrief,
};
