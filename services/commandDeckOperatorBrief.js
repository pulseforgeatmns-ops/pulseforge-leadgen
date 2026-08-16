'use strict';

const pool = require('../db');
const aoBriefing = require('./aoBriefingService');
const { CAMPAIGN_NAME, DIRECT_MAIL_TARGETS } = require('../scripts/data/anchorDirectMailTargets');

async function clientHasAoActivity(clientId) {
  const { rows } = await pool.query(`
    SELECT COUNT(*)::int AS n FROM ao_leads WHERE client_id = $1 LIMIT 1
  `, [clientId]);
  return (rows[0]?.n || 0) > 0;
}

function formatActionItem(item) {
  return {
    action: item.action || item.title || 'Review',
    detail: item.business || item.contact || item.detail || null,
    href: item.href || null,
    lead_id: item.lead_id || null,
    escalation_id: item.escalation_id || null,
  };
}

function buildMikeInstructions({ mikeActions, campaign, narrative }) {
  const lines = [
    'Mike — Campaign 001 field priorities',
    '',
    ...mikeActions.map((a, i) => `${i + 1}. ${a.action}${a.detail ? ` (${a.detail})` : ''}`),
    '',
    `Queue: ${campaign.remaining_route_queue} stops remaining · ${campaign.target_total} total targets`,
    '',
    narrative,
  ];
  return lines.filter(Boolean).join('\n');
}

/**
 * Simplified Command Deck slice — narrative + actions, no metric grids.
 */
async function buildOperatorBrief(clientId) {
  const hasAo = await clientHasAoActivity(clientId);
  if (!hasAo) return null;

  const briefing = await aoBriefing.buildBriefing(clientId);
  const campaign = briefing.campaign_001 || {};
  const today = briefing.today || {};
  const visitsToday = today.visits_today || 0;
  const targetTotal = campaign.target_total || DIRECT_MAIL_TARGETS.length;

  let narrative;
  if (visitsToday === 0 && (campaign.seeded_in_ao > 0 || targetTotal > 0)) {
    narrative = `Mike has ${targetTotal} Campaign 001 targets queued. No visits logged today yet.`;
    if (campaign.remaining_route_queue > 0) {
      narrative += ` ${campaign.remaining_route_queue} stops are still on the route.`;
    }
  } else {
    narrative = briefing.daily_digest?.text || briefing.daily_digest?.paragraphs?.join(' ') || '';
  }

  let highestLeverage;
  if (visitsToday === 0 && (campaign.remaining_route_queue > 0 || targetTotal > 0)) {
    highestLeverage = {
      title: 'Have Mike start the Manchester direct-mail route',
      detail: 'Review progress after 3 stops — then check AO Briefing for escalations and warm leads.',
    };
  } else if (briefing.needs_jake?.length) {
    const top = briefing.needs_jake[0];
    highestLeverage = {
      title: top.recommended_action || 'Jake follow-up required',
      detail: [top.business_name, top.contact_name].filter(Boolean).join(' — ') || top.reason,
    };
  } else if (briefing.warm_opportunities?.length) {
    const w = briefing.warm_opportunities[0];
    highestLeverage = {
      title: `Follow up: ${w.business_name}`,
      detail: `${w.warm_reason}. Next: ${w.next_step}.`,
    };
  } else if (briefing.recommended_actions?.mike?.length) {
    const m = briefing.recommended_actions.mike[0];
    highestLeverage = {
      title: m.action,
      detail: m.detail || '',
    };
  } else {
    highestLeverage = {
      title: 'Continue Campaign 001 outreach',
      detail: `${campaign.remaining_route_queue || 0} route stops remaining.`,
    };
  }

  const jakeActions = (briefing.recommended_actions?.jake || []).slice(0, 6).map(formatActionItem);
  if (!jakeActions.length) {
    if (visitsToday === 0) {
      jakeActions.push(formatActionItem({
        action: 'Review AO Briefing after Mike\'s first 3 stops',
        detail: 'No field activity logged yet today',
      }));
    } else {
      jakeActions.push(formatActionItem({
        action: 'No Jake actions pending',
        detail: 'Check warm opportunities in AO Briefing if anything heats up',
      }));
    }
  }

  const mikeActions = (briefing.recommended_actions?.mike || []).slice(0, 6).map(formatActionItem);
  if (!mikeActions.length) {
    mikeActions.push(formatActionItem({
      action: `Start Campaign 001 route (${campaign.remaining_route_queue || targetTotal} stops)`,
      detail: 'Manchester direct-mail in-person revisits',
    }));
  }

  const openEscalations = today.open_escalations || briefing.needs_jake?.length || 0;
  const actionCards = [
    { id: 'ao_briefing', label: 'Open AO Briefing', href: '/max-briefing', kind: 'link' },
    { id: 'field_visits', label: 'Open Field Visits', href: '/admin/field-visits', kind: 'link' },
    {
      id: 'escalations',
      label: openEscalations ? `View Escalations (${openEscalations})` : 'View Escalations',
      href: '/max-briefing#escalations',
      kind: 'link',
    },
    { id: 'ao_route', label: 'Open Mike\'s Route', href: '/ao', kind: 'link' },
    { id: 'copy_instructions', label: 'Copy Mike Instructions', kind: 'copy' },
  ];

  const mikeInstructions = buildMikeInstructions({ mikeActions, campaign, narrative });

  return {
    narrative,
    highestLeverage,
    jakeActions,
    mikeActions,
    actionCards,
    mikeInstructions,
    generatedAt: briefing.generated_at || new Date().toISOString(),
    campaign_name: CAMPAIGN_NAME,
  };
}

module.exports = {
  buildOperatorBrief,
  clientHasAoActivity,
};
