'use strict';

const pool = require('../db');
const aoBriefing = require('./aoBriefingService');
const {
  CAMPAIGN_NAME,
  DIRECT_MAIL_TARGETS,
  formatActionItem,
  buildActionCards,
  buildMikeInstructions,
  buildTodayChanges,
  buildCommandRail,
  buildDayZeroOperatorBrief,
} = require('../utils/aoCommandDeckBrief');
const { CLIENT_ID: ANCHOR_CLIENT_ID } = require('../scripts/data/anchorDirectMailTargets');

const ANCHOR_CLIENT = ANCHOR_CLIENT_ID || 10;

async function clientHasAoActivity(clientId) {
  const { rows } = await pool.query(`
    SELECT COUNT(*)::int AS n FROM ao_leads WHERE client_id = $1 LIMIT 1
  `, [clientId]);
  return (rows[0]?.n || 0) > 0;
}

async function shouldBuildOperatorBrief(clientId) {
  if (Number(clientId) === ANCHOR_CLIENT) return true;
  return clientHasAoActivity(clientId);
}

/**
 * Command Deck operator slice — AO/Campaign 001 intelligence as the primary brief.
 */
async function buildOperatorBrief(clientId) {
  const eligible = await shouldBuildOperatorBrief(clientId);
  if (!eligible) return null;

  const hasAo = await clientHasAoActivity(clientId);
  if (!hasAo) {
    return buildDayZeroOperatorBrief();
  }

  const briefing = await aoBriefing.buildBriefing(clientId);
  const campaign = briefing.campaign_001 || {};
  const today = briefing.today || {};
  const visitsToday = today.visits_today || 0;
  const targetTotal = campaign.target_total || DIRECT_MAIL_TARGETS.length;
  const promoCount = (briefing.promotion_candidates || []).length;

  let narrative;
  if (visitsToday === 0 && (campaign.seeded_in_ao > 0 || targetTotal > 0)) {
    narrative = `Mike has ${targetTotal} Campaign 001 targets queued. No visits logged today yet.`;
    if (campaign.remaining_route_queue > 0) {
      narrative += ` ${campaign.remaining_route_queue} stops are still on the route.`;
    }
    narrative += ' Highest-leverage action: have Mike start the Manchester direct-mail route and review after 3 stops.';
  } else {
    narrative = briefing.daily_digest?.text || briefing.daily_digest?.paragraphs?.join(' ') || '';
  }

  let highestLeverage;
  if (visitsToday === 0 && (campaign.remaining_route_queue > 0 || targetTotal > 0)) {
    highestLeverage = {
      title: 'Have Mike start the Manchester direct-mail route',
      detail: 'Review progress after 3 stops — then check escalations and warm leads on Command Deck.',
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
        action: 'Review field progress after Mike\'s first 3 stops',
        detail: 'No field activity logged yet today',
      }));
    } else {
      jakeActions.push(formatActionItem({
        action: 'No Jake actions pending',
        detail: 'Monitor warm opportunities — use View Escalations if anything heats up',
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

  const openEscalations = today.open_escalations || 0;
  const todayChanges = (visitsToday === 0 && (campaign.seeded_in_ao > 0 || targetTotal > 0))
    ? buildTodayChanges({
      visitsToday: 0,
      campaign,
      openEscalations,
      paragraphs: [
        `${targetTotal} Campaign 001 targets queued in AO.`,
        'No field visits logged today yet.',
      ],
    })
    : buildTodayChanges({
      visitsToday,
      campaign,
      openEscalations,
      paragraphs: briefing.daily_digest?.paragraphs || [],
    });
  const commandRail = buildCommandRail({
    escalations: briefing.needs_jake || [],
    campaign,
    today,
    mikeActions,
    promoCount,
    visitsToday,
  });

  return {
    narrative,
    highestLeverage,
    todayChanges,
    jakeActions,
    mikeActions,
    actionCards: buildActionCards({ openEscalations, promoCount }),
    mikeInstructions: buildMikeInstructions({ mikeActions, campaign, narrative }),
    commandRail,
    drillDown: {
      escalations: briefing.needs_jake || [],
      campaign,
      promoCandidates: briefing.promotion_candidates || [],
    },
    generatedAt: briefing.generated_at || new Date().toISOString(),
    campaign_name: CAMPAIGN_NAME,
    mode: 'ao_operator',
  };
}

module.exports = {
  buildOperatorBrief,
  clientHasAoActivity,
  shouldBuildOperatorBrief,
  buildDayZeroOperatorBrief,
};
