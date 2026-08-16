'use strict';

const pool = require('../db');
const { addProspect, addCompany } = require('../dbClient');
const {
  deriveOperationalState,
  deriveCampaignOutcome,
  buildRelationshipIntel,
  compareOperationalPriority,
  recommendCrmPromotion,
  mapEscalationUrgency,
  recommendEscalationAction,
  extractObjections,
  extractVendorComplaints,
  parseProbeAnswers,
  inferSignalType,
} = require('../utils/aoOperationalState');
const { CAMPAIGN_NAME, DIRECT_MAIL_TARGETS } = require('../scripts/data/anchorDirectMailTargets');

const CAMPAIGN_001 = CAMPAIGN_NAME;

function todayISO(date = new Date()) {
  return date.toISOString().slice(0, 10);
}

function mapLeadRow(row) {
  const operationalState = deriveOperationalState(row);
  const intel = buildRelationshipIntel(row);
  const promotion = recommendCrmPromotion(row);
  return {
    id: row.id,
    business_name: row.business_name,
    address: row.address,
    business_type: row.business_type,
    status: row.status,
    operational_state: operationalState,
    campaign_outcome: deriveCampaignOutcome(row),
    interest_level: row.interest_level,
    ao_owner_id: row.ao_owner_id,
    ao_name: row.ao_name || null,
    attribution_source: row.attribution_source,
    campaign_name: row.campaign_name,
    crm_prospect_id: row.crm_prospect_id || null,
    contact_name: row.contact_name,
    contact_title: row.contact_title,
    contact_phone: row.contact_phone,
    contact_email: row.contact_email,
    is_decision_maker: row.is_decision_maker,
    original_visit_note: row.original_visit_note,
    last_contact_date: row.last_contact_date,
    next_follow_up_date: row.next_follow_up_date,
    open_escalation_id: row.open_escalation_id || null,
    open_escalation_status: row.open_escalation_status || null,
    waiting_on_jake: Boolean(row.waiting_on_jake),
    open_next_action: row.open_next_action || null,
    open_task_due: row.open_task_due || null,
    relationship: intel,
    crm_promotion: promotion,
    warm_score: computeWarmScore(row, operationalState, intel),
  };
}

function computeWarmScore(row, state, intel) {
  let score = 0;
  if (state === 'walkthrough_requested') score += 100;
  if (state === 'jake_action_needed') score += 90;
  if (state === 'decision_maker_reached') score += 70;
  if (intel.interest_level === 'high') score += 30;
  if (intel.interest_level === 'medium') score += 15;
  if (intel.current_pain) score += 20;
  if (intel.signal_type === 'real_buying_signal') score += 25;
  if (row.open_escalation_id && row.open_escalation_status === 'new') score += 40;
  if (intel.price_shopping_risk === 'likely') score -= 10;
  return score;
}

async function fetchEnrichedLeads(clientId, { campaignName = null, aoOwnerId = null } = {}) {
  const params = [clientId];
  let where = 'l.client_id = $1';
  if (campaignName) {
    params.push(campaignName);
    where += ` AND l.campaign_name = $${params.length}`;
  }
  if (aoOwnerId) {
    params.push(aoOwnerId);
    where += ` AND l.ao_owner_id = $${params.length}`;
  }

  const { rows } = await pool.query(`
    SELECT
      l.*,
      u.name AS ao_name,
      c.contact_name, c.contact_title, c.phone AS contact_phone, c.email AS contact_email,
      c.is_decision_maker,
      ot.id AS open_task_id,
      ot.status AS open_task_status,
      ot.next_action AS open_next_action,
      ot.due_date AS open_task_due,
      ot.waiting_on_jake,
      ot.last_interaction_summary,
      oe.id AS open_escalation_id,
      oe.status AS open_escalation_status,
      oe.reason AS open_escalation_reason
    FROM ao_leads l
    JOIN users u ON u.id = l.ao_owner_id
    LEFT JOIN LATERAL (
      SELECT * FROM ao_contacts
      WHERE lead_id = l.id
      ORDER BY is_decision_maker DESC, created_at ASC
      LIMIT 1
    ) c ON true
    LEFT JOIN LATERAL (
      SELECT * FROM ao_follow_up_tasks
      WHERE lead_id = l.id AND status = 'open'
      ORDER BY due_date ASC, created_at ASC
      LIMIT 1
    ) ot ON true
    LEFT JOIN LATERAL (
      SELECT * FROM ao_escalations
      WHERE lead_id = l.id AND status NOT IN ('resolved', 'ignored')
      ORDER BY created_at DESC
      LIMIT 1
    ) oe ON true
    WHERE ${where}
    ORDER BY l.updated_at DESC
  `, params);

  return rows.map(mapLeadRow);
}

async function getTodayActivity(clientId, asOf = todayISO()) {
  const { rows } = await pool.query(`
    SELECT
      (SELECT COUNT(*)::int FROM ao_leads
        WHERE client_id = $1 AND DATE(last_contact_date) = $2::date) AS visits_today,
      (SELECT COUNT(*)::int FROM ao_follow_up_tasks t
        JOIN ao_leads l ON l.id = t.lead_id
        WHERE l.client_id = $1 AND t.status = 'done'
          AND t.next_action = 'phone_follow_up'
          AND DATE(t.completed_at) = $2::date) AS calls_today,
      (SELECT COUNT(*)::int FROM ao_escalations e
        JOIN ao_leads l ON l.id = e.lead_id
        WHERE l.client_id = $1 AND e.status NOT IN ('resolved', 'ignored')) AS open_escalations,
      (SELECT COUNT(*)::int FROM ao_follow_up_tasks t
        JOIN ao_leads l ON l.id = t.lead_id
        WHERE l.client_id = $1 AND t.status = 'open' AND t.due_date < $2::date) AS overdue_follow_ups,
      (SELECT COUNT(*)::int FROM ao_follow_up_tasks t
        JOIN ao_leads l ON l.id = t.lead_id
        WHERE l.client_id = $1 AND t.status = 'open' AND t.due_date = $2::date) AS due_today
  `, [clientId, asOf]);
  return rows[0] || {};
}

async function listEscalationInbox(clientId, { status = null, includeResolved = false } = {}) {
  const params = [clientId];
  let where = 'l.client_id = $1';
  if (status) {
    params.push(status);
    where += ` AND e.status = $${params.length}`;
  } else if (!includeResolved) {
    where += ` AND e.status NOT IN ('resolved', 'ignored')`;
  }

  const { rows } = await pool.query(`
    SELECT
      e.*,
      l.business_name, l.address, l.interest_level, l.attribution_source, l.campaign_name,
      l.original_visit_note, l.id AS lead_id,
      c.contact_name, c.contact_title, c.phone AS contact_phone, c.email AS contact_email,
      u.name AS ao_name
    FROM ao_escalations e
    JOIN ao_leads l ON l.id = e.lead_id
    LEFT JOIN ao_contacts c ON c.id = e.contact_id
    JOIN users u ON u.id = e.ao_owner_id
    WHERE ${where}
    ORDER BY
      CASE e.status WHEN 'new' THEN 0 WHEN 'seen' THEN 1 WHEN 'in_progress' THEN 2 ELSE 9 END,
      e.created_at DESC
    LIMIT 200
  `, params);

  return rows.map(row => ({
    id: row.id,
    lead_id: row.lead_id,
    business_name: row.business_name,
    address: row.address,
    contact_name: row.contact_name,
    contact_title: row.contact_title,
    phone: row.contact_phone,
    email: row.contact_email,
    source: row.attribution_source,
    campaign: row.campaign_name,
    ao_owner: row.ao_name,
    ao_owner_id: row.ao_owner_id,
    visit_summary: row.summary,
    reason: row.reason,
    recommended_action: recommendEscalationAction(row),
    urgency: mapEscalationUrgency(row),
    status: row.status,
    created_at: row.created_at,
    resolved_at: row.resolved_at,
    interest_level: row.interest_level,
    probe_answers: parseProbeAnswers(row.probe_answers),
    admin_visit_url: `/admin/field-visits/?lead=${row.lead_id}`,
  }));
}

async function getCampaign001Progress(clientId) {
  const leads = await fetchEnrichedLeads(clientId, { campaignName: CAMPAIGN_001 });
  const targetTotal = DIRECT_MAIL_TARGETS.length;
  const seeded = leads.length;
  const touched = leads.filter(l => l.operational_state !== 'not_started').length;
  const notYetTouched = leads.filter(l => l.operational_state === 'not_started').length;
  const meaningful = leads.filter(l =>
    ['decision_maker_reached', 'walkthrough_requested', 'jake_action_needed', 'contact_identified'].includes(l.operational_state)
    || (l.relationship.interest_level === 'high' && l.operational_state !== 'not_started')
  ).length;
  const decisionMakers = leads.filter(l =>
    ['decision_maker_reached', 'walkthrough_requested', 'jake_action_needed'].includes(l.operational_state)
  ).length;
  const escalations = leads.filter(l => l.open_escalation_id).length;
  const walkthroughs = leads.filter(l => l.operational_state === 'walkthrough_requested').length;
  const disqualified = leads.filter(l => l.operational_state === 'disqualified').length;
  const remainingQueue = leads.filter(l =>
    l.operational_state === 'not_started'
    || (l.open_next_action === 'in_person_revisit' && l.open_task_due)
  ).length;

  const outcomeCounts = {};
  for (const lead of leads) {
    const key = lead.campaign_outcome;
    outcomeCounts[key] = (outcomeCounts[key] || 0) + 1;
  }

  return {
    campaign_name: CAMPAIGN_001,
    target_total: targetTotal,
    seeded_in_ao: seeded,
    visited: touched,
    not_yet_touched: notYetTouched,
    meaningful_conversations: meaningful,
    decision_makers_reached: decisionMakers,
    escalations,
    walkthrough_requests: walkthroughs,
    disqualified,
    remaining_route_queue: remainingQueue,
    outcome_counts: outcomeCounts,
  };
}

function buildFieldIntelligence(leads) {
  const objections = {};
  const vendorComplaints = {};
  const painPoints = {};
  const segmentNotes = {};

  for (const lead of leads) {
    for (const obj of lead.relationship.objections || []) {
      objections[obj] = (objections[obj] || 0) + 1;
    }
    for (const complaint of lead.relationship.vendor_complaints || []) {
      vendorComplaints[complaint] = (vendorComplaints[complaint] || 0) + 1;
    }
    if (lead.relationship.current_pain) {
      painPoints[lead.relationship.current_pain] = (painPoints[lead.relationship.current_pain] || 0) + 1;
    }
    const seg = lead.business_type || 'unknown';
    if (!segmentNotes[seg]) segmentNotes[seg] = { touched: 0, warm: 0 };
    if (lead.operational_state !== 'not_started') segmentNotes[seg].touched += 1;
    if (lead.warm_score >= 70) segmentNotes[seg].warm += 1;
  }

  return {
    objections: sortCountMap(objections),
    vendor_complaints: sortCountMap(vendorComplaints),
    pain_points: sortCountMap(painPoints),
    segment_notes: segmentNotes,
  };
}

function sortCountMap(map) {
  return Object.entries(map)
    .sort((a, b) => b[1] - a[1])
    .map(([text, count]) => ({ text, count }));
}

function buildRecommendedActions(leads, escalations, campaign) {
  const jakeActions = [];
  const mikeActions = [];

  for (const esc of escalations.filter(e => ['new', 'seen', 'in_progress'].includes(e.status)).slice(0, 5)) {
    jakeActions.push({
      priority: esc.urgency === 'high' ? 1 : 2,
      action: esc.recommended_action,
      business: esc.business_name,
      contact: esc.contact_name,
      reason: esc.reason,
      escalation_id: esc.id,
    });
  }

  for (const lead of leads.filter(l => l.waiting_on_jake && !l.open_escalation_id).slice(0, 3)) {
    jakeActions.push({
      priority: 2,
      action: 'Follow up on AO-flagged lead',
      business: lead.business_name,
      contact: lead.contact_name,
      reason: lead.open_next_action || 'Waiting on Jake',
      lead_id: lead.id,
    });
  }

  const warmSorted = [...leads]
    .filter(l => !['disqualified', 'converted_to_crm', 'not_started'].includes(l.operational_state))
    .sort((a, b) => b.warm_score - a.warm_score);

  for (const lead of warmSorted.filter(l => l.crm_promotion.eligible).slice(0, 3)) {
    jakeActions.push({
      priority: 3,
      action: `Consider CRM promotion — ${lead.crm_promotion.reasons.join(', ')}`,
      business: lead.business_name,
      contact: lead.contact_name,
      lead_id: lead.id,
    });
  }

  const overdue = leads.filter(l => l.open_task_due && l.open_task_due < todayISO());
  if (overdue.length) {
    mikeActions.push({
      priority: 1,
      action: `Clear ${overdue.length} overdue follow-up${overdue.length > 1 ? 's' : ''}`,
      detail: overdue.slice(0, 3).map(l => l.business_name).join(', '),
    });
  }

  const routeRemaining = campaign.remaining_route_queue;
  if (routeRemaining > 0) {
    mikeActions.push({
      priority: 2,
      action: `Continue ${CAMPAIGN_001} route — ${routeRemaining} stop${routeRemaining > 1 ? 's' : ''} remaining`,
    });
  }

  const phoneQueue = leads.filter(l => l.open_next_action === 'phone_follow_up' && l.open_task_due);
  if (phoneQueue.length) {
    mikeActions.push({
      priority: 3,
      action: `Work phone follow-up queue (${phoneQueue.length})`,
      detail: phoneQueue.slice(0, 3).map(l => l.business_name).join(', '),
    });
  }

  return {
    jake: jakeActions.sort((a, b) => a.priority - b.priority),
    mike: mikeActions.sort((a, b) => a.priority - b.priority),
  };
}

async function buildBriefing(clientId, { asOf = todayISO() } = {}) {
  const [today, leads, escalations, campaign] = await Promise.all([
    getTodayActivity(clientId, asOf),
    fetchEnrichedLeads(clientId),
    listEscalationInbox(clientId),
    getCampaign001Progress(clientId),
  ]);

  const campaignLeads = leads.filter(l => l.campaign_name === CAMPAIGN_001);
  const warmOpportunities = [...leads]
    .filter(l => l.warm_score >= 50 && !['disqualified', 'converted_to_crm', 'not_started'].includes(l.operational_state))
    .sort((a, b) => b.warm_score - a.warm_score)
    .slice(0, 8)
    .map(l => ({
      business_name: l.business_name,
      contact_name: l.contact_name,
      contact_title: l.contact_title,
      phone: l.contact_phone,
      warm_reason: describeWarmReason(l),
      next_step: l.open_next_action || recommendNextStep(l),
      operational_state: l.operational_state,
      warm_score: l.warm_score,
      lead_id: l.id,
      price_shopping_risk: l.relationship.price_shopping_risk,
    }));

  const needsJake = escalations
    .filter(e => ['new', 'seen', 'in_progress'].includes(e.status))
    .slice(0, 10);

  const fieldIntel = buildFieldIntelligence(leads);
  const recommendations = buildRecommendedActions(leads, escalations, campaign);
  const digest = buildDailyDigestText({ today, leads, escalations, campaign, warmOpportunities, recommendations, asOf });

  const categorized = {
    walkthrough_requested: leads.filter(l => l.operational_state === 'walkthrough_requested'),
    jake_action_needed: leads.filter(l => l.operational_state === 'jake_action_needed'),
    decision_maker_reached: leads.filter(l => l.operational_state === 'decision_maker_reached'),
    disqualified: leads.filter(l => l.operational_state === 'disqualified'),
    gatekeepers: leads.filter(l => l.operational_state === 'gatekeeper_reached'),
    not_started: leads.filter(l => l.operational_state === 'not_started'),
    crm_candidates: leads.filter(l => l.crm_promotion.eligible),
  };

  return {
    as_of: asOf,
    client_id: clientId,
    today,
    needs_jake: needsJake,
    warm_opportunities: warmOpportunities,
    campaign_001: campaign,
    field_intelligence: fieldIntel,
    recommended_actions: recommendations,
    daily_digest: digest,
    lead_summary: {
      total: leads.length,
      by_operational_state: countBy(leads, 'operational_state'),
    },
    categorized_counts: Object.fromEntries(
      Object.entries(categorized).map(([k, v]) => [k, v.length])
    ),
    promotion_candidates: categorized.crm_candidates.slice(0, 10).map(l => ({
      lead_id: l.id,
      business_name: l.business_name,
      contact_name: l.contact_name,
      reasons: l.crm_promotion.reasons,
      warm_score: l.warm_score,
    })),
    generated_at: new Date().toISOString(),
  };
}

function countBy(items, key) {
  const map = {};
  for (const item of items) {
    const val = item[key];
    map[val] = (map[val] || 0) + 1;
  }
  return map;
}

function describeWarmReason(lead) {
  const parts = [];
  if (lead.operational_state === 'walkthrough_requested') parts.push('Walkthrough requested');
  if (lead.relationship.interest_level === 'high') parts.push('High interest');
  if (lead.relationship.current_pain) parts.push(`Pain: ${lead.relationship.current_pain}`);
  if (lead.open_escalation_id) parts.push('Open escalation');
  if (lead.is_decision_maker) parts.push('Decision-maker reached');
  if (lead.relationship.signal_type === 'real_buying_signal') parts.push('Buying signal');
  if (!parts.length) parts.push('Active follow-up with engagement');
  return parts.join(' · ');
}

function recommendNextStep(lead) {
  if (lead.operational_state === 'walkthrough_requested') return 'Schedule walkthrough';
  if (lead.waiting_on_jake || lead.open_escalation_id) return 'Jake should call';
  if (lead.open_next_action) return lead.open_next_action.replace(/_/g, ' ');
  return 'Review and assign follow-up';
}

function buildDailyDigestText(ctx) {
  const { today, leads, escalations, campaign, warmOpportunities, recommendations, asOf } = ctx;
  const dmCount = leads.filter(l => l.operational_state === 'decision_maker_reached'
    || l.operational_state === 'walkthrough_requested'
    || l.operational_state === 'jake_action_needed').length;
  const jakeFollowUps = escalations.filter(e => e.status === 'new').length;
  const top = warmOpportunities[0];
  const vendorMentions = leads.filter(l => l.relationship.vendor_complaints.length).length;

  const lines = [];
  lines.push(
    `Today Mike logged ${today.visits_today || 0} visit${(today.visits_today || 0) === 1 ? '' : 's'}`
    + (today.calls_today ? ` and ${today.calls_today} phone follow-up${today.calls_today === 1 ? '' : 's'}` : '')
    + `.`
  );
  if (dmCount) lines.push(`Reached ${dmCount} decision-maker${dmCount === 1 ? '' : 's'} or equivalent buying conversations.`);
  if (vendorMentions) lines.push(`${vendorMentions} business${vendorMentions === 1 ? '' : 'es'} mentioned current cleaner issues.`);
  if (jakeFollowUps) lines.push(`${jakeFollowUps} escalation${jakeFollowUps === 1 ? '' : 's'} need Jake's attention.`);
  if (top) {
    let topLine = `Strongest opportunity: ${top.business_name}`;
    if (top.contact_name) topLine += ` (${top.contact_name})`;
    topLine += ` — ${top.warm_reason}.`;
    if (top.price_shopping_risk === 'likely') topLine += ' Risk: possible price shopping.';
    lines.push(topLine);
  }
  if (recommendations.jake[0]) {
    lines.push(`Jake should: ${recommendations.jake[0].action}${recommendations.jake[0].business ? ` — ${recommendations.jake[0].business}` : ''}.`);
  }
  if (recommendations.mike[0]) {
    lines.push(`Mike should: ${recommendations.mike[0].action}.`);
  }
  lines.push(
    `${CAMPAIGN_001}: ${campaign.visited}/${campaign.target_total} touched, `
    + `${campaign.walkthrough_requests} walkthrough request${campaign.walkthrough_requests === 1 ? '' : 's'}, `
    + `${campaign.remaining_route_queue} remaining in queue.`
  );

  return {
    text: lines.join(' '),
    paragraphs: lines,
    as_of: asOf,
  };
}

async function answerAoQuestion(clientId, question) {
  const q = String(question || '').trim().toLowerCase();
  const briefing = await buildBriefing(clientId);
  const leads = await fetchEnrichedLeads(clientId);
  const campaignLeads = leads.filter(l => l.campaign_name === CAMPAIGN_001);

  if (/what happened today|what did mike do|today/.test(q)) {
    return {
      answer: briefing.daily_digest.text,
      sources: ['ao_leads', 'ao_follow_up_tasks', 'ao_escalations'],
    };
  }

  if (/who do i need to call|who should i call|needs my attention|need to follow up/.test(q)) {
    const calls = briefing.needs_jake.slice(0, 5);
    if (!calls.length) {
      return { answer: 'No open escalations need Jake right now. Check warm opportunities for proactive follow-ups.', sources: ['ao_escalations'] };
    }
    const lines = calls.map(e =>
      `${e.business_name}${e.contact_name ? ` — ${e.contact_name}` : ''}: ${e.recommended_action} (${e.urgency} urgency, ${e.reason}).`
    );
    return { answer: lines.join('\n'), sources: ['ao_escalations'] };
  }

  if (/hottest|warmest|best opport/.test(q)) {
    const warm = briefing.warm_opportunities.slice(0, 5);
    if (!warm.length) return { answer: 'No warm AO opportunities yet — Campaign 001 visits may still be in early outreach.', sources: ['ao_leads'] };
    const lines = warm.map(w => `${w.business_name}: ${w.warm_reason}. Next: ${w.next_step}.`);
    return { answer: lines.join('\n'), sources: ['ao_leads'] };
  }

  if (/what did mike learn|field intel|learn/.test(q)) {
    const intel = briefing.field_intelligence;
    const parts = [];
    if (intel.vendor_complaints.length) {
      parts.push(`Vendor complaints: ${intel.vendor_complaints.slice(0, 3).map(v => `"${v.text}" (${v.count}x)`).join('; ')}.`);
    }
    if (intel.pain_points.length) {
      parts.push(`Pain points: ${intel.pain_points.slice(0, 3).map(p => `"${p.text}"`).join('; ')}.`);
    }
    if (intel.objections.length) {
      parts.push(`Objections: ${intel.objections.slice(0, 3).map(o => o.text).join(', ')}.`);
    }
    return {
      answer: parts.length ? parts.join(' ') : 'No strong field patterns yet — Mike may still be early in the route.',
      sources: ['ao_leads.probe_answers'],
    };
  }

  if (/objection/.test(q)) {
    const objs = briefing.field_intelligence.objections;
    if (!objs.length) return { answer: 'No repeated objections logged yet.', sources: ['ao_leads'] };
    return {
      answer: objs.map(o => `${o.text} (${o.count}x)`).join('\n'),
      sources: ['ao_leads'],
    };
  }

  if (/current cleaner|vendor|outside cleaner/.test(q)) {
    const matches = leads.filter(l => l.relationship.current_vendor || l.relationship.vendor_complaints.length);
    if (!matches.length) return { answer: 'No businesses have mentioned a current cleaner in logged visits yet.', sources: ['ao_leads'] };
    const lines = matches.slice(0, 8).map(l => {
      const vendor = l.relationship.current_vendor || 'mentioned issues';
      const complaint = l.relationship.vendor_complaints[0] || '';
      return `${l.business_name}: ${vendor}${complaint ? ` — ${complaint}` : ''}`;
    });
    return { answer: lines.join('\n'), sources: ['ao_leads.probe_answers'] };
  }

  if (/still need a visit|not yet touched|remaining|route/.test(q)) {
    const pending = campaignLeads.filter(l => l.operational_state === 'not_started'
      || l.open_next_action === 'in_person_revisit');
    if (!pending.length) return { answer: 'All Campaign 001 targets have been touched or are in active follow-up.', sources: ['ao_leads'] };
    return {
      answer: `${pending.length} targets still need visits:\n${pending.map(l => l.business_name).join('\n')}`,
      sources: ['ao_leads', 'ao_follow_up_tasks'],
    };
  }

  if (/promot.*crm|crm promot/.test(q)) {
    const candidates = briefing.promotion_candidates;
    if (!candidates.length) return { answer: 'No AO leads meet CRM promotion criteria yet.', sources: ['ao_leads'] };
    const lines = candidates.map(c => `${c.business_name}: ${c.reasons.join(', ')}`);
    return { answer: `CRM promotion candidates:\n${lines.join('\n')}`, sources: ['ao_leads'] };
  }

  if (/mike.*tomorrow|mike should|what should mike/.test(q)) {
    const actions = briefing.recommended_actions.mike;
    if (!actions.length) return { answer: 'Mike should continue Campaign 001 route and clear any due follow-ups.', sources: ['ao_follow_up_tasks'] };
    return {
      answer: actions.map(a => a.action + (a.detail ? ` (${a.detail})` : '')).join('\n'),
      sources: ['ao_follow_up_tasks'],
    };
  }

  if (/campaign 001|campaign progress/.test(q)) {
    const c = briefing.campaign_001;
    return {
      answer: `${CAMPAIGN_001}: ${c.visited}/${c.target_total} touched, ${c.meaningful_conversations} meaningful conversations, `
        + `${c.decision_makers_reached} decision-makers, ${c.escalations} escalations, `
        + `${c.walkthrough_requests} walkthrough requests, ${c.remaining_route_queue} remaining in queue.`,
      sources: ['ao_leads'],
    };
  }

  if (/anchor|attention/.test(q)) {
    return {
      answer: briefing.daily_digest.text,
      sources: ['ao_briefing'],
    };
  }

  return {
    answer: briefing.daily_digest.text,
    sources: ['ao_briefing'],
    note: 'Matched general briefing — try specific questions like "Who do I need to call?" or "Which AO leads are hottest?"',
  };
}

async function promoteLeadToCrm(leadId, clientId, operatorId) {
  const { rows } = await pool.query(`
    SELECT l.*, c.contact_name, c.contact_title, c.phone, c.email, c.is_decision_maker
    FROM ao_leads l
    LEFT JOIN LATERAL (
      SELECT * FROM ao_contacts WHERE lead_id = l.id
      ORDER BY is_decision_maker DESC, created_at ASC LIMIT 1
    ) c ON true
    WHERE l.id = $1 AND l.client_id = $2
    LIMIT 1
  `, [leadId, clientId]);

  const lead = rows[0];
  if (!lead) return { error: 'Lead not found', status: 404 };
  if (lead.status === 'converted_to_crm' || lead.crm_prospect_id) {
    return { error: 'Already promoted', prospect_id: lead.crm_prospect_id, status: 409 };
  }

  const promotion = recommendCrmPromotion(mapLeadRow(lead));
  if (!promotion.eligible) {
    return { error: 'Lead does not meet promotion criteria', reasons: promotion.reasons, status: 400 };
  }

  const nameParts = String(lead.contact_name || 'Unknown Contact').trim().split(/\s+/);
  const firstName = nameParts[0] || 'Unknown';
  const lastName = nameParts.slice(1).join(' ') || 'Contact';

  const companyId = await addCompany({
    name: lead.business_name,
    industry: lead.business_type || 'commercial',
    location: lead.address || 'Manchester NH',
    client_id: clientId,
    icp_score: 75,
  });

  const notes = [
    lead.original_visit_note,
    `AO campaign: ${lead.campaign_name || lead.attribution_source}`,
    `Promoted by operator ${operatorId}`,
  ].filter(Boolean).join(' — ');

  const prospectId = await addProspect({
    company_id: companyId,
    first_name: firstName,
    last_name: lastName,
    email: lead.email || `ao+${lead.id.slice(0, 8)}@placeholder.local`,
    phone: lead.phone || null,
    job_title: lead.contact_title || null,
    decision_maker: Boolean(lead.is_decision_maker),
    source: `ao_field:${lead.attribution_source}`,
    icp_score: 75,
    client_id: clientId,
  });

  if (!prospectId) {
    return { error: 'Prospect creation failed — email may already exist', status: 409 };
  }

  await pool.query(`
    UPDATE prospects
    SET notes = COALESCE(notes, '') || $2,
        vertical = COALESCE(vertical, $3)
    WHERE id = $1 AND client_id = $4
  `, [prospectId, `\n${notes}`, lead.business_type || 'commercial', clientId]);

  await pool.query(`
    UPDATE ao_leads
    SET status = 'converted_to_crm', crm_prospect_id = $2, updated_at = NOW()
    WHERE id = $1
  `, [leadId, prospectId]);

  await pool.query(`
    INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, client_id)
    VALUES ('max', 'ao_crm_promotion', $1, $2, 'success', $3)
  `, [prospectId, JSON.stringify({
    ao_lead_id: leadId,
    business_name: lead.business_name,
    operator_id: operatorId,
    promotion_reasons: promotion.reasons,
  }), clientId]);

  return {
    success: true,
    prospect_id: prospectId,
    company_id: companyId,
    ao_lead_id: leadId,
    reasons: promotion.reasons,
  };
}

async function assignEscalationFollowUp(escalationId, clientId, { dueDate, nextAction, notes } = {}) {
  const { rows } = await pool.query(`
    SELECT e.*, l.business_name, l.ao_owner_id
    FROM ao_escalations e
    JOIN ao_leads l ON l.id = e.lead_id
    WHERE e.id = $1 AND l.client_id = $2
    LIMIT 1
  `, [escalationId, clientId]);
  const esc = rows[0];
  if (!esc) return null;

  const taskDue = dueDate || todayISO();
  const { rows: taskRows } = await pool.query(`
    INSERT INTO ao_follow_up_tasks (
      lead_id, contact_id, ao_owner_id, due_date, priority, next_action,
      last_interaction_summary, waiting_on_jake
    ) VALUES ($1,$2,$3,$4,'high',$5,$6,true)
    RETURNING *
  `, [
    esc.lead_id,
    esc.contact_id,
    esc.ao_owner_id,
    taskDue,
    nextAction || 'Jake follow-up assigned from escalation',
    notes || esc.summary,
  ]);

  await pool.query(`
    UPDATE ao_escalations SET status = 'in_progress' WHERE id = $1
  `, [escalationId]);

  return taskRows[0];
}

module.exports = {
  buildBriefing,
  buildDailyDigestText,
  answerAoQuestion,
  fetchEnrichedLeads,
  getTodayActivity,
  listEscalationInbox,
  getCampaign001Progress,
  promoteLeadToCrm,
  assignEscalationFollowUp,
  CAMPAIGN_001,
};
