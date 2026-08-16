'use strict';

/**
 * Maps AO lead/task/escalation data to operator-facing operational states.
 * Computed at read time — no duplicate state column on ao_leads.
 */

const OPERATIONAL_STATES = Object.freeze([
  'not_started',
  'visited',
  'no_contact',
  'gatekeeper_reached',
  'contact_identified',
  'decision_maker_reached',
  'follow_up_needed',
  'jake_action_needed',
  'walkthrough_requested',
  'disqualified',
  'converted_to_crm',
]);

const STATE_PRIORITY = Object.freeze({
  walkthrough_requested: 1,
  jake_action_needed: 2,
  decision_maker_reached: 3,
  follow_up_needed: 4,
  contact_identified: 5,
  gatekeeper_reached: 6,
  visited: 7,
  no_contact: 8,
  not_started: 9,
  disqualified: 10,
  converted_to_crm: 11,
});

const OUTCOME_LABELS = Object.freeze({
  not_started: 'Not started',
  attempted: 'Attempted',
  visited: 'Visited',
  no_one_available: 'No one available',
  gatekeeper_reached: 'Gatekeeper reached',
  decision_maker_reached: 'Decision-maker reached',
  follow_up_needed: 'Follow-up needed',
  call_needed: 'Call needed',
  walkthrough_requested: 'Walkthrough requested',
  not_interested: 'Not interested',
  bad_fit: 'Bad fit',
  converted_to_crm: 'Converted to CRM',
});

function parseProbeAnswers(raw) {
  if (!raw) return {};
  if (typeof raw === 'object') return raw;
  try {
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

function inferContactRole(row) {
  if (row.is_decision_maker) return 'decision_maker';
  const probes = parseProbeAnswers(row.probe_answers);
  const role = String(probes.contact_role || probes.reached_decision_maker || '').toLowerCase();
  if (/decision|owner|principal|partner|president|manager who decides/i.test(role)) return 'decision_maker';
  if (/gatekeeper|front desk|reception|assistant|not the decision/i.test(role)) return 'gatekeeper';
  if (row.contact_name) return 'contact_identified';
  return 'unknown';
}

function inferPriceShoppingRisk(row) {
  const text = [
    row.original_visit_note,
    row.last_interaction_summary,
    JSON.stringify(parseProbeAnswers(row.probe_answers)),
  ].filter(Boolean).join(' ').toLowerCase();
  if (/price shop|shopping|another quote|compare|cheaper|lowest bid|already have a cleaner.*quote/i.test(text)) {
    return 'likely';
  }
  if (/have a cleaner|current vendor|existing provider/i.test(text) && /quote|compare|bid/i.test(text)) {
    return 'possible';
  }
  return 'none';
}

function inferSignalType(row) {
  const state = deriveOperationalState(row);
  const interest = String(row.interest_level || 'medium').toLowerCase();
  if (state === 'disqualified') return 'bad_fit';
  if (state === 'walkthrough_requested' || /quote|walkthrough/i.test(String(row.open_next_action || ''))) {
    return 'real_buying_signal';
  }
  if (state === 'jake_action_needed') return 'real_buying_signal';
  if (state === 'decision_maker_reached' && interest === 'high') return 'real_buying_signal';
  if (state === 'gatekeeper_reached') return 'gatekeeper_conversation';
  if (/just send info|send info|email me/i.test(String(row.original_visit_note || ''))) return 'just_send_info';
  if (inferPriceShoppingRisk(row) !== 'none') return 'price_shopping';
  if (interest === 'low') return 'curiosity';
  if (state === 'decision_maker_reached') return 'real_buying_signal';
  return 'curiosity';
}

function isDirectMailSeedOnly(row) {
  const note = String(row.original_visit_note || '').trim();
  return /received direct mail before ao visit/i.test(note)
    && !row.last_interaction_summary
    && row.open_task_status === 'open'
    && row.attribution_source === 'direct_mail_campaign';
}

function deriveOperationalState(row) {
  const status = String(row.status || 'new_visit');
  const probes = parseProbeAnswers(row.probe_answers);
  const role = inferContactRole(row);
  const hasOpenEscalation = Boolean(row.open_escalation_id)
    && !['resolved', 'ignored'].includes(String(row.open_escalation_status || ''));
  const waitingOnJake = Boolean(row.waiting_on_jake) || hasOpenEscalation;

  if (status === 'converted_to_crm' || row.crm_prospect_id) return 'converted_to_crm';
  if (['not_a_fit', 'do_not_contact', 'closed_lost'].includes(status)) return 'disqualified';
  if (status === 'closed_won') return 'converted_to_crm';
  if (['walkthrough_requested', 'walkthrough_booked'].includes(status)) return 'walkthrough_requested';
  if (waitingOnJake || status === 'proposal_needed') return 'jake_action_needed';
  if (role === 'decision_maker' || probes.reached_decision_maker === 'yes') return 'decision_maker_reached';
  if (role === 'gatekeeper') return 'gatekeeper_reached';
  if (row.contact_name && !row.is_decision_maker) return 'contact_identified';
  if (status === 'decision_maker_absent') return 'no_contact';
  if (isDirectMailSeedOnly(row)) return 'not_started';
  if (status === 'needs_follow_up' || status === 'walkthrough_completed') return 'follow_up_needed';
  if (status === 'new_visit') {
    const note = String(row.original_visit_note || '').trim();
    if (!note && row.open_task_status === 'open' && row.attribution_source === 'direct_mail_campaign') {
      return 'not_started';
    }
    return 'visited';
  }
  return 'visited';
}

function deriveCampaignOutcome(row) {
  const state = deriveOperationalState(row);
  if (state === 'not_started') return 'not_started';
  if (state === 'no_contact') return 'no_one_available';
  if (state === 'gatekeeper_reached') return 'gatekeeper_reached';
  if (state === 'decision_maker_reached') return 'decision_maker_reached';
  if (state === 'walkthrough_requested') return 'walkthrough_requested';
  if (state === 'jake_action_needed') return 'call_needed';
  if (state === 'follow_up_needed') return 'follow_up_needed';
  if (state === 'disqualified') {
    return /not interested|hard no|no interest/i.test(String(row.original_visit_note || ''))
      ? 'not_interested'
      : 'bad_fit';
  }
  if (state === 'converted_to_crm') return 'converted_to_crm';
  if (state === 'contact_identified' || state === 'visited') return 'visited';
  if (row.open_next_action === 'phone_follow_up') return 'call_needed';
  return 'attempted';
}

function buildRelationshipIntel(row) {
  const probes = parseProbeAnswers(row.probe_answers);
  const role = inferContactRole(row);
  return {
    decision_maker_name: row.is_decision_maker ? row.contact_name : (probes.cleaning_decision_maker || null),
    decision_maker_title: row.is_decision_maker ? row.contact_title : null,
    contact_name: row.contact_name || null,
    contact_title: row.contact_title || null,
    contact_role: role,
    current_vendor: probes.outside_cleaner || probes.current_cleaner || null,
    current_pain: probes.cleaner_issues || null,
    urgency_timing: row.next_follow_up_date || null,
    price_shopping_risk: inferPriceShoppingRisk(row),
    interest_level: row.interest_level || 'medium',
    next_promised_action: row.open_next_action || row.next_action || null,
    latest_ao_note: row.last_interaction_summary || row.original_visit_note || null,
    signal_type: inferSignalType(row),
    objections: extractObjections(row),
    vendor_complaints: extractVendorComplaints(row),
  };
}

function extractObjections(row) {
  const text = [
    row.original_visit_note,
    row.last_interaction_summary,
    parseProbeAnswers(row.probe_answers).cleaner_issues,
  ].filter(Boolean).join(' ');
  const hits = [];
  for (const pattern of [
    /too expensive/i,
    /happy with current/i,
    /under contract/i,
    /not the right time/i,
    /no budget/i,
    /already have a cleaner/i,
    /send info/i,
  ]) {
    const match = text.match(pattern);
    if (match) hits.push(match[0]);
  }
  return [...new Set(hits)];
}

function extractVendorComplaints(row) {
  const probes = parseProbeAnswers(row.probe_answers);
  const issues = probes.cleaner_issues || '';
  if (!issues || /none|no issues|n\/a|nothing/i.test(String(issues))) return [];
  return [String(issues).trim()];
}

function compareOperationalPriority(a, b) {
  const stateA = typeof a === 'string' ? a : deriveOperationalState(a);
  const stateB = typeof b === 'string' ? b : deriveOperationalState(b);
  const priA = STATE_PRIORITY[stateA] || 99;
  const priB = STATE_PRIORITY[stateB] || 99;
  if (priA !== priB) return priA - priB;
  const warmA = String(a.interest_level || '').toLowerCase() === 'high' ? 0 : 1;
  const warmB = String(b.interest_level || '').toLowerCase() === 'high' ? 0 : 1;
  return warmA - warmB;
}

function recommendCrmPromotion(row) {
  const state = deriveOperationalState(row);
  const intel = buildRelationshipIntel(row);
  const reasons = [];
  if (state === 'walkthrough_requested') reasons.push('Walkthrough requested');
  if (/quote/i.test(String(row.open_next_action || row.original_visit_note || ''))) reasons.push('Quote requested');
  if (state === 'jake_action_needed') reasons.push('Jake follow-up required');
  if (state === 'decision_maker_reached' && ['high', 'medium'].includes(String(row.interest_level))) {
    reasons.push('Decision-maker identified and interested');
  }
  if (intel.current_pain && intel.interest_level !== 'low') reasons.push('Clear cleaning pain identified');
  if (intel.signal_type === 'real_buying_signal') reasons.push('Meaningful buying signal');
  if (state === 'converted_to_crm') return { eligible: false, reasons: ['Already converted'] };
  if (state === 'disqualified' || state === 'not_started') return { eligible: false, reasons: [] };
  return { eligible: reasons.length > 0, reasons };
}

function mapEscalationUrgency(row) {
  const reason = String(row.reason || '').toLowerCase();
  if (/walkthrough|quote|pricing|high_interest|jake should/i.test(reason)) return 'high';
  if (/negative|risk|overdue/i.test(reason)) return 'high';
  if (row.status === 'new') return 'medium';
  return 'low';
}

function recommendEscalationAction(row) {
  const reason = String(row.reason || '').toLowerCase();
  if (/walkthrough|tour/i.test(reason)) return 'Schedule walkthrough with decision-maker';
  if (/quote|pricing/i.test(reason)) return 'Call to discuss quote and scope';
  if (/jake should call|high_interest/i.test(reason)) return 'Jake should call within 24 hours';
  if (/negative|risk/i.test(reason)) return 'Review risk and decide whether to pursue';
  if (/overdue/i.test(reason)) return 'Assign follow-up and confirm next step';
  return 'Review visit summary and assign next action';
}

module.exports = {
  OPERATIONAL_STATES,
  STATE_PRIORITY,
  OUTCOME_LABELS,
  parseProbeAnswers,
  inferContactRole,
  inferPriceShoppingRisk,
  inferSignalType,
  deriveOperationalState,
  deriveCampaignOutcome,
  buildRelationshipIntel,
  extractObjections,
  extractVendorComplaints,
  compareOperationalPriority,
  recommendCrmPromotion,
  mapEscalationUrgency,
  recommendEscalationAction,
};
