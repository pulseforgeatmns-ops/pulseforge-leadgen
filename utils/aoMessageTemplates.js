const TEMPLATES = Object.freeze({
  nice_meeting_you: {
    id: 'nice_meeting_you',
    label: 'Nice meeting you',
    body: 'Hi {{contact_name}}, great meeting you at {{business_name}} today. I\'m {{ao_name}} with Anchor Cleaning — we help offices like yours stay spotless without the hassle. Happy to answer any questions whenever works for you.',
  },
  follow_up_after_visit: {
    id: 'follow_up_after_visit',
    label: 'Follow up after visit',
    body: 'Hi {{contact_name}}, thanks for your time at {{business_name}} today. I wanted to follow up on what we discussed and see if a quick next step makes sense.',
  },
  checking_who_handles_cleaning: {
    id: 'checking_who_handles_cleaning',
    label: 'Checking who handles cleaning',
    body: 'Hi {{contact_name}}, I stopped by {{business_name}} earlier. Could you point me to whoever handles your office cleaning? I\'d love to introduce how we support local businesses.',
  },
  walkthrough_ask: {
    id: 'walkthrough_ask',
    label: 'Walkthrough ask',
    body: 'Hi {{contact_name}}, thanks for your time at {{business_name}}. Would a quick walkthrough work so we can see your space and share what a tailored cleaning plan could look like? I can coordinate with our team on timing.',
  },
  follow_up_send_info: {
    id: 'follow_up_send_info',
    label: 'Follow-up after "send info"',
    body: 'Hi {{contact_name}}, following up from my visit to {{business_name}}. As requested, here\'s a quick overview of how we support local offices — happy to answer questions or set up a brief walkthrough when convenient.',
  },
  current_cleaner_follow_up: {
    id: 'current_cleaner_follow_up',
    label: 'Current cleaner follow-up',
    body: 'Hi {{contact_name}}, good speaking with you at {{business_name}}. You mentioned your current cleaning setup — I\'d love to learn what\'s working and what isn\'t, and share how we might help if the timing is right.',
  },
  decision_maker_unavailable: {
    id: 'decision_maker_unavailable',
    label: 'Decision-maker unavailable',
    body: 'Hi {{contact_name}}, I stopped by {{business_name}} today and wanted to connect with whoever manages your office cleaning. When would be a good time to reach the right person?',
  },
  revisit_next_week: {
    id: 'revisit_next_week',
    label: 'Revisit next week',
    body: 'Hi {{contact_name}}, checking back after my visit to {{business_name}}. Would next week work for a quick follow-up? No pressure — just want to stay helpful if timing is better then.',
  },
  not_interested_leave_door_open: {
    id: 'not_interested_leave_door_open',
    label: 'Not interested, leave door open',
    body: 'Hi {{contact_name}}, thanks for being straight with me at {{business_name}}. If anything changes down the road, feel free to reach out — we\'re always here for local businesses.',
  },
  jake_follow_up: {
    id: 'jake_follow_up',
    label: 'Jake follow-up',
    body: 'Hi {{contact_name}}, great connecting at {{business_name}} today. Jake from our team will reach out shortly to answer your questions and talk next steps.',
  },
});

const ESCALATION_REASONS = Object.freeze([
  'pricing_request',
  'walkthrough_request',
  'multiple_properties',
  'current_cleaner_issues',
  'insurance_proof',
  'contract_terms',
  'high_interest',
  'overdue_follow_up',
]);

function sanitizeUserFacingText(text) {
  return String(text || '')
    .replace(/\bpulseforge admin\b/gi, 'Jake')
    .replace(/\badmin should call\b/gi, 'Jake should call')
    .replace(/\bowner should call\b/gi, 'Jake should call')
    .trim();
}

function renderTemplate(templateId, vars = {}) {
  const template = TEMPLATES[templateId];
  if (!template) return null;
  const body = template.body.replace(/\{\{(\w+)\}\}/g, (_, key) => vars[key] || '');
  return sanitizeUserFacingText(body);
}

function parseContactRole(value) {
  const v = String(value || '').trim().toLowerCase();
  if (/decision|owner|manager|dm\b|boss|principal|partner/.test(v) && !/gate|front|reception|assistant/.test(v)) {
    return 'decision_maker';
  }
  if (/gate|front desk|reception|assistant|secretary|not decision/.test(v)) {
    return 'gatekeeper';
  }
  if (/unknown|not sure|unsure|don't know|dont know/.test(v)) {
    return 'unknown';
  }
  return 'unknown';
}

function suggestTemplate({
  interestLevel,
  status,
  nextAction,
  visitNote,
  contactName,
  contactTitle,
  contactRole,
  escalationReason,
}) {
  const note = String(visitNote || '').toLowerCase();
  const next = String(nextAction || '').toLowerCase();
  const blob = `${note} ${next} ${String(contactTitle || '').toLowerCase()}`;

  if (escalationReason === 'walkthrough_request' || status === 'walkthrough_requested' || /walkthrough|tour/.test(next)) {
    return TEMPLATES.walkthrough_ask;
  }
  if (/cleaner|cleaning vendor|current vendor|inconsistent|not happy|unhappy|switch|inconsistent/.test(blob)) {
    return TEMPLATES.current_cleaner_follow_up;
  }
  if (/send info|more info|email info|brochure|overview|information/.test(blob)) {
    return TEMPLATES.follow_up_send_info;
  }
  if (contactRole === 'gatekeeper' || status === 'decision_maker_absent' || /manager wasn't|not there|absent|gatekeeper/.test(blob)) {
    return TEMPLATES.decision_maker_unavailable;
  }
  if (/jake|owner follow|owner call|follow-up needed|call back/.test(next)) {
    return TEMPLATES.jake_follow_up;
  }
  if (/revisit|next week|check back/.test(next)) {
    return TEMPLATES.revisit_next_week;
  }
  if (interestLevel === 'low' || status === 'not_a_fit' || /not interested|no interest/.test(blob)) {
    return TEMPLATES.not_interested_leave_door_open;
  }
  if (contactName && contactRole === 'decision_maker' && interestLevel === 'high') {
    return TEMPLATES.nice_meeting_you;
  }
  if (contactName && contactRole !== 'unknown') {
    return TEMPLATES.follow_up_after_visit;
  }
  if (status === 'new_visit' && contactName) {
    return TEMPLATES.follow_up_after_visit;
  }
  if (!contactName) {
    return TEMPLATES.checking_who_handles_cleaning;
  }
  return TEMPLATES.follow_up_after_visit;
}

function buildSuggestedMessage(ctx = {}) {
  const vars = {
    contact_name: ctx.contactName || 'there',
    business_name: ctx.businessName || 'your business',
    ao_name: ctx.aoName || 'your Anchor rep',
  };
  const template = suggestTemplate(ctx);
  return renderTemplate(template.id, vars);
}

function shouldEscalate({ reason, interestLevel, overdueDays, contactRole }) {
  if (reason && ESCALATION_REASONS.includes(reason)) return true;
  if (interestLevel === 'high' && contactRole === 'decision_maker') return true;
  if (interestLevel === 'high') return true;
  if (overdueDays >= 3) return true;
  return false;
}

function safeGuidance(topic) {
  const normalized = String(topic || '').toLowerCase();
  if (/pric|cost|rate|quote/.test(normalized)) {
    return {
      guidance: 'Do not share pricing. Tell them Jake will follow up with accurate numbers after a quick walkthrough.',
      escalate: true,
      reason: 'pricing_request',
    };
  }
  if (/walkthrough|tour|visit|see the space/.test(normalized)) {
    return {
      guidance: 'Confirm interest, capture best contact/time, and let them know Jake will coordinate the walkthrough.',
      escalate: true,
      reason: 'walkthrough_request',
    };
  }
  if (/already have|current cleaner|vendor|inconsistent/.test(normalized)) {
    return {
      guidance: 'Acknowledge their setup, ask what\'s working well vs. what they wish were better. Do not criticize their vendor.',
      escalate: true,
      reason: 'current_cleaner_issues',
    };
  }
  if (/insur|bond|certificate|coi/.test(normalized)) {
    return {
      guidance: 'Confirm we are fully insured and Jake can provide proof of insurance directly.',
      escalate: true,
      reason: 'insurance_proof',
    };
  }
  if (/contract|term|agreement|cancel/.test(normalized)) {
    return {
      guidance: 'Do not discuss contract terms. Tell them Jake handles agreements and will follow up.',
      escalate: true,
      reason: 'contract_terms',
    };
  }
  if (/manager|owner|decision|not there|absent/.test(normalized)) {
    return {
      guidance: 'Get the decision-maker\'s name, role, and best time to reach them. Leave a friendly note for follow-up.',
      escalate: false,
    };
  }
  if (/email info|send info|brochure|more info/.test(normalized)) {
    return {
      guidance: 'Confirm email, promise a brief overview (no pricing), and schedule a follow-up task for 2 business days.',
      escalate: false,
    };
  }
  if (/multiple|properties|locations|portfolio/.test(normalized)) {
    return {
      guidance: 'Note how many locations and who oversees them. Escalate to Jake — multi-site needs his input.',
      escalate: true,
      reason: 'multiple_properties',
    };
  }
  return {
    guidance: 'Stay curious, listen, and capture what they care about. Offer a walkthrough only if they seem open — never push pricing or contracts.',
    escalate: false,
  };
}

function normalizeNextAction(nextAction) {
  const raw = sanitizeUserFacingText(nextAction);
  if (/pulseforge|admin should|owner follow-up needed/i.test(String(nextAction || ''))) {
    if (/call/.test(raw)) return 'Jake should call';
    return 'Owner follow-up needed';
  }
  return raw || 'Follow up';
}

function buildCompletionReply({ businessName, escalated, suggestedMessage, loggedOnly = false }) {
  const lines = [`Logged ${businessName}.`];
  if (suggestedMessage) {
    lines.push(`\nSuggested follow-up:\n"${suggestedMessage}"`);
  }
  if (escalated) {
    lines.push('\nLogged and escalated. Jake will follow up.');
  } else if (!loggedOnly) {
    lines.push('\nYou\'re all set.');
  }
  lines.push('\nLog another visit or check your queue.');
  return sanitizeUserFacingText(lines.join(''));
}

module.exports = {
  TEMPLATES,
  ESCALATION_REASONS,
  renderTemplate,
  suggestTemplate,
  buildSuggestedMessage,
  sanitizeUserFacingText,
  normalizeNextAction,
  parseContactRole,
  shouldEscalate,
  safeGuidance,
  buildCompletionReply,
};
