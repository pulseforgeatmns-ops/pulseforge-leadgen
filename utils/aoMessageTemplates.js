const TEMPLATES = Object.freeze({
  nice_meeting_you: {
    id: 'nice_meeting_you',
    label: 'Nice meeting you',
    body: 'Hi {{contact_name}}, great meeting you at {{business_name}} today. I\'m {{ao_name}} with Anchor Cleaning — we help offices like yours stay spotless without the hassle. Happy to answer any questions whenever works for you.',
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

function renderTemplate(templateId, vars = {}) {
  const template = TEMPLATES[templateId];
  if (!template) return null;
  return template.body.replace(/\{\{(\w+)\}\}/g, (_, key) => vars[key] || '');
}

function suggestTemplate({ interestLevel, status, nextAction, escalationReason }) {
  if (escalationReason === 'walkthrough_request' || status === 'walkthrough_requested') {
    return TEMPLATES.walkthrough_ask;
  }
  if (status === 'decision_maker_absent') return TEMPLATES.decision_maker_unavailable;
  if (nextAction && /send info|email info/i.test(nextAction)) return TEMPLATES.follow_up_send_info;
  if (nextAction && /revisit|next week/i.test(nextAction)) return TEMPLATES.revisit_next_week;
  if (interestLevel === 'low' || status === 'not_a_fit') return TEMPLATES.not_interested_leave_door_open;
  if (status === 'new_visit') return TEMPLATES.nice_meeting_you;
  return TEMPLATES.checking_who_handles_cleaning;
}

function shouldEscalate({ reason, interestLevel, overdueDays }) {
  if (reason && ESCALATION_REASONS.includes(reason)) return true;
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
  if (/already have|current cleaner|vendor/.test(normalized)) {
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
  if (/email info|send info|brochure/.test(normalized)) {
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

module.exports = {
  TEMPLATES,
  ESCALATION_REASONS,
  renderTemplate,
  suggestTemplate,
  shouldEscalate,
  safeGuidance,
};
