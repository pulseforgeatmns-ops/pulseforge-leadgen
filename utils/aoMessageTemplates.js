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

const OWNER_ESCALATION_PATTERN = /\b(jake|admin|owner|pulseforge|anchor)\b.*\b(call|follow|reach|contact|handle|take over|take it)\b|\b(needs jake|needs owner|escalate|jake should|admin should|owner should|someone from anchor)\b/i;

const AO_OWNED_FOLLOWUP_PATTERN = /\b(i should|i'll|ill |i will|ao should|rep should)\b.*\b(follow up|follow-up|call|stop back|check back|revisit)\b/i;

const WEAK_VISIT_NOTE_RE = /^(i should follow up|jake should follow up|jake should call|admin should call|owner should follow up|follow up later|call back|send info|they need a call|need to revisit|follow up|follow-up|check back|need a call|revisit later|call them back|need to follow up)[.!?\s]*$/i;

const VISIT_NOTE_CLARIFY_QUESTION = 'What did they actually say, or what did you learn on the visit?';

function isWeakVisitNote(text) {
  const v = String(text || '').trim();
  if (!v) return true;
  if (WEAK_VISIT_NOTE_RE.test(v)) return true;
  const words = v.split(/\s+/);
  if (words.length <= 4
    && /^(follow|call|send|revisit|check|escalate|jake|admin|info)/i.test(v)
    && !/\b(said|mentioned|told|because|unhappy|interested|cleaner|manager|owner|frustrated|walkthrough)\b/i.test(v)) {
    return true;
  }
  return false;
}

function needsVisitNoteClarification(payload, message) {
  return !payload._visit_note_clarified && isWeakVisitNote(message);
}

function isAoOwnedFollowUp(nextAction) {
  return AO_OWNED_FOLLOWUP_PATTERN.test(String(nextAction || ''));
}

function formatDecisionMakerStatus({ contactRole, isDecisionMaker, contactTitle } = {}) {
  const role = contactRole || (isDecisionMaker ? 'decision_maker' : null);
  if (role === 'decision_maker' || isDecisionMaker) return 'Decision-maker';
  if (role === 'gatekeeper') return 'Gatekeeper';
  if (/gate|front desk|reception|assistant|secretary/i.test(String(contactTitle || ''))) return 'Gatekeeper';
  return 'Unknown';
}

function normalizeNextAction(nextAction) {
  const raw = sanitizeUserFacingText(nextAction);
  const original = String(nextAction || '');
  if (OWNER_ESCALATION_PATTERN.test(original) || /pulseforge|admin should|owner follow-up needed/i.test(original)) {
    if (/call/.test(raw)) return 'Jake should call';
    if (/walkthrough|tour/.test(raw)) return 'Book walkthrough';
    return 'Jake should follow up';
  }
  return raw || 'Follow up';
}

function parseInterestLevel(value) {
  const v = String(value || '').trim().toLowerCase();
  if (v.startsWith('h')) return 'high';
  if (v.startsWith('l')) return 'low';
  return 'medium';
}

function isOwnerEscalation(nextAction) {
  const text = String(nextAction || '');
  return OWNER_ESCALATION_PATTERN.test(text)
    || /\bpulseforge admin\b/i.test(text)
    || /\bowner follow[- ]?up needed\b/i.test(text);
}

function resolveNextActionOwner(nextAction, payload = {}) {
  const next = String(nextAction || '').toLowerCase();
  const visitNote = String(payload.visit_note || '').toLowerCase();
  const interest = parseInterestLevel(payload.interest_level);

  if (/not a fit|no fit|do not contact|hard no|wrong fit|mark.*not a fit/.test(next)
    || (interest === 'low' && /not a fit|hard no|do not contact|wrong fit/.test(visitNote))) {
    return 'not_a_fit';
  }
  if (/no follow[- ]?up|nothing needed|none needed|all set|no action|n\/a\b|nothing else/.test(next)) {
    return 'none';
  }
  if (/walkthrough|tour|site visit|see the space|book a walk/.test(next)
    || /walkthrough|tour/.test(visitNote)) {
    return 'walkthrough';
  }
  if (isOwnerEscalation(nextAction)) {
    return 'jake';
  }
  if (isAoOwnedFollowUp(nextAction)) {
    return 'ao';
  }
  return 'ao';
}

function selectVisitProbes(payload = {}, { phase = 'after_visit_note', maxTotal = 2, existingKeys = [] } = {}) {
  const visitNote = String(payload.visit_note || '').toLowerCase();
  const interest = parseInterestLevel(payload.interest_level);
  const contactRole = parseContactRole(payload.contact_role);
  const candidates = [];

  if (contactRole === 'gatekeeper') {
    candidates.push({ key: 'probe_gatekeeper_dm', question: 'Did they mention who actually handles cleaning decisions?', priority: 10 });
    candidates.push({ key: 'probe_gatekeeper_availability', question: 'Did you learn when that person is usually available?', priority: 9 });
    candidates.push({ key: 'probe_gatekeeper_handoff', question: 'Did you ask the best way to get info to the decision-maker?', priority: 8 });
  }

  if (/inconsistent|unhappy|not happy|missed visit|poor quality|current cleaner|current vendor|vendor issue/.test(visitNote)) {
    candidates.push({ key: 'probe_cleaner_type', question: 'What kind of inconsistency — missed visits, quality, communication, timing, or something else?', priority: 10 });
    candidates.push({ key: 'probe_cleaner_switch', question: 'Did they seem frustrated enough to consider switching, or just mildly annoyed?', priority: 9 });
  }

  if (/send info|more info|brochure|email info|overview|information/.test(visitNote)) {
    candidates.push({ key: 'probe_info_requested', question: 'What info did they ask for specifically?', priority: 10 });
    candidates.push({ key: 'probe_info_channel', question: 'Did they want Jake to call, email, or stop by?', priority: 8 });
  }

  if (phase === 'after_interest_level' && payload.interest_level) {
    if (interest === 'medium') {
      candidates.push({ key: 'probe_medium_why', question: 'What made it medium instead of high?', priority: 10 });
      candidates.push({ key: 'probe_medium_walkthrough', question: 'What would need to happen to turn this into a walkthrough?', priority: 9 });
    }
    if (interest === 'low') {
      candidates.push({ key: 'probe_low_reason', question: 'Was it a hard no, bad timing, or just not the right person?', priority: 10 });
      candidates.push({ key: 'probe_low_revisit', question: 'Should we revisit later or mark this as not a fit?', priority: 9 });
    }
  }

  if (phase === 'after_interest_level' && isOwnerEscalation(payload.next_action)) {
    candidates.push({ key: 'probe_jake_context', question: 'What should Jake know before reaching out?', priority: 11 });
    candidates.push({ key: 'probe_jake_avoid', question: 'Is there anything Jake should avoid saying?', priority: 8 });
    candidates.push({ key: 'probe_jake_channel', question: 'Is a call, email, or in-person revisit best?', priority: 7 });
  }

  if (/walkthrough|tour/.test(visitNote) || resolveNextActionOwner(payload.next_action, payload) === 'walkthrough') {
    candidates.push({ key: 'probe_walkthrough_timing', question: 'Did they mention preferred days or times?', priority: 11 });
    candidates.push({ key: 'probe_walkthrough_attendees', question: 'Who needs to be present for the walkthrough?', priority: 9 });
    candidates.push({ key: 'probe_walkthrough_areas', question: 'Are there any specific problem areas Jake should ask to see?', priority: 8 });
  }

  const remaining = Math.max(0, maxTotal - existingKeys.length);
  return candidates
    .filter(c => !existingKeys.includes(c.key))
    .sort((a, b) => b.priority - a.priority)
    .slice(0, remaining)
    .map(({ key, question }) => ({ key, question }));
}

function formatProbeAnswers(probeAnswers = {}) {
  return Object.entries(probeAnswers)
    .filter(([, answer]) => String(answer || '').trim())
    .map(([key, answer]) => {
      const label = key.replace(/^probe_/, '').replace(/_/g, ' ');
      return `${label}: ${answer}`;
    })
    .join('\n');
}

function resolveCompletionType({ nextActionOwner, escalated, status }) {
  if (nextActionOwner === 'not_a_fit' || status === 'not_a_fit') return 'not_a_fit';
  if (nextActionOwner === 'none') return 'no_follow_up';
  if (nextActionOwner === 'walkthrough' || status === 'walkthrough_requested') return 'walkthrough';
  if (nextActionOwner === 'jake' || escalated) return 'jake_escalation';
  return 'ao_follow_up';
}

function buildCompletionReply({
  businessName,
  completionType,
  suggestedMessage,
  preferredTiming,
  // legacy params
  escalated,
  loggedOnly = false,
}) {
  const type = completionType || (escalated ? 'jake_escalation' : 'ao_follow_up');
  const name = businessName || 'that visit';

  switch (type) {
    case 'ao_follow_up':
      return sanitizeUserFacingText([
        `Got it — logged ${name}. Here's a suggested follow-up you can send:`,
        suggestedMessage ? `"${suggestedMessage}"` : null,
        suggestedMessage ? 'I added it to your queue.' : 'I added it to your queue.',
        '\nLog another visit or check your queue.',
      ].filter(Boolean).join('\n'));

    case 'jake_escalation':
      return sanitizeUserFacingText([
        `Got it — logged ${name} and escalated it to Jake. Jake will follow up. You're all set.`,
        '\nLog another visit or check your queue.',
      ].join(''));

    case 'walkthrough': {
      const lines = [
        `Got it — logged ${name} and escalated the walkthrough request to Jake. Jake will handle next steps.`,
      ];
      if (preferredTiming) lines.push(`Preferred timing: ${preferredTiming}.`);
      lines.push('\nLog another visit or check your queue.');
      return sanitizeUserFacingText(lines.join('\n'));
    }

    case 'no_follow_up':
      return sanitizeUserFacingText([
        `Got it — logged ${name}. No follow-up needed.`,
        '\nLog another visit or check your queue.',
      ].join(''));

    case 'not_a_fit':
      return sanitizeUserFacingText([
        `Got it — logged ${name} as not a fit.`,
        '\nLog another visit or check your queue.',
      ].join(''));

    default:
      if (loggedOnly) return sanitizeUserFacingText(`Got it — logged ${name}.`);
      return sanitizeUserFacingText(`Got it — logged ${name}.\n\nLog another visit or check your queue.`);
  }
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
  parseInterestLevel,
  isOwnerEscalation,
  isAoOwnedFollowUp,
  isWeakVisitNote,
  needsVisitNoteClarification,
  VISIT_NOTE_CLARIFY_QUESTION,
  formatDecisionMakerStatus,
  resolveNextActionOwner,
  selectVisitProbes,
  formatProbeAnswers,
  resolveCompletionType,
  shouldEscalate,
  safeGuidance,
  buildCompletionReply,
};
