'use strict';

const ACCOUNT_PRIORITIZATION_PATTERNS = [
  /\bwhat accounts should i focus on\b/i,
  /\bwhich accounts should i focus on\b/i,
  /\bwhat should i focus on today\b/i,
  /\bwhat are my top accounts\b/i,
  /\bwho should i work first\b/i,
  /\bwhat should i prioritize\b/i,
  /\bwhere should i go today\b/i,
  /\bwho needs attention\b/i,
  /\bwho needs follow[- ]?up\b/i,
  /\bwhat follow[- ]?ups are most important\b/i,
  /\bwhat should i do next\b/i,
  /\bwhat('s| is) on my (queue|plate) today\b/i,
  /\bwho should i visit (first|today)\b/i,
  /\bwhat('s| is) my priority\b/i,
];

const ACCOUNT_BRIEFING_PATTERNS = [
  /\bbrief me on (.+)/i,
  /\bwhat do i know about (.+)/i,
  /\bwho should i ask for at (.+)/i,
  /\bwhat happened last time with (.+)/i,
  /\btell me about (.+)/i,
  /\bgive me (?:a )?briefing on (.+)/i,
  /\bwhat('s| is) the status of (.+)/i,
];

const COACHING_PATTERNS = [
  /\bhow should i (?:approach|handle|deal with|respond to)\b/i,
  /\bhow do i handle\b/i,
  /\bwhat should i say when\b/i,
  /\bhelp me with (?:this )?conversation\b/i,
  /\bhelp me with a gatekeeper\b/i,
  /\bobjection\b/i,
  /\bgatekeeper\b/i,
  /\bprice objection\b/i,
  /\balready have a cleaner\b/i,
  /\bcoaching\b/i,
];

function normalizeIntentText(text) {
  return String(text || '').trim().replace(/\s+/g, ' ');
}

function extractBriefingTarget(message) {
  const text = normalizeIntentText(message);
  for (const pattern of ACCOUNT_BRIEFING_PATTERNS) {
    const match = text.match(pattern);
    if (match?.[1]) {
      return match[1].replace(/[?.!]+$/, '').trim();
    }
  }
  return null;
}

function classifyAoMaxIntent(message) {
  const text = normalizeIntentText(message);
  if (!text) return { intent: 'coaching', briefingTarget: null };

  for (const pattern of ACCOUNT_PRIORITIZATION_PATTERNS) {
    if (pattern.test(text)) {
      return { intent: 'account_prioritization', briefingTarget: null };
    }
  }

  const briefingTarget = extractBriefingTarget(text);
  if (briefingTarget) {
    return { intent: 'account_briefing', briefingTarget };
  }

  for (const pattern of COACHING_PATTERNS) {
    if (pattern.test(text)) {
      return { intent: 'coaching', briefingTarget: null };
    }
  }

  return { intent: 'coaching', briefingTarget: null };
}

module.exports = {
  ACCOUNT_PRIORITIZATION_PATTERNS,
  ACCOUNT_BRIEFING_PATTERNS,
  COACHING_PATTERNS,
  classifyAoMaxIntent,
  extractBriefingTarget,
};
