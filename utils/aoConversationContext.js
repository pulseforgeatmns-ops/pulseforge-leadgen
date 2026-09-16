'use strict';

const { classifyAoMaxIntent, extractBriefingTarget } = require('./aoMaxIntent');

const MAX_HISTORY_TURNS = 20;

const WHY_FIRST_PATTERNS = [
  /\bwhy\b.*\bfirst\b/i,
  /\bwhy (?:did you (?:pick|choose|rank)|is)\b/i,
  /\bwhy (?:that|this) one\b/i,
  /\bwhy (?:them|her|him|it)\b/i,
];

const CONTEXT_BRIEFING_PATTERNS = [
  /\bbrief me on (?:them|that one|that account|it|the first(?: one)?)\b/i,
  /\btell me about (?:them|that one|it|the first(?: one)?)\b/i,
  /\bwhat do i know about (?:them|that one|it)\b/i,
];

const CONTEXT_CONTACT_PATTERNS = [
  /\bwho should i ask for\b/i,
  /\bwho (?:is|are) the (?:decision[- ]?maker|contact)\b/i,
  /\bwho do i talk to\b/i,
];

const PRONOUN_ACCOUNT_PATTERNS = [
  /\b(?:them|they|their|that account|this account|the account we(?:'re| are) discussing)\b/i,
];

function normalizeText(text) {
  return String(text || '').trim().replace(/\s+/g, ' ');
}

function trimHistory(messages, limit = MAX_HISTORY_TURNS) {
  if (!Array.isArray(messages)) return [];
  return messages.slice(-limit);
}

function findAccountByName(name, context) {
  const query = normalizeText(name).toLowerCase();
  if (!query) return null;

  const accounts = context?.prioritized_accounts || [];
  const exact = accounts.find(a => String(a.business_name || '').toLowerCase() === query);
  if (exact) return exact;

  const partial = accounts.find(a => String(a.business_name || '').toLowerCase().includes(query));
  if (partial) return partial;

  if (context?.selected_account) {
    const selectedName = String(context.selected_account.business_name || '').toLowerCase();
    if (selectedName.includes(query) || query.includes(selectedName)) {
      return context.selected_account;
    }
  }

  return null;
}

function extractWhyTarget(message, context) {
  const text = normalizeText(message);
  const quoted = text.match(/["']([^"']+)["']/);
  if (quoted?.[1]) return quoted[1];

  const whyMatch = text.match(/\bwhy(?:\s+\w+){0,3}\s+(?:is\s+)?(.+?)\s+first\b/i);
  if (whyMatch?.[1]) {
    return whyMatch[1].replace(/\?+$/, '').trim();
  }

  if (/\b(the )?first one\b/i.test(text) && context?.prioritized_accounts?.[0]) {
    return context.prioritized_accounts[0].business_name;
  }

  if (context?.selected_account?.business_name) {
    return context.selected_account.business_name;
  }

  if (context?.prioritized_accounts?.[0]?.business_name) {
    return context.prioritized_accounts[0].business_name;
  }

  return null;
}

function resolveActiveAccount(message, context) {
  if (context?.selected_account?.business_name) {
    return context.selected_account;
  }

  const briefingTarget = extractBriefingTarget(message);
  if (briefingTarget) {
    const fromList = findAccountByName(briefingTarget, context);
    if (fromList) return fromList;
  }

  const whyTarget = extractWhyTarget(message, context);
  if (whyTarget) {
    const fromWhy = findAccountByName(whyTarget, context);
    if (fromWhy) return fromWhy;
  }

  const text = normalizeText(message);
  if (PRONOUN_ACCOUNT_PATTERNS.test(text) && context?.prioritized_accounts?.[0]) {
    return context.prioritized_accounts[0];
  }

  if (/\b(the )?first one\b/i.test(text) && context?.prioritized_accounts?.[0]) {
    return context.prioritized_accounts[0];
  }

  return null;
}

function resolveConversationIntent(message, context = {}) {
  const text = normalizeText(message);
  if (!text) return null;

  for (const pattern of WHY_FIRST_PATTERNS) {
    if (pattern.test(text)) {
      const targetName = extractWhyTarget(text, context);
      const account = targetName ? findAccountByName(targetName, context) : resolveActiveAccount(text, context);
      return {
        intent: 'why_prioritized',
        account,
        targetName: targetName || account?.business_name || null,
      };
    }
  }

  for (const pattern of CONTEXT_BRIEFING_PATTERNS) {
    if (pattern.test(text)) {
      const account = resolveActiveAccount(text, context);
      if (account?.business_name) {
        return { intent: 'account_briefing', briefingTarget: account.business_name, account };
      }
    }
  }

  for (const pattern of CONTEXT_CONTACT_PATTERNS) {
    if (pattern.test(text) && !extractBriefingTarget(text)) {
      const account = resolveActiveAccount(text, context);
      if (account?.business_name) {
        return { intent: 'account_contacts', briefingTarget: account.business_name, account };
      }
    }
  }

  const classified = classifyAoMaxIntent(text);
  if (classified.intent === 'coaching' && resolveActiveAccount(text, context)) {
    const account = resolveActiveAccount(text, context);
    if (account && PRONOUN_ACCOUNT_PATTERNS.test(text)) {
      return { intent: 'coaching', account, briefingTarget: account.business_name };
    }
  }

  if (classified.intent === 'account_briefing' && classified.briefingTarget) {
    return { intent: 'account_briefing', briefingTarget: classified.briefingTarget };
  }

  if (classified.intent === 'account_prioritization') {
    return { intent: 'account_prioritization' };
  }

  if (classified.intent === 'coaching') {
    const account = resolveActiveAccount(text, context);
    return { intent: 'coaching', account: account || null };
  }

  return { intent: classified.intent, briefingTarget: classified.briefingTarget || null };
}

function buildWhyPrioritizedReply(account, context) {
  if (!account?.business_name) {
    return 'Tell me which account you mean — or ask for your top accounts again.';
  }

  const fromList = (context?.prioritized_accounts || []).find(
    a => String(a.business_name || '').toLowerCase() === String(account.business_name).toLowerCase(),
  );

  const why = fromList?.why_now || account.why_now || 'it is highest on your assigned queue right now.';
  const rank = (context?.prioritized_accounts || []).findIndex(
    a => String(a.business_name || '').toLowerCase() === String(account.business_name).toLowerCase(),
  );

  const rankLine = rank === 0
    ? `${account.business_name} is first on your list`
    : `${account.business_name} is ranked #${rank + 1} on your list`;

  return `${rankLine} because ${why.replace(/\.$/, '')}.`;
}

function buildAccountContactsReply(lead) {
  if (!lead) {
    return 'I could not find that account in your assigned list.';
  }

  const lines = [`Contacts — ${lead.business_name}`, ''];

  if (lead.contact_name) {
    const role = lead.is_decision_maker ? 'decision-maker' : 'contact';
    lines.push(`Primary contact: ${lead.contact_name}${lead.contact_title ? ` (${lead.contact_title})` : ''} · ${role}`);
  } else {
    lines.push('No contact captured yet — ask for the office manager or owner who handles cleaning decisions.');
  }

  if (lead.is_decision_maker) {
    lines.push('Ask for them directly — they are the decision-maker on file.');
  } else if (lead.contact_name) {
    lines.push(`Ask for the cleaning decision-maker; if unavailable, leave a message for ${lead.contact_name}.`);
  }

  if (lead.contact_phone) {
    lines.push(`Phone on file: ${lead.contact_phone}`);
  }

  return lines.join('\n');
}

function mergeConversationContext(payload, turnResult) {
  const next = {
    ...(payload || {}),
    last_intent: turnResult.intent || payload?.last_intent || null,
  };

  if (turnResult.accounts?.length) {
    next.prioritized_accounts = turnResult.accounts.map(a => ({
      business_name: a.business_name,
      lead_id: a.lead_id,
      why_now: a.why_now,
      status_label: a.status_label,
      next_step: a.next_step,
    }));
  }

  if (turnResult.account?.business_name) {
    next.selected_account = {
      business_name: turnResult.account.business_name,
      lead_id: turnResult.account.lead_id || turnResult.account.id || null,
    };
  } else if (turnResult.intent === 'account_briefing' && turnResult.account) {
    next.selected_account = {
      business_name: turnResult.account.business_name,
      lead_id: turnResult.account.lead_id || null,
    };
  }

  return next;
}

module.exports = {
  MAX_HISTORY_TURNS,
  normalizeText,
  trimHistory,
  resolveConversationIntent,
  resolveActiveAccount,
  buildWhyPrioritizedReply,
  buildAccountContactsReply,
  mergeConversationContext,
  findAccountByName,
};
