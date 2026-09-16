'use strict';

const { classifyAoMaxIntent, extractBriefingTarget } = require('./aoMaxIntent');
const {
  resolveAccountReference,
  buildAmbiguityReply,
  isContextReference,
} = require('./aoAccountResolution');
const { formatAccountContactsReply } = require('./aoAccountBriefing');

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

function findAccountByName(name, context, assignedAccounts = []) {
  const resolution = resolveAccountReference({
    query: name,
    message: '',
    context,
    assignedAccounts,
  });
  if (resolution.status === 'resolved') return resolution.account;
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

  if (isContextReference(text)) {
    const contextual = resolveAccountReference({ message: text, context }).account;
    if (contextual?.business_name) return contextual.business_name;
  }

  if (context?.selected_account?.business_name) {
    return context.selected_account.business_name;
  }

  if (context?.prioritized_accounts?.[0]?.business_name) {
    return context.prioritized_accounts[0].business_name;
  }

  return null;
}

function resolveActiveAccount(message, context, assignedAccounts = []) {
  const text = normalizeText(message);

  if (isContextReference(text)) {
    const contextual = resolveAccountReference({ message: text, context, assignedAccounts });
    if (contextual.status === 'resolved') return contextual.account;
  }

  const briefingTarget = extractBriefingTarget(message);
  if (briefingTarget) {
    const resolution = resolveAccountReference({
      query: briefingTarget,
      message: text,
      context,
      assignedAccounts,
    });
    if (resolution.status === 'resolved') return resolution.account;
  }

  const whyTarget = extractWhyTarget(message, context);
  if (whyTarget) {
    const resolution = resolveAccountReference({
      query: whyTarget,
      message: text,
      context,
      assignedAccounts,
    });
    if (resolution.status === 'resolved') return resolution.account;
  }

  if (PRONOUN_ACCOUNT_PATTERNS.test(text) && context?.prioritized_accounts?.[0]) {
    return context.prioritized_accounts[0];
  }

  if (/\b(the )?first one\b/i.test(text) && context?.prioritized_accounts?.[0]) {
    return context.prioritized_accounts[0];
  }

  return null;
}

function isImplicitContactReference(message, context) {
  const text = normalizeText(message);
  if (!text) return false;
  if (isContextReference(text)) return true;
  if (CONTEXT_CONTACT_PATTERNS.some(pattern => pattern.test(text)) && !extractBriefingTarget(text)) {
    return Boolean(context?.selected_account?.business_name || context?.prioritized_accounts?.[0]);
  }
  return false;
}

function resolveNamedAccountTarget({ message, context, query, assignedAccounts = [] }) {
  if (!query && isImplicitContactReference(message, context)) {
    const contextual = context?.selected_account || context?.prioritized_accounts?.[0] || null;
    if (contextual?.business_name) {
      return {
        account: contextual,
        briefingTarget: contextual.business_name,
        ambiguous: false,
        ambiguityReply: null,
      };
    }
    return null;
  }

  if (!query) {
    return null;
  }

  const resolution = resolveAccountReference({
    query,
    message,
    context,
    assignedAccounts,
  });

  if (resolution.status === 'resolved') {
    return {
      account: resolution.account,
      briefingTarget: resolution.account.business_name,
      ambiguous: false,
      ambiguityReply: null,
    };
  }

  if (resolution.status === 'ambiguous') {
    return {
      account: null,
      briefingTarget: query,
      ambiguous: true,
      ambiguityReply: buildAmbiguityReply(resolution.query, resolution.candidates),
    };
  }

  return {
    account: null,
    briefingTarget: query,
    ambiguous: false,
    ambiguityReply: null,
  };
}

function resolveConversationIntent(message, context = {}, assignedAccounts = []) {
  const text = normalizeText(message);
  if (!text) return null;

  for (const pattern of WHY_FIRST_PATTERNS) {
    if (pattern.test(text)) {
      const targetName = extractWhyTarget(text, context);
      const account = targetName
        ? findAccountByName(targetName, context, assignedAccounts)
        : resolveActiveAccount(text, context, assignedAccounts);
      return {
        intent: 'why_prioritized',
        account,
        targetName: targetName || account?.business_name || null,
      };
    }
  }

  for (const pattern of CONTEXT_BRIEFING_PATTERNS) {
    if (pattern.test(text)) {
      const resolved = resolveNamedAccountTarget({ message: text, context, assignedAccounts });
      if (resolved?.account?.business_name) {
        return {
          intent: 'account_briefing',
          briefingTarget: resolved.briefingTarget,
          account: resolved.account,
        };
      }
    }
  }

  for (const pattern of CONTEXT_CONTACT_PATTERNS) {
    if (pattern.test(text) && !extractBriefingTarget(text)) {
      const resolved = resolveNamedAccountTarget({ message: text, context, assignedAccounts });
      if (resolved?.account?.business_name) {
        return {
          intent: 'account_contacts',
          briefingTarget: resolved.briefingTarget,
          account: resolved.account,
        };
      }
    }
  }

  const classified = classifyAoMaxIntent(text);
  if (classified.intent === 'coaching' && resolveActiveAccount(text, context, assignedAccounts)) {
    const account = resolveActiveAccount(text, context, assignedAccounts);
    if (account && PRONOUN_ACCOUNT_PATTERNS.some(pattern => pattern.test(text))) {
      return { intent: 'coaching', account, briefingTarget: account.business_name };
    }
  }

  if (classified.intent === 'account_briefing' && classified.briefingTarget) {
    const resolved = resolveNamedAccountTarget({
      message: text,
      context,
      query: classified.briefingTarget,
      assignedAccounts,
    });
    return {
      intent: 'account_briefing',
      briefingTarget: resolved.briefingTarget,
      account: resolved.account,
      ambiguous: resolved.ambiguous,
      ambiguityReply: resolved.ambiguityReply,
    };
  }

  if (classified.intent === 'account_prioritization') {
    return { intent: 'account_prioritization' };
  }

  if (classified.intent === 'coaching') {
    const account = resolveActiveAccount(text, context, assignedAccounts);
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
  const sameScore = (context?.prioritized_accounts || []).filter(a => (
    fromList?.rank_score != null
    && a.rank_score != null
    && a.rank_score === fromList.rank_score
  ));

  if (sameScore.length > 1 && rank === 0) {
    return `${account.business_name} is listed first among accounts tied on current operational priority. Ranking uses due date, then name, when scores match.`;
  }

  const rankLine = rank === 0
    ? `${account.business_name} is first on your list`
    : `${account.business_name} is ranked #${rank + 1} on your list`;

  return `${rankLine} because ${why.replace(/\.$/, '')}.`;
}

function buildAccountContactsReply(lead) {
  return formatAccountContactsReply(lead);
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
      rank_score: a.rank_score,
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
  resolveNamedAccountTarget,
  buildWhyPrioritizedReply,
  buildAccountContactsReply,
  mergeConversationContext,
  findAccountByName,
};
