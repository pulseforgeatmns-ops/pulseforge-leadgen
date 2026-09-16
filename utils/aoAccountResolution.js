'use strict';

const { levenshtein, maxEditDistanceForToken } = require('../packages/max/workspace/BoundedTypoNormalization');

const COMPANY_SUFFIXES = Object.freeze([
  'llc',
  'inc',
  'incorporated',
  'company',
  'companies',
  'properties',
  'property',
  'group',
  'management',
  'corp',
  'corporation',
  'co',
  'ltd',
  'limited',
]);

const PRONOUN_REFERENCE_PATTERNS = [
  /\b(?:them|they|their|it|that one|that account|this account|the account we(?:'re| are) discussing)\b/i,
  /\bthere\b/i,
  /\bthe top account\b/i,
];

const ORDINAL_PATTERNS = [
  { pattern: /\b(?:the )?first(?: one| account)?\b/i, index: 0 },
  { pattern: /\b(?:the )?second(?: one| account)?\b/i, index: 1 },
  { pattern: /\b(?:the )?third(?: one| account)?\b/i, index: 2 },
  { pattern: /\b(?:the )?fourth(?: one| account)?\b/i, index: 3 },
  { pattern: /\b(?:the )?fifth(?: one| account)?\b/i, index: 4 },
];

const TYPO_MIN_QUERY_LENGTH = 4;
const TYPO_MIN_SCORE = 0.72;
const AMBIGUITY_SCORE_DELTA = 0.06;

function normalizeAccountName(name) {
  let text = String(name || '')
    .toLowerCase()
    .trim()
    .replace(/[^\w\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return '';

  let tokens = text.split(/\s+/);
  while (tokens.length > 1 && COMPANY_SUFFIXES.includes(tokens[tokens.length - 1])) {
    tokens = tokens.slice(0, -1);
  }

  return tokens.join(' ');
}

function tokenizeAccountName(name) {
  const normalized = normalizeAccountName(name);
  return normalized ? normalized.split(/\s+/).filter(Boolean) : [];
}

function uniqueAccounts(accounts) {
  const seen = new Set();
  const out = [];

  for (const account of accounts || []) {
    const businessName = String(account?.business_name || '').trim();
    if (!businessName) continue;

    const key = `${account.lead_id || ''}:${businessName.toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      business_name: businessName,
      lead_id: account.lead_id || account.id || null,
      why_now: account.why_now || null,
      status_label: account.status_label || null,
      next_step: account.next_step || null,
    });
  }

  return out;
}

function buildCandidatePool(context = {}, assignedAccounts = []) {
  const prioritized = context?.prioritized_accounts || [];
  const selected = context?.selected_account ? [context.selected_account] : [];
  return uniqueAccounts([...prioritized, ...selected, ...assignedAccounts]);
}

function isContextReference(message) {
  const text = String(message || '').trim();
  if (!text) return false;

  if (ORDINAL_PATTERNS.some(({ pattern }) => pattern.test(text))) {
    return true;
  }

  return PRONOUN_REFERENCE_PATTERNS.some(pattern => pattern.test(text));
}

function resolveOrdinalAccount(message, context) {
  const text = String(message || '');
  for (const { pattern, index } of ORDINAL_PATTERNS) {
    if (pattern.test(text)) {
      return context?.prioritized_accounts?.[index] || null;
    }
  }
  return null;
}

function resolveContextReference(message, context) {
  const ordinal = resolveOrdinalAccount(message, context);
  if (ordinal) return ordinal;

  if (!isContextReference(message)) {
    return null;
  }

  if (context?.selected_account?.business_name) {
    return context.selected_account;
  }
  if (context?.prioritized_accounts?.[0]) {
    return context.prioritized_accounts[0];
  }

  return null;
}

function scoreExactMatch(query, accountName) {
  const normalizedQuery = normalizeAccountName(query);
  const normalizedName = normalizeAccountName(accountName);
  if (!normalizedQuery || !normalizedName) return 0;
  if (normalizedQuery === normalizedName) return 1;
  return 0;
}

function scorePrefixMatch(query, accountName) {
  const normalizedQuery = normalizeAccountName(query);
  const tokens = tokenizeAccountName(accountName);
  if (!normalizedQuery || !tokens.length) return 0;

  for (const token of tokens) {
    if (token === normalizedQuery) return 0.95;
    if (token.startsWith(normalizedQuery) && normalizedQuery.length >= 3) return 0.9;
    if (normalizedQuery.startsWith(token) && token.length >= 3) return 0.85;
  }

  if (normalizeAccountName(accountName).startsWith(normalizedQuery) && normalizedQuery.length >= 3) {
    return 0.88;
  }

  return 0;
}

function scoreTypoMatch(query, accountName) {
  const normalizedQuery = normalizeAccountName(query);
  if (!normalizedQuery || normalizedQuery.length < TYPO_MIN_QUERY_LENGTH) return 0;

  const tokens = tokenizeAccountName(accountName);
  let best = 0;

  for (const token of tokens) {
    if (token.length < 3) continue;
    const maxDistance = maxEditDistanceForToken(normalizedQuery);
    const distance = levenshtein(normalizedQuery, token);
    if (distance > maxDistance) continue;

    const ratio = 1 - (distance / Math.max(normalizedQuery.length, token.length));
    if (ratio > best) best = ratio;
  }

  const normalizedName = normalizeAccountName(accountName);
  if (normalizedName) {
    const maxDistance = maxEditDistanceForToken(normalizedQuery);
    const distance = levenshtein(normalizedQuery, normalizedName);
    if (distance <= maxDistance) {
      const ratio = 1 - (distance / Math.max(normalizedQuery.length, normalizedName.length));
      if (ratio > best) best = ratio;
    }
  }

  return best >= TYPO_MIN_SCORE ? best : 0;
}

function scoreAccountMatch(query, accountName) {
  const exact = scoreExactMatch(query, accountName);
  if (exact) return { score: exact, method: 'exact' };

  const prefix = scorePrefixMatch(query, accountName);
  if (prefix) return { score: prefix, method: 'prefix' };

  const typo = scoreTypoMatch(query, accountName);
  if (typo) return { score: typo, method: 'typo' };

  return { score: 0, method: null };
}

function rankAccountMatches(query, accounts) {
  return accounts
    .map(account => {
      const { score, method } = scoreAccountMatch(query, account.business_name);
      return { account, score, method };
    })
    .filter(entry => entry.score > 0)
    .sort((a, b) => b.score - a.score || a.account.business_name.localeCompare(b.account.business_name));
}

function buildAmbiguityReply(query, candidates) {
  const names = candidates.slice(0, 3).map(entry => entry.account.business_name);
  if (names.length === 2) {
    return `Did you mean ${names[0]} or ${names[1]}?`;
  }
  const last = names.pop();
  return `Did you mean ${names.join(', ')}, or ${last}?`;
}

/**
 * Deterministic account resolution scoped to conversation context and optional assigned accounts.
 * @returns {{ status: 'resolved'|'ambiguous'|'unresolved', account?: object, candidates?: object[], method?: string, query?: string }}
 */
function resolveAccountReference({ query = null, message = '', context = {}, assignedAccounts = [] }) {
  const candidatePool = buildCandidatePool(context, assignedAccounts);

  if (isContextReference(message)) {
    const contextual = resolveContextReference(message, context);
    if (contextual?.business_name) {
      return {
        status: 'resolved',
        account: contextual,
        method: 'context_reference',
        query: query || null,
      };
    }
  }

  const normalizedQuery = normalizeAccountName(query);
  if (!normalizedQuery) {
    return { status: 'unresolved', query: query || null };
  }

  const ranked = rankAccountMatches(normalizedQuery, candidatePool);
  if (!ranked.length) {
    return { status: 'unresolved', query: normalizedQuery };
  }

  const best = ranked[0];
  const runnerUp = ranked[1];

  if (
    runnerUp
    && best.method === 'typo'
    && runnerUp.method === 'typo'
    && (best.score - runnerUp.score) < AMBIGUITY_SCORE_DELTA
  ) {
    return {
      status: 'ambiguous',
      query: normalizedQuery,
      candidates: ranked.slice(0, 3),
    };
  }

  if (
    runnerUp
    && best.score >= TYPO_MIN_SCORE
    && runnerUp.score >= TYPO_MIN_SCORE
    && (best.score - runnerUp.score) < AMBIGUITY_SCORE_DELTA
  ) {
    return {
      status: 'ambiguous',
      query: normalizedQuery,
      candidates: ranked.slice(0, 3),
    };
  }

  return {
    status: 'resolved',
    account: best.account,
    method: best.method,
    query: normalizedQuery,
  };
}

module.exports = {
  normalizeAccountName,
  tokenizeAccountName,
  buildCandidatePool,
  isContextReference,
  resolveContextReference,
  resolveAccountReference,
  rankAccountMatches,
  buildAmbiguityReply,
  scoreAccountMatch,
};
