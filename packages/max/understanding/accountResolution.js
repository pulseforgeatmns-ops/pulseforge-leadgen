'use strict';

const { AMBIGUITY_KIND } = require('./types');
const { normalizeText } = require('../stateIngestion/claimParser');

const TYPO_ACCOUNT_MAP = Object.freeze({
  'exter philips': 'Exeter Phillips',
  'exter phillips': 'Exeter Phillips',
  'exter philip': 'Exeter Phillips',
});

function normalizeTypoAccount(fragment) {
  const key = normalizeText(fragment).toLowerCase();
  return TYPO_ACCOUNT_MAP[key] || null;
}

function canonicalAccountLabel(name) {
  const typo = normalizeTypoAccount(name);
  if (typo) return typo;
  const n = normalizeText(name);
  if (/^exter\b/i.test(n) && /phil/i.test(n)) return 'Exeter Phillips';
  return n;
}

function collectKnownAccounts(memory, extra = []) {
  const seen = new Map();
  const add = (name) => {
    const label = canonicalAccountLabel(name);
    if (!label || label.length < 3) return;
    const key = label.toLowerCase();
    if (!seen.has(key)) seen.set(key, label);
  };
  for (const name of extra || []) add(name);
  if (memory?.knownAccountNames) {
    for (const name of memory.knownAccountNames()) add(name);
  }
  return [...seen.values()];
}

function accountMatchesReference(reference, accountName) {
  const ref = normalizeText(reference).toLowerCase();
  const acct = normalizeText(accountName).toLowerCase();
  if (!ref || !acct) return false;
  if (ref === acct) return true;
  if (acct.startsWith(`${ref} `)) return true;
  if (acct.includes(ref) && ref.length >= 4) return true;
  const refTokens = ref.split(/\s+/).filter(Boolean);
  const acctTokens = acct.split(/\s+/).filter(Boolean);
  if (refTokens.length === 1 && acctTokens[0] === refTokens[0]) return true;
  return false;
}

function resolveAccountReference({ phrase, memory, contextAccounts = [] }) {
  const known = collectKnownAccounts(memory, contextAccounts);
  if (!phrase || !known.length) {
    return { account: phrase ? canonicalAccountLabel(phrase) : null, ambiguous: false, candidates: [] };
  }
  const matches = known.filter(a => accountMatchesReference(phrase, a));
  if (matches.length === 1) {
    return { account: matches[0], ambiguous: false, candidates: matches };
  }
  if (matches.length > 1) {
    return {
      account: null,
      ambiguous: true,
      candidates: matches,
      ambiguity: {
        kind: AMBIGUITY_KIND.ACCOUNT,
        phrase,
        candidates: matches,
        clarification: `Which account did you mean — ${matches.join(' or ')}?`,
      },
    };
  }
  const canonical = canonicalAccountLabel(phrase);
  return { account: canonical, ambiguous: false, candidates: canonical ? [canonical] : [] };
}

function extractShorthandAccountReference(text) {
  const raw = normalizeText(text);
  const m = raw.match(/^([A-Za-z][A-Za-z0-9&.'\-]{2,24})\s+said\b/i);
  if (m) return m[1];
  const m2 = raw.match(/\b([A-Za-z][A-Za-z0-9&.'\-]{2,24})\s+said\s+call\b/i);
  if (m2) return m2[1];
  return null;
}

module.exports = {
  canonicalAccountLabel,
  normalizeTypoAccount,
  collectKnownAccounts,
  resolveAccountReference,
  extractShorthandAccountReference,
  accountMatchesReference,
};
