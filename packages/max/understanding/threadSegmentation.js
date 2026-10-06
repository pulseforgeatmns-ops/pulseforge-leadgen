'use strict';

const { normalizeText } = require('../stateIngestion/claimParser');
const { canonicalAccountLabel } = require('./accountResolution');

const SPLIT_MARKERS = [
  /\.\s+I also stopped at\s+/i,
  /\.\s+I also visited\s+/i,
  /\.\s+Also stopped at\s+/i,
  /\.\s+Also,\s+/i,
  /\.\s+Then I (?:stopped|went) to\s+/i,
  /,\s+then stopped at\s+/i,
  /\.\s+then stopped at\s+/i,
];

const ACCOUNT_WORD = '[A-Z][A-Za-z0-9&\'\\-]+(?:\\s+[A-Z][A-Za-z0-9&\'\\-]+){0,4}';

const ACCOUNT_PATTERNS = [
  new RegExp(`\\bat\\s+(${ACCOUNT_WORD})\\b`, 'g'),
  new RegExp(`\\b(?:stopped at|visited|left)\\s+(${ACCOUNT_WORD})\\b`, 'gi'),
];

const CONTACT_INTRO_RE = /\b(?:talked|spoke|spoken|chat(?:ted)?|met|caught)\s+(?:to|with)\s+/i;

const SKIP_NAMES = new Set([
  'tony', 'rory', 'jake', 'dave', 'sarah', 'lisa', 'mike', 'billy',
  'friday', 'thursday', 'tuesday', 'wednesday', 'monday', 'saturday', 'sunday',
]);

function sanitizeAccountFragment(fragment) {
  const trimmed = normalizeText(fragment).split(/[.!?;,]/)[0].trim();
  return canonicalAccountLabel(trimmed);
}

function extractAccountNames(text) {
  const raw = normalizeText(text);
  const found = [];
  const seen = new Set();
  for (const pattern of ACCOUNT_PATTERNS) {
    const re = new RegExp(pattern.source, pattern.flags.includes('g') ? pattern.flags : `${pattern.flags}g`);
    let m;
    while ((m = re.exec(raw)) !== null) {
      const name = sanitizeAccountFragment(m[1]);
      if (name.length < 4 || SKIP_NAMES.has(name.toLowerCase())) continue;
      const key = name.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      found.push({ name, index: m.index });
    }
  }
  found.sort((a, b) => a.index - b.index);
  return found;
}

function accountsReferToSameEntity(a, b) {
  if (!a || !b) return false;
  return sanitizeAccountFragment(a).toLowerCase() === sanitizeAccountFragment(b).toLowerCase();
}

function mergeThreadInterpretations(threads) {
  if (!threads?.length) return threads || [];
  const merged = [];
  for (const thread of threads) {
    const accountKey = thread.accountName ? sanitizeAccountFragment(thread.accountName).toLowerCase() : null;
    const prior = accountKey
      ? merged.find(t => accountsReferToSameEntity(t.accountName, thread.accountName))
      : null;
    if (!prior) {
      merged.push({ ...thread });
      continue;
    }
    const listFields = [
      'entities', 'events', 'claims', 'ingestionClaims', 'painPoints', 'objections',
      'commitments', 'decisionMakerSignals', 'temporalReferences', 'questions',
      'requestedActions', 'commentary', 'corrections', 'ambiguities', 'evidence',
    ];
    for (const field of listFields) {
      prior[field] = [...(prior[field] || []), ...(thread[field] || [])];
    }
    if (!prior.text?.includes(thread.text)) {
      prior.text = `${prior.text || ''} ${thread.text || ''}`.trim();
    }
  }
  return merged;
}

function segmentIntoThreads(text) {
  const raw = String(text || '').trim();
  if (!raw) return [{ text: raw, accountHint: null }];

  for (const marker of SPLIT_MARKERS) {
    const match = raw.match(marker);
    if (match && match.index != null) {
      const splitAt = match.index + 1;
      const first = raw.slice(0, splitAt).trim();
      const rest = raw.slice(splitAt).replace(/^I also stopped at\s+/i, '').replace(/^I also visited\s+/i, '').trim();
      const threads = [];
      if (first) threads.push({ text: first, accountHint: null });
      if (rest) threads.push({ text: rest, accountHint: null });
      return threads.length > 1 ? threads : [{ text: raw, accountHint: null }];
    }
  }

  const accounts = extractAccountNames(raw);
  if (accounts.length <= 1) {
    return [{ text: raw, accountHint: accounts[0]?.name || null }];
  }

  const distinct = [];
  for (const acct of accounts) {
    const last = distinct[distinct.length - 1];
    if (last && accountsReferToSameEntity(last.name, acct.name)) continue;
    distinct.push(acct);
  }
  if (distinct.length <= 1) {
    return [{ text: raw, accountHint: distinct[0]?.name || null }];
  }

  const threads = [];
  for (let i = 0; i < distinct.length; i += 1) {
    const start = distinct[i].index;
    const end = i + 1 < distinct.length ? distinct[i + 1].index : raw.length;
    const slice = raw.slice(start, end).trim();
    const contextual = i === 0 && start > 40 ? `${raw.slice(0, start).trim()} ${slice}`.trim() : slice;
    threads.push({ text: contextual, accountHint: distinct[i].name });
  }
  return threads;
}

module.exports = {
  segmentIntoThreads,
  extractAccountNames,
  sanitizeAccountFragment,
  mergeThreadInterpretations,
  accountsReferToSameEntity,
  CONTACT_INTRO_RE,
};
