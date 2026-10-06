'use strict';

const { normalizeText } = require('../stateIngestion/claimParser');

const SPLIT_MARKERS = [
  /\.\s+I also stopped at\s+/i,
  /\.\s+I also visited\s+/i,
  /\.\s+Also stopped at\s+/i,
  /\.\s+Also,\s+/i,
  /\.\s+Then I (?:stopped|went) to\s+/i,
];

const ACCOUNT_PATTERNS = [
  /\bat\s+([A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z][A-Za-z0-9&.'\-]+){0,4})\b/g,
  /\bto\s+([A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z][A-Za-z0-9&.'\-]+){0,4})\b/g,
  /\b(?:stopped at|visited|left)\s+([A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z][A-Za-z0-9&.'\-]+){0,4})\b/gi,
];

const SKIP_NAMES = new Set(['tony', 'rory', 'jake', 'dave', 'sarah', 'lisa', 'mike', 'friday', 'thursday', 'tuesday', 'wednesday']);

function extractAccountNames(text) {
  const raw = normalizeText(text);
  const found = [];
  const seen = new Set();
  for (const pattern of ACCOUNT_PATTERNS) {
    const re = new RegExp(pattern.source, pattern.flags.includes('g') ? pattern.flags : `${pattern.flags}g`);
    let m;
    while ((m = re.exec(raw)) !== null) {
      const name = normalizeText(m[1]);
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

  const threads = [];
  for (let i = 0; i < accounts.length; i += 1) {
    const start = accounts[i].index;
    const end = i + 1 < accounts.length ? accounts[i + 1].index : raw.length;
    const slice = raw.slice(start, end).trim();
    const contextual = i === 0 && start > 40 ? `${raw.slice(0, start).trim()} ${slice}`.trim() : slice;
    threads.push({ text: contextual, accountHint: accounts[i].name });
  }
  return threads;
}

module.exports = {
  segmentIntoThreads,
  extractAccountNames,
};
