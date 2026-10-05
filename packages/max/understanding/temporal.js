'use strict';

const { EPISTEMIC_CATEGORY } = require('./types');

const WEEKDAY = Object.freeze({
  sunday: 0,
  monday: 1,
  tuesday: 2,
  wednesday: 3,
  thursday: 4,
  friday: 5,
  saturday: 6,
});

function normalizeTemporalPhrase(raw, now = new Date()) {
  const phrase = String(raw || '').trim().toLowerCase();
  if (!phrase) {
    return { original: raw, normalized: null, epistemic: EPISTEMIC_CATEGORY.UNCERTAIN, ambiguous: true };
  }

  const startOfDay = d => new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const addDays = (d, n) => {
    const x = new Date(d);
    x.setUTCDate(x.getUTCDate() + n);
    return x;
  };

  const today = startOfDay(now);

  if (phrase === 'today') {
    return {
      original: raw,
      normalized: { kind: 'day', iso: today.toISOString().slice(0, 10) },
      epistemic: EPISTEMIC_CATEGORY.CONFIRMED,
      ambiguous: false,
    };
  }
  if (phrase === 'yesterday') {
    const d = addDays(today, -1);
    return {
      original: raw,
      normalized: { kind: 'day', iso: d.toISOString().slice(0, 10) },
      epistemic: EPISTEMIC_CATEGORY.CONFIRMED,
      ambiguous: false,
    };
  }
  if (phrase === 'tomorrow') {
    const d = addDays(today, 1);
    return {
      original: raw,
      normalized: { kind: 'day', iso: d.toISOString().slice(0, 10) },
      epistemic: EPISTEMIC_CATEGORY.CONFIRMED,
      ambiguous: false,
    };
  }

  const weekdayMatch = phrase.match(/\b(sunday|monday|tuesday|wednesday|thursday|friday|saturday)\b/);
  if (weekdayMatch) {
    const target = WEEKDAY[weekdayMatch[1]];
    const current = now.getUTCDay();
    let delta = (target - current + 7) % 7;
    if (delta === 0) delta = 7;
    const d = addDays(today, delta);
    const morning = /morning/.test(phrase);
    return {
      original: raw,
      normalized: {
        kind: morning ? 'window' : 'day',
        iso: d.toISOString().slice(0, 10),
        part: morning ? 'morning' : null,
      },
      epistemic: /next week|later/i.test(phrase) ? EPISTEMIC_CATEGORY.UNCERTAIN : EPISTEMIC_CATEGORY.INFERRED,
      ambiguous: /sometime|maybe|around/i.test(phrase),
    };
  }

  if (/this week/.test(phrase)) {
    const end = addDays(today, 7 - now.getUTCDay());
    return {
      original: raw,
      normalized: { kind: 'range', starts_at: today.toISOString(), ends_at: end.toISOString() },
      epistemic: EPISTEMIC_CATEGORY.INFERRED,
      ambiguous: false,
    };
  }
  if (/next week/.test(phrase)) {
    const start = addDays(today, 7 - now.getUTCDay() + 1);
    const end = addDays(start, 6);
    return {
      original: raw,
      normalized: { kind: 'range', starts_at: start.toISOString(), ends_at: end.toISOString() },
      epistemic: EPISTEMIC_CATEGORY.INFERRED,
      ambiguous: true,
    };
  }
  if (/this month|sometime this month/.test(phrase)) {
    return {
      original: raw,
      normalized: { kind: 'month', month: now.getUTCMonth() + 1, year: now.getUTCFullYear() },
      epistemic: EPISTEMIC_CATEGORY.UNCERTAIN,
      ambiguous: true,
    };
  }

  return {
    original: raw,
    normalized: { kind: 'phrase', text: raw },
    epistemic: EPISTEMIC_CATEGORY.UNCERTAIN,
    ambiguous: true,
  };
}

function extractTemporalReferences(text, now = new Date()) {
  const lower = String(text || '').toLowerCase();
  const patterns = [
    /\b(today|yesterday|tomorrow)\b/gi,
    /\b(this week|next week|later this week)\b/gi,
    /\b(sunday|monday|tuesday|wednesday|thursday|friday|saturday)(?:\s+morning)?\b/gi,
    /\bsometime this month\b/gi,
  ];
  const out = [];
  const seen = new Set();
  for (const re of patterns) {
    let m;
    const r = new RegExp(re.source, re.flags);
    while ((m = r.exec(text)) !== null) {
      const key = m[0].toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      const resolved = normalizeTemporalPhrase(m[0], now);
      out.push({
        id: `temp_${out.length + 1}`,
        phrase: m[0],
        ...resolved,
      });
    }
  }
  return out;
}

module.exports = {
  normalizeTemporalPhrase,
  extractTemporalReferences,
};
