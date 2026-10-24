'use strict';

const { EPISTEMIC_CATEGORY } = require('./types');

const TEMPORAL_ROLE = Object.freeze({
  EXPECTED_EVENT_TIME: 'expected_event_time',
  DEADLINE: 'deadline',
  FOLLOW_UP_TIME: 'follow_up_time',
  CONDITIONAL_FOLLOW_UP_TIME: 'conditional_follow_up_time',
  AVAILABILITY_TIME: 'availability_time',
  CORRECTED_TIME: 'corrected_time',
});

const DAY_TOKEN = /\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday)(?:\s+morning)?\b/gi;

function hasConditionalFollowUpContext(lower) {
  return /\bif i don'?t\b|\bif i do not\b|\bif no\b|\bif i don'?t hear\b|\bif i do not hear\b/i.test(lower);
}

function hasTemporalCorrectionLanguage(text, matchIndex = 0) {
  const lower = String(text || '').toLowerCase();
  const slice = lower.slice(Math.max(0, matchIndex - 48), matchIndex + 96);
  return /\b(?:actually|sorry|i meant|meant to say|not \w+,)\b/.test(slice)
    || /—\s*actually/.test(slice)
    || /\bno,?\s+sorry\b/.test(slice);
}

function extractTemporalCorrections(text, dayMatches) {
  const raw = String(text || '');
  const corrections = [];
  if (!dayMatches || dayMatches.length < 2) return corrections;

  for (let i = 0; i < dayMatches.length - 1; i += 1) {
    const prior = dayMatches[i];
    const finalPhrase = dayMatches[dayMatches.length - 1];
    const idx = raw.toLowerCase().indexOf(prior.toLowerCase());
    if (!hasTemporalCorrectionLanguage(raw, idx >= 0 ? idx : 0)) continue;
    if (prior.toLowerCase() === finalPhrase.toLowerCase()) continue;
    corrections.push({ priorValue: prior, newValue: finalPhrase });
  }
  return corrections;
}

function classifyTemporalRoles(text, temporalRefs = []) {
  const lower = String(text || '').toLowerCase();
  const conditional = hasConditionalFollowUpContext(lower);
  const deadlineIdx = lower.search(/\b(?:hear back|call back|callback|response|should hear|by)\b/);

  for (const ref of temporalRefs) {
    const phraseLower = ref.phrase.toLowerCase();
    const idx = lower.indexOf(phraseLower);
    if (conditional && /follow[- ]?up|remind me/i.test(lower.slice(Math.max(0, idx - 30), idx + phraseLower.length + 20))) {
      ref.role = TEMPORAL_ROLE.CONDITIONAL_FOLLOW_UP_TIME;
      continue;
    }
    if (deadlineIdx >= 0 && idx >= deadlineIdx - 8 && /\bby\b/.test(lower.slice(Math.max(0, idx - 12), idx + 4))) {
      ref.role = TEMPORAL_ROLE.DEADLINE;
      continue;
    }
    if (/out until|available|there then|usually there/i.test(lower.slice(Math.max(0, idx - 24), idx + phraseLower.length + 24))) {
      ref.role = TEMPORAL_ROLE.AVAILABILITY_TIME;
      continue;
    }
    if (ref.canonical && ref.supersedes) {
      ref.role = TEMPORAL_ROLE.CORRECTED_TIME;
      continue;
    }
    ref.role = ref.role || TEMPORAL_ROLE.EXPECTED_EVENT_TIME;
  }
  return temporalRefs;
}

function dedupeCorrections(corrections = []) {
  const seen = new Set();
  const out = [];
  for (const corr of corrections) {
    if (corr.kind === 'temporal') {
      const prior = String(corr.priorValue || '').toLowerCase().trim();
      const next = String(corr.newValue || '').toLowerCase().trim();
      if (!prior || prior === next) continue;
    }
    const key = `${corr.kind}:${corr.contactName || ''}:${corr.priorValue}:${corr.newValue}:${corr.targetClaim || ''}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(corr);
  }
  return out;
}

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
  TEMPORAL_ROLE,
  DAY_TOKEN,
  normalizeTemporalPhrase,
  extractTemporalReferences,
  hasConditionalFollowUpContext,
  hasTemporalCorrectionLanguage,
  extractTemporalCorrections,
  classifyTemporalRoles,
  dedupeCorrections,
};
