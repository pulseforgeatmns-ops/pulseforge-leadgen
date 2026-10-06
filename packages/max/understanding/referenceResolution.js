'use strict';

const { AMBIGUITY_KIND, ENTITY_KIND, CONTACT_ROLE } = require('./types');

function resolvePronoun({ pronoun, memory, threadContacts = [], accountName = null }) {
  const p = String(pronoun || '').toLowerCase();
  const genderHint = /\bhe\b|\bhim\b|\bhis\b/.test(p) ? 'male' : /\bshe\b|\bher\b/.test(p) ? 'female' : null;

  const inThread = threadContacts.filter(c => c.kind === ENTITY_KIND.CONTACT || c.kind === 'contact');
  if (memory?.durableLoadFailed && genderHint && inThread.length === 0) {
    return {
      entity: null,
      ambiguous: true,
      ambiguity: {
        kind: AMBIGUITY_KIND.PRONOUN,
        pronoun,
        candidates: [],
        clarification: 'Prior conversational context is unavailable — who did you mean?',
        staleContext: true,
        memoryUnavailable: true,
      },
    };
  }
  if (inThread.length === 1) {
    return { entity: inThread[0], ambiguous: false };
  }
  if (inThread.length > 1) {
    const males = inThread.filter(c => c.gender === 'male');
    if (genderHint === 'male' && males.length === 1) {
      return { entity: males[0], ambiguous: false };
    }
    return {
      entity: null,
      ambiguous: true,
      ambiguity: {
        kind: AMBIGUITY_KIND.PRONOUN,
        pronoun,
        candidates: inThread.map(c => c.name),
        clarification: buildPronounClarification(inThread, accountName),
      },
    };
  }

  const recent = memory?.recentContacts({ genderHint, accountName }) || [];
  const unique = [];
  const seen = new Set();
  for (const c of recent) {
    const key = `${c.name}:${c.accountName || ''}`;
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(c);
  }

  if (unique.length === 1) {
    const staleTurns = memory?.unrelatedAccountTurnsSinceContact?.(unique[0].name);
    if (typeof staleTurns === 'number' && staleTurns >= 2) {
      return {
        entity: null,
        ambiguous: true,
        ambiguity: {
          kind: AMBIGUITY_KIND.PRONOUN,
          pronoun,
          candidates: [unique[0].name],
          clarification: `Which contact did you mean by "${pronoun}"? Recent context may be stale.`,
          staleContext: true,
        },
      };
    }
    return { entity: unique[0], ambiguous: false };
  }
  if (unique.length > 1) {
    return {
      entity: null,
      ambiguous: true,
      ambiguity: {
        kind: AMBIGUITY_KIND.PRONOUN,
        pronoun,
        candidates: unique.map(c => c.name),
        clarification: buildPronounClarification(unique, accountName),
      },
    };
  }
  return { entity: null, ambiguous: false };
}

function buildPronounClarification(candidates, accountName) {
  if (candidates.length === 2) {
    return `Do you mean ${candidates[0].name}${accountName ? ` at ${accountName}` : ''}, or ${candidates[1].name}?`;
  }
  const names = candidates.map(c => c.name).join(', ');
  return `Which contact did you mean (${names})?`;
}

function applyRoleCorrection({ contactEntity, correctionText }) {
  const lower = String(correctionText || '').toLowerCase();
  if (!contactEntity) return null;
  const updated = { ...contactEntity };
  if (/not (?:the )?decision maker|isn'?t (?:the )?decision maker|not actually the decision maker/i.test(lower)) {
    updated.role = CONTACT_ROLE.INFLUENCER;
    updated.decisionMaker = false;
    updated.epistemic = 'confirmed';
    return {
      contact: updated,
      correction: {
        targetClaim: 'decision_maker_role',
        priorValue: CONTACT_ROLE.SUSPECTED_DECISION_MAKER,
        newValue: CONTACT_ROLE.INFLUENCER,
        text: correctionText,
      },
    };
  }
  return null;
}

module.exports = {
  resolvePronoun,
  applyRoleCorrection,
  buildPronounClarification,
};
