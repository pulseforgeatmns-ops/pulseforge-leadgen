'use strict';

const { RESOLUTION } = require('./types');
const { levenshtein, normalizeText } = require('./claimParser');

function scoreNameMatch(query, candidate) {
  const left = normalizeText(query).toLowerCase();
  const right = normalizeText(candidate).toLowerCase();
  if (!left || !right) return 0;
  if (left === right) return 1;
  if (right.includes(left) || left.includes(right)) return 0.92;
  const dist = levenshtein(left, right);
  const maxLen = Math.max(left.length, right.length);
  return Math.max(0, 1 - dist / maxLen);
}

function resolveAo(claim, context = {}) {
  const name = claim.payload?.name || claim.payload?.ao_name;
  if (!name) {
    return { status: RESOLUTION.UNRESOLVED, candidates: [], entity: null };
  }
  const users = context.users || [];
  const matches = users
    .map(u => ({ user: u, score: scoreNameMatch(name, u.name) }))
    .filter(m => m.score >= 0.85)
    .sort((a, b) => b.score - a.score);
  if (matches.length === 1) {
    return { status: RESOLUTION.RESOLVED, entity: matches[0].user, candidates: matches, confidence: matches[0].score };
  }
  if (matches.length > 1 && matches[0].score - (matches[1]?.score || 0) < 0.05) {
    return { status: RESOLUTION.AMBIGUOUS, candidates: matches, entity: null };
  }
  if (matches.length > 1) {
    return { status: RESOLUTION.RESOLVED, entity: matches[0].user, candidates: matches, confidence: matches[0].score };
  }
  return { status: RESOLUTION.UNRESOLVED, candidates: [], entity: null };
}

function resolveAccount(claim, context = {}) {
  const name = claim.payload?.name;
  if (!name) {
    return { status: RESOLUTION.UNRESOLVED, candidates: [], entity: null };
  }
  const companies = context.companies || [];
  const prospects = context.prospects || [];
  const companyMatches = companies
    .map(c => ({ kind: 'company', entity: c, score: scoreNameMatch(name, c.name) }))
    .filter(m => m.score >= 0.78);
  const prospectMatches = prospects
    .map(p => ({
      kind: 'prospect',
      entity: p,
      score: scoreNameMatch(name, p.company_name || p.account_name || ''),
    }))
    .filter(m => m.score >= 0.78);

  const matches = [...companyMatches, ...prospectMatches].sort((a, b) => b.score - a.score);
  if (matches.length === 0) {
    return { status: RESOLUTION.UNRESOLVED, candidates: [], entity: null, provisionalName: name };
  }
  if (
    matches.length > 1
    && matches[0].kind === 'company'
    && matches[1].kind === 'prospect'
    && matches[1].entity?.company_id === matches[0].entity?.id
    && matches[0].score >= 0.82
  ) {
    return {
      status: RESOLUTION.RESOLVED,
      entity: matches[1].entity,
      kind: 'prospect',
      candidates: matches,
      confidence: matches[1].score,
    };
  }
  if (matches.length > 1 && matches[0].score < 0.95 && Math.abs(matches[0].score - matches[1].score) < 0.04) {
    return { status: RESOLUTION.AMBIGUOUS, candidates: matches, entity: null };
  }
  const top = matches[0];
  if (top.score >= 0.95) {
    return { status: RESOLUTION.RESOLVED, entity: top.entity, kind: top.kind, candidates: matches, confidence: top.score };
  }
  if (top.score >= 0.82) {
    return {
      status: RESOLUTION.PROVISIONALLY_RESOLVED,
      entity: top.entity,
      kind: top.kind,
      candidates: matches,
      confidence: top.score,
    };
  }
  return { status: RESOLUTION.AMBIGUOUS, candidates: matches, entity: null };
}

function resolveContact(claim, context = {}, bindings = {}) {
  const account = bindings.account?.entity;
  const ao = bindings.ao?.entity;
  if (!account && !claim.payload?.name) {
    return { status: RESOLUTION.UNRESOLVED, candidates: [], entity: null };
  }
  const contacts = context.contacts || [];
  const filtered = contacts.filter(c => {
    if (account?.id && c.prospect_id && c.prospect_id !== account.id && c.company_id !== account.id) {
      return false;
    }
    if (claim.payload?.name && scoreNameMatch(claim.payload.name, c.name || `${c.first_name || ''} ${c.last_name || ''}`) < 0.85) {
      return false;
    }
    if (ao?.id && c.ao_id && String(c.ao_id) !== String(ao.id)) return false;
    return true;
  });
  if (claim.payload?.name && filtered.length === 1) {
    return { status: RESOLUTION.RESOLVED, entity: filtered[0], candidates: filtered };
  }
  if (claim.payload?.name && filtered.length === 0) {
    return {
      status: RESOLUTION.UNRESOLVED,
      candidates: [],
      entity: null,
      createCandidate: { ...claim.payload, ao_id: ao?.id || null, prospect_id: account?.id || null },
    };
  }
  if (/his contact|existing contact/i.test(claim.payload?.description || '')) {
    const related = contacts.filter(c => {
      if (!account) return false;
      return c.prospect_id === account.id || c.company_id === account.company_id;
    });
    if (related.length === 1) {
      return { status: RESOLUTION.RESOLVED, entity: related[0], candidates: related };
    }
    if (related.length > 1) {
      return { status: RESOLUTION.AMBIGUOUS, candidates: related, entity: null };
    }
  }
  if (claim.payload?.email || claim.payload?.phone) {
    return {
      status: RESOLUTION.UNRESOLVED,
      candidates: [],
      entity: null,
      createCandidate: { ...claim.payload, ao_id: ao?.id || null, prospect_id: account?.id || null },
    };
  }
  return { status: RESOLUTION.UNRESOLVED, candidates: filtered, entity: null };
}

function resolveClaim(claim, context, bindings = {}) {
  switch (claim.claim_type) {
    case 'AO':
      return resolveAo(claim, context);
    case 'ACCOUNT':
      return resolveAccount(claim, context);
    case 'CONTACT':
    case 'RELATIONSHIP':
      return resolveContact(claim, context, bindings);
    default:
      return { status: RESOLUTION.RESOLVED, entity: null, meta: true };
  }
}

module.exports = {
  resolveAo,
  resolveAccount,
  resolveContact,
  resolveClaim,
  scoreNameMatch,
};
