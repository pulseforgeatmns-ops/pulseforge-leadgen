'use strict';

/**
 * SPEC-112 Phase 1 — Market Understanding.
 * ICP is reasoning, not demographics.
 */

const { asText, asList, isPlainObject, haystack } = require('./types');

function buildReasoningField(partial = {}, fallbackQuestion = '') {
  if (typeof partial === 'string') {
    return {
      question: fallbackQuestion,
      reasoning: asText(partial),
      known: Boolean(asText(partial)),
      unknowns: asText(partial) ? [] : [fallbackQuestion || 'Not yet supplied.'],
      signals: [],
      exclusions: [],
    };
  }
  const reasoning = asText(partial.reasoning || partial.summary || partial.value);
  const unknowns = asList(partial.unknowns);
  const known = partial.known != null ? Boolean(partial.known) : Boolean(reasoning);
  if (!known && !unknowns.length) {
    unknowns.push(fallbackQuestion || 'Not yet supplied.');
  }
  return {
    question: asText(partial.question) || fallbackQuestion,
    reasoning,
    known,
    unknowns,
    signals: asList(partial.signals || partial.include),
    exclusions: asList(partial.exclusions || partial.avoid),
  };
}

function buildMission(partial = {}) {
  const transformation = asText(
    partial.transformation || partial.statement || partial.summary
  );
  return {
    question: asText(partial.question) || 'What transformation does this client create?',
    transformation,
    known: Boolean(transformation),
    unknowns: transformation ? asList(partial.unknowns) : ['Mission transformation is not yet supplied.'],
  };
}

function buildIcp(partial = {}) {
  const src = isPlainObject(partial) ? partial : {};
  return {
    kind: 'reasoning',
    company: buildReasoningField(
      src.company,
      'What kinds of businesses?'
    ),
    founder: buildReasoningField(
      src.founder,
      'What stage are they in?'
    ),
    size: buildReasoningField(
      src.size,
      'Employees? Revenue?'
    ),
    geography: buildReasoningField(
      src.geography,
      'Where?'
    ),
    exclusions: buildReasoningField(
      src.exclusions,
      'Who should we avoid?'
    ),
  };
}

function buildTransformation(partial = {}) {
  const currentState = asText(partial.currentState || partial.current);
  const futureState = asText(partial.futureState || partial.future);
  return {
    currentState,
    futureState,
    known: Boolean(currentState && futureState),
    unknowns: [
      ...(currentState ? [] : ['Current state is not yet supplied.']),
      ...(futureState ? [] : ['Future state is not yet supplied.']),
    ],
  };
}

function buildMarketUnderstanding(partial = {}) {
  return {
    mission: buildMission(partial.mission),
    icp: buildIcp(partial.icp),
    transformation: buildTransformation(partial.transformation),
  };
}

function textBlob(prospect = {}) {
  return haystack([
    prospect.name,
    prospect.companyName,
    prospect.industry,
    prospect.vertical,
    prospect.segment,
    prospect.jobTitle,
    prospect.description,
    prospect.snippet,
    prospect.website,
    prospect.notes,
    prospect.founderStage,
    prospect.size,
    prospect.employeeCount,
    prospect.revenue,
    prospect.location,
    prospect.address,
    prospect.ownership,
    ...(Array.isArray(prospect.signals)
      ? prospect.signals.map((s) => s.label || s.text || s.type)
      : []),
    ...(Array.isArray(prospect.observations) ? prospect.observations.map((o) => o.text || o) : []),
  ]);
}

function fieldHits(field, blob) {
  if (!field || !field.known) return { hit: false, matched: [], unknown: true };
  const needles = field.signals.length ? field.signals : [];
  const matched = needles.filter((n) => blob.includes(String(n).toLowerCase()));
  if (!needles.length) {
    return { hit: Boolean(field.reasoning), matched: [], unknown: true };
  }
  return { hit: matched.length > 0, matched, unknown: false };
}

function exclusionHits(field, blob) {
  const needles = field && field.signals && field.signals.length
    ? field.signals
    : field && field.exclusions
      ? field.exclusions
      : [];
  return needles.filter((n) => blob.includes(String(n).toLowerCase()));
}

/**
 * Score ICP fit as reasoning over evidence — not a demographic checklist.
 * Unknown size/geography do not invent a match; they lower confidence later.
 *
 * @param {object} icp
 * @param {object} prospect
 * @returns {{ score: number, reasons: string[], unknowns: string[], excluded: boolean }}
 */
function evaluateIcpFit(icp, prospect = {}) {
  const reasons = [];
  const unknowns = [];
  const blob = textBlob(prospect);

  const excluded = exclusionHits(icp.exclusions, blob);
  if (excluded.length) {
    return {
      score: 0.08,
      reasons: [
        `Excluded: evidence matches "${excluded[0]}" — AIM says avoid businesses that already sit outside this transformation.`,
      ],
      unknowns: [],
      excluded: true,
      matched: { company: [], founder: [], size: [], geography: [] },
    };
  }

  const company = fieldHits(icp.company, blob);
  const founder = fieldHits(icp.founder, blob);
  const size = fieldHits(icp.size, blob);
  const geography = fieldHits(icp.geography, blob);

  let score = 0.2;
  if (company.hit) {
    score += 0.28;
    reasons.push(
      `Company resembles the AIM ICP: ${company.matched[0] || icp.company.reasoning}`
    );
  } else if (company.unknown) {
    unknowns.push('Company-type evidence is thin relative to AIM reasoning.');
  } else {
    reasons.push('Company does not yet show founder-led / operator-owned characteristics.');
  }

  if (founder.hit) {
    score += 0.28;
    reasons.push(
      `Founder stage matches AIM: ${founder.matched[0] || icp.founder.reasoning}`
    );
  } else if (founder.unknown) {
    unknowns.push(icp.founder.unknowns[0] || 'Founder stage is not evidenced.');
  } else {
    reasons.push('No evidence the founder is still in the operating loop.');
  }

  if (!icp.size.known) {
    unknowns.push(icp.size.unknowns[0] || 'Size bands are not yet named by the client.');
  } else if (size.hit) {
    score += 0.12;
    reasons.push(`Size evidence: ${size.matched[0]}`);
  } else {
    unknowns.push('Employee/revenue evidence is missing; size is not scored as a match.');
  }

  if (!icp.geography.known) {
    unknowns.push(icp.geography.unknowns[0] || 'Geography is not a named beachhead yet.');
  } else if (geography.hit) {
    score += 0.12;
    reasons.push(`Geography evidence: ${geography.matched[0]}`);
  } else if (prospect.location || prospect.address) {
    reasons.push('Location is present but does not match the named beachhead.');
    score -= 0.08;
  }

  if (company.hit && founder.hit) {
    reasons.push('This looks like a business the client transforms, not merely a demographic hit.');
  }

  return {
    score: Math.max(0, Math.min(0.95, score)),
    reasons,
    unknowns,
    excluded: false,
    matched: {
      company: company.matched,
      founder: founder.matched,
      size: size.matched,
      geography: geography.matched,
    },
  };
}

module.exports = {
  buildMission,
  buildIcp,
  buildTransformation,
  buildMarketUnderstanding,
  evaluateIcpFit,
  textBlob,
};
